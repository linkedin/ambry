package com.github.ambry.router;

import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.ConnectionManager;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


enum ChunkState {
  Free,
  Building,
  Ready,
  Failed,
  Succeeded
}

/** PutChunk class
 *
 */
class PutChunk {
  long chunkId; //@todo long vs int in all fields.
  ChunkState state;
  ByteBuffer buf;
  ByteBufferChannel chunkChannel;
  int requestParallelism;
  int successTarget;
  int successCount;
  int failureCount;
  int requestsInProgress;

  public PutChunk() {
    state = ChunkState.Free;

    // @todo make these configurable.
    requestParallelism = 3;
    successTarget = 2;
    successCount = 0;
    failureCount = 0;
    requestsInProgress = 0;
  }

  void reiInitialize() {
    //@todo use the Buffer pool to free the buffer.
    this.chunkId = -1;
    chunkChannel = null;
    state = ChunkState.Free;
    successCount = 0;
    failureCount = 0;
    requestsInProgress = 0;
  }

  boolean isFree() {
    return state == ChunkState.Free;
  }

  boolean isBuilding() {
    return state == ChunkState.Building;
  }

  boolean isReady() {
    return state == ChunkState.Ready;
  }

  boolean isComplete() {
    return state == ChunkState.Failed || state == ChunkState.Succeeded;
  }

  void prepare(long chunkId, int size) {
    // @todo use the Buffer pool later.
    this.chunkId = chunkId;
    buf = ByteBuffer.allocate(size);
    chunkChannel = new ByteBufferChannel(buf);
    state = ChunkState.Building;
  }

  int write(ReadableStreamChannel srcChannel)
      throws IOException {
    // @todo assert that state is Building
    int written = srcChannel.read(chunkChannel);
    if (buf.position() == buf.capacity()) {
      state = ChunkState.Ready;
      // flip as write has finished. However, we will not use the ByteBuffer methods to read
      // as we need to read from the underlying byte array multiple times.
      buf.flip();
    }
    return written;
  }

  void fetchPutRequests(List<NetworkSend> requests) {
    // @todo:
    // This will internally handle timeouts, quorums,
    // slip puts and return list of requests accordingly. When requests are
    // created, the putChunks will internally checkout connections from the
    // connection manager, if there are any. If no connection is available, then
    // the connection manager will return null (after initiating new connections
    // if required). Only if a connection is successfully checked out will a
    // request be created and returned by the putChunk.
  }
}

class MetadataPutChunk extends PutChunk {
  //@todo
}

/** PutManager class
 *
 */
public class PutManager {
  private static final Logger logger = LoggerFactory.getLogger(PutManager.class);
  private Thread chunkFillerThread;
  private ConcurrentSkipListMap<Long, PutOperation> putOperations;
  private final int maxChunkSize;
  ConnectionManager connectionManager;

  public PutManager(int maxChunkSize, ConnectionManager connectionManager) {
    this.maxChunkSize = maxChunkSize;
    this.connectionManager = connectionManager;
    this.chunkFillerThread = Utils.newThread("ChunkFiller thread", new ChunkFiller(), false);
  }

  public FutureResult<String> submitPutBlobOperation(long operationId, BlobProperties blobProperties,
      byte[] usermetadata, ReadableStreamChannel channel, Callback<String> callback) {
    FutureResult<String> futureResult = new FutureResult<String>();
    PutOperation putOperation =
        new PutOperation(operationId, blobProperties, usermetadata, channel, futureResult, callback);
    putOperations.put(operationId, putOperation);
    return futureResult;
  }

  public void poll(List<NetworkSend> requests) {
    Iterator<PutOperation> iter = putOperations.values().iterator();
    while (iter.hasNext()) {
      PutOperation op = iter.next();
      op.fetchPutRequests(requests);
    }
  }

  List<String> getIdsToDelete() {
    // @todo save and return ids of failed puts.
    return null;
  }

  /** PutOperation class
   *
   */
  class PutOperation {
    long operationId;
    BlobId blobId;
    final BlobProperties blobProperties;
    final byte[] userMetadata;
    final ReadableStreamChannel channel;
    final FutureResult<String> futureResult;
    final Callback<String> callback;
    MetadataPutChunk metadataPutChunk;
    PutChunk[] putChunks;
    // at any time, only one chunk will be in BUILDING state. This is the index to that chunk. A value of -1
    // means that no chunk is in Building state.
    // points to the chunk in the array that is in BUILDING state, if there is one.
    PutChunk buildingChunk;
    long currentChunkId;
    final long numChunks;

    PutOperation(long operationId, BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
        FutureResult<String> futureResult, Callback<String> callback) {
      this.operationId = operationId;
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.channel = channel;
      this.futureResult = futureResult;
      this.callback = callback;
      this.numChunks = blobProperties.getBlobSize() == 0 ? 1 : (blobProperties.getBlobSize() - 1) / maxChunkSize + 1;
      this.putChunks = new PutChunk[4]; //@todo
      this.currentChunkId = -1;
    }

    int getChunkSize() {
      if (currentChunkId + 1 == numChunks) {
        if (blobProperties.getBlobSize() == 0) {
          return 0;
        }
        return Math.min(maxChunkSize, (int) (blobProperties.getBlobSize() % maxChunkSize));
      } else {
        return maxChunkSize;
      }
    }

    /**
     * For this operation, get a list of put requests for chunks (in the form of NetworkSends) to send out.
     * @return
     */
    void fetchPutRequests(List<NetworkSend> requests) {
      for (PutChunk chunk : putChunks) {
        if (chunk.isReady()) {
          chunk.fetchPutRequests(requests);
        }
      }
    }

    /**
     * Get the chunk to be filled.
     * Whenever this method is called, at most one chunk will be in Building state, and if there is
     * one, that will always be the chunk to fill.
     * @return the chunk to fill.
     */
    PutChunk getChunkToFill() {
      if (buildingChunk != null) {
        return buildingChunk;
      } else {
        for (PutChunk chunk : putChunks) {
          if (chunk.isFree()) {
            currentChunkId++;
            chunk.prepare(currentChunkId, getChunkSize());
            buildingChunk = chunk;
            return chunk;
          }
        }
      }
      return null;
    }

    void fillChunks() {
      boolean done = false;
      while (!done) {
        PutChunk chunkToFill = getChunkToFill();
        if (chunkToFill == null) {
          done = true;
        } else {
          try {
            chunkToFill.write(channel);
            // if the chunk is not fully read, there is no more data available. Otherwise,
            // continue filling in chunks.
            if (!chunkToFill.isReady()) {
              done = true;
            }
          } catch (IOException e) {
            logger.error("IO Exception when reading from channel for put operation " + operationId);
            //@todo error handling for an operation - calling callback, removing from putOperations etc.
          }
        }
      }
    }
  }

  /** ChunkFiller class
   *
   */
  class ChunkFiller implements Runnable {
    public void run() {
      while (true) {
        Iterator<PutOperation> iter = putOperations.values().iterator();
        while (iter.hasNext()) {
          PutOperation op = iter.next();
          op.fillChunks();
        }
        // yield?
      }
    }
  }
}
