package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;

import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


enum ChunkState {
  Free,
  Building,
  Ready,
  Failed,
  Succeeded
}

/**
 * PutOperation class is responsible for all the state associated with a particular put operation.
 */
class PutOperation {
  private final PutManager putManager;
  private final long operationId;
  private final ByteBufferAsyncWritableChannel chunkFillerChannel;
  private final int numChunks;
  private final long blobSize;
  private long bytesFilled;
  private final Time time;
  protected final PutChunk[] putChunks;
  private PutChunk latestFillerChunk;
  private int currentChunkIndex; //@todo rename
  private ByteBuffer channelReadBuffer;

  // these are not private as the PutManager needs to access it.
  final FutureResult<String> futureResult;
  final Callback<String> callback;
  final MetadataPutChunk metadataPutChunk;
  final BlobProperties blobProperties;
  final byte[] userMetadata;
  BlobId blobId;
  volatile boolean operationComplete = false;
  volatile Exception operationException;

  private static final int chunkArrayLength = 4;
  private static final AtomicInteger correlationIdGenerator = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(PutOperation.class);

  /**
   * Construct a PutOperation.
   * @param putManager the {@link PutManager} associated with this PutOperation.
   * @param operationId the operation id of the put operation.
   * @param blobProperties the BlobProperties associated with the put operation.
   * @param userMetadata the userMetadata associated with the put operation.
   * @param chunkFillerChannel the {@link AsyncWritableChannel} containing the blob data.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param time the Time instance to use.
   */
  PutOperation(PutManager putManager, long operationId, BlobProperties blobProperties, byte[] userMetadata,
      ByteBufferAsyncWritableChannel chunkFillerChannel, FutureResult<String> futureResult, Callback<String> callback,
      Time time) {
    this.putManager = putManager;
    this.operationId = operationId;
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.chunkFillerChannel = chunkFillerChannel;
    this.futureResult = futureResult;
    this.callback = callback;
    this.time = time;
    this.blobSize = blobProperties.getBlobSize();
    this.bytesFilled = 0;
    //@todo handle zero byte data in tests.
    // numChunks will never be more than Integer.MAX
    numChunks = (int) (blobSize == 0 ? 1 : (blobSize - 1) / putManager.maxChunkSize + 1);
    putChunks = new PutChunk[chunkArrayLength]; //@todo
    //@todo: we may not need to do this in the constructor.
    for (int i = 0; i < chunkArrayLength; i++) {
      putChunks[i] = new PutChunk(); //@todo this is not good, do not pass this in the constructor.
    }
    currentChunkIndex = -1;
    if (numChunks > 1) {
      metadataPutChunk = new MetadataPutChunk();
    } else {
      metadataPutChunk = null;
    }
  }

  int getNumChunks() {
    return numChunks;
  }

  /**
   * Get the chunk size of the chunk at the given position.
   * @param pos the position of the chunk in the overall blob.
   * @return the size of the chunk.
   */
  int getChunkSize(int pos) {
    if (pos + 1 == numChunks) {
      if (blobProperties.getBlobSize() == 0) {
        return 0;
      }
      return Math.min(putManager.maxChunkSize, (int) (blobProperties.getBlobSize() % putManager.maxChunkSize));
    } else {
      return putManager.maxChunkSize;
    }
  }

  /**
   * For this operation, get a list of put requests for chunks (in the form of NetworkSends) to send out.
   * @return
   */
  void fetchChunkPutRequests(List<RequestInfo> requests) {
    /*if (operationComplete) {
      completePutOperation();
    }*/
    try {
      if (metadataPutChunk.isReady()) {
        metadataPutChunk.fetchRequests(requests);
      } else {
        for (PutChunk chunk : putChunks) {
          if (chunk.isReady()) {
            chunk.fetchRequests(requests);
          }
        }
      }
    } catch (RouterException e) {
      //@todo if (abortable) then do this.
      operationException = e;
      completeOperation();
    }
  }

  void resolvePutChunk(PutChunk chunk) {
    if (chunk.getChunkBlobId() == null) {
      // there was an error
      if (operationException == null) {
        operationException = new RouterException("Could not complete operation", RouterErrorCode.AmbryUnavailable);
      }
      // operation has failed.
      completeOperation();
    } else if (numChunks == 1 || chunk == metadataPutChunk) {
      blobId = chunk.getChunkBlobId();
      completeOperation();
    } else {
      // a data chunk has succeeded.
      metadataPutChunk.addChunkId(chunk.chunkBlobId, chunk.chunkIndex);
    }
  }

  private void completeOperation() {
    // do all the cleanup here
    putManager.onComplete(operationId);
  }

  /**
   * This method runs in the context of the ChunkFiller thread. As long as there are chunks available to
   * be written to, It gets a chunk that is to be filled and keeps filling it with the data from the chunkFillerChannel,
   * if there is any.
   * @return
   */

  boolean fillChunks() {
    boolean workDone = false;
    PutChunk chunkToFill;
    while (bytesFilled < blobSize) {
      // Attempt to fill a chunk
      if (channelReadBuffer == null) {
        try {
          channelReadBuffer = chunkFillerChannel.getNextChunk(0);
        } catch (InterruptedException e) {
          // ignore.
        }
      }
      if (channelReadBuffer != null) {
        chunkToFill = getChunkToFill();
        if (chunkToFill == null ) {
          // no chunks are free to be filled yet.
          break;
        }
        bytesFilled += chunkToFill.fillFrom(channelReadBuffer);
        if (chunkToFill.isFull() || bytesFilled == blobSize) {
          // There is no more data to be filled into this chunk.
          chunkToFill.prepareForSending();
        }
        workDone = true;
        if (!channelReadBuffer.hasRemaining()) {
          chunkFillerChannel.resolveChunk(channelReadBuffer, null);
          channelReadBuffer = null;
        }
      } else {
        // channel does not have more data yet.
        break;
      }
    }
    return workDone;
  }

  boolean fillChunks1() {
    PutChunk chunkToFill;
    boolean workDone = false;
    do {
      chunkToFill = getChunkToFill();
      if (chunkToFill == null) {
        break;
      }
      if (channelReadBuffer == null) {
        try {
          channelReadBuffer = chunkFillerChannel.getNextChunk(0);
          if (channelReadBuffer == null) {
            chunkToFill.prepareForSending();
            break;
          }
        } catch (InterruptedException e) {
          // No biggie.
        }
      }
      if (channelReadBuffer != null) {
        chunkToFill.fillFrom(channelReadBuffer);
        if (!channelReadBuffer.hasRemaining()) {
          chunkFillerChannel.resolveChunk(channelReadBuffer, null);
          channelReadBuffer = null;
        }
        workDone = true;
      } else {
        chunkToFill.prepareForSending();
      }
      // if the chunk is not fully read, there is no more data available. Otherwise,
      // continue filling in chunks.
    } while (chunkToFill.isReady());
    return workDone;
  }

  /**
   * Get the chunk to be filled. At most one chunk for an operation will ever be in Building state. If there is such
   * a chunk, that is returned. If not, if there is a Free chunk, that is returned. If no Free chunks are available
   * either, then null is returned.
   * @return the chunk to fill.
   */
  private PutChunk getChunkToFill() {
    if (latestFillerChunk != null && latestFillerChunk.isBuilding()) {
      return latestFillerChunk;
    } else {
      for (PutChunk chunk : putChunks) {
        if (chunk.isFree()) {
          currentChunkIndex++;
          chunk.prepareForBuilding(currentChunkIndex, getChunkSize(currentChunkIndex));
          latestFillerChunk = chunk;
          return chunk;
        }
      }
    }
    // No free or building chunks
    return null;
  }

  void setOperationException(Exception exception) {
    operationException = exception;
    operationComplete = true;
  }

  class ChunkPutRequest {
    int correlationId;
    ReplicaId replicaId;
    long startTimeMs;
    //boolean done;

    ChunkPutRequest(int correlationId, ReplicaId replicaId, long startTimeMs) {
      this.correlationId = correlationId;
      this.replicaId = replicaId;
      this.startTimeMs = startTimeMs;
      //this.done = false;
    }
  }

  /** PutChunk class
   *
   */
  class PutChunk {
    private int chunkIndex; //@todo long vs int in all fields.
    protected BlobId chunkBlobId;
    protected int size;
    private Exception chunkException;
    private volatile ChunkState state;
    ByteBuffer buf;
    private OperationTracker operationTracker;
    int failedAttempts = 0;
    PartitionId partitionId;
    ConcurrentSkipListMap<Integer, ChunkPutRequest> correlationIdToChunkPutRequest;

    // @todo: should this be a config?
    // this is the maximum number of times we will retry a put with a different partitions,
    // before failing the chunk put operation (which will also fail the entire operation).
    private static final int MAX_SLIP_PUTS = 1;

    public PutChunk() {
      clear();
    }

    int getChunkIndex() {
      return chunkIndex;
    }

    ChunkState getState() {
      return state;
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

    boolean isFull() {
      return buf.position() == buf.capacity();
    }

    boolean isComplete() {
      return state == ChunkState.Failed || state == ChunkState.Succeeded;
    }

    BlobId getChunkBlobId() {
      return chunkBlobId;
    }

    void clear() {
      // @todo: what else needs to be done to clear the state?
      state = ChunkState.Free;
      correlationIdToChunkPutRequest = new ConcurrentSkipListMap<Integer, ChunkPutRequest>();
    }

    void prepareForBuilding(int chunkIndex, int size) {
      this.chunkIndex = chunkIndex;
      // @todo use the Buffer pool later.
      if (buf == null) {
        buf = ByteBuffer.allocate(size);
      } else {
        buf.clear();
        //@todo size is going to be the same.
      }
      this.size = size;
      state = ChunkState.Building;
    }

    private void prepareForSending() {
      buf.flip();
      try {
        partitionId = getPartitionForPut();
        chunkBlobId = new BlobId(partitionId);
        operationTracker = new SimpleOperationTracker(putManager.routerConfig.routerDatacenterName, partitionId, false,
            putManager.routerConfig.routerPutSuccessTarget, putManager.routerConfig.routerPutRequestParallelism);
        correlationIdToChunkPutRequest.clear();
        state = ChunkState.Ready;
      } catch (RouterException e) {
        operationException = e;
      }
    }

    int fillFrom(ByteBuffer channelReadBuffer) {
      int toWrite = Math.min(channelReadBuffer.remaining(), buf.remaining());
      if (channelReadBuffer.remaining() > buf.remaining()) {
        // Manipulate limit of the source buffer in order to read only enough to fill the chunk
        int savedLimit = channelReadBuffer.limit();
        channelReadBuffer.limit(channelReadBuffer.position() + buf.remaining());
        buf.put(channelReadBuffer);
        channelReadBuffer.limit(savedLimit);
      } else {
        buf.put(channelReadBuffer);
      }
      return toWrite;
    }

    /**
     * Fetch put requests to send.
     * @param requests
     * @throws RouterException
     */
    void fetchRequests(List<RequestInfo> requests)
        throws RouterException {
      //First, check if any of the existing requests have timed out.
      Iterator<Map.Entry<Integer, ChunkPutRequest>> inFlightRequestsIterator =
          correlationIdToChunkPutRequest.entrySet().iterator();
      while (inFlightRequestsIterator.hasNext()) {
        Map.Entry<Integer, ChunkPutRequest> entry = inFlightRequestsIterator.next();
        if (time.milliseconds() - entry.getValue().startTimeMs > putManager.routerConfig.routerRequestTimeoutMs) {
          operationTracker.onResponse(entry.getValue().replicaId, false);
          inFlightRequestsIterator.remove();
        } else {
          // the entries are ordered by operation id and time. Break on the first request that has not timed out.
          break;
        }
      }

      // Now, check if this chunk is done.
      if (operationTracker.isDone()) {
        if (!operationTracker.hasSucceeded()) {
          failedAttempts++;
          if (failedAttempts <= MAX_SLIP_PUTS) {
            prepareForSending();
          } else {
            // this chunk could not be successfully put. The whole operation has to fail.
            chunkBlobId = null;
            resolvePutChunk(this);
            clear();
            return;
          }
        } else {
          resolvePutChunk(this);
          // Clear the chunk to continue filling in data.
          clear();
          return;
        }
      }

      // @todo remove
      int nnn = getNumChunks();
      Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
      while (replicaIterator.hasNext()) {
        ReplicaId replicaId = replicaIterator.next();
        String hostname = replicaId.getDataNodeId().getHostname();
        // @todo: getPortToConnectTo() in the ClusterMap interface should simply return the correct port without
        // @todo: taking any parameters. Rather than make the change to plug in sslEnabledDataCenters into
        // @todo: the NonBlockingRouter only to change later, we will hard code it here for now.
        Port port = new Port(replicaId.getDataNodeId().getPort(), PortType.PLAINTEXT);
        // @todo: check if the buf is used right.
        PutRequest putRequest = createPutRequest();
        RequestInfo request = new RequestInfo(hostname, port, putRequest);
        int correlationId = putRequest.getCorrelationId();
        correlationIdToChunkPutRequest
            .put(correlationId, new ChunkPutRequest(correlationId, replicaId, time.milliseconds()));
        putManager.associateRequestWithChunk(correlationId, this);
        requests.add(request);
        replicaIterator.remove();
      }
    }

    protected PutRequest createPutRequest() {
      return new PutRequest(correlationIdGenerator.incrementAndGet(), putManager.routerConfig.routerHostname,
          chunkBlobId, blobProperties, ByteBuffer.wrap(userMetadata), new ByteBufferInputStream(buf.duplicate()), size,
          BlobType.DataBlob);
    }

    private PartitionId getPartitionForPut()
        throws RouterException {
      List<PartitionId> partitions = putManager.clusterMap.getWritablePartitionIds();
      if (partitions.isEmpty()) {
        throw new RouterException("No writable partitions available.", RouterErrorCode.AmbryUnavailable);
      }
      return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
    }

    /**
     * This method is the entry point for handling responses received for requests sent out on behalf of this chunk.
     * @todo
     * @param responseInfo the response received for a request sent out on behalf of this chunk.
     */
    void handleResponse(ResponseInfo responseInfo) {
      int correlationId = ((RequestOrResponse) responseInfo.getRequest()).getCorrelationId();
      ChunkPutRequest request = correlationIdToChunkPutRequest.remove(correlationId);
      if (request == null) {
        return;
        // this means the request was timed out and removed from the map. Ignore right away.
      }

      boolean isSuccessful;
      if (responseInfo.getError() != null) {
        // Currently, do not differentiate between different errors.
        isSuccessful = false;
      } else {
        try {
          // @todo: PutResponse takes an InputStream, which could be changed to take a ByteBuffer.
          PutResponse putResponse =
              PutResponse.readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())));
          if (putResponse.getCorrelationId() != correlationId) {
            // The NetworkClient associates a response with a request based on the fact that only one rquest is sent
            // out over a connection id, and the response received on a connection id must be for the latest request
            // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
            // There is no other way to handle it.
            // @todo: metric
            isSuccessful = false;
            logger.error("The correlation id in the PutResponse " + putResponse.getCorrelationId()
                + " is not the same as the correlation id in the associated PutRequest: " + correlationId);
          } else {
            switch (putResponse.getError()) {
              case No_Error:
                logger.trace("The putRequest was successful");
                isSuccessful = true;
                break;
              default:
                // for puts, all errors are effectively Ambry_Unavailable. One could argue that certain errors
                // are retryable and are eligible for slip puts, but what could those be? Partition_ReadOnly or
                // BlobId_Already_Exists both are outlier cases (should not really happen) that we should not even
                // handle them.
                // @todo: However, for metrics, we will need to distinguish them here.
                isSuccessful = false;
                logger.trace("Server returned an error: ", putResponse.getError());
            }
          }
        } catch (IOException e) {
          // @todo: Even this should really not happen. But we need a metric.
          isSuccessful = false;
        }
      }
      operationTracker.onResponse(request.replicaId, isSuccessful);
    }
  }

  //@todo: No metadata chunk for < 4MB requests. Make sure things work.
  class MetadataPutChunk extends PutChunk {
    StoreKey[] chunkIds;
    int chunksDone;

    MetadataPutChunk() {
      chunkIds = new BlobId[numChunks];
      chunksDone = 0;
    }

    void addChunkId(BlobId chunkBlobId, int chunkIndex) {
      chunkIds[chunkIndex] = chunkBlobId;
      chunksDone++;
    }

    @Override
    boolean isReady() {
      return chunksDone == numChunks;
    }

    /*
    @Override
    protected PutRequest createPutRequest() {
      if (!isReady()) { //@todo: remove before commit
        throw new IllegalStateException("Metadata Chunk is not yet ready");
      }
      ByteBuffer buf = MetadataContentSerDe.serializeMetadataContent(Arrays.asList(chunkIds));
      return new PutRequest(correlationIdGenerator.incrementAndGet(), putManager.routerConfig.routerHostname,
          chunkBlobId, blobProperties, ByteBuffer.wrap(userMetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(buf.array())), size, BlobType.DataBlob);
    }
    //override other methods and make them illegal.
    */
  }
}