package com.github.ambry.router;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.network.ConnectionManager;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.utils.ByteBufferChannel;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GetChunk
 *
 */
class GetChunk {
  long chunkId; //@todo long vs int in all fields.
  ChunkState state;
  ByteBuffer buf;
  ByteBufferChannel chunkChannel;
  int requestParallelism;
  int successTarget;
  int successCount;
  int failureCount;
  int requestsInProgress;

  public GetChunk() {
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

  void fetchGetRequests(List<NetworkSend> requests) {
    // @todo:
    // This will internally handle timeouts and quorums,
    // and return list of requests accordingly. When requests are
    // created, the getChunk will internally checkout connections from the
    // connection manager, if there are any. If no connection is available, then
    // the connection manager will return null (after initiating new connections
    // if required). Only if a connection is successfully checked out will a
    // request be created and returned by the getChunk.
  }
}

public class GetManager {
  private static final Logger logger = LoggerFactory.getLogger(GetManager.class);
  private ConcurrentSkipListMap<Long, GetOperation> getOperations;
  private ConnectionManager connectionManager;

  public GetManager(ConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  public FutureResult<ReadableStreamChannel> submitGetBlobOperation(long operationId, String blobId,
      Callback<ReadableStreamChannel> callback) {
    FutureResult<ReadableStreamChannel> futureResult = new FutureResult<ReadableStreamChannel>();
    GetBlobOperation getOperation =
        new GetBlobOperation(operationId, blobId, futureResult, callback);
    getOperations.put(operationId, getOperation);
    return futureResult;
  }

  public FutureResult<BlobInfo> submitGetBlobInfoOperation(long operationId, String blobId,
      Callback<BlobInfo> callback) {
    FutureResult<BlobInfo> futureResult = new FutureResult<BlobInfo>();
    GetBlobInfoOperation getBlobInfoOperation =
        new GetBlobInfoOperation(operationId, blobId, futureResult, callback);
    getOperations.put(operationId, getBlobInfoOperation);
    return futureResult;
  }

  public void poll(List<NetworkSend> requests) {
    Iterator<GetOperation> iter = getOperations.values().iterator();
    while (iter.hasNext()) {
      GetOperation op = iter.next();
      op.fetchGetRequests(requests);
    }
  }

  class GetOperation {
    long operationId;
    OperationType operationType;
    String blobId;
    GetChunk[] getChunks;

    GetOperation(long operationId, String blobId, OperationType operationType) {
      if (operationType != OperationType.GetBlob && operationType != OperationType.GetBlobInfo) {
        throw new IllegalArgumentException("Invalid get operation type: " + operationType);
      }
      this.operationId = operationId;
      this.operationType = operationType;
      this.blobId = blobId;
    }

    /**
     * For this operation, populate a list of get requests (in the form of NetworkSends) to send out.
     * @param requests list to populate with the requests.
     */
    void fetchGetRequests(List<NetworkSend> requests) {
      for (GetChunk chunk : getChunks) {
        if (chunk.isReady()) {
          chunk.fetchGetRequests(requests);
        }
      }
    }
  }

  class GetBlobOperation extends GetOperation {
    FutureResult<ReadableStreamChannel> futureResult;
    Callback<ReadableStreamChannel> callback;
    GetBlobOperation(long operationId, String blobId, FutureResult<ReadableStreamChannel> futureResult,
        Callback<ReadableStreamChannel> callback) {
      super(operationId, blobId, OperationType.GetBlob);
      this.futureResult = futureResult;
      this.callback = callback;
    }
  }

  class GetBlobInfoOperation extends GetOperation {
    FutureResult<BlobInfo> futureResult;
    Callback<BlobInfo> callback;
    GetBlobInfoOperation(long operationId, String blobId, FutureResult<BlobInfo> futureResult,
        Callback<BlobInfo> callback) {
      super(operationId, blobId, OperationType.GetBlobInfo);
      this.futureResult = futureResult;
      this.callback = callback;
    }
  }
}

