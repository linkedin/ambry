package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MetadataContentSerDe;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PutOperation class is responsible for maintaining the state associated with a put operation, and completing it.
 */
class PutOperation {
  // Operation arguments.
  private final RouterConfig routerConfig;
  private final ClusterMap clusterMap;
  private final ResponseHandler responseHandler;
  private final BlobProperties blobProperties;
  private final byte[] userMetadata;
  private final ByteBufferAsyncWritableChannel chunkFillerChannel;
  private final FutureResult<String> futureResult;
  private final Callback<String> callback;
  private final Time time;

  // Parameters associated with the state.

  // the list of PutChunks that will be used to hold chunks that are sent out. A PutChunk will only hold one chunk at
  // any time, but will be reused as and when the operation on the chunk is complete.
  protected final PutChunk[] putChunks;
  // total number of data chunks this operation will result in.
  private final int numDataChunks;
  // the total size of the object (the overall blob). This is the same as the size in blobProperties,
  // and the size to be read from the channel.
  private final long blobSize;
  // total bytes of this object that has been filled so far by the ChunkFillerThread.
  private long bytesFilledSoFar;
  // the reference to the chunk in putChunks that was most recently filled or became eligible for getting filled.
  private PutChunk chunkToFill;
  // counter for tracking the chunks being filled.
  private int chunkCounter;
  // the current ByteBuffer/position in the chunkFillerChannel.
  private ByteBuffer channelReadBuffer;
  // denotes whether chunk filling is complete.
  private boolean chunkFillingCompleted = false;
  // the metadata chunk for this operation. If this operation results in only one chunk,
  // then there will be no metadata chunk and this will be null.
  private final MetadataPutChunk metadataPutChunk;
  // denotes whether the operation is complete.
  private boolean operationCompleted = false;
  // the blob id of the overall blob. This will be set if and when the operation is successful.
  private BlobId blobId;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // To find the PutChunk to hand over the response quickly.
  private final Map<Integer, PutChunk> correlationIdToPutChunk = new HashMap<Integer, PutChunk>();

  private static final int MAX_IN_MEM_CHUNKS = 4;
  private static final Logger logger = LoggerFactory.getLogger(PutOperation.class);

  /**
   * Construct a PutOperation with the given parameters. For any operation, based on the max chunk size for puts,
   * an object contained within the {@link ReadableStreamChannel} will either be put as a single blob if its size is
   * less than the max chunk size; or will be split into as many chunks as required each of which is no longer in
   * size than the max chunk put size, and a single metadata blob containing the information about each of these
   * chunks.
   * @param routerConfig the {@link RouterConfig} containing the configs for put operations.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param operationId the operation id of the put operation.
   * @param blobProperties the BlobProperties associated with the put operation.
   * @param userMetadata the userMetadata associated with the put operation.
   * @param channel the {@link ReadableStreamChannel} containing the blob data.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param time the Time instance to use.
   * @throws {@link RouterException} if there is an error in constructing the PutOperation with the given parameters.
   */
  PutOperation(RouterConfig routerConfig, ClusterMap clusterMap, ResponseHandler responseHandler, long operationId,
      BlobProperties blobProperties, byte[] userMetadata, ReadableStreamChannel channel,
      FutureResult<String> futureResult, Callback<String> callback, Time time)
      throws RouterException {
    blobSize = blobProperties.getBlobSize();
    if (channel.getSize() != blobSize) {
      throw new RouterException(
          "Channel size: " + channel.getSize() + " different from size in BlobProperties: " + blobProperties
              .getBlobSize(), RouterErrorCode.BadInputChannel);
    }
    // Set numDataChunks
    // the max blob size that can be supported is technically limited by the max chunk size configured.
    long numDataChunksL = blobSize == 0 ? 1 : (blobSize - 1) / routerConfig.routerMaxPutChunkSizeBytes + 1;
    if (numDataChunksL > Integer.MAX_VALUE) {
      throw new RouterException("Cannot support a blob size of " + blobSize + " with a chunk size of " +
          routerConfig.routerMaxPutChunkSizeBytes, RouterErrorCode.BlobTooLarge);
    }
    numDataChunks = (int) numDataChunksL;
    this.routerConfig = routerConfig;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.futureResult = futureResult;
    this.callback = callback;
    this.time = time;
    bytesFilledSoFar = 0;
    chunkCounter = -1;

    // Initialize chunks
    putChunks = new PutChunk[Math.min(numDataChunks, MAX_IN_MEM_CHUNKS)];
    for (int i = 0; i < putChunks.length; i++) {
      putChunks[i] = new PutChunk();
    }
    metadataPutChunk = numDataChunks > 1 ? new MetadataPutChunk() : null;

    chunkFillerChannel = new ByteBufferAsyncWritableChannel();
    channel.readInto(chunkFillerChannel, new Callback<Long>() {
      @Override
      public void onCompletion(Long result, Exception exception) {
        if (exception != null) {
          setOperationException(exception);
        } else if (result != blobSize) {
          setOperationException(new RouterException("Incorrect number of bytes: " + result +
              " read in from the channel, expected: " + blobSize, RouterErrorCode.BadInputChannel));
        }
      }
    });
  }

  /**
   * returns whether the operation has completed.
   * @return whether the operation has completed.
   */
  boolean isOperationComplete() {
    return operationCompleted;
  }

  /**
   * For this operation, create and populate put requests for chunks (in the form of {@link RequestInfo}) to
   * send out.
   * @param requestRegistrationCallback the {@link PutRequestRegistrationCallback} to call for every request that gets created
   *                                    as part of this poll operation.
   */
  void poll(PutRequestRegistrationCallback requestRegistrationCallback) {
    if (operationCompleted) {
      return;
    }
    if (metadataPutChunk != null && metadataPutChunk.isReady()) {
      metadataPutChunk.poll(requestRegistrationCallback);
      if (metadataPutChunk.isComplete()) {
        onChunkOperationComplete(metadataPutChunk);
        if (operationCompleted) {
          return;
        }
      }
    } else {
      for (PutChunk chunk : putChunks) {
        if (chunk.isReady()) {
          chunk.poll(requestRegistrationCallback);
          if (chunk.isComplete()) {
            onChunkOperationComplete(chunk);
            // After each chunk is processed, check whether the operation itself has completed
            if (operationCompleted) {
              return;
            }
          }
        }
      }
    }
  }

  /**
   * Handle the given {@link ResponseInfo} by handing it over to the correct {@link PutChunk} that issued the request.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   */
  void handleResponse(ResponseInfo responseInfo) {
    PutChunk putChunk =
        correlationIdToPutChunk.remove(((RequestOrResponse) responseInfo.getRequest()).getCorrelationId());
    putChunk.handleResponse(responseInfo);
    if (putChunk.isComplete()) {
      onChunkOperationComplete(putChunk);
    }
  }

  /**
   * Called when the operation on a {@link PutChunk} is complete: That is, the chunk is successfully put or there was
   * an irrecoverable error in doing so. The {@link PutChunk} guarantees that in the former case,
   * the blobId of the chunk is set; and in the latter case, it is null.
   * @param chunk the {@link PutChunk} that has completed its operation.
   */
  void onChunkOperationComplete(PutChunk chunk) {
    if (chunk.getChunkBlobId() == null) {
      // the overall operation has failed if any of the chunk fails.
      // @todo: metric.
      logger.error("Failed putting chunk at index: " + chunk.getChunkIndex() + ", failing the entire operation");
      operationCompleted = true;
    } else if (numDataChunks == 1 || chunk == metadataPutChunk) {
      blobId = chunk.getChunkBlobId();
      // the overall operation has succeeded.
      if (chunk.failedAttempts > 0) {
        // @todo: metric.
        logger.info("Slipped put succeeded for blob: " + chunk.getChunkBlobId());
      } else {
        logger.trace("Successfully put blob: " + chunk.getChunkBlobId());
      }
      operationCompleted = true;
    } else {
      // a data chunk has succeeded. More to come.
      logger.trace("Successfully put chunk with blob id: " + chunk.getChunkBlobId());
      metadataPutChunk.addChunkId(chunk.chunkBlobId, chunk.chunkIndex);
    }
    chunk.clear();
  }

  /**
   * Returns whether chunk filling is complete.
   * @return true if chunk filling is complete, false otherwise.
   */
  boolean isChunkFillComplete() {
    return chunkFillingCompleted || operationCompleted;
  }

  /**
   * This method runs in the context of the ChunkFiller thread. As long as there are chunks available to
   * be written to, it gets the chunk that is to be filled and keeps filling it with the data from the
   * chunkFillerChannel, if there is any.
   */
  void fillChunks() {
    PutChunk chunkToFill;
    if (!chunkFillingCompleted && !operationCompleted) {
      do {
        // Attempt to fill a chunk
        if (channelReadBuffer == null) {
          try {
            channelReadBuffer = chunkFillerChannel.getNextChunk(0);
          } catch (InterruptedException e) {
            throw new IllegalStateException(
                "Received InterruptedException from a call that is expected to be non-blocking.");
          }
        }
        if (channelReadBuffer != null) {
          chunkToFill = getChunkToFill();
          if (chunkToFill == null) {
            // no chunks are free to be filled yet.
            break;
          }
          bytesFilledSoFar += chunkToFill.fillFrom(channelReadBuffer);
          if (!channelReadBuffer.hasRemaining()) {
            chunkFillerChannel.resolveOldestChunk(null);
            channelReadBuffer = null;
          }
        } else {
          // channel does not have more data yet.
          break;
        }
      } while (bytesFilledSoFar < blobSize);
      if (bytesFilledSoFar == blobSize) {
        chunkFillingCompleted = true;
      }
    }
  }

  /**
   * Get the chunk to be filled. At most one chunk for an operation will ever be in Building state. If there is such
   * a chunk, that is returned. If not, if there is a Free chunk, that is returned. If no Free chunks are available
   * either, then null is returned.
   * @return the chunk to fill, or null if there are no chunks eligible for filling.
   */
  private PutChunk getChunkToFill() {
    if (chunkToFill != null && chunkToFill.isBuilding()) {
      return chunkToFill;
    } else {
      for (PutChunk chunk : putChunks) {
        if (chunk.isFree()) {
          chunkCounter++;
          chunk.prepareForBuilding(chunkCounter, getSizeOfChunkAt(chunkCounter));
          chunkToFill = chunk;
          return chunk;
        }
      }
    }
    // No free or building chunks.
    return null;
  }

  /**
   * Get the chunk size of the chunk at the given position.
   * @param pos the position of the chunk in the overall blob.
   * @return the size of the chunk.
   */
  private int getSizeOfChunkAt(int pos) {
    return pos == numDataChunks - 1 ? (int) (blobSize - 1) % routerConfig.routerMaxPutChunkSizeBytes + 1
        : routerConfig.routerMaxPutChunkSizeBytes;
  }

  /**
   * Return the number of data chunks that this operation will result in.
   * @return the number of data chunks that this operation will result in.
   */
  int getNumDataChunks() {
    return numDataChunks;
  }

  /**
   * Return the blobId string associated with this operation if it was successful, or null if it failed. This should
   * (obviously) only be called once the operation is complete.
   * @return the blobId if the operation is successful; null otherwise.
   */
  String getBlobIdString() {
    return blobId == null ? null : blobId.getID();
  }

  /**
   * Return the {@link BlobProperties} associated with this operation.
   * @return the {@link BlobProperties} associated with this operation.
   */
  BlobProperties getBlobProperties() {
    return blobProperties;
  }

  /**
   * Return the userMetadata associated with this operation.
   * @return the userMetadata associated with this operation.
   */
  byte[] getUserMetadata() {
    return userMetadata;
  }

  /**
   * Return the {@link Callback} associated with this operation.
   * @return the {@link Callback} associated with this operation.
   */
  Callback<String> getCallback() {
    return callback;
  }

  /**
   * Return the {@link FutureResult} associated with this operation.
   * @return the {@link FutureResult} associated with this operation.
   */
  FutureResult<String> getFuture() {
    return futureResult;
  }

  /**
   * The exception associated with this operation if it failed; null otherwise.
   * @return exception associated with this operation if it failed; null otherwise.
   */
  Exception getOperationException() {
    return operationException.get();
  }

  /**
   * Set the irrecoverable exception associated with this operation. When this is called, the operation has failed.
   * @param exception the irrecoverable exception associated with this operation.
   */
  void setOperationException(Exception exception) {
    operationException.set(exception);
    operationCompleted = true;
  }

  /**
   * PutChunk is responsible for storing chunks to be put, managing their state and completing the operation on the
   * chunks. A PutChunk object is not really associated with one single chunk of data. Instead, it acts a holder that
   * handles a chunk of data and takes it to completion, and once done, moves on to handle more chunks of data. This
   * why there is a reference to the "current chunk" in the comments.
   */
  class PutChunk {
    // the position of the current chunk in the overall blob.
    private int chunkIndex;
    // the blobId of the current chunk.
    protected BlobId chunkBlobId;
    // The exception encountered while putting the current chunk. Not all errors are irrecoverable. An error may or
    // may not get overridden by a subsequent error, and this variable is meant to store the most relevant error.
    private RouterException chunkException;
    // the state of the current chunk.
    protected ChunkState state;
    // the ByteBuffer that has the data for the current chunk.
    protected ByteBuffer buf;
    // the OperationTracker used to track the status of requests for the current chunk.
    protected OperationTracker operationTracker;
    // the number of times a put was attempted for the current chunk.
    private int failedAttempts;
    // the partitionId chosen for the current chunk.
    private PartitionId partitionId;
    // the list of partitions already attempted for this chunk.
    private List<PartitionId> attemptedPartitionIds = new ArrayList<PartitionId>();
    // map of correlation id to the request metadata for every request issued for the current chunk.
    private final Map<Integer, ChunkPutRequest> correlationIdToChunkPutRequest =
        new TreeMap<Integer, ChunkPutRequest>();

    // this is the maximum number of times we will retry a put with a different partitions,
    // before failing the chunk put operation (which will also fail the entire operation).
    // @todo: should this be a config?
    private static final int MAX_SLIPPED_PUTS = 1;
    private final Logger logger = LoggerFactory.getLogger(PutChunk.class);

    /**
     * Construct a PutChunk
     */
    public PutChunk() {
      clear();
    }

    /**
     * Clear the state to make way for a new data chunk.
     */
    void clear() {
      chunkIndex = -1;
      chunkBlobId = null;
      chunkException = null;
      state = ChunkState.Free;
      failedAttempts = 0;
      partitionId = null;
      correlationIdToChunkPutRequest.clear();
      attemptedPartitionIds.clear();
    }

    /**
     * @return the position of the current chunk in the overall blob.
     */
    int getChunkIndex() {
      return chunkIndex;
    }

    /**
     * @return the state of the PutChunk.
     */
    ChunkState getState() {
      return state;
    }

    /**
     * @return the {@link BlobId} of the current chunk - valid only after the chunk is successfully put.
     */
    BlobId getChunkBlobId() {
      return chunkBlobId;
    }

    /**
     * @return true if this PutChunk is free so a chunk of the overall blob can be filled in.
     */
    boolean isFree() {
      return state == ChunkState.Free;
    }

    /**
     * @return true if this PutChunk is being built/filled with a chunk of the overall blob.
     */
    boolean isBuilding() {
      return state == ChunkState.Building;
    }

    /**
     * @return true if this PutChunk is ready with a chunk of the overall blob.
     */
    boolean isReady() {
      return state == ChunkState.Ready;
    }

    /**
     * @return true if the operation on the current chunk is complete.
     */
    boolean isComplete() {
      return state == ChunkState.Complete;
    }

    /**
     * Prepare this chunk for building, that is, for being filled with data from the channel.
     * @param chunkIndex the position in the overall blob that this chunk is going to  be in.
     * @param size size to allocate memory for the buffer that will hold the data for this chunk.
     */
    private void prepareForBuilding(int chunkIndex, int size) {
      this.chunkIndex = chunkIndex;
      // @todo use the Buffer pool eventually.
      // The current implementation does not allow for reuse of the same buffer, and uses locks. This needs discussion.
      if (buf == null) {
        buf = ByteBuffer.allocate(size);
      } else {
        buf.clear();
        buf.limit(size);
      }
      state = ChunkState.Building;
    }

    /**
     * Prepare this chunk (that is completely built), for puts.
     */
    private void prepareForSending() {
      try {
        // if this is part of a retry, make sure no previously attempted partitions are retried.
        if (partitionId != null) {
          attemptedPartitionIds.add(partitionId);
        }
        partitionId = getPartitionForPut(attemptedPartitionIds);
        chunkBlobId = new BlobId(partitionId);
        operationTracker = new SimpleOperationTracker(routerConfig.routerDatacenterName, partitionId, false,
            routerConfig.routerPutSuccessTarget, routerConfig.routerPutRequestParallelism);
        correlationIdToChunkPutRequest.clear();
        state = ChunkState.Ready;
      } catch (RouterException e) {
        setOperationException(e);
      }
    }

    /**
     * Do the actions required when the chunk has been completely built.
     */
    void onFillComplete() {
      buf.flip();
      prepareForSending();
    }

    /**
     * Fill the buffer of the current chunk with the data from the given {@link ByteBuffer}.
     * @param channelReadBuffer the {@link ByteBuffer} from which to read data.
     * @return the number of bytes transferred in this operation.
     */
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
      if (!buf.hasRemaining()) {
        onFillComplete();
      }
      return toWrite;
    }

    /**
     * Check if the operation on the chunk is eligible for completion, if so complete it.
     */
    void checkAndMaybeComplete() {
      boolean done = false;
      // Now, check if this chunk is done.
      if (operationTracker.isDone()) {
        if (!operationTracker.hasSucceeded()) {
          failedAttempts++;
          if (failedAttempts <= MAX_SLIPPED_PUTS) {
            logger.trace("Attempt to put chunk with id: " + chunkBlobId + " failed, attempting slipped put");
            // @todo: add metric.
            prepareForSending();
          } else {
            // this chunk could not be successfully put. The whole operation has to fail.
            chunkBlobId = null;
            setOperationException(chunkException);
            done = true;
          }
        } else {
          chunkException = null;
          done = true;
        }
      }
      if (done) {
        state = ChunkState.Complete;
      }
    }

    /**
     * Fetch put requests to send for the current data chunk.
     * This is one of two main entry points to this class, the other being {@link #handleResponse(ResponseInfo)}.
     * Apart from fetching requests to send out, this also checks for timeouts of issued requests,
     * status of the operation and anything else that needs to be done within this PutChunk. The callers guarantee
     * that this method is called on all the PutChunks of an operation until either the operation,
     * or the chunk operation is completed.
     * @param requestFillCallback the {@link PutRequestRegistrationCallback} to call for every request that gets created as
     *                            part of this poll operation.
     */
    void poll(PutRequestRegistrationCallback requestFillCallback) {
      //First, check if any of the existing requests have timed out.
      Iterator<Map.Entry<Integer, ChunkPutRequest>> inFlightRequestsIterator =
          correlationIdToChunkPutRequest.entrySet().iterator();
      while (inFlightRequestsIterator.hasNext()) {
        Map.Entry<Integer, ChunkPutRequest> entry = inFlightRequestsIterator.next();
        if (time.milliseconds() - entry.getValue().startTimeMs > routerConfig.routerRequestTimeoutMs) {
          operationTracker.onResponse(entry.getValue().replicaId, false);
          chunkException = new RouterException("Timed out waiting for responses", RouterErrorCode.OperationTimedOut);
          inFlightRequestsIterator.remove();
        } else {
          // the entries are ordered by correlation id and time. Break on the first request that has not timed out.
          break;
        }
      }

      checkAndMaybeComplete();
      if (isComplete()) {
        return;
      }

      Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
      while (replicaIterator.hasNext()) {
        ReplicaId replicaId = replicaIterator.next();
        String hostname = replicaId.getDataNodeId().getHostname();
        // @todo: getPortToConnectTo() in the ClusterMap interface should simply return the correct port without
        // taking any parameters. Rather than make the change to plug in sslEnabledDataCenters into
        // the NonBlockingRouter only to change later, we will hard code it here for now.
        Port port = new Port(replicaId.getDataNodeId().getPort(), PortType.PLAINTEXT);
        PutRequest putRequest = createPutRequest();
        RequestInfo request = new RequestInfo(hostname, port, putRequest);
        int correlationId = putRequest.getCorrelationId();
        correlationIdToChunkPutRequest.put(correlationId, new ChunkPutRequest(replicaId, time.milliseconds()));
        correlationIdToPutChunk.put(correlationId, this);
        requestFillCallback.registerRequestToSend(PutOperation.this, request);
        replicaIterator.remove();
      }
    }

    /**
     * Create and return the {@link PutRequest} associated with the current chunk. This method should only be called
     * when the chunk is in ready state.
     * @return the crated {@link PutRequest}.
     */
    protected PutRequest createPutRequest() {
      return new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
          chunkBlobId, blobProperties, ByteBuffer.wrap(userMetadata), new ByteBufferInputStream(buf.duplicate()),
          buf.remaining(), BlobType.DataBlob);
    }

    /**
     * Choose a random {@link PartitionId} for putting the current chunk and return it.
     * @param partitionIdsToExclude the list of {@link PartitionId}s that should be excluded from consideration.
     * @return the chosen {@link PartitionId}
     * @throws RouterException
     */
    protected PartitionId getPartitionForPut(List<PartitionId> partitionIdsToExclude)
        throws RouterException {
      // getWritablePartitions creates and returns a new list, so it is safe to manipulate it.
      List<PartitionId> partitions = clusterMap.getWritablePartitionIds();
      partitions.removeAll(partitionIdsToExclude);
      if (partitions.isEmpty()) {
        throw new RouterException("No writable partitions available.", RouterErrorCode.AmbryUnavailable);
      }
      return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
    }

    /**
     * This method is the entry point for handling responses received for requests sent out on behalf of this chunk.
     * For puts, processing involves determining whether the request was successful, notifying the operation
     * tracker so it can track the status of the operation, and notifying the response handler for failure detection.
     * Finally, a check is done to determine whether the operation on the chunk is eligible for completion,
     * if so the operation is completed right away.
     * @param responseInfo the response received for a request sent out on behalf of this chunk.
     */
    void handleResponse(ResponseInfo responseInfo) {
      int correlationId = ((PutRequest) responseInfo.getRequest()).getCorrelationId();
      ChunkPutRequest request = correlationIdToChunkPutRequest.remove(correlationId);
      if (request == null) {
        // Ignore right away. This could mean:
        // - the response is valid for this chunk, but was timed out and removed from the map.
        // - the response is for an earlier attempt of this chunk (slipped put scenario). And the map was cleared
        // before attempting the slipped put.
        // - the response is for an earlier chunk held by this PutChunk.
        return;
      }
      boolean isSuccessful;
      if (responseInfo.getError() != null) {
        setChunkException(new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut));
        responseHandler.onRequestResponseException(request.replicaId, new IOException("NetworkClient error"));
        isSuccessful = false;
      } else {
        try {
          // @todo: PutResponse takes an InputStream, which could be changed to take a ByteBuffer to avoid new object
          // creations.
          PutResponse putResponse =
              PutResponse.readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())));
          if (putResponse.getCorrelationId() != correlationId) {
            // The NetworkClient associates a response with a request based on the fact that only one request is sent
            // out over a connection id, and the response received on a connection id must be for the latest request
            // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
            // There is no other way to handle it.
            // @todo: metric
            logger.error("The correlation id in the PutResponse " + putResponse.getCorrelationId()
                + " is not the same as the correlation id in the associated PutRequest: " + correlationId);
            setChunkException(
                new RouterException("Unexpected internal error", RouterErrorCode.UnexpectedInternalError));
            isSuccessful = false;
            // we do not notify the ResponseHandler responsible for failure detection as this is an unexpected error.
          } else {
            ServerErrorCode putError = putResponse.getError();
            responseHandler.onRequestResponseError(request.replicaId, putError);
            if (putError == ServerErrorCode.No_Error) {
              logger.trace("The putRequest was successful");
              isSuccessful = true;
            } else {
              processServerError(putResponse.getError());
              isSuccessful = false;
              logger.trace("Server returned an error: ", putResponse.getError());
            }
          }
        } catch (IOException e) {
          // @todo: metric.
          // This should really not happen. Again, we do not notify the ResponseHandler responsible for failure
          // detection.
          setChunkException(new RouterException("Response deserialization received an unexpected error", e,
              RouterErrorCode.UnexpectedInternalError));
          isSuccessful = false;
        }
      }
      operationTracker.onResponse(request.replicaId, isSuccessful);
      checkAndMaybeComplete();
    }

    /**
     * Possibly set the exception for this chunk using the given exception. Calling this method with an exception does
     * not necessarily result in that being set as the chunkException. The idea is to set the most relevant exception
     * in case of errors.
     * @param exception the exception that may be set as the chunkException.
     */
    private void setChunkException(RouterException exception) {
      // @todo: set the most relevant error. An exception being set does not necessarily mean that the operation on
      // the chunk will fail, so this method could get called multiple times and the operation could still succeed.
      // If the operation fails, then the most relevant exception this method was called with must be the exception
      // returned to the caller.
      chunkException = exception;
    }

    /**
     * Process an error received from the server. The idea is to convert from the ServerErrorCode to a RouterErrorCode.
     * @param error the ServerErrorCode received from a response to a request.
     */
    private void processServerError(ServerErrorCode error) {
      // for puts, all errors are effectively Ambry_Unavailable. One could argue that certain errors
      // are retryable and are eligible for slipped puts, but what could those be? Partition_ReadOnly and
      // BlobId_Already_Exists are outliers (should not really happen) that those should really
      // result in Ambry_Unavailable or UnexpectedInternalError.
      // However, for metrics, we will need to distinguish them here.
      setChunkException(new RouterException("Could not complete operation, server returned: " + error,
          RouterErrorCode.AmbryUnavailable));
    }

    /**
     * A class that holds information about requests sent out by this PutChunk.
     */
    private class ChunkPutRequest {
      private ReplicaId replicaId;
      private long startTimeMs;

      /**
       * Construct a ChunkPutRequest
       * @param replicaId the replica to which this request is being sent.
       * @param startTimeMs the time at which this request was created.
       */
      ChunkPutRequest(ReplicaId replicaId, long startTimeMs) {
        this.replicaId = replicaId;
        this.startTimeMs = startTimeMs;
      }
    }
  }

  /**
   * MetadataPutChunk responsible for maintaining the state of the metadata chunk and completing the chunk operation
   * on it.
   */
  class MetadataPutChunk extends PutChunk {
    StoreKey[] chunkIds;
    int chunksDone;

    /**
     * Initialize the MetadataPutChunk.
     */
    MetadataPutChunk() {
      chunkIds = new BlobId[numDataChunks];
      chunksDone = 0;
      // metadata blob is in building state.
      state = ChunkState.Building;
    }

    /**
     * Add the given blobId of a successfully put data chunk to the metadata at its position in the overall blob.
     * @param chunkBlobId the blobId of the associated data chunk
     * @param chunkIndex the position of the associated data chunk in the overall blob.
     */
    void addChunkId(BlobId chunkBlobId, int chunkIndex) {
      chunkIds[chunkIndex] = chunkBlobId;
      chunksDone++;
      if (chunksDone == numDataChunks) {
        buf = MetadataContentSerDe.serializeMetadataContent(Arrays.asList(chunkIds));
        onFillComplete();
      }
    }

    /**
     * {@inheritDoc}
     *
     * In constructing the put request for the metadata blob, MetadataPutChunk serializes the metadata.
     * @return the created {@link PutRequest}.
     */
    @Override
    protected PutRequest createPutRequest() {
      return new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
          chunkBlobId, blobProperties, ByteBuffer.wrap(userMetadata), new ByteBufferInputStream(buf.duplicate()),
          buf.remaining(), BlobType.MetadataBlob);
    }
  }
}

/**
 * Different states of a PutChunk.
 */
enum ChunkState {
  /**
   * The Chunk is free and can be filled with data.
   */
  Free,
  /**
   * The Chunk is being built. It may have some data but is not yet ready to be sent.
   */
  Building,
  /**
   * The Chunk is ready to be sent out.
   */
  Ready,
  /**
   * The Chunk is complete.
   */
  Complete,
}
