/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Time;
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
 * The associated object of a put operation ends up as one or more chunks, depending on its size. If the size is less
 * than max put chunk size, then the operation will involve a single chunk. Those that are larger are split into
 * multiple chunks and distributed across partitions individually. Each chunk will be a blob at the server and will
 * have its own blob id. For such composite blobs, an associated metadata chunk will be created that consists of
 * the blob ids of every data chunk; and the blob id of this metadata chunk will become the blob id of the overall
 * object.
 *
 * For simple blobs (blobs that are under the max put chunk size), only a single chunk is created and held
 * within a single PutChunk object which is used to complete the whole operation. No metadata chunks are created. The
 * blob id of the single data chunk will be the blob id of the blob.
 *
 * For composite blobs, a PutOperation consists of a fixed number of PutChunk objects and a MetadataPutChunk object.
 * Each PutChunk holds one of the chunks of the object at any time and attempts to put the chunk successfully, and then
 * possibly moves on to hold another chunk of the object. This is repeated until either the operation fails or until all
 * the chunks are put successfully. If all the chunks are put successfully, the MetadataPutChunk is created to put
 * the metadata chunk consisting of the blob ids of all the data chunks, and the successful put of the metadata
 * chunk successfully completes the operation.
 */
class PutOperation {
  // Operation arguments.
  private final RouterConfig routerConfig;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ClusterMap clusterMap;
  private final ResponseHandler responseHandler;
  private final BlobProperties blobProperties;
  private final byte[] userMetadata;
  private final ReadableStreamChannel channel;
  private final ByteBufferAsyncWritableChannel chunkFillerChannel;
  private final FutureResult<String> futureResult;
  private final Callback<String> callback;
  private final ReadyForPollCallback readyForPollCallback;
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
  // The time at which the operation was submitted.
  private final long submissionTimeMs;
  // The point in time at which the most recent wait for free chunk availability started.
  private long startTimeForChunkAvailabilityWaitMs;
  // The point in time at which the most recent wait for channel data availability started.
  private long startTimeForChannelDataAvailabilityMs;
  // The time spent in waiting for a chunk to become available to be filled when the channel had data.
  private long waitTimeForCurrentChunkAvailabilityMs;
  // The time spent by a chunk for data to be available in the channel.
  private long waitTimeForChannelDataAvailabilityMs;

  private static final Logger logger = LoggerFactory.getLogger(PutOperation.class);

  /**
   * Construct a PutOperation with the given parameters. For any operation, based on the max chunk size for puts,
   * an object contained within the {@link ReadableStreamChannel} will either be put as a single blob if its size is
   * less than the max chunk size; or will be split into as many chunks as required each of which is no longer in
   * size than the max chunk put size, and a single metadata blob containing the information about each of these
   * chunks.
   * @param routerConfig the {@link RouterConfig} containing the configs for put operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param blobProperties the BlobProperties associated with the put operation.
   * @param userMetadata the userMetadata associated with the put operation.
   * @param channel the {@link ReadableStreamChannel} containing the blob data.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param readyForPollCallback The callback to be used to notify the router of any state changes within this
   *                             operation.
   * @param time the Time instance to use.
   * @throws RouterException if there is an error in constructing the PutOperation with the given parameters.
   */
  PutOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, BlobProperties blobProperties, byte[] userMetadata,
      ReadableStreamChannel channel, FutureResult<String> futureResult, Callback<String> callback,
      ReadyForPollCallback readyForPollCallback,
      ByteBufferAsyncWritableChannel.ChannelEventListener writableChannelEventListener, Time time)
      throws RouterException {
    submissionTimeMs = time.milliseconds();
    blobSize = blobProperties.getBlobSize();
    if (channel.getSize() != blobSize) {
      throw new RouterException("Channel size: " + channel.getSize() + " different from size in BlobProperties: "
          + blobProperties.getBlobSize(), RouterErrorCode.BadInputChannel);
    }
    // Set numDataChunks
    // the max blob size that can be supported is technically limited by the max chunk size configured.
    long numDataChunksL = blobSize == 0 ? 1 : (blobSize - 1) / routerConfig.routerMaxPutChunkSizeBytes + 1;
    if (numDataChunksL > Integer.MAX_VALUE) {
      throw new RouterException("Cannot support a blob size of " + blobSize + " with a chunk size of "
          + routerConfig.routerMaxPutChunkSizeBytes, RouterErrorCode.BlobTooLarge);
    }
    numDataChunks = (int) numDataChunksL;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.channel = channel;
    this.futureResult = futureResult;
    this.callback = callback;
    this.readyForPollCallback = readyForPollCallback;
    this.time = time;
    bytesFilledSoFar = 0;
    chunkCounter = -1;

    // Initialize chunks
    putChunks = new PutChunk[Math.min(numDataChunks, NonBlockingRouter.MAX_IN_MEM_CHUNKS)];
    for (int i = 0; i < putChunks.length; i++) {
      putChunks[i] = new PutChunk();
    }
    metadataPutChunk = numDataChunks > 1 ? new MetadataPutChunk() : null;
    chunkFillerChannel = new ByteBufferAsyncWritableChannel(writableChannelEventListener);
  }

  /**
   * Start reading from the channel containing the data for this operation.
   */
  void startReadingFromChannel() {
    channel.readInto(chunkFillerChannel, new Callback<Long>() {
      @Override
      public void onCompletion(Long result, Exception exception) {
        if (exception != null) {
          setOperationExceptionAndComplete(exception);
        } else if (result != blobSize) {
          setOperationExceptionAndComplete(new RouterException(
              "Incorrect number of bytes: " + result + " read in from the channel, expected: " + blobSize,
              RouterErrorCode.BadInputChannel));
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
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
   *                                    created as part of this poll operation.
   */
  void poll(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
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
   * @param putResponse the {@link PutResponse} associated with this response.
   */
  void handleResponse(ResponseInfo responseInfo, PutResponse putResponse) {
    PutChunk putChunk = correlationIdToPutChunk.remove(
        ((RequestOrResponse) responseInfo.getRequestInfo().getRequest()).getCorrelationId());
    putChunk.handleResponse(responseInfo, putResponse);
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
      if (chunk.getChunkException() == null) {
        logger.error("Operation on chunk failed, but no exception was set");
      }
      logger.error("Failed putting chunk at index: " + chunk.getChunkIndex() + ", failing the entire operation");
      operationCompleted = true;
    } else if (numDataChunks == 1 || chunk == metadataPutChunk) {
      blobId = chunk.getChunkBlobId();
      // the overall operation has succeeded.
      if (chunk.failedAttempts > 0) {
        logger.trace("Slipped put succeeded for chunk: " + chunk.getChunkBlobId());
        routerMetrics.slippedPutSuccessCount.inc();
      } else {
        logger.trace("Successfully put chunk: " + chunk.getChunkBlobId());
      }
      operationCompleted = true;
    } else {
      // a data chunk has succeeded. More to come.
      logger.trace("Successfully put chunk with blob id: " + chunk.getChunkBlobId());
      metadataPutChunk.addChunkId(chunk.chunkBlobId, chunk.chunkIndex);
    }
    routerMetrics.putChunkOperationLatencyMs.update(time.milliseconds() - chunk.chunkReadyAtMs);
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
   * @throws InterruptedException if the call to get a chunk from the chunkFillerChannel is interrupted.
   */
  void fillChunks() {
    try {
      PutChunk chunkToFill;
      if (!chunkFillingCompleted && !operationCompleted) {
        do {
          // Attempt to fill a chunk
          if (channelReadBuffer == null) {
            channelReadBuffer = chunkFillerChannel.getNextChunk(0);
          }
          if (channelReadBuffer != null) {
            maybeStopTrackingWaitForChannelDataTime();
            chunkToFill = getChunkToFill();
            if (chunkToFill == null) {
              // channel has data, but no chunks are free to be filled yet.
              maybeStartTrackingWaitForChunkTime();
              break;
            } else {
              // channel has data, and there is a chunk that can be filled.
              maybeStopTrackingWaitForChunkTime();
              bytesFilledSoFar += chunkToFill.fillFrom(channelReadBuffer);
              if (chunkToFill.isReady()) {
                readyForPollCallback.onPollReady();
                updateChunkFillerWaitTimeMetrics();
              }
              if (!channelReadBuffer.hasRemaining()) {
                chunkFillerChannel.resolveOldestChunk(null);
                channelReadBuffer = null;
              }
            }
          } else {
            // channel does not have more data yet.
            if (getFreeChunk() != null) {
              // this means there is a chunk available to be filled, but no data in the channel.
              maybeStartTrackingWaitForChannelDataTime();
            }
            break;
          }
        } while (bytesFilledSoFar < blobSize);
        if (bytesFilledSoFar == blobSize) {
          chunkFillingCompleted = true;
        }
      }
    } catch (Exception e) {
      routerMetrics.chunkFillerUnexpectedErrorCount.inc();
      readyForPollCallback.onPollReady();
      setOperationExceptionAndComplete(new RouterException("PutOperation fillChunks encountered unexpected error", e,
          RouterErrorCode.UnexpectedInternalError));
    }
  }

  /**
   * Called whenever the channel has data but no free or building chunk is available to be filled.
   */
  private void maybeStartTrackingWaitForChunkTime() {
    if (startTimeForChunkAvailabilityWaitMs == 0) {
      // this is the first point in time after the last chunk filling (if any) when the filling was blocked due to
      // chunk unavailability, so mark this time.
      startTimeForChunkAvailabilityWaitMs = time.milliseconds();
    } else {
      // the wait was already initiated, so do nothing.
    }
  }

  /**
   * Called whenever the channel has data and there is a free or building chunk available to be filled.
   */
  private void maybeStopTrackingWaitForChunkTime() {
    if (startTimeForChunkAvailabilityWaitMs != 0) {
      // this is the first point in time since the last wait that a chunk became available for filling.
      waitTimeForCurrentChunkAvailabilityMs = time.milliseconds() - startTimeForChunkAvailabilityWaitMs;
      startTimeForChunkAvailabilityWaitMs = 0;
    }
  }

  /**
   * Called whenever a chunk is available to be filled but there is no data available in the channel.
   */
  private void maybeStartTrackingWaitForChannelDataTime() {
    if (startTimeForChannelDataAvailabilityMs == 0) {
      // this is the first point in time after the last time data was read from the channel that data became
      // unavailable in the channel, so mark this time.
      startTimeForChannelDataAvailabilityMs = time.milliseconds();
    } else {
      // the wait was already initiated, so do nothing.
    }
  }

  /**
   * Called whenever data becomes available in the channel.
   */
  private void maybeStopTrackingWaitForChannelDataTime() {
    if (startTimeForChannelDataAvailabilityMs != 0) {
      // this is the first point in time since the last wait that data became available in the channel.
      waitTimeForChannelDataAvailabilityMs += time.milliseconds() - startTimeForChannelDataAvailabilityMs;
      startTimeForChannelDataAvailabilityMs = 0;
    }
  }

  /**
   * Update metrics related to how long a channel had to wait for a chunk to become available for filling, and
   * how long the chunk had to wait for data to become available in the channel.
   */
  private void updateChunkFillerWaitTimeMetrics() {
    routerMetrics.waitTimeForFreeChunkAvailabilityMs.update(waitTimeForCurrentChunkAvailabilityMs);
    routerMetrics.waitTimeForChannelDataAvailabilityMs.update(waitTimeForChannelDataAvailabilityMs);
    waitTimeForCurrentChunkAvailabilityMs = 0;
    waitTimeForChannelDataAvailabilityMs = 0;
  }

  /**
   * Get the chunk to be filled. At most one chunk for an operation will ever be in Building state. If there is such
   * a chunk, that is returned. If not, if there is a Free chunk, that is returned. If no Free chunks are available
   * either, then null is returned.
   * @return the chunk to fill, or null if there are no chunks eligible for filling.
   */
  private PutChunk getChunkToFill() {
    if (chunkToFill == null || !chunkToFill.isBuilding()) {
      chunkToFill = getFreeChunk();
      if (chunkToFill != null) {
        chunkCounter++;
        chunkToFill.prepareForBuilding(chunkCounter, getSizeOfChunkAt(chunkCounter));
      }
    }
    return chunkToFill;
  }

  /**
   * @return A free chunk, if one is available; null otherwise.
   */
  private PutChunk getFreeChunk() {
    for (PutChunk chunk : putChunks) {
      if (chunk.isFree()) {
        return chunk;
      }
    }
    return null;
  }

  /**
   * Get the chunk size of the chunk at the given position.
   * @param pos the position of the chunk in the overall blob.
   * @return the size of the chunk.
   */
  private int getSizeOfChunkAt(int pos) {
    return pos == numDataChunks - 1 ? (int) ((blobSize - 1) % routerConfig.routerMaxPutChunkSizeBytes + 1)
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
   * The time at which this operation was submitted.
   * @return the time at which the operation was submitted.
   */
  long getSubmissionTimeMs() {
    return submissionTimeMs;
  }

  /**
   * if this is a composite object, fill the list with successfully put chunk ids.
   * @param chunkIdList the list to fill with chunk ids.
   */
  void addSuccessfullyPutChunkIds(List<String> chunkIdList) {
    if (numDataChunks > 1) {
      metadataPutChunk.addChunkIds(chunkIdList);
    }
  }

  /**
   * Set the irrecoverable exception associated with this operation. When this is called, the operation has failed.
   * @param exception the irrecoverable exception associated with this operation.
   */
  void setOperationExceptionAndComplete(Exception exception) {
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
    // the most recent time at which this chunk became Free.
    private long chunkFreeAtMs;
    // the most recent time time at which this chunk became ready.
    private long chunkReadyAtMs;
    // The exception encountered while putting the current chunk. Not all errors are irrecoverable. An error may or
    // may not get overridden by a subsequent error, and this variable is meant to store the most relevant error.
    private RouterException chunkException;
    // the state of the current chunk.
    protected volatile ChunkState state;
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
    private final Map<Integer, ChunkPutRequestInfo> correlationIdToChunkPutRequestInfo =
        new TreeMap<Integer, ChunkPutRequestInfo>();
    // list of buffers that were once associated with this chunk and are not yet freed.
    private final List<DefunctBufferInfo> defunctBufferInfos = new ArrayList<>();
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
      failedAttempts = 0;
      partitionId = null;
      attemptedPartitionIds.clear();
      maybeUpdateDefunctBufferInfos();
      correlationIdToChunkPutRequestInfo.clear();
      // this assignment should be the last statement as this immediately makes this chunk available to the
      // ChunkFiller thread for filling.
      state = ChunkState.Free;
      chunkFreeAtMs = time.milliseconds();
    }

    /**
     * Go through the list of requests for which responses were not received, and if there are any that are not yet
     * sent out completely, add the associated buffer to the defunct list for freeing in the future.
     */
    private void maybeUpdateDefunctBufferInfos() {
      ArrayList<PutRequest> requestsAwaitingSendCompletion = null;
      for (Map.Entry<Integer, ChunkPutRequestInfo> entry : correlationIdToChunkPutRequestInfo.entrySet()) {
        if (!entry.getValue().putRequest.isSendComplete()) {
          if (requestsAwaitingSendCompletion == null) {
            requestsAwaitingSendCompletion = new ArrayList<>();
          }
          requestsAwaitingSendCompletion.add(entry.getValue().putRequest);
        }
      }

      if (requestsAwaitingSendCompletion != null) {
        // This means that the buffer associated with this PutChunk could get read by the NetworkClient in the
        // future and assigning this PutChunk to a subsequent chunk of the overall blob could lead to this buffer
        // getting read and written concurrently, or other undefined behavior. There are multiple ways to handle this,
        // and the simplest way is to set the buf to null so that it gets allocated afresh if/when this PutChunk gets
        // assigned for a subsequent chunk of the overall blob. Every time this chunk gets polled, an attempt to clear
        // out the list will be made.
        defunctBufferInfos.add(new DefunctBufferInfo(buf, requestsAwaitingSendCompletion));
        buf = null;
      }
    }

    /**
     * Iterate defunctBufferInfos and possibly free up entries from it.
     */
    private void maybeFreeDefunctBuffers() {
      for (Iterator<DefunctBufferInfo> iter = defunctBufferInfos.iterator(); iter.hasNext(); ) {
        boolean canBeFreed = true;
        for (PutRequest putRequest : iter.next().putRequests) {
          if (!putRequest.isSendComplete()) {
            canBeFreed = false;
          }
        }
        if (canBeFreed) {
          // this is where the buffer will be freed if the buffer pool is used. For now, simply remove the reference.
          iter.remove();
        }
      }
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
     * @return the {@link RouterException}, if any, encountered for the current chunk.
     */
    RouterException getChunkException() {
      return chunkException;
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
        correlationIdToChunkPutRequestInfo.clear();
        state = ChunkState.Ready;
      } catch (RouterException e) {
        setOperationExceptionAndComplete(e);
      } catch (Exception e) {
        setOperationExceptionAndComplete(new RouterException("Operation tracker could not be initialized", e,
            RouterErrorCode.UnexpectedInternalError));
      }
    }

    /**
     * Do the actions required when the chunk has been completely built.
     */
    void onFillComplete() {
      buf.flip();
      prepareForSending();
      chunkReadyAtMs = time.milliseconds();
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
        routerMetrics.chunkFillTimeMs.update(chunkReadyAtMs - chunkFreeAtMs);
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
          if (failedAttempts <= routerConfig.routerMaxSlippedPutAttempts) {
            logger.trace("Attempt to put chunk with id: " + chunkBlobId + " failed, attempting slipped put");
            routerMetrics.slippedPutAttemptCount.inc();
            prepareForSending();
          } else {
            // this chunk could not be successfully put. The whole operation has to fail.
            chunkBlobId = null;
            setOperationExceptionAndComplete(chunkException);
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
     * This is one of two main entry points to this class, the other being
     * {@link #handleResponse(ResponseInfo, PutResponse)}.
     * Apart from fetching requests to send out, this also checks for timeouts of issued requests,
     * status of the operation and anything else that needs to be done within this PutChunk. The callers guarantee
     * that this method is called on all the PutChunks of an operation until either the operation,
     * or the chunk operation is completed.
     * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
     *                                    created as part of this poll operation.
     */
    void poll(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
      maybeFreeDefunctBuffers();
      cleanupExpiredInFlightRequests();
      checkAndMaybeComplete();
      if (!isComplete()) {
        fetchRequests(requestRegistrationCallback);
      }
    }

    /**
     * Clean up requests sent out by this operation that have now timed out.
     */
    private void cleanupExpiredInFlightRequests() {
      Iterator<Map.Entry<Integer, ChunkPutRequestInfo>> inFlightRequestsIterator =
          correlationIdToChunkPutRequestInfo.entrySet().iterator();
      while (inFlightRequestsIterator.hasNext()) {
        Map.Entry<Integer, ChunkPutRequestInfo> entry = inFlightRequestsIterator.next();
        if (time.milliseconds() - entry.getValue().startTimeMs > routerConfig.routerRequestTimeoutMs) {
          onErrorResponse(entry.getValue().replicaId);
          // Do not notify this as a failure to the response handler, as this timeout could simply be due to
          // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
          // response and the response handler will be notified accordingly.
          chunkException = new RouterException("Timed out waiting for a response", RouterErrorCode.OperationTimedOut);
          inFlightRequestsIterator.remove();
        } else {
          // the entries are ordered by correlation id and time. Break on the first request that has not timed out.
          break;
        }
      }
    }

    /**
     * Fetch {@link PutRequest}s to send for the current data chunk.
     */
    private void fetchRequests(RequestRegistrationCallback<PutOperation> requestRegistrationCallback) {
      Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
      while (replicaIterator.hasNext()) {
        ReplicaId replicaId = replicaIterator.next();
        String hostname = replicaId.getDataNodeId().getHostname();
        Port port = replicaId.getDataNodeId().getPortToConnectTo();
        PutRequest putRequest = createPutRequest();
        RouterRequestInfo request = new RouterRequestInfo(hostname, port, putRequest, replicaId);
        int correlationId = putRequest.getCorrelationId();
        correlationIdToChunkPutRequestInfo.put(correlationId,
            new ChunkPutRequestInfo(replicaId, putRequest, time.milliseconds()));
        correlationIdToPutChunk.put(correlationId, this);
        requestRegistrationCallback.registerRequestToSend(PutOperation.this, request);
        replicaIterator.remove();
        if (RouterUtils.isRemoteReplica(routerConfig, replicaId)) {
          logger.trace("Making request to a remote replica in", replicaId.getDataNodeId().getDatacenterName());
          routerMetrics.crossColoRequestCount.inc();
        }
        routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).putRequestRate.mark();
      }
    }

    /**
     * Create and return the {@link PutRequest} associated with the current chunk. This method should only be called
     * when the chunk is in ready state.
     * @return the crated {@link PutRequest}.
     */
    protected PutRequest createPutRequest() {
      return new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
          chunkBlobId, blobProperties, ByteBuffer.wrap(userMetadata), buf.duplicate(), buf.remaining(),
          BlobType.DataBlob);
    }

    /**
     * Choose a random {@link PartitionId} for putting the current chunk and return it.
     * @param partitionIdsToExclude the list of {@link PartitionId}s that should be excluded from consideration.
     * @return the chosen {@link PartitionId}
     * @throws RouterException
     */
    protected PartitionId getPartitionForPut(List<PartitionId> partitionIdsToExclude) throws RouterException {
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
     * @param putResponse the {@link PutResponse} associated with this response.
     */
    void handleResponse(ResponseInfo responseInfo, PutResponse putResponse) {
      int correlationId = ((PutRequest) responseInfo.getRequestInfo().getRequest()).getCorrelationId();
      ChunkPutRequestInfo chunkPutRequestInfo = correlationIdToChunkPutRequestInfo.remove(correlationId);
      if (chunkPutRequestInfo == null) {
        // Ignore right away. This could mean:
        // - the response is valid for this chunk, but was timed out and removed from the map.
        // - the response is for an earlier attempt of this chunk (slipped put scenario). And the map was cleared
        // before attempting the slipped put.
        // - the response is for an earlier chunk held by this PutChunk.
        return;
      }
      long requestLatencyMs = time.milliseconds() - chunkPutRequestInfo.startTimeMs;
      routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
      routerMetrics.getDataNodeBasedMetrics(chunkPutRequestInfo.replicaId.getDataNodeId()).putRequestLatencyMs.update(
          requestLatencyMs);
      boolean isSuccessful;
      if (responseInfo.getError() != null) {
        setChunkException(new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut));
        isSuccessful = false;
      } else {
        if (putResponse == null) {
          setChunkException(new RouterException("Response deserialization received an unexpected error",
              RouterErrorCode.UnexpectedInternalError));
          isSuccessful = false;
        } else {
          if (putResponse.getCorrelationId() != correlationId) {
            // The NetworkClient associates a response with a request based on the fact that only one request is sent
            // out over a connection id, and the response received on a connection id must be for the latest request
            // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
            // There is no other way to handle it.
            routerMetrics.unknownReplicaResponseError.inc();
            logger.error("The correlation id in the PutResponse " + putResponse.getCorrelationId()
                + " is not the same as the correlation id in the associated PutRequest: " + correlationId);
            setChunkException(
                new RouterException("Unexpected internal error", RouterErrorCode.UnexpectedInternalError));
            isSuccessful = false;
            // we do not notify the ResponseHandler responsible for failure detection as this is an unexpected error.
          } else {
            ServerErrorCode putError = putResponse.getError();
            if (putError == ServerErrorCode.No_Error) {
              logger.trace("The putRequest was successful");
              isSuccessful = true;
            } else {
              // chunkException will be set within processServerError.
              processServerError(putResponse.getError());
              isSuccessful = false;
            }
          }
        }
      }
      if (isSuccessful) {
        operationTracker.onResponse(chunkPutRequestInfo.replicaId, true);
        if (RouterUtils.isRemoteReplica(routerConfig, chunkPutRequestInfo.replicaId)) {
          logger.trace("Cross colo request successful for remote replica in ",
              chunkPutRequestInfo.replicaId.getDataNodeId().getDatacenterName());
          routerMetrics.crossColoSuccessCount.inc();
        }
      } else {
        onErrorResponse(chunkPutRequestInfo.replicaId);
      }
      checkAndMaybeComplete();
    }

    /**
     * Perform the necessary actions when a request to a replica fails.
     * @param replicaId the {@link ReplicaId} associated with the failed response.
     */
    void onErrorResponse(ReplicaId replicaId) {
      operationTracker.onResponse(replicaId, false);
      routerMetrics.routerRequestErrorCount.inc();
      routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).putRequestErrorCount.inc();
    }

    /**
     * Possibly set the exception for this chunk using the given exception. Calling this method with an exception does
     * not necessarily result in that being set as the chunkException. The idea is to set the most relevant exception
     * in case of errors.
     * @param exception the exception that may be set as the chunkException.
     */
    private void setChunkException(RouterException exception) {
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
      logger.trace("Server returned an error: ", error);
      setChunkException(new RouterException("Could not complete operation, server returned: " + error,
          RouterErrorCode.AmbryUnavailable));
    }

    /**
     * A class that holds information about requests sent out by this PutChunk.
     */
    private class ChunkPutRequestInfo {
      final ReplicaId replicaId;
      final PutRequest putRequest;
      final long startTimeMs;

      /**
       * Construct a ChunkPutRequestInfo
       * @param replicaId the replica to which this request is being sent.
       * @param startTimeMs the time at which this request was created.
       */
      ChunkPutRequestInfo(ReplicaId replicaId, PutRequest putRequest, long startTimeMs) {
        this.replicaId = replicaId;
        this.putRequest = putRequest;
        this.startTimeMs = startTimeMs;
      }
    }

    /**
     * Class that holds the buffer of a chunk that will no longer be used and is kept around only because the
     * associated requests are not yet completely sent out.
     */
    private class DefunctBufferInfo {
      // the buffer that is now defunct, but not yet freed.
      final ByteBuffer buf;
      // Requests that are reading from this buffer.
      final List<PutRequest> putRequests;

      /**
       * Construct a DefunctBufferInfo
       * @param buf the buffer that is now defunct and waiting to be freed.
       * @param putRequests the requests associated with this buffer whose send completion blocks the freeing of this
       *                    buffer.
       */
      DefunctBufferInfo(ByteBuffer buf, List<PutRequest> putRequests) {
        this.buf = buf;
        this.putRequests = putRequests;
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
    // since chunk operations could complete out of order, this index simply tracks the farthest chunk that was
    // successfully put (which helps in the getChunkIds() method).
    int maxFilledChunkIndex = -1;

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
      if (chunkIndex > maxFilledChunkIndex) {
        maxFilledChunkIndex = chunkIndex;
      }
      if (chunksDone == numDataChunks) {
        buf = MetadataContentSerDe.serializeMetadataContent(routerConfig.routerMaxPutChunkSizeBytes, blobSize,
            Arrays.asList(chunkIds));
        onFillComplete();
      }
    }

    /**
     * Add all the successfully put chunk ids of the overall blob to the passed in list.
     * @param chunkIdList list to fill with chunk ids.
     */
    void addChunkIds(List<String> chunkIdList) {
      for (int i = 0; i <= maxFilledChunkIndex; i++) {
        if (chunkIds[i] != null) {
          chunkIdList.add(chunkIds[i].getID());
        }
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
          chunkBlobId, blobProperties, ByteBuffer.wrap(userMetadata), buf.duplicate(), buf.remaining(),
          BlobType.MetadataBlob);
    }
  }

  /**
   * Different states of a PutChunk.
   */
  enum ChunkState {
    /**
     * The Chunk is free and can be filled with data.
     */
    Free, /**
     * The Chunk is being built. It may have some data but is not yet ready to be sent.
     */
    Building, /**
     * The Chunk is ready to be sent out.
     */
    Ready, /**
     * The Chunk is complete.
     */
    Complete,
  }
}

