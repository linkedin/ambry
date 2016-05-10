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
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GetBlobOperation class is responsible for maintaining the state associated with a GetBlob operation, and completing
 * it. It would help to look at this class in conjunction with {@link PutOperation}.
 *
 * Blobs that need to be fetched are either simple (those that consist of only a single chunk) or composite (those
 * that consists of one metadata chunk and two or more data chunks). Whether a blob is simple or composite can only be
 * determined after the first chunk is fetched, based on whether the first chunk turns out to be a metadata chunk or
 * a data chunk.
 *
 * As soon as the first data chunk is fetched (which could be the first chunk in the case of simple blobs and the
 * second chunk fetched in the case of composite blobs), the operation callback is invoked (and the future is marked
 * as done) so that the caller can start reading in data. The rest of the chunks are asynchronously fetched and
 * buffered up to the maximum that can be buffered. When fetched chunks are consumed by the caller, subsequent chunks
 * become eligible to be fetched.
 *
 * This class implements {@link ReadableStreamChannel} and passes itself as the result of this operation.
 */
class GetBlobOperation extends GetOperation<ReadableStreamChannel> implements ReadableStreamChannel {
  // the callback to use to complete the operation.
  private final OperationCompleteCallback operationCompleteCallback;
  // whether the operationCompleteCallback has been called already.
  private final AtomicBoolean operationCallbackInvoked = new AtomicBoolean(false);
  // The first chunk may be a metadata chunk if the blob is composite, or the only data chunk if the blob is simple.
  private final FirstGetChunk firstChunk;
  // Associated with all data chunks in the case of composite blobs. Only a fixed number of these are initialized.
  // Each of these is initialized with the information required to fetch a data chunk and is responsible for
  // retrieving and adding it to the list of chunk buffers. Once complete, they are reused to fetch subsequent data
  // chunks.
  private GetChunk[] dataChunks;
  // the factory to use to deserialize keys in a metadata chunk.
  private final BlobIdFactory blobIdFactory;
  // the total number of data chunks associated with this blob.
  private int numChunksTotal;
  // the total number of data chunks retrieved so far (and may or may not have been written out yet).
  private int numChunksRetrieved;
  // a list iterator to the chunk ids that need to be fetched for this operation, if this is a composite blob.
  private ListIterator<StoreKey> chunkIdIterator;
  // chunk index to retrieved chunk buffer mapping.
  private Map<Integer, ByteBuffer> chunkIndexToBuffer;
  // To find the GetChunk to hand over the response quickly.
  private final Map<Integer, GetChunk> correlationIdToGetChunk = new HashMap<>();

  // ReadableStreamChannel implementation related variables:

  // whether this ReadableStreamChannel is open.
  private boolean isOpen = true;
  // whether readInto() has been called yet by the caller on this ReadableStreamChannel.
  private volatile boolean readCalled = false;
  // The channel to write chunks of the blob into. This will be initialized when the caller calls the readInto().
  private AsyncWritableChannel asyncWritableChannel;
  // the callback to call when all the chunks are successfully written out into the asyncWritableChannel.
  private Callback<Long> readIntoCallback;
  // the future to mark as done when all the chunks are successfully written out into the asyncWritableChannel.
  private FutureResult<Long> readIntoFuture;
  // the number of bytes written out to the asyncWritableChannel. This would be the size of the blob eventually.
  private Long bytesWritten = 0L;
  // the number of chunks that have been written out to the asyncWritableChannel.
  private final AtomicInteger numChunksWrittenOut = new AtomicInteger(0);
  // the index of the next chunk that is to be written out to the asyncWritableChannel.
  private int indexOfNextChunkToWriteOut = 0;
  // whether this object has called the readIntoCallback yet.
  private AtomicBoolean readIntoCallbackCalled = new AtomicBoolean(false);
  // the callback that is passed into the asyncWritableChannel write() operation.
  Callback<Long> chunkAsyncWriteCallback = new Callback<Long>() {
    @Override
    public void onCompletion(Long result, Exception exception) {
      bytesWritten += result;
      if (exception != null) {
        operationException.set(exception);
      }
      numChunksWrittenOut.incrementAndGet();
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(GetBlobOperation.class);

  /**
   * Construct a GetBlobOperation
   * @param routerConfig the {@link RouterConfig} containing the configs for get operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param blobIdStr the blob id associated with the operation in string form.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param operationCompleteCallback the {@link OperationCompleteCallback} to use to complete operations.
   * @param time the Time instance to use.
   * @throws RouterException if there is an error with any of the parameters, such as an invalid blob id.
   */
  GetBlobOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, String blobIdStr, FutureResult<ReadableStreamChannel> futureResult,
      Callback<ReadableStreamChannel> callback, OperationCompleteCallback operationCompleteCallback, Time time)
      throws RouterException {
    super(routerConfig, routerMetrics, clusterMap, responseHandler, blobIdStr, futureResult, callback, time);
    this.operationCompleteCallback = operationCompleteCallback;
    this.blobIdFactory = new BlobIdFactory(clusterMap);
    firstChunk = new FirstGetChunk(blobId);
  }

  /**
   * {@inheritDoc}
   * <br>
   * If the callback for the operation was already called, then abort the reads from the channel and the caller
   * will be notified as part of the read callback eventually. If not, the notification will happen as part of the
   * operation completion callback.
   */
  @Override
  void abort(Exception abortCause) {
    if (operationCallbackInvoked.compareAndSet(false, true)) {
      operationCompleteCallback.completeOperation(operationFuture, operationCallback, null, abortCause);
    } else {
      operationException.set(abortCause);
      if (readCalled) {
        completeRead();
      }
    }
    operationCompleted = true;
  }

  /**
   * Return the {@link MessageFormatFlags} to associate with a getBlob operation.
   * @return {@link MessageFormatFlags#Blob}
   */
  @Override
  MessageFormatFlags getOperationFlag() {
    return MessageFormatFlags.Blob;
  }

  /**
   * Do all that needs to be done (cleanup, notification, etc.) on chunk completion and mark the state of the chunk
   * appropriately.
   * @param chunk the chunk that has completed.
   */
  private void onChunkOperationComplete(GetChunk chunk) {
    if (chunk.getChunkException() != null) {
      // if operation callback was already called, then this exception will have to be notified as part of the
      // read callback.
      setOperationException(chunk.getChunkException());
    }
    if (chunk == firstChunk) {
      if (operationCallbackInvoked.compareAndSet(false, true)) {
        // Complete the operation from the caller's perspective, so that the caller can start reading from the
        // channel. The operation will not be marked as complete internally as subsequent chunk retrievals and
        // channel writes will need to happen and for that, this operation needs the GetManager to poll it
        // periodically. If any exception is encountered while processing subsequent chunks, those will be notified
        // during the channel read.
        ReadableStreamChannel result = getOperationException() == null ? this : null;
        operationCompleteCallback
            .completeOperation(operationFuture, operationCallback, result, getOperationException());
      }
    }
    chunk.postCompletionCleanup();
    maybeWriteToChannel();
  }

  /**
   * Handle the given {@link ResponseInfo} by handing it over to the appropriate chunk that issued the request.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   */
  @Override
  void handleResponse(ResponseInfo responseInfo) {
    GetChunk getChunk =
        correlationIdToGetChunk.remove(((RequestOrResponse) responseInfo.getRequest()).getCorrelationId());
    getChunk.handleResponse(responseInfo);
    if (getChunk.isComplete()) {
      onChunkOperationComplete(getChunk);
    }
  }

  /**
   * Attempt to write the data associated with the blob to the channel passed in by the caller (if the caller has
   * done so).
   */
  private void maybeWriteToChannel() {
    // if there are chunks available to be written out, do now.
    if (firstChunk.isComplete() && readCalled) {
      while (indexOfNextChunkToWriteOut < numChunksTotal && chunkIndexToBuffer
          .containsKey(indexOfNextChunkToWriteOut)) {
        ByteBuffer chunkBuf = chunkIndexToBuffer.remove(indexOfNextChunkToWriteOut);
        asyncWritableChannel.write(chunkBuf, chunkAsyncWriteCallback);
        indexOfNextChunkToWriteOut++;
      }
      if (operationException.get() != null || numChunksWrittenOut.get() == numChunksTotal) {
        completeRead();
        operationCompleted = true;
      }
    }
  }

  /**
   * Complete the read from this {@link ReadableStreamChannel} by invoking the callback and marking the future.
   */
  void completeRead() {
    if (readIntoCallbackCalled.compareAndSet(false, true)) {
      if (operationException.get() != null) {
        bytesWritten = null;
      }
      readIntoFuture.done(bytesWritten, operationException.get());
      if (readIntoCallback != null) {
        readIntoCallback.onCompletion(bytesWritten, operationException.get());
      }
    }
  }

  /**
   * As part of the poll, GetBlobOperation fetches new requests to be issued and updates the state based on
   * whether previously issued requests have timed out and so on. Additionally, any writes of previously retrieved
   * chunk buffers into the {@link AsyncWritableChannel} passed in by the caller also happens within this method.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
   *                                    created as part of this poll operation.
   */
  @Override
  void poll(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
    if (operationCompleted) {
      return;
    }
    if (firstChunk.isReady() || firstChunk.isInProgress()) {
      firstChunk.poll(requestRegistrationCallback);
    }
    if (firstChunk.isComplete()) {
      // Although an attempt is made to write to the channel as soon as a chunk is successfully retrieved,
      // the caller might not have called readInto() and passed in a channel at the time. So an attempt is always
      // made from within poll.
      maybeWriteToChannel();
      // If this is a composite blob, poll for requests for subsequent chunks.
      if (dataChunks != null) {
        for (GetChunk dataChunk : dataChunks) {
          if (dataChunk.isFree() && chunkIdIterator.hasNext()) {
            dataChunk.initialize(chunkIdIterator.nextIndex(), (BlobId) chunkIdIterator.next());
          }
          if (dataChunk.isInProgress() || (dataChunk.isReady()
              && numChunksRetrieved - numChunksWrittenOut.get() < NonBlockingRouter.MAX_IN_MEM_CHUNKS)) {
            dataChunk.poll(requestRegistrationCallback);
          }
        }
      }
    }
    if (operationException.get() != null) {
      abort(operationException.get());
    }
  }

  // ReadableStreamChannel implementation:

  /**
   * {@inheritDoc}
   * <br>
   * The bytes that will be read from this channel is not known until the read is complete.
   * @return -1.
   */
  @Override
  public long getSize() {
    return -1;
  }

  /**
   * {@inheritDoc}
   * <br>
   * @param asyncWritableChannel the {@link AsyncWritableChannel} to read the data into.
   * @param callback the {@link Callback} that will be invoked either when all the data in the channel has been emptied
   *                 into the {@code asyncWritableChannel} or if there is an exception in doing so. This can be null.
   * @return the {@link Future} that will eventually contain the result of the operation.
   */
  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    if (readCalled) {
      throw new IllegalStateException("Cannot read the result of a GetBlob operation more than once");
    }
    this.asyncWritableChannel = asyncWritableChannel;
    readIntoCallback = callback;
    readIntoFuture = new FutureResult<>();
    readCalled = true;
    return readIntoFuture;
  }

  /**
   * {@inheritDoc}
   * @return true, unless the channel is closed.
   */
  @Override
  public boolean isOpen() {
    return isOpen;
  }

  /**
   * {@inheritDoc}
   * @throws IOException
   */
  @Override
  public void close()
      throws IOException {
    isOpen = false;
  }

  /**
   * GetChunk is used to retrieve and hold a data chunk of a composite blob. An object of this class is initialized
   * to retrieve one data chunk at a time. Once the associated chunk is successfully retrieved, this object can be
   * reinitialized and used to retrieve a subsequent chunk.
   */
  private class GetChunk {
    // the operation tracker used to track the operation on the current chunk.
    private OperationTracker chunkOperationTracker;
    // whether the operation on the current chunk has completed.
    private boolean chunkCompleted;
    // the blob id of the current chunk.
    private BlobId chunkBlobId;
    // the index of the current chunk in the overall blob.
    private int chunkIndex;
    // the most relevant exception encountered for the current chunk.
    protected RouterException chunkException;
    // For a GetChunk, responses may be handled multiple times. Regardless of the successTarget,
    // the actual body of the response is deserialized only once.
    protected boolean successfullyDeserialized;
    // map of correlation id to the request metadata for every request issued for this operation.
    protected final Map<Integer, GetRequestInfo> correlationIdToGetRequestInfo = new TreeMap<>();
    // the state of the chunk.
    protected volatile ChunkState state;

    /**
     * Construct a GetChunk
     * @param index the index (in the overall blob) of the initial data chunk that this GetChunk has to fetch.
     * @param id the {@link BlobId} of the initial data chunk that this GetChunk has to fetch.
     */
    GetChunk(int index, BlobId id) {
      reset();
      initialize(index, id);
    }

    /**
     * Do what needs to be done after the GetBlobOperation is done with the current chunk that has completed. When
     * this is called, the state of the chunk should be {@link ChunkState#Complete}
     */
    void postCompletionCleanup() {
      reset();
    }

    /**
     * Reset the state of this GetChunk.
     */
    void reset() {
      chunkOperationTracker = null;
      chunkCompleted = false;
      chunkBlobId = null;
      chunkIndex = -1;
      chunkException = null;
      successfullyDeserialized = false;
      correlationIdToGetRequestInfo.clear();
      state = ChunkState.Free;
    }

    /**
     * Assign a chunk of the overall blob to this GetChunk.
     * @param index the index of the chunk of the overall blob that needs to be fetched through this GetChunk.
     * @param id the id of the chunk of the overall blob that needs to be fetched through this GetChunk.
     */
    void initialize(int index, BlobId id) {
      chunkIndex = index;
      chunkBlobId = id;
      chunkOperationTracker = new SimpleOperationTracker(routerConfig.routerDatacenterName, chunkBlobId.getPartition(),
          routerConfig.routerGetCrossDcEnabled, routerConfig.routerGetSuccessTarget,
          routerConfig.routerGetRequestParallelism);
      state = ChunkState.Ready;
    }

    /**
     * return the {@link RouterException} associated with the operation on this chunk, if any.
     * @return the {@link RouterException} associated with the operation on this chunk, if any.
     */
    RouterException getChunkException() {
      return chunkException;
    }

    /**
     * This is one of two main entry points to this class, the other being {@link #handleResponse(ResponseInfo)}.
     * Apart from fetching requests to send out, this also checks for timeouts of issued requests,
     * status of the operation and anything else that needs to be done within this GetChunk. The callers guarantee
     * that this method is called on the GetChunks of an operation until either the operation, or the chunk operation
     * is completed.
     * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
     *                                    created as part of this poll operation.
     */
    void poll(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
      //First, check if any of the existing requests have timed out.
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
      //First, check if any of the existing requests have timed out.
      Iterator<Map.Entry<Integer, GetRequestInfo>> inFlightRequestsIterator =
          correlationIdToGetRequestInfo.entrySet().iterator();
      while (inFlightRequestsIterator.hasNext()) {
        Map.Entry<Integer, GetRequestInfo> entry = inFlightRequestsIterator.next();
        if (time.milliseconds() - entry.getValue().startTimeMs > routerConfig.routerRequestTimeoutMs) {
          chunkOperationTracker.onResponse(entry.getValue().replicaId, false);
          chunkException = new RouterException("Timed out waiting for responses", RouterErrorCode.OperationTimedOut);
          inFlightRequestsIterator.remove();
        } else {
          // the entries are ordered by correlation id and time. Break on the first request that has not timed out.
          break;
        }
      }
    }

    /**
     * Fetch {@link GetRequest}s to send for the current data chunk.
     */
    private void fetchRequests(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
      Iterator<ReplicaId> replicaIterator = chunkOperationTracker.getReplicaIterator();
      while (replicaIterator.hasNext()) {
        ReplicaId replicaId = replicaIterator.next();
        replicaIterator.remove();
        String hostname = replicaId.getDataNodeId().getHostname();
        Port port = replicaId.getDataNodeId().getPortToConnectTo();
        GetRequest getRequest = createGetRequest(chunkBlobId, getOperationFlag());
        RequestInfo request = new RequestInfo(hostname, port, getRequest);
        int correlationId = getRequest.getCorrelationId();
        correlationIdToGetRequestInfo.put(correlationId, new GetRequestInfo(replicaId, time.milliseconds()));
        correlationIdToGetChunk.put(correlationId, this);
        requestRegistrationCallback.registerRequestToSend(GetBlobOperation.this, request);
        state = ChunkState.InProgress;
      }
    }

    /**
     * Check if the operation on the chunk is eligible for completion, if so complete it.
     */
    void checkAndMaybeComplete() {
      if (chunkOperationTracker.isDone()) {
        if (chunkOperationTracker.hasSucceeded()) {
          // override any previously set exceptions.
          chunkException = null;
        }
        chunkCompleted = true;
      }
      if (chunkCompleted) {
        operationException.set(chunkException);
        state = ChunkState.Complete;
      }
    }

    /**
     * Handle the body of the response: Deserialize and add to the list of chunk buffers.
     * @param payload the body of the response.
     * @throws IOException if there is an IOException while deserializing the body.
     * @throws MessageFormatException if there is a MessageFormatException while deserializing the body.
     */
    void handleBody(InputStream payload)
        throws IOException, MessageFormatException {
      if (!successfullyDeserialized) {
        BlobData blobData = MessageFormatRecord.deserializeBlob(payload);
        chunkIndexToBuffer.put(chunkIndex,
            ByteBuffer.wrap(Utils.readBytesFromStream(blobData.getStream(), (int) blobData.getSize())));
        numChunksRetrieved++;
        successfullyDeserialized = true;
      } else {
        // If successTarget > 1, then content reconciliation may have to be done. For now, ignore subsequent responses.
      }
    }

    /**
     * This method is the entry point for handling responses received for requests sent out on behalf of this chunk.
     * For gets, processing involves determining whether the request was successful, notifying the operation
     * tracker so it can track the status of the operation, and notifying the response handler for failure detection.
     * Finally, a check is done to determine whether the operation on the chunk is eligible for completion,
     * if so the chunk operation is completed right away.
     * @param responseInfo the response received for a request sent out on behalf of this chunk.
     */
    void handleResponse(ResponseInfo responseInfo) {
      int correlationId = ((GetRequest) responseInfo.getRequest()).getCorrelationId();
      // Get the GetOperation that generated the request.
      GetRequestInfo getRequestInfo = correlationIdToGetRequestInfo.remove(correlationId);
      if (getRequestInfo == null) {
        // Ignore right away. This associated operation has completed.
        return;
      }
      if (responseInfo.getError() != null) {
        chunkException = new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut);
        responseHandler.onRequestResponseException(getRequestInfo.replicaId, new IOException("NetworkClient error"));
        chunkOperationTracker.onResponse(getRequestInfo.replicaId, false);
      } else {
        try {
          GetResponse getResponse = GetResponse
              .readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())), clusterMap);
          if (getResponse.getCorrelationId() != correlationId) {
            // The NetworkClient associates a response with a request based on the fact that only one request is sent
            // out over a connection id, and the response received on a connection id must be for the latest request
            // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
            // There is no other way to handle it.
            chunkException = new RouterException(
                "The correlation id in the GetResponse " + getResponse.getCorrelationId()
                    + " is not the same as the correlation id in the associated GetRequest: " + correlationId,
                RouterErrorCode.UnexpectedInternalError);
            chunkOperationTracker.onResponse(getRequestInfo.replicaId, false);
            // we do not notify the ResponseHandler responsible for failure detection as this is an unexpected error.
          } else {
            processGetBlobResponse(getRequestInfo, getResponse);
          }
        } catch (IOException | MessageFormatException e) {
          // This should really not happen. Again, we do not notify the ResponseHandler responsible for failure
          // detection.
          chunkException = new RouterException("Response deserialization received an unexpected error", e,
              RouterErrorCode.UnexpectedInternalError);
          chunkOperationTracker.onResponse(getRequestInfo.replicaId, false);
        }
      }
      checkAndMaybeComplete();
    }

    /**
     * Process the GetResponse extracted from a {@link ResponseInfo}
     * @param getRequestInfo the associated {@link RequestInfo} for which this response was received.
     * @param getResponse the {@link GetResponse} extracted from the {@link ResponseInfo}
     * @throws IOException if there is an error during deserialization of the GetResponse.
     * @throws MessageFormatException if there is an error during deserialization of the GetResponse.
     */
    private void processGetBlobResponse(GetRequestInfo getRequestInfo, GetResponse getResponse)
        throws IOException, MessageFormatException {
      ServerErrorCode getError = getResponse.getError();
      if (getError == ServerErrorCode.No_Error) {
        int partitionsInResponse = getResponse.getPartitionResponseInfoList().size();
        // Each get request issued by the router is for a single blob.
        if (partitionsInResponse != 1) {
          chunkException = new RouterException("Unexpected number of partition responses, expected: 1, " +
              "received: " + partitionsInResponse, RouterErrorCode.UnexpectedInternalError);
        } else {
          getError = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
          responseHandler.onRequestResponseError(getRequestInfo.replicaId, getError);
          if (getError == ServerErrorCode.No_Error) {
            handleBody(getResponse.getInputStream());
            chunkOperationTracker.onResponse(getRequestInfo.replicaId, true);
          } else {
            // process and set the most relevant exception.
            processServerError(getError);
            if (getError == ServerErrorCode.Blob_Deleted || getError == ServerErrorCode.Blob_Expired) {
              // this is a successful response and one that completes the operation regardless of whether the
              // success target has been reached or not.
              chunkCompleted = true;
            } else {
              chunkOperationTracker.onResponse(getRequestInfo.replicaId, false);
            }
          }
        }
      } else {
        responseHandler.onRequestResponseError(getRequestInfo.replicaId, getError);
        chunkOperationTracker.onResponse(getRequestInfo.replicaId, false);
      }
    }

    /**
     * Process the given {@link ServerErrorCode} and set operation status accordingly.
     * @param errorCode the {@link ServerErrorCode} to process.
     */
    void processServerError(ServerErrorCode errorCode) {
      setChunkException(new RouterException("Server returned: " + errorCode, RouterErrorCode.UnexpectedInternalError));
    }

    /**
     * Set the exception associated with this chunk operation.
     * A {@link ServerErrorCode#Blob_Deleted} or {@link ServerErrorCode#Blob_Expired} error overrides any other
     * previously received exception.
     * @param exception the {@link RouterException} to possibly set.
     */
    void setChunkException(RouterException exception) {
      if (chunkException == null || exception.getErrorCode() == RouterErrorCode.BlobDeleted
          || exception.getErrorCode() == RouterErrorCode.BlobExpired) {
        chunkException = exception;
      }
    }

    /**
     * @return true if this GetChunk is free so a chunk of the overall blob can be assigned to it.
     */
    boolean isFree() {
      return state == ChunkState.Free;
    }

    /**
     * @return true if this GetChunk is assigned with a chunk of the overall blob and is ready to issue requests.
     */
    boolean isReady() {
      return state == ChunkState.Ready;
    }

    /**
     * @return true if the operation on this chunk is in progress.
     */
    boolean isInProgress() {
      return state == ChunkState.InProgress;
    }

    /**
     * @return true if the operation on the current chunk is complete.
     */
    boolean isComplete() {
      return state == ChunkState.Complete;
    }
  }

  /**
   * Special GetChunk used to retrieve and hold the first chunk of a blob. The first chunk is special because it
   * could either be a metadata chunk of a composite blob, or the single chunk of a simple blob,
   * and whether a chunk is composite or simple can only be determined after the first chunk is fetched.
   */
  private class FirstGetChunk extends GetChunk {
    /**
     * Construct a FirstGetChunk and initialize it with the given {@link BlobId}.
     * @param blobId the {@link BlobId} to assign to this chunk. This will be the id of the overall blob.
     */
    FirstGetChunk(BlobId blobId) {
      super(-1, blobId);
    }

    /**
     * {@inheritDoc}
     * <br>
     * Post completion cleanup for the FirstGetChunk only involves clearing the correlation id map (so that
     * subsequent responses are ignored). The rest of the state of the FirstGetChunk is never cleared.
     */
    @Override
    void postCompletionCleanup() {
      correlationIdToGetRequestInfo.clear();
    }

    /**
     * {@inheritDoc}
     * <br>
     * It would help to keep in mind while going through this method that the first chunk is either a metadata chunk
     * or the only chunk of the blob.
     */
    @Override
    void handleBody(InputStream payload)
        throws IOException, MessageFormatException {
      if (!successfullyDeserialized) {
        BlobData blobData = MessageFormatRecord.deserializeBlob(payload);
        BlobType blobType = blobData.getBlobType();
        chunkIndexToBuffer = new TreeMap<>();
        if (blobType == BlobType.MetadataBlob) {
          ByteBuffer serializedMetadataContent =
              ByteBuffer.wrap(Utils.readBytesFromStream(blobData.getStream(), (int) blobData.getSize()));
          List<StoreKey> keys =
              MetadataContentSerDe.deserializeMetadataContentRecord(serializedMetadataContent, blobIdFactory);
          chunkIdIterator = keys.listIterator();
          numChunksTotal = keys.size();
          dataChunks = new GetChunk[Math.min(keys.size(), NonBlockingRouter.MAX_IN_MEM_CHUNKS)];
          for (int i = 0; i < dataChunks.length; i++) {
            dataChunks[i] = new GetChunk(chunkIdIterator.nextIndex(), (BlobId) chunkIdIterator.next());
          }
        } else {
          chunkIdIterator = null;
          numChunksTotal = 1;
          dataChunks = null;
          chunkIndexToBuffer
              .put(0, ByteBuffer.wrap(Utils.readBytesFromStream(blobData.getStream(), (int) blobData.getSize())));
          numChunksRetrieved = 1;
        }
        successfullyDeserialized = true;
        state = ChunkState.Complete;
      } else {
        // Currently, regardless of the successTarget, only the first successful response is honored. Subsequent ones
        // are ignored. If ever in the future, we need some kind of reconciliation, this is the place
        // to do that. (Only after the reconciliation will the state be marked as complete).
      }
    }

    /**
     * {@inheritDoc}
     * <br>
     * Receiving a {@link ServerErrorCode#Blob_Deleted}, {@link ServerErrorCode#Blob_Expired} or
     * {@link ServerErrorCode#Blob_Not_Found} is not unexpected for the first chunk, unlike for subsequent chunks.
     */
    @Override
    void processServerError(ServerErrorCode errorCode) {
      switch (errorCode) {
        case Blob_Deleted:
          logger.trace("Requested blob was deleted");
          setChunkException(new RouterException("Server returned: " + errorCode, RouterErrorCode.BlobDeleted));
          break;
        case Blob_Expired:
          logger.trace("Requested blob is expired");
          setChunkException(new RouterException("Server returned: " + errorCode, RouterErrorCode.BlobExpired));
          break;
        case Blob_Not_Found:
          logger.trace("Requested blob was not found on this server");
          setChunkException(new RouterException("Server returned: " + errorCode, RouterErrorCode.BlobDoesNotExist));
          break;
        default:
          setChunkException(
              new RouterException("Server returned: " + errorCode, RouterErrorCode.UnexpectedInternalError));
          break;
      }
    }
  }

  /**
   * Different states of a GetChunk.
   */
  enum ChunkState {
    /**
     * The GetChunk is free and can be assigned to hold a chunk of an object.
     */
    Free,
    /**
     * The GetChunk has been assigned to get and hold a chunk of an object.
     */
    Ready,
    /**
     * The GetChunk has issued requests and the operation on the chunk it holds is in progress.
     */
    InProgress,
    /**
     * The GetChunk is complete.
     */
    Complete,
  }
}

