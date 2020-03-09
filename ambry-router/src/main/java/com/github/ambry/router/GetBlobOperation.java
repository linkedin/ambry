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
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.CompositeBlobInfo;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
 * As soon as the first data chunk is fetched, the operation callback is invoked (and the future is marked
 * as done) so that the caller can start reading in data. The rest of the chunks are asynchronously fetched and
 * buffered up to the maximum that can be buffered. When fetched chunks are consumed by the caller, subsequent chunks
 * become eligible to be fetched.
 */
class GetBlobOperation extends GetOperation {
  // the callback to use to notify the router about events and state changes
  private final RouterCallback routerCallback;
  // whether the operationCallback has been called already.
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
  // the total size of the object being fetched in this operation
  private long totalSize;
  // a byte range with defined start/end offsets that has been verified to be within the total blob size
  private ByteRange resolvedByteRange;
  // a list iterator to the chunk ids that need to be fetched for this operation, if this is a composite blob.
  private ListIterator<CompositeBlobInfo.ChunkMetadata> chunkIdIterator;
  // chunk index to retrieved chunk buffer mapping.
  private Map<Integer, ByteBuf> chunkIndexToBuf;
  private Map<Integer, ByteBuf> chunkIndexToBufWaitingForRelease;
  // To find the GetChunk to hand over the response quickly.
  private final Map<Integer, GetChunk> correlationIdToGetChunk = new HashMap<>();
  // the blob info that is populated on OperationType.BlobInfo or OperationType.All
  private BlobInfo blobInfo;
  // the ReadableStreamChannel that is populated on OperationType.Blob or OperationType.All requests.
  private BlobDataReadableStreamChannel blobDataChannel;
  // the CompositeBlobInfo that will be set if (and when) this blob turns out to be a composite blob.
  private CompositeBlobInfo compositeBlobInfo;

  private static final Logger logger = LoggerFactory.getLogger(GetBlobOperation.class);

  /**
   * Construct a GetBlobOperation
   * @param routerConfig the {@link RouterConfig} containing the configs for get operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param blobId the {@link BlobId} associated with the operation.
   * @param options the {@link GetBlobOptionsInternal} associated with the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param routerCallback the {@link RouterCallback} to use to complete operations.
   * @param blobIdFactory the factory to use to deserialize keys in a metadata chunk.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the Time instance to use.
   * @param isEncrypted if the encrypted bit is set based on the original blobId string of a {@link BlobId}.
   */
  GetBlobOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, BlobId blobId, GetBlobOptionsInternal options,
      Callback<GetBlobResultInternal> callback, RouterCallback routerCallback, BlobIdFactory blobIdFactory,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time,
      boolean isEncrypted) {
    super(routerConfig, routerMetrics, clusterMap, responseHandler, blobId, options, callback, kms, cryptoService,
        cryptoJobHandler, time, isEncrypted);
    this.routerCallback = routerCallback;
    this.blobIdFactory = blobIdFactory;
    firstChunk = new FirstGetChunk();
  }

  /**
   * Release all the {@link ByteBuf} in the map. Use {@link ConcurrentHashMap#remove(Object)} method to avoid
   * conflict with the release call in the chunk async callback.
   */
  private void releaseResource() {
    if (chunkIndexToBuf == null) {
      return;
    }
    for (Integer key : chunkIndexToBuf.keySet()) {
      ByteBuf byteBuf = chunkIndexToBuf.remove(key);
      if (byteBuf != null) {
        byteBuf.release();
      }
    }
  }

  /**
   * Set the operation to be completed and release all the resources
   */
  private void setOperationCompleted() {
    operationCompleted = true;
    releaseResource();
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
      NonBlockingRouter.completeOperation(null, getOperationCallback, null, abortCause);
    } else {
      setOperationException(abortCause);
      if (blobDataChannel != null && blobDataChannel.isReadCalled()) {
        blobDataChannel.completeRead();
      }
    }
    setOperationCompleted();
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
        Exception e = getOperationException();
        if (options.getChunkIdsOnly) {
          // If this is an operation just to get the chunk ids, then these ids will be returned as part of the
          // result callback and no more chunks will be fetched, so mark the operation as complete to let the
          // GetManager remove this operation.
          setOperationCompleted();
          List<StoreKey> chunkIds = e == null && compositeBlobInfo != null ? compositeBlobInfo.getKeys() : null;
          operationResult = new GetBlobResultInternal(null, chunkIds);
        } else {
          // Complete the operation from the caller's perspective, so that the caller can start reading from the
          // channel if there is no exception. The operation will not be marked as complete internally as subsequent
          // chunk retrievals and channel writes will need to happen and for that, this operation needs the GetManager to
          // poll it periodically. If any exception is encountered while processing subsequent chunks, those will be
          // notified during the channel read.
          long timeElapsed = time.milliseconds() - submissionTimeMs;
          if (isEncrypted) {
            routerMetrics.getEncryptedBlobOperationLatencyMs.update(timeElapsed);
          } else {
            routerMetrics.getBlobOperationLatencyMs.update(timeElapsed);
          }
          if (e == null) {
            // In order to mitigate impact of replication logic that set the size field in BlobProperties incorrectly,
            // we will replace the field with the size from inside of the metadata content.
            if (blobInfo != null) {
              if (blobInfo.getBlobProperties().getBlobSize() != totalSize) {
                if (compositeBlobInfo != null) {
                  routerMetrics.compositeBlobSizeMismatchCount.inc();
                  logger.debug("Blob size mismatch for composite blob: {}", getBlobIdStr());
                } else if (blobInfo.getBlobProperties().isEncrypted()) {
                  routerMetrics.simpleEncryptedBlobSizeMismatchCount.inc();
                  logger.debug("Blob size mismatch for simple encrypted blob: {}", getBlobIdStr());
                } else {
                  routerMetrics.simpleUnencryptedBlobSizeMismatchCount.inc();
                  logger.warn("Blob size mismatch for simple unencrypted blob (should not happen): {}", getBlobIdStr());
                }
                blobInfo.getBlobProperties().setBlobSize(totalSize);
              }
            }
            if (options.getBlobOptions.getOperationType() != GetBlobOptions.OperationType.BlobInfo) {
              blobDataChannel = new BlobDataReadableStreamChannel();
            } else {
              setOperationCompleted();
            }
            operationResult = new GetBlobResultInternal(new GetBlobResult(blobInfo, blobDataChannel), null);
          } else {
            blobDataChannel = null;
            operationResult = null;
            routerMetrics.onGetBlobError(e, options, isEncrypted);
          }
        }
        NonBlockingRouter.completeOperation(null, getOperationCallback, operationResult, e);
      }
    }
    chunk.postCompletionCleanup();
    if (blobDataChannel != null) {
      blobDataChannel.maybeWriteToChannel();
    }
  }

  /**
   * Handle the given {@link ResponseInfo} by handing it over to the appropriate chunk that issued the request.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   * @param getResponse the {@link GetResponse} associated with this response.
   */
  @Override
  void handleResponse(ResponseInfo responseInfo, GetResponse getResponse) {
    responseInfo.touch(blobId);
    GetChunk getChunk = correlationIdToGetChunk.remove(
        ((RequestOrResponse) responseInfo.getRequestInfo().getRequest()).getCorrelationId());
    getChunk.handleResponse(responseInfo, getResponse);
    if (getChunk.isComplete()) {
      onChunkOperationComplete(getChunk);
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
    if (operationException.get() == null) {
      if (firstChunk.isReady() || firstChunk.isInProgress()) {
        firstChunk.poll(requestRegistrationCallback);
      }
      if (firstChunk.isComplete()) {
        onChunkOperationComplete(firstChunk);
        // Although an attempt is made to write to the channel as soon as a chunk is successfully retrieved,
        // the caller might not have called readInto() and passed in a channel at the time. So an attempt is always
        // made from within poll.
        if (blobDataChannel != null) {
          blobDataChannel.maybeWriteToChannel();
        }
        // If this is a composite blob, poll for requests for subsequent chunks.
        if (dataChunks != null) {
          for (GetChunk dataChunk : dataChunks) {
            if (dataChunk.isFree() && chunkIdIterator.hasNext()) {
              dataChunk.initialize(chunkIdIterator.nextIndex(), chunkIdIterator.next());
            }
            if (dataChunk.isInProgress() || (dataChunk.isReady()
                && numChunksRetrieved - blobDataChannel.getNumChunksWrittenOut()
                < routerConfig.routerMaxInMemGetChunks)) {
              dataChunk.poll(requestRegistrationCallback);
              if (dataChunk.isComplete()) {
                onChunkOperationComplete(dataChunk);
                if (operationCompleted) {
                  break;
                }
              }
            }
          }
        }
      }
    }
    if (operationException.get() != null) {
      abort(operationException.get());
    }
  }

  /**
   * @return the {@link OperationTracker} being used by first chunk.
   */
  OperationTracker getFirstChunkOperationTrackerInUse() {
    return firstChunk.getChunkOperationTrackerInUse();
  }

  // ReadableStreamChannel implementation:

  /**
   * A class that implements the result of this GetBlobOperation. This is instantiated if/when the first data chunk of
   * the blob arrives, when the operation callback is invoked.
   */
  private class BlobDataReadableStreamChannel implements ReadableStreamChannel {
    // whether this ReadableStreamChannel is open.
    private AtomicBoolean isOpen = new AtomicBoolean(true);
    // whether readInto() has been called yet by the caller on this ReadableStreamChannel.
    private volatile boolean readCalled = false;
    // The channel to write chunks of the blob into. This will be initialized when the caller calls the readInto().
    private AsyncWritableChannel asyncWritableChannel;
    // the callback to call when all the chunks are successfully written out into the asyncWritableChannel.
    private Callback<Long> readIntoCallback;
    // the future to mark as done when all the chunks are successfully written out into the asyncWritableChannel.
    private FutureResult<Long> readIntoFuture;
    // the number of bytes written out to the asyncWritableChannel. This would be the size of the blob eventually.
    private AtomicLong bytesWritten = new AtomicLong(0);
    // the number of chunks that have been written out to the asyncWritableChannel.
    private AtomicInteger numChunksWrittenOut = new AtomicInteger(0);
    // the index of the next chunk that is to be written out to the asyncWritableChannel.
    private int indexOfNextChunkToWriteOut = 0;
    // whether this object has called the readIntoCallback yet.
    private final AtomicBoolean readIntoCallbackCalled = new AtomicBoolean(false);
    // the callback that is passed into the asyncWritableChannel write() operation.
    private final Callback<Long> chunkAsyncWriteCallback = new Callback<Long>() {
      @Override
      public void onCompletion(Long result, Exception exception) {
        bytesWritten.addAndGet(result);
        if (exception != null) {
          setOperationException(exception);
        }
        int currentNumChunk = numChunksWrittenOut.get();
        ByteBuf byteBuf = chunkIndexToBufWaitingForRelease.remove(currentNumChunk);
        if (byteBuf != null) {
          byteBuf.release();
        }
        numChunksWrittenOut.incrementAndGet();
        routerCallback.onPollReady();
      }
    };

    /**
     * The bytes that will be read from this channel is not known until the read is complete.
     * @return -1
     */
    @Override
    public long getSize() {
      return -1;
    }

    @Override
    public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
      if (!isOpen()) {
        throw new IllegalStateException("This ReadableStreamChannel has been closed");
      }
      if (readCalled) {
        throw new IllegalStateException("Cannot read the result of a GetBlob operation more than once");
      }
      this.asyncWritableChannel = asyncWritableChannel;
      readIntoCallback = callback;
      readIntoFuture = new FutureResult<>();
      readCalled = true;
      if (operationException.get() != null) {
        completeRead();
      }
      routerCallback.onPollReady();
      return readIntoFuture;
    }

    @Override
    public boolean isOpen() {
      return isOpen.get();
    }

    @Override
    public void close() throws IOException {
      if (isOpen.compareAndSet(true, false)) {
        releaseResource();
        if (numChunksWrittenOut.get() != numChunksTotal) {
          setOperationException(new RouterException(
              "The ReadableStreamChannel for blob data has been closed by the user before all chunks were written out.",
              RouterErrorCode.ChannelClosed));
        }
      }
    }

    /**
     * @return whether readInto() has been called yet.
     */
    boolean isReadCalled() {
      return readCalled;
    }

    /**
     * @return the number of chunks that have been written out to the {@link AsyncWritableChannel}
     */
    int getNumChunksWrittenOut() {
      return numChunksWrittenOut.get();
    }

    /**
     * Attempt to write the data associated with the blob to the channel passed in by the caller (if the caller has
     * done so).
     */
    private void maybeWriteToChannel() {
      // if there are chunks available to be written out, do now.
      if (firstChunk.isComplete() && readCalled) {
        while (operationException.get() == null && chunkIndexToBuf.containsKey(indexOfNextChunkToWriteOut)) {
          ByteBuf byteBuf = chunkIndexToBuf.remove(indexOfNextChunkToWriteOut);
          if (byteBuf != null) {
            chunkIndexToBufWaitingForRelease.put(indexOfNextChunkToWriteOut, byteBuf);
            asyncWritableChannel.write(byteBuf, chunkAsyncWriteCallback);
            indexOfNextChunkToWriteOut++;
          }
        }
        if (operationException.get() != null || numChunksWrittenOut.get() == numChunksTotal) {
          completeRead();
        }
      }
    }

    /**
     * Complete the read from this {@link ReadableStreamChannel} by invoking the callback and marking the future.
     */
    void completeRead() {
      if (readIntoCallbackCalled.compareAndSet(false, true)) {
        Exception e = operationException.get();
        readIntoFuture.done(bytesWritten.get(), e);
        if (readIntoCallback != null) {
          readIntoCallback.onCompletion(bytesWritten.get(), e);
        }
        if (e == null) {
          updateChunkingAndSizeMetricsOnSuccessfulGet();
        } else {
          routerMetrics.onGetBlobError(e, options, isEncrypted);
        }
        long totalTime = time.milliseconds() - submissionTimeMs;
        if (isEncrypted) {
          routerMetrics.getEncryptedBlobOperationTotalTimeMs.update(totalTime);
        } else {
          routerMetrics.getBlobOperationTotalTimeMs.update(totalTime);
        }
        releaseResource();
      }
      setOperationCompleted();
    }

    /**
     * Update chunking and size related metrics - blob size, chunk count, and whether the blob is simple or composite.
     */
    private void updateChunkingAndSizeMetricsOnSuccessfulGet() {
      routerMetrics.getBlobSizeBytes.update(bytesWritten.get());
      routerMetrics.getBlobChunkCount.update(numChunksTotal);
      if (options != null) {
        if (options.getBlobOptions.getRange() != null) {
          routerMetrics.getBlobWithRangeSizeBytes.update(bytesWritten.get());
          routerMetrics.getBlobWithRangeTotalBlobSizeBytes.update(totalSize);
        }
        if (options.getBlobOptions.hasBlobSegmentIdx()) {
          routerMetrics.getBlobWithSegmentSizeBytes.update(bytesWritten.get());
          routerMetrics.getBlobWithSegmentTotalBlobSizeBytes.update(totalSize);
        }
      }
      if (!options.getBlobOptions.isRawMode()) {
        if (numChunksTotal == 1) {
          routerMetrics.simpleBlobGetCount.inc();
        } else {
          routerMetrics.compositeBlobGetCount.inc();
        }
      } else {
        routerMetrics.rawBlobGetCount.inc();
      }
    }
  }

  /**
   * GetChunk is used to retrieve and hold a data chunk of a composite blob. An object of this class is initialized
   * to retrieve one data chunk at a time. Once the associated chunk is successfully retrieved, this object can be
   * reinitialized and used to retrieve a subsequent chunk.
   */
  private class GetChunk {
    // the operation tracker used to track the operation on the current chunk.
    private OperationTracker chunkOperationTracker;
    // the blob id of the current chunk.
    private BlobId chunkBlobId;
    // byte offset of the data in the chunk relative to the entire blob
    private long offset;
    // size of the chunk
    private long chunkSize;
    // whether the operation on the current chunk has completed.
    private boolean chunkCompleted;
    // progress tracker used to track whether the operation is completed or not and whether it succeeded or failed on complete
    protected ProgressTracker progressTracker;
    // DecryptCallBackResultInfo that holds all info about decrypt job callback
    protected DecryptCallBackResultInfo decryptCallbackResultInfo;
    // In general, when the operation tracker returns success, any previously saved exceptions are cleared. This flag
    // indicates that the set chunk exception should not be overwritten even when the operation tracker reports success.
    protected boolean retainChunkExceptionOnSuccess;
    // the index of the current chunk in the overall blob.
    protected int chunkIndex;
    // the most relevant exception encountered for the current chunk.
    protected RouterException chunkException;
    // For a GetChunk, responses may be handled multiple times. Regardless of the successTarget,
    // the actual body of the response is deserialized only once.
    protected boolean successfullyDeserialized;
    // map of correlation id to the request metadata for every request issued for this operation.
    protected final Map<Integer, GetRequestInfo> correlationIdToGetRequestInfo = new TreeMap<>();
    // the state of the chunk.
    protected volatile ChunkState state;
    // metrics tracker to track decrypt jobs
    protected CryptoJobMetricsTracker decryptJobMetricsTracker =
        new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics);

    /**
     * Construct a GetChunk
     * @param index the index (in the overall blob) of the initial data chunk that this GetChunk has to fetch.
     * @param chunkMetadata the {@link BlobId}, data content size, and offset relative to the total blob
     *                           of the initial data chunk that this GetChunk has to fetch.
     */
    GetChunk(int index, CompositeBlobInfo.ChunkMetadata chunkMetadata) {
      reset();
      initialize(index, chunkMetadata);
    }

    /**
     * Do what needs to be done after the GetBlobOperation is done with the current chunk that has completed. When
     * this is called, the state of the chunk should be {@link ChunkState#Complete}
     */
    void postCompletionCleanup() {
      reset();
    }

    /**
     * @return the {@link GetOption} to associate with the {@link GetRequest}s that will be issued by this GetChunk.
     */
    GetOption getGetOption() {
      // Anything other than the first GetChunk should ignore Delete and Expired flags. This is to avoid errors due
      // to the blob getting expired or deleted in the middle of a retrieval - after the metadata chunk was
      // successfully retrieved.
      return GetOption.Include_All;
    }

    /**
     * Return the {@link MessageFormatFlags} to associate with a getBlob chunk operation.
     * @return {@link MessageFormatFlags#Blob}
     */
    MessageFormatFlags getOperationFlag() {
      return MessageFormatFlags.Blob;
    }

    /**
     * Reset the state of this GetChunk.
     */
    void reset() {
      chunkOperationTracker = null;
      chunkCompleted = false;
      retainChunkExceptionOnSuccess = false;
      chunkBlobId = null;
      chunkIndex = -1;
      chunkException = null;
      successfullyDeserialized = false;
      progressTracker = null;
      decryptCallbackResultInfo = null;
      decryptJobMetricsTracker = new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics);
      correlationIdToGetRequestInfo.clear();
      state = ChunkState.Free;
    }

    /**
     * Assign a chunk of the overall blob to this GetChunk.
     * @param index the index of the chunk of the overall blob that needs to be fetched through this GetChunk.
     * @param chunkMetadata the id, data content size, and offset (relative to the total size)
     *                           of the chunk of the overall blob that needs to be fetched through this GetChunk.
     */
    void initialize(int index, CompositeBlobInfo.ChunkMetadata chunkMetadata) {
      chunkIndex = index;
      chunkBlobId = (BlobId) chunkMetadata.getStoreKey();
      offset = chunkMetadata.getOffset();
      chunkSize = chunkMetadata.getSize();
      chunkOperationTracker = getOperationTracker(chunkBlobId.getPartition(), chunkBlobId.getDatacenterId(),
          RouterOperation.GetBlobOperation);
      progressTracker = new ProgressTracker(chunkOperationTracker);
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
     * This is one of two main entry points to this class, the other being
     * {@link #handleResponse(ResponseInfo, GetResponse)}.
     * Apart from fetching requests to send out, this also checks for timeouts of issued requests,
     * status of the operation and anything else that needs to be done within this GetChunk. The callers guarantee
     * that this method is called on the GetChunks of an operation until either the operation, or the chunk operation
     * is completed.
     * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
     *                                    created as part of this poll operation.
     */
    void poll(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
      //First, check if any of the existing requests have timed out.
      cleanupExpiredInFlightRequests(requestRegistrationCallback);
      maybeProcessCallbacks();
      checkAndMaybeComplete();
      if (!isComplete()) {
        fetchRequests(requestRegistrationCallback);
      }
    }

    /**
     * Maybe process callbacks if applicable. This is a no-op for blobs that do not need
     * any async processing. As of now, decryption is the only async processing that could happen if applicable.
     */
    protected void maybeProcessCallbacks() {
      if (progressTracker.isCryptoJobRequired() && decryptCallbackResultInfo.decryptJobComplete) {
        logger.trace("Processing decrypt callback stored result for data chunk {}", chunkBlobId);
        decryptJobMetricsTracker.onJobResultProcessingStart();
        // Only when the blob is encrypted should we need to call this method. When finish decryption, we don't need
        // response info anymore.
        if (decryptCallbackResultInfo.exception == null) {
          chunkIndexToBuf.put(chunkIndex,
              filterChunkToRange(Unpooled.wrappedBuffer(decryptCallbackResultInfo.result.getDecryptedBlobContent())));
          numChunksRetrieved++;
          logger.trace("Decrypt result successfully updated for data chunk {}", chunkBlobId);
          progressTracker.setCryptoJobSuccess();
        } else {
          decryptJobMetricsTracker.incrementOperationError();
          logger.trace("Setting operation exception as decryption callback invoked with exception {} for data chunk {}",
              decryptCallbackResultInfo.exception, chunkBlobId);
          setOperationException(buildChunkException("Exception thrown on decrypting the content for data chunk",
              decryptCallbackResultInfo.exception, RouterErrorCode.UnexpectedInternalError));
          progressTracker.setCryptoJobFailed();
        }
        decryptJobMetricsTracker.onJobResultProcessingComplete();
        logger.trace("Marking blob content available to process for data chunk {}", chunkBlobId);
      }
    }

    /**
     * Clean up requests sent out by this operation that have now timed out.
     * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
     */
    private void cleanupExpiredInFlightRequests(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
      //First, check if any of the existing requests have timed out.
      Iterator<Map.Entry<Integer, GetRequestInfo>> inFlightRequestsIterator =
          correlationIdToGetRequestInfo.entrySet().iterator();
      while (inFlightRequestsIterator.hasNext()) {
        Map.Entry<Integer, GetRequestInfo> entry = inFlightRequestsIterator.next();
        int correlationId = entry.getKey();
        GetRequestInfo info = entry.getValue();
        if (time.milliseconds() - info.startTimeMs > routerConfig.routerRequestTimeoutMs) {
          logger.trace("GetBlobRequest with correlationId {} in flight has expired for replica {} ", correlationId,
              info.replicaId.getDataNodeId());
          // Do not notify this as a failure to the response handler, as this timeout could simply be due to
          // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
          // response and the response handler will be notified accordingly.
          onErrorResponse(info.replicaId,
              RouterUtils.buildTimeoutException(correlationId, info.replicaId.getDataNodeId(), chunkBlobId));
          requestRegistrationCallback.registerRequestToDrop(correlationId);
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
        Port port = RouterUtils.getPortToConnectTo(replicaId, routerConfig.routerEnableHttp2NetworkClient);
        GetRequest getRequest = createGetRequest(chunkBlobId, getOperationFlag(), getGetOption());
        RequestInfo request = new RequestInfo(hostname, port, getRequest, replicaId);
        int correlationId = getRequest.getCorrelationId();
        correlationIdToGetRequestInfo.put(correlationId, new GetRequestInfo(replicaId, time.milliseconds()));
        correlationIdToGetChunk.put(correlationId, this);
        requestRegistrationCallback.registerRequestToSend(GetBlobOperation.this, request);
        if (RouterUtils.isRemoteReplica(routerConfig, replicaId)) {
          logger.trace("Making request with correlationId {} to a remote replica {} in {} ", correlationId,
              replicaId.getDataNodeId(), replicaId.getDataNodeId().getDatacenterName());
          routerMetrics.crossColoRequestCount.inc();
        } else {
          logger.trace("Making request with correlationId {} to a local replica {}", correlationId,
              replicaId.getDataNodeId());
        }
        routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getRequestRate.mark();
        state = ChunkState.InProgress;
      }
    }

    /**
     * Check if the operation on the chunk is eligible for completion, if so complete it.
     */
    void checkAndMaybeComplete() {
      if (progressTracker.isDone()) {
        if (progressTracker.hasSucceeded() && !retainChunkExceptionOnSuccess) {
          chunkException = null;
        } else if (chunkOperationTracker.hasFailedOnNotFound()) {
          chunkException =
              buildChunkException("Get Chunk failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist);
        }
        chunkCompleted = true;
      }
      if (chunkCompleted) {
        setOperationException(chunkException);
        state = ChunkState.Complete;
      }
    }

    /**
     * Handle the body of the response: Deserialize and add to the list of chunk buffers.
     * @param payload the body of the response.
     * @param messageMetadata the {@link MessageMetadata} associated with the message.
     * @param messageInfo the {@link MessageInfo} associated with the message.
     * @throws IOException if there is an IOException while deserializing the body.
     * @throws MessageFormatException if there is a MessageFormatException while deserializing the body.
     */
    void handleBody(InputStream payload, MessageMetadata messageMetadata, MessageInfo messageInfo)
        throws IOException, MessageFormatException {
      if (!successfullyDeserialized) {
        BlobData blobData = MessageFormatRecord.deserializeBlob(payload);
        ByteBuffer encryptionKey = messageMetadata == null ? null : messageMetadata.getEncryptionKey();
        ByteBuf chunkBuf = blobData.content();

        try {
          boolean launchedJob = maybeLaunchCryptoJob(chunkBuf, null, encryptionKey, chunkBlobId);
          if (!launchedJob) {
            chunkBuf = filterChunkToRange(chunkBuf);
            chunkIndexToBuf.put(chunkIndex, chunkBuf.retainedDuplicate());
            numChunksRetrieved++;
          }

          successfullyDeserialized = true;
        } finally {
          chunkBuf.release();
        }
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
     * @param getResponse the {@link GetResponse} associated with this response.
     */
    void handleResponse(ResponseInfo responseInfo, GetResponse getResponse) {
      int correlationId = ((GetRequest) responseInfo.getRequestInfo().getRequest()).getCorrelationId();
      // Get the GetOperation that generated the request.
      GetRequestInfo getRequestInfo = correlationIdToGetRequestInfo.remove(correlationId);
      if (getRequestInfo == null) {
        // Ignore right away. This associated operation has completed.
        return;
      }
      long requestLatencyMs = time.milliseconds() - getRequestInfo.startTimeMs;
      routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
      routerMetrics.getDataNodeBasedMetrics(getRequestInfo.replicaId.getDataNodeId()).getRequestLatencyMs.update(
          requestLatencyMs);
      if (responseInfo.getError() != null) {
        // responseInfo.getError() returns NetworkClientErrorCode. If error is not null, it probably means (1) connection
        // checkout timed out; (2) pending connection timed out; (3) established connection timed out. In all these cases,
        // the latency histogram in adaptive operation tracker should not be updated.
        logger.trace("GetBlobRequest with response correlationId {} timed out for replica {} ", correlationId,
            getRequestInfo.replicaId.getDataNodeId());
        onErrorResponse(getRequestInfo.replicaId,
            buildChunkException("Operation timed out", RouterErrorCode.OperationTimedOut));
      } else {
        if (getResponse == null) {
          logger.trace(
              "GetBlobRequest with response correlationId {} received an unexpected error on response deserialization from replica {} ",
              correlationId, getRequestInfo.replicaId.getDataNodeId());
          onErrorResponse(getRequestInfo.replicaId,
              buildChunkException("Response deserialization received an unexpected error",
                  RouterErrorCode.UnexpectedInternalError));
        } else {
          if (getResponse.getCorrelationId() != correlationId) {
            // The NetworkClient associates a response with a request based on the fact that only one request is sent
            // out over a connection id, and the response received on a connection id must be for the latest request
            // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
            // There is no other way to handle it.
            logger.trace("GetBlobRequest with response correlationId {} mismatch from response {} for replica {} ",
                correlationId, getResponse.getCorrelationId(), getRequestInfo.replicaId.getDataNodeId());
            routerMetrics.unknownReplicaResponseError.inc();
            onErrorResponse(getRequestInfo.replicaId, buildChunkException(
                "The correlation id in the GetResponse " + getResponse.getCorrelationId()
                    + " is not the same as the correlation id in the associated GetRequest: " + correlationId,
                RouterErrorCode.UnexpectedInternalError));
            // we do not notify the ResponseHandler responsible for failure detection as this is an unexpected error.
          } else {
            try {
              processGetBlobResponse(getRequestInfo, getResponse);
            } catch (IOException | MessageFormatException e) {
              // This should really not happen. Again, we do not notify the ResponseHandler responsible for failure
              // detection.
              logger.trace(
                  "GetBlobRequest with response correlationId {} response deserialization failed for replica {} ",
                  correlationId, getRequestInfo.replicaId.getDataNodeId());
              routerMetrics.responseDeserializationErrorCount.inc();
              onErrorResponse(getRequestInfo.replicaId,
                  buildChunkException("Response deserialization received an unexpected error", e,
                      RouterErrorCode.UnexpectedInternalError));
            }
          }
        }
      }
      checkAndMaybeComplete();
    }

    /**
     * Launch a crypto job as needed.
     * @param dataBuf to buffer to encrypt or decrypt.
     * @param userMetadata userMetadata of the blob.
     * @param encryptionKey encryption key for the blob. Could be null for non encrypted blob.
     * @param targetBlobId the {@link BlobId} of the blob.
     * @return {@code true} if a crypto job was launched, otherwise {@code false}.
     */
    protected boolean maybeLaunchCryptoJob(ByteBuf dataBuf, byte[] userMetadata, ByteBuffer encryptionKey,
        BlobId targetBlobId) {
      //
      // Three cases to handle:
      // 1) rawMode false and encryptionKey not null => decrypt buffer
      // 2) rawMode true and encryptionKey is null => encrypt buffer (later)
      // 3) other cases => leave buffer as is
      //
      boolean rawMode = options.getBlobOptions.isRawMode();
      if ((encryptionKey == null && !rawMode) || (encryptionKey != null && rawMode)) {
        return false;
      }

      if (encryptionKey != null && !rawMode) {
        logger.trace("Submitting decrypt job for blob {}", targetBlobId);
        long startTimeMs = System.currentTimeMillis();
        decryptCallbackResultInfo = new DecryptCallBackResultInfo();
        progressTracker.initializeCryptoJobTracker(CryptoJobType.DECRYPTION);
        decryptJobMetricsTracker.onJobSubmission();
        cryptoJobHandler.submitJob(new DecryptJob(targetBlobId, encryptionKey, dataBuf.retainedDuplicate(),
            userMetadata != null ? ByteBuffer.wrap(userMetadata) : null, cryptoService, kms, decryptJobMetricsTracker,
            (DecryptJob.DecryptJobResult result, Exception exception) -> {
              routerMetrics.decryptTimeMs.update(System.currentTimeMillis() - startTimeMs);
              decryptJobMetricsTracker.onJobCallbackProcessingStart();
              logger.trace("Handling decrypt job call back for blob {} to set decrypt callback results", targetBlobId);
              decryptCallbackResultInfo.setResultAndException(result, exception);
              routerCallback.onPollReady();
              decryptJobMetricsTracker.onJobCallbackProcessingComplete();
            }));
        return true;
      } else {
        // encryptionKey == null && rawMode
        // TODO: encrypt buffer if caller wants it
        return false;
      }
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
          onErrorResponse(getRequestInfo.replicaId, buildChunkException(
              "Unexpected number of partition responses, expected: 1, " + "received: " + partitionsInResponse,
              RouterErrorCode.UnexpectedInternalError));
        } else {
          getError = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
          if (getError == ServerErrorCode.No_Error) {
            PartitionResponseInfo partitionResponseInfo = getResponse.getPartitionResponseInfoList().get(0);
            int objectsInPartitionResponse = partitionResponseInfo.getMessageInfoList().size();
            if (objectsInPartitionResponse != 1) {
              onErrorResponse(getRequestInfo.replicaId, buildChunkException(
                  "Unexpected number of messages in a partition response, expected: 1, " + "received: "
                      + objectsInPartitionResponse, RouterErrorCode.UnexpectedInternalError));
            } else {
              MessageMetadata messageMetadata = partitionResponseInfo.getMessageMetadataList().get(0);
              MessageInfo messageInfo = partitionResponseInfo.getMessageInfoList().get(0);
              handleBody(getResponse.getInputStream(), messageMetadata, messageInfo);
              chunkOperationTracker.onResponse(getRequestInfo.replicaId, TrackedRequestFinalState.SUCCESS);
              if (RouterUtils.isRemoteReplica(routerConfig, getRequestInfo.replicaId)) {
                logger.trace("Cross colo request successful for remote replica in {} ",
                    getRequestInfo.replicaId.getDataNodeId().getDatacenterName());
                routerMetrics.crossColoSuccessCount.inc();
              }
            }
          } else {
            // process and set the most relevant exception.
            RouterErrorCode routerErrorCode = processServerError(getError);
            if (getError == ServerErrorCode.Disk_Unavailable) {
              chunkOperationTracker.onResponse(getRequestInfo.replicaId, TrackedRequestFinalState.DISK_DOWN);
              setChunkException(buildChunkException("Server returned: " + getError, routerErrorCode));
              routerMetrics.routerRequestErrorCount.inc();
              routerMetrics.getDataNodeBasedMetrics(
                  getRequestInfo.replicaId.getDataNodeId()).getRequestErrorCount.inc();
            } else {
              if (getError == ServerErrorCode.Blob_Deleted || getError == ServerErrorCode.Blob_Expired
                  || getError == ServerErrorCode.Blob_Authorization_Failure) {
                // this is a successful response and one that completes the operation regardless of whether the
                // success target has been reached or not.
                chunkCompleted = true;
                chunkException = buildChunkException("Server returned: " + getError, routerErrorCode);
              }
              // any server error code that is not equal to ServerErrorCode.No_Error, the onErrorResponse should be invoked
              // because the operation itself doesn't succeed although the response in some cases is successful (i.e. Blob_Deleted)
              onErrorResponse(getRequestInfo.replicaId,
                  buildChunkException("Server returned: " + getError, routerErrorCode));
            }
          }
        }
      } else {
        logger.trace("Replica {} returned an error {} for a GetBlobRequest with response correlationId : {} ",
            getRequestInfo.replicaId.getDataNodeId(), getError, getResponse.getCorrelationId());
        // process and set the most relevant exception.
        onErrorResponse(getRequestInfo.replicaId, buildChunkException("Server returned", processServerError(getError)));
      }
    }

    /**
     * Perform the necessary actions when a request to a replica fails.
     * @param replicaId the {@link ReplicaId} associated with the failed response.
     * @param exception the {@link RouterException} associated with the failed response.
     */
    private void onErrorResponse(ReplicaId replicaId, RouterException exception) {
      chunkOperationTracker.onResponse(replicaId,
          TrackedRequestFinalState.fromRouterErrorCodeToFinalState(exception.getErrorCode()));
      setChunkException(exception);
      routerMetrics.routerRequestErrorCount.inc();
      routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getRequestErrorCount.inc();
    }

    /**
     * Process the given {@link ServerErrorCode} and set operation status accordingly.
     * Receiving a {@link ServerErrorCode#Blob_Deleted}, {@link ServerErrorCode#Blob_Expired} or
     * {@link ServerErrorCode#Blob_Not_Found} is unexpected for all chunks except for the first.
     * @param errorCode the {@link ServerErrorCode} to process.
     * @return the {@link RouterErrorCode} mapped from input server error code.
     */
    RouterErrorCode processServerError(ServerErrorCode errorCode) {
      return RouterErrorCode.UnexpectedInternalError;
    }

    /**
     * Set the exception associated with this chunk operation.
     * First, if the current chunkException is null, directly set it as provided exception;
     * Second, if the chunkException exists but the precedence level of the provided exception's error code is smaller
     * than the precedence level of the chunkException's error code, then update the chunkException.
     * @param exception the {@link RouterException} to possibly set.
     */
    void setChunkException(RouterException exception) {
      if (chunkException == null || getPrecedenceLevel(exception.getErrorCode()) < getPrecedenceLevel(
          chunkException.getErrorCode())) {
        chunkException = exception;
      }
    }

    /**
     * Slice this chunk's data to only include the bytes within the operation's specified byte range.
     * @param buf the {@link ByteBuf} representing the content of this chunk.
     * @return A {@link ByteBuf} that only includes bytes within the operation's specified byte range.
     */
    protected ByteBuf filterChunkToRange(ByteBuf buf) {
      if (options.getBlobOptions.getRange() == null) {
        return buf;
      }
      if (resolvedByteRange.getRangeSize() == 0) {
        buf.clear();
      } else {
        long relativeOffset = offset;
        if (options.getBlobOptions.hasBlobSegmentIdx()) {
          relativeOffset = 0;
        }
        long startOffsetInThisChunk = chunkIndex == 0 ? resolvedByteRange.getStartOffset() - relativeOffset : 0;
        long endOffsetInThisChunkExclusive =
            chunkIndex == (numChunksTotal - 1) ? resolvedByteRange.getEndOffset() - relativeOffset + 1 : chunkSize;
        buf.setIndex(buf.readerIndex() + (int) startOffsetInThisChunk,
            buf.readerIndex() + (int) endOffsetInThisChunkExclusive);
      }
      return buf;
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

    /**
     * @return {@link OperationTracker} being used by this chunk.
     */
    OperationTracker getChunkOperationTrackerInUse() {
      return chunkOperationTracker;
    }

    /**
     * @param message the message to include in the exception.
     * @param cause the cause of the exception.
     * @param errorCode the error code to use.
     * @return a {@link RouterException} with additional info about the chunk that failed.
     */
    private RouterException buildChunkException(String message, Throwable cause, RouterErrorCode errorCode) {
      return new RouterException(message + ". Chunk ID: " + chunkBlobId, cause, errorCode);
    }

    /**
     * @param message the message to include in the exception.
     * @param errorCode the error code to use.
     * @return a {@link RouterException} with additional info about the chunk that failed.
     */
    private RouterException buildChunkException(String message, RouterErrorCode errorCode) {
      return new RouterException(message + ". Chunk ID: " + chunkBlobId, errorCode);
    }
  }

  /**
   * Special GetChunk used to retrieve and hold the first chunk of a blob. The first chunk is special because it
   * could either be a metadata chunk of a composite blob, or the single chunk of a simple blob,
   * and whether a chunk is composite or simple can only be determined after the first chunk is fetched.
   */
  private class FirstGetChunk extends GetChunk {

    // refers to the blob type.
    private BlobType blobType;
    private List<CompositeBlobInfo.ChunkMetadata> chunkMetadataList;
    private BlobProperties serverBlobProperties;

    /**
     * Construct a FirstGetChunk and initialize it with the {@link BlobId} of the overall operation.
     */
    FirstGetChunk() {
      super(-1, new CompositeBlobInfo.ChunkMetadata(blobId, 0L, -1L));
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

    @Override
    GetOption getGetOption() {
      return options.getBlobOptions.getGetOption();
    }

    /**
     * Return the {@link MessageFormatFlags} to associate with the first getBlob chunk operation.
     * @return {@link MessageFormatFlags#Blob} for {@link GetBlobOptions.OperationType#Data}, or
     *         {@link MessageFormatFlags#All} by default.
     */
    @Override
    MessageFormatFlags getOperationFlag() {
      return options.getBlobOptions.getOperationType() == GetBlobOptions.OperationType.Data ? MessageFormatFlags.Blob
          : MessageFormatFlags.All;
    }

    @Override
    protected void maybeProcessCallbacks() {
      if (progressTracker.isCryptoJobRequired() && decryptCallbackResultInfo.decryptJobComplete) {
        decryptJobMetricsTracker.onJobResultProcessingStart();
        if (decryptCallbackResultInfo.exception != null) {
          decryptJobMetricsTracker.incrementOperationError();
          logger.trace("Decryption job callback invoked with exception for {} blob {} ", blobType, blobId,
              decryptCallbackResultInfo.exception);
          setOperationException(
              new RouterException("Exception thrown on decrypting content for " + blobType + " blob " + blobId,
                  decryptCallbackResultInfo.exception, RouterErrorCode.UnexpectedInternalError));
          progressTracker.setCryptoJobFailed();
        } else {
          // in case of Metadata blob, only user-metadata needs decryption if the blob is encrypted
          if (blobType == BlobType.MetadataBlob) {
            logger.trace("Processing stored decryption callback result for Metadata blob {}", blobId);
            initializeDataChunks();
            blobInfo =
                new BlobInfo(serverBlobProperties, decryptCallbackResultInfo.result.getDecryptedUserMetadata().array());
            progressTracker.setCryptoJobSuccess();
            logger.trace("BlobContent available to process for Metadata blob {}", blobId);
          } else {
            logger.trace("Processing stored decryption callback result for simple blob {}", blobId);
            // In case of simple blobs, user-metadata may or may not be passed into decryption job based on GetOptions flag.
            // Only in-case of GetBlobInfo and GetBlobAll, user-metadata is required to be decrypted
            if (decryptCallbackResultInfo.result.getDecryptedUserMetadata() != null) {
              blobInfo = new BlobInfo(serverBlobProperties,
                  decryptCallbackResultInfo.result.getDecryptedUserMetadata().array());
            }
            totalSize = decryptCallbackResultInfo.result.getDecryptedBlobContent().remaining();
            if (!resolveRange(totalSize)) {
              chunkIndexToBuf.put(0, filterChunkToRange(
                  Unpooled.wrappedBuffer(decryptCallbackResultInfo.result.getDecryptedBlobContent())));
              numChunksRetrieved = 1;
              progressTracker.setCryptoJobSuccess();
              logger.trace("BlobContent available to process for simple blob {}", blobId);
            } else {
              progressTracker.setCryptoJobFailed();
            }
          }
        }
        decryptJobMetricsTracker.onJobResultProcessingComplete();
      }
    }

    /**
     * {@inheritDoc}
     * <br>
     * It would help to keep in mind while going through this method that the first chunk is either a metadata chunk
     * or the only chunk of the blob.
     */
    @Override
    void handleBody(InputStream payload, MessageMetadata messageMetadata, MessageInfo messageInfo)
        throws IOException, MessageFormatException {
      if (!successfullyDeserialized) {
        BlobData blobData;
        ByteBuffer encryptionKey;
        byte[] userMetadata = null;

        ByteBuffer rawPayloadBuffer = null;
        boolean rawMode = options.getBlobOptions.isRawMode();
        if (rawMode) {
          if (messageInfo.getSize() > Integer.MAX_VALUE) {
            throw new IllegalStateException("Payload too large for blob " + blobId.getID());
          }
          // TODO: avoid extra copy using InputStreamReadableStreamChannel
          byte[] payloadBytes = new byte[(int) messageInfo.getSize()];
          payload.read(payloadBytes);
          payload = new ByteArrayInputStream(payloadBytes);
          rawPayloadBuffer = ByteBuffer.wrap(payloadBytes);
        }

        if (getOperationFlag() == MessageFormatFlags.Blob) {
          blobData = MessageFormatRecord.deserializeBlob(payload);
          encryptionKey = messageMetadata == null ? null : messageMetadata.getEncryptionKey();
        } else {
          BlobAll blobAll = MessageFormatRecord.deserializeBlobAll(payload, blobIdFactory);
          BlobInfo serverBlobInfo = blobAll.getBlobInfo();
          updateTtlIfRequired(serverBlobInfo.getBlobProperties(), messageInfo);
          getOptions().ageAtAccessTracker.trackAgeAtAccess(serverBlobInfo.getBlobProperties().getCreationTimeInMs());
          blobData = blobAll.getBlobData();
          encryptionKey = blobAll.getBlobEncryptionKey();
          if (encryptionKey == null) {
            // set blobInfo only if decryption is not required. If not, maybeProcessCallbackAndComplete() will set appropriate
            // value
            blobInfo = serverBlobInfo;
          } else {
            serverBlobProperties = serverBlobInfo.getBlobProperties();
            userMetadata = serverBlobInfo.getUserMetadata();
          }
        }
        blobType = blobData.getBlobType();
        chunkIndexToBuf = new ConcurrentHashMap<>();
        chunkIndexToBufWaitingForRelease = new ConcurrentHashMap<>();
        if (rawMode) {
          if (blobData != null) {
            // RawMode, release blob data.
            blobData.release();
          }
          // Return the raw bytes from storage
          if (encryptionKey != null) {
            chunkIdIterator = null;
            dataChunks = null;
            chunkIndex = 0;
            numChunksTotal = 1;
            chunkIndexToBuf.put(0, Unpooled.wrappedBuffer(rawPayloadBuffer));
            numChunksRetrieved = 1;
          } else {
            setOperationException(new IllegalStateException("Only encrypted blobs supported in raw mode"));
          }
        } else {
          if (blobType == BlobType.MetadataBlob) {
            handleMetadataBlob(blobData, userMetadata, encryptionKey);
          } else {
            handleSimpleBlob(blobData, userMetadata, encryptionKey);
          }
        }
        successfullyDeserialized = true;
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
    RouterErrorCode processServerError(ServerErrorCode errorCode) {
      logger.trace("Server returned an error: {} ", errorCode);
      RouterErrorCode resolvedRouterErrorCode;
      switch (errorCode) {
        case Blob_Authorization_Failure:
          resolvedRouterErrorCode = RouterErrorCode.BlobAuthorizationFailure;
          break;
        case Blob_Deleted:
          resolvedRouterErrorCode = RouterErrorCode.BlobDeleted;
          break;
        case Blob_Expired:
          resolvedRouterErrorCode = RouterErrorCode.BlobExpired;
          break;
        case Blob_Not_Found:
          resolvedRouterErrorCode = RouterErrorCode.BlobDoesNotExist;
          break;
        case Disk_Unavailable:
        case Replica_Unavailable:
          resolvedRouterErrorCode = RouterErrorCode.AmbryUnavailable;
          break;
        default:
          resolvedRouterErrorCode = RouterErrorCode.UnexpectedInternalError;
      }
      return resolvedRouterErrorCode;
    }

    /**
     * Process a metadata blob to find the data chunks that need to be fetched.
     * @param blobData the metadata blob's data.
     * @param userMetadata userMetadata of the blob
     * @param encryptionKey blob encryption key. Could be null for un-encrypted blobs
     * @throws IOException
     * @throws MessageFormatException
     */
    private void handleMetadataBlob(BlobData blobData, byte[] userMetadata, ByteBuffer encryptionKey)
        throws IOException, MessageFormatException {
      ByteBuf serializedMetadataContent = blobData.content();
      try {
        compositeBlobInfo =
            MetadataContentSerDe.deserializeMetadataContentRecord(serializedMetadataContent.nioBuffer(), blobIdFactory);
        totalSize = compositeBlobInfo.getTotalSize();
        chunkMetadataList = compositeBlobInfo.getChunkMetadataList();
        boolean rangeResolutionFailure = false;
        try {
          long rangeTotalSize = totalSize;
          if (options.getBlobOptions.hasBlobSegmentIdx()) {
            int requestedSegment = options.getBlobOptions.getBlobSegmentIdx();
            if (requestedSegment < 0 || requestedSegment >= chunkMetadataList.size()) {
              throw new IllegalArgumentException(
                  "Bad segment number: " + requestedSegment + ", num of keys: " + chunkMetadataList.size());
            }
            chunkMetadataList = chunkMetadataList.subList(requestedSegment, requestedSegment + 1);
            rangeTotalSize = chunkMetadataList.get(0).getSize();
          }
          if (options.getBlobOptions.getRange() != null) {
            resolvedByteRange = options.getBlobOptions.getRange().toResolvedByteRange(rangeTotalSize);
            // Get only the chunks within the range.
            if (!options.getBlobOptions.hasBlobSegmentIdx()) {
              chunkMetadataList = compositeBlobInfo.getStoreKeysInByteRange(resolvedByteRange.getStartOffset(),
                  resolvedByteRange.getEndOffset());
            }
          }
        } catch (IllegalArgumentException e) {
          onInvalidRange(e);
          rangeResolutionFailure = true;
        }
        if (!rangeResolutionFailure) {
          if (options.getChunkIdsOnly || getOperationFlag() == MessageFormatFlags.Blob || encryptionKey == null) {
            initializeDataChunks();
          } else {
            // if blob is encrypted, then decryption is required only in case of GetBlobInfo and GetBlobAll (since user-metadata
            // is expected to be encrypted). Incase of GetBlob, Metadata blob does not need any decryption even if BlobProperties says so
            decryptCallbackResultInfo = new DecryptCallBackResultInfo();
            progressTracker.initializeCryptoJobTracker(CryptoJobType.DECRYPTION);
            decryptJobMetricsTracker.onJobSubmission();
            logger.trace("Submitting decrypt job for Metadata chunk {}", blobId);
            long startTimeMs = System.currentTimeMillis();
            cryptoJobHandler.submitJob(
                new DecryptJob(blobId, encryptionKey, null, ByteBuffer.wrap(userMetadata), cryptoService, kms,
                    decryptJobMetricsTracker, (DecryptJob.DecryptJobResult result, Exception exception) -> {
                  routerMetrics.decryptTimeMs.update(System.currentTimeMillis() - startTimeMs);
                  decryptJobMetricsTracker.onJobCallbackProcessingStart();
                  logger.trace("Handling decrypt job call back for Metadata chunk {} to set decrypt callback results",
                      blobId);
                  decryptCallbackResultInfo.setResultAndException(result, exception);
                  routerCallback.onPollReady();
                  decryptJobMetricsTracker.onJobCallbackProcessingComplete();
                }));
          }
        }
      } finally {
        serializedMetadataContent.release();
      }
    }

    /**
     * Initialize data chunks and few other cast for metadata chunk
     */
    private void initializeDataChunks() {
      if (options.getChunkIdsOnly
          || options.getBlobOptions.getOperationType() == GetBlobOptions.OperationType.BlobInfo) {
        chunkIdIterator = null;
        numChunksTotal = 0;
        dataChunks = null;
      } else {
        chunkIdIterator = chunkMetadataList.listIterator();
        numChunksTotal = chunkMetadataList.size();
        dataChunks = new GetChunk[Math.min(chunkMetadataList.size(), routerConfig.routerMaxInMemGetChunks)];
        for (int i = 0; i < dataChunks.length; i++) {
          int idx = chunkIdIterator.nextIndex();
          CompositeBlobInfo.ChunkMetadata keyAndOffset = chunkIdIterator.next();
          dataChunks[i] = new GetChunk(idx, keyAndOffset);
        }
      }
    }

    /**
     * Process a simple blob and extract the requested data from the blob.
     * @param blobData the simple blob's data
     * @param userMetadata userMetadata of the blob
     * @param encryptionKey encryption key for the blob. Could be null for non encrypted blob.
     */
    private void handleSimpleBlob(BlobData blobData, byte[] userMetadata, ByteBuffer encryptionKey) {
      try {
        ByteBuf chunkBuf = blobData.content();
        boolean rangeResolutionFailure = false;
        if (encryptionKey == null) {
          totalSize = blobData.getSize();
          rangeResolutionFailure = resolveRange(totalSize);
        } else {
          // for encrypted blobs, Blob data will not have the right size. Will have to wait until decryption is complete
        }
        if (!rangeResolutionFailure) {
          chunkIdIterator = null;
          numChunksTotal = 0;
          dataChunks = null;
          if (!options.getChunkIdsOnly) {
            chunkIndex = 0;
            numChunksTotal = 1;
            boolean launchedJob = maybeLaunchCryptoJob(chunkBuf, userMetadata, encryptionKey, blobId);
            if (!launchedJob) {
              chunkBuf = filterChunkToRange(chunkBuf);
              chunkIndexToBuf.put(0, chunkBuf.retainedDuplicate());
              numChunksRetrieved = 1;
            }
          }
        }
      } finally {
        blobData.release();
      }
    }

    /**
     * Resolves range to be served
     * @param totalSize total size of the chunk
     * @return {@code true} if range resolution failed. {@code true} otherwise
     */
    private boolean resolveRange(long totalSize) {
      boolean failure = false;
      try {
        if (options != null && options.getBlobOptions.getRange() != null) {
          resolvedByteRange = options.getBlobOptions.getRange().toResolvedByteRange(totalSize);
        }
      } catch (IllegalArgumentException e) {
        onInvalidRange(e);
        failure = true;
      }
      return failure;
    }

    /**
     * On an invalid range, set a {@link RouterErrorCode#RangeNotSatisfiable} exception for this chunk, mark the chunk
     * as unconditionally completed, and set the chunk counters such that the operation will be completed.
     * @param exception the reason that the range was invalid.
     */
    private void onInvalidRange(Exception exception) {
      setChunkException(
          new RouterException("Range provided was not satisfiable.", exception, RouterErrorCode.RangeNotSatisfiable));
      retainChunkExceptionOnSuccess = true;
      chunkIdIterator = null;
      dataChunks = null;
      numChunksTotal = 0;
      numChunksRetrieved = 0;
    }
  }

  /**
   * Different states of a GetChunk.
   */
  enum ChunkState {
    /**
     * The GetChunk is free and can be assigned to hold a chunk of the overall blob.
     */
    Free,

    /**
     * The GetChunk has been assigned to get and hold a chunk of the overall blob.
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

/**
 * Holds info about decryption job callback
 */
class DecryptCallBackResultInfo {
  boolean decryptJobComplete;
  Exception exception;
  DecryptJob.DecryptJobResult result;

  /**
   * Sets the result and exception from decrypt job callback
   * @param result {@link DecryptJob.DecryptJobResult} from the decrypt job callback. Could be null on failure.
   * @param exception {@link Exception} from the decrypt job callback. Could be null if the decrypt job succeeded.
   */
  void setResultAndException(DecryptJob.DecryptJobResult result, Exception exception) {
    this.result = result;
    this.exception = exception;
    this.decryptJobComplete = true;
  }
}
