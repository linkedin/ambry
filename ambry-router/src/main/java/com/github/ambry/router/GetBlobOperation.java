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
import com.github.ambry.commons.AmbryCache;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.Callback;
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
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
  private static final Logger logger = LoggerFactory.getLogger(GetBlobOperation.class);
  // the callback to use to notify the router about events and state changes
  private final RouterCallback routerCallback;
  // whether the operationCallback has been called already.
  private final AtomicBoolean operationCallbackInvoked = new AtomicBoolean(false);
  // The first chunk may be a metadata chunk if the blob is composite, or the only data chunk if the blob is simple.
  private final FirstGetChunk firstChunk;
  // the factory to use to deserialize keys in a metadata chunk.
  private final BlobIdFactory blobIdFactory;
  // To find the GetChunk to hand over the response quickly.
  private final Map<Integer, GetChunk> correlationIdToGetChunk = new HashMap<>();
  // Callback to charge against quota for each chunk that's fetched.
  private final QuotaChargeCallback quotaChargeCallback;
  // Associated with all data chunks in the case of composite blobs. Only a fixed number of these are initialized.
  // Each of these is initialized with the information required to fetch a data chunk and is responsible for
  // retrieving and adding it to the list of chunk buffers. Once complete, they are reused to fetch subsequent data
  // chunks.
  private GetChunk[] dataChunks;
  // the total number of data chunks associated with this blob.
  private int numChunksTotal;
  // the total number of data chunks retrieved so far (and may or may not have been written out yet).
  private final AtomicInteger numChunksRetrieved = new AtomicInteger();
  // the total size of the object being fetched in this operation
  private long totalSize;
  // a byte range with defined start/end offsets that has been verified to be within the total blob size
  private ByteRange resolvedByteRange;
  // a list iterator to the chunk ids that need to be fetched for this operation, if this is a composite blob.
  private ListIterator<CompositeBlobInfo.ChunkMetadata> chunkIdIterator;
  // chunk index to retrieved chunk buffer mapping.
  private Map<Integer, ByteBuf> chunkIndexToBuf;
  private Map<Integer, ByteBuf> chunkIndexToBufWaitingForRelease;
  // the blob info that is populated on OperationType.BlobInfo or OperationType.All
  private BlobInfo blobInfo;
  // the ReadableStreamChannel that is populated on OperationType.Blob or OperationType.All requests.
  private BlobDataReadableStreamChannel blobDataChannel;
  // the CompositeBlobInfo that will be set if (and when) this blob turns out to be a composite blob.
  private CompositeBlobInfo compositeBlobInfo;
  // A cache for blob metadata of composite blobs
  private AmbryCache blobMetadataCache;
  private NonBlockingRouter nonBlockingRouter;
  // A shared instance of decompression service.
  private final CompressionService decompressionService;

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
   * @param blobMetadataCache A cache to save blob metadata for composite blobs
   * @param nonBlockingRouter The non-blocking router object
   * @param compressionService The compression service to use.
   */
  GetBlobOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, BlobId blobId, GetBlobOptionsInternal options, Callback<GetBlobResult> callback,
      RouterCallback routerCallback, BlobIdFactory blobIdFactory, KeyManagementService kms, CryptoService cryptoService,
      CryptoJobHandler cryptoJobHandler, Time time, boolean isEncrypted, QuotaChargeCallback quotaChargeCallback,
      AmbryCache blobMetadataCache, NonBlockingRouter nonBlockingRouter, CompressionService compressionService) {
    super(routerConfig, routerMetrics, clusterMap, responseHandler, blobId, options, callback, kms, cryptoService,
        cryptoJobHandler, time, isEncrypted);
    this.routerCallback = routerCallback;
    this.blobIdFactory = blobIdFactory;
    this.quotaChargeCallback = quotaChargeCallback;
    this.blobMetadataCache = blobMetadataCache;
    BlobMetadata blobMetadata =
        shouldLookupMetadataCache() ? (BlobMetadata) blobMetadataCache.getObject(blobId.toString()) : null;
    firstChunk = (blobMetadata == null) ? new FirstGetChunk() : new CachedFirstChunk(blobMetadata);
    this.nonBlockingRouter = nonBlockingRouter;
    this.decompressionService = compressionService;
  }

  /**
   * Returns whether the current request is a range request or not.
   * @return True if range request, else False.
   */
  boolean isRangeRequest() {
    return options.getBlobOptions.getRange() != null;
  }

  /**
   * Decides if we should look up the metadata cache during a GET request
   * @return True if and only if
   *  - cache exists
   *  - it is a RANGE request
   *  - it is not a raw mode request
   *  - it is not a GET request for a segment
   *  - the blobID is for a metadata blob (this info is present only in v5 and v6 version of blobID)
   *  False, otherwise.
   */
  boolean shouldLookupMetadataCache() {
    return (blobMetadataCache != null && isRangeRequest() && options.getBlobOptions.hasBlobSegmentIdx() == false
        && options.getBlobOptions.isRawMode() == false && blobId.getBlobDataType() == BlobId.BlobDataType.METADATA);
  }

  /**
   * This helper decides if metadata must be deleted from cache or not.
   * This mainly reduces the impact on metadata cache due to a large number of GET requests for
   * simple blobs that do not exist.
   * @param abortCause Reason to abort GetBlobOperation
   * @return True if metadata must be deleted, false otherwise.
   */
  boolean shouldDeleteMetadata(Exception abortCause) {
    if (abortCause != null && abortCause instanceof RouterException) {
      switch (((RouterException) abortCause).getErrorCode()) {
        /* If the blob is not found, then delete its metadata from frontend cache. */
        case BlobDoesNotExist:
        case BlobDeleted:
        case BlobExpired:
          return (blobMetadataCache != null && blobId.getBlobDataType() == BlobId.BlobDataType.METADATA);
      }
    }
    return false;
  }

  /**
   * Conditionally saves blob metadata for composite blobs
   * @return True if metadata was saved successfully, else False.
   */
  boolean saveMetadata() {
    boolean putResult = false;
    if (firstChunk.shouldSaveMetadata()) {
      BlobMetadata blobMetadata = new BlobMetadata(blobId.toString(), blobInfo, compositeBlobInfo);
      putResult = blobMetadataCache.putObject(blobMetadata.getBlobId(), blobMetadata);
      logger.debug("[{}] Issued save-metadata for blobId = {}, result = {}", blobMetadata.getBlobId(), blobId,
          putResult);
    }
    return putResult;
  }

  /**
   * Unconditionally deletes blob metadata for composite blobs.
   * @param reason String describing reason for deletion, useful for triaging.
   * @return True if metadata was deleted successfully, else False.
   */
  boolean deleteMetadata(String reason) {
    if (blobMetadataCache == null) {
      return false;
    }
    boolean deleteResult = blobMetadataCache.deleteObject(blobId.toString());
    logger.debug("[{}] Issued delete-metadata for blobId = {}, reason = {}, result = {}",
        blobMetadataCache.getCacheId(), blobId, reason, deleteResult);
    return deleteResult;
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
        ReferenceCountUtil.safeRelease(byteBuf);
      }
    }
    firstChunk.maybeReleaseDecryptionResultBuffer();
    if (dataChunks != null) {
      for (GetChunk getChunk : dataChunks) {
        getChunk.maybeReleaseDecryptionResultBuffer();
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
      nonBlockingRouter.completeOperation(null, getOperationCallback, null, abortCause);
    } else {
      setOperationException(abortCause);
      if (blobDataChannel != null && blobDataChannel.isReadCalled()) {
        blobDataChannel.completeRead();
      }
    }
    if (shouldDeleteMetadata(abortCause)) {
      deleteMetadata(((RouterException) abortCause).getErrorCode().toString());
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
    Exception e = getOperationException();
    if (chunk == firstChunk) {
      if (operationCallbackInvoked.compareAndSet(false, true)) {
        if (chunk.chunkBlobId.getBlobDataType() == BlobId.BlobDataType.METADATA) {
          /* Save metadata on successful completion. Everything is decrypted at this point. */
          saveMetadata();
          routerMetrics.getMetadataChunkLatencyMs.update(time.milliseconds() - chunk.initializedTimeMs);
        }
        if (options.getChunkIdsOnly) {
          // If this is an operation just to get the chunk ids, then these ids will be returned as part of the
          // result callback and no more chunks will be fetched, so mark the operation as complete to let the
          // GetManager remove this operation.
          setOperationCompleted();
          List<StoreKey> chunkIds = e == null && compositeBlobInfo != null ? compositeBlobInfo.getKeys() : null;
          operationResult = new GetBlobResult(blobInfo, null, chunkIds);
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
            if (blobInfo != null) {
              // For segmented blob we have already filtered out not-required chunks by now. So the first chunk is the segment we need.
              fixBlobSizeIfRequired(blobInfo.getBlobProperties().getBlobSize(),
                  options.getBlobOptions.hasBlobSegmentIdx() ? firstChunk.getChunkMetadataList().get(0).getSize()
                      : totalSize);
            }
            if (options.getBlobOptions.getOperationType() != GetBlobOptions.OperationType.BlobInfo) {
              blobDataChannel = new BlobDataReadableStreamChannel();
            } else {
              setOperationCompleted();
            }
            operationResult = new GetBlobResult(blobInfo, blobDataChannel, null);
          } else {
            blobDataChannel = null;
            operationResult = null;
            routerMetrics.onGetBlobError(e, options, isEncrypted);
          }
        }
        nonBlockingRouter.completeOperation(null, getOperationCallback, operationResult, e);
      }
    }
    chunk.postCompletionCleanup();
    if (blobDataChannel != null) {
      blobDataChannel.maybeWriteToChannel();
    }
  }

  /**
   * In order to mitigate impact of replication logic that set the size field in BlobProperties incorrectly,
   * and in case of GET for segment of a blob, replace the field with the size from inside of the metadata content.
   * @param blobSizeFromProperties blob size obtained from blob properties.
   * @param blobSizeFromMetadata blob size obtained from metadata of the given blob or its composite blob.
   */
  private void fixBlobSizeIfRequired(long blobSizeFromProperties, long blobSizeFromMetadata) {
    // In order to mitigate impact of replication logic that set the size field in BlobProperties incorrectly,
    // and in case of GET for segment of a blob, we will replace the field with the size from inside of the metadata content.
    if (blobSizeFromProperties != blobSizeFromMetadata) {
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
      blobInfo.getBlobProperties().setBlobSize(blobSizeFromMetadata);
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
    GetChunk getChunk = correlationIdToGetChunk.remove(responseInfo.getRequestInfo().getRequest().getCorrelationId());
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
    if (isOperationComplete()) {
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
                && numChunksRetrieved.get() - blobDataChannel.getNumChunksWrittenOut()
                < routerConfig.routerMaxInMemGetChunks)) {
              dataChunk.poll(requestRegistrationCallback);
              if (dataChunk.isComplete()) {
                onChunkOperationComplete(dataChunk);
                if (isOperationComplete()) {
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

  MessageFormatFlags getFirstChunkOperationFlag() {
    return firstChunk.getOperationFlag();
  }

  // ReadableStreamChannel implementation:

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

  /**
   * A class that implements the result of this GetBlobOperation. This is instantiated if/when the first data chunk of
   * the blob arrives, when the operation callback is invoked.
   */
  private class BlobDataReadableStreamChannel implements ReadableStreamChannel {
    // whether this object has called the readIntoCallback yet.
    private final AtomicBoolean readIntoCallbackCalled = new AtomicBoolean(false);
    // whether this ReadableStreamChannel is open.
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    // whether readInto() has been called yet by the caller on this ReadableStreamChannel.
    private volatile boolean readCalled = false;
    // The channel to write chunks of the blob into. This will be initialized when the caller calls the readInto().
    private AsyncWritableChannel asyncWritableChannel;
    // the callback to call when all the chunks are successfully written out into the asyncWritableChannel.
    private Callback<Long> readIntoCallback;
    // the future to mark as done when all the chunks are successfully written out into the asyncWritableChannel.
    private FutureResult<Long> readIntoFuture;
    // the number of bytes written out to the asyncWritableChannel. This would be the size of the blob eventually.
    private final AtomicLong bytesWritten = new AtomicLong(0);
    // the number of chunks that have been written out to the asyncWritableChannel.
    private AtomicInteger numChunksWrittenOut = new AtomicInteger(0);
    // the time of last chunk written done
    private AtomicLong lastChunkWrittenDoneTime = new AtomicLong(0);
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
          ReferenceCountUtil.safeRelease(byteBuf);
        }
        lastChunkWrittenDoneTime.set(SystemTime.getInstance().milliseconds());
        numChunksWrittenOut.incrementAndGet();
        routerCallback.onPollReady();
      }
    };
    // the index of the next chunk that is to be written out to the asyncWritableChannel.
    private int indexOfNextChunkToWriteOut = 0;

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
        if (numChunksWrittenOut.get() != numChunksTotal) {
          setOperationException(new RouterException(
              "The ReadableStreamChannel for blob data has been closed by the user before all chunks were written out.",
              RouterErrorCode.ChannelClosed));
        }
        releaseResource();
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
        if (e instanceof RouterException) {
          RouterException routerException = (RouterException) e;
          if (routerException.getErrorCode() == RouterErrorCode.UnexpectedInternalError) {
            // Client deserves to know why there is an unexpected internal error
            e = new RestServiceException(routerException.getMessage(), RestServiceErrorCode.InternalServerError, true,
                false, null);
          }
        }
        readIntoFuture.done(bytesWritten.get(), e);
        if (readIntoCallback != null) {
          readIntoCallback.onCompletion(bytesWritten.get(), e);
        }
        if (e == null) {
          updateChunkingAndSizeMetricsOnSuccessfulGet();
        } else {
          logger.warn(
              "GetBlobOperationError BlobId: {}, numChunksRetrieved:{}, numChunksWrittenOut: {}. Time since last chunk write done: {}ms",
              blobId, numChunksRetrieved, numChunksWrittenOut,
              SystemTime.getInstance().milliseconds() - lastChunkWrittenDoneTime.get(), e);
          routerMetrics.onGetBlobError(e, options, isEncrypted);
        }
        long totalTime = time.milliseconds() - submissionTimeMs;
        if (isEncrypted) {
          routerMetrics.getEncryptedBlobOperationTotalTimeMs.update(totalTime);
        } else {
          routerMetrics.getBlobOperationTotalTimeMs.update(totalTime);
        }
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
    // map of correlation id to the request metadata for every request issued for this operation.
    protected final Map<Integer, RequestInfo> correlationIdToGetRequestInfo = new LinkedHashMap<>();
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
    // the state of the chunk.
    protected volatile ChunkState state;
    // metrics tracker to track decrypt jobs
    protected CryptoJobMetricsTracker decryptJobMetricsTracker =
        new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics);
    // the operation tracker used to track the operation on the current chunk.
    private OperationTracker chunkOperationTracker;
    // the blob id of the current chunk.
    private BlobId chunkBlobId;
    // byte offset of the data in the chunk relative to the entire blob
    private long offset;
    // size of the chunk
    private long chunkSize;
    // whether the operation on the current chunk has completed.
    protected boolean chunkCompleted;
    // Tracks quota charging for this chunk.
    private OperationQuotaCharger operationQuotaCharger;
    protected long initializedTimeMs;
    private long timeLimitForRetryInMs;
    private int failedAttempts;
    protected boolean isChunkCompressed;

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
      operationQuotaCharger =
          new OperationQuotaCharger(quotaChargeCallback, GetBlobOperation.class.getSimpleName(), routerMetrics);
      initializedTimeMs = -1;
      timeLimitForRetryInMs = 0;
      failedAttempts = 0;
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
          RouterOperation.GetBlobOperation, chunkBlobId);
      progressTracker = new ProgressTracker(chunkOperationTracker);
      state = ChunkState.Ready;
      initializedTimeMs = time.milliseconds();
      timeLimitForRetryInMs = initializedTimeMs + TimeUnit.SECONDS.toMillis(routerConfig.routerGetBlobRetryLimitInSec);
    }

    /**
     * Reset some variables for retry.
     */
    void resetForRetry() {
      logger.trace("BlobId {}: Retry for chunk Id: {}, failed attempts: {}, initializedTime: {}", blobId, chunkBlobId,
          failedAttempts, initializedTimeMs);
      chunkException = null;
      chunkOperationTracker = getOperationTracker(chunkBlobId.getPartition(), chunkBlobId.getDatacenterId(),
          RouterOperation.GetBlobOperation, chunkBlobId);
      progressTracker = new ProgressTracker(chunkOperationTracker);
      state = ChunkState.Ready;
      failedAttempts++;
      routerMetrics.getBlobRetryCount.inc();
    }

    /**
     * return the {@link RouterException} associated with the operation on this chunk, if any.
     * @return the {@link RouterException} associated with the operation on this chunk, if any.
     */
    RouterException getChunkException() {
      return chunkException;
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
     * Maybe process callbacks if applicable. This is a no-op for blobs that do not need any async processing.
     * As of now, decryption is the only async processing that could happen if applicable.
     */
    protected void maybeProcessCallbacks() {
      if (!isOperationComplete() && progressTracker.isCryptoJobRequired()
          && decryptCallbackResultInfo.decryptJobComplete) {
        logger.trace("Processing decrypt callback stored result for data chunk {}", chunkBlobId);
        decryptJobMetricsTracker.onJobResultProcessingStart();
        // Only when the blob is encrypted should we need to call this method. When finish decryption, we don't need
        // response info anymore.
        if (decryptCallbackResultInfo.exception == null) {
          // Nullify result so we know we already passed the bytebuf to chunkIndexToBuf map
          DecryptJob.DecryptJobResult result = decryptCallbackResultInfo.result.getAndSet(null);
          if (result != null) {
            if (chunkIndex == 0) {
              routerMetrics.getFirstDataChunkLatencyMs.update(System.currentTimeMillis() - submissionTimeMs);
            }
            routerMetrics.getDataChunkLatencyMs.update(System.currentTimeMillis() - initializedTimeMs);
            ByteBuf decryptedContent = result.getDecryptedBlobContent();
            ByteBuf decompressedContent = decompressContent(decryptedContent);
            if (decompressedContent != null) {
              chunkIndexToBuf.put(chunkIndex, filterChunkToRange(decompressedContent));
              numChunksRetrieved.incrementAndGet();
            }
          } else {
            // only in maybeReleaseDecryptionResultBuffer() will result be set to null, this means the bytebuf has been
            // released, don't have to release it again here.
          }
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
     * Decompress the compressed buffer specified.
     * If the buffer is not compressed, based on blobCompressed property, then return the input buffer.
     *
     * @param sourceBuffer The source buffer to decompress.
     * @return A new buffer if decompressed successfully and the source buffer is released.
     *         The source buffer if it is not compressed.
     *         NULL if decompression failed and the source buffer is released.
     */
    protected ByteBuf decompressContent(ByteBuf sourceBuffer) {
      if (!isChunkCompressed) {
        // Blob is not compressed.  Return source buffer.
        return sourceBuffer;
      }

      try {
        return decompressionService.decompress(sourceBuffer, routerConfig.routerMaxPutChunkSizeBytes, true);
      } catch (Exception ex) {
        // Note: No need to emit metrics here because decompress() already emits failure metrics.
        logger.error("Failed to decompress chunk data for chunkBlobId " + chunkBlobId + " due to exception.", ex);
        setOperationException(buildChunkException("Exception thrown on decompressing the data chunk", ex,
            RouterErrorCode.UnexpectedInternalError));
        setOperationCompleted();
        return null;
      } finally {
        sourceBuffer.release();
      }
    }

    /**
     * Maybe release the bytebuf in the decryption result. This is used in {@link #releaseResource()} when releasing
     * the resource, if we have just finished a decryption task, and not yet called poll method to add the decryption
     * result bytebuf to chunkIndexToBuf, then we have to call this method to release this result bytebuf.
     */
    protected void maybeReleaseDecryptionResultBuffer() {
      if (isInProgress() && progressTracker.isCryptoJobRequired() && decryptCallbackResultInfo.decryptJobComplete) {
        DecryptJob.DecryptJobResult result = decryptCallbackResultInfo.result.getAndSet(null);
        if (result != null) {
          ReferenceCountUtil.safeRelease(result.getDecryptedBlobContent());
        }
      }
    }

    /**
     * Clean up requests sent out by this operation that have now timed out.
     * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
     */
    private void cleanupExpiredInFlightRequests(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
      //First, check if any of the existing requests have timed out.
      Iterator<Map.Entry<Integer, RequestInfo>> inFlightRequestsIterator =
          correlationIdToGetRequestInfo.entrySet().iterator();
      while (inFlightRequestsIterator.hasNext()) {
        Map.Entry<Integer, RequestInfo> entry = inFlightRequestsIterator.next();
        int correlationId = entry.getKey();
        RequestInfo requestInfo = entry.getValue();
        // If request times out due to no response from server or due to being stuck in router itself (due to bandwidth
        // throttling, etc) for long time, drop the request.
        long currentTimeInMs = time.milliseconds();
        RouterUtils.RouterRequestExpiryReason routerRequestExpiryReason =
            RouterUtils.isRequestExpired(requestInfo, currentTimeInMs);
        if (routerRequestExpiryReason != RouterUtils.RouterRequestExpiryReason.NO_TIMEOUT) {
          logger.trace("GetBlobRequest with correlationId {} in flight has expired for replica {} due to {} ",
              correlationId, requestInfo.getReplicaId().getDataNodeId(), routerRequestExpiryReason.name());
          // Do not notify this as a failure to the response handler, as this timeout could simply be due to
          // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
          // response and the response handler will be notified accordingly.
          onErrorResponse(requestInfo.getReplicaId(),
              RouterUtils.buildTimeoutException(correlationId, requestInfo.getReplicaId().getDataNodeId(),
                  chunkBlobId));
          requestRegistrationCallback.registerRequestToDrop(correlationId);
          inFlightRequestsIterator.remove();
        } else {
          // Note: Even though the requests are ordered by correlation id and their creation time, we cannot break out of
          // the while loop here. This is because time outs for all requests may not be equal now.

          // For example, request 1 in the map may have been assigned high time out since it might be sent at a
          // time when the load is high and request 2 may have been assigned lower time out value since the load might have
          // decreased by the time it is sent out. In this case, we should continue iterating the loop and clean up
          // request 2 in the map.

          // The cost of iterating all entries should be okay since the map contains outstanding requests whose number
          // should be small. The maximum outstanding requests possible would be equal to the operation parallelism value
          // and may be few more if adaptive operation tracker is used.
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
        RequestInfo requestInfo =
            new RequestInfo(hostname, port, getRequest, replicaId, prepareQuotaCharger(), time.milliseconds(),
                routerConfig.routerRequestNetworkTimeoutMs, routerConfig.routerRequestTimeoutMs);
        int correlationId = getRequest.getCorrelationId();
        correlationIdToGetRequestInfo.put(correlationId, requestInfo);
        correlationIdToGetChunk.put(correlationId, this);
        requestRegistrationCallback.registerRequestToSend(GetBlobOperation.this, requestInfo);
        if (RouterUtils.isRemoteReplica(routerConfig, replicaId)) {
          logger.trace("Making request with correlationId {} to a remote replica {} in {} ", correlationId,
              replicaId.getDataNodeId(), replicaId.getDataNodeId().getDatacenterName());
          routerMetrics.crossColoRequestCount.inc();
        } else {
          logger.trace("Making request with correlationId {} to a local replica {}", correlationId,
              replicaId.getDataNodeId());
        }
        routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getRequestRate.mark();
        routerMetrics.routerGetRequestRate.mark();
        state = ChunkState.InProgress;
      }
    }

    /**
     * Return a boolean value to indicate whether we should retry on fetching this chunk based on the given exception.
     * To Retry on a Get failure, these conditions have to be met at the same time
     * <p>
     *   <ol>
     *     <li>Exception is AmbryUnavailable, or OperationTimeout, or UnexpectedInternalError. The errors that return 503</li>
     *     <li>Failed attempts is less than the maximum retry count</li>
     *     <li>Operation duration is less than maximum retry duration</li>
     *   </ol>
     * </p>
     * @param exception The exception for this chunk operation.
     * @return {@code True} to retry this chunk. otherwise, return false.
     */
    boolean shouldRetry(RouterException exception) {
      if (exception == null) {
        return false;
      }
      switch (exception.getErrorCode()) {
        case AmbryUnavailable:
        case OperationTimedOut:
        case UnexpectedInternalError:
          return failedAttempts < routerConfig.routerGetBlobRetryLimitCount
              && time.milliseconds() < timeLimitForRetryInMs;
        default:
          return false;
      }
    }

    /**
     * Check if the operation on the chunk is eligible for completion, if so complete it.
     */
    void checkAndMaybeComplete() {
      if (progressTracker.isDone()) {
        if (progressTracker.hasSucceeded() && !retainChunkExceptionOnSuccess) {
          chunkException = null;
        } else if (chunkOperationTracker.maybeFailedDueToOfflineReplicas()) {
          chunkException =
              buildChunkException("Get Chunk failed because of offline replicas", RouterErrorCode.AmbryUnavailable);
        } else if (chunkOperationTracker.hasFailedOnNotFound()) {
          chunkException =
              buildChunkException("Get Chunk failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist);
        }
        if (shouldRetry(chunkException)) {
          resetForRetry();
        } else {
          chunkCompleted = true;
        }
      }
      if (chunkCompleted) {
        if (state != ChunkState.Complete && QuotaUtils.postProcessCharge(quotaChargeCallback)
            && chunkException == null) {
          try {
            if (chunkSize != -1) {
              quotaChargeCallback.checkAndCharge(false, true, chunkSize);
            } else {
              if (this instanceof FirstGetChunk && ((FirstGetChunk) this).blobType == BlobType.DataBlob) {
                quotaChargeCallback.checkAndCharge(false, true, totalSize);
              }
              // other cases mean that either this was a metadata blob, or there was an error.
            }
          } catch (QuotaException quotaException) {
            logger.info("Exception {} occurred during the quota charge event of blob {}", quotaException,
                blobId.getID());
          }
        }
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
      // Responses of same GetChunk would return in sequence so there is no need to protect these methods
      // from concurrent operations.
      if (!successfullyDeserialized) {
        BlobData blobData = MessageFormatRecord.deserializeBlob(payload);
        ByteBuffer encryptionKey = messageMetadata == null ? null : messageMetadata.getEncryptionKey();
        ByteBuf chunkBuf = blobData.content();
        this.isChunkCompressed = blobData.isCompressed();

        try {
          boolean launchedJob = maybeLaunchCryptoJob(chunkBuf, null, encryptionKey, chunkBlobId);
          if (!launchedJob) {
            if (chunkIndex == 0) {
              routerMetrics.getFirstDataChunkLatencyMs.update(System.currentTimeMillis() - submissionTimeMs);
            }
            routerMetrics.getDataChunkLatencyMs.update(System.currentTimeMillis() - initializedTimeMs);
            ByteBuf decompressedContent = decompressContent(chunkBuf.retainedDuplicate());
            if (decompressedContent != null) {
              chunkIndexToBuf.put(chunkIndex, filterChunkToRange(decompressedContent));
              numChunksRetrieved.incrementAndGet();
            }
          }

          successfullyDeserialized = true;
        } finally {
          ReferenceCountUtil.safeRelease(chunkBuf);
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
      int correlationId = responseInfo.getRequestInfo().getRequest().getCorrelationId();
      // Get the GetOperation that generated the request.
      RequestInfo getRequestInfo = correlationIdToGetRequestInfo.remove(correlationId);
      if (getRequestInfo == null) {
        // Ignore right away. This associated operation has completed.
        return;
      }
      if (responseInfo.isQuotaRejected()) {
        processQuotaRejectedResponse(correlationId, getRequestInfo.getReplicaId());
        return;
      }
      // Track the over all time taken for the response since the creation of the request.
      long requestLatencyMs = time.milliseconds() - getRequestInfo.getRequestCreateTime();
      routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
      routerMetrics.getDataNodeBasedMetrics(getRequestInfo.getReplicaId().getDataNodeId()).getRequestLatencyMs.update(
          requestLatencyMs);
      if (responseInfo.getError() != null) {
        // responseInfo.getError() returns NetworkClientErrorCode. If error is not null, it probably means (1) connection
        // checkout timed out; (2) pending connection timed out; (3) established connection timed out. In all these cases,
        // the latency histogram in adaptive operation tracker should not be updated.
        logger.trace("GetBlobRequest with response correlationId {} timed out for replica {} ", correlationId,
            getRequestInfo.getReplicaId().getDataNodeId());
        onErrorResponse(getRequestInfo.getReplicaId(), buildChunkException(
            "Operation timed out because of " + responseInfo.getError() + " at DataNode " + responseInfo.getDataNode(),
            RouterErrorCode.OperationTimedOut));
      } else {
        if (getResponse == null) {
          logger.trace(
              "GetBlobRequest with response correlationId {} received an unexpected error on response deserialization from replica {} ",
              correlationId, getRequestInfo.getReplicaId().getDataNodeId());
          onErrorResponse(getRequestInfo.getReplicaId(),
              buildChunkException("Response deserialization received an unexpected error",
                  RouterErrorCode.UnexpectedInternalError));
        } else {
          if (getResponse.getCorrelationId() != correlationId) {
            // The NetworkClient associates a response with a request based on the fact that only one request is sent
            // out over a connection id, and the response received on a connection id must be for the latest request
            // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
            // There is no other way to handle it.
            logger.trace("GetBlobRequest with response correlationId {} mismatch from response {} for replica {} ",
                correlationId, getResponse.getCorrelationId(), getRequestInfo.getReplicaId().getDataNodeId());
            routerMetrics.unknownReplicaResponseError.inc();
            onErrorResponse(getRequestInfo.getReplicaId(), buildChunkException(
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
                  correlationId, getRequestInfo.getReplicaId().getDataNodeId());
              routerMetrics.responseDeserializationErrorCount.inc();
              onErrorResponse(getRequestInfo.getReplicaId(),
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
            userMetadata != null ? ByteBuffer.wrap(userMetadata) : null, cryptoService, kms, options.getBlobOptions,
            decryptJobMetricsTracker, (DecryptJob.DecryptJobResult result, Exception exception) -> {
          routerMetrics.decryptTimeMs.update(System.currentTimeMillis() - startTimeMs);
          decryptJobMetricsTracker.onJobCallbackProcessingStart();
          logger.trace("Handling decrypt job call back for blob {} to set decrypt callback results", targetBlobId);
          if (isOperationComplete() || operationException.get() != null) {
            if (exception == null && result.getDecryptedBlobContent() != null) {
              ReferenceCountUtil.safeRelease(result.getDecryptedBlobContent());
            }
            return;
          }
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
    private void processGetBlobResponse(RequestInfo getRequestInfo, GetResponse getResponse)
        throws IOException, MessageFormatException {
      ServerErrorCode getError = getResponse.getError();
      if (getError == ServerErrorCode.No_Error) {
        int partitionsInResponse = getResponse.getPartitionResponseInfoList().size();
        // Each get request issued by the router is for a single blob.
        if (partitionsInResponse != 1) {
          onErrorResponse(getRequestInfo.getReplicaId(), buildChunkException(
              "Unexpected number of partition responses, expected: 1, " + "received: " + partitionsInResponse,
              RouterErrorCode.UnexpectedInternalError));
        } else {
          getError = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
          if (getError == ServerErrorCode.No_Error) {
            PartitionResponseInfo partitionResponseInfo = getResponse.getPartitionResponseInfoList().get(0);
            int objectsInPartitionResponse = partitionResponseInfo.getMessageInfoList().size();
            if (objectsInPartitionResponse != 1) {
              onErrorResponse(getRequestInfo.getReplicaId(), buildChunkException(
                  "Unexpected number of messages in a partition response, expected: 1, " + "received: "
                      + objectsInPartitionResponse, RouterErrorCode.UnexpectedInternalError));
            } else {
              MessageMetadata messageMetadata = partitionResponseInfo.getMessageMetadataList().get(0);
              MessageInfo messageInfo = partitionResponseInfo.getMessageInfoList().get(0);
              handleBody(getResponse.getInputStream(), messageMetadata, messageInfo);
              chunkOperationTracker.onResponse(getRequestInfo.getReplicaId(), TrackedRequestFinalState.SUCCESS);
              if (RouterUtils.isRemoteReplica(routerConfig, getRequestInfo.getReplicaId())) {
                logger.trace("Cross colo request successful for remote replica in {} ",
                    getRequestInfo.getReplicaId().getDataNodeId().getDatacenterName());
                routerMetrics.crossColoSuccessCount.inc();
              }
            }
          } else {
            if (chunkBlobId.getBlobDataType() == BlobId.BlobDataType.DATACHUNK
                && getError == ServerErrorCode.Blob_Not_Found) {
              // Missing a data chunk
              routerMetrics.missingDataChunkErrorCount.inc();
            }
            // process and set the most relevant exception.
            RouterErrorCode routerErrorCode = processServerError(getError);
            if (getError == ServerErrorCode.Disk_Unavailable) {
              chunkOperationTracker.onResponse(getRequestInfo.getReplicaId(), TrackedRequestFinalState.DISK_DOWN);
              setChunkException(buildChunkException("Server returned: " + getError, routerErrorCode));
              routerMetrics.routerRequestErrorCount.inc();
              routerMetrics.getDataNodeBasedMetrics(
                  getRequestInfo.getReplicaId().getDataNodeId()).getRequestErrorCount.inc();
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
              onErrorResponse(getRequestInfo.getReplicaId(),
                  buildChunkException("Server returned: " + getError, routerErrorCode));
            }
          }
        }
      } else {
        logger.trace("Replica {} returned an error {} for a GetBlobRequest with response correlationId : {} ",
            getRequestInfo.getReplicaId().getDataNodeId(), getError, getResponse.getCorrelationId());
        // process and set the most relevant exception.
        onErrorResponse(getRequestInfo.getReplicaId(),
            buildChunkException("Server returned", processServerError(getError)));
      }
    }

    /**
     * Process response if it was rejected due to quota compliance.
     * @param correlationId correlation id of the request.
     * @param replicaId {@link ReplicaId} of the request.
     */
    private void processQuotaRejectedResponse(int correlationId, ReplicaId replicaId) {
      logger.trace("GetBlobRequest with response correlationId {} rejected because it exceeded quota", correlationId);
      onErrorResponse(replicaId, new RouterException("QuotaExceeded", RouterErrorCode.TooManyRequests), false);
      chunkCompleted = true;
      checkAndMaybeComplete();
    }

    /**
     * Perform the necessary actions when a request to a replica fails.
     * @param replicaId the {@link ReplicaId} associated with the failed response.
     * @param exception the {@link RouterException} associated with the failed response.
     */
    private void onErrorResponse(ReplicaId replicaId, RouterException exception) {
      onErrorResponse(replicaId, exception, true);
    }

    /**
     * Perform the necessary actions when a request to a replica fails.
     * @param replicaId the {@link ReplicaId} associated with the failed response.
     * @param exception the {@link RouterException} associated with the failed response.
     * @param updateDataNodeMetrics {@code true} if data node metrics should be updated. {@code false} otherwise.
     */
    private void onErrorResponse(ReplicaId replicaId, RouterException exception, boolean updateDataNodeMetrics) {
      chunkOperationTracker.onResponse(replicaId,
          TrackedRequestFinalState.fromRouterErrorCodeToFinalState(exception.getErrorCode()));
      setChunkException(exception);
      routerMetrics.routerRequestErrorCount.inc();
      if (updateDataNodeMetrics) {
        routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getRequestErrorCount.inc();
      }
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

    /**
     * Set the blobId and chunkSize in the operationQuotaCharger, and return the {@link OperationQuotaCharger} object.
     * Note that this method might set the blobId and chunkSize for the same chunk multiple times. This is ok because
     * these are idempotent operation for the same chunk.
     * @return OperationQuotaCharger object.
     */
    private OperationQuotaCharger prepareQuotaCharger() {
      operationQuotaCharger.setBlobId(chunkBlobId);
      operationQuotaCharger.setChunkSize(chunkSize);
      return operationQuotaCharger;
    }
  }

  /**
   * Special GetChunk used to retrieve and hold the first chunk of a blob. The first chunk is special because it
   * could either be a metadata chunk of a composite blob, or the single chunk of a simple blob,
   * and whether a chunk is composite or simple can only be determined after the first chunk is fetched.
   */
  private class FirstGetChunk extends GetChunk {

    // refers to the blob type.
    protected BlobType blobType;
    protected List<CompositeBlobInfo.ChunkMetadata> chunkMetadataList;
    protected BlobProperties serverBlobProperties;
    protected short lifeVersion;

    /**
     * Construct a FirstGetChunk and initialize it with the {@link BlobId} of the overall operation.
     */
    FirstGetChunk() {
      super(-1, new CompositeBlobInfo.ChunkMetadata(blobId, 0L, -1L));
    }

    /**
     * Decides if we should cache metadata during a GET request
     * This method should be called after decryption is complete.
     * @return True if and only if
     *  - cache exists
     *  - it's a RANGE request
     *  - it's not a GET request for a segment
     *  - it is not a raw mode request
     *  - the blobID is for a metadata blob (this info is present only in v5 and v6 version of blobID)
     *  - the blob type is metadata blob
     *  - firstChunk is complete
     *  - there are no exceptions
     *  False, otherwise.
     */
    protected boolean shouldSaveMetadata() {
      return (blobMetadataCache != null && isRangeRequest() && options.getBlobOptions.hasBlobSegmentIdx() == false
          && options.getBlobOptions.isRawMode() == false && blobId.getBlobDataType() == BlobId.BlobDataType.METADATA
          && blobType == BlobType.MetadataBlob && totalSize >= routerConfig.routerSmallestBlobForMetadataCache
          && isComplete() && getChunkException() == null && getOperationException() == null);
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
      if (!isOperationComplete() && progressTracker.isCryptoJobRequired()
          && decryptCallbackResultInfo.decryptJobComplete) {
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
            blobInfo = new BlobInfo(serverBlobProperties,
                decryptCallbackResultInfo.result.get().getDecryptedUserMetadata().array(), lifeVersion);
            progressTracker.setCryptoJobSuccess();
            logger.trace("BlobContent available to process for Metadata blob {}", blobId);
          } else {
            logger.trace("Processing stored decryption callback result for simple blob {}", blobId);
            // In case of simple blobs, user-metadata may or may not be passed into decryption job based on GetOptions flag.
            // Only in-case of GetBlobInfo and GetBlobAll, user-metadata is required to be decrypted
            DecryptJob.DecryptJobResult result = decryptCallbackResultInfo.result.getAndSet(null);
            if (result != null) {
              if (result.getDecryptedUserMetadata() != null) {
                blobInfo = new BlobInfo(serverBlobProperties, result.getDecryptedUserMetadata().array(), lifeVersion);
              }
              ByteBuf decryptedBlobContent = result.getDecryptedBlobContent();
              routerMetrics.getFirstDataChunkLatencyMs.update(System.currentTimeMillis() - submissionTimeMs);
              routerMetrics.getDataChunkLatencyMs.update(System.currentTimeMillis() - initializedTimeMs);
              ByteBuf decompressedContent = decompressContent(decryptedBlobContent);
              if (decompressedContent == null) {
                ReferenceCountUtil.safeRelease(decryptedBlobContent);
                progressTracker.setCryptoJobFailed();
              } else {
                totalSize = decompressedContent.readableBytes();
                if (!resolveRange(totalSize)) {
                  chunkIndexToBuf.put(0, filterChunkToRange(decompressedContent));
                  numChunksRetrieved.set(1);
                  progressTracker.setCryptoJobSuccess();
                  logger.trace("BlobContent available to process for simple blob {}", blobId);
                } else {
                  ReferenceCountUtil.safeRelease(decompressedContent);
                  ReferenceCountUtil.safeRelease(decryptedBlobContent);
                  progressTracker.setCryptoJobFailed();
                }
              }
            } else {
              // only in maybeReleaseDecryptionResultBuffer() will result be set to null, this means the bytebuf has been
              // released, don't have to release it again here.
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
          if (Objects.isNull(encryptionKey) || options.getChunkIdsOnly) {
            // set blobInfo iff:
            //   1. blob is not encrypted OR,
            //   2. getChunkIdsOnly is set.
            // If the blob is encrypted, then the user metadata is encrypted too. So in that case,
            // maybeProcessCallbackAndComplete() will set the appropriate value for blobInfo after decryption.
            // In the case of getChunkIdsOnly call, we don't need the user metadata but still need blob
            // properties to evaluate if the metadata chunk is already updated for update requests. So we allow blobInfo
            // to be set here.
            blobInfo = serverBlobInfo;
            blobInfo.setLifeVersion(messageInfo.getLifeVersion());
          } else {
            serverBlobProperties = serverBlobInfo.getBlobProperties();
            lifeVersion = messageInfo.getLifeVersion();
            userMetadata = serverBlobInfo.getUserMetadata();
          }
        }
        blobType = blobData.getBlobType();
        isChunkCompressed = blobData.isCompressed();
        logger.trace(
            "In FirstGetChunk.handleBody(), is Chunk compressed = " + isChunkCompressed + " for blob ID = " + blobId);
        chunkIndexToBuf = new ConcurrentHashMap<>();
        chunkIndexToBufWaitingForRelease = new ConcurrentHashMap<>();
        if (rawMode) {
          if (blobData != null) {
            // RawMode, release blob data.
            ReferenceCountUtil.safeRelease(blobData);
          }
          // Return the raw bytes from storage
          if (encryptionKey != null) {
            chunkIdIterator = null;
            dataChunks = null;
            chunkIndex = 0;
            numChunksTotal = 1;
            ByteBuf decompressedBuffer = decompressContent(Unpooled.wrappedBuffer(rawPayloadBuffer));
            if (decompressedBuffer != null) {
              chunkIndexToBuf.put(0, decompressedBuffer);
              numChunksRetrieved.set(1);
            }
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
     * @return {@code chunkMetadataList}.
     */
    public List<CompositeBlobInfo.ChunkMetadata> getChunkMetadataList() {
      return chunkMetadataList;
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
            // is expected to be encrypted). In case of GetBlob, Metadata blob does not need any decryption even if BlobProperties says so
            decryptCallbackResultInfo = new DecryptCallBackResultInfo();
            progressTracker.initializeCryptoJobTracker(CryptoJobType.DECRYPTION);
            decryptJobMetricsTracker.onJobSubmission();
            logger.trace("Submitting decrypt job for Metadata chunk {}", blobId);
            long startTimeMs = System.currentTimeMillis();
            cryptoJobHandler.submitJob(
                new DecryptJob(blobId, encryptionKey, null, ByteBuffer.wrap(userMetadata), cryptoService, kms,
                    options.getBlobOptions, decryptJobMetricsTracker,
                    (DecryptJob.DecryptJobResult result, Exception exception) -> {
                      routerMetrics.decryptTimeMs.update(System.currentTimeMillis() - startTimeMs);
                      if (isOperationComplete() || operationException.get() != null) {
                        if (result != null && result.getDecryptedBlobContent() != null) {
                          ReferenceCountUtil.safeRelease(result.getDecryptedBlobContent());
                        }
                      } else {
                        decryptJobMetricsTracker.onJobCallbackProcessingStart();
                        logger.trace(
                            "Handling decrypt job call back for Metadata chunk {} to set decrypt callback results",
                            blobId);
                        decryptCallbackResultInfo.setResultAndException(result, exception);
                        routerCallback.onPollReady();
                        decryptJobMetricsTracker.onJobCallbackProcessingComplete();
                      }
                    }));
          }
        }
      } finally {
        ReferenceCountUtil.safeRelease(serializedMetadataContent);
      }
    }

    /**
     * Initialize data chunks and few other cast for metadata chunk
     */
    protected void initializeDataChunks() {
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
        chunkIdIterator = null;
        numChunksTotal = 0;
        dataChunks = null;
        if (!options.getChunkIdsOnly) {
          chunkIndex = 0;
          numChunksTotal = 1;
          boolean launchedJob = maybeLaunchCryptoJob(chunkBuf, userMetadata, encryptionKey, blobId);
          if (!launchedJob) {
            routerMetrics.getFirstDataChunkLatencyMs.update(System.currentTimeMillis() - submissionTimeMs);
            routerMetrics.getDataChunkLatencyMs.update(System.currentTimeMillis() - initializedTimeMs);

            ByteBuf decompressedContent = decompressContent(chunkBuf.retainedDuplicate());
            if (decompressedContent != null) {
              totalSize = decompressedContent.readableBytes();
              if (!resolveRange(totalSize)) {
                chunkIndexToBuf.put(0, filterChunkToRange(decompressedContent));
                numChunksRetrieved.set(1);
              } else {
                decompressedContent.release();
              }
            }
          }
        }
      } finally {
        ReferenceCountUtil.safeRelease(blobData);
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
          resolvedByteRange = options.getBlobOptions.getRange()
              .toResolvedByteRange(totalSize, options.getBlobOptions.resolveRangeOnEmptyBlob());
        }
      } catch (IllegalArgumentException e) {
        onInvalidRange(e);
        failure = true;
      }
      return failure;
    }

    /**
     * Wrapper over resolveRange
     * @param rangeTotalSize byte range to be used for range resolution
     * @return {@code true} if range resolution succeeded or no range resolution was done. {@code false} otherwise
     */
    protected boolean resolveRangeProper(long rangeTotalSize) {
      /*
       * resolveRange is a weird function that returns false on success and true on failure !
       * Hence, this wrapper to make code readable !
       */
      return !resolveRange(rangeTotalSize);
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
      numChunksRetrieved.set(0);
    }
  }

  private class CachedFirstChunk extends FirstGetChunk {

    /**
     * Construct a CachedFirstChunk and initialize it with the {@link BlobMetadata} of the overall operation.
     * @param blobMetadata Cached blob metadata for composite blobs
     */
    CachedFirstChunk(BlobMetadata blobMetadata) {
      super();
      /*
       * If cache hit, extract all relevant metadata and save in appropriate variables.
       * If the entry gets evicted later, we are still safe as we have saved all the metadata.
       */
      blobType = BlobType.MetadataBlob;
      compositeBlobInfo = blobMetadata.getCompositeBlobInfo();
      totalSize = compositeBlobInfo.getTotalSize();
      chunkMetadataList = compositeBlobInfo.getChunkMetadataList();
      blobInfo = blobMetadata.getBlobInfo();
      lifeVersion = blobInfo.getLifeVersion();
      serverBlobProperties = blobInfo.getBlobProperties();
      chunkIndexToBuf = new ConcurrentHashMap<>();
      chunkIndexToBufWaitingForRelease = new ConcurrentHashMap<>();
      successfullyDeserialized = true;
    }

    /**
     * Return {@link MessageFormatFlags} to associate with cached first chunk.
     * For a cached metadata chunk, we just need to get blobInfo to validate it.
     * We don't have to fetch the entire metadata chunk again.
     * @return {@link MessageFormatFlags#BlobInfo}
     */
    @Override
    MessageFormatFlags getOperationFlag() {
      return MessageFormatFlags.BlobInfo;
    }

    /**
     * Decides if we should save metadata or not.
     * @return False for cached first chunk. There is nothing to cache.
     */
    @Override
    protected boolean shouldSaveMetadata() {
      return false;
    }

    /**
     * On an invalid cache entry, set a {@link RouterErrorCode} exception for first chunk, mark the chunk
     * as completed, and set the chunk counters such that the operation will be completed.
     * @param routerException Exception due to which cache entry was invalid.
     */
    protected void onInvalidCacheEntry(RouterException routerException) {
      setChunkException(routerException);
      chunkCompleted = true;
      retainChunkExceptionOnSuccess = true;
      chunkIdIterator = null;
      dataChunks = null;
      numChunksTotal = 0;
      numChunksRetrieved.set(0);
    }

    /**
     * Validate cached metadata chunk and proceed to request data chunks
     * @param payload Bytes received from backend
     * @param messageMetadata  Metadata associated with messages sent out
     * @param messageInfo Message info class that contains basic info about a message
     * @throws IOException
     * @throws MessageFormatException
     */
    @Override
    void handleBody(InputStream payload, MessageMetadata messageMetadata, MessageInfo messageInfo)
        throws IOException, MessageFormatException {
      /* If we find blob metadata in frontend cache, validate it and use it. */
      BlobProperties receivedBlobProperties = MessageFormatRecord.deserializeBlobProperties(payload);
      updateTtlIfRequired(receivedBlobProperties, messageInfo);
      /*
       * Some cached variables are mutable.
       * Update them and then compare if other immutable variables are same or not.
       */
      updateTtlIfRequired(serverBlobProperties, messageInfo);
      lifeVersion = messageInfo.getLifeVersion();
      blobInfo.setLifeVersion(lifeVersion);
      if (!serverBlobProperties.equals(receivedBlobProperties)) {
        /*
         * This is a rare error case.
         * If we find blob properties is in our frontend cache, we request just blobInfo from backend to validate
         * the cache entry. If cached blob properties does not match received blob properties, due to a random bit flip for eg.,
         * we must delete the cache entry and throw a retriable exception to the user.
         * The next request will re-fill the cache with a valid entry if the blob stills exists.
         * We do not need to decrypt anything at this point.
         */
        String reason = "Cached blob property does not match received blob property";
        logger.error("[{}] {} for blobId = {}, cached blob property = {}, received blob property = {}",
            blobMetadataCache.getCacheId(), reason, blobId, serverBlobProperties, receivedBlobProperties);
        deleteMetadata(reason);
        onInvalidCacheEntry(new RouterException(reason, RouterErrorCode.UnexpectedInternalError));
        return;
      }
      getOptions().ageAtAccessTracker.trackAgeAtAccess(serverBlobProperties.getCreationTimeInMs());

      /*
       * If we reach here, then the cached metadata is valid and already decrypted from a previous GET/RANGE request.
       * Just use it directly and initiate fetching data chunks.
       */
      if (isRangeRequest() && resolveRangeProper(totalSize)) {
        chunkMetadataList = compositeBlobInfo.getStoreKeysInByteRange(resolvedByteRange.getStartOffset(),
            resolvedByteRange.getEndOffset());
        initializeDataChunks();
      }
    }
  }
}

/**
 * Holds info about decryption job callback
 */
class DecryptCallBackResultInfo {
  volatile boolean decryptJobComplete;
  volatile Exception exception;
  AtomicReference<DecryptJob.DecryptJobResult> result = new AtomicReference<>();

  /**
   * Sets the result and exception from decrypt job callback
   * @param result {@link DecryptJob.DecryptJobResult} from the decrypt job callback. Could be null on failure.
   * @param exception {@link Exception} from the decrypt job callback. Could be null if the decrypt job succeeded.
   */
  void setResultAndException(DecryptJob.DecryptJobResult result, Exception exception) {
    this.result.set(result);
    this.exception = exception;
    this.decryptJobComplete = true;
  }
}
