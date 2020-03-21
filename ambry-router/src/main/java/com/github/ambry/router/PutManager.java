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

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PutManager class is responsible for handling putBlob operations. PutManager creates a {@link PutOperation} for each
 * operation submitted to it, and tracks them.
 */
class PutManager {
  private static final Logger logger = LoggerFactory.getLogger(PutManager.class);

  private final Set<PutOperation> putOperations;
  private final NotificationSystem notificationSystem;
  private final KeyManagementService kms;
  private final CryptoService cryptoService;
  private final CryptoJobHandler cryptoJobHandler;
  private final AccountService accountService;
  private final Time time;
  private final Thread chunkFillerThread;
  private final Object chunkFillerSynchronizer = new Object();
  private volatile boolean isChunkFillerThreadAsleep = false;
  private volatile boolean chunkFillerThreadMaySleep = false;
  // This helps the PutManager quickly find the appropriate PutOperation to hand over the response to.
  // Requests are added before they are sent out and get cleaned up as and when responses come in.
  // Because there is a guaranteed response from the NetworkClient for every request sent out, entries
  // get cleaned up periodically.
  private final Map<Integer, PutOperation> correlationIdToPutOperation;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  private final RouterCallback routerCallback;
  private final ByteBufferAsyncWritableChannel.ChannelEventListener chunkArrivalListener;

  // shared by all PutOperations
  private final ClusterMap clusterMap;
  private final RouterConfig routerConfig;
  private final ResponseHandler responseHandler;
  private final NonBlockingRouterMetrics routerMetrics;
  private final String defaultPartitionClass;

  // A single callback as this will never get called concurrently. The list of request to fill will be set as
  // appropriate before the callback is passed on to the PutOperations, every time.
  private final RequestRegistrationCallback<PutOperation> requestRegistrationCallback;

  /**
   * Create a PutManager
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param notificationSystem The {@link NotificationSystem} used for notifying blob creations.
   * @param routerConfig  The {@link RouterConfig} containing the configs for the PutManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param routerCallback The {@link RouterCallback} to use for callbacks to the router.
   * @param suffix the suffix to associate with the names of the threads created by this PutManager
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param accountService the {@link AccountService} to use.
   * @param defaultPartitionClass the default partition class to choose partitions from (if none is found in the
   *                              container config). Can be {@code null} if no affinity is required for the puts for
   *                              which the container contains no partition class hints.
   * @param time The {@link Time} instance to use.
   */
  PutManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, RouterCallback routerCallback, String suffix,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler,
      AccountService accountService, Time time, String defaultPartitionClass) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.routerCallback = routerCallback;
    this.defaultPartitionClass = defaultPartitionClass;
    this.chunkArrivalListener = eventType -> {
      synchronized (chunkFillerSynchronizer) {
        // At this point, the chunk for which this notification came in (if any) could already have been consumed by
        // the chunk filler, and this might unnecessarily wake it up from its sleep, which should be okay.
        chunkFillerThreadMaySleep = false;
        if (isChunkFillerThreadAsleep) {
          chunkFillerSynchronizer.notify();
        }
      }
    };
    this.kms = kms;
    this.cryptoService = cryptoService;
    this.cryptoJobHandler = cryptoJobHandler;
    this.accountService = accountService;
    this.time = time;
    putOperations = ConcurrentHashMap.newKeySet();
    correlationIdToPutOperation = new HashMap<>();
    requestRegistrationCallback = new RequestRegistrationCallback<>(correlationIdToPutOperation);
    chunkFillerThread = Utils.newThread("ChunkFillerThread-" + suffix, new ChunkFiller(), true);
    chunkFillerThread.start();
    routerMetrics.initializePutManagerMetrics(chunkFillerThread);
  }

  /**
   * Submit a put blob operation to be processed asynchronously.
   * @param blobProperties the blobProperties associated with the blob being put.
   * @param userMetaData the userMetaData associated with the blob being put.
   * @param channel the {@link ReadableStreamChannel} containing the blob content.
   * @param options The {@link PutBlobOptions} associated with the request. This cannot be null.
   * @param futureResult the {@link FutureResult} that contains the pending result of the operation.
   * @param callback the {@link Callback} object to be called on completion of the operation.
   */
  void submitPutBlobOperation(BlobProperties blobProperties, byte[] userMetaData, ReadableStreamChannel channel,
      PutBlobOptions options, FutureResult<String> futureResult, Callback<String> callback) {
    String partitionClass = getPartitionClass(blobProperties);
    PutOperation putOperation =
        PutOperation.forUpload(routerConfig, routerMetrics, clusterMap, notificationSystem, accountService,
            userMetaData, channel, options, futureResult, callback, routerCallback, chunkArrivalListener, kms,
            cryptoService, cryptoJobHandler, time, blobProperties, partitionClass);
    // TODO: netty send this request
    putOperations.add(putOperation);
    putOperation.startOperation();
  }

  /**
   * Submit a stitch blob operation to be processed asynchronously.
   * @param blobProperties the blobProperties associated with the blob being put.
   * @param userMetaData the userMetaData associated with the blob being put.
   * @param chunksToStitch the list of chunks to stitch together.
   * @param futureResult the {@link FutureResult} that contains the pending result of the operation.
   * @param callback the {@link Callback} object to be called on completion of the operation.
   */
  void submitStitchBlobOperation(BlobProperties blobProperties, byte[] userMetaData, List<ChunkInfo> chunksToStitch,
      FutureResult<String> futureResult, Callback<String> callback) {
    String partitionClass = getPartitionClass(blobProperties);
    PutOperation putOperation =
        PutOperation.forStitching(routerConfig, routerMetrics, clusterMap, notificationSystem, accountService,
            userMetaData, chunksToStitch, futureResult, callback, routerCallback, kms, cryptoService, cryptoJobHandler,
            time, blobProperties, partitionClass);
    putOperations.add(putOperation);
    putOperation.startOperation();
  }

  /**
   * @param blobProperties the properties of the blob being put
   * @return the partition class as required by the properties
   */
  private String getPartitionClass(BlobProperties blobProperties) {
    String partitionClass = defaultPartitionClass;
    Account account = accountService.getAccountById(blobProperties.getAccountId());
    if (account != null) {
      Container container = account.getContainerById(blobProperties.getContainerId());
      if (container != null && container.getReplicationPolicy() != null) {
        partitionClass = container.getReplicationPolicy();
      }
    }
    return partitionClass;
  }

  /**
   * Creates and returns requests in the form of {@link RequestInfo} to be sent to data nodes in order to
   * complete put operations. Since this is the only method guaranteed to be called periodically by the
   * RequestResponseHandler thread in the {@link NonBlockingRouter} ({@link #handleResponse} gets called only if a
   * response is received for a put operation), any error handling or operation completion and cleanup also usually
   * gets done in the context of this method.
   * @param requestsToSend list to be filled with the requests created
   * @param requestsToDrop list to be filled with the requests to drop.
   */
  void poll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    long startTime = time.milliseconds();
    requestRegistrationCallback.setRequestsToSend(requestsToSend);
    requestRegistrationCallback.setRequestsToDrop(requestsToDrop);
    for (PutOperation op : putOperations) {
      try {
        op.poll(requestRegistrationCallback);
      } catch (Exception e) {
        op.setOperationExceptionAndComplete(
            new RouterException("Put poll encountered unexpected error", e, RouterErrorCode.UnexpectedInternalError));
      }
      if (op.isOperationComplete() && putOperations.remove(op)) {
        // In order to ensure that an operation is completed only once, call onComplete() only at the place where the
        // operation actually gets removed from the set of operations. See comment within closePendingOperations().
        onComplete(op);
      }
    }
    routerMetrics.putManagerPollTimeMs.update(time.milliseconds() - startTime);
  }

  /**
   * Hands over the response to the associated PutOperation that issued the request.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    long startTime = time.milliseconds();
    PutResponse putResponse =
        RouterUtils.extractResponseAndNotifyResponseHandler(responseHandler, routerMetrics, responseInfo,
            PutResponse::readFrom, PutResponse::getError);
    RequestInfo routerRequestInfo = responseInfo.getRequestInfo();
    int correlationId = routerRequestInfo.getRequest().getCorrelationId();
    // Get the PutOperation that generated the request.
    PutOperation putOperation = correlationIdToPutOperation.remove(correlationId);
    // If it is still an active operation, hand over the response. Otherwise, ignore.
    // putOperation may be null if the operation was already completed in a previous event loop iteration.
    if (putOperation != null && putOperations.contains(putOperation)) {
      try {
        putOperation.handleResponse(responseInfo, putResponse);
      } catch (Exception e) {
        putOperation.setOperationExceptionAndComplete(
            new RouterException("Put handleResponse encountered unexpected error", e,
                RouterErrorCode.UnexpectedInternalError));
      }
      if (putOperation.isOperationComplete() && putOperations.remove(putOperation)) {
        onComplete(putOperation);
      }
      routerMetrics.putManagerHandleResponseTimeMs.update(time.milliseconds() - startTime);
    } else {
      logger.debug("Put operation not found in map for {} : {}", correlationId, putOperation);
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  /**
   * Called for a {@link PutOperation} when the operation is complete. Any cleanup that the PutManager needs to do
   * with respect to this operation will have to be done here. The PutManager also finishes the operation by
   * performing the callback and notification.
   * @param op the {@link PutOperation} that has completed.
   */
  private void onComplete(PutOperation op) {
    Exception e = op.getOperationException();
    String blobId = op.getBlobIdString();
    op.maybeNotifyForBlobCreation();
    if (blobId == null && e == null) {
      e = new RouterException("Operation failed, but exception was not set", RouterErrorCode.UnexpectedInternalError);
      routerMetrics.operationFailureWithUnsetExceptionCount.inc();
    }
    if (e != null) {
      blobId = null;
      routerMetrics.onPutBlobError(e, op.isEncryptionEnabled(), op.isStitchOperation());
      routerCallback.scheduleDeletes(op.getSuccessfullyPutChunkIdsIfCompositeDirectUpload(), op.getServiceId());
    } else {
      updateChunkingAndSizeMetricsOnSuccessfulPut(op);
    }
    routerMetrics.operationDequeuingRate.mark();
    long operationLatencyMs = time.milliseconds() - op.getSubmissionTimeMs();
    if (op.isStitchOperation()) {
      (op.isEncryptionEnabled() ? routerMetrics.stitchEncryptedBlobOperationLatencyMs
          : routerMetrics.stitchBlobOperationLatencyMs).update(operationLatencyMs);
    } else {
      (op.isEncryptionEnabled() ? routerMetrics.putEncryptedBlobOperationLatencyMs
          : routerMetrics.putBlobOperationLatencyMs).update(operationLatencyMs);
    }
    // Preemptively clean up of correlation IDs to avoid retaining a reference to the PutOperation for longer than
    // required. We do not care about any other
    for (Integer correlationId : op.getInFlightCorrelationIds()) {
      correlationIdToPutOperation.remove(correlationId);
    }
    NonBlockingRouter.completeOperation(op.getFuture(), op.getCallback(), blobId, e);
  }

  /**
   * Update chunking and size related metrics - blob size, chunk count, and whether the blob is simple or composite.
   * @param op the {@link PutOperation} that completed successfully.
   */
  private void updateChunkingAndSizeMetricsOnSuccessfulPut(PutOperation op) {
    routerMetrics.putBlobSizeBytes.update(op.getBlobProperties().getBlobSize());
    routerMetrics.putBlobChunkCount.update(op.getNumDataChunks());
    if (op.getNumDataChunks() == 1) {
      routerMetrics.simpleBlobPutCount.inc();
    } else {
      routerMetrics.compositeBlobPutCount.inc();
    }
  }

  /**
   * Check if the PutManager is open.
   */
  boolean isOpen() {
    return isOpen.get();
  }

  /**
   * Close the PutManager.
   * First notify the chunkFillerThread about closing and wait for it to exit. Then, complete all existing operations.
   */
  void close() {
    if (isOpen.compareAndSet(true, false)) {
      synchronized (chunkFillerSynchronizer) {
        if (isChunkFillerThreadAsleep) {
          chunkFillerThreadMaySleep = false;
          chunkFillerSynchronizer.notify();
        }
      }
      try {
        chunkFillerThread.join(NonBlockingRouter.SHUTDOWN_WAIT_MS);
      } catch (InterruptedException e) {
        logger.error("Caught interrupted exception while waiting for chunkFillerThread to finish");
        Thread.currentThread().interrupt();
      }
      completePendingOperations();
    }
  }

  /**
   * Complete all existing operations.
   * This can get called two ways:
   * 1. As part of {@link #close()} when it is called in the context of the router. This is the normal case.
   * 2. By the {@link ChunkFiller} thread when it exits abnormally.
   */
  void completePendingOperations() {
    for (PutOperation op : putOperations) {
      // There is a rare scenario where the operation gets removed from this set and gets completed concurrently by
      // the RequestResponseHandler thread when it is in poll() or handleResponse(). In order to avoid the completion
      // from happening twice, complete it here only if the remove was successful.
      if (putOperations.remove(op)) {
        Exception e = new RouterException("Aborted operation because Router is closed.", RouterErrorCode.RouterClosed);
        routerMetrics.operationDequeuingRate.mark();
        routerMetrics.operationAbortCount.inc();
        routerMetrics.onPutBlobError(e, op.isEncryptionEnabled(), op.isStitchOperation());
        NonBlockingRouter.completeOperation(op.getFuture(), op.getCallback(), null, e);
      }
    }
  }

  /**
   * the ChunkFiller thread continuously iterates over all the putOperations submitted, reads from the {@link
   * AsyncWritableChannel} associated with the operation, and fills in chunks. The channel will be populated
   * by the {@link ReadableStreamChannel} associated with the operation.
   */
  private class ChunkFiller implements Runnable {
    public void run() {
      try {
        while (isOpen.get()) {
          chunkFillerThreadMaySleep = true;
          for (PutOperation op : putOperations) {
            op.fillChunks();
            if (!op.isChunkFillingDone()) {
              chunkFillerThreadMaySleep = false;
            }
          }
          if (chunkFillerThreadMaySleep) {
            synchronized (chunkFillerSynchronizer) {
              while (chunkFillerThreadMaySleep && isOpen.get()) {
                isChunkFillerThreadAsleep = true;
                chunkFillerSynchronizer.wait();
              }
              isChunkFillerThreadAsleep = false;
            }
          }
        }
      } catch (Throwable e) {
        logger.error("Aborting, chunkFillerThread received an unexpected error:", e);
        routerMetrics.chunkFillerUnexpectedErrorCount.inc();
        if (isOpen.compareAndSet(true, false)) {
          completePendingOperations();
        }
      }
    }
  }
}
