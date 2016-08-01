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
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
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
  private final OperationCompleteCallback operationCompleteCallback;
  private final ReadyForPollCallback readyForPollCallback;
  private final ByteBufferAsyncWritableChannel.ChannelEventListener chunkArrivalListener;

  // shared by all PutOperations
  private final ClusterMap clusterMap;
  private final RouterConfig routerConfig;
  private final ResponseHandler responseHandler;
  private final NonBlockingRouterMetrics routerMetrics;

  private class PutRequestRegistrationCallbackImpl implements RequestRegistrationCallback<PutOperation> {
    private List<RequestInfo> requestListToFill;

    @Override
    public void registerRequestToSend(PutOperation putOperation, RequestInfo requestInfo) {
      requestListToFill.add(requestInfo);
      correlationIdToPutOperation.put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(), putOperation);
    }
  }

  // A single callback as this will never get called concurrently. The list of request to fill will be set as
  // appropriate before the callback is passed on to the PutOperations, every time.
  private final PutRequestRegistrationCallbackImpl requestRegistrationCallback =
      new PutRequestRegistrationCallbackImpl();

  /**
   * Create a PutManager
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param notificationSystem The {@link NotificationSystem} used for notifying blob creations.
   * @param routerConfig  The {@link RouterConfig} containing the configs for the PutManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param operationCompleteCallback The {@link OperationCompleteCallback} to use to complete operations.
   * @param readyForPollCallback The callback to be used to notify the router of any state changes within the
   *                             operations.
   * @param index the index of the {@link NonBlockingRouter.OperationController} in the {@link NonBlockingRouter}
   * @param time The {@link Time} instance to use.
   */
  PutManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      OperationCompleteCallback operationCompleteCallback, ReadyForPollCallback readyForPollCallback, int index,
      Time time) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.operationCompleteCallback = operationCompleteCallback;
    this.readyForPollCallback = readyForPollCallback;
    this.chunkArrivalListener = new ByteBufferAsyncWritableChannel.ChannelEventListener() {
      @Override
      public void onEvent(ByteBufferAsyncWritableChannel.EventType e) {
        synchronized (chunkFillerSynchronizer) {
          // At this point, the chunk for which this notification came in (if any) could already have been consumed by
          // the chunk filler, and this might unnecessarily wake it up from its sleep, which should be okay.
          chunkFillerThreadMaySleep = false;
          if (isChunkFillerThreadAsleep) {
            chunkFillerSynchronizer.notify();
          }
        }
      }
    };
    this.time = time;
    putOperations = Collections.newSetFromMap(new ConcurrentHashMap<PutOperation, Boolean>());
    correlationIdToPutOperation = new HashMap<Integer, PutOperation>();
    chunkFillerThread = Utils.newThread("ChunkFillerThread-" + index, new ChunkFiller(), true);
    chunkFillerThread.start();
    routerMetrics.initializePutManagerMetrics(chunkFillerThread);
  }

  /**
   * Submit a put blob operation to be processed asynchronously.
   * @param blobProperties the blobProperties associated with the blob being put.
   * @param userMetaData the userMetaData associated with the blob being put.
   * @param channel the {@link ReadableStreamChannel} containing the blob content.
   * @param futureResult the {@link FutureResult} that contains the pending result of the operation.
   * @param callback the {@link Callback} object to be called on completion of the operation.
   */
  void submitPutBlobOperation(BlobProperties blobProperties, byte[] userMetaData, ReadableStreamChannel channel,
      FutureResult<String> futureResult, Callback<String> callback) {
    try {
      PutOperation putOperation =
          new PutOperation(routerConfig, routerMetrics, clusterMap, responseHandler, blobProperties, userMetaData,
              channel, futureResult, callback, readyForPollCallback, chunkArrivalListener, time);
      putOperations.add(putOperation);
      putOperation.startReadingFromChannel();
    } catch (RouterException e) {
      routerMetrics.operationDequeuingRate.mark();
      routerMetrics.putBlobErrorCount.inc();
      routerMetrics.countError(e);
      operationCompleteCallback.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * Creates and returns requests in the form of {@link RequestInfo} to be sent to data nodes in order to
   * complete put operations. Since this is the only method guaranteed to be called periodically by the
   * RequestResponseHandler thread in the {@link NonBlockingRouter} ({@link #handleResponse} gets called only if a
   * response is received for a put operation), any error handling or operation completion and cleanup also usually
   * gets done in the context of this method.
   * @param requestListToFill list to be filled with the requests created
   */
  void poll(List<RequestInfo> requestListToFill) {
    long startTime = time.milliseconds();
    requestRegistrationCallback.requestListToFill = requestListToFill;
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
    PutResponse putResponse = extractPutResponseAndNotifyResponseHandler(responseInfo);
    RouterRequestInfo routerRequestInfo = (RouterRequestInfo) responseInfo.getRequestInfo();
    int correlationId = ((PutRequest) routerRequestInfo.getRequest()).getCorrelationId();
    // Get the PutOperation that generated the request.
    PutOperation putOperation = correlationIdToPutOperation.remove(correlationId);
    // If it is still an active operation, hand over the response. Otherwise, ignore.
    if (putOperations.contains(putOperation)) {
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
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  /**
   * Extract the {@link PutResponse} from the given {@link ResponseInfo}
   * @param responseInfo the {@link ResponseInfo} from which the {@link PutResponse} is to be extracted.
   * @return the extracted {@link PutResponse} if there is one; null otherwise.
   */
  private PutResponse extractPutResponseAndNotifyResponseHandler(ResponseInfo responseInfo) {
    PutResponse putResponse = null;
    ReplicaId replicaId = ((RouterRequestInfo) responseInfo.getRequestInfo()).getReplicaId();
    NetworkClientErrorCode networkClientErrorCode = responseInfo.getError();
    if (networkClientErrorCode != null) {
      logger.trace("Network client returned an error, notifying response handler");
      responseHandler.onRequestResponseException(replicaId, new IOException("NetworkClient error"));
    } else {
      try {
        putResponse = PutResponse.readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())));
        responseHandler.onRequestResponseError(replicaId, putResponse.getError());
      } catch (Exception e) {
        // Ignore. There is no value in notifying the response handler.
        logger.error("Response deserialization received unexpected error", e);
        routerMetrics.responseDeserializationErrorCount.inc();
      }
    }
    return putResponse;
  }

  /**
   * Returns a list of ids of successfully put chunks that were part of unsuccessful put operations.
   */
  void getIdsToDelete(List<String> idsToDelete) {
    // @todo save and return ids of failed puts.
  }

  /**
   * Called for a {@link PutOperation} when the operation is complete. Any cleanup that the PutManager needs to do
   * with respect to this operation will have to be done here. The PutManager also finishes the operation by
   * performing the callback and notification.
   * @param op the {@link PutOperation} that has completed.
   */
  void onComplete(PutOperation op) {
    Exception e = op.getOperationException();
    if (e != null) {
      // @todo add blobs in the metadata chunk to ids_to_delete
      routerMetrics.putBlobErrorCount.inc();
      routerMetrics.countError(e);
    } else {
      notificationSystem.onBlobCreated(op.getBlobIdString(), op.getBlobProperties(), op.getUserMetadata());
    }
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.putBlobOperationLatencyMs.update(time.milliseconds() - op.getSubmissionTimeMs());
    operationCompleteCallback
        .completeOperation(op.getFuture(), op.getCallback(), op.getBlobIdString(), op.getOperationException());
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
        routerMetrics.putBlobErrorCount.inc();
        routerMetrics.countError(e);
        operationCompleteCallback.completeOperation(op.getFuture(), op.getCallback(), null, e);
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
            if (!op.isChunkFillComplete()) {
              op.fillChunks();
              chunkFillerThreadMaySleep = false;
            }
          }
          if (chunkFillerThreadMaySleep) {
            synchronized (chunkFillerSynchronizer) {
              while (chunkFillerThreadMaySleep) {
                isChunkFillerThreadAsleep = true;
                chunkFillerSynchronizer.wait();
              }
              isChunkFillerThreadAsleep = false;
            }
          }
        }
      } catch (InterruptedException e) {
        logger.error("ChunkFillerThread was interrupted", e);
        if (isOpen.compareAndSet(true, false)) {
          completePendingOperations();
        }
      }
    }
  }
}
