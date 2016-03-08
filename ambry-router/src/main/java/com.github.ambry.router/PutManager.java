/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PutManager class is responsible for handling putBlob operations. PutManager creates a {@link PutOperation} for each
 * operation submitted to it, and tracks them.
 */
class PutManager {
  private static final Logger logger = LoggerFactory.getLogger(PutManager.class);

  private final ConcurrentSkipListMap<Long, PutOperation> putOperations;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final Thread chunkFillerThread;
  // This helps the PutManager quickly find the appropriate PutChunk to hand over the response to.
  // Requests are added before they are sent out and get cleaned up as and when responses come in.
  // Because there is a guaranteed response from the NetworkClient for every request sent out, entries
  // get cleaned up periodically.
  private final ConcurrentSkipListMap<Integer, PutOperation.PutChunk> correlationIdToPutChunk;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  private final NonBlockingRouter router;

  // shared by all PutOperations
  final ClusterMap clusterMap;
  final RouterConfig routerConfig;
  final ResponseHandler responseHandler;

  /**
   * @param router the {@link NonBlockingRouter} that this PutManager belongs to.
   */
  PutManager(NonBlockingRouter router) {
    this.router = router;
    clusterMap = router.clusterMap;
    responseHandler = router.responseHandler;
    routerConfig = router.routerConfig;
    notificationSystem = router.notificationSystem;
    time = router.time;
    putOperations = new ConcurrentSkipListMap<Long, PutOperation>();
    correlationIdToPutChunk = new ConcurrentSkipListMap<Integer, PutOperation.PutChunk>();
    chunkFillerThread = Utils.newThread("ChunkFillerThread", new ChunkFiller(), false);
    chunkFillerThread.start();
  }

  /**
   * Submit a put blob operation to be processed asynchronously.
   * @param operationId the operation id for the put operation.
   * @param blobProperties the blobProperties associated with the blob being put.
   * @param userMetaData the userMetaData associated with the blob being put.
   * @param channel the {@link ReadableStreamChannel} containing the blob content.
   * @param futureResult the {@link FutureResult} that contains the pending result of the operation.
   * @param callback the {@link Callback} object to be called on completion of the operation.
   */
  void submitPutBlobOperation(long operationId, BlobProperties blobProperties, byte[] userMetaData,
      final ReadableStreamChannel channel, FutureResult<String> futureResult, Callback<String> callback) {
    try {
      PutOperation putOperation =
          new PutOperation(this, operationId, blobProperties, userMetaData, channel, futureResult, callback, time);
      putOperations.put(operationId, putOperation);
    } catch (RouterException e) {
      logger.error("Error creating PutOperation with the given operation parameters", e);
      router.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * Creates and returns requests in the form of {@link RequestInfo} to be sent to data nodes in order to
   * complete put operations. Since this is the only method guaranteed to be called periodically by the chunkFiller
   * thread ({@link #handleResponse} gets called only if a response is received for a put operation),
   * any error handling or operation completion and cleanup also usually gets done in the context of this method.
   * @param requestListToFill list to be filled with the requests created
   * @throws RouterException if an error is encountered during the creation of new requests.
   */
  void poll(List<RequestInfo> requestListToFill) {
    Iterator<PutOperation> putOperationIterator = putOperations.values().iterator();
    while (putOperationIterator.hasNext()) {
      PutOperation op = putOperationIterator.next();
      op.fetchChunkPutRequests(requestListToFill);
    }
  }

  /**
   * Hands over the response to the associated PutChunk that issued the request.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    int correlationId = ((PutRequest) responseInfo.getRequest()).getCorrelationId();
    correlationIdToPutChunk.remove(correlationId).handleResponse(responseInfo);
  }

  /**
   * Associates a request (identified by its correlation id) with the corresponding PutChunk that issued it. This is
   * used to hand over the response for a request directly to the associated PutChunk for handling.
   * @param correlationId the correlation id of the request.
   * @param putChunk the PutChunk that issued the request.
   */
  void associateRequestWithChunk(int correlationId, PutOperation.PutChunk putChunk) {
    correlationIdToPutChunk.put(correlationId, putChunk);
  }

  /**
   * Returns a list of ids of successfully put chunks that were part of unsuccessful put operations.
   * @return list of ids to delete.
   */
  void getIdsToDelete(List<String> idsToDelete) {
    // @todo save and return ids of failed puts.
  }

  /**
   * Called by a {@link PutOperation} when the operation is complete. Any cleanup that the PutManager needs to do
   * with respect to this operation will have to be done here. The PutManager also finishes the operation by
   * performing the callback and notification.
   * @param operationId the operation id of the put operation that has completed.
   */
  void onComplete(long operationId) {
    PutOperation op = putOperations.remove(operationId);
    if (op.getOperationException() != null) {
      // @todo add blobs in the metadata chunk to ids_to_delete
    } else {
      notificationSystem.onBlobCreated(op.getBlobIdString(), op.getBlobProperties(), op.getUserMetadata());
    }
    router.completeOperation(op.getFuture(), op.getCallback(), op.getBlobIdString(), op.getOperationException());
  }

  /**
   * Close the PutManager.
   */
  void close() {
    isOpen.set(false);
    try {
      chunkFillerThread.join(NonBlockingRouter.SHUTDOWN_WAIT_MS);
    } catch (InterruptedException e) {
      logger.error("Caught interrupted exception while waiting for chunkFillerThread to finish");
      Thread.currentThread().interrupt();
    }
    Iterator<PutOperation> iter = putOperations.values().iterator();
    while (iter.hasNext()) {
      PutOperation op = iter.next();
      router.completeOperation(op.getFuture(), op.getCallback(), null,
          new RouterException("Cannot accept operation because Router is closed", RouterErrorCode.RouterClosed));
      iter.remove();
    }
  }

  /**
   * the ChunkFiller thread continuously iterates over all the putOperations submitted, reads from the {@link
   * AsyncWritableChannel} associated with the operation, and fills in chunks. The channel will be populated
   * by the {@link ReadableStreamChannel} associated with the operation.
   */
  private class ChunkFiller implements Runnable {
    private final int sleepTimeWhenIdleMs = 10;

    public void run() {
      while (isOpen.get()) {
        boolean allChunksFillComplete = true;
        Iterator<PutOperation> iter = putOperations.values().iterator();
        while (iter.hasNext()) {
          PutOperation op = iter.next();
          if (!op.fillChunks()) {
            allChunksFillComplete = false;
          }
        }
        if (allChunksFillComplete) {
          try {
            Thread.sleep(sleepTimeWhenIdleMs);
          } catch (InterruptedException e) {
            logger.info("Caught interrupted exception while sleeping", e);
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }
}
