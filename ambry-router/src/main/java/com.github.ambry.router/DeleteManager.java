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
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.utils.Time;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DeleteManager that handles {@link DeleteOperation}. A {@code DeleteManager} keeps track of all the delete
 * operations that are assigned to it, and manages their states and life cycles.
 */
class DeleteManager {
  private final Set<DeleteOperation> deleteOperations;
  private final HashMap<Integer, DeleteOperation> correlationIdToDeleteOperation;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  private final ResponseHandler responseHandler;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ClusterMap clusterMap;
  private final RouterConfig routerConfig;

  private static final Logger logger = LoggerFactory.getLogger(DeleteManager.class);

  /**
   * Initialize a {@link DeleteManager}.
   */
  public DeleteManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, Time time) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.time = time;
    deleteOperations = Collections.newSetFromMap(new ConcurrentHashMap<DeleteOperation, Boolean>());
    correlationIdToDeleteOperation = new HashMap<Integer, DeleteOperation>();
  }

  /**
   * Submits a {@link DeleteOperation} to this {@code DeleteManager}.
   * @param blobIdString The blobId string to be deleted.
   * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
   * @param callback The {@link Callback} that will be called on completion of the request.
   */
  void submitDeleteBlobOperation(String blobIdString, FutureResult<Void> futureResult, Callback<Void> callback) {
    try {
      BlobId blobId = RouterUtils.getBlobIdFromString(blobIdString, clusterMap);
      DeleteOperation deleteOperation =
          new DeleteOperation(routerConfig, blobId, clusterMap, responseHandler, futureResult, callback,
              new DeleteRequestRegistrationCallbackImpl(), time);
      deleteOperations.add(deleteOperation);
    } catch (RouterException e) {
      NonBlockingRouter.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * Poll among all delete operations. For each polled {@link DeleteOperation}, its generates a number of
   * {@link RequestInfo} for actual sending.
   * @param requestInfos the list of {@link RequestInfo} to fill.
   */
  public void poll(List<RequestInfo> requestInfos) {
    Iterator<DeleteOperation> iter = deleteOperations.iterator();
    while (iter.hasNext()) {
      DeleteOperation deleteOperation = iter.next();
      deleteOperation.fetchRequest(requestInfos);
      if (deleteOperation.isOperationCompleted()) {
        onOperationComplete(deleteOperation);
      }
    }
  }

  /**
   * Handle responses received for each of the {@link DeleteOperation} within this delete manager.
   * @param responseInfo A response from {@link com.github.ambry.network.NetworkClient}
   */
  void handleResponse(ResponseInfo responseInfo) {
    DeleteRequest deleteRequest = (DeleteRequest) responseInfo.getRequest();
    DeleteOperation deleteOperation = correlationIdToDeleteOperation.remove(deleteRequest.getCorrelationId());
    if (deleteOperations.contains(deleteOperation)) {
      deleteOperation.handleResponse(responseInfo);
    } else {
      // This will happen if the DeleteOperation has already been finished by getting enough responses
      // or failed by some server error.
      logger.trace("Received response for a request of an operation that has completed.");
    }
  }

  /**
   * Called when the delete operation is completed. The {@code DeleteManager} also finishes the delete operation
   * by performing the callback and notification.
   * @param deleteOperation The {@lilnk DeleteOperation} that has completed.
   */
  void onOperationComplete(DeleteOperation deleteOperation) {
    if (deleteOperation.getOperationException() == null) {
      notificationSystem.onBlobDeleted(deleteOperation.getBlobId().getID());
    }
    NonBlockingRouter.completeOperation(deleteOperation.getFutureResult(), deleteOperation.getCallback(),
        deleteOperation.getOperationResult(), deleteOperation.getOperationException());
    deleteOperations.remove(deleteOperation);
  }

  /**
   * Close the {@code DeleteManager}. A {@code DeleteManager} can be closed for only once. Any further close action
   * will have no effect.
   */
  void close() {
    if (isOpen.compareAndSet(true, false)) {
      Iterator<DeleteOperation> iter = deleteOperations.iterator();
      while (iter.hasNext()) {
        DeleteOperation deleteOperation = iter.next();
        NonBlockingRouter.completeOperation(deleteOperation.getFutureResult(), deleteOperation.getCallback(), null,
            new RouterException("Cannot process operation because Router is closed.", RouterErrorCode.RouterClosed));
        iter.remove();
      }
    }
  }

  /**
   * Used by a {@link DeleteOperation} to associate a {@code CorrelationId} to a {@link DeleteOperation}.
   */
  private class DeleteRequestRegistrationCallbackImpl implements DeleteRequestRegistrationCallback {
    @Override
    public void registerRequestToSend(DeleteOperation deleteOperation, RequestInfo requestInfo) {
      correlationIdToDeleteOperation
          .put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(), deleteOperation);
    }
  }
}
