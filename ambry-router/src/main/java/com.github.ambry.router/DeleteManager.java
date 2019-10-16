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
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles {@link DeleteOperation}. A {@code DeleteManager} keeps track of all the delete
 * operations that are assigned to it, and manages their states and life cycles.
 */
class DeleteManager {
  private final Set<DeleteOperation> deleteOperations;
  private final HashMap<Integer, DeleteOperation> correlationIdToDeleteOperation;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final ResponseHandler responseHandler;
  private final AccountService accountService;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ClusterMap clusterMap;
  private final RouterConfig routerConfig;
  private final RouterCallback routerCallback;

  private static final Logger logger = LoggerFactory.getLogger(DeleteManager.class);

  private final RequestRegistrationCallback<DeleteOperation> requestRegistrationCallback;

  /**
   * Creates a DeleteManager.
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param accountService The {@link AccountService} used for account/container id and name mapping.
   * @param notificationSystem The {@link NotificationSystem} used for notifying blob deletions.
   * @param routerConfig The {@link RouterConfig} containing the configs for the DeleteManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param routerCallback The {@link RouterCallback} to use for callbacks to the router.
   * @param time The {@link Time} instance to use.
   */
  DeleteManager(ClusterMap clusterMap, ResponseHandler responseHandler, AccountService accountService,
      NotificationSystem notificationSystem, RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      RouterCallback routerCallback, Time time) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.accountService = accountService;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.routerCallback = routerCallback;
    this.time = time;
    deleteOperations = ConcurrentHashMap.newKeySet();
    correlationIdToDeleteOperation = new HashMap<>();
    requestRegistrationCallback = new RequestRegistrationCallback<>(correlationIdToDeleteOperation);
  }

  /**
   * Submits a {@link DeleteOperation} to this {@code DeleteManager}.
   * @param blobIdStr The original blobId string for a {@link BlobId}.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
   * @param callback The {@link Callback} that will be called on completion of the request.
   * @throws RouterException if the blobIdStr is invalid.
   */
  void submitDeleteBlobOperation(String blobIdStr, String serviceId, FutureResult<Void> futureResult,
      Callback<Void> callback) throws RouterException {
    final BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
    if (blobId.getDatacenterId() != ClusterMapUtils.UNKNOWN_DATACENTER_ID
        && blobId.getDatacenterId() != clusterMap.getLocalDatacenterId()) {
      routerMetrics.deleteBlobNotOriginateLocalOperationRate.mark();
    }
    DeleteOperation deleteOperation =
        new DeleteOperation(clusterMap, routerConfig, routerMetrics, responseHandler, blobId, serviceId, callback, time,
            futureResult);
    deleteOperations.add(deleteOperation);
  }

  /**
   * Polls all delete operations and populates a list of {@link RequestInfo} to be sent to data nodes in order to
   * complete delete operations.
   * @param requestsToSend list to be filled with the requests created.
   * @param requestsToDrop list to be filled with the requests to drop.
   */
  public void poll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    long startTime = time.milliseconds();
    requestRegistrationCallback.setRequestsToSend(requestsToSend);
    requestRegistrationCallback.setRequestsToDrop(requestsToDrop);
    for (DeleteOperation op : deleteOperations) {
      boolean exceptionEncountered = false;
      try {
        op.poll(requestRegistrationCallback);
      } catch (Exception e) {
        exceptionEncountered = true;
        op.setOperationException(new RouterException("Delete poll encountered unexpected error", e,
            RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || op.isOperationComplete()) {
        if (deleteOperations.remove(op)) {
          // In order to ensure that an operation is completed only once, call onComplete() only at the place where the
          // operation actually gets removed from the set of operations. See comment within close().
          onComplete(op);
        }
      }
    }
    routerMetrics.deleteManagerPollTimeMs.update(time.milliseconds() - startTime);
  }

  /**
   * Handles responses received for each of the {@link DeleteOperation} within this delete manager.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    long startTime = time.milliseconds();
    DeleteResponse deleteResponse =
        RouterUtils.extractResponseAndNotifyResponseHandler(responseHandler, routerMetrics, responseInfo,
            DeleteResponse::readFrom, DeleteResponse::getError);
    RequestInfo routerRequestInfo = responseInfo.getRequestInfo();
    int correlationId = ((DeleteRequest) routerRequestInfo.getRequest()).getCorrelationId();
    DeleteOperation deleteOperation = correlationIdToDeleteOperation.remove(correlationId);
    // If it is still an active operation, hand over the response. Otherwise, ignore.
    if (deleteOperations.contains(deleteOperation)) {
      boolean exceptionEncountered = false;
      try {
        deleteOperation.handleResponse(responseInfo, deleteResponse);
      } catch (Exception e) {
        exceptionEncountered = true;
        deleteOperation.setOperationException(
            new RouterException("Delete handleResponse encountered unexpected error", e,
                RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || deleteOperation.isOperationComplete()) {
        if (deleteOperations.remove(deleteOperation)) {
          onComplete(deleteOperation);
        }
      }
      routerMetrics.deleteManagerHandleResponseTimeMs.update(time.milliseconds() - startTime);
    } else {
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  /**
   * Called when the delete operation is completed. The {@code DeleteManager} also finishes the delete operation
   * by performing the callback and notification.
   * @param op The {@link DeleteOperation} that has completed.
   */
  void onComplete(DeleteOperation op) {
    Exception e = op.getOperationException();
    if (e == null) {
      BlobId blobId = op.getBlobId();
      Pair<Account, Container> accountContainer =
          RouterUtils.getAccountContainer(accountService, blobId.getAccountId(), blobId.getContainerId());
      notificationSystem.onBlobDeleted(blobId.getID(), op.getServiceId(), accountContainer.getFirst(),
          accountContainer.getSecond());
    } else {
      routerMetrics.onDeleteBlobError(e);
    }
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.deleteBlobOperationLatencyMs.update(time.milliseconds() - op.getSubmissionTimeMs());
    NonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), op.getOperationResult(),
        op.getOperationException());
  }

  /**
   * Closes the {@code DeleteManager}. A {@code DeleteManager} can be closed for only once. Any further close action
   * will have no effect.
   */
  void close() {
    for (DeleteOperation op : deleteOperations) {
      // There is a rare scenario where the operation gets removed from this set and gets completed concurrently by
      // the RequestResponseHandler thread when it is in poll() or handleResponse(). In order to avoid the completion
      // from happening twice, complete it here only if the remove was successful.
      if (deleteOperations.remove(op)) {
        Exception e = new RouterException("Aborted operation because Router is closed.", RouterErrorCode.RouterClosed);
        routerMetrics.operationDequeuingRate.mark();
        routerMetrics.operationAbortCount.inc();
        routerMetrics.onDeleteBlobError(e);
        NonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), null, e);
      }
    }
  }
}
