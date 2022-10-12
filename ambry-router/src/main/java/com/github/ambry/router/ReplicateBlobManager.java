/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.ReplicateBlobResponse;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Handles {@link ReplicateBlobOperation}. A {@code ReplicateBlobManager} keeps track of all the ReplicateBlob
 * operations that are assigned to it, and manages their states and life cycles.
 */
class ReplicateBlobManager {
  private final Set<ReplicateBlobOperation> replicateBlobOperations;
  private final HashMap<Integer, ReplicateBlobOperation> correlationIdToReplicateBlobOperation;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final ResponseHandler responseHandler;
  private final AccountService accountService;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ClusterMap clusterMap;
  private final RouterConfig routerConfig;
  private final NonBlockingRouter nonBlockingRouter;

  private final RequestRegistrationCallback<ReplicateBlobOperation> requestRegistrationCallback;

  /**
   * Creates a ReplicateBlobManager.
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param accountService The {@link AccountService} used for account/container id and name mapping.
   * @param notificationSystem The {@link NotificationSystem} used for notifying blob replication.
   * @param routerConfig The {@link RouterConfig} containing the configs for the ReplicateBlobManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param time The {@link Time} instance to use.
   * @param nonBlockingRouter The non-blocking router object
   */
  ReplicateBlobManager(ClusterMap clusterMap, ResponseHandler responseHandler, AccountService accountService,
      NotificationSystem notificationSystem, RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      Time time, NonBlockingRouter nonBlockingRouter) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.accountService = accountService;
    this.notificationSystem = notificationSystem;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.time = time;
    this.nonBlockingRouter = nonBlockingRouter;
    replicateBlobOperations = ConcurrentHashMap.newKeySet();
    correlationIdToReplicateBlobOperation = new HashMap<>();
    requestRegistrationCallback = new RequestRegistrationCallback<>(correlationIdToReplicateBlobOperation);
  }

  /**
   * Submits a {@link ReplicateBlobOperation} to this {@code ReplicateBlobManager}.
   * @param blobIdStr The original blobId string for a {@link BlobId}.
   * @param serviceId The service ID of the service replicating the blob. This can be null if unknown.
   * @param sourceDataNode The source {@link DataNodeId} to get the blob from.
   * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
   * @param callback The {@link Callback} that will be called on completion of the request.
   * @throws RouterException if blob replication failed.
   */
  void submitReplicateBlobOperation(String blobIdStr, String serviceId, DataNodeId sourceDataNode,
      FutureResult<Void> futureResult, Callback<Void> callback) throws RouterException {
    final BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
    ReplicateBlobOperation replicateBlobOperation =
        new ReplicateBlobOperation(clusterMap, routerConfig, routerMetrics, blobId, serviceId, sourceDataNode, callback,
            time, futureResult);
    replicateBlobOperations.add(replicateBlobOperation);
  }

  /**
   * Polls all ReplicateBlob operations and populates a list of {@link RequestInfo} to be sent to data nodes in order to
   * complete ReplicateBlob operations.
   * @param requestsToSend list to be filled with the requests created.
   * @param requestsToDrop list to be filled with the requests to drop.
   */
  public void poll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    long startTime = time.milliseconds();
    requestRegistrationCallback.setRequestsToSend(requestsToSend);
    requestRegistrationCallback.setRequestsToDrop(requestsToDrop);
    for (ReplicateBlobOperation op : replicateBlobOperations) {
      boolean exceptionEncountered = false;
      try {
        op.poll(requestRegistrationCallback);
      } catch (Exception e) {
        exceptionEncountered = true;
        op.setOperationException(new RouterException("ReplicateBlob poll encountered unexpected error", e,
            RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || op.isOperationComplete()) {
        if (replicateBlobOperations.remove(op)) {
          // In order to ensure that an operation is completed only once, call onComplete() only at the place where the
          // operation actually gets removed from the set of operations. See comment within close().
          onComplete(op);
        }
      }
    }
    routerMetrics.replicateBlobManagerPollTimeMs.update(time.milliseconds() - startTime);
  }

  /**
   * Handles responses received for each of the {@link ReplicateBlobOperation} within this ReplicateBlob manager.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    long startTime = time.milliseconds();
    ReplicateBlobResponse replicateBlobResponse =
        RouterUtils.extractResponseAndNotifyResponseHandler(responseHandler, routerMetrics, responseInfo,
            ReplicateBlobResponse::readFrom, ReplicateBlobResponse::getError);
    RequestInfo routerRequestInfo = responseInfo.getRequestInfo();
    int correlationId = routerRequestInfo.getRequest().getCorrelationId();
    ReplicateBlobOperation replicateBlobOperation = correlationIdToReplicateBlobOperation.remove(correlationId);
    // If it is still an active operation, hand over the response. Otherwise, ignore.
    if (replicateBlobOperations.contains(replicateBlobOperation)) {
      boolean exceptionEncountered = false;
      try {
        replicateBlobOperation.handleResponse(responseInfo, replicateBlobResponse);
      } catch (Exception e) {
        exceptionEncountered = true;
        replicateBlobOperation.setOperationException(
            new RouterException("ReplicateBlob handleResponse encountered unexpected error", e,
                RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || replicateBlobOperation.isOperationComplete()) {
        if (replicateBlobOperations.remove(replicateBlobOperation)) {
          onComplete(replicateBlobOperation);
        }
      }
      routerMetrics.replicateBlobManagerHandleResponseTimeMs.update(time.milliseconds() - startTime);
    } else {
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  /**
   * Called when the ReplicateBlob operation is completed. The {@code ReplicateBlobManager} also finishes the ReplicateBlob operation
   * by performing the callback and notification.
   * @param op The {@link ReplicateBlobOperation} that has completed.
   */
  void onComplete(ReplicateBlobOperation op) {
    Exception e = op.getOperationException();
    if (e == null) {
      BlobId blobId = op.getBlobId();
      Pair<Account, Container> accountContainer =
          RouterUtils.getAccountContainer(accountService, blobId.getAccountId(), blobId.getContainerId());
      notificationSystem.onBlobReplicated(blobId.getID(), op.getServiceId(), accountContainer.getFirst(),
          accountContainer.getSecond(), op.getSourceDataNode());
    } else {
      routerMetrics.onReplicateBlobError(e);
    }
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.replicateBlobOperationLatencyMs.update(time.milliseconds() - op.getSubmissionTimeMs());
    nonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), op.getOperationResult(),
        op.getOperationException());
  }

  /**
   * Closes the {@code ReplicateBlobManager}. A {@code ReplicateBlobManager} can be closed for only once. Any further close action
   * will have no effect.
   */
  void close() {
    for (ReplicateBlobOperation op : replicateBlobOperations) {
      // There is a rare scenario where the operation gets removed from this set and gets completed concurrently by
      // the RequestResponseHandler thread when it is in poll() or handleResponse(). In order to avoid the completion
      // from happening twice, complete it here only if the remove was successful.
      if (replicateBlobOperations.remove(op)) {
        Exception e = new RouterException("Aborted operation because Router is closed.", RouterErrorCode.RouterClosed);
        routerMetrics.operationDequeuingRate.mark();
        routerMetrics.operationAbortCount.inc();
        routerMetrics.onReplicateBlobError(e);
        nonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), null, e);
      }
    }
  }
}