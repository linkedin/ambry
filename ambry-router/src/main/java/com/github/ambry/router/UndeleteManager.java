/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Handles {@link UndeleteOperation}. A {@code UndeleteManager} keeps track of all the Undelete
 * operations that are assigned to it, and manages their states and life cycles.
 */
public class UndeleteManager {
  private final ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final ResponseHandler responseHandler;
  private final AccountService accountService;
  private final NonBlockingRouterMetrics routerMetrics;
  private final RouterConfig routerConfig;
  private final Set<UndeleteOperation> undeleteOperations = ConcurrentHashMap.newKeySet();
  private final Map<Integer, UndeleteOperation> correlationIdToUndeleteOperation = new HashMap<>();
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  private final RequestRegistrationCallback<UndeleteOperation> requestRegistrationCallback =
      new RequestRegistrationCallback<>(correlationIdToUndeleteOperation);

  /**
   * Creates a UndeleteManager.
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param notificationSystem The {@link NotificationSystem} used for notifying undelete for blobs.
   * @param accountService The {@link AccountService} used for account/container id and name mapping.
   * @param routerConfig The {@link RouterConfig} containing the configs for the UndeleteManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param time The {@link Time} instance to use.
   */
  UndeleteManager(ClusterMap clusterMap, ResponseHandler responseHandler, NotificationSystem notificationSystem,
      AccountService accountService, RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, Time time) {
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.notificationSystem = notificationSystem;
    this.accountService = accountService;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.time = time;
  }

  /**
   * Submits {@link UndeleteOperation}(s) to this {@link UndeleteManager}.
   * @param blobIdStrs The original blobId strings
   * @param serviceId The service ID of the service undeleting the blob(s). This can be null if unknown.
   * @param futureResult The {@link FutureResult} that will contain the result eventually and exception if any.
   * @param callback The {@link Callback} that will be called on completion of the request.
   * @throws RouterException if the blobIdStr is invalid.
   */
  void submitUndeleteOperation(Collection<String> blobIdStrs, String serviceId, FutureResult<Void> futureResult,
      Callback<Void> callback) throws RouterException {
    if (!isOpen()) {
      throw new IllegalStateException("UndeleteManager is closed");
    }
    List<BlobId> blobIds = new ArrayList<>();
    for (String blobIdStr : blobIdStrs) {
      BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
      if (blobId.getDatacenterId() != ClusterMapUtils.UNKNOWN_DATACENTER_ID
          && blobId.getDatacenterId() != clusterMap.getLocalDatacenterId()) {
        routerMetrics.undeleteBlobNotOriginateLocalOperationRate.mark();
      }
      blobIds.add(blobId);
    }
    if (blobIds.size() == 1) {
      UndeleteOperation undeleteOperation =
          new UndeleteOperation(clusterMap, routerConfig, routerMetrics, blobIds.get(0), serviceId, time.milliseconds(),
              callback, time, futureResult);
      undeleteOperations.add(undeleteOperation);
    } else {
      BatchOperationCallbackTracker tracker = new BatchOperationCallbackTracker(blobIds, futureResult, callback);
      long operationTimeMs = time.milliseconds();
      for (BlobId blobId : blobIds) {
        UndeleteOperation undeleteOperation =
            new UndeleteOperation(clusterMap, routerConfig, routerMetrics, blobId, serviceId, operationTimeMs,
                tracker.getCallback(blobId), time, BatchOperationCallbackTracker.DUMMY_FUTURE);
        undeleteOperations.add(undeleteOperation);
      }
    }
  }

  /**
   * Polls all undelete operations and populates a list of {@link RequestInfo} to be sent to data nodes in order to
   * complete undelete operations.
   * @param requestsToSend list to be filled with the requests created.
   * @param requestsToDrop list to be filled with the requests to drop.
   */
  void poll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    long startTime = time.milliseconds();
    requestRegistrationCallback.setRequestsToSend(requestsToSend);
    requestRegistrationCallback.setRequestsToDrop(requestsToDrop);
    for (UndeleteOperation op : undeleteOperations) {
      boolean exceptionEncountered = false;
      try {
        op.poll(requestRegistrationCallback);
      } catch (Exception e) {
        exceptionEncountered = true;
        op.setOperationException(new RouterException("Undelete poll encountered unexpected error", e,
            RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || op.isOperationComplete()) {
        if (undeleteOperations.remove(op)) {
          // In order to ensure that an operation is completed only once, call onComplete() only at the place where the
          // operation actually gets removed from the set of operations. See comment within close().
          onComplete(op);
        }
      }
    }
    routerMetrics.undeleteManagerPollTimeMs.update(time.milliseconds() - startTime);
  }

  /**
   * Handles responses received for each of the {@link UndeleteOperation} within this UndeleteManager.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    long startTime = time.milliseconds();
    UndeleteResponse undeleteResponse =
        RouterUtils.extractResponseAndNotifyResponseHandler(responseHandler, routerMetrics, responseInfo,
            UndeleteResponse::readFrom, UndeleteResponse::getError);
    RequestInfo routerRequestInfo = responseInfo.getRequestInfo();
    int correlationId = routerRequestInfo.getRequest().getCorrelationId();
    UndeleteOperation undeleteOperation = correlationIdToUndeleteOperation.remove(correlationId);
    // If it is still an active operation, hand over the response. Otherwise, ignore.
    if (undeleteOperations.contains(undeleteOperation)) {
      boolean exceptionEncountered = false;
      try {
        undeleteOperation.handleResponse(responseInfo, undeleteResponse);
      } catch (Exception e) {
        exceptionEncountered = true;
        undeleteOperation.setOperationException(
            new RouterException("Undelete handleResponse encountered unexpected error", e,
                RouterErrorCode.UnexpectedInternalError));
      }
      if (exceptionEncountered || undeleteOperation.isOperationComplete()) {
        if (undeleteOperations.remove(undeleteOperation)) {
          onComplete(undeleteOperation);
        }
      }
      routerMetrics.undeleteManagerHandleResponseTimeMs.update(time.milliseconds() - startTime);
    } else {
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  /**
   * Called when the undelete operation is completed. The {@link UndeleteManager} also finishes the undelete
   * operation by performing the callback and notification.
   * @param op The {@link UndeleteOperation} that has completed.
   */
  private void onComplete(UndeleteOperation op) {
    Exception e = op.getOperationException();
    if (e == null) {
      BlobId blobId = op.getBlobId();
      Pair<Account, Container> accountContainer =
          RouterUtils.getAccountContainer(accountService, blobId.getAccountId(), blobId.getContainerId());
      notificationSystem.onBlobUndeleted(op.getBlobId().getID(), op.getServiceId(), accountContainer.getFirst(),
          accountContainer.getSecond());
    } else {
      routerMetrics.onUndeleteBlobError(e);
    }
    routerMetrics.operationDequeuingRate.mark();
    routerMetrics.undeleteBlobOperationLatencyMs.update(time.milliseconds() - op.getOperationTimeMs());
    NonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), op.getOperationResult(),
        op.getOperationException());
  }

  /**
   * Check if the UndeleteManager is open.
   */
  boolean isOpen() {
    return isOpen.get();
  }

  /**
   * Closes the {@link UndeleteManager}. A {@link UndeleteManager} can be closed for only once. Any further close action
   * will have no effect.
   */
  void close() {
    if (isOpen.compareAndSet(true, false)) {
      for (UndeleteOperation op : undeleteOperations) {
        // There is a rare scenario where the operation gets removed from this set and gets completed concurrently by
        // the RequestResponseHandler thread when it is in poll() or handleResponse(). In order to avoid the completion
        // from happening twice, complete it here only if the remove was successful.
        if (undeleteOperations.remove(op)) {
          Exception e =
              new RouterException("Aborted operation because Router is closed.", RouterErrorCode.RouterClosed);
          routerMetrics.operationDequeuingRate.mark();
          routerMetrics.operationAbortCount.inc();
          routerMetrics.onUndeleteBlobError(e);
          NonBlockingRouter.completeOperation(op.getFutureResult(), op.getCallback(), null, e);
        }
      }
    }
  }
}
