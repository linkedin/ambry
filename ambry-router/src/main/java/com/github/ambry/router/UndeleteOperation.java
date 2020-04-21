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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Time;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages the internal state of a {@link UndeleteOperation} during its life cycle.
 */
public class UndeleteOperation {
  //Operation arguments
  private final RouterConfig routerConfig;
  private final BlobId blobId;
  private final String serviceId;
  private final FutureResult<Void> futureResult;
  private final Callback<Void> callback;
  private final Time time;
  private final NonBlockingRouterMetrics routerMetrics;
  private final long operationTimeMs;
  // Parameters associated with the state.
  // The operation tracker that tracks the state of this operation.
  private final OperationTracker operationTracker;
  // A map used to find inflight requests using a correlation id.
  private final Map<Integer, UndeleteRequestInfo> undeleteRequestInfos = new TreeMap<>();
  // The result of this operation to be set into FutureResult.
  private final Void operationResult = null;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // RouterErrorCode that is resolved from all the received ServerErrorCode for this operation.
  private RouterErrorCode resolvedRouterErrorCode;
  // Denotes whether the operation is complete.
  private boolean operationCompleted = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(UndeleteOperation.class);

  /**
   * Instantiates a {@link UndeleteOperation}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param routerConfig The {@link RouterConfig} that contains router-level configurations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to record all router-related metrics.
   * @param blobId The {@link BlobId} needs to be undeleted by this {@link UndeleteOperation}.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param operationTimeMs the time at which the operation occurred.
   * @param callback The {@link Callback} that is supplied by the caller.
   * @param time A {@link Time} reference.
   * @param futureResult The {@link FutureResult} that is returned to the caller.
   */
  UndeleteOperation(ClusterMap clusterMap, RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      BlobId blobId, String serviceId, long operationTimeMs, Callback<Void> callback, Time time,
      FutureResult<Void> futureResult) {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.blobId = blobId;
    this.serviceId = serviceId;
    this.futureResult = futureResult;
    this.callback = callback;
    this.time = time;
    this.operationTimeMs = operationTimeMs;
    byte blobDcId = blobId.getDatacenterId();
    String originatingDcName = clusterMap.getDatacenterName(blobDcId);
    this.operationTracker =
        new SimpleOperationTracker(routerConfig, RouterOperation.UndeleteOperation, blobId.getPartition(),
            originatingDcName, false);
  }

  /**
   * Gets a list of {@link UndeleteRequest} for sending to replicas.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request
   *                            that gets created as part of this poll operation.
   */
  void poll(RequestRegistrationCallback<UndeleteOperation> requestRegistrationCallback) {
  }

  /**
   * Handles a response for a Undelete operation. It determines whether the request was successful and updates
   * operation tracker. For the same operation, it is possible that different {@link ServerErrorCode} are received from
   * different replicas. These error codes are eventually resolved to a single {@link RouterErrorCode}.
   * @param responseInfo The {@link ResponseInfo} to be handled.
   * @param undeleteResponse The {@link UndeleteResponse} associated with this response.
   */
  void handleResponse(ResponseInfo responseInfo, UndeleteResponse undeleteResponse) {
  }

  /**
   * A wrapper class that is used to check if a request has been expired.
   */
  private class UndeleteRequestInfo {
    final long startTimeMs;
    final ReplicaId replica;

    UndeleteRequestInfo(long submissionTime, ReplicaId replica) {
      this.startTimeMs = submissionTime;
      this.replica = replica;
    }
  }

  /**
   * Set the exception associated with this operation.
   * If operationException exists, compare ErrorCodes of exception and existing operation Exception depending
   * on precedence level. An ErrorCode with a smaller precedence level overrides an ErrorCode with a larger precedence
   * level. Update the operationException if necessary.
   * @param exception the {@link RouterException} to possibly set.
   */
  void setOperationException(RouterException exception) {
    RouterUtils.replaceOperationException(operationException, exception, this::getPrecedenceLevel);
  }

  /**
   * Gets the precedence level for a {@link RouterErrorCode}. A precedence level is a relative priority assigned
   * to a {@link RouterErrorCode}. If a {@link RouterErrorCode} has not been assigned a precedence level, a
   * {@code Integer.MIN_VALUE} will be returned.
   * @param routerErrorCode The {@link RouterErrorCode} for which to get its precedence level.
   * @return The precedence level of the {@link RouterErrorCode}.
   */
  private int getPrecedenceLevel(RouterErrorCode routerErrorCode) {
    switch (routerErrorCode) {
      case BlobAuthorizationFailure:
        return 0;
      case BlobDeleted:
        return 1;
      case BlobExpired:
        return 2;
      case BlobUpdateNotAllowed:
        return 3;
      case AmbryUnavailable:
        return 4;
      case UnexpectedInternalError:
        return 5;
      case OperationTimedOut:
        return 6;
      case BlobDoesNotExist:
        return 7;
      default:
        return Integer.MIN_VALUE;
    }
  }

  /**
   * Returns whether the operation has completed.
   * @return whether the operation has completed.
   */
  boolean isOperationComplete() {
    return operationCompleted;
  }

  /**
   * Gets {@link BlobId} of this {@link UndeleteOperation}.
   * @return The {@link BlobId}.
   */
  BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the service ID for the service requesting this undelete operation.
   */
  String getServiceId() {
    return serviceId;
  }

  /**
   * Get the {@link FutureResult} for this {@link UndeleteOperation}.
   * @return The {@link FutureResult}.
   */
  FutureResult<Void> getFutureResult() {
    return futureResult;
  }

  /**
   * Gets the {@link Callback} for this {@link UndeleteOperation}.
   * @return The {@link Callback}.
   */
  Callback<Void> getCallback() {
    return callback;
  }

  /**
   * Gets the exception associated with this operation if it failed; null otherwise.
   * @return exception associated with this operation if it failed; null otherwise.
   */
  Exception getOperationException() {
    return operationException.get();
  }

  /**
   * Gets the result for this {@link UndeleteOperation}. In a {@link UndeleteOperation}, nothing is returned
   * to the caller as a result of this operation. Including this {@link Void} result is for consistency
   * with other operations.
   * @return Void.
   */
  Void getOperationResult() {
    return operationResult;
  }

  /**
   * @return the time (in ms) at which the operation was submitted
   */
  long getOperationTimeMs() {
    return operationTimeMs;
  }
}
