/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Time;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages the internal state of a {@link TtlUpdateOperation} during its life cycle.
 */
class TtlUpdateOperation {
  //Operation arguments
  private final RouterConfig routerConfig;
  private final BlobId blobId;
  private final String serviceId;
  private final FutureResult<Void> futureResult;
  private final Callback<Void> callback;
  private final Time time;
  private final NonBlockingRouterMetrics routerMetrics;
  private final long expiresAtMs;
  private final long operationTimeMs;
  // Parameters associated with the state.
  // The operation tracker that tracks the state of this operation.
  private final OperationTracker operationTracker;
  // A map used to find inflight requests using a correlation id.
  private final Map<Integer, TtlUpdateRequestInfo> ttlUpdateRequestInfos = new TreeMap<>();
  // The result of this operation to be set into FutureResult.
  private final Void operationResult = null;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // Denotes whether the operation is complete.
  private boolean operationCompleted = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(TtlUpdateOperation.class);

  /**
   * Instantiates a {@link TtlUpdateOperation}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param routerConfig The {@link RouterConfig} that contains router-level configurations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to record all router-related metrics.
   * @param blobId The {@link BlobId} whose TTL needs to be updated by this {@link TtlUpdateOperation}.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param expiresAtMs the new time at which the blob should expire.
   * @param operationTimeMs the time at which the operation occurred.
   * @param callback The {@link Callback} that is supplied by the caller.
   * @param time A {@link Time} reference.
   * @param futureResult The {@link FutureResult} that is returned to the caller.
   */
  TtlUpdateOperation(ClusterMap clusterMap, RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      BlobId blobId, String serviceId, long expiresAtMs, long operationTimeMs, Callback<Void> callback, Time time,
      FutureResult<Void> futureResult) {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.blobId = blobId;
    this.serviceId = serviceId;
    this.futureResult = futureResult;
    this.callback = callback;
    this.time = time;
    this.expiresAtMs = expiresAtMs;
    this.operationTimeMs = operationTimeMs;
    byte blobDcId = blobId.getDatacenterId();
    String originatingDcName = clusterMap.getDatacenterName(blobDcId);
    this.operationTracker =
        new SimpleOperationTracker(routerConfig, RouterOperation.TtlUpdateOperation, blobId.getPartition(),
            originatingDcName, false);
  }

  /**
   * Gets a list of {@link TtlUpdateRequest} for sending to replicas.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request
   *                            that gets created as part of this poll operation.
   */
  void poll(RequestRegistrationCallback<TtlUpdateOperation> requestRegistrationCallback) {
    cleanupExpiredInflightRequests(requestRegistrationCallback);
    checkAndMaybeComplete();
    if (!isOperationComplete()) {
      fetchRequests(requestRegistrationCallback);
    }
  }

  /**
   * Fetch {@link TtlUpdateRequest}s to send for the operation.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to use for addition of requests that
   *                                    need to be sent to the storage server
   */
  private void fetchRequests(RequestRegistrationCallback<TtlUpdateOperation> requestRegistrationCallback) {
    Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
    while (replicaIterator.hasNext()) {
      ReplicaId replica = replicaIterator.next();
      String hostname = replica.getDataNodeId().getHostname();
      Port port = RouterUtils.getPortToConnectTo(replica, routerConfig.routerEnableHttp2NetworkClient);
      TtlUpdateRequest ttlUpdateRequest = createTtlUpdateRequest();
      ttlUpdateRequestInfos.put(ttlUpdateRequest.getCorrelationId(),
          new TtlUpdateRequestInfo(time.milliseconds(), replica));
      RequestInfo requestInfo = new RequestInfo(hostname, port, ttlUpdateRequest, replica);
      requestRegistrationCallback.registerRequestToSend(this, requestInfo);
      replicaIterator.remove();
      if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
        LOGGER.trace("Making request with correlationId {} to a remote replica {} in {} ",
            ttlUpdateRequest.getCorrelationId(), replica.getDataNodeId(), replica.getDataNodeId().getDatacenterName());
        routerMetrics.crossColoRequestCount.inc();
      } else {
        LOGGER.trace("Making request with correlationId {} to a local replica {} ", ttlUpdateRequest.getCorrelationId(),
            replica.getDataNodeId());
      }
      routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).ttlUpdateRequestRate.mark();
    }
  }

  /**
   * @return a {@link TtlUpdateRequest} to send to a replica.
   */
  private TtlUpdateRequest createTtlUpdateRequest() {
    return new TtlUpdateRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
        blobId, expiresAtMs, operationTimeMs);
  }

  /**
   * Handles a response for a TTL update operation. It determines whether the request was successful and updates
   * operation tracker. For the same operation, it is possible that different {@link ServerErrorCode} are received from
   * different replicas. These error codes are eventually resolved to a single {@link RouterErrorCode}.
   * @param responseInfo The {@link ResponseInfo} to be handled.
   * @param ttlUpdateResponse The {@link TtlUpdateResponse} associated with this response.
   */
  void handleResponse(ResponseInfo responseInfo, TtlUpdateResponse ttlUpdateResponse) {
    TtlUpdateRequest ttlUpdateRequest = (TtlUpdateRequest) responseInfo.getRequestInfo().getRequest();
    TtlUpdateRequestInfo ttlUpdateRequestInfo = ttlUpdateRequestInfos.remove(ttlUpdateRequest.getCorrelationId());
    // ttlUpdateRequestInfo can be null if this request was timed out before this response is received. No
    // metric is updated here, as corresponding metrics have been updated when the request was timed out.
    if (ttlUpdateRequestInfo == null) {
      return;
    }
    ReplicaId replica = ttlUpdateRequestInfo.replica;
    long requestLatencyMs = time.milliseconds() - ttlUpdateRequestInfo.startTimeMs;
    routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
    routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).ttlUpdateRequestLatencyMs.update(requestLatencyMs);
    // Check the error code from NetworkClient.
    if (responseInfo.getError() != null) {
      LOGGER.debug("TtlUpdateRequest with response correlationId {} timed out for replica {} ",
          ttlUpdateRequest.getCorrelationId(), replica.getDataNodeId());
      onErrorResponse(replica, new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut));
    } else {
      if (ttlUpdateResponse == null) {
        LOGGER.debug(
            "TtlUpdateRequest with response correlationId {} received UnexpectedInternalError on response deserialization for replica {} ",
            ttlUpdateRequest.getCorrelationId(), replica.getDataNodeId());
        onErrorResponse(replica, new RouterException("Response deserialization received an unexpected error",
            RouterErrorCode.UnexpectedInternalError));
      } else {
        // The true case below should not really happen. This means a response has been received
        // not for its original request. We will immediately fail this operation.
        if (ttlUpdateResponse.getCorrelationId() != ttlUpdateRequest.getCorrelationId()) {
          LOGGER.error("The correlation id in the TtlUpdateResponse " + ttlUpdateResponse.getCorrelationId()
              + " is not the same as the correlation id in the associated TtlUpdateRequest: "
              + ttlUpdateRequest.getCorrelationId());
          routerMetrics.unknownReplicaResponseError.inc();
          onErrorResponse(replica,
              new RouterException("Received wrong response that is not for the corresponding request.",
                  RouterErrorCode.UnexpectedInternalError));
        } else {
          ServerErrorCode serverError = ttlUpdateResponse.getError();
          if (serverError == ServerErrorCode.No_Error || serverError == ServerErrorCode.Blob_Already_Updated) {
            operationTracker.onResponse(replica, TrackedRequestFinalState.SUCCESS);
            if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
              LOGGER.trace("Cross colo request successful for remote replica {} in {} ", replica.getDataNodeId(),
                  replica.getDataNodeId().getDatacenterName());
              routerMetrics.crossColoSuccessCount.inc();
            }
          } else if (serverError == ServerErrorCode.Disk_Unavailable) {
            LOGGER.debug(
                "Replica {} returned Disk_Unavailable for a Ttl update request with response correlationId : {} ",
                replica.getDataNodeId(), ttlUpdateResponse.getCorrelationId());
            operationTracker.onResponse(replica, TrackedRequestFinalState.DISK_DOWN);
            setOperationException(
                new RouterException("Server returned: " + serverError, RouterErrorCode.AmbryUnavailable));
            routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).ttlUpdateRequestErrorCount.inc();
          } else {
            LOGGER.debug("Replica {} returned an error {} for a Ttl update request with response correlationId : {} ",
                replica.getDataNodeId(), serverError, ttlUpdateResponse.getCorrelationId());
            RouterErrorCode routerErrorCode = processServerError(serverError);
            if (ttlUpdateResponse.getError() == ServerErrorCode.Blob_Authorization_Failure) {
              // this is a successful response and one that completes the operation regardless of whether the
              // success target has been reached or not.
              operationCompleted = true;
            }
            // any server error code that is not equal to ServerErrorCode.No_Error, the onErrorResponse should be invoked
            // because the operation itself doesn't succeed although the response in some cases is successful (i.e. Blob_Deleted)
            onErrorResponse(replica, new RouterException("Server returned: " + serverError, routerErrorCode));
          }
        }
      }
    }
    checkAndMaybeComplete();
  }

  /**
   * A wrapper class that is used to check if a request has been expired.
   */
  private class TtlUpdateRequestInfo {
    final long startTimeMs;
    final ReplicaId replica;

    TtlUpdateRequestInfo(long submissionTime, ReplicaId replica) {
      this.startTimeMs = submissionTime;
      this.replica = replica;
    }
  }

  /**
   * Goes through the inflight request list of this {@link TtlUpdateOperation} and remove those that
   * have been timed out.
   * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
   */
  private void cleanupExpiredInflightRequests(
      RequestRegistrationCallback<TtlUpdateOperation> requestRegistrationCallback) {
    Iterator<Map.Entry<Integer, TtlUpdateRequestInfo>> itr = ttlUpdateRequestInfos.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<Integer, TtlUpdateRequestInfo> ttlUpdateRequestInfoEntry = itr.next();
      int correlationId = ttlUpdateRequestInfoEntry.getKey();
      TtlUpdateRequestInfo ttlUpdateRequestInfo = ttlUpdateRequestInfoEntry.getValue();
      if (time.milliseconds() - ttlUpdateRequestInfo.startTimeMs > routerConfig.routerRequestTimeoutMs) {
        itr.remove();
        LOGGER.trace("TTL Request with correlationid {} in flight has expired for replica {} ", correlationId,
            ttlUpdateRequestInfo.replica.getDataNodeId());
        // Do not notify this as a failure to the response handler, as this timeout could simply be due to
        // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
        // response and the response handler will be notified accordingly.
        onErrorResponse(ttlUpdateRequestInfo.replica,
            RouterUtils.buildTimeoutException(correlationId, ttlUpdateRequestInfo.replica.getDataNodeId(), blobId));
        requestRegistrationCallback.registerRequestToDrop(correlationId);
      } else {
        // the entries are ordered by correlation id and time. Break on the first request that has not timed out.
        break;
      }
    }
  }

  /**
   * Processes {@link ServerErrorCode} received from {@code replica}. This method maps a {@link ServerErrorCode}
   * to a {@link RouterErrorCode}
   * @param serverErrorCode The ServerErrorCode received from the replica.
   * @return the {@link RouterErrorCode} mapped from server error code.
   */
  private RouterErrorCode processServerError(ServerErrorCode serverErrorCode) {
    switch (serverErrorCode) {
      case Blob_Authorization_Failure:
        return RouterErrorCode.BlobAuthorizationFailure;
      case Blob_Deleted:
        return RouterErrorCode.BlobDeleted;
      case Blob_Expired:
        return RouterErrorCode.BlobExpired;
      case Blob_Not_Found:
        return RouterErrorCode.BlobDoesNotExist;
      case Disk_Unavailable:
      case Replica_Unavailable:
        return RouterErrorCode.AmbryUnavailable;
      case Blob_Update_Not_Allowed:
        return RouterErrorCode.BlobUpdateNotAllowed;
      default:
        return RouterErrorCode.UnexpectedInternalError;
    }
  }

  /**
   * Perform the necessary actions when a request to a replica fails.
   * @param replicaId the {@link ReplicaId} associated with the failed response.
   * @param exception the {@link RouterException} associated with the failed response.
   */
  private void onErrorResponse(ReplicaId replicaId, RouterException exception) {
    operationTracker.onResponse(replicaId,
        TrackedRequestFinalState.fromRouterErrorCodeToFinalState(exception.getErrorCode()));
    setOperationException(exception);
    if (exception.getErrorCode() != RouterErrorCode.BlobDeleted
        && exception.getErrorCode() != RouterErrorCode.BlobExpired) {
      routerMetrics.routerRequestErrorCount.inc();
    }
    routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).ttlUpdateRequestErrorCount.inc();
  }

  /**
   * Completes the {@link TtlUpdateOperation} if it is done.
   */
  private void checkAndMaybeComplete() {
    // operationCompleted is true if Blob_Authorization_Failure was received.
    if (operationTracker.isDone() || operationCompleted) {
      if (operationTracker.hasSucceeded()) {
        operationException.set(null);
      } else if (operationTracker.hasFailedOnNotFound()) {
        operationException.set(
            new RouterException("TtlUpdateOperation failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist));
      }
      operationCompleted = true;
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
   * Gets {@link BlobId} of this {@link TtlUpdateOperation}.
   * @return The {@link BlobId}.
   */
  BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the service ID for the service requesting this TTL update operation.
   */
  String getServiceId() {
    return serviceId;
  }

  /**
   * Get the {@link FutureResult} for this {@link TtlUpdateOperation}.
   * @return The {@link FutureResult}.
   */
  FutureResult<Void> getFutureResult() {
    return futureResult;
  }

  /**
   * Gets the {@link Callback} for this {@link TtlUpdateOperation}.
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
   * Gets the result for this {@link TtlUpdateOperation}. In a {@link TtlUpdateOperation}, nothing is returned
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

  /**
   * @return the expiresAtMs that will be set for the blob by this operation
   */
  long getExpiresAtMs() {
    return expiresAtMs;
  }
}
