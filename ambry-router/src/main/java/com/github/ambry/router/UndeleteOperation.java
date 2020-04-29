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
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Time;
import java.util.Iterator;
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
  // Denotes whether the operation is complete.
  private boolean operationCompleted = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(UndeleteOperation.class);

  private Short lifeVersion = null;
  // The replica that sends back the first response
  private ReplicaId firstResponseReplicaId = null;

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
    this.operationTracker = new UndeleteOperationTracker(routerConfig, blobId.getPartition(),
        clusterMap.getDatacenterName(blobId.getDatacenterId()));
  }

  /**
   * Gets a list of {@link UndeleteRequest} for sending to replicas.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request
   *                            that gets created as part of this poll operation.
   */
  void poll(RequestRegistrationCallback<UndeleteOperation> requestRegistrationCallback) {
    cleanupExpiredInflightRequests(requestRegistrationCallback);
    checkAndMaybeComplete();
    if (!isOperationComplete()) {
      fetchRequests(requestRegistrationCallback);
    }
  }

  /**
   * Fetch {@link UndeleteRequest}s to send for the operation.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to use for addition of requests that
   *                                    need to be sent to the storage server
   */
  private void fetchRequests(RequestRegistrationCallback<UndeleteOperation> requestRegistrationCallback) {
    Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
    while (replicaIterator.hasNext()) {
      ReplicaId replica = replicaIterator.next();
      String hostname = replica.getDataNodeId().getHostname();
      Port port = RouterUtils.getPortToConnectTo(replica, routerConfig.routerEnableHttp2NetworkClient);
      UndeleteRequest undeleteRequest = createUndeleteRequest();
      undeleteRequestInfos.put(undeleteRequest.getCorrelationId(),
          new UndeleteRequestInfo(time.milliseconds(), replica));
      RequestInfo requestInfo = new RequestInfo(hostname, port, undeleteRequest, replica);
      requestRegistrationCallback.registerRequestToSend(this, requestInfo);
      replicaIterator.remove();
      if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
        LOGGER.trace("Making request with correlationId {} to a remote replica {} in {} ",
            undeleteRequest.getCorrelationId(), replica.getDataNodeId(), replica.getDataNodeId().getDatacenterName());
        routerMetrics.crossColoRequestCount.inc();
      } else {
        LOGGER.trace("Making request with correlationId {} to a local replica {} ", undeleteRequest.getCorrelationId(),
            replica.getDataNodeId());
      }
      routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).undeleteRequestRate.mark();
    }
  }

  /**
   * @return a {@link UndeleteRequest} to send to a replica.
   */
  private UndeleteRequest createUndeleteRequest() {
    return new UndeleteRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
        blobId, operationTimeMs);
  }

  /**
   * Handles a response for a Undelete operation. It determines whether the request was successful and updates
   * operation tracker. For the same operation, it is possible that different {@link ServerErrorCode} are received from
   * different replicas. These error codes are eventually resolved to a single {@link RouterErrorCode}.
   * @param responseInfo The {@link ResponseInfo} to be handled.
   * @param undeleteResponse The {@link UndeleteResponse} associated with this response.
   */
  void handleResponse(ResponseInfo responseInfo, UndeleteResponse undeleteResponse) {
    UndeleteRequest undeleteRequest = (UndeleteRequest) responseInfo.getRequestInfo().getRequest();
    UndeleteRequestInfo undeleteRequestInfo = undeleteRequestInfos.remove(undeleteRequest.getCorrelationId());
    // undeleteRequestInfo can be null if this request was timed out before this response is received. No
    // metric is updated here, as corresponding metrics have been updated when the request was timed out.
    if (undeleteRequestInfo == null) {
      return;
    }
    ReplicaId replica = undeleteRequestInfo.replica;
    long requestLatencyMs = time.milliseconds() - undeleteRequestInfo.startTimeMs;
    routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
    routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).undeleteRequestLatencyMs.update(requestLatencyMs);
    // Check the error code from NetworkClient.
    if (responseInfo.getError() != null) {
      LOGGER.trace("UndeleteRequest with response correlationId {} timed out for replica {} ",
          undeleteRequest.getCorrelationId(), replica.getDataNodeId());
      onErrorResponse(replica, new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut));
    } else {
      if (undeleteResponse == null) {
        LOGGER.trace(
            "UndeleteRequest with response correlationId {} received UnexpectedInternalError on response deserialization for replica {} ",
            undeleteRequest.getCorrelationId(), replica.getDataNodeId());
        onErrorResponse(replica, new RouterException("Response deserialization received an unexpected error",
            RouterErrorCode.UnexpectedInternalError));
      } else {
        // The true case below should not really happen. This means a response has been received
        // not for its original request. We will immediately fail this operation.
        if (undeleteResponse.getCorrelationId() != undeleteRequest.getCorrelationId()) {
          LOGGER.error("The correlation id in the DeleteResponse " + undeleteResponse.getCorrelationId()
              + " is not the same as the correlation id in the associated DeleteRequest: "
              + undeleteRequest.getCorrelationId());
          routerMetrics.unknownReplicaResponseError.inc();
          onErrorResponse(replica,
              new RouterException("Received wrong response that is not for the corresponding request.",
                  RouterErrorCode.UnexpectedInternalError));
        } else {
          ServerErrorCode serverError = undeleteResponse.getError();
          if (serverError == ServerErrorCode.No_Error) {
            operationTracker.onResponse(replica, TrackedRequestFinalState.SUCCESS);
            if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
              LOGGER.trace("Cross colo request successful for remote replica {} in {} ", replica.getDataNodeId(),
                  replica.getDataNodeId().getDatacenterName());
              routerMetrics.crossColoSuccessCount.inc();
            }
            if (lifeVersion == null) {
              // This is first successful response.
              lifeVersion = undeleteResponse.getLifeVersion();
              firstResponseReplicaId = replica;
            } else {
              if (lifeVersion != undeleteResponse.getLifeVersion()) {
                String message = String.format(
                    "LifeVersion from Replica {} is different than the lifeVersion from replica {}, {} != {}",
                    firstResponseReplicaId, replica, lifeVersion, undeleteResponse.getLifeVersion());
                LOGGER.error(message);
                // this is a successful response and one that completes the operation regardless of whether the
                // success target has been reached or not.
                operationCompleted = true;
                onErrorResponse(replica, new RouterException(message, RouterErrorCode.LifeVersionConflict));
              }
            }
          } else if (serverError == ServerErrorCode.Disk_Unavailable) {
            LOGGER.trace("Replica {} returned Disk_Unavailable for an undelete request with correlationId : {} ",
                replica, undeleteRequest.getCorrelationId());
            operationTracker.onResponse(replica, TrackedRequestFinalState.DISK_DOWN);
            setOperationException(
                new RouterException("Server returned: " + serverError, RouterErrorCode.AmbryUnavailable));
            routerMetrics.routerRequestErrorCount.inc();
            routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).undeleteRequestErrorCount.inc();
          } else {
            LOGGER.trace("Replica {} returned an error {} for an undelete request with response correlationId : {} ",
                replica, serverError, undeleteRequest.getCorrelationId());
            RouterErrorCode routerErrorCode = processServerError(serverError);
            if (serverError == ServerErrorCode.Blob_Authorization_Failure) {
              // this is a successful response and one that completes the operation regardless of whether the
              // success target has been reached or not.
              operationCompleted = true;
            }
            // any server error code that is not equal to ServerErrorCode.No_Error, the onErrorResponse should be invoked
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
  private class UndeleteRequestInfo {
    final long startTimeMs;
    final ReplicaId replica;

    UndeleteRequestInfo(long submissionTime, ReplicaId replica) {
      this.startTimeMs = submissionTime;
      this.replica = replica;
    }
  }

  /**
   * Goes through the inflight request list of this {@link UndeleteOperation} and remove those that
   * have been timed out.
   * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
   */
  private void cleanupExpiredInflightRequests(
      RequestRegistrationCallback<UndeleteOperation> requestRegistrationCallback) {
    Iterator<Map.Entry<Integer, UndeleteRequestInfo>> iter = undeleteRequestInfos.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Integer, UndeleteRequestInfo> undeleteRequestInfoEntry = iter.next();
      int correlationId = undeleteRequestInfoEntry.getKey();
      UndeleteRequestInfo undeleteRequestInfo = undeleteRequestInfoEntry.getValue();
      if (time.milliseconds() - undeleteRequestInfo.startTimeMs > routerConfig.routerRequestTimeoutMs) {
        iter.remove();
        LOGGER.warn("Undelete request with correlationid {} in flight has expired for replica {} ", correlationId,
            undeleteRequestInfo.replica.getDataNodeId());
        // Do not notify this as a failure to the response handler, as this timeout could simply be due to
        // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
        // response and the response handler will be notified accordingly.
        onErrorResponse(undeleteRequestInfo.replica,
            RouterUtils.buildTimeoutException(correlationId, undeleteRequestInfo.replica.getDataNodeId(), blobId));
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
      case Blob_Not_Deleted:
        return RouterErrorCode.BlobNotDeleted;
      case Blob_Already_Undeleted:
        return RouterErrorCode.BlobUndeleted;
      case Blob_Life_Version_Conflict:
        return RouterErrorCode.LifeVersionConflict;
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
    routerMetrics.routerRequestErrorCount.inc();
    routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).undeleteRequestErrorCount.inc();
  }

  /**
   * Completes the {@code UndeleteOperation} if it is done.
   */
  private void checkAndMaybeComplete() {
    // operationCompleted is true if Blob_Authorization_Failure was received.
    if (operationTracker.isDone() || operationCompleted) {
      if (operationTracker.hasSucceeded()) {
        operationException.set(null);
      } else if (operationTracker.hasFailedOnNotFound()) {
        operationException.set(
            new RouterException("UndeleteOperation failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist));
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
      case LifeVersionConflict:
        return 1;
      case BlobExpired:
        return 2;
      case AmbryUnavailable:
        return 3;
      case UnexpectedInternalError:
        return 4;
      case OperationTimedOut:
        return 5;
      case BlobDoesNotExist:
        return 6;
      case BlobNotDeleted:
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
