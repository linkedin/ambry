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
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Time;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages the internal state of a {@code DeleteOperation} during its life cycle. A {@code DeleteOperation}
 * can be issued to two types of blobs.
 * Simple blob: A single blob that is under the max put chunk size. The {@code DeleteOperation} will be issued to the
 * actual blob.
 * Composite blob: A blob consists of a number of data blobs and a metadata blob that manages the medadata of all the
 * data blobs. The {@code DeleteOperation} is issued only to delete the metadata blob.
 */
class DeleteOperation {
  //Operation arguments
  private final RouterConfig routerConfig;
  private final ResponseHandler responseHandler;
  private final BlobId blobId;
  private final String serviceId;
  private final FutureResult<Void> futureResult;
  private final Callback<Void> callback;
  private final Time time;
  private final NonBlockingRouterMetrics routerMetrics;
  private final long submissionTimeMs;
  private final long deletionTimeMs;

  // Parameters associated with the state.

  // The operation tracker that tracks the state of this operation.
  private final OperationTracker operationTracker;
  // A map used to find inflight requests using a correlation id.
  private final Map<Integer, DeleteRequestInfo> deleteRequestInfos;
  // The result of this operation to be set into FutureResult.
  private final Void operationResult = null;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // Denotes whether the operation is complete.
  private boolean operationCompleted = false;

  private static final Logger logger = LoggerFactory.getLogger(DeleteOperation.class);

  /**
   * Instantiates a {@link DeleteOperation}.
   * @param routerConfig The {@link RouterConfig} that contains router-level configurations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to record all router-related metrics.
   * @param responsehandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param blobId The {@link BlobId} that is to be deleted by this {@code DeleteOperation}.
   * @param serviceId The service ID of the service deleting the blob. This can be null if unknown.
   * @param callback The {@link Callback} that is supplied by the caller.
   * @param time A {@link Time} reference.
   * @param futureResult The {@link FutureResult} that is returned to the caller.
   */
  DeleteOperation(ClusterMap clusterMap, RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      ResponseHandler responsehandler, BlobId blobId, String serviceId, Callback<Void> callback, Time time,
      FutureResult<Void> futureResult) {
    this.submissionTimeMs = time.milliseconds();
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.responseHandler = responsehandler;
    this.blobId = blobId;
    this.serviceId = serviceId;
    this.futureResult = futureResult;
    this.callback = callback;
    this.time = time;
    this.deletionTimeMs = time.milliseconds();
    this.deleteRequestInfos = new TreeMap<Integer, DeleteRequestInfo>();
    byte blobDcId = blobId.getDatacenterId();
    String originatingDcName = clusterMap.getDatacenterName(blobDcId);
    this.operationTracker =
        new SimpleOperationTracker(routerConfig, RouterOperation.DeleteOperation, blobId.getPartition(),
            originatingDcName, false);
  }

  /**
   * Gets a list of {@link DeleteRequest} for sending to replicas.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request
   *                            that gets created as part of this poll operation.
   */
  void poll(RequestRegistrationCallback<DeleteOperation> requestRegistrationCallback) {
    cleanupExpiredInflightRequests(requestRegistrationCallback);
    checkAndMaybeComplete();
    if (!isOperationComplete()) {
      fetchRequests(requestRegistrationCallback);
    }
  }

  /**
   * Fetch {@link DeleteRequest}s to send for the operation.
   */
  private void fetchRequests(RequestRegistrationCallback<DeleteOperation> requestRegistrationCallback) {
    Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
    while (replicaIterator.hasNext()) {
      ReplicaId replica = replicaIterator.next();
      String hostname = replica.getDataNodeId().getHostname();
      Port port = RouterUtils.getPortToConnectTo(replica, routerConfig.routerEnableHttp2NetworkClient);
      DeleteRequest deleteRequest = createDeleteRequest();
      deleteRequestInfos.put(deleteRequest.getCorrelationId(), new DeleteRequestInfo(time.milliseconds(), replica));
      RequestInfo requestInfo = new RequestInfo(hostname, port, deleteRequest, replica);
      requestRegistrationCallback.registerRequestToSend(this, requestInfo);
      replicaIterator.remove();
      if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
        logger.trace("Making request with correlationId {} to a remote replica {} in {} ",
            deleteRequest.getCorrelationId(), replica.getDataNodeId(), replica.getDataNodeId().getDatacenterName());
        routerMetrics.crossColoRequestCount.inc();
      } else {
        logger.trace("Making request with correlationId {} to a local replica {} ", deleteRequest.getCorrelationId(),
            replica.getDataNodeId());
      }
      routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).deleteRequestRate.mark();
    }
  }

  /**
   * Create a {@link DeleteRequest} for sending to a replica.
   * @return The DeleteRequest.
   */
  private DeleteRequest createDeleteRequest() {
    return new DeleteRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
        blobId, deletionTimeMs);
  }

  /**
   * Handles a response for a delete operation. It determines whether the request was successful,
   * updates operation tracker, and notifies the response handler for failure detection.
   * can be different cases during handling a response. For the same delete operation, it is possible
   * that different {@link ServerErrorCode} are received from different replicas. These error codes
   * are eventually resolved to a single {@link RouterErrorCode}.
   * @param responseInfo The {@link ResponseInfo} to be handled.
   * @param deleteResponse The {@link DeleteResponse} associated with this response.
   */
  void handleResponse(ResponseInfo responseInfo, DeleteResponse deleteResponse) {
    DeleteRequest deleteRequest = (DeleteRequest) responseInfo.getRequestInfo().getRequest();
    DeleteRequestInfo deleteRequestInfo = deleteRequestInfos.remove(deleteRequest.getCorrelationId());
    // deleteRequestInfo can be null if this request was timed out before this response is received. No
    // metric is updated here, as corresponding metrics have been updated when the request was timed out.
    if (deleteRequestInfo == null) {
      return;
    }
    ReplicaId replica = deleteRequestInfo.replica;
    long requestLatencyMs = time.milliseconds() - deleteRequestInfo.startTimeMs;
    routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
    routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).deleteRequestLatencyMs.update(requestLatencyMs);
    // Check the error code from NetworkClient.
    if (responseInfo.getError() != null) {
      logger.trace("DeleteRequest with response correlationId {} timed out for replica {} ",
          deleteRequest.getCorrelationId(), replica.getDataNodeId());
      onErrorResponse(replica, new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut));
    } else {
      if (deleteResponse == null) {
        logger.trace(
            "DeleteRequest with response correlationId {} received UnexpectedInternalError on response deserialization for replica {} ",
            deleteRequest.getCorrelationId(), replica.getDataNodeId());
        onErrorResponse(replica, new RouterException("Response deserialization received an unexpected error",
            RouterErrorCode.UnexpectedInternalError));
      } else {
        // The true case below should not really happen. This means a response has been received
        // not for its original request. We will immediately fail this operation.
        if (deleteResponse.getCorrelationId() != deleteRequest.getCorrelationId()) {
          logger.error("The correlation id in the DeleteResponse " + deleteResponse.getCorrelationId()
              + " is not the same as the correlation id in the associated DeleteRequest: "
              + deleteRequest.getCorrelationId());
          routerMetrics.unknownReplicaResponseError.inc();
          onErrorResponse(replica,
              new RouterException("Received wrong response that is not for the corresponding request.",
                  RouterErrorCode.UnexpectedInternalError));
        } else {
          ServerErrorCode serverError = deleteResponse.getError();
          if (serverError == ServerErrorCode.No_Error || serverError == ServerErrorCode.Blob_Deleted) {
            operationTracker.onResponse(replica, TrackedRequestFinalState.SUCCESS);
            if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
              logger.trace("Cross colo request successful for remote replica {} in {} ", replica.getDataNodeId(),
                  replica.getDataNodeId().getDatacenterName());
              routerMetrics.crossColoSuccessCount.inc();
            }
          } else if (serverError == ServerErrorCode.Disk_Unavailable) {
            logger.trace("Replica {} returned Disk_Unavailable for a delete request with correlationId : {} ", replica,
                deleteRequest.getCorrelationId());
            operationTracker.onResponse(replica, TrackedRequestFinalState.DISK_DOWN);
            setOperationException(
                new RouterException("Server returned: " + serverError, RouterErrorCode.AmbryUnavailable));
            routerMetrics.routerRequestErrorCount.inc();
            routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).deleteRequestErrorCount.inc();
          } else {
            logger.trace("Replica {} returned an error {} for a delete request with response correlationId : {} ",
                replica, serverError, deleteRequest.getCorrelationId());
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
  private class DeleteRequestInfo {
    final long startTimeMs;
    final ReplicaId replica;

    DeleteRequestInfo(long submissionTime, ReplicaId replica) {
      this.startTimeMs = submissionTime;
      this.replica = replica;
    }
  }

  /**
   * Goes through the inflight request list of this {@code DeleteOperation} and remove those that
   * have been timed out.
   * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
   */
  private void cleanupExpiredInflightRequests(
      RequestRegistrationCallback<DeleteOperation> requestRegistrationCallback) {
    Iterator<Map.Entry<Integer, DeleteRequestInfo>> itr = deleteRequestInfos.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<Integer, DeleteRequestInfo> entry = itr.next();
      int correlationId = entry.getKey();
      DeleteRequestInfo deleteRequestInfo = entry.getValue();
      if (time.milliseconds() - deleteRequestInfo.startTimeMs > routerConfig.routerRequestTimeoutMs) {
        itr.remove();
        logger.trace("Delete Request with correlationId {} in flight has expired for replica {} ", correlationId,
            deleteRequestInfo.replica.getDataNodeId());
        // Do not notify this as a failure to the response handler, as this timeout could simply be due to
        // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
        // response and the response handler will be notified accordingly.
        onErrorResponse(deleteRequestInfo.replica,
            RouterUtils.buildTimeoutException(correlationId, deleteRequestInfo.replica.getDataNodeId(), blobId));
        requestRegistrationCallback.registerRequestToDrop(correlationId);
      } else {
        // the entries are ordered by correlation id and time. Break on the first request that has not timed out.
        break;
      }
    }
  }

  /**
   * Processes {@link ServerErrorCode} received from {@code replica}. This method maps a {@link ServerErrorCode}
   * to a {@link RouterErrorCode}.
   * @param serverErrorCode The ServerErrorCode received from the replica.
   * @return the {@link RouterErrorCode} mapped from server error code.
   */
  private RouterErrorCode processServerError(ServerErrorCode serverErrorCode) {
    switch (serverErrorCode) {
      case Blob_Authorization_Failure:
        return RouterErrorCode.BlobAuthorizationFailure;
      case Blob_Expired:
        return RouterErrorCode.BlobExpired;
      case Blob_Not_Found:
        return RouterErrorCode.BlobDoesNotExist;
      case Disk_Unavailable:
      case Replica_Unavailable:
        return RouterErrorCode.AmbryUnavailable;
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
    routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).deleteRequestErrorCount.inc();
  }

  /**
   * Completes the {@code DeleteOperation} if it is done.
   */
  private void checkAndMaybeComplete() {
    // operationCompleted is true if Blob_Authorization_Failure was received.
    if (operationTracker.isDone() || operationCompleted) {
      if (operationTracker.hasSucceeded()) {
        operationException.set(null);
      } else if (operationTracker.hasFailedOnNotFound()) {
        operationException.set(
            new RouterException("DeleteOperation failed because of BlobNotFound", RouterErrorCode.BlobDoesNotExist));
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
  private Integer getPrecedenceLevel(RouterErrorCode routerErrorCode) {
    switch (routerErrorCode) {
      case BlobAuthorizationFailure:
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
   * Gets {@link BlobId} of this {@code DeleteOperation}.
   * @return The {@link BlobId}.
   */
  BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the service ID for the service requesting this delete operation.
   */
  String getServiceId() {
    return serviceId;
  }

  /**
   * Get the {@link FutureResult} for this {@code DeleteOperation}.
   * @return The {@link FutureResult}.
   */
  FutureResult<Void> getFutureResult() {
    return futureResult;
  }

  /**
   * Gets the {@link Callback} for this {@code DeleteOperation}.
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
   * Gets the result for this {@code DeleteOperation}. In a {@link DeleteOperation}, nothing is returned
   * to the caller as a result of this operation. Including this {@link Void} result is for consistency
   * with other operations.
   * @return Void.
   */
  Void getOperationResult() {
    return operationResult;
  }

  long getSubmissionTimeMs() {
    return submissionTimeMs;
  }
}
