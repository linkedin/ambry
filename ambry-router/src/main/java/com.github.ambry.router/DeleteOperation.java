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

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
  private final HashMap<Integer, DeleteRequestInfo> deleteRequestInfos;
  // The result of this operation to be set into FutureResult.
  private final Void operationResult = null;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // RouterErrorCode that is resolved from all the received ServerErrorCode for this operation.
  private RouterErrorCode resolvedRouterErrorCode;
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
  DeleteOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ResponseHandler responsehandler,
      BlobId blobId, String serviceId, Callback<Void> callback, Time time, FutureResult<Void> futureResult) {
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
    this.deleteRequestInfos = new HashMap<Integer, DeleteRequestInfo>();
    this.operationTracker = new SimpleOperationTracker(routerConfig.routerDatacenterName, blobId.getPartition(), true,
        routerConfig.routerDeleteSuccessTarget, routerConfig.routerDeleteRequestParallelism, false);
  }

  /**
   * Gets a list of {@link DeleteRequest} for sending to replicas.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request
   *                            that gets created as part of this poll operation.
   */
  void poll(RequestRegistrationCallback<DeleteOperation> requestRegistrationCallback) {
    cleanupExpiredInflightRequests();
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
      Port port = replica.getDataNodeId().getPortToConnectTo();
      DeleteRequest deleteRequest = createDeleteRequest();
      deleteRequestInfos.put(deleteRequest.getCorrelationId(), new DeleteRequestInfo(time.milliseconds(), replica));
      RouterRequestInfo requestInfo = new RouterRequestInfo(hostname, port, deleteRequest, replica);
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
      updateOperationState(replica, RouterErrorCode.OperationTimedOut);
    } else {
      if (deleteResponse == null) {
        logger.trace(
            "DeleteRequest with response correlationId {} received UnexpectedInternalError on response deserialization for replica {} ",
            deleteRequest.getCorrelationId(), replica.getDataNodeId());
        updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
      } else {
        // The true case below should not really happen. This means a response has been received
        // not for its original request. We will immediately fail this operation.
        if (deleteResponse.getCorrelationId() != deleteRequest.getCorrelationId()) {
          logger.error("The correlation id in the DeleteResponse " + deleteResponse.getCorrelationId()
              + " is not the same as the correlation id in the associated DeleteRequest: "
              + deleteRequest.getCorrelationId());
          routerMetrics.unknownReplicaResponseError.inc();
          setOperationException(
              new RouterException("Received wrong response that is not for the corresponding request.",
                  RouterErrorCode.UnexpectedInternalError));
          updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
        } else {
          // The status of operation tracker will be updated within the processServerError method.
          processServerError(replica, deleteResponse.getError(), deleteResponse.getCorrelationId());
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
   */
  private void cleanupExpiredInflightRequests() {
    Iterator<Map.Entry<Integer, DeleteRequestInfo>> itr = deleteRequestInfos.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<Integer, DeleteRequestInfo> deleteRequestInfoEntry = itr.next();
      DeleteRequestInfo deleteRequestInfo = deleteRequestInfoEntry.getValue();
      if (time.milliseconds() - deleteRequestInfo.startTimeMs > routerConfig.routerRequestTimeoutMs) {
        itr.remove();
        logger.trace("Delete Request with correlationid {} in flight has expired for replica {} ",
            deleteRequestInfoEntry.getKey(), deleteRequestInfo.replica.getDataNodeId());
        // Do not notify this as a failure to the response handler, as this timeout could simply be due to
        // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
        // response and the response handler will be notified accordingly.
        updateOperationState(deleteRequestInfo.replica, RouterErrorCode.OperationTimedOut);
      }
    }
  }

  /**
   * Processes {@link ServerErrorCode} received from {@code replica}. This method maps a {@link ServerErrorCode}
   * to a {@link RouterErrorCode}, and then makes corresponding state update.
   * @param replica The replica for which the ServerErrorCode was generated.
   * @param serverErrorCode The ServerErrorCode received from the replica.
   * @param correlationId the correlationId of the request
   */
  private void processServerError(ReplicaId replica, ServerErrorCode serverErrorCode, int correlationId) {
    switch (serverErrorCode) {
      case No_Error:
      case Blob_Deleted:
        operationTracker.onResponse(replica, true);
        if (RouterUtils.isRemoteReplica(routerConfig, replica)) {
          logger.trace("Cross colo request successful for remote replica {} in {} ", replica.getDataNodeId(),
              replica.getDataNodeId().getDatacenterName());
          routerMetrics.crossColoSuccessCount.inc();
        }
        break;
      case Blob_Expired:
        updateOperationState(replica, RouterErrorCode.BlobExpired);
        break;
      case Blob_Not_Found:
        updateOperationState(replica, RouterErrorCode.BlobDoesNotExist);
        break;
      case Partition_Unknown:
        updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
        break;
      case Disk_Unavailable:
        updateOperationState(replica, RouterErrorCode.AmbryUnavailable);
        break;
      default:
        updateOperationState(replica, RouterErrorCode.UnexpectedInternalError);
        break;
    }
    if (serverErrorCode != ServerErrorCode.No_Error) {
      logger.trace("Replica {} returned an error {} for a delete request with response correlationId : {} ",
          replica.getDataNodeId(), serverErrorCode, correlationId);
    }
  }

  /**
   * Updates the state of the {@code DeleteOperation}. This includes two parts: 1) resolves the
   * {@link RouterErrorCode} depending on the precedence level of the new router error code from
   * {@code replica} and the current {@code resolvedRouterErrorCode}. An error code with a smaller
   * precedence level overrides an error code with a larger precedence level. 2) updates the
   * {@code DeleteOperation} based on the {@link RouterErrorCode}, and the source {@link ReplicaId}
   * for which the {@link RouterErrorCode} is generated.
   * @param replica The replica for which the RouterErrorCode was generated.
   * @param error {@link RouterErrorCode} that indicates the error for the replica.
   */
  private void updateOperationState(ReplicaId replica, RouterErrorCode error) {
    if (resolvedRouterErrorCode == null) {
      resolvedRouterErrorCode = error;
    } else {
      if (getPrecedenceLevel(error) < getPrecedenceLevel(resolvedRouterErrorCode)) {
        resolvedRouterErrorCode = error;
      }
    }
    operationTracker.onResponse(replica, false);
    if (error != RouterErrorCode.BlobDeleted && error != RouterErrorCode.BlobExpired) {
      routerMetrics.routerRequestErrorCount.inc();
    }
    routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).deleteRequestErrorCount.inc();
  }

  /**
   * Completes the {@code DeleteOperation} if it is done.
   */
  private void checkAndMaybeComplete() {
    if (operationTracker.isDone()) {
      if (!operationTracker.hasSucceeded()) {
        setOperationException(
            new RouterException("The DeleteOperation could not be completed.", resolvedRouterErrorCode));
      }
      operationCompleted = true;
    }
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
      case BlobExpired:
        return 1;
      case AmbryUnavailable:
        return 2;
      case UnexpectedInternalError:
        return 3;
      case OperationTimedOut:
        return 4;
      case BlobDoesNotExist:
        return 5;
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

  /**
   * Sets the exception associated with this operation. When this is called, the operation has failed.
   * @param exception the irrecoverable exception associated with this operation.
   */
  void setOperationException(Exception exception) {
    operationException.set(exception);
  }

  long getSubmissionTimeMs() {
    return submissionTimeMs;
  }
}
