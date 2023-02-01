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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Time;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages the internal state of a {@link TtlUpdateOperation} during its life cycle.
 */
class TtlUpdateOperation {
  private static final Logger LOGGER = LoggerFactory.getLogger(TtlUpdateOperation.class);
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
  private final QuotaChargeCallback quotaChargeCallback;
  // Parameters associated with the state.
  // The operation tracker that tracks the state of this operation.
  private OperationTracker operationTracker;
  // A map used to find inflight requests using a correlation id.
  private final Map<Integer, RequestInfo> ttlUpdateRequestInfos = new LinkedHashMap<>();
  // The result of this operation to be set into FutureResult.
  private final Void operationResult = null;
  // the cause for failure of this operation. This will be set if and when the operation encounters an irrecoverable
  // failure.
  private final AtomicReference<Exception> operationException = new AtomicReference<Exception>();
  // Denotes whether the operation is complete.
  private boolean operationCompleted = false;
  // Quota charger for this operation.
  private final OperationQuotaCharger operationQuotaCharger;
  private final NonBlockingRouter nonBlockingRouter;
  private ReplicateBlobCallback replicateBlobCallback;
  private final String originatingDcName;

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
   * @param quotaChargeCallback The {@link QuotaChargeCallback} object.
   * @param nonBlockingRouter The non-blocking router object
   */
  TtlUpdateOperation(ClusterMap clusterMap, RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      BlobId blobId, String serviceId, long expiresAtMs, long operationTimeMs, Callback<Void> callback, Time time,
      FutureResult<Void> futureResult, QuotaChargeCallback quotaChargeCallback, NonBlockingRouter nonBlockingRouter) {
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
    this.originatingDcName = clusterMap.getDatacenterName(blobDcId);
    this.operationTracker =
        new SimpleOperationTracker(routerConfig, RouterOperation.TtlUpdateOperation, blobId.getPartition(),
            originatingDcName, false, routerMetrics, blobId);
    this.quotaChargeCallback = quotaChargeCallback;
    this.operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, blobId, this.getClass().getSimpleName(), routerMetrics);
    this.nonBlockingRouter = nonBlockingRouter;
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
      RequestInfo requestInfo =
          new RequestInfo(hostname, port, ttlUpdateRequest, replica, operationQuotaCharger, time.milliseconds(),
              routerConfig.routerRequestNetworkTimeoutMs, routerConfig.routerRequestTimeoutMs);
      ttlUpdateRequestInfos.put(ttlUpdateRequest.getCorrelationId(), requestInfo);
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
    RequestInfo ttlUpdateRequestInfo = ttlUpdateRequestInfos.remove(ttlUpdateRequest.getCorrelationId());
    // ttlUpdateRequestInfo can be null if this request was timed out before this response is received. No
    // metric is updated here, as corresponding metrics have been updated when the request was timed out.
    if (ttlUpdateRequestInfo == null) {
      return;
    }
    ReplicaId replica = ttlUpdateRequestInfo.getReplicaId();
    if (responseInfo.isQuotaRejected()) {
      processQuotaRejectedResponse(ttlUpdateRequest.getCorrelationId(), replica);
      return;
    }
    // Track the over all time taken for the response since the creation of the request.
    long requestLatencyMs = time.milliseconds() - ttlUpdateRequestInfo.getRequestCreateTime();
    routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
    routerMetrics.getDataNodeBasedMetrics(replica.getDataNodeId()).ttlUpdateRequestLatencyMs.update(requestLatencyMs);
    // Check the error code from NetworkClient.
    if (responseInfo.getError() != null) {
      LOGGER.debug("TtlUpdateRequest with response correlationId {} timed out for replica {} ",
          ttlUpdateRequest.getCorrelationId(), replica.getDataNodeId());
      onErrorResponse(replica, new RouterException(
          "Operation timed out because of " + responseInfo.getError() + " at DataNode " + responseInfo.getDataNode(),
          RouterErrorCode.OperationTimedOut));
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
          LOGGER.error(
              "The correlation id in the TtlUpdateResponse {} is not the same as the correlation id in the associated TtlUpdateRequest: {}",
              ttlUpdateResponse.getCorrelationId(), ttlUpdateRequest.getCorrelationId());
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
   * Goes through the inflight request list of this {@link TtlUpdateOperation} and remove those that
   * have been timed out.
   * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
   */
  private void cleanupExpiredInflightRequests(
      RequestRegistrationCallback<TtlUpdateOperation> requestRegistrationCallback) {
    Iterator<Map.Entry<Integer, RequestInfo>> itr = ttlUpdateRequestInfos.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<Integer, RequestInfo> ttlUpdateRequestInfoEntry = itr.next();
      int correlationId = ttlUpdateRequestInfoEntry.getKey();
      RequestInfo requestInfo = ttlUpdateRequestInfoEntry.getValue();
      // If request times out due to no response from server or due to being stuck in router itself (due to bandwidth
      // throttling, etc) for long time, drop the request.
      long currentTimeInMs = time.milliseconds();
      RouterUtils.RouterRequestExpiryReason routerRequestExpiryReason =
          RouterUtils.isRequestExpired(requestInfo, currentTimeInMs);
      if (routerRequestExpiryReason != RouterUtils.RouterRequestExpiryReason.NO_TIMEOUT) {
        itr.remove();
        LOGGER.trace("TTL Request with correlationid {} in flight has expired for replica {} due to {}", correlationId,
            requestInfo.getReplicaId().getDataNodeId(), routerRequestExpiryReason.name());
        // Do not notify this as a failure to the response handler, as this timeout could simply be due to
        // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
        // response and the response handler will be notified accordingly.
        onErrorResponse(requestInfo.getReplicaId(),
            RouterUtils.buildTimeoutException(correlationId, requestInfo.getReplicaId().getDataNodeId(), blobId));
        requestRegistrationCallback.registerRequestToDrop(correlationId);
      } else {
        // Note: Even though the requests are ordered by correlation id and their creation time, we cannot break out of
        // the while loop here. This is because time outs for all requests may not be equal now.

        // For example, request 1 in the map may have been assigned high time out since it might be sent at a
        // time when the load is high and request 2 may have been assigned lower time out value since the load might have
        // decreased by the time it is sent out. In this case, we should continue iterating the loop and clean up
        // request 2 in the map.

        // The cost of iterating all entries should be okay since the map contains outstanding requests whose number
        // should be small. The maximum outstanding requests possible would be equal to the operation parallelism value
        // and may be few more if adaptive operation tracker is used.
      }
    }
  }

  /**
   * Process response if it was rejected due to quota compliance.
   * @param correlationId correlation id of the request.
   * @param replicaId {@link ReplicaId} of the request.
   */
  private void processQuotaRejectedResponse(int correlationId, ReplicaId replicaId) {
    LOGGER.debug("TtlUpdateRequest with response correlationId {} was rejected because quota was exceeded.",
        correlationId);
    operationCompleted = true;
    onErrorResponse(replicaId, new RouterException("QuotaExceeded", RouterErrorCode.TooManyRequests), false);
    checkAndMaybeComplete();
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
    onErrorResponse(replicaId, exception, true);
  }

  /**
   * Perform the necessary actions when a request to a replica fails.
   * @param replicaId the {@link ReplicaId} associated with the failed response.
   * @param exception the {@link RouterException} associated with the failed response.
   * @param updateDataNodeMetrics {@code true} if data node metrics should be updated. {@code false} otherwise.
   */
  private void onErrorResponse(ReplicaId replicaId, RouterException exception, boolean updateDataNodeMetrics) {
    operationTracker.onResponse(replicaId,
        TrackedRequestFinalState.fromRouterErrorCodeToFinalState(exception.getErrorCode()));
    setOperationException(exception);
    if (exception.getErrorCode() != RouterErrorCode.BlobDeleted
        && exception.getErrorCode() != RouterErrorCode.BlobExpired) {
      routerMetrics.routerRequestErrorCount.inc();
    }
    if (updateDataNodeMetrics) {
      routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).ttlUpdateRequestErrorCount.inc();
    }
  }

  /**
   * Check if we should replicate the blob and retry the operation.
   * @return true if need to do the retry.
   */
  private boolean shouldReplicateBlobAndRetry() {
    // the conditions to trigger ReplicateBlob and retry the original operation:
    // 1. the feature is enabled.
    // 2. and at least one replica returned NOT_FOUND status.
    // 3. and at least one replica returned successful status.
    // 4. and error code precedence is over AmbryUnavailable. If we have one success and one delete, shouldn't do retry.
    RouterErrorCode errorCode = ((RouterException) operationException.get()).getErrorCode();
    return (routerConfig.routerRepairWithReplicateBlobEnabled && operationTracker.hasNotFound()
        && operationTracker.getSuccessCount() > 0 && getPrecedenceLevel(errorCode) >= getPrecedenceLevel(
        RouterErrorCode.AmbryUnavailable));
  }

  /**
   * Completes the {@link TtlUpdateOperation} if it is done.
   */
  private void checkAndMaybeComplete() {
    // operationCompleted is true if Blob_Authorization_Failure was received.
    if (operationTracker.isDone() || operationCompleted) {

      if (operationTracker.hasSucceeded()) {
        operationException.set(null);
      } else {
        // the operation is failed, try to recover with ReplicateBlob
        if (shouldReplicateBlobAndRetry()) {
          // if retryWithReplicateBlob returns true, it's under retry. operation is not completed yet. so return.
          // if it returns false, continue the following code to set the proper operationException and complete the request.
          if (retryWithReplicateBlob()) {
            return;
          }
        }
        if (operationTracker.hasFailedOnNotFound()) {
          operationException.set(new RouterException("TtlUpdateOperation failed because of BlobNotFound",
              RouterErrorCode.BlobDoesNotExist));
        } else if (operationTracker.hasSomeUnavailability()) {
          setOperationException(new RouterException("TtlUpdateOperation failed possibly because of offline replicas",
              RouterErrorCode.AmbryUnavailable));
        }
      }

      if (QuotaUtils.postProcessCharge(quotaChargeCallback)) {
        try {
          quotaChargeCallback.checkAndCharge(false, true);
        } catch (QuotaException quotaException) {
          LOGGER.info("Exception {} while charging quota for ttl update operation.", quotaException.toString());
        }
      }
      operationCompleted = true;
    }
  }

  /**
   * Try to replicate the blob and then retry the TtlUpdate Request
   * @return true if it's under retry. return false if cannot do retry or retry is finished.
   */
  private boolean retryWithReplicateBlob() {
    // it's the first time we hit the 503
    if (replicateBlobCallback == null) {
      List<ReplicaId> successfulReplica = operationTracker.getSuccessReplica();
      DataNodeId sourceDataNode = successfulReplica.get(0).getDataNodeId();
      replicateBlobCallback = new ReplicateBlobCallback(blobId, sourceDataNode);

      nonBlockingRouter.replicateBlob(blobId.getID(), this.getClass().getSimpleName(), sourceDataNode,
          replicateBlobCallback);
      LOGGER.trace("Start the on-demand replication {} {}.", replicateBlobCallback.getBlobId(),
          replicateBlobCallback.getSourceDataNode());
      return true;
    }

    ReplicateBlobCallback.State state = replicateBlobCallback.getState();
    switch (state) {
      case REPLICATING_BLOB:
        // Has sent out the ReplicateBlob operation and is waiting for its completion.
        LOGGER.trace("Running on-demand replication {} {}.", replicateBlobCallback.getBlobId(),
            replicateBlobCallback.getSourceDataNode());
        return true;
      case REPLICATION_DONE:
        // ReplicateBlob is done.
        // If ReplicateBlob fails, recovery procedure is done. Will complete the TtlUpdateOperation
        if (replicateBlobCallback.getException() != null) {
          LOGGER.error("on-demand replication {} {} failed.", replicateBlobCallback.getBlobId(),
              replicateBlobCallback.getSourceDataNode(), replicateBlobCallback.getException());
          return false;
        }
        // If ReplicateBlob is successful, retry the TtlUpdateOperation.
        LOGGER.info("On-demand replication {} {} is successful.", replicateBlobCallback.getBlobId(),
            replicateBlobCallback.getSourceDataNode());
        replicateBlobCallback.setState(ReplicateBlobCallback.State.RETRYING);

        // retry the TtlUpdate
        operationException.set(null);
        operationTracker =
            new SimpleOperationTracker(routerConfig, RouterOperation.TtlUpdateOperation, blobId.getPartition(),
                originatingDcName, false, routerMetrics, blobId);
        ttlUpdateRequestInfos.clear();
        return true;
      case RETRYING:
        // TtlUpdateOperation retry is finished but still failed. Nothing else we can do. Will complete the operation.
        return false;
      default:
        // code shouldn't come here
        throw new IllegalStateException("Unrecognized state type");
    }
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
      case TooManyRequests:
        return 4;
      case AmbryUnavailable:
        return 5;
      case UnexpectedInternalError:
        return 6;
      case OperationTimedOut:
        return 7;
      case BlobDoesNotExist:
        return 8;
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
