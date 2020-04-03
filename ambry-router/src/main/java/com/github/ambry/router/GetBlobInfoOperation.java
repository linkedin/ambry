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
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GetBlobInfoOperation class is responsible for maintaining the state associated with a getBlobInfo operation,
 * and completing it. A GetBlobInfo operation only needs to make requests for a single chunk to get the BlobInfo -
 * which is either the only chunk in the case of a simple blob, or the metadata chunk in the case of composite blobs.
 */
class GetBlobInfoOperation extends GetOperation {
  // whether the operationCallback has been called already.
  private final AtomicBoolean operationCallbackInvoked = new AtomicBoolean(false);
  // the callback to use to notify the router about events and state changes
  private final RouterCallback routerCallback;
  private final OperationTracker operationTracker;
  // progress tracker used to track whether the operation is completed or not and whether it succeeded or failed on complete
  private final ProgressTracker progressTracker;
  // refers to blob properties received from the server
  private BlobProperties serverBlobProperties;
  // metrics tracker to track decrypt jobs
  private final CryptoJobMetricsTracker decryptJobMetricsTracker =
      new CryptoJobMetricsTracker(routerMetrics.decryptJobMetrics);
  // map of correlation id to the request metadata for every request issued for this operation.
  private final Map<Integer, GetRequestInfo> correlationIdToGetRequestInfo = new TreeMap<Integer, GetRequestInfo>();

  private static final Logger logger = LoggerFactory.getLogger(GetBlobInfoOperation.class);

  /**
   * Construct a GetBlobInfoOperation
   * @param routerConfig the {@link RouterConfig} containing the configs for get operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param blobId the {@link BlobId} associated with the operation.
   * @param options the {@link GetBlobOptionsInternal} containing the options associated with this operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param routerCallback the {@link RouterCallback} to use to complete operations.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the Time instance to use.
   * @param isEncrypted if encrypted bit set based on original string of a {@link BlobId}
   */
  GetBlobInfoOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, BlobId blobId, GetBlobOptionsInternal options,
      Callback<GetBlobResultInternal> callback, RouterCallback routerCallback, KeyManagementService kms,
      CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time, boolean isEncrypted) {
    super(routerConfig, routerMetrics, clusterMap, responseHandler, blobId, options, callback, kms, cryptoService,
        cryptoJobHandler, time, isEncrypted);
    this.routerCallback = routerCallback;
    operationTracker =
        getOperationTracker(blobId.getPartition(), blobId.getDatacenterId(), RouterOperation.GetBlobInfoOperation);
    progressTracker = new ProgressTracker(operationTracker);
  }

  @Override
  void abort(Exception abortCause) {
    if (operationCallbackInvoked.compareAndSet(false, true)) {
      NonBlockingRouter.completeOperation(null, getOperationCallback, null, abortCause);
      operationCompleted = true;
    }
  }

  /**
   * Return the {@link MessageFormatFlags} to associate with a getBlobInfo operation.
   * @return {@link MessageFormatFlags#BlobInfo}
   */
  MessageFormatFlags getOperationFlag() {
    return MessageFormatFlags.BlobInfo;
  }

  /**
   * For this operation, create and populate get requests (in the form of {@link RequestInfo}) to send out.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
   *                                    created as part of this poll operation.
   */
  @Override
  void poll(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
    //First, check if any of the existing requests have timed out.
    cleanupExpiredInFlightRequests(requestRegistrationCallback);
    checkAndMaybeComplete();
    if (!isOperationComplete()) {
      fetchRequests(requestRegistrationCallback);
    }
  }

  /**
   * Clean up requests sent out by this operation that have now timed out.
   * @param requestRegistrationCallback The callback to use to notify the networking layer of dropped requests.
   */
  private void cleanupExpiredInFlightRequests(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
    Iterator<Map.Entry<Integer, GetRequestInfo>> inFlightRequestsIterator =
        correlationIdToGetRequestInfo.entrySet().iterator();
    while (inFlightRequestsIterator.hasNext()) {
      Map.Entry<Integer, GetRequestInfo> entry = inFlightRequestsIterator.next();
      int correlationId = entry.getKey();
      GetRequestInfo info = entry.getValue();
      if (time.milliseconds() - info.startTimeMs > routerConfig.routerRequestTimeoutMs) {
        logger.trace("GetBlobInfoRequest with correlationId {} in flight has expired for replica {} ", correlationId,
            info.replicaId.getDataNodeId());
        // Do not notify this as a failure to the response handler, as this timeout could simply be due to
        // connection unavailability. If there is indeed a network error, the NetworkClient will provide an error
        // response and the response handler will be notified accordingly.
        onErrorResponse(entry.getValue().replicaId,
            RouterUtils.buildTimeoutException(correlationId, info.replicaId.getDataNodeId(), blobId));
        requestRegistrationCallback.registerRequestToDrop(correlationId);
        inFlightRequestsIterator.remove();
      } else {
        // the entries are ordered by correlation id and time. Break on the first request that has not timed out.
        break;
      }
    }
  }

  /**
   * Fetch {@link GetRequest}s to send for the operation.
   */
  private void fetchRequests(RequestRegistrationCallback<GetOperation> requestRegistrationCallback) {
    Iterator<ReplicaId> replicaIterator = operationTracker.getReplicaIterator();
    while (replicaIterator.hasNext()) {
      ReplicaId replicaId = replicaIterator.next();
      String hostname = replicaId.getDataNodeId().getHostname();
      Port port = RouterUtils.getPortToConnectTo(replicaId, routerConfig.routerEnableHttp2NetworkClient);
      GetRequest getRequest = createGetRequest(blobId, getOperationFlag(), options.getBlobOptions.getGetOption());
      RequestInfo request = new RequestInfo(hostname, port, getRequest, replicaId);
      int correlationId = getRequest.getCorrelationId();
      correlationIdToGetRequestInfo.put(correlationId, new GetRequestInfo(replicaId, time.milliseconds()));
      requestRegistrationCallback.registerRequestToSend(this, request);
      replicaIterator.remove();
      if (RouterUtils.isRemoteReplica(routerConfig, replicaId)) {
        logger.trace("Making request with correlationId {} to a remote replica {} in {} ", correlationId,
            replicaId.getDataNodeId(), replicaId.getDataNodeId().getDatacenterName());
        routerMetrics.crossColoRequestCount.inc();
      } else {
        logger.trace("Making request with correlationId {} to a local replica {} ", correlationId,
            replicaId.getDataNodeId());
      }
      routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getBlobInfoRequestRate.mark();
    }
  }

  /**
   * Handle the given {@link ResponseInfo} and update the status of the operation.
   * Based on the status of the response, this involves handling the body, notifying the operation tracker,
   * notifying the response handler and possibly setting the operationException.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   * @param getResponse The {@link GetResponse} associated with this response.
   */
  @Override
  void handleResponse(ResponseInfo responseInfo, GetResponse getResponse) {
    if (isOperationComplete() || operationTracker.isDone()) {
      // If the successTarget is more than 1, then, different responses will have to be reconciled in some way. Here is where that
      // would be done. Since the store is immutable, currently we handle this by ignoring subsequent responses.
      return;
    }
    int correlationId = responseInfo.getRequestInfo().getRequest().getCorrelationId();
    // Get the GetOperation that generated the request.
    GetRequestInfo getRequestInfo = correlationIdToGetRequestInfo.remove(correlationId);
    if (getRequestInfo == null) {
      // Ignore. The request must have timed out.
      return;
    }
    long requestLatencyMs = time.milliseconds() - getRequestInfo.startTimeMs;
    routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
    routerMetrics.getDataNodeBasedMetrics(getRequestInfo.replicaId.getDataNodeId()).getBlobInfoRequestLatencyMs.update(
        requestLatencyMs);
    if (responseInfo.getError() != null) {
      logger.trace("GetBlobInfoRequest with response correlationId {} timed out for replica {} ", correlationId,
          getRequestInfo.replicaId.getDataNodeId());
      onErrorResponse(getRequestInfo.replicaId,
          new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut));
    } else {
      if (getResponse == null) {
        logger.trace(
            "GetBlobInfoRequest with response correlationId {} received an unexpected error on response deserialization from replica {} ",
            correlationId, getRequestInfo.replicaId.getDataNodeId());
        onErrorResponse(getRequestInfo.replicaId,
            new RouterException("Response deserialization received an unexpected error",
                RouterErrorCode.UnexpectedInternalError));
      } else {
        if (getResponse.getCorrelationId() != correlationId) {
          // The NetworkClient associates a response with a request based on the fact that only one request is sent
          // out over a connection id, and the response received on a connection id must be for the latest request
          // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
          // There is no other way to handle it.
          routerMetrics.unknownReplicaResponseError.inc();
          logger.trace("GetBlobInfoRequest with response correlationId {} mismatch from response {} for replica {} ",
              correlationId, getResponse.getCorrelationId(), getRequestInfo.replicaId.getDataNodeId());
          onErrorResponse(getRequestInfo.replicaId, new RouterException(
              "The correlation id in the GetResponse " + getResponse.getCorrelationId()
                  + "is not the same as the correlation id in the associated GetRequest: " + correlationId,
              RouterErrorCode.UnexpectedInternalError));
          // we do not notify the ResponseHandler responsible for failure detection as this is an unexpected error.
        } else {
          try {
            processGetBlobInfoResponse(getRequestInfo, getResponse);
          } catch (IOException | MessageFormatException e) {
            // This should really not happen. Again, we do not notify the ResponseHandler responsible for failure
            // detection.
            logger.trace(
                "GetBlobInfoRequest with response correlationId {} response deserialization failed for replica {} ",
                correlationId, getRequestInfo.replicaId.getDataNodeId());
            routerMetrics.responseDeserializationErrorCount.inc();
            onErrorResponse(getRequestInfo.replicaId,
                new RouterException("Response deserialization received an unexpected error", e,
                    RouterErrorCode.UnexpectedInternalError));
          }
        }
      }
    }
    checkAndMaybeComplete();
  }

  /**
   * Process the {@link GetResponse} extracted from a {@link ResponseInfo}
   * @param getRequestInfo the associated {@link GetRequestInfo} for which this response was received.
   * @param getResponse the {@link GetResponse} extracted from the {@link ResponseInfo}
   * @throws IOException if there is an error during deserialization of the GetResponse.
   * @throws MessageFormatException if there is an error during deserialization of the GetResponse.
   */
  private void processGetBlobInfoResponse(GetRequestInfo getRequestInfo, GetResponse getResponse)
      throws IOException, MessageFormatException {
    ServerErrorCode getError = getResponse.getError();
    if (getError == ServerErrorCode.No_Error) {
      int partitionsInResponse = getResponse.getPartitionResponseInfoList().size();
      // Each get request issued by the router is for a single blob.
      if (partitionsInResponse != 1) {
        onErrorResponse(getRequestInfo.replicaId, new RouterException(
            "Unexpected number of partition responses, expected: 1, " + "received: " + partitionsInResponse,
            RouterErrorCode.UnexpectedInternalError));
        // Again, no need to notify the responseHandler.
      } else {
        getError = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
        if (getError == ServerErrorCode.No_Error) {
          PartitionResponseInfo partitionResponseInfo = getResponse.getPartitionResponseInfoList().get(0);
          int msgsInResponse = partitionResponseInfo.getMessageInfoList().size();
          if (msgsInResponse != 1) {
            onErrorResponse(getRequestInfo.replicaId, new RouterException(
                "Unexpected number of messages in a partition response, expected: 1, " + "received: " + msgsInResponse,
                RouterErrorCode.UnexpectedInternalError));
          } else {
            MessageMetadata messageMetadata = partitionResponseInfo.getMessageMetadataList().get(0);
            MessageInfo messageInfo = partitionResponseInfo.getMessageInfoList().get(0);
            handleBody(getResponse.getInputStream(), messageMetadata, messageInfo);
            operationTracker.onResponse(getRequestInfo.replicaId, TrackedRequestFinalState.SUCCESS);
            if (RouterUtils.isRemoteReplica(routerConfig, getRequestInfo.replicaId)) {
              logger.trace("Cross colo request successful for remote replica in {} ",
                  getRequestInfo.replicaId.getDataNodeId().getDatacenterName());
              routerMetrics.crossColoSuccessCount.inc();
            }
          }
        } else {
          // process and set the most relevant exception.
          logger.trace("Replica  {} returned error {} with response correlationId {} ",
              getRequestInfo.replicaId.getDataNodeId(), getError, getResponse.getCorrelationId());
          RouterErrorCode routerErrorCode = processServerError(getError);
          if (getError == ServerErrorCode.Disk_Unavailable) {
            operationTracker.onResponse(getRequestInfo.replicaId, TrackedRequestFinalState.DISK_DOWN);
            setOperationException(new RouterException("Server returned: " + getError, routerErrorCode));
            routerMetrics.routerRequestErrorCount.inc();
            routerMetrics.getDataNodeBasedMetrics(getRequestInfo.replicaId.getDataNodeId()).getBlobInfoRequestErrorCount
                .inc();
          } else {
            if (getError == ServerErrorCode.Blob_Deleted || getError == ServerErrorCode.Blob_Expired
                || getError == ServerErrorCode.Blob_Authorization_Failure) {
              // this is a successful response and one that completes the operation regardless of whether the
              // success target has been reached or not.
              operationCompleted = true;
            }
            // any server error code that is not equal to ServerErrorCode.No_Error, the onErrorResponse should be invoked
            // because the operation itself doesn't succeed although the response in some cases is successful (i.e. Blob_Deleted)
            onErrorResponse(getRequestInfo.replicaId,
                new RouterException("Server returned: " + getError, routerErrorCode));
          }
        }
      }
    } else {
      logger.trace("Replica {} returned an error {} for a GetBlobInfoRequest with response correlationId : {} ",
          getRequestInfo.replicaId.getDataNodeId(), getError, getResponse.getCorrelationId());
      onErrorResponse(getRequestInfo.replicaId, new RouterException("Server returned", processServerError(getError)));
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
    routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getBlobInfoRequestErrorCount.inc();
  }

  /**
   * Handle the body of the response: Deserialize and set the {@link BlobInfo} to return if no decryption is required.
   * If decryption is required, submit a job for decryption.
   * @param payload the body of the response.
   * @param messageMetadata the {@link MessageMetadata} associated with the message.
   * @param messageInfo the {@link MessageInfo} associated with the message.
   * @throws IOException if there is an IOException while deserializing the body.
   * @throws MessageFormatException if there is a MessageFormatException while deserializing the body.
   */
  private void handleBody(InputStream payload, MessageMetadata messageMetadata, MessageInfo messageInfo)
      throws IOException, MessageFormatException {
    ByteBuffer encryptionKey = messageMetadata == null ? null : messageMetadata.getEncryptionKey();
    serverBlobProperties = MessageFormatRecord.deserializeBlobProperties(payload);
    updateTtlIfRequired(serverBlobProperties, messageInfo);
    ByteBuffer userMetadata = MessageFormatRecord.deserializeUserMetadata(payload);
    if (encryptionKey == null) {
      // if blob is not encrypted, move the state to Complete
      operationResult =
          new GetBlobResultInternal(new GetBlobResult(new BlobInfo(serverBlobProperties, userMetadata.array()), null),
              null);
    } else {
      // submit decrypt job
      progressTracker.initializeCryptoJobTracker(CryptoJobType.DECRYPTION);
      logger.trace("Submitting decrypt job for {}", blobId);
      decryptJobMetricsTracker.onJobSubmission();
      long startTimeMs = System.currentTimeMillis();
      cryptoJobHandler.submitJob(
          new DecryptJob(blobId, encryptionKey.duplicate(), null, userMetadata, cryptoService, kms,
              decryptJobMetricsTracker, (DecryptJob.DecryptJobResult result, Exception exception) -> {
            decryptJobMetricsTracker.onJobResultProcessingStart();
            logger.trace("Handling decrypt job callback results for {}", blobId);
            routerMetrics.decryptTimeMs.update(System.currentTimeMillis() - startTimeMs);
            if (exception == null) {
              logger.trace("Successfully updating decrypt job callback results for {}", blobId);
              operationResult = new GetBlobResultInternal(
                  new GetBlobResult(new BlobInfo(serverBlobProperties, result.getDecryptedUserMetadata().array()),
                      null), null);
              progressTracker.setCryptoJobSuccess();
            } else {
              decryptJobMetricsTracker.incrementOperationError();
              logger.trace("Exception {} thrown on decryption for {}", exception, blobId);
              setOperationException(
                  new RouterException("Exception thrown on decrypting the content for " + blobId, exception,
                      RouterErrorCode.UnexpectedInternalError));
              progressTracker.setCryptoJobFailed();
            }
            decryptJobMetricsTracker.onJobResultProcessingComplete();
            routerCallback.onPollReady();
          }));
    }
  }

  /**
   * Process the given {@link ServerErrorCode} and set operation status accordingly.
   * @param errorCode the {@link ServerErrorCode} to process.
   * @return the {@link RouterErrorCode} mapped from server error code.
   */
  private RouterErrorCode processServerError(ServerErrorCode errorCode) {
    RouterErrorCode resolvedRouterErrorCode;
    switch (errorCode) {
      case Blob_Authorization_Failure:
        logger.trace("Requested blob authorization failed");
        resolvedRouterErrorCode = RouterErrorCode.BlobAuthorizationFailure;
        break;
      case Blob_Deleted:
        logger.trace("Requested blob was deleted");
        resolvedRouterErrorCode = RouterErrorCode.BlobDeleted;
        break;
      case Blob_Expired:
        logger.trace("Requested blob has expired");
        resolvedRouterErrorCode = RouterErrorCode.BlobExpired;
        break;
      case Blob_Not_Found:
        logger.trace("Requested blob was not found on this server");
        resolvedRouterErrorCode = RouterErrorCode.BlobDoesNotExist;
        break;
      case Disk_Unavailable:
      case Replica_Unavailable:
        logger.trace("Disk or replica on which the requested blob resides is not accessible");
        resolvedRouterErrorCode = RouterErrorCode.AmbryUnavailable;
        break;
      default:
        resolvedRouterErrorCode = RouterErrorCode.UnexpectedInternalError;
    }
    return resolvedRouterErrorCode;
  }

  /**
   * Check whether the operation can be completed, if so complete it.
   */
  private void checkAndMaybeComplete() {
    if (progressTracker.isDone()) {
      if (progressTracker.hasSucceeded()) {
        operationException.set(null);
      } else if (operationTracker.hasFailedOnNotFound()) {
        operationException.set(new RouterException("GetBlobInfoOperation failed because of BlobNotFound",
            RouterErrorCode.BlobDoesNotExist));
      }
      operationCompleted = true;
    }

    if (operationCompleted && operationCallbackInvoked.compareAndSet(false, true)) {
      Exception e = operationException.get();
      if (operationResult == null && e == null) {
        e = new RouterException("Operation failed, but exception was not set", RouterErrorCode.UnexpectedInternalError);
        routerMetrics.operationFailureWithUnsetExceptionCount.inc();
      }
      if (e != null) {
        operationResult = null;
        routerMetrics.onGetBlobError(e, options, isEncrypted);
      }
      long operationLatencyMs = time.milliseconds() - submissionTimeMs;
      if (isEncrypted) {
        routerMetrics.getEncryptedBlobInfoOperationLatencyMs.update(operationLatencyMs);
      } else {
        routerMetrics.getBlobInfoOperationLatencyMs.update(operationLatencyMs);
      }
      NonBlockingRouter.completeOperation(null, getOperationCallback, operationResult, e);
    }
  }

  /**
   * @return {@link OperationTracker} associated with this operation
   */
  OperationTracker getOperationTrackerInUse() {
    return operationTracker;
  }
}

