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
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.Port;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GetBlobInfoOperation class is responsible for maintaining the state associated with a getBlobInfo operation,
 * and completing it. A GetBlobInfo operation only needs to make requests for a single chunk to get the BlobInfo -
 * which is either the only chunk in the case of a simple blob, or the metadata chunk in the case of composite blobs.
 */
class GetBlobInfoOperation extends GetOperation<BlobInfo> {
  private final OperationCompleteCallback operationCompleteCallback;
  private final SimpleOperationTracker operationTracker;
  // map of correlation id to the request metadata for every request issued for this operation.
  private final Map<Integer, GetRequestInfo> correlationIdToGetRequestInfo = new TreeMap<Integer, GetRequestInfo>();

  private static final Logger logger = LoggerFactory.getLogger(GetBlobInfoOperation.class);

  /**
   * Construct a GetBlobInfoOperation
   * @param routerConfig the {@link RouterConfig} containing the configs for get operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param blobIdStr the blob id associated with the operation in string form.
   * @param futureResult the future that will contain the result of the operation.
   * @param callback the callback that is to be called when the operation completes.
   * @param operationCompleteCallback the {@link OperationCompleteCallback} to use to complete operations.
   * @param time the Time instance to use.
   * @throws RouterException if there is an error with any of the parameters, such as an invalid blob id.
   */
  GetBlobInfoOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, String blobIdStr, FutureResult<BlobInfo> futureResult,
      Callback<BlobInfo> callback, OperationCompleteCallback operationCompleteCallback, Time time)
      throws RouterException {
    super(routerConfig, routerMetrics, clusterMap, responseHandler, blobIdStr, futureResult, callback, time);
    this.operationCompleteCallback = operationCompleteCallback;
    operationTracker = new SimpleOperationTracker(routerConfig.routerDatacenterName, blobId.getPartition(),
        routerConfig.routerGetCrossDcEnabled, routerConfig.routerGetSuccessTarget,
        routerConfig.routerGetRequestParallelism);
  }

  @Override
  void abort(Exception abortCause) {
    operationCompleteCallback.completeOperation(operationFuture, operationCallback, null, abortCause);
    operationCompleted = true;
  }

  /**
   * Return the {@link MessageFormatFlags} to associate with a getBlobInfo operation.
   * @return {@link MessageFormatFlags#BlobInfo}
   */
  @Override
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
    cleanupExpiredInFlightRequests();
    checkAndMaybeComplete();
    if (!isOperationComplete()) {
      fetchRequests(requestRegistrationCallback);
    }
  }

  /**
   * Clean up requests sent out by this operation that have now timed out.
   */
  private void cleanupExpiredInFlightRequests() {
    Iterator<Map.Entry<Integer, GetRequestInfo>> inFlightRequestsIterator =
        correlationIdToGetRequestInfo.entrySet().iterator();
    while (inFlightRequestsIterator.hasNext()) {
      Map.Entry<Integer, GetRequestInfo> entry = inFlightRequestsIterator.next();
      if (time.milliseconds() - entry.getValue().startTimeMs > routerConfig.routerRequestTimeoutMs) {
        onErrorResponse(entry.getValue().replicaId);
        responseHandler.onRequestResponseException(entry.getValue().replicaId,
            new IOException("Timed out waiting for a response"));
        setOperationException(
            new RouterException("Timed out waiting for a response", RouterErrorCode.OperationTimedOut));
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
      Port port = replicaId.getDataNodeId().getPortToConnectTo();
      GetRequest getRequest = createGetRequest(blobId, getOperationFlag(), GetOptions.None);
      RequestInfo request = new RequestInfo(hostname, port, getRequest);
      int correlationId = getRequest.getCorrelationId();
      correlationIdToGetRequestInfo.put(correlationId, new GetRequestInfo(replicaId, time.milliseconds()));
      requestRegistrationCallback.registerRequestToSend(this, request);
      replicaIterator.remove();
      if (RouterUtils.isRemoteReplica(routerConfig, replicaId)) {
        logger.trace("Making request to a remote replica in", replicaId.getDataNodeId().getDatacenterName());
        routerMetrics.crossColoRequestCount.inc();
      }
      routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getBlobInfoRequestRate.mark();
    }
  }

  /**
   * Handle the given {@link ResponseInfo} and update the status of the operation.
   * Based on the status of the response, this involves handling the body, notifying the operation tracker,
   * notifying the response handler and possibly setting the operationException.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   */
  @Override
  void handleResponse(ResponseInfo responseInfo) {
    if (isOperationComplete()) {
      return;
    }
    int correlationId = ((GetRequest) responseInfo.getRequest()).getCorrelationId();
    // Get the GetOperation that generated the request.
    GetRequestInfo getRequestInfo = correlationIdToGetRequestInfo.remove(correlationId);
    if (getRequestInfo == null) {
      // Ignore. The request must have timed out.
      return;
    }
    long requestLatencyMs = time.milliseconds() - getRequestInfo.startTimeMs;
    routerMetrics.routerRequestLatencyMs.update(requestLatencyMs);
    routerMetrics.getDataNodeBasedMetrics(getRequestInfo.replicaId.getDataNodeId()).getBlobInfoRequestLatencyMs
        .update(requestLatencyMs);
    if (responseInfo.getError() != null) {
      setOperationException(new RouterException("Operation timed out", RouterErrorCode.OperationTimedOut));
      responseHandler.onRequestResponseException(getRequestInfo.replicaId, new IOException("NetworkClient error"));
      onErrorResponse(getRequestInfo.replicaId);
    } else {
      try {
        GetResponse getResponse = GetResponse
            .readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())), clusterMap);
        if (getResponse.getCorrelationId() != correlationId) {
          // The NetworkClient associates a response with a request based on the fact that only one request is sent
          // out over a connection id, and the response received on a connection id must be for the latest request
          // sent over it. The check here ensures that is indeed the case. If not, log an error and fail this request.
          // There is no other way to handle it.
          routerMetrics.unknownReplicaResponseError.inc();
          setOperationException(
              new RouterException("The correlation id in the GetResponse " + getResponse.getCorrelationId() +
                  "is not the same as the correlation id in the associated GetRequest: " + correlationId,
                  RouterErrorCode.UnexpectedInternalError));
          onErrorResponse(getRequestInfo.replicaId);
          // we do not notify the ResponseHandler responsible for failure detection as this is an unexpected error.
        } else {
          processGetBlobInfoResponse(getRequestInfo, getResponse);
        }
      } catch (IOException | MessageFormatException e) {
        // This should really not happen. Again, we do not notify the ResponseHandler responsible for failure
        // detection.
        setOperationException(new RouterException("Response deserialization received an unexpected error", e,
            RouterErrorCode.UnexpectedInternalError));
        onErrorResponse(getRequestInfo.replicaId);
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
        setOperationException(new RouterException("Unexpected number of partition responses, expected: 1, " +
            "received: " + partitionsInResponse, RouterErrorCode.UnexpectedInternalError));
        onErrorResponse(getRequestInfo.replicaId);
        // Again, no need to notify the responseHandler.
      } else {
        getError = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
        responseHandler.onRequestResponseError(getRequestInfo.replicaId, getError);
        if (getError == ServerErrorCode.No_Error) {
          handleBody(getResponse.getInputStream());
          operationTracker.onResponse(getRequestInfo.replicaId, true);
          if (RouterUtils.isRemoteReplica(routerConfig, getRequestInfo.replicaId)) {
            logger.trace("Cross colo request successful for remote replica in ",
                getRequestInfo.replicaId.getDataNodeId().getDatacenterName());
            routerMetrics.crossColoSuccessCount.inc();
          }
        } else {
          // process and set the most relevant exception.
          processServerError(getError);
          if (getError == ServerErrorCode.Blob_Deleted || getError == ServerErrorCode.Blob_Expired) {
            // this is a successful response and one that completes the operation regardless of whether the
            // success target has been reached or not.
            operationCompleted = true;
          } else {
            onErrorResponse(getRequestInfo.replicaId);
          }
        }
      }
    } else {
      responseHandler.onRequestResponseError(getRequestInfo.replicaId, getError);
      onErrorResponse(getRequestInfo.replicaId);
    }
  }

  /**
   * Perform the necessary actions when a request to a replica fails.
   * @param replicaId the {@link ReplicaId} associated with the failed response.
   */
  void onErrorResponse(ReplicaId replicaId) {
    operationTracker.onResponse(replicaId, false);
    routerMetrics.routerRequestErrorCount.inc();
    routerMetrics.getDataNodeBasedMetrics(replicaId.getDataNodeId()).getBlobInfoRequestErrorCount.inc();
  }

  /**
   * Handle the body of the response: Deserialize and set the {@link BlobInfo} to return.
   * @param payload the body of the response.
   * @throws IOException if there is an IOException while deserializing the body.
   * @throws MessageFormatException if there is a MessageFormatException while deserializing the body.
   */
  private void handleBody(InputStream payload)
      throws IOException, MessageFormatException {
    if (operationResult == null) {
      operationResult = new BlobInfo(MessageFormatRecord.deserializeBlobProperties(payload),
          MessageFormatRecord.deserializeUserMetadata(payload).array());
    } else {
      // If the successTarget is 1, this case will never get executed.
      // If it is more than 1, then, different responses will have to be reconciled in some way. Here is where that
      // would be done. Since the store is immutable, currently we handle this by ignoring subsequent responses.
    }
  }

  /**
   * Process the given {@link ServerErrorCode} and set operation status accordingly.
   * @param errorCode the {@link ServerErrorCode} to process.
   */
  private void processServerError(ServerErrorCode errorCode) {
    logger.trace("Server returned an error: ", errorCode);
    switch (errorCode) {
      case Blob_Deleted:
        logger.trace("Requested blob was deleted");
        setOperationException(new RouterException("Server returned: " + errorCode, RouterErrorCode.BlobDeleted));
        break;
      case Blob_Expired:
        logger.trace("Requested blob has expired");
        setOperationException(new RouterException("Server returned: " + errorCode, RouterErrorCode.BlobExpired));
        break;
      case Blob_Not_Found:
        logger.trace("Requested blob was not found on this server");
        setOperationException(new RouterException("Server returned: " + errorCode, RouterErrorCode.BlobDoesNotExist));
        break;
      default:
        setOperationException(
            new RouterException("Server returned: " + errorCode, RouterErrorCode.UnexpectedInternalError));
        break;
    }
  }

  /**
   * Check whether the operation can be completed, if so complete it.
   */
  private void checkAndMaybeComplete() {
    if (operationTracker.isDone()) {
      if (operationTracker.hasSucceeded()) {
        operationException.set(null);
      }
      operationCompleted = true;
    }

    if (operationCompleted) {
      Exception e = operationException.get();
      if (e != null) {
        routerMetrics.getBlobInfoErrorCount.inc();
        routerMetrics.countError(e);
      }
      routerMetrics.getBlobInfoOperationLatencyMs.update(time.milliseconds() - submissionTimeMs);
      operationCompleteCallback.completeOperation(operationFuture, operationCallback, operationResult, e);
    }
  }
}

