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
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.utils.Time;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GetManager manages GetBlob and GetBlobInfo operations. This is just a template for now.
 * These methods have to be thread safe.
 */
class GetManager {
  private static final Logger logger = LoggerFactory.getLogger(GetManager.class);

  private final Set<GetOperation> getOperations;
  private final Time time;
  // This helps the GetManager quickly find the appropriate GetOperation to hand over the response to.
  // Requests are added before they are sent out and get cleaned up as and when responses come in.
  // Because there is a guaranteed response from the NetworkClient for every request sent out, entries
  // get cleaned up periodically.
  private final Map<Integer, GetOperation> correlationIdToGetOperation = new HashMap<Integer, GetOperation>();

  // shared by all GetOperations
  private final ClusterMap clusterMap;
  private final BlobIdFactory blobIdFactory;
  private final RouterConfig routerConfig;
  private final ResponseHandler responseHandler;
  private final NonBlockingRouterMetrics routerMetrics;
  private final OperationCompleteCallback operationCompleteCallback;

  private class GetRequestRegistrationCallbackImpl implements RequestRegistrationCallback<GetOperation> {
    private List<RequestInfo> requestListToFill;

    @Override
    public void registerRequestToSend(GetOperation getOperation, RequestInfo requestInfo) {
      requestListToFill.add(requestInfo);
      correlationIdToGetOperation.put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(), getOperation);
    }
  }

  // A single callback as this will never get called concurrently. The list of request to fill will be set as
  // appropriate before the callback is passed on to GetOperations, every time.
  private final GetRequestRegistrationCallbackImpl requestRegistrationCallback =
      new GetRequestRegistrationCallbackImpl();

  /**
   * Create a GetManager
   * @param clusterMap The {@link ClusterMap} of the cluster.
   * @param responseHandler The {@link ResponseHandler} used to notify failures for failure detection.
   * @param routerConfig  The {@link RouterConfig} containing the configs for the PutManager.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param operationCompleteCallback The {@link OperationCompleteCallback} to use to complete operations.
   * @param time The {@link Time} instance to use.
   */
  GetManager(ClusterMap clusterMap, ResponseHandler responseHandler, RouterConfig routerConfig,
      NonBlockingRouterMetrics routerMetrics, OperationCompleteCallback operationCompleteCallback, Time time) {
    this.clusterMap = clusterMap;
    blobIdFactory = new BlobIdFactory(clusterMap);
    this.responseHandler = responseHandler;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.operationCompleteCallback = operationCompleteCallback;
    this.time = time;
    getOperations = Collections.newSetFromMap(new ConcurrentHashMap<GetOperation, Boolean>());
  }

  /**
   * Submit an operation to get the BlobInfo associated with a blob asynchronously.
   * @param blobId the blobId for which the BlobInfo is being requested, in string form.
   * @param futureResult the {@link FutureResult} that contains the pending result of the operation.
   * @param callback the {@link Callback} object to be called on completion of the operation.
   */
  void submitGetBlobInfoOperation(String blobId, FutureResult<BlobInfo> futureResult, Callback<BlobInfo> callback) {
    try {
      GetBlobInfoOperation getBlobInfoOperation =
          new GetBlobInfoOperation(routerConfig, routerMetrics, clusterMap, responseHandler, blobId, futureResult,
              callback, operationCompleteCallback, time);
      getOperations.add(getBlobInfoOperation);
    } catch (RouterException e) {
      routerMetrics.getBlobInfoErrorCount.inc();
      routerMetrics.countError(e);
      routerMetrics.operationDequeuingRate.mark();
      operationCompleteCallback.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * Submit an operation to get a blob asynchronously.
   * @param blobId the blobId for which the BlobInfo is being requested, in string form.
   * @param futureResult the {@link FutureResult} that contains the pending result of the operation.
   * @param callback the {@link Callback} object to be called on completion of the operation.
   */
  void submitGetBlobOperation(String blobId, FutureResult<ReadableStreamChannel> futureResult,
      Callback<ReadableStreamChannel> callback) {
    try {
      GetBlobOperation getBlobOperation =
          new GetBlobOperation(routerConfig, routerMetrics, clusterMap, responseHandler, blobId, futureResult, callback,
              operationCompleteCallback, blobIdFactory, time);
      getOperations.add(getBlobOperation);
    } catch (RouterException e) {
      routerMetrics.getBlobErrorCount.inc();
      routerMetrics.countError(e);
      routerMetrics.operationDequeuingRate.mark();
      operationCompleteCallback.completeOperation(futureResult, callback, null, e);
    }
  }

  /**
   * Remove the operation from the set of operations handled by the GetManager.
   * This can potentially be called concurrently for the same operation, which is fine.
   * @param op the {@link GetOperation} to remove.
   * @return true if the operation was removed in this call.
   */
  private boolean remove(GetOperation op) {
    if (getOperations.remove(op)) {
      routerMetrics.operationDequeuingRate.mark();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Creates and returns requests in the form of {@link RequestInfo} to be sent to data nodes in order to complete
   * get operations. Since this is the only method guaranteed to be called periodically by the RequestResponseHandler
   * thread in the {@link NonBlockingRouter} ({@link #handleResponse} gets called only if a
   * response is received for a get operation), any error handling or operation completion and cleanup also usually
   * gets done in the context of this method.
   * @param requestListToFill list to be filled with the requests created
   */
  void poll(List<RequestInfo> requestListToFill) {
    requestRegistrationCallback.requestListToFill = requestListToFill;
    for (GetOperation op : getOperations) {
      op.poll(requestRegistrationCallback);
      if (op.isOperationComplete()) {
        remove(op);
      }
    }
  }

  /**
   * Hands over the response to the associated GetOperation that issued the request.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    GetRequest getRequest = (GetRequest) responseInfo.getRequest();
    GetOperation getOperation = correlationIdToGetOperation.remove(getRequest.getCorrelationId());
    if (getOperations.contains(getOperation)) {
      getOperation.handleResponse(responseInfo);
      if (getOperation.isOperationComplete()) {
        remove(getOperation);
      }
    } else {
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  /**
   * Close the GetManager.
   * Complete all existing get operations.
   */
  void close() {
    for (GetOperation op : getOperations) {
      // There is a rare scenario where the operation gets removed from this set and gets completed concurrently by
      // the RequestResponseHandler thread when it is in poll() or handleResponse(). In order to avoid the completion
      // from happening twice, complete it here only if the remove was successful.
      if (remove(op)) {
        RouterException e =
            new RouterException("Aborted operation because Router is closed", RouterErrorCode.RouterClosed);
        op.abort(e);
        routerMetrics.operationAbortCount.inc();
        routerMetrics.countError(e);
      }
    }
  }
}

