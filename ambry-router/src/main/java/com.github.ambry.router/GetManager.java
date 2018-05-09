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

import com.codahale.metrics.Meter;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Time;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GetManager manages GetBlob and GetBlobInfo operations.
 * These methods have to be thread safe.
 */
class GetManager {
  private static final Logger logger = LoggerFactory.getLogger(GetManager.class);

  private final Set<GetOperation> getOperations;
  private final KeyManagementService kms;
  private final CryptoService cryptoService;
  private final CryptoJobHandler cryptoJobHandler;
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
  private final RouterCallback routerCallback;

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
   * @param routerCallback The {@link RouterCallback} to use for callbacks to the router.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time The {@link Time} instance to use.
   */
  GetManager(ClusterMap clusterMap, ResponseHandler responseHandler, RouterConfig routerConfig,
      NonBlockingRouterMetrics routerMetrics, RouterCallback routerCallback, KeyManagementService kms,
      CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time) {
    this.clusterMap = clusterMap;
    blobIdFactory = new BlobIdFactory(clusterMap);
    this.responseHandler = responseHandler;
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.routerCallback = routerCallback;
    this.kms = kms;
    this.cryptoService = cryptoService;
    this.cryptoJobHandler = cryptoJobHandler;
    this.time = time;
    getOperations = Collections.newSetFromMap(new ConcurrentHashMap<GetOperation, Boolean>());
  }

  /**
   * Submit an operation to get a blob asynchronously.
   * @param blobIdStr The ID of the blob for which blob data is requested.
   * @param options The {@link GetBlobOptionsInternal} associated with the operation.
   * @param callback The {@link Callback} object to be called on completion of the operation.
   * @throws RouterException if the blobIdStr is invalid.
   */
  void submitGetBlobOperation(String blobIdStr, GetBlobOptionsInternal options,
      Callback<GetBlobResultInternal> callback) throws RouterException {
    GetOperation getOperation;
    BlobId blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
    boolean isEncrypted = false;
    try {
      BlobId.isEncrypted(blobIdStr);
    } catch (IOException e) {
      logger.warn(
          "This shouldn't happen because getBlobIdFromString() should have thrown RouterException for this case.", e);
    }
    if (blobId.getDatacenterId() != ClusterMapUtils.UNKNOWN_DATACENTER_ID
        && blobId.getDatacenterId() != clusterMap.getLocalDatacenterId()) {
      routerMetrics.getBlobNotOriginateLocalOperationRate.mark();
    }
    trackGetBlobRateMetrics(options.getBlobOptions, isEncrypted);
    if (options.getBlobOptions.getOperationType() == GetBlobOptions.OperationType.BlobInfo) {
      getOperation =
          new GetBlobInfoOperation(routerConfig, routerMetrics, clusterMap, responseHandler, blobId, options, callback,
              routerCallback, kms, cryptoService, cryptoJobHandler, time, isEncrypted);
    } else {
      getOperation =
          new GetBlobOperation(routerConfig, routerMetrics, clusterMap, responseHandler, blobId, options, callback,
              routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time, isEncrypted);
    }
    getOperations.add(getOperation);
  }

  /**
   * Track get blob rate metrics based on the {@link GetBlobOptions} and whether the blob is encrypted or not
   * @param options {@link GetBlobOptions} instance to use
   * @param isEncrypted {@code true} if the blob is encrypted, {@code false} otherwise
   */
  private void trackGetBlobRateMetrics(GetBlobOptions options, boolean isEncrypted) {
    if (options.getOperationType() == GetBlobOptions.OperationType.BlobInfo) {
      Meter blobInfoOperationRate =
          isEncrypted ? routerMetrics.getEncryptedBlobInfoOperationRate : routerMetrics.getBlobInfoOperationRate;
      blobInfoOperationRate.mark();
    } else {
      Meter blobOperationRate =
          isEncrypted ? routerMetrics.getEncryptedBlobOperationRate : routerMetrics.getBlobOperationRate;
      blobOperationRate.mark();
    }
    if (options.getRange() != null) {
      Meter blobWithRangeOperationRate = isEncrypted ? routerMetrics.getEncryptedBlobWithRangeOperationRate
          : routerMetrics.getBlobWithRangeOperationRate;
      blobWithRangeOperationRate.mark();
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
    long startTime = time.milliseconds();
    requestRegistrationCallback.requestListToFill = requestListToFill;
    for (GetOperation op : getOperations) {
      try {
        op.poll(requestRegistrationCallback);
        if (op.isOperationComplete()) {
          remove(op);
        }
      } catch (Exception e) {
        removeAndAbort(op,
            new RouterException("Get poll encountered unexpected error", e, RouterErrorCode.UnexpectedInternalError));
      }
    }
    routerMetrics.getManagerPollTimeMs.update(time.milliseconds() - startTime);
  }

  /**
   * Hands over the response to the associated GetOperation that issued the request.
   * @param responseInfo the {@link ResponseInfo} containing the response.
   */
  void handleResponse(ResponseInfo responseInfo) {
    long startTime = time.milliseconds();
    GetResponse getResponse = extractGetResponseAndNotifyResponseHandler(responseInfo);
    RouterRequestInfo routerRequestInfo = (RouterRequestInfo) responseInfo.getRequestInfo();
    GetRequest getRequest = (GetRequest) routerRequestInfo.getRequest();
    GetOperation getOperation = correlationIdToGetOperation.remove(getRequest.getCorrelationId());
    if (getOperations.contains(getOperation)) {
      try {
        getOperation.handleResponse(responseInfo, getResponse);
        if (getOperation.isOperationComplete()) {
          remove(getOperation);
        }
      } catch (Exception e) {
        removeAndAbort(getOperation, new RouterException("Get handleResponse encountered unexpected error", e,
            RouterErrorCode.UnexpectedInternalError));
      }
      routerMetrics.getManagerHandleResponseTimeMs.update(time.milliseconds() - startTime);
    } else {
      routerMetrics.ignoredResponseCount.inc();
    }
  }

  /**
   * Extract the {@link GetResponse} from the given {@link ResponseInfo}
   * @param responseInfo the {@link ResponseInfo} from which the {@link GetResponse} is to be extracted.
   * @return the extracted {@link GetResponse} if there is one; null otherwise.
   */
  private GetResponse extractGetResponseAndNotifyResponseHandler(ResponseInfo responseInfo) {
    GetResponse getResponse = null;
    ReplicaId replicaId = ((RouterRequestInfo) responseInfo.getRequestInfo()).getReplicaId();
    NetworkClientErrorCode networkClientErrorCode = responseInfo.getError();
    if (networkClientErrorCode == null) {
      try {
        getResponse = GetResponse.readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())),
            clusterMap);
        ServerErrorCode serverError = getResponse.getError();
        if (serverError == ServerErrorCode.No_Error) {
          serverError = getResponse.getPartitionResponseInfoList().get(0).getErrorCode();
        }
        responseHandler.onEvent(replicaId, serverError);
      } catch (Exception e) {
        // Ignore. There is no value in notifying the response handler.
        logger.error("Response deserialization received unexpected error", e);
        routerMetrics.responseDeserializationErrorCount.inc();
      }
    } else {
      responseHandler.onEvent(replicaId, networkClientErrorCode);
    }
    return getResponse;
  }

  /**
   * Close the GetManager.
   * Complete all existing get operations.
   */
  void close() {
    for (GetOperation op : getOperations) {
      removeAndAbort(op,
          new RouterException("Aborted operation because Router is closed", RouterErrorCode.RouterClosed));
    }
  }

  /**
   * Remove an operation from the set and abort.
   * @param op the operation to abort
   * @param abortCause the reason for aborting
   */
  private void removeAndAbort(GetOperation op, Exception abortCause) {
    // There is a rare scenario where the operation gets removed from this set and gets completed concurrently by
    // the RequestResponseHandler thread when it is in poll() or handleResponse(). In order to avoid the completion
    // from happening twice, complete it here only if the remove was successful.
    if (remove(op)) {
      op.abort(abortCause);
      routerMetrics.operationAbortCount.inc();
      routerMetrics.onGetBlobError(abortCause, op.getOptions(), op.isEncrypted);
    }
  }
}

/**
 * An internal options class containing parameters to the GetBlob operation.
 */
class GetBlobOptionsInternal {
  final GetBlobOptions getBlobOptions;
  final boolean getChunkIdsOnly;
  final NonBlockingRouterMetrics.AgeAtAccessMetrics ageAtAccessTracker;

  /**
   * Construct an GetBlobOptionsInternal instance
   * @param getBlobOptions the {@link GetBlobOptions} associated with this instance.
   * @param getChunkIdsOnly {@code true} if this operation is to fetch just the chunk ids of a composite blob.
   * @param ageAtAccessTracker the {@link NonBlockingRouterMetrics.AgeAtAccessMetrics} tracker to use.
   */
  GetBlobOptionsInternal(GetBlobOptions getBlobOptions, boolean getChunkIdsOnly,
      NonBlockingRouterMetrics.AgeAtAccessMetrics ageAtAccessTracker) {
    this.getBlobOptions = getBlobOptions;
    this.getChunkIdsOnly = getChunkIdsOnly;
    this.ageAtAccessTracker = ageAtAccessTracker;
  }
}

class GetBlobResultInternal {
  GetBlobResult getBlobResult;
  List<StoreKey> storeKeys;

  /**
   * Construct a GetBlobResultInternal instance.
   * @param getBlobResult The {@link GetBlobResult} associated with this instance, if there is one..
   * @param storeKeys The store keys associated with this instance, if there are any.
   */
  public GetBlobResultInternal(GetBlobResult getBlobResult, List<StoreKey> storeKeys) {
    this.getBlobResult = getBlobResult;
    this.storeKeys = storeKeys;
  }
}
