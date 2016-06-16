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
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.utils.Time;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An abstract class for a get operation.
 * @param <T> the type of the result of this operation.
 */
abstract class GetOperation<T> {
  protected final RouterConfig routerConfig;
  protected final NonBlockingRouterMetrics routerMetrics;
  protected final ClusterMap clusterMap;
  protected final ResponseHandler responseHandler;
  protected final FutureResult<T> operationFuture;
  protected final Callback<T> operationCallback;
  protected final BlobId blobId;
  protected final Time time;
  protected volatile boolean operationCompleted = false;
  protected final AtomicReference<Exception> operationException = new AtomicReference<>();
  protected T operationResult;
  protected final long submissionTimeMs;

  private static final Logger logger = LoggerFactory.getLogger(GetOperation.class);

  /**
   * Construct a GetOperation
   * @param routerConfig the {@link RouterConfig} containing the configs for put operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param blobIdStr the blobId of the associated blob in string form.
   * @param futureResult the future that will contain the result of the operation.
   * @param operationCallback the callback that is to be called when the operation completes.
   * @param time the {@link Time} instance to use.
   * @throws RouterException if there is an error with any of the parameters, such as an invalid blob id.
   */
  GetOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, String blobIdStr, FutureResult<T> futureResult, Callback<T> operationCallback,
      Time time)
      throws RouterException {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.operationFuture = futureResult;
    this.operationCallback = operationCallback;
    this.time = time;
    submissionTimeMs = time.milliseconds();
    blobId = RouterUtils.getBlobIdFromString(blobIdStr, clusterMap);
  }

  /**
   * Return the {@link FutureResult} associated with this operation.
   * @return the {@link FutureResult} associated with this operation.
   */
  FutureResult<T> getFuture() {
    return operationFuture;
  }

  /**
   * Return the {@link Callback} associated with this operation.
   * @return the {@link Callback} associated with this operation.
   */
  Callback<T> getCallback() {
    return operationCallback;
  }

  /**
   * The exception associated with this operation if it failed; null otherwise.
   * @return exception associated with this operation if it failed; null otherwise.
   */
  Exception getOperationException() {
    return operationException.get();
  }

  /**
   * Return the result of the operation.
   * @return the operation result.
   */
  T getOperationResult() {
    return operationResult;
  }

  /**
   * Return the blob id string
   * @return return the blob id string
   */
  String getBlobIdStr() {
    return blobId.getID();
  }

  /**
   * returns whether the operation has completed.
   * @return whether the operation has completed.
   */
  boolean isOperationComplete() {
    return operationCompleted;
  }

  /**
   * Return the {@link MessageFormatFlags} to associate with the requests for this operation.
   * @return the {@link MessageFormatFlags} to associate with the requests for this operation.
   */
  abstract MessageFormatFlags getOperationFlag();

  /**
   * For this operation, create and populate get requests to send out.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
   *                                    created as part of this poll operation.
   */
  abstract void poll(RequestRegistrationCallback<GetOperation> requestRegistrationCallback);

  /**
   * Handle the given {@link ResponseInfo} received for a request that was sent out.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   */
  abstract void handleResponse(ResponseInfo responseInfo);

  /**
   * Abort operation by invoking any callbacks and updating futures with an exception.
   * @param abortCause the exception that is the cause for the abort.
   */
  abstract void abort(Exception abortCause);

  /**
   * Set the exception associated with this operation.
   * A {@link ServerErrorCode#Blob_Deleted} or {@link ServerErrorCode#Blob_Expired} error overrides any other
   * previously received exception.
   * @param exception the {@link RouterException} to possibly set.
   */
  void setOperationException(RouterException exception) {
    if (operationException.get() == null || exception.getErrorCode() == RouterErrorCode.BlobDeleted
        || exception.getErrorCode() == RouterErrorCode.BlobExpired) {
      operationException.set(exception);
    }
  }

  /**
   * Create and return the {@link GetRequest} associated with the given blobId.
   * @return the created {@link GetRequest}.
   * @param blobId The {@link BlobId} for which the {@link GetRequest} is being created.
   * @param flag The {@link MessageFormatFlags} to be set with the GetRequest.
   * @return the created GetRequest.
   */
  protected GetRequest createGetRequest(BlobId blobId, MessageFormatFlags flag, GetOptions getOptions) {
    List<BlobId> blobIds = Collections.singletonList(blobId);
    List<PartitionRequestInfo> partitionRequestInfoList =
        Collections.singletonList(new PartitionRequestInfo(blobId.getPartition(), blobIds));
    return new GetRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname, flag,
        partitionRequestInfoList, getOptions);
  }
}

/**
 * A class that holds information about the get requests sent out.
 */
class GetRequestInfo {
  final ReplicaId replicaId;
  final long startTimeMs;

  /**
   * Construct a GetRequestInfo
   * @param replicaId the replica to which this request is being sent.
   * @param startTimeMs the time at which this request was created.
   */
  GetRequestInfo(ReplicaId replicaId, long startTimeMs) {
    this.replicaId = replicaId;
    this.startTimeMs = startTimeMs;
  }
}

