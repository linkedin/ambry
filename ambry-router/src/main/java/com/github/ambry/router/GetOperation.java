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
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An abstract class for a get operation.
 */
abstract class GetOperation {
  protected final RouterConfig routerConfig;
  protected final NonBlockingRouterMetrics routerMetrics;
  protected final ClusterMap clusterMap;
  protected final ResponseHandler responseHandler;
  protected final Callback<GetBlobResultInternal> getOperationCallback;
  protected final BlobId blobId;
  protected final GetBlobOptionsInternal options;
  protected final KeyManagementService kms;
  protected final CryptoService cryptoService;
  protected final CryptoJobHandler cryptoJobHandler;
  protected final Time time;
  protected volatile boolean operationCompleted = false;
  protected final AtomicReference<Exception> operationException = new AtomicReference<>();
  protected GetBlobResultInternal operationResult;
  protected final long submissionTimeMs;
  protected final boolean isEncrypted;

  private static final Logger logger = LoggerFactory.getLogger(GetOperation.class);

  /**
   * Construct a GetOperation
   * @param routerConfig the {@link RouterConfig} containing the configs for get operations.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to be used for reporting metrics.
   * @param clusterMap the {@link ClusterMap} of the cluster
   * @param responseHandler the {@link ResponseHandler} responsible for failure detection.
   * @param blobId the {@link BlobId} of the associated blob
   * @param options the {@link GetBlobOptionsInternal} associated with this operation.
   * @param getOperationCallback the callback that is to be called when the operation completes.
   * @param kms {@link KeyManagementService} to assist in fetching container keys for encryption or decryption
   * @param cryptoService {@link CryptoService} to assist in encryption or decryption
   * @param cryptoJobHandler {@link CryptoJobHandler} to assist in the execution of crypto jobs
   * @param time the {@link Time} instance to use.
   * @param isEncrypted if encrypted bit is set based on original blobId string.
   */
  GetOperation(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics, ClusterMap clusterMap,
      ResponseHandler responseHandler, BlobId blobId, GetBlobOptionsInternal options,
      Callback<GetBlobResultInternal> getOperationCallback, KeyManagementService kms, CryptoService cryptoService,
      CryptoJobHandler cryptoJobHandler, Time time, boolean isEncrypted) {
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.options = options;
    this.getOperationCallback = getOperationCallback;
    this.kms = kms;
    this.cryptoService = cryptoService;
    this.cryptoJobHandler = cryptoJobHandler;
    this.time = time;
    submissionTimeMs = time.milliseconds();
    this.blobId = blobId;
    this.isEncrypted = isEncrypted;
    validateTrackerType();
  }

  /**
   * Return the {@link Callback} associated with this operation.
   * @return the {@link Callback} associated with this operation.
   */
  Callback<GetBlobResultInternal> getCallback() {
    return getOperationCallback;
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
  GetBlobResultInternal getOperationResult() {
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
   * @return The {@link GetBlobOptions} associated with this operation.
   */
  GetBlobOptionsInternal getOptions() {
    return options;
  }

  /**
   * returns whether the operation has completed.
   * @return whether the operation has completed.
   */
  boolean isOperationComplete() {
    return operationCompleted;
  }

  /**
   * For this operation, create and populate get requests to send out.
   * @param requestRegistrationCallback the {@link RequestRegistrationCallback} to call for every request that gets
   *                                    created as part of this poll operation.
   */
  abstract void poll(RequestRegistrationCallback<GetOperation> requestRegistrationCallback);

  /**
   * Handle the given {@link ResponseInfo} received for a request that was sent out.
   * @param responseInfo the {@link ResponseInfo} to be handled.
   * @param getResponse the {@link GetResponse} associated with this response.
   */
  abstract void handleResponse(ResponseInfo responseInfo, GetResponse getResponse);

  /**
   * Abort operation by invoking any callbacks and updating futures with an exception.
   * @param abortCause the exception that is the cause for the abort.
   */
  abstract void abort(Exception abortCause);

  /**
   * Set the exception associated with this operation.
   * First, if current operationException is null, directly set operationException as exception;
   * Second, if operationException exists, compare ErrorCodes of exception and existing operation Exception depending
   * on precedence level. An ErrorCode with a smaller precedence level overrides an ErrorCode with a larger precedence
   * level. Update the operationException if necessary.
   * @param exception the {@link RouterException} to possibly set.
   */
  void setOperationException(Exception exception) {
    if (exception instanceof RouterException) {
      RouterUtils.replaceOperationException(operationException, (RouterException) exception, this::getPrecedenceLevel);
    } else {
      operationException.compareAndSet(null, exception);
    }
  }

  /**
   * Gets the precedence level for a {@link RouterErrorCode}. A precedence level is a relative priority assigned
   * to a {@link RouterErrorCode}. If a {@link RouterErrorCode} has not been assigned a precedence level, a
   * {@code Integer.MIN_VALUE} will be returned.
   * @param routerErrorCode The {@link RouterErrorCode} for which to get its precedence level.
   * @return The precedence level of the {@link RouterErrorCode}.
   */
  protected int getPrecedenceLevel(RouterErrorCode routerErrorCode) {
    switch (routerErrorCode) {
      case BlobAuthorizationFailure:
        return 1;
      case BlobDeleted:
        return 2;
      case BlobExpired:
        return 3;
      case RangeNotSatisfiable:
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
   * Create and return the {@link GetRequest} associated with the given blobId.
   * @return the created {@link GetRequest}.
   * @param blobId The {@link BlobId} for which the {@link GetRequest} is being created.
   * @param flag The {@link MessageFormatFlags} to be set with the GetRequest.
   * @return the created GetRequest.
   */
  protected GetRequest createGetRequest(BlobId blobId, MessageFormatFlags flag, GetOption getOption) {
    List<BlobId> blobIds = Collections.singletonList(blobId);
    List<PartitionRequestInfo> partitionRequestInfoList =
        Collections.singletonList(new PartitionRequestInfo(blobId.getPartition(), blobIds));
    return new GetRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname, flag,
        partitionRequestInfoList, getOption);
  }

  /**
   * Gets an {@link OperationTracker} based on the config and {@code partitionId}.
   * @param partitionId the {@link PartitionId} for which a tracker is required.
   * @param datacenterId the id of datacenter in which the blob originated.
   * @param routerOperation The type of router operation used by tracker.
   * @return an {@link OperationTracker} based on the config and {@code partitionId}.
   */
  protected OperationTracker getOperationTracker(PartitionId partitionId, byte datacenterId,
      RouterOperation routerOperation) {
    OperationTracker operationTracker;
    String trackerType = routerConfig.routerGetOperationTrackerType;
    String originatingDcName = clusterMap.getDatacenterName(datacenterId);
    if (trackerType.equals(SimpleOperationTracker.class.getSimpleName())) {
      operationTracker =
          new SimpleOperationTracker(routerConfig, routerOperation, partitionId, originatingDcName, true);
    } else if (trackerType.equals(AdaptiveOperationTracker.class.getSimpleName())) {
      operationTracker =
          new AdaptiveOperationTracker(routerConfig, routerMetrics, routerOperation, partitionId, originatingDcName,
              time);
    } else {
      throw new IllegalArgumentException("Unrecognized tracker type: " + trackerType);
    }
    return operationTracker;
  }

  /**
   * Updates the TTL in {@code blobProperties} if required
   * @param blobProperties the {@link BlobProperties} of the blob
   * @param messageInfo the {@link MessageInfo} received with the GET response
   */
  protected void updateTtlIfRequired(BlobProperties blobProperties, MessageInfo messageInfo) {
    if (messageInfo.isTtlUpdated()) {
      long newTtlSecs =
          Utils.getTtlInSecsFromExpiryMs(messageInfo.getExpirationTimeInMs(), blobProperties.getCreationTimeInMs());
      blobProperties.setTimeToLiveInSeconds(newTtlSecs);
    }
  }

  /**
   * Validates the tracker type in config.
   */
  private void validateTrackerType() {
    String trackerType = routerConfig.routerGetOperationTrackerType;
    if (!trackerType.equals(SimpleOperationTracker.class.getSimpleName()) && !trackerType.equals(
        AdaptiveOperationTracker.class.getSimpleName())) {
      throw new IllegalArgumentException("Unrecognized tracker type: " + trackerType);
    }
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

