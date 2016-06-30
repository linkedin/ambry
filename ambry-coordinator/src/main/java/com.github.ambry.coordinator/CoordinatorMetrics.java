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
package com.github.ambry.coordinator;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.messageformat.MessageFormatErrorCodes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Metrics for the coordinator
 */
public class CoordinatorMetrics {
  public final Histogram putBlobOperationLatencyInMs;
  public final Histogram deleteBlobOperationLatencyInMs;
  public final Histogram getBlobPropertiesOperationLatencyInMs;
  public final Histogram getBlobUserMetadataOperationLatencyInMs;
  public final Histogram getBlobOperationLatencyInMs;
  public final Histogram operationRequestQueuingTimeInMs;

  public final Meter putBlobOperationRate;
  public final Meter deleteBlobOperationRate;
  public final Meter getBlobPropertiesOperationRate;
  public final Meter getBlobUserMetadataOperationRate;
  public final Meter getBlobOperationRate;
  public final Meter operationExceptionRate;
  public final Meter plainTextConnectionsRequestRate;
  public final Meter sslConnectionsRequestRate;

  public final Counter blobAlreadyExistsInLocalColoError;
  public final Counter blobAlreadyExistsInRemoteColoError;

  private final Counter putBlobError;
  private final Counter deleteBlobError;
  private final Counter getBlobPropertiesError;
  private final Counter getBlobUserMetadataError;
  private final Counter getBlobError;

  private final Counter unexpectedInternalError;
  private final Counter ambryUnavailableError;
  private final Counter operationTimedOutError;
  private final Counter invalidBlobIdError;
  private final Counter invalidPutArgumentError;
  private final Counter insufficientCapacityError;
  private final Counter blobTooLargeError;
  private final Counter blobDoesNotExistError;
  private final Counter blobDeletedError;
  private final Counter blobExpiredError;
  private final Counter unknownError;

  public final Counter corruptionError;

  public final Counter unknownReplicaResponseError;
  public final Counter successfulCrossColoProxyCallCount;
  public final Counter totalCrossColoProxyCallCount;

  public final Gauge<Integer> crossDCCallsEnabled;
  public final AtomicInteger totalRequestsInFlight = new AtomicInteger(0);
  public final AtomicInteger totalRequestsInExecution = new AtomicInteger(0);

  private final Map<DataNodeId, RequestMetrics> requestMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public CoordinatorMetrics(ClusterMap clusterMap, final boolean isCrossDatacenterCallsEnabled) {
    MetricRegistry registry = clusterMap.getMetricRegistry();
    putBlobOperationLatencyInMs =
        registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "putBlobOperationLatencyInMs"));
    deleteBlobOperationLatencyInMs =
        registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "deleteBlobOperationLatencyInMs"));
    getBlobPropertiesOperationLatencyInMs =
        registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "getBlobPropertiesOperationLatencyInMs"));
    getBlobUserMetadataOperationLatencyInMs =
        registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "getBlobUserMetadataOperationLatencyInMs"));
    getBlobOperationLatencyInMs =
        registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "getBlobOperationLatencyInMs"));
    operationRequestQueuingTimeInMs =
        registry.histogram(MetricRegistry.name(AmbryCoordinator.class, "operationRequestQueuingTimeInMs"));

    putBlobOperationRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "putBlobOperationRate"));
    deleteBlobOperationRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "deleteBlobOperationRate"));
    getBlobPropertiesOperationRate =
        registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobPropertiesOperationRate"));
    getBlobUserMetadataOperationRate =
        registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobUserMetadataOperationRate"));
    getBlobOperationRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "getBlobOperationRate"));
    operationExceptionRate = registry.meter(MetricRegistry.name(AmbryCoordinator.class, "operationExceptionRate"));

    putBlobError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "putBlobError"));
    deleteBlobError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "deleteBlobError"));
    getBlobPropertiesError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "getBlobPropertiesError"));
    getBlobUserMetadataError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "getBlobUserMetadataError"));
    getBlobError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "getBlobError"));

    unexpectedInternalError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unexpectedInternalError"));
    ambryUnavailableError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "ambryUnavailableError"));
    operationTimedOutError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "operationTimedOutError"));
    invalidBlobIdError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "invalidBlobIdError"));
    invalidPutArgumentError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "invalidPutArgumentError"));
    insufficientCapacityError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "insufficientCapacityError"));
    blobTooLargeError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobTooLargeError"));
    blobAlreadyExistsInLocalColoError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobAlreadyExistsInLocalColoError"));
    blobAlreadyExistsInRemoteColoError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobAlreadyExistsInRemoteColoError"));
    blobDoesNotExistError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobDoesNotExistError"));
    blobDeletedError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobDeletedError"));
    blobExpiredError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "blobExpiredError"));
    unknownError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unknownError"));
    corruptionError = registry.counter(MetricRegistry.name(AmbryCoordinator.class, "corruptionError"));
    unknownReplicaResponseError =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "unknownReplicaResponseError"));
    successfulCrossColoProxyCallCount =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "successfulCrossColoProxyCallCount"));
    totalCrossColoProxyCallCount =
        registry.counter(MetricRegistry.name(AmbryCoordinator.class, "totalCrossColoProxyCallCount"));
    this.crossDCCallsEnabled = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return (isCrossDatacenterCallsEnabled == true ? 1 : 0);
      }
    };
    plainTextConnectionsRequestRate =
        registry.meter(MetricRegistry.name(AmbryCoordinator.class, "plainTextConnectionsRequestRate"));
    sslConnectionsRequestRate =
        registry.meter(MetricRegistry.name(AmbryCoordinator.class, "sslConnectionsRequestRate"));

    // Track metrics at DataNode granularity.
    // In the future, could track at Disk and/or Partition granularity as well/instead.
    requestMetrics = new HashMap<DataNodeId, RequestMetrics>();
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      requestMetrics.put(dataNodeId, new RequestMetrics(registry, dataNodeId));
    }

    if (!registry.getNames().contains(MetricRegistry.name(AmbryCoordinator.class, "totalRequestsInFlight"))) {
      Gauge<Integer> requestsInFlight = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return totalRequestsInFlight.get();
        }
      };
      registry.register(MetricRegistry.name(AmbryCoordinator.class, "totalRequestsInFlight"), requestsInFlight);

      Gauge<Integer> requestsInExecution = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return totalRequestsInExecution.get();
        }
      };
      registry.register(MetricRegistry.name(AmbryCoordinator.class, "totalRequestsInExecution"), requestsInExecution);
    }
  }

  public enum CoordinatorOperationType {
    PutBlob,
    DeleteBlob,
    GetBlobProperties,
    GetBlobUserMetadata,
    GetBlob
  }

  public void countError(CoordinatorOperationType operation, CoordinatorError error) {
    operationExceptionRate.mark();
    switch (error) {
      case UnexpectedInternalError:
        updateOperationMetric(operation);
        unexpectedInternalError.inc();
        break;
      case AmbryUnavailable:
        updateOperationMetric(operation);
        ambryUnavailableError.inc();
        break;
      case OperationTimedOut:
        updateOperationMetric(operation);
        operationTimedOutError.inc();
        break;
      case InvalidBlobId:
        updateOperationMetric(operation);
        invalidBlobIdError.inc();
        break;
      case InvalidPutArgument:
        updateOperationMetric(operation);
        invalidPutArgumentError.inc();
        break;
      case InsufficientCapacity:
        updateOperationMetric(operation);
        insufficientCapacityError.inc();
        break;
      case BlobTooLarge:
        updateOperationMetric(operation);
        blobTooLargeError.inc();
        break;
      case BlobDoesNotExist:
        blobDoesNotExistError.inc();
        break;
      case BlobDeleted:
        blobDeletedError.inc();
        break;
      case BlobExpired:
        blobExpiredError.inc();
        break;
      default:
        updateOperationMetric(operation);
        logger.warn("Unknown CoordinatorError being counted: " + error);
        unknownError.inc();
        break;
    }
  }

  private void updateOperationMetric(CoordinatorOperationType operation) {
    switch (operation) {
      case PutBlob:
        putBlobError.inc();
        break;
      case DeleteBlob:
        deleteBlobError.inc();
        break;
      case GetBlobProperties:
        getBlobPropertiesError.inc();
        break;
      case GetBlobUserMetadata:
        getBlobUserMetadataError.inc();
        break;
      case GetBlob:
        getBlobError.inc();
        break;
      default:
        logger.warn("Error for unknown CoordinatorOperationType being counted: " + operation);
        unknownError.inc();
        break;
    }
  }

  public RequestMetrics getRequestMetrics(DataNodeId dataNodeId) {
    if (requestMetrics.containsKey(dataNodeId)) {
      return requestMetrics.get(dataNodeId);
    } else {
      String message = "Could not find RequestMetrics for DataNode " + dataNodeId;
      logger.warn(message);
      return null;
    }
  }

  public class RequestMetrics {
    public final Histogram putBlobRequestLatencyInMs;
    public final Histogram deleteBlobRequestLatencyInMs;
    public final Histogram getBlobPropertiesRequestLatencyInMs;
    public final Histogram getBlobUserMetadataRequestLatencyInMs;
    public final Histogram getBlobRequestLatencyInMs;

    public final Histogram plainTextPutBlobRequestLatencyInMs;
    public final Histogram plainTextDeleteBlobRequestLatencyInMs;
    public final Histogram plainTextGetBlobPropertiesRequestLatencyInMs;
    public final Histogram plainTextGetBlobUserMetadataRequestLatencyInMs;
    public final Histogram plainTextGetBlobRequestLatencyInMs;

    public final Histogram sslPutBlobRequestLatencyInMs;
    public final Histogram sslDeleteBlobRequestLatencyInMs;
    public final Histogram sslGetBlobPropertiesRequestLatencyInMs;
    public final Histogram sslGetBlobUserMetadataRequestLatencyInMs;
    public final Histogram sslGetBlobRequestLatencyInMs;

    public final Meter putBlobRequestRate;
    public final Meter deleteBlobRequestRate;
    public final Meter getBlobPropertiesRequestRate;
    public final Meter getBlobUserMetadataRequestRate;
    public final Meter getBlobRequestRate;
    public final Meter requestErrorRate;

    public final Meter plainTextPutBlobRequestRate;
    public final Meter plainTextDeleteBlobRequestRate;
    public final Meter plainTextGetBlobPropertiesRequestRate;
    public final Meter plainTextGetBlobUserMetadataRequestRate;
    public final Meter plainTextGetBlobRequestRate;
    public final Meter plainTextRequestErrorRate;

    public final Meter sslPutBlobRequestRate;
    public final Meter sslDeleteBlobRequestRate;
    public final Meter sslGetBlobPropertiesRequestRate;
    public final Meter sslGetBlobUserMetadataRequestRate;
    public final Meter sslGetBlobRequestRate;
    public final Meter sslRequestErrorRate;

    private final Counter unexpectedError;
    private final Counter ioError;
    private final Counter timeoutError;
    private final Counter unknownError;
    private final Counter messageFormatDataCorruptError;
    private final Counter messageFormatHeaderConstraintError;
    private final Counter messageFormatUnknownFormatError;

    private final Counter plainTextUnexpectedError;
    private final Counter plainTextIoError;
    private final Counter plainTextTimeoutError;
    private final Counter plainTextUnknownError;
    private final Counter plainTextMessageFormatDataCorruptError;
    private final Counter plainTextMessageFormatHeaderConstraintError;
    private final Counter plainTextMessageFormatUnknownFormatError;

    private final Counter sslUnexpectedError;
    private final Counter sslIoError;
    private final Counter sslTimeoutError;
    private final Counter sslUnknownError;
    private final Counter sslMessageFormatDataCorruptError;
    private final Counter sslMessageFormatHeaderConstraintError;
    private final Counter sslMessageFormatUnknownFormatError;

    RequestMetrics(MetricRegistry registry, DataNodeId dataNodeId) {
      putBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "putBlobRequestLatencyInMs"));
      deleteBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "deleteBlobRequestLatencyInMs"));
      getBlobPropertiesRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobPropertiesRequestLatencyInMs"));
      getBlobUserMetadataRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobUserMetadataRequestLatencyInMs"));
      getBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobRequestLatencyInMs"));

      plainTextPutBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextPutBlobRequestLatencyInMs"));
      plainTextDeleteBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextDeleteBlobRequestLatencyInMs"));
      plainTextGetBlobPropertiesRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextGetBlobPropertiesRequestLatencyInMs"));
      plainTextGetBlobUserMetadataRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextGetBlobUserMetadataRequestLatencyInMs"));
      plainTextGetBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextGetBlobRequestLatencyInMs"));

      sslPutBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslPutBlobRequestLatencyInMs"));
      sslDeleteBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslDeleteBlobRequestLatencyInMs"));
      sslGetBlobPropertiesRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslGetBlobPropertiesRequestLatencyInMs"));
      sslGetBlobUserMetadataRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslGetBlobUserMetadataRequestLatencyInMs"));
      sslGetBlobRequestLatencyInMs = registry.histogram(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslGetBlobRequestLatencyInMs"));

      putBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "putBlobRequestRate"));
      deleteBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "deleteBlobRequestRate"));
      getBlobPropertiesRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobPropertiesRequestRate"));
      getBlobUserMetadataRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobUserMetadataRequestRate"));
      getBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "getBlobRequestRate"));
      requestErrorRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "requestErrorRate"));

      plainTextPutBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextPutBlobRequestRate"));
      plainTextDeleteBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextDeleteBlobRequestRate"));
      plainTextGetBlobPropertiesRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextGetBlobPropertiesRequestRate"));
      plainTextGetBlobUserMetadataRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextGetBlobUserMetadataRequestRate"));
      plainTextGetBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextGetBlobRequestRate"));
      plainTextRequestErrorRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextRequestErrorRate"));

      sslPutBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslPutBlobRequestRate"));
      sslDeleteBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslDeleteBlobRequestRate"));
      sslGetBlobPropertiesRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslGetBlobPropertiesRequestRate"));
      sslGetBlobUserMetadataRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslGetBlobUserMetadataRequestRate"));
      sslGetBlobRequestRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslGetBlobRequestRate"));
      sslRequestErrorRate = registry.meter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslRequestErrorRate"));

      unexpectedError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "unexpectedError"));
      ioError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "ioError"));
      timeoutError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "timeoutError"));
      unknownError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "unknownError"));
      messageFormatDataCorruptError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "messageFormatDataCorruptError"));
      messageFormatHeaderConstraintError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "messageFormatHeaderConstraintError"));
      messageFormatUnknownFormatError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "messageFormatUnknownFormatError"));

      plainTextUnexpectedError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextUnexpectedError"));
      plainTextIoError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextIoError"));
      plainTextTimeoutError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextTimeoutError"));
      plainTextUnknownError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextUnknownError"));
      plainTextMessageFormatDataCorruptError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextMessageFormatDataCorruptError"));
      plainTextMessageFormatHeaderConstraintError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextMessageFormatHeaderConstraintError"));
      plainTextMessageFormatUnknownFormatError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "plainTextMessageFormatUnknownFormatError"));

      sslUnexpectedError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslUnexpectedError"));
      sslIoError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslIoError"));
      sslTimeoutError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslTimeoutError"));
      sslUnknownError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslUnknownError"));
      sslMessageFormatDataCorruptError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslMessageFormatDataCorruptError"));
      sslMessageFormatHeaderConstraintError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslMessageFormatHeaderConstraintError"));
      sslMessageFormatUnknownFormatError = registry.counter(MetricRegistry
          .name(OperationRequest.class, dataNodeId.getDatacenterName(), dataNodeId.getHostname(),
              Integer.toString(dataNodeId.getPort()), "sslMessageFormatUnknownFormatError"));
    }

    public void incrementPutBlobRequestRate(boolean sslRequest) {
      putBlobRequestRate.mark();
      if (sslRequest) {
        sslPutBlobRequestRate.mark();
      } else {
        plainTextPutBlobRequestRate.mark();
      }
    }

    public void updatePutBlobRequestLatency(long latency, boolean sslRequest) {
      putBlobRequestLatencyInMs.update(latency);
      if (sslRequest) {
        sslPutBlobRequestLatencyInMs.update(latency);
      } else {
        plainTextPutBlobRequestLatencyInMs.update(latency);
      }
    }

    public void incrementGetBlobRequestRate(boolean sslRequest) {
      getBlobRequestRate.mark();
      if (sslRequest) {
        sslGetBlobRequestRate.mark();
      } else {
        plainTextGetBlobRequestRate.mark();
      }
    }

    public void updateGetBlobRequestLatency(long latency, boolean sslRequest) {
      getBlobRequestLatencyInMs.update(latency);
      if (sslRequest) {
        sslGetBlobRequestLatencyInMs.update(latency);
      } else {
        plainTextGetBlobRequestLatencyInMs.update(latency);
      }
    }

    public void incrementGetBlobPropertiesRequestRate(boolean sslRequest) {
      getBlobPropertiesRequestRate.mark();
      if (sslRequest) {
        sslGetBlobPropertiesRequestRate.mark();
      } else {
        plainTextGetBlobPropertiesRequestRate.mark();
      }
    }

    public void updateGetBlobPropertiesRequestLatency(long latency, boolean sslRequest) {
      getBlobPropertiesRequestLatencyInMs.update(latency);
      if (sslRequest) {
        sslGetBlobPropertiesRequestLatencyInMs.update(latency);
      } else {
        plainTextGetBlobPropertiesRequestLatencyInMs.update(latency);
      }
    }

    public void incrementGetBlobUserMetadataRequestRate(boolean sslRequest) {
      getBlobUserMetadataRequestRate.mark();
      if (sslRequest) {
        sslGetBlobUserMetadataRequestRate.mark();
      } else {
        plainTextGetBlobUserMetadataRequestRate.mark();
      }
    }

    public void updateGetBlobUserMetadataRequestLatency(long latency, boolean sslRequest) {
      getBlobUserMetadataRequestLatencyInMs.update(latency);
      if (sslRequest) {
        sslGetBlobUserMetadataRequestLatencyInMs.update(latency);
      } else {
        plainTextGetBlobUserMetadataRequestLatencyInMs.update(latency);
      }
    }

    public void incrementDeleteBlobRequestRate(boolean sslRequest) {
      deleteBlobRequestRate.mark();
      if (sslRequest) {
        sslDeleteBlobRequestRate.mark();
      } else {
        plainTextDeleteBlobRequestRate.mark();
      }
    }

    public void updateDeleteBlobRequestLatency(long latency, boolean sslEnabled) {
      deleteBlobRequestLatencyInMs.update(latency);
      if (sslEnabled) {
        sslDeleteBlobRequestLatencyInMs.update(latency);
      } else {
        plainTextDeleteBlobRequestLatencyInMs.update(latency);
      }
    }

    public void countError(MessageFormatErrorCodes error, boolean sslRequest) {
      requestErrorRate.mark();
      switch (error) {
        case Data_Corrupt:
          messageFormatDataCorruptError.inc();
          if (sslRequest) {
            sslMessageFormatDataCorruptError.inc();
          } else {
            plainTextMessageFormatDataCorruptError.inc();
          }
          break;
        case Header_Constraint_Error:
          messageFormatHeaderConstraintError.inc();
          if (sslRequest) {
            sslMessageFormatHeaderConstraintError.inc();
          } else {
            plainTextMessageFormatHeaderConstraintError.inc();
          }
          break;
        case Unknown_Format_Version:
          messageFormatUnknownFormatError.inc();
          if (sslRequest) {
            sslMessageFormatUnknownFormatError.inc();
          } else {
            plainTextMessageFormatUnknownFormatError.inc();
          }
          break;
        default:
          logger.warn("Unknown MessageFormatErrorCodes: " + error);
          unknownError.inc();
          if (sslRequest) {
            sslUnknownError.inc();
          } else {
            plainTextUnknownError.inc();
          }
          break;
      }
    }

    public void countError(RequestResponseError error, boolean sslRequest) {
      requestErrorRate.mark();
      switch (error) {
        case UNEXPECTED_ERROR:
          unexpectedError.inc();
          if (sslRequest) {
            sslUnexpectedError.inc();
          } else {
            plainTextUnexpectedError.inc();
          }
          break;
        case IO_ERROR:
          ioError.inc();
          if (sslRequest) {
            sslIoError.inc();
          } else {
            plainTextIoError.inc();
          }
          break;
        case TIMEOUT_ERROR:
          timeoutError.inc();
          if (sslRequest) {
            sslTimeoutError.inc();
          } else {
            plainTextTimeoutError.inc();
          }
          break;
        default:
          logger.warn("Unknown RequestResponseError: " + error);
          unknownError.inc();
          if (sslRequest) {
            sslUnknownError.inc();
          } else {
            plainTextUnknownError.inc();
          }
          break;
      }
    }
  }
}
