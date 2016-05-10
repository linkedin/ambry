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
package com.github.ambry.admin;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.RestRequestMetrics;


/**
 * Admin specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the admin to the provided {@link MetricRegistry}.
 */
class AdminMetrics {

  // RestRequestMetrics instances
  // DELETE
  public final RestRequestMetrics deleteBlobMetrics;
  // HEAD
  public final RestRequestMetrics headBlobMetrics;
  // GET
  public final RestRequestMetrics echoMetrics;
  public final RestRequestMetrics getBlobMetrics;
  public final RestRequestMetrics getReplicasForBlobIdMetrics;
  // POST
  public final RestRequestMetrics postBlobMetrics;

  // Rates
  // AdminBlobStorageService
  // DELETE
  public final Meter deleteBlobRate;
  // HEAD
  public final Meter headBlobRate;
  // GET
  public final Meter echoRate;
  public final Meter getBlobRate;
  public final Meter getReplicasForBlobIdRate;
  // POST
  public final Meter postBlobRate;

  // Latencies
  // AdminBlobStorageService
  // DELETE
  public final Histogram deletePreProcessingTimeInMs;
  // HEAD
  public final Histogram headPreProcessingTimeInMs;
  // GET
  public final Histogram getPreProcessingTimeInMs;
  // POST
  public final Histogram blobPropsBuildTimeInMs;
  public final Histogram postPreProcessingTimeInMs;
  // DeleteCallback
  public final Histogram deleteCallbackProcessingTimeInMs;
  public final Histogram deleteTimeInMs;
  // EchoHandler
  public final Histogram echoProcessingTimeInMs;
  // HeadCallback
  public final Histogram headCallbackProcessingTimeInMs;
  public final Histogram headTimeInMs;
  // HeadForGetCallback
  public final Histogram headForGetCallbackProcessingTimeInMs;
  public final Histogram headForGetTimeInMs;
  // GetCallback
  public final Histogram getCallbackProcessingTimeInMs;
  public final Histogram getTimeInMs;
  // GetReplicasForBlobIdHandler
  public final Histogram getReplicasForBlobIdProcessingTimeInMs;
  // PostCallback
  public final Histogram postCallbackProcessingTimeInMs;
  public final Histogram postTimeInMs;

  // Errors
  // AdminBlobStorageService
  public final Counter callbackProcessingError;
  public final Counter operationError;
  public final Counter missingOperationHandlerError;
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;
  // EchoHandler
  public final Counter echoGetMissingParameterError;
  public final Counter echoGetResponseBuildingError;
  // GetReplicasForBlobIdHandler
  public final Counter getReplicasForBlobIdInvalidBlobIdError;
  public final Counter getReplicasForBlobIdObjectCreationError;
  public final Counter getReplicasForBlobIdMissingParameterError;
  public final Counter getReplicasForBlobIdPartitionNullError;
  public final Counter getReplicasForBlobIdResponseBuildingError;

  /**
   * Creates an instance of AdminMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public AdminMetrics(MetricRegistry metricRegistry) {
    // RestRequestMetrics instances
    // DELETE
    deleteBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "DeleteBlob", metricRegistry);
    // HEAD
    headBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "HeadBlob", metricRegistry);
    // GET
    echoMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "Echo", metricRegistry);
    getBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "GetBlob", metricRegistry);
    getReplicasForBlobIdMetrics =
        new RestRequestMetrics(AdminBlobStorageService.class, "GetReplicasForBlobId", metricRegistry);
    // POST
    postBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "PostBlob", metricRegistry);

    // Rates
    // AdminBlobStorageService
    // DELETE
    deleteBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "DeleteBlobRate"));
    // HEAD
    headBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "HeadBlobRate"));
    // GET
    echoRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "EchoRate"));
    getBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "GetBlobRate"));
    getReplicasForBlobIdRate =
        metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "GetReplicasForBlobIdRate"));
    // POST
    postBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "PostBlobRate"));

    // Latencies
    // AdminBlobStorageService
    // DELETE
    deletePreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "DeletePreProcessingTimeInMs"));
    // HEAD
    headPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "HeadPreProcessingTimeInMs"));
    // GET
    getPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "GetPreProcessingTimeInMs"));
    // POST
    blobPropsBuildTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "BlobPropsBuildTimeInMs"));
    postPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "PostPreProcessingTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ProcessingTimeInMs"));
    deleteTimeInMs = metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ResultTimeInMs"));
    // EchoHandler
    echoProcessingTimeInMs = metricRegistry.histogram(MetricRegistry.name(EchoHandler.class, "ProcessingTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HeadCallback.class, "ProcessingTimeInMs"));
    headTimeInMs = metricRegistry.histogram(MetricRegistry.name(HeadCallback.class, "ResultTimeInMs"));
    // HeadForGetCallback
    headForGetCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HeadForGetCallback.class, "ProcessingTimeInMs"));
    headForGetTimeInMs = metricRegistry.histogram(MetricRegistry.name(HeadForGetCallback.class, "ResultTimeInMs"));
    // GetCallback
    getCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetCallback.class, "ProcessingTimeInMs"));
    getTimeInMs = metricRegistry.histogram(MetricRegistry.name(GetCallback.class, "ResultTimeInMs"));
    // GetReplicasForBlobIdHandler
    getReplicasForBlobIdProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "ProcessingTimeInMs"));
    // PostCallback
    postCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(PostCallback.class, "ProcessingTimeInMs"));
    postTimeInMs = metricRegistry.histogram(MetricRegistry.name(PostCallback.class, "ResultTimeInMs"));

    // Errors
    // AdminBlobStorageService
    callbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "CallbackProcessingError"));
    operationError = metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "OperationError"));
    missingOperationHandlerError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "MissingOperationHandlerError"));
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "ResourceReleaseError"));
    // EchoHandler
    echoGetMissingParameterError =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "MissingParameterError"));
    echoGetResponseBuildingError =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "ResponseBuildingError"));
    // GetReplicasForBlobIdHandler
    getReplicasForBlobIdInvalidBlobIdError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "InvalidBlobIdError"));
    getReplicasForBlobIdObjectCreationError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "ObjectCreationError"));
    getReplicasForBlobIdMissingParameterError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "MissingParameterError"));
    getReplicasForBlobIdPartitionNullError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "PartitionNullError"));
    getReplicasForBlobIdResponseBuildingError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "ResponseBuildingError"));
  }
}
