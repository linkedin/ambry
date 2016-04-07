/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.RestRequestMetrics;


/**
 * Ambry frontend specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the Ambry frontend to the provided {@link MetricRegistry}.
 */
class FrontendMetrics {

  // RestRequestMetrics instances
  // DELETE
  public final RestRequestMetrics deleteBlobMetrics;
  // HEAD
  public final RestRequestMetrics headBlobMetrics;
  // GET
  public final RestRequestMetrics getBlobInfoMetrics;
  public final RestRequestMetrics getBlobMetrics;
  public final RestRequestMetrics getUserMetadataMetrics;
  // POST
  public final RestRequestMetrics postBlobMetrics;

  // Rates
  // AmbryBlobStorageService
  // DELETE
  public final Meter deleteBlobRate;
  // HEAD
  public final Meter headBlobRate;
  // GET
  public final Meter getBlobRate;
  // POST
  public final Meter postBlobRate;
  // security service
  public final Meter securityServiceProcessRequestRate;
  public final Meter securityServiceProcessResponseRate;
  // Id converter
  public final Meter idConverterRequestRate;

  // Latencies
  // AmbryBlobStorageService
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
  // HeadCallback
  public final Histogram headCallbackProcessingTimeInMs;
  public final Histogram headTimeInMs;
  // HeadForGetCallback
  public final Histogram headForGetCallbackProcessingTimeInMs;
  public final Histogram headForGetTimeInMs;
  // GetCallback
  public final Histogram getCallbackProcessingTimeInMs;
  public final Histogram getTimeInMs;
  // PostCallback
  public final Histogram postCallbackProcessingTimeInMs;
  public final Histogram postTimeInMs;
  // security service
  public final Histogram securityServiceProcessRequestTimeInMs;
  public final Histogram securityServiceProcessResponseTimeInMs;
  // Id converter
  public final Histogram idConverterRequestProcessingTimeInMs;

  // Errors
  // AmbryBlobStorageService
  public final Counter callbackProcessingError;
  public final Counter operationError;
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;

  /**
   * Creates an instance of FrontendMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public FrontendMetrics(MetricRegistry metricRegistry) {
    // RestRequestMetrics instances
    // DELETE
    deleteBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "DeleteBlob", metricRegistry);
    // HEAD
    headBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "HeadBlob", metricRegistry);
    // GET
    getBlobInfoMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlobInfo", metricRegistry);
    getBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlob", metricRegistry);
    getUserMetadataMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetUserMetadata", metricRegistry);
    // POST
    postBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "PostBlob", metricRegistry);

    // Rates
    // AmbryBlobStorageService
    // DELETE
    deleteBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteBlobRate"));
    // HEAD
    headBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "HeadBlobRate"));
    // GET
    getBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "GetBlobRate"));
    // POST
    postBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "PostBlobRate"));
    // security service
    securityServiceProcessRequestRate =
        metricRegistry.meter(MetricRegistry.name(AmbrySecurityService.class, "SecurityServiceProcessRequestRate"));
    securityServiceProcessResponseRate =
        metricRegistry.meter(MetricRegistry.name(AmbrySecurityService.class, "SecurityServiceProcessResponseRate"));
    // Id converter
    idConverterRequestRate =
        metricRegistry.meter(MetricRegistry.name(AmbryIdConverterFactory.class, "IdConverterRequestRate"));

    // Latencies
    // AmbryBlobStorageService
    // DELETE
    deletePreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeletePreProcessingTimeInMs"));
    // HEAD
    headPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadPreProcessingTimeInMs"));
    // GET
    getPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetPreProcessingTimeInMs"));
    // POST
    blobPropsBuildTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "BlobPropsBuildTimeInMs"));
    postPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostPreProcessingTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ProcessingTimeInMs"));
    deleteTimeInMs = metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ResultTimeInMs"));
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
    // PostCallback
    postCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(PostCallback.class, "ProcessingTimeInMs"));
    postTimeInMs = metricRegistry.histogram(MetricRegistry.name(PostCallback.class, "ResultTimeInMs"));
    // security service
    securityServiceProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbrySecurityService.class, "ProcessingTimeInMs"));
    securityServiceProcessResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbrySecurityService.class, "ProcessingTimeInMs"));
    // Id converter
    idConverterRequestProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryIdConverterFactory.class, "ProcessingTimeInMs"));

    // Errors
    // AmbryBlobStorageService
    callbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "CallbackProcessingError"));
    operationError = metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "OperationError"));
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResourceReleaseError"));
  }
}
