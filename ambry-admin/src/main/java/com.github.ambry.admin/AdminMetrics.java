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
 * Exports metrics that are triggered by the Admin to the provided {@link MetricRegistry}.
 */
class AdminMetrics {
  private static final String SSL_SUFFIX = "Ssl";

  // RestRequestMetrics instances
  // DELETE
  public final RestRequestMetrics deleteBlobMetrics;
  public final RestRequestMetrics deleteBlobSSLMetrics;
  // HEAD
  public final RestRequestMetrics headBlobMetrics;
  public final RestRequestMetrics headBlobSSLMetrics;
  // GET
  public final RestRequestMetrics getBlobInfoMetrics;
  public final RestRequestMetrics getBlobInfoSSLMetrics;
  public final RestRequestMetrics getBlobMetrics;
  public final RestRequestMetrics getBlobSSLMetrics;
  public final RestRequestMetrics getUserMetadataMetrics;
  public final RestRequestMetrics getUserMetadataSSLMetrics;
  public final RestRequestMetrics getReplicasMetrics;
  public final RestRequestMetrics getReplicasSSLMetrics;
  // POST
  public final RestRequestMetrics postBlobMetrics;
  public final RestRequestMetrics postBlobSSLMetrics;

  // Rates
  // AdminSecurityService
  public final Meter securityServiceProcessRequestRate;
  public final Meter securityServiceProcessResponseRate;
  // AdminIdConverter
  public final Meter idConverterRequestRate;

  // Latencies
  // AdminBlobStorageService
  // DELETE
  public final Histogram deletePreProcessingTimeInMs;
  // HEAD
  public final Histogram headPreProcessingTimeInMs;
  // GET
  public final Histogram getPreProcessingTimeInMs;
  // DeleteCallback
  public final Histogram deleteCallbackProcessingTimeInMs;
  public final Histogram deleteTimeInMs;
  // HeadCallback
  public final Histogram headCallbackProcessingTimeInMs;
  public final Histogram headTimeInMs;
  public final Histogram headSecurityResponseTimeInMs;
  public final Histogram headSecurityResponseCallbackProcessingTimeInMs;
  // GetCallback
  public final Histogram getCallbackProcessingTimeInMs;
  public final Histogram getTimeInMs;
  public final Histogram getSecurityResponseCallbackProcessingTimeInMs;
  public final Histogram getSecurityResponseTimeInMs;
  // InboundIdConverterCallback
  public final Histogram inboundIdConversionCallbackProcessingTimeInMs;
  public final Histogram inboundIdConversionTimeInMs;
  // SecurityProcessRequestCallback
  public final Histogram deleteSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram getSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram headSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram deleteSecurityRequestTimeInMs;
  public final Histogram getSecurityRequestTimeInMs;
  public final Histogram headSecurityRequestTimeInMs;
  // AdminSecurityService
  public final Histogram securityServiceProcessRequestTimeInMs;
  public final Histogram securityServiceProcessResponseTimeInMs;
  // AdminIdConverter
  public final Histogram idConverterProcessingTimeInMs;
  // GetReplicasHandler
  public final Histogram getReplicasProcessingTimeInMs;

  // Errors
  // AdminBlobStorageService
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;
  public final Counter routerCallbackError;
  // DeleteCallback
  public final Counter deleteCallbackProcessingError;
  // HeadCallback
  public final Counter headCallbackProcessingError;
  // GetCallback
  public final Counter getCallbackProcessingError;
  public final Counter getSecurityResponseCallbackProcessingError;
  // GetReplicasHandler
  public final Counter invalidBlobIdError;
  public final Counter responseConstructionError;

  // Other
  // AdminBlobStorageService
  public final Histogram blobStorageServiceStartupTimeInMs;
  public final Histogram blobStorageServiceShutdownTimeInMs;

  /**
   * Creates an instance of AdminMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public AdminMetrics(MetricRegistry metricRegistry) {
    // RestRequestMetrics instances
    // DELETE
    deleteBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "DeleteBlob", metricRegistry);
    deleteBlobSSLMetrics =
        new RestRequestMetrics(AdminBlobStorageService.class, "DeleteBlob" + SSL_SUFFIX, metricRegistry);
    // HEAD
    headBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "HeadBlob", metricRegistry);
    headBlobSSLMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "HeadBlob" + SSL_SUFFIX, metricRegistry);
    // GET
    getBlobInfoMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "GetBlobInfo", metricRegistry);
    getBlobInfoSSLMetrics =
        new RestRequestMetrics(AdminBlobStorageService.class, "GetBlobInfo" + SSL_SUFFIX, metricRegistry);
    getBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "GetBlob", metricRegistry);
    getBlobSSLMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "GetBlob" + SSL_SUFFIX, metricRegistry);
    getUserMetadataMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "GetUserMetadata", metricRegistry);
    getUserMetadataSSLMetrics =
        new RestRequestMetrics(AdminBlobStorageService.class, "GetUserMetadata" + SSL_SUFFIX, metricRegistry);
    getReplicasMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "GetReplicas", metricRegistry);
    getReplicasSSLMetrics =
        new RestRequestMetrics(AdminBlobStorageService.class, "GetReplicas" + SSL_SUFFIX, metricRegistry);
    // POST
    postBlobMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "PostBlob", metricRegistry);
    postBlobSSLMetrics = new RestRequestMetrics(AdminBlobStorageService.class, "PostBlob" + SSL_SUFFIX, metricRegistry);

    // Rates
    // AdminSecurityService
    securityServiceProcessRequestRate =
        metricRegistry.meter(MetricRegistry.name(AdminSecurityService.class, "ProcessRequestRate"));
    securityServiceProcessResponseRate =
        metricRegistry.meter(MetricRegistry.name(AdminSecurityService.class, "ProcessResponseRate"));
    // AdminIdConverter
    idConverterRequestRate = metricRegistry.meter(MetricRegistry.name(AdminIdConverterFactory.class, "RequestRate"));

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
    // DeleteCallback
    deleteCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AdminBlobStorageService.class, "DeleteCallbackProcessingTimeInMs"));
    deleteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "DeleteCallbackResultTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "HeadCallbackProcessingTimeInMs"));
    headTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "HeadCallbackResultTimeInMs"));
    headSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AdminBlobStorageService.class, "HeadSecurityResponseCallbackProcessingTimeInMs"));
    headSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "HeadSecurityResponseTimeInMs"));
    // GetCallback
    getCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "GetCallbackProcessingTimeInMs"));
    getTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "GetCallbackResultTimeInMs"));
    getSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AdminBlobStorageService.class, "GetSecurityResponseCallbackProcessingTimeInMs"));
    getSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "GetSecurityResponseTimeInMs"));
    // InboundIdConverterCallback
    inboundIdConversionCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AdminBlobStorageService.class, "InboundIdCallbackProcessingTimeInMs"));
    inboundIdConversionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "InboundIdConversionTimeInMs"));
    // SecurityProcessRequestCallback
    deleteSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AdminBlobStorageService.class, "DeleteSecurityRequestCallbackProcessingTimeInMs"));
    deleteSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "DeleteSecurityRequestTimeInMs"));
    headSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AdminBlobStorageService.class, "HeadSecurityRequestCallbackProcessingTimeInMs"));
    headSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "HeadSecurityRequestTimeInMs"));
    getSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AdminBlobStorageService.class, "GetSecurityRequestCallbackProcessingTimeInMs"));
    getSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "GetSecurityRequestTimeInMs"));
    // AdminSecurityService
    securityServiceProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminSecurityService.class, "RequestProcessingTimeInMs"));
    securityServiceProcessResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminSecurityService.class, "ResponseProcessingTimeInMs"));
    // AdminIdConverter
    idConverterProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminIdConverterFactory.class, "ProcessingTimeInMs"));
    // GetReplicasHandler
    getReplicasProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetReplicasHandler.class, "ProcessingTimeInMs"));

    // Errors
    // AdminBlobStorageService
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "ResourceReleaseError"));
    routerCallbackError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "RouterCallbackError"));
    // DeleteCallback
    deleteCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "DeleteCallbackProcessingError"));
    // HeadCallback
    headCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "HeadCallbackProcessingError"));
    // GetCallback
    getCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "GetCallbackProcessingError"));
    getSecurityResponseCallbackProcessingError = metricRegistry.counter(
        MetricRegistry.name(AdminBlobStorageService.class, "GetSecurityResponseCallbackProcessingError"));
    // GetReplicasHandler
    invalidBlobIdError = metricRegistry.counter(MetricRegistry.name(GetReplicasHandler.class, "InvalidBlobIdError"));
    responseConstructionError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasHandler.class, "ResponseConstructionError"));

    // Other
    blobStorageServiceStartupTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "StartupTimeInMs"));
    blobStorageServiceShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "ShutdownTimeInMs"));
  }
}
