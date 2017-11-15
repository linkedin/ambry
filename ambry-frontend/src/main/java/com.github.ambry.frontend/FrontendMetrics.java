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
  public final RestRequestMetrics getPeersMetrics;
  public final RestRequestMetrics getPeersSSLMetrics;
  public final RestRequestMetrics getReplicasMetrics;
  public final RestRequestMetrics getReplicasSSLMetrics;
  public final RestRequestMetrics getSignedUrlMetrics;
  public final RestRequestMetrics getSignedUrlSSLMetrics;
  // POST
  public final RestRequestMetrics postBlobMetrics;
  public final RestRequestMetrics postBlobSSLMetrics;
  // OPTIONS
  public final RestRequestMetrics optionsMetrics;
  public final RestRequestMetrics optionsSSLMetrics;

  // Rates
  // AmbrySecurityService
  public final Meter securityServiceProcessRequestRate;
  public final Meter securityServicePostProcessRequestRate;
  public final Meter securityServiceProcessResponseRate;
  // AmbryIdConverter
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
  // OPTIONS
  public final Histogram optionsPreProcessingTimeInMs;
  public final Histogram optionsSecurityRequestTimeInMs;
  public final Histogram optionsSecurityResponseTimeInMs;
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
  // PostCallback
  public final Histogram outboundIdConversionCallbackProcessingTimeInMs;
  public final Histogram outboundIdConversionTimeInMs;
  public final Histogram postCallbackProcessingTimeInMs;
  public final Histogram postTimeInMs;
  public final Histogram postSecurityResponseTimeInMs;
  public final Histogram postSecurityResponseCallbackProcessingTimeInMs;
  // InboundIdConverterCallback
  public final Histogram inboundIdConversionCallbackProcessingTimeInMs;
  public final Histogram inboundIdConversionTimeInMs;
  // SecurityProcessRequestCallback
  public final Histogram deleteSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram getSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram getPeersSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram getSignedUrlSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram headSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram postSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram deleteSecurityRequestTimeInMs;
  public final Histogram getSecurityRequestTimeInMs;
  public final Histogram headSecurityRequestTimeInMs;
  public final Histogram postSecurityRequestTimeInMs;
  public final Histogram getPeersSecurityRequestTimeInMs;
  public final Histogram getSignedUrlSecurityRequestTimeInMs;
  // SecurityPostProcessRequestCallback
  public final Histogram getPeersSecurityPostProcessRequestTimeInMs;
  public final Histogram getSignedUrlSecurityPostProcessRequestTimeInMs;
  // AmbrySecurityService
  public final Histogram securityServiceProcessRequestTimeInMs;
  public final Histogram securityServicePostProcessRequestTimeInMs;
  public final Histogram securityServiceProcessResponseTimeInMs;
  // AmbryIdConverter
  public final Histogram idConverterProcessingTimeInMs;
  // GetPeersHandler
  public final Histogram getPeersProcessingTimeInMs;
  // GetReplicasHandler
  public final Histogram getReplicasProcessingTimeInMs;
  // GetSignedUrlHandler
  public final Histogram getSignedUrlProcessingTimeInMs;

  // Errors
  // AmbryBlobStorageService
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;
  public final Counter routerCallbackError;
  public final Counter ttlTooLargeError;
  // DeleteCallback
  public final Counter deleteCallbackProcessingError;
  // HeadCallback
  public final Counter headCallbackProcessingError;
  // GetCallback
  public final Counter getCallbackProcessingError;
  public final Counter getSecurityResponseCallbackProcessingError;
  // PostCallback
  public final Counter postCallbackProcessingError;
  public final Counter outboundIdConversionCallbackProcessingError;
  // GetPeersHandler
  public final Counter unknownDatanodeError;
  // GetReplicasHandler
  public final Counter invalidBlobIdError;
  public final Counter responseConstructionError;
  // Other
  // AmbryBlobStorageService
  public final Histogram blobStorageServiceStartupTimeInMs;
  public final Histogram blobStorageServiceShutdownTimeInMs;

  // Account
  public final Counter unrecognizedServiceIdCount;
  public final Counter unrecognizedAccountNameCount;
  public final Counter unrecognizedContainerNameCount;
  public final Counter getHeadDeleteUnrecognizedAccountCount;
  public final Counter getHeadDeleteUnrecognizedContainerCount;
  public final Meter putWithServiceIdForAccountNameRate;
  public final Meter putWithAccountAndContainerHeaderRate;

  /**
   * Creates an instance of FrontendMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public FrontendMetrics(MetricRegistry metricRegistry) {
    // RestRequestMetrics instances
    // DELETE
    deleteBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "DeleteBlob", metricRegistry);
    deleteBlobSSLMetrics =
        new RestRequestMetrics(AmbryBlobStorageService.class, "DeleteBlob" + SSL_SUFFIX, metricRegistry);
    // HEAD
    headBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "HeadBlob", metricRegistry);
    headBlobSSLMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "HeadBlob" + SSL_SUFFIX, metricRegistry);
    // GET
    getBlobInfoMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlobInfo", metricRegistry);
    getBlobInfoSSLMetrics =
        new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlobInfo" + SSL_SUFFIX, metricRegistry);
    getBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlob", metricRegistry);
    getBlobSSLMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlob" + SSL_SUFFIX, metricRegistry);
    getUserMetadataMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetUserMetadata", metricRegistry);
    getUserMetadataSSLMetrics =
        new RestRequestMetrics(AmbryBlobStorageService.class, "GetUserMetadata" + SSL_SUFFIX, metricRegistry);
    getPeersMetrics = new RestRequestMetrics(GetPeersHandler.class, "GetPeers", metricRegistry);
    getPeersSSLMetrics = new RestRequestMetrics(GetPeersHandler.class, "GetPeers" + SSL_SUFFIX, metricRegistry);
    getReplicasMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetReplicas", metricRegistry);
    getReplicasSSLMetrics =
        new RestRequestMetrics(AmbryBlobStorageService.class, "GetReplicas" + SSL_SUFFIX, metricRegistry);
    getSignedUrlMetrics = new RestRequestMetrics(GetSignedUrlHandler.class, "GetSignedUrl", metricRegistry);
    getSignedUrlSSLMetrics =
        new RestRequestMetrics(GetSignedUrlHandler.class, "GetSignedUrl" + SSL_SUFFIX, metricRegistry);
    // POST
    postBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "PostBlob", metricRegistry);
    postBlobSSLMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "PostBlob" + SSL_SUFFIX, metricRegistry);
    // OPTIONS
    optionsMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "Options", metricRegistry);
    optionsSSLMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "Options" + SSL_SUFFIX, metricRegistry);

    // Rates
    // AmbrySecurityService
    securityServiceProcessRequestRate =
        metricRegistry.meter(MetricRegistry.name(AmbrySecurityService.class, "ProcessRequestRate"));
    securityServicePostProcessRequestRate =
        metricRegistry.meter(MetricRegistry.name(AmbrySecurityService.class, "PostProcessRequestRate"));
    securityServiceProcessResponseRate =
        metricRegistry.meter(MetricRegistry.name(AmbrySecurityService.class, "ProcessResponseRate"));
    // AmbryIdConverter
    idConverterRequestRate = metricRegistry.meter(MetricRegistry.name(AmbryIdConverterFactory.class, "RequestRate"));

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
    // OPTIONS
    optionsPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "OptionsPreProcessingTimeInMs"));
    optionsSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "OptionsSecurityRequestTimeInMs"));
    optionsSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "OptionsSecurityResponseTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "DeleteCallbackProcessingTimeInMs"));
    deleteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteCallbackResultTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadCallbackProcessingTimeInMs"));
    headTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadCallbackResultTimeInMs"));
    headSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityResponseCallbackProcessingTimeInMs"));
    headSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityResponseTimeInMs"));
    // GetCallback
    getCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetCallbackProcessingTimeInMs"));
    getTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetCallbackResultTimeInMs"));
    getSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityResponseCallbackProcessingTimeInMs"));
    getSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityResponseTimeInMs"));
    // PostCallback
    outboundIdConversionCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "OutboundIdCallbackProcessingTimeInMs"));
    outboundIdConversionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "OutboundIdConversionTimeInMs"));
    postCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostCallbackProcessingTimeInMs"));
    postTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostCallbackResultTimeInMs"));
    postSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostSecurityResponseTimeInMs"));
    postSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "PostSecurityResponseCallbackProcessingTimeInMs"));
    // InboundIdConverterCallback
    inboundIdConversionCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "InboundIdCallbackProcessingTimeInMs"));
    inboundIdConversionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "InboundIdConversionTimeInMs"));
    // SecurityProcessRequestCallback
    deleteSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "DeleteSecurityRequestCallbackProcessingTimeInMs"));
    deleteSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteSecurityRequestTimeInMs"));
    headSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityRequestCallbackProcessingTimeInMs"));
    headSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadSecurityRequestTimeInMs"));
    getSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityRequestCallbackProcessingTimeInMs"));
    getPeersSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(GetPeersHandler.class, "SecurityRequestCallbackProcessingTimeInMs"));
    getSignedUrlSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(GetSignedUrlHandler.class, "SecurityRequestCallbackProcessingTimeInMs"));
    getSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityRequestTimeInMs"));
    postSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AmbryBlobStorageService.class, "PostSecurityRequestCallbackProcessingTimeInMs"));
    postSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostSecurityRequestTimeInMs"));
    getPeersSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetPeersHandler.class, "SecurityRequestTimeInMs"));
    getSignedUrlSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetSignedUrlHandler.class, "SecurityRequestTimeInMs"));
    // SecurityPostProcessRequestCallback
    getPeersSecurityPostProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetPeersHandler.class, "SecurityPostProcessRequestTimeInMs"));
    getSignedUrlSecurityPostProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetSignedUrlHandler.class, "SecurityPostProcessRequestTimeInMs"));
    // AmbrySecurityService
    securityServiceProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbrySecurityService.class, "RequestProcessingTimeInMs"));
    securityServicePostProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbrySecurityService.class, "RequestPostProcessingTimeInMs"));
    securityServiceProcessResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbrySecurityService.class, "ResponseProcessingTimeInMs"));
    // AmbryIdConverter
    idConverterProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryIdConverterFactory.class, "ProcessingTimeInMs"));
    // GetPeersHandler
    getPeersProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetPeersHandler.class, "ProcessingTimeInMs"));
    // GetReplicasHandler
    getReplicasProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetReplicasHandler.class, "ProcessingTimeInMs"));
    // GetSignedUrlHandler
    getSignedUrlProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetSignedUrlHandler.class, "ProcessingTimeInMs"));

    // Errors
    // AmbryBlobStorageService
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResourceReleaseError"));
    routerCallbackError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "RouterCallbackError"));
    // DeleteCallback
    deleteCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteCallbackProcessingError"));
    // HeadCallback
    headCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "HeadCallbackProcessingError"));
    // GetCallback
    getCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "GetCallbackProcessingError"));
    getSecurityResponseCallbackProcessingError = metricRegistry.counter(
        MetricRegistry.name(AmbryBlobStorageService.class, "GetSecurityResponseCallbackProcessingError"));
    // PostCallback
    postCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "PostCallbackProcessingError"));
    outboundIdConversionCallbackProcessingError = metricRegistry.counter(
        MetricRegistry.name(AmbryBlobStorageService.class, "OutboundIdConversionCallbackProcessingError"));
    ttlTooLargeError = metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "TtlTooLargeError"));
    // GetPeersHandler
    unknownDatanodeError = metricRegistry.counter(MetricRegistry.name(GetPeersHandler.class, "UnknownDatanodeError"));
    // GetReplicasHandler
    invalidBlobIdError = metricRegistry.counter(MetricRegistry.name(GetReplicasHandler.class, "InvalidBlobIdError"));
    responseConstructionError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasHandler.class, "ResponseConstructionError"));

    // Other
    blobStorageServiceStartupTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "StartupTimeInMs"));
    blobStorageServiceShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "ShutdownTimeInMs"));

    // Account and container
    unrecognizedServiceIdCount =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "UnrecognizedServiceIdCount"));
    unrecognizedAccountNameCount =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "UnrecognizedAccountNameCount"));
    unrecognizedContainerNameCount =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "UnrecognizedContainerNameCount"));
    getHeadDeleteUnrecognizedAccountCount = metricRegistry.counter(
        MetricRegistry.name(AmbryBlobStorageService.class, "GetHeadDeleteUnrecognizedAccountCount"));
    getHeadDeleteUnrecognizedContainerCount = metricRegistry.counter(
        MetricRegistry.name(AmbryBlobStorageService.class, "GetHeadDeleteUnrecognizedContainerCount"));
    putWithServiceIdForAccountNameRate =
        metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "PutWithServiceIdForAccountNameRate"));
    putWithAccountAndContainerHeaderRate = metricRegistry.meter(
        MetricRegistry.name(AmbryBlobStorageService.class, "PutWithAccountAndContainerHeaderRate"));
  }
}
