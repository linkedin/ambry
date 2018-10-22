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
import com.github.ambry.utils.AsyncOperationTracker;


/**
 * Ambry frontend specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the Ambry frontend to the provided {@link MetricRegistry}.
 */
public class FrontendMetrics {
  private static final String BLOB = "Blob";
  private static final String BLOB_INFO = "BlobInfo";
  private static final String USER_METADATA = "UserMetadata";
  private static final String SSL_SUFFIX = "Ssl";
  private static final String ENCRYPTED = "Encrypted";
  private final MetricRegistry metricRegistry;

  // RestRequestMetrics instances
  // DELETE
  public final RestRequestMetrics deleteBlobMetrics;
  public final RestRequestMetrics deleteBlobSSLMetrics;
  // GET
  public final RestRequestMetrics getPeersMetrics;
  public final RestRequestMetrics getPeersSSLMetrics;
  public final RestRequestMetrics getReplicasMetrics;
  public final RestRequestMetrics getReplicasSSLMetrics;
  public final RestRequestMetrics getSignedUrlMetrics;
  public final RestRequestMetrics getSignedUrlSSLMetrics;
  // OPTIONS
  public final RestRequestMetrics optionsMetrics;
  public final RestRequestMetrics optionsSSLMetrics;
  // PUT
  public final RestRequestMetrics updateBlobTtlMetrics;
  public final RestRequestMetrics updateBlobTtlSSLMetrics;

  // RestRequestMetricsGroup
  public final RestRequestMetricsGroup postRequestMetricsGroup;
  public final RestRequestMetricsGroup getBlobRequestMetricsGroup;
  public final RestRequestMetricsGroup getBlobInfoRequestMetricsGroup;
  public final RestRequestMetricsGroup getUserMetadataRequestMetricsGroup;
  public final RestRequestMetricsGroup headRequestMetricsGroup;

  // AsyncOperationTracker.Metrics instances
  public final AsyncOperationTracker.Metrics postSecurityPreProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postReadStitchRequestMetrics;
  public final AsyncOperationTracker.Metrics postRouterStitchBlobMetrics;
  public final AsyncOperationTracker.Metrics postRouterPutBlobMetrics;
  public final AsyncOperationTracker.Metrics postIdConversionMetrics;
  public final AsyncOperationTracker.Metrics postSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics updateBlobTtlSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics updateBlobTtlSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics updateBlobTtlRouterMetrics;
  public final AsyncOperationTracker.Metrics updateBlobTtlIdConversionMetrics;
  public final AsyncOperationTracker.Metrics updateBlobTtlSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics getSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics headSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics deleteSecurityPostProcessRequestMetrics;
  // Rates
  // AmbrySecurityService
  public final Meter securityServicePreProcessRequestRate;
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
  // PUT
  public final Histogram putPreProcessingTimeInMs;
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
  public final Histogram getPeersSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram getSignedUrlSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram headSecurityRequestCallbackProcessingTimeInMs;
  public final Histogram deleteSecurityRequestTimeInMs;
  public final Histogram getSecurityRequestTimeInMs;
  public final Histogram headSecurityRequestTimeInMs;
  public final Histogram getPeersSecurityRequestTimeInMs;
  public final Histogram getSignedUrlSecurityRequestTimeInMs;
  // SecurityPostProcessRequestCallback
  public final Histogram getPeersSecurityPostProcessRequestTimeInMs;
  public final Histogram getSignedUrlSecurityPostProcessRequestTimeInMs;
  // SecurityProcessResponseCallback
  public final Histogram getSignedUrlSecurityResponseTimeInMs;
  // AmbrySecurityService
  public final Histogram securityServicePreProcessRequestTimeInMs;
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
  public final Histogram getSignedUrlIdConverterCallbackProcessingTimeInMs;

  // Errors
  // AmbryBlobStorageService
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;
  public final Counter ttlTooLargeError;
  public final Counter ttlNotCompliantError;
  // DeleteCallback
  public final Counter deleteCallbackProcessingError;
  // HeadCallback
  public final Counter headCallbackProcessingError;
  // GetCallback
  public final Counter getCallbackProcessingError;
  public final Counter getSecurityResponseCallbackProcessingError;
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
    this.metricRegistry = metricRegistry;
    // RestRequestMetrics instances
    // DELETE
    deleteBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "DeleteBlob", metricRegistry);
    deleteBlobSSLMetrics =
        new RestRequestMetrics(AmbryBlobStorageService.class, "DeleteBlob" + SSL_SUFFIX, metricRegistry);
    // GET
    getPeersMetrics = new RestRequestMetrics(GetPeersHandler.class, "GetPeers", metricRegistry);
    getPeersSSLMetrics = new RestRequestMetrics(GetPeersHandler.class, "GetPeers" + SSL_SUFFIX, metricRegistry);
    getReplicasMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetReplicas", metricRegistry);
    getReplicasSSLMetrics =
        new RestRequestMetrics(AmbryBlobStorageService.class, "GetReplicas" + SSL_SUFFIX, metricRegistry);
    getSignedUrlMetrics = new RestRequestMetrics(GetSignedUrlHandler.class, "GetSignedUrl", metricRegistry);
    getSignedUrlSSLMetrics =
        new RestRequestMetrics(GetSignedUrlHandler.class, "GetSignedUrl" + SSL_SUFFIX, metricRegistry);
    // OPTIONS
    optionsMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "Options", metricRegistry);
    optionsSSLMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "Options" + SSL_SUFFIX, metricRegistry);
    // PUT
    updateBlobTtlMetrics = new RestRequestMetrics(TtlUpdateHandler.class, "UpdateBlobTtl", metricRegistry);
    updateBlobTtlSSLMetrics =
        new RestRequestMetrics(TtlUpdateHandler.class, "UpdateBlobTtl" + SSL_SUFFIX, metricRegistry);

    // RestRequestMetricsGroup
    postRequestMetricsGroup = new RestRequestMetricsGroup("Post", BLOB);
    getBlobInfoRequestMetricsGroup = new RestRequestMetricsGroup("Get", BLOB_INFO);
    getBlobRequestMetricsGroup = new RestRequestMetricsGroup("Get", BLOB);
    getUserMetadataRequestMetricsGroup = new RestRequestMetricsGroup("Get", USER_METADATA);
    headRequestMetricsGroup = new RestRequestMetricsGroup("Head", BLOB);

    // AsyncOperationTracker.Metrics instances
    postSecurityPreProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postSecurityPreProcessRequest", metricRegistry);
    postSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postSecurityProcessRequest", metricRegistry);
    postSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postSecurityPostProcessRequest", metricRegistry);
    postReadStitchRequestMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postReadStitchRequest", metricRegistry);
    postRouterStitchBlobMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postRouterStitchBlob", metricRegistry);
    postRouterPutBlobMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postRouterPutBlob", metricRegistry);
    postIdConversionMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postIdConversion", metricRegistry);
    postSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(PostBlobHandler.class, "postSecurityProcessResponse", metricRegistry);

    updateBlobTtlSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(TtlUpdateHandler.class, "securityProcessRequest", metricRegistry);
    updateBlobTtlSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(TtlUpdateHandler.class, "securityPostProcessRequest", metricRegistry);
    updateBlobTtlRouterMetrics = new AsyncOperationTracker.Metrics(TtlUpdateHandler.class, "router", metricRegistry);
    updateBlobTtlIdConversionMetrics =
        new AsyncOperationTracker.Metrics(TtlUpdateHandler.class, "idConversion", metricRegistry);
    updateBlobTtlSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(TtlUpdateHandler.class, "securityProcessResponse", metricRegistry);

    getSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(AmbryBlobStorageService.class, "getSecurityPostProcessRequest",
            metricRegistry);
    headSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(AmbryBlobStorageService.class, "headSecurityPostProcessRequest",
            metricRegistry);
    deleteSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(AmbryBlobStorageService.class, "deleteSecurityPostProcessRequest",
            metricRegistry);

    // Rates
    // AmbrySecurityService
    securityServicePreProcessRequestRate =
        metricRegistry.meter(MetricRegistry.name(AmbrySecurityService.class, "PreProcessRequestRate"));
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
    // PUT
    putPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PutPreProcessingTimeInMs"));
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
    getPeersSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetPeersHandler.class, "SecurityRequestTimeInMs"));
    getSignedUrlSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetSignedUrlHandler.class, "SecurityRequestTimeInMs"));
    // SecurityPostProcessRequestCallback
    getPeersSecurityPostProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetPeersHandler.class, "SecurityPostProcessRequestTimeInMs"));
    getSignedUrlSecurityPostProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetSignedUrlHandler.class, "SecurityPostProcessRequestTimeInMs"));
    // SecurityPostProcessResponseCallback
    getSignedUrlSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetSignedUrlHandler.class, "SecurityResponseTimeInMs"));
    // AmbrySecurityService
    securityServicePreProcessRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbrySecurityService.class, "RequestPreProcessingTimeInMs"));
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
    getSignedUrlIdConverterCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetSignedUrlHandler.class, "IdConverterProcessingTimeInMs"));

    // Errors
    // AmbryBlobStorageService
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResourceReleaseError"));
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
    // PostBlobHandler
    ttlTooLargeError = metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "TtlTooLargeError"));
    ttlNotCompliantError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "TtlNotCompliantError"));
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

  /**
   * Class to hold different {@link RestRequestMetrics} for plain text, ssl, with and without encryption
   */
  class RestRequestMetricsGroup {
    private final RestRequestMetrics blobMetrics;
    private final RestRequestMetrics blobSslMetrics;
    private final RestRequestMetrics blobEncryptedMetrics;
    private final RestRequestMetrics blobSslEncryptedMetrics;

    /**
     * Instantiates {@link RestRequestMetricsGroup} for the given requestType and resource
     * @param requestType refers to the type of request (Head/Get/Post)
     * @param resource refers to the resource type (Blob/BlobInfo/UserMetadata)
     */
    RestRequestMetricsGroup(String requestType, String resource) {
      blobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, requestType + resource, metricRegistry);
      blobSslMetrics =
          new RestRequestMetrics(AmbryBlobStorageService.class, requestType + resource + SSL_SUFFIX, metricRegistry);
      blobEncryptedMetrics =
          new RestRequestMetrics(AmbryBlobStorageService.class, requestType + resource + ENCRYPTED, metricRegistry);
      blobSslEncryptedMetrics =
          new RestRequestMetrics(AmbryBlobStorageService.class, requestType + resource + SSL_SUFFIX + ENCRYPTED,
              metricRegistry);
    }

    /**
     * Fetches the appropriate {@link RestRequestMetrics} based on the params
     * @param sslUsed {@code true} if the request is sent over ssl. {@code false} otherwise
     * @param encrypted {@code true} if the blob is encrypted. {@code false} otherwise
     * @return the appropriate {@link RestRequestMetrics} based on the params
     */
    RestRequestMetrics getRestRequestMetrics(boolean sslUsed, boolean encrypted) {
      RestRequestMetrics toReturn;
      if (sslUsed && encrypted) {
        toReturn = blobSslEncryptedMetrics;
      } else if (sslUsed) {
        toReturn = blobSslMetrics;
      } else if (encrypted) {
        toReturn = blobEncryptedMetrics;
      } else {
        toReturn = blobMetrics;
      }
      return toReturn;
    }
  }
}
