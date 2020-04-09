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
import com.github.ambry.utils.AsyncOperationTracker;


/**
 * Ambry frontend specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the Ambry frontend to the provided {@link MetricRegistry}.
 */
public class FrontendMetrics {

  // RestRequestMetricsGroup
  // DELETE
  public final RestRequestMetricsGroup deleteBlobMetricsGroup;
  // GET
  public final RestRequestMetricsGroup getBlobMetricsGroup;
  public final RestRequestMetricsGroup getBlobInfoMetricsGroup;
  public final RestRequestMetricsGroup getUserMetadataMetricsGroup;
  public final RestRequestMetricsGroup getPeersMetricsGroup;
  public final RestRequestMetricsGroup getReplicasMetricsGroup;
  public final RestRequestMetricsGroup getSignedUrlMetricsGroup;
  public final RestRequestMetricsGroup getClusterMapSnapshotMetricsGroup;
  public final RestRequestMetricsGroup getAccountsMetricsGroup;
  // HEAD
  public final RestRequestMetricsGroup headBlobMetricsGroup;
  // OPTIONS
  public final RestRequestMetricsGroup optionsMetricsGroup;
  // POST
  public final RestRequestMetricsGroup postBlobMetricsGroup;
  public final RestRequestMetricsGroup postAccountsMetricsGroup;
  // PUT
  public final RestRequestMetricsGroup updateBlobTtlMetricsGroup;
  public final RestRequestMetricsGroup undeleteBlobMetricsGroup;

  // AsyncOperationTracker.Metrics instances
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

  public final AsyncOperationTracker.Metrics undeleteBlobSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobRouterMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobIdConversionMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics getClusterMapSnapshotSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getClusterMapSnapshotSecurityPostProcessRequestMetrics;

  public final AsyncOperationTracker.Metrics getAccountsSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getAccountsSecurityPostProcessRequestMetrics;

  public final AsyncOperationTracker.Metrics postAccountsSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postAccountsSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postAccountsReadRequestMetrics;

  public final AsyncOperationTracker.Metrics getPreProcessingMetrics;
  public final AsyncOperationTracker.Metrics headPreProcessingMetrics;
  public final AsyncOperationTracker.Metrics deletePreProcessingMetrics;
  public final AsyncOperationTracker.Metrics postPreProcessingMetrics;
  public final AsyncOperationTracker.Metrics putPreProcessingMetrics;

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
  // FrontendRestRequestService
  // POST
  public final Histogram blobPropsBuildTimeInMs;
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
  public final Counter securePathValidationFailedCount;
  // AmbryIdConverter
  public final Histogram idConverterProcessingTimeInMs;
  // GetPeersHandler
  public final Histogram getPeersProcessingTimeInMs;
  // GetReplicasHandler
  public final Histogram getReplicasProcessingTimeInMs;
  // GetSignedUrlHandler
  public final Histogram getSignedUrlProcessingTimeInMs;
  public final Histogram getSignedUrlIdConverterCallbackProcessingTimeInMs;
  // GetClusterMapSnapshotHandler
  public final Histogram getClusterMapSnapshotProcessingTimeInMs;

  // Errors
  // FrontendRestRequestService
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
  // FrontendRestRequestService
  public final Histogram restRequestServiceStartupTimeInMs;
  public final Histogram restRequestServiceShutdownTimeInMs;

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
    // RestRequestMetricsGroup
    // DELETE
    deleteBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "DeleteBlob", false, true, metricRegistry);
    // GET
    getBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetBlob", true, true, metricRegistry);
    getBlobInfoMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetBlobInfo", true, true, metricRegistry);
    getUserMetadataMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetUserMetadata", true, false, metricRegistry);
    getPeersMetricsGroup = new RestRequestMetricsGroup(GetPeersHandler.class, "GetPeers", false, false, metricRegistry);
    getReplicasMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetReplicas", false, false, metricRegistry);
    getSignedUrlMetricsGroup =
        new RestRequestMetricsGroup(GetSignedUrlHandler.class, "GetSignedUrl", false, true, metricRegistry);
    getClusterMapSnapshotMetricsGroup =
        new RestRequestMetricsGroup(GetClusterMapSnapshotHandler.class, "GetClusterMapSnapshot", false, false,
            metricRegistry);
    getAccountsMetricsGroup =
        new RestRequestMetricsGroup(GetAccountsHandler.class, "GetAccounts", false, false, metricRegistry);
    // HEAD
    headBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "HeadBlob", false, false, metricRegistry);
    // OPTIONS
    optionsMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "Options", false, false, metricRegistry);
    // POST
    postBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "PostBlob", true, true, metricRegistry);
    postAccountsMetricsGroup =
        new RestRequestMetricsGroup(PostAccountsHandler.class, "PostAccounts", false, false, metricRegistry);
    // PUT
    updateBlobTtlMetricsGroup =
        new RestRequestMetricsGroup(TtlUpdateHandler.class, "UpdateBlobTtl", false, true, metricRegistry);
    undeleteBlobMetricsGroup =
        new RestRequestMetricsGroup(UndeleteHandler.class, "UndeleteBlob", false, true, metricRegistry);

    // AsyncOperationTracker.Metrics instances
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

    undeleteBlobSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "securityProcessRequest", metricRegistry);
    undeleteBlobSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "securityPostProcessRequest", metricRegistry);
    undeleteBlobRouterMetrics = new AsyncOperationTracker.Metrics(UndeleteHandler.class, "router", metricRegistry);
    undeleteBlobIdConversionMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "idConversion", metricRegistry);
    undeleteBlobSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "securityProcessResponse", metricRegistry);

    getClusterMapSnapshotSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetClusterMapSnapshotHandler.class, "SecurityProcessRequest", metricRegistry);
    getClusterMapSnapshotSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetClusterMapSnapshotHandler.class, "SecurityPostProcessRequest",
            metricRegistry);

    getAccountsSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetAccountsHandler.class, "SecurityProcessRequest", metricRegistry);
    getAccountsSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetAccountsHandler.class, "SecurityPostProcessRequest", metricRegistry);

    postAccountsSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostAccountsHandler.class, "SecurityProcessRequest", metricRegistry);
    postAccountsSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostAccountsHandler.class, "SecurityPostProcessRequest", metricRegistry);
    postAccountsReadRequestMetrics =
        new AsyncOperationTracker.Metrics(PostAccountsHandler.class, "ReadRequest", metricRegistry);

    getPreProcessingMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "GetPreProcessing", metricRegistry);
    headPreProcessingMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "HeadPreProcessing", metricRegistry);
    deletePreProcessingMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "DeletePreProcessing", metricRegistry);
    postPreProcessingMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "PostPreProcessing", metricRegistry);
    putPreProcessingMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "PutPreProcessing", metricRegistry);

    getSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "getSecurityPostProcessRequest",
            metricRegistry);
    headSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "headSecurityPostProcessRequest",
            metricRegistry);
    deleteSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(FrontendRestRequestService.class, "deleteSecurityPostProcessRequest",
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
    // FrontendRestRequestService
    // POST
    blobPropsBuildTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "BlobPropsBuildTimeInMs"));
    // OPTIONS
    optionsPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "OptionsPreProcessingTimeInMs"));
    optionsSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "OptionsSecurityRequestTimeInMs"));
    optionsSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "OptionsSecurityResponseTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "DeleteCallbackProcessingTimeInMs"));
    deleteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "DeleteCallbackResultTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "HeadCallbackProcessingTimeInMs"));
    headTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "HeadCallbackResultTimeInMs"));
    headSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "HeadSecurityResponseCallbackProcessingTimeInMs"));
    headSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "HeadSecurityResponseTimeInMs"));
    // GetCallback
    getCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "GetCallbackProcessingTimeInMs"));
    getTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "GetCallbackResultTimeInMs"));
    getSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "GetSecurityResponseCallbackProcessingTimeInMs"));
    getSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "GetSecurityResponseTimeInMs"));
    // InboundIdConverterCallback
    inboundIdConversionCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "InboundIdCallbackProcessingTimeInMs"));
    inboundIdConversionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "InboundIdConversionTimeInMs"));
    // SecurityProcessRequestCallback
    deleteSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "DeleteSecurityRequestCallbackProcessingTimeInMs"));
    deleteSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "DeleteSecurityRequestTimeInMs"));
    headSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "HeadSecurityRequestCallbackProcessingTimeInMs"));
    headSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "HeadSecurityRequestTimeInMs"));
    getSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "GetSecurityRequestCallbackProcessingTimeInMs"));
    getPeersSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(GetPeersHandler.class, "SecurityRequestCallbackProcessingTimeInMs"));
    getSignedUrlSecurityRequestCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(GetSignedUrlHandler.class, "SecurityRequestCallbackProcessingTimeInMs"));
    getSecurityRequestTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "GetSecurityRequestTimeInMs"));
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
    securePathValidationFailedCount =
        metricRegistry.counter(MetricRegistry.name(AmbrySecurityService.class, "SecurePathValidationFailedCount"));
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
    // GetClusterMapSnapshotHandler
    getClusterMapSnapshotProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetClusterMapSnapshotHandler.class, "ProcessingTimeInMs"));

    // Errors
    // FrontendRestRequestService
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "ResourceReleaseError"));
    // DeleteCallback
    deleteCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "DeleteCallbackProcessingError"));
    // HeadCallback
    headCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "HeadCallbackProcessingError"));
    // GetCallback
    getCallbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "GetCallbackProcessingError"));
    getSecurityResponseCallbackProcessingError = metricRegistry.counter(
        MetricRegistry.name(FrontendRestRequestService.class, "GetSecurityResponseCallbackProcessingError"));
    // PostBlobHandler
    ttlTooLargeError = metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "TtlTooLargeError"));
    ttlNotCompliantError =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "TtlNotCompliantError"));
    // GetPeersHandler
    unknownDatanodeError = metricRegistry.counter(MetricRegistry.name(GetPeersHandler.class, "UnknownDatanodeError"));
    // GetReplicasHandler
    invalidBlobIdError = metricRegistry.counter(MetricRegistry.name(GetReplicasHandler.class, "InvalidBlobIdError"));
    responseConstructionError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasHandler.class, "ResponseConstructionError"));

    // Other
    restRequestServiceStartupTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "StartupTimeInMs"));
    restRequestServiceShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "ShutdownTimeInMs"));

    // Account and container
    unrecognizedServiceIdCount =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "UnrecognizedServiceIdCount"));
    unrecognizedAccountNameCount =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "UnrecognizedAccountNameCount"));
    unrecognizedContainerNameCount =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "UnrecognizedContainerNameCount"));
    getHeadDeleteUnrecognizedAccountCount = metricRegistry.counter(
        MetricRegistry.name(FrontendRestRequestService.class, "GetHeadDeleteUnrecognizedAccountCount"));
    getHeadDeleteUnrecognizedContainerCount = metricRegistry.counter(
        MetricRegistry.name(FrontendRestRequestService.class, "GetHeadDeleteUnrecognizedContainerCount"));
    putWithServiceIdForAccountNameRate =
        metricRegistry.meter(MetricRegistry.name(FrontendRestRequestService.class, "PutWithServiceIdForAccountNameRate"));
    putWithAccountAndContainerHeaderRate = metricRegistry.meter(
        MetricRegistry.name(FrontendRestRequestService.class, "PutWithAccountAndContainerHeaderRate"));
  }
}
