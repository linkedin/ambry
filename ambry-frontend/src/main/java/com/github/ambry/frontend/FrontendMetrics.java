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
import com.github.ambry.config.FrontendConfig;
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
  public final RestRequestMetricsGroup getBlobChunkIdsMetricsGroup;
  public final RestRequestMetricsGroup getSignedUrlMetricsGroup;
  public final RestRequestMetricsGroup getClusterMapSnapshotMetricsGroup;
  public final RestRequestMetricsGroup getAccountsMetricsGroup;
  public final RestRequestMetricsGroup getDatasetsMetricsGroup;
  public final RestRequestMetricsGroup getStatsReportMetricsGroup;
  // HEAD
  public final RestRequestMetricsGroup headBlobMetricsGroup;
  // OPTIONS
  public final RestRequestMetricsGroup optionsMetricsGroup;
  // POST
  public final RestRequestMetricsGroup postBlobMetricsGroup;
  public final RestRequestMetricsGroup postAccountsMetricsGroup;
  public final RestRequestMetricsGroup postDatasetsMetricsGroup;

  // PUT
  public final RestRequestMetricsGroup updateBlobTtlMetricsGroup;
  public final RestRequestMetricsGroup undeleteBlobMetricsGroup;
  public final RestRequestMetricsGroup putBlobMetricsGroup;

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

  public final AsyncOperationTracker.Metrics deleteBlobSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics deleteBlobSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics deleteBlobRouterMetrics;
  public final AsyncOperationTracker.Metrics deleteBlobIdConversionMetrics;
  public final AsyncOperationTracker.Metrics deleteBlobSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics undeleteBlobSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobRouterMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobIdConversionMetrics;
  public final AsyncOperationTracker.Metrics undeleteBlobSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics headBlobSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics headBlobSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics headBlobRouterMetrics;
  public final AsyncOperationTracker.Metrics headBlobIdConversionMetrics;
  public final AsyncOperationTracker.Metrics headBlobSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics getBlobSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getBlobSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getBlobRouterMetrics;
  public final AsyncOperationTracker.Metrics getBlobIdConversionMetrics;
  public final AsyncOperationTracker.Metrics getBlobSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics putSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics putSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics putReadStitchRequestMetrics;
  public final AsyncOperationTracker.Metrics putRouterStitchBlobMetrics;
  public final AsyncOperationTracker.Metrics putRouterPutBlobMetrics;
  public final AsyncOperationTracker.Metrics putIdConversionMetrics;
  public final AsyncOperationTracker.Metrics putBlobRouterMetrics;
  public final AsyncOperationTracker.Metrics putBlobSecurityProcessResponseMetrics;

  public final AsyncOperationTracker.Metrics listSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics listSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics listDbLookupMetrics;

  public final AsyncOperationTracker.Metrics getClusterMapSnapshotSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getClusterMapSnapshotSecurityPostProcessRequestMetrics;

  public final AsyncOperationTracker.Metrics getAccountsSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getAccountsSecurityPostProcessRequestMetrics;

  public final AsyncOperationTracker.Metrics getDatasetsSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getDatasetsSecurityPostProcessRequestMetrics;

  public final AsyncOperationTracker.Metrics getStatsReportSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics getStatsReportSecurityPostProcessRequestMetrics;

  public final AsyncOperationTracker.Metrics postAccountsSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postAccountsSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postAccountsReadRequestMetrics;

  public final AsyncOperationTracker.Metrics postDatasetsSecurityProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postDatasetsSecurityPostProcessRequestMetrics;
  public final AsyncOperationTracker.Metrics postDatasetsReadRequestMetrics;

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
  // NAMED BLOB PUT
  public final Histogram blobPropsBuildForNameBlobPutTimeInMs;

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
  public final Histogram idConversionDownstreamCallbackTimeInMs;

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
  public final Meter putWithAccountAndContainerUriRate;

  // Dataset
  public final Counter unrecognizedDatasetNameCount;
  public final Meter getDatasetWithAccountAndContainerHeaderRate;

  //Dataset Version
  public final Counter addDatasetVersionError;
  public final Counter getDatasetVersionError;
  public final Meter addDatasetVersionRate;
  public final Meter getDatasetVersionRate;
  public final Histogram addDatasetVersionProcessingTimeInMs;
  public final Histogram getDatasetVersionProcessingTimeInMs;

  /**
   * Creates an instance of FrontendMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   * @param frontendConfig the {@link FrontendConfig} to use for the metrics.
   */
  public FrontendMetrics(MetricRegistry metricRegistry, FrontendConfig frontendConfig) {
    // RestRequestMetricsGroup
    // DELETE
    deleteBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "DeleteBlob", false, metricRegistry,
            frontendConfig);
    // GET
    getBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetBlob", true, metricRegistry, frontendConfig);
    getBlobInfoMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetBlobInfo", true, metricRegistry,
            frontendConfig);
    getUserMetadataMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetUserMetadata", true, metricRegistry,
            frontendConfig);
    getPeersMetricsGroup =
        new RestRequestMetricsGroup(GetPeersHandler.class, "GetPeers", false, metricRegistry, frontendConfig);
    getReplicasMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetReplicas", false, metricRegistry,
            frontendConfig);
    getBlobChunkIdsMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "GetBlobChunkIds", false, metricRegistry,
            frontendConfig);
    getSignedUrlMetricsGroup =
        new RestRequestMetricsGroup(GetSignedUrlHandler.class, "GetSignedUrl", false, metricRegistry, frontendConfig);
    getClusterMapSnapshotMetricsGroup =
        new RestRequestMetricsGroup(GetClusterMapSnapshotHandler.class, "GetClusterMapSnapshot", false, metricRegistry,
            frontendConfig);
    getAccountsMetricsGroup =
        new RestRequestMetricsGroup(GetAccountsHandler.class, "GetAccounts", false, metricRegistry, frontendConfig);
    getDatasetsMetricsGroup =
        new RestRequestMetricsGroup(GetDatasetsHandler.class, "GetDatasets", false, metricRegistry, frontendConfig);
    getStatsReportMetricsGroup =
        new RestRequestMetricsGroup(GetStatsReportHandler.class, "GetStatsReport", false, metricRegistry,
            frontendConfig);
    // HEAD
    headBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "HeadBlob", false, metricRegistry,
            frontendConfig);
    // OPTIONS
    optionsMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "Options", false, metricRegistry, frontendConfig);
    // POST
    postBlobMetricsGroup =
        new RestRequestMetricsGroup(FrontendRestRequestService.class, "PostBlob", true, metricRegistry, frontendConfig);
    postAccountsMetricsGroup =
        new RestRequestMetricsGroup(PostAccountsHandler.class, "PostAccounts", false, metricRegistry, frontendConfig);
    postDatasetsMetricsGroup =
        new RestRequestMetricsGroup(PostDatasetsHandler.class, "PostDatasets", false, metricRegistry, frontendConfig);
    // PUT
    updateBlobTtlMetricsGroup =
        new RestRequestMetricsGroup(TtlUpdateHandler.class, "UpdateBlobTtl", false, metricRegistry, frontendConfig);
    undeleteBlobMetricsGroup =
        new RestRequestMetricsGroup(UndeleteHandler.class, "UndeleteBlob", false, metricRegistry, frontendConfig);
    putBlobMetricsGroup =
        new RestRequestMetricsGroup(NamedBlobPutHandler.class, "PutBlob", false, metricRegistry, frontendConfig);
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

    deleteBlobSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(DeleteBlobHandler.class, "securityProcessRequest", metricRegistry);
    deleteBlobSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(DeleteBlobHandler.class, "securityPostProcessRequest", metricRegistry);
    deleteBlobRouterMetrics = new AsyncOperationTracker.Metrics(DeleteBlobHandler.class, "router", metricRegistry);
    deleteBlobIdConversionMetrics =
        new AsyncOperationTracker.Metrics(DeleteBlobHandler.class, "idConversion", metricRegistry);
    deleteBlobSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(DeleteBlobHandler.class, "securityProcessResponse", metricRegistry);

    undeleteBlobSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "securityProcessRequest", metricRegistry);
    undeleteBlobSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "securityPostProcessRequest", metricRegistry);
    undeleteBlobRouterMetrics = new AsyncOperationTracker.Metrics(UndeleteHandler.class, "router", metricRegistry);
    undeleteBlobIdConversionMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "idConversion", metricRegistry);
    undeleteBlobSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(UndeleteHandler.class, "securityProcessResponse", metricRegistry);

    headBlobSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(HeadBlobHandler.class, "securityProcessRequest", metricRegistry);
    headBlobSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(HeadBlobHandler.class, "securityPostProcessRequest", metricRegistry);
    headBlobRouterMetrics = new AsyncOperationTracker.Metrics(HeadBlobHandler.class, "router", metricRegistry);
    headBlobIdConversionMetrics = new AsyncOperationTracker.Metrics(HeadBlobHandler.class, "idConversion", metricRegistry);
    headBlobSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(HeadBlobHandler.class, "securityProcessResponse", metricRegistry);

    getBlobSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetBlobHandler.class, "securityProcessRequest", metricRegistry);
    getBlobSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetBlobHandler.class, "securityPostProcessRequest", metricRegistry);
    getBlobRouterMetrics = new AsyncOperationTracker.Metrics(GetBlobHandler.class, "router", metricRegistry);
    getBlobIdConversionMetrics =
        new AsyncOperationTracker.Metrics(GetBlobHandler.class, "idConversion", metricRegistry);
    getBlobSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(GetBlobHandler.class, "securityProcessResponse", metricRegistry);

    putSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "putSecurityProcessRequest", metricRegistry);
    putSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "putSecurityPostProcessRequest", metricRegistry);
    putReadStitchRequestMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "putReadStitchRequest", metricRegistry);
    putRouterStitchBlobMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "putRouterStitchBlob", metricRegistry);
    putRouterPutBlobMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "putRouterPutBlob", metricRegistry);
    putIdConversionMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "putIdConversion", metricRegistry);
    putBlobRouterMetrics = new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "router", metricRegistry);
    putBlobSecurityProcessResponseMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobPutHandler.class, "securityProcessResponse", metricRegistry);

    listSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobListHandler.class, "SecurityProcessRequest", metricRegistry);
    listSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(NamedBlobListHandler.class, "SecurityPostProcessRequest", metricRegistry);
    listDbLookupMetrics = new AsyncOperationTracker.Metrics(NamedBlobListHandler.class, "DbLookup", metricRegistry);

    getClusterMapSnapshotSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetClusterMapSnapshotHandler.class, "SecurityProcessRequest", metricRegistry);
    getClusterMapSnapshotSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetClusterMapSnapshotHandler.class, "SecurityPostProcessRequest",
            metricRegistry);

    getAccountsSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetAccountsHandler.class, "SecurityProcessRequest", metricRegistry);
    getAccountsSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetAccountsHandler.class, "SecurityPostProcessRequest", metricRegistry);

    getDatasetsSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetDatasetsHandler.class, "SecurityProcessRequest", metricRegistry);
    getDatasetsSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetDatasetsHandler.class, "SecurityPostProcessRequest", metricRegistry);

    getStatsReportSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetStatsReportHandler.class, "SecurityProcessRequest", metricRegistry);
    getStatsReportSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(GetStatsReportHandler.class, "SecurityPostProcessRequest", metricRegistry);

    postAccountsSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostAccountsHandler.class, "SecurityProcessRequest", metricRegistry);
    postAccountsSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostAccountsHandler.class, "SecurityPostProcessRequest", metricRegistry);
    postAccountsReadRequestMetrics =
        new AsyncOperationTracker.Metrics(PostAccountsHandler.class, "ReadRequest", metricRegistry);

    postDatasetsSecurityProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostDatasetsHandler.class, "SecurityProcessRequest", metricRegistry);
    postDatasetsSecurityPostProcessRequestMetrics =
        new AsyncOperationTracker.Metrics(PostDatasetsHandler.class, "SecurityPostProcessRequest", metricRegistry);
    postDatasetsReadRequestMetrics =
        new AsyncOperationTracker.Metrics(PostDatasetsHandler.class, "ReadRequest", metricRegistry);

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
    // NAMEDBLOBPUT
    blobPropsBuildForNameBlobPutTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "blobPropsBuildForNameBlobPutTimeInMs"));
    // OPTIONS
    optionsPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "OptionsPreProcessingTimeInMs"));
    optionsSecurityRequestTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "OptionsSecurityRequestTimeInMs"));
    optionsSecurityResponseTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "OptionsSecurityResponseTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "DeleteCallbackProcessingTimeInMs"));
    deleteTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "DeleteCallbackResultTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "HeadCallbackProcessingTimeInMs"));
    headTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "HeadCallbackResultTimeInMs"));
    headSecurityResponseCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "HeadSecurityResponseCallbackProcessingTimeInMs"));
    headSecurityResponseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(FrontendRestRequestService.class, "HeadSecurityResponseTimeInMs"));
    // GetCallback
    getCallbackProcessingTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "GetCallbackProcessingTimeInMs"));
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
    deleteSecurityRequestTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(FrontendRestRequestService.class, "DeleteSecurityRequestTimeInMs"));
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
    idConversionDownstreamCallbackTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryIdConverterFactory.class, "DownstreamCallbackTimeInMs"));
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
    ttlTooLargeError =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "TtlTooLargeError"));
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
    putWithServiceIdForAccountNameRate = metricRegistry.meter(
        MetricRegistry.name(FrontendRestRequestService.class, "PutWithServiceIdForAccountNameRate"));
    putWithAccountAndContainerHeaderRate = metricRegistry.meter(
        MetricRegistry.name(FrontendRestRequestService.class, "PutWithAccountAndContainerHeaderRate"));
    putWithAccountAndContainerUriRate = metricRegistry.meter(
        MetricRegistry.name(FrontendRestRequestService.class, "PutWithAccountAndContainerUriRate"));

    // Dataset
    unrecognizedDatasetNameCount =
        metricRegistry.counter(MetricRegistry.name(FrontendRestRequestService.class, "UnrecognizedDatasetNameCount"));
    getDatasetWithAccountAndContainerHeaderRate = metricRegistry.meter(
        MetricRegistry.name(FrontendRestRequestService.class, "GetDatasetWithAccountAndContainerHeaderRate"));

    // Dataset Version
    addDatasetVersionError =
        metricRegistry.counter(MetricRegistry.name(NamedBlobPutHandler.class, "AddDatasetVersionError"));
    getDatasetVersionError =
        metricRegistry.counter(MetricRegistry.name(GetBlobHandler.class, "GetDatasetVersionError"));
    addDatasetVersionRate =
        metricRegistry.meter(MetricRegistry.name(NamedBlobPutHandler.class, "AddDatasetVersionRate"));
    getDatasetVersionRate =
        metricRegistry.meter(MetricRegistry.name(GetBlobHandler.class, "GetDatasetVersionRate"));
    addDatasetVersionProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(NamedBlobPutHandler.class, "AddDatasetVersionProcessingTimeInMs"));
    getDatasetVersionProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetBlobHandler.class, "GetDatasetVersionProcessingTimeInMs"));
  }
}
