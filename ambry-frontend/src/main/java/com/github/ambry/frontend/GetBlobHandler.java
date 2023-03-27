/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Dataset;
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.store.StoreKey;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handler to handle all the http GET requests on blobs.
 */
public class GetBlobHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(GetBlobHandler.class);
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
  static final String BLOB_CHUNK_IDS_KEY = "blobChunkIds";

  private final FrontendConfig frontendConfig;
  private final Router router;
  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final ClusterMap clusterMap;
  private final QuotaManager quotaManager;
  private final GetReplicasHandler getReplicasHandler;
  private final AccountService accountService;

  GetBlobHandler(FrontendConfig frontendConfig, Router router, SecurityService securityService, IdConverter idConverter,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics, ClusterMap clusterMap,
      QuotaManager quotaManager, AccountService accountService) {
    this.frontendConfig = frontendConfig;
    this.router = router;
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.clusterMap = clusterMap;
    this.quotaManager = quotaManager;
    getReplicasHandler = new GetReplicasHandler(metrics, clusterMap);
    this.accountService = accountService;
  }

  void handle(RequestPath requestPath, RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    SubResource subResource = requestPath.getSubResource();
    GetBlobOptions options = buildGetBlobOptions(restRequest.getArgs(), subResource,
        getGetOption(restRequest, frontendConfig.defaultRouterGetOption), restRequest, requestPath.getBlobSegmentIdx());
    RestRequestMetricsGroup metricsGroup = getMetricsGroupForGet(metrics, subResource);
    RestRequestMetrics restRequestMetrics = metricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
    // named blob requests have their account/container in the URI, so checks can be done prior to ID conversion.
    if (requestPath.matchesOperation(Operations.NAMED_BLOB)) {
      accountAndContainerInjector.injectAccountContainerAndDatasetForNamedBlob(restRequest, metricsGroup);
    }
    restRequest.getMetricsTracker().injectMetrics(restRequestMetrics);
    new CallbackChain(restRequest, restResponseChannel, metricsGroup, requestPath, subResource, options,
        callback).start();
  }

  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;
    private final RestRequestMetricsGroup metricsGroup;
    private final RequestPath requestPath;
    private final SubResource subResource;
    private final GetBlobOptions options;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        RestRequestMetricsGroup metricsGroup, RequestPath requestPath, SubResource subResource, GetBlobOptions options,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.metricsGroup = metricsGroup;
      this.requestPath = requestPath;
      this.subResource = subResource;
      this.options = options;
      this.finalCallback = finalCallback;
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link IdConverter#convert} to convert the incoming
     * ID if required.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(metrics.getBlobSecurityProcessRequestMetrics, result -> {
        String blobIdStr;
        if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
          try {
            metrics.getDatasetVersionRate.mark();
            blobIdStr = getDatasetVersion(restRequest);
          } catch (RestServiceException e) {
            metrics.getDatasetVersionError.inc();
            throw e;
          }
        } else {
          blobIdStr = requestPath.getOperationOrBlobId(false);
        }
        idConverter.convert(restRequest, blobIdStr, idConverterCallback());
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link IdConverter#convert} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link IdConverter#convert}.
     */
    private Callback<String> idConverterCallback() {
      return buildCallback(metrics.getBlobIdConversionMetrics, convertedBlobId -> {
        BlobId blobId = FrontendUtils.getBlobIdFromString(convertedBlobId, clusterMap);
        if (restRequest.getArgs().get(TARGET_ACCOUNT_KEY) == null) {
          // Inject account and container when they are missing from the rest request.
          accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(blobId, restRequest, metricsGroup);
        }
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback(blobId));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#getBlob} to dget
     * the blob info from the storage layer.
     * @param blobId the {@link BlobId} to get info
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobId blobId) {
      return buildCallback(metrics.getSecurityPostProcessRequestMetrics, result -> {
        // inject encryption metrics if need be
        if (BlobId.isEncrypted(blobId.getID())) {
          RestRequestMetrics restRequestMetrics = metricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true);
          restRequest.getMetricsTracker().injectMetrics(restRequestMetrics);
        }
        if (subResource == null) {
          router.getBlob(blobId.getID(), options, routerCallback(),
              QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, true));
        } else {
          switch (subResource) {
            case BlobInfo:
            case UserMetadata:
            case BlobChunkIds:
            case Segment:
              router.getBlob(blobId.getID(), options, routerCallback(),
                  QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, true));
              break;
            case Replicas:
              finalCallback.onCompletion(getReplicasHandler.getReplicas(blobId.getID(), restResponseChannel), null);
              break;
            default:
              LOGGER.error("Unexpected subResource {} when post process request", subResource);
          }
        }
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link Router#getBlob} finishes, call {@link SecurityService#processResponse}.
     * @return a {@link Callback} to be used with {@link Router#getBlob}.
     */
    private Callback<GetBlobResult> routerCallback() {
      final AtomicReference<GetBlobResult> resultRef = new AtomicReference(null);
      return buildCallback(metrics.getBlobRouterMetrics, result -> {
            resultRef.set(result);
            accountAndContainerInjector.ensureAccountAndContainerInjected(restRequest,
                result.getBlobInfo().getBlobProperties(), metricsGroup);
            securityService.processResponse(restRequest, restResponseChannel, result.getBlobInfo(),
                securityProcessResponseCallback(result));
          }, restRequest.getUri(), LOGGER,
          // Still pass result.getBlobDataChannel() to finalCallback if we already have it.
          (r, e) -> {
            // Even we failed in router operations, we already used some of the resources in router,
            // so let's record the charges for this request.
            securityService.processRequestCharges(restRequest, restResponseChannel, null);
            finalCallback.onCompletion(resultRef.get() != null ? resultRef.get().getBlobDataChannel() : null, e);
          });
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback(GetBlobResult result) {
      return buildCallback(metrics.getBlobSecurityProcessResponseMetrics, securityCheckResult -> {
            ReadableStreamChannel response = result.getBlobDataChannel();
            if (subResource == null) {
              response = closeChannelIfNotModified(result);
            } else {
              switch (subResource) {
                case BlobInfo:
                case UserMetadata:
                  response = setMetadataInResponseChannel(result);
                  break;
                case Segment:
                  response = closeChannelIfNotModified(result);
                  break;
                case BlobChunkIds:
                  response = setChunkBlobIdsInResponseChannel(result);
                  break;
                default:
                  LOGGER.error("Unexpected subResource {} when handle response", subResource);
              }
            }
            finalCallback.onCompletion(response, null);
          }, restRequest.getUri(), LOGGER,
          // Still pass result.getBlobDataChannel() to finalCallback since we already have it.
          (r, e) -> finalCallback.onCompletion(result != null ? result.getBlobDataChannel() : null, e));
    }

    /**
     * Close the channel if the blob was not modified, as it will not be submitted to the RestResponseHandler.
     * @param result The result of the request i.e a {@link GetBlobResult} object with the properties of the blob.
     * @return a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
     * @throws IOException if there's any error during channel close.
     */
    private ReadableStreamChannel closeChannelIfNotModified(GetBlobResult result) throws IOException {
      if (restResponseChannel.getStatus() == ResponseStatus.NotModified) {
        result.getBlobDataChannel().close();
        return null;
      }
      return result.getBlobDataChannel();
    }

    /**
     * Set metadata in response channel.
     * @param result result The result of the request i.e a {@link GetBlobResult} object with the properties of the blob.
     * @return a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
     */
    private ReadableStreamChannel setMetadataInResponseChannel(GetBlobResult result) {
      BlobInfo blobInfo = result.getBlobInfo();
      if (restRequest.getArgs().containsKey(SEND_USER_METADATA_AS_RESPONSE_BODY) && (boolean) restRequest.getArgs()
          .get(SEND_USER_METADATA_AS_RESPONSE_BODY)) {
        restResponseChannel.setHeader(Headers.CONTENT_TYPE, "application/octet-stream");
        restResponseChannel.setHeader(Headers.CONTENT_LENGTH, blobInfo.getUserMetadata().length);
        return new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blobInfo.getUserMetadata()));
      } else {
        restResponseChannel.setHeader(Headers.CONTENT_LENGTH, 0);
        return new ByteBufferReadableStreamChannel(EMPTY_BUFFER);
      }
    }

    /**
     * Set chunk blob Ids for composite blob in response channel.
     * @param result The result of the request i.e a {@link GetBlobResult} object with the properties of the blob.
     * @return a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
     * @throws RestServiceException if there's any error during serialization.
     */
    private ReadableStreamChannel setChunkBlobIdsInResponseChannel(GetBlobResult result)
        throws RestServiceException {
      JSONObject jsonObject = new JSONObject();
      List<StoreKey> blobChunkIds = result.getBlobChunkIds();
      if (blobChunkIds == null || blobChunkIds.isEmpty()) {
        jsonObject.put(BLOB_CHUNK_IDS_KEY, new ArrayList<>());
      } else {
        for (StoreKey blobChunkId : blobChunkIds) {
          jsonObject.append(BLOB_CHUNK_IDS_KEY, blobChunkId.getID());
        }
      }
      return FrontendUtils.serializeJsonToChannel(jsonObject);
    }

    /**
     * Support get dataset version queries before get the named blob.
     * @param restRequest {@link RestRequest} representing the request.
     * @return the named blob path uri corresponding to the dataset version.
     * @throws RestServiceException
     */
    private String getDatasetVersion(RestRequest restRequest) throws RestServiceException {
      long startGetDatasetVersionTime = System.currentTimeMillis();
      String accountName = null;
      String containerName = null;
      String datasetName = null;
      String version = null;
      try {
        Dataset dataset = (Dataset) restRequest.getArgs().get(InternalKeys.TARGET_DATASET);
        accountName = dataset.getAccountName();
        containerName = dataset.getContainerName();
        datasetName = dataset.getDatasetName();
        version = (String) restRequest.getArgs().get(TARGET_DATASET_VERSION);
        DatasetVersionRecord datasetVersionRecord =
            accountService.getDatasetVersion(accountName, containerName, datasetName, version);
        FrontendUtils.replaceRequestPathWithNewOperationOrBlobIdIfNeeded(restRequest, datasetVersionRecord, version);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_NAME, datasetName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_VERSION, datasetVersionRecord.getVersion());
        metrics.getDatasetVersionProcessingTimeInMs.update(System.currentTimeMillis() - startGetDatasetVersionTime);
        // If version is null, use the latest version + 1 from DatasetVersionRecord to construct named blob path.
        return NAMED_BLOB_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + datasetName + SLASH
            + datasetVersionRecord.getVersion();
      } catch (AccountServiceException ex) {
        LOGGER.error(
            "Failed to get dataset version for accountName: " + accountName + " containerName: " + containerName
                + " datasetName: " + datasetName + " version: " + version);
        throw new RestServiceException(ex.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }
  }
}
