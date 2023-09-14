/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.Container;
import com.github.ambry.account.Dataset;
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.commons.RetryExecutor;
import com.github.ambry.commons.RetryPolicies;
import com.github.ambry.commons.RetryPolicy;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.protocol.DatasetVersionState;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.NoOpResponseChannel;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.WrappedRestRequest;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;
import static com.github.ambry.router.RouterErrorCode.*;


/**
 * Handler for put named blob requests. The following request types are handled by {@link NamedBlobPutHandler}:
 * <h2>Direct uploads</h2>
 * Direct upload requests treat the body of the request as the content to upload to Ambry. The request path should be
 * "/named/account_name/container_name/blob_name". In these requests, the blob properties and user metadata are supplied as headers. See
 * {@link RestUtils#buildBlobProperties(Map)} and {@link RestUtils#buildBlobProperties(Map)} for more details.
 * <h2>Stitched uploads</h2>
 * Stitched upload requests allow clients to stitch together previously uploaded data chunks into a single logical blob.
 * The request path should be "/named/account_name/container_name/blob_name", This request accepts the same headers as direct
 * upload requests for supplying the blob properties and user metadata of the stitched blob, but, instead of the actual
 * blob content, accepts a UTF-8 JSON object that includes the signed IDs for the chunks to stitch, and header x-ambry-put-mode should set as "STITCH".
 * <h3>Request body format</h3>
 * The body of the request should be a JSON object that conforms to the format described in {@link StitchRequestSerDe}.
 */
public class NamedBlobPutHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NamedBlobPutHandler.class);
  /**
   * Key to represent the time at which a blob will expire in ms. Used within the metadata map in signed IDs.
   */
  static final String EXPIRATION_TIME_MS_KEY = "et";
  private final SecurityService securityService;
  private final NamedBlobDb namedBlobDb;
  private final IdConverter idConverter;
  private final IdSigningService idSigningService;
  private final AccountService accountService;
  private final Router router;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final String clusterName;
  private final QuotaManager quotaManager;
  private final RetryPolicy retryPolicy = RetryPolicies.defaultPolicy();
  private final RetryExecutor retryExecutor = new RetryExecutor(Executors.newScheduledThreadPool(2));
  private final Set<RouterErrorCode> retriableRouterError =
      EnumSet.of(AmbryUnavailable, ChannelClosed, UnexpectedInternalError, OperationTimedOut);
  private final DeleteBlobHandler deleteBlobHandler;

  /**
   * Constructs a handler for handling requests for uploading or stitching blobs.
   * @param securityService the {@link SecurityService} to use.
   * @param namedBlobDb the {@link NamedBlobDb} to use.
   * @param idConverter the {@link IdConverter} to use.
   * @param idSigningService the {@link IdSigningService} to use.
   * @param router the {@link Router} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param frontendConfig the {@link FrontendConfig} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param clusterName the name of the storage cluster that the router communicates with
   * @param quotaManager The {@link QuotaManager} class to account for quota usage in serving requests.
   * @param accountService The {@link AccountService} to get the account and container id based on names.
   * @param deleteBlobHandler
   */
  NamedBlobPutHandler(SecurityService securityService, NamedBlobDb namedBlobDb, IdConverter idConverter,
      IdSigningService idSigningService, Router router, AccountAndContainerInjector accountAndContainerInjector,
      FrontendConfig frontendConfig, FrontendMetrics frontendMetrics, String clusterName, QuotaManager quotaManager,
      AccountService accountService, DeleteBlobHandler deleteBlobHandler) {
    this.securityService = securityService;
    this.namedBlobDb = namedBlobDb;
    this.idConverter = idConverter;
    this.idSigningService = idSigningService;
    this.router = router;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.clusterName = clusterName;
    this.quotaManager = quotaManager;
    this.accountService = accountService;
    this.deleteBlobHandler = deleteBlobHandler;
  }

  /**
   * Handles a request for put a blob
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    restRequest.setArg(SEND_FAILURE_REASON, Boolean.TRUE);
    new NamedBlobPutHandler.CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> finalCallback;
    private final Callback<Void> deleteDatasetCallback;
    private final String uri;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<Void> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.deleteDatasetCallback = deleteDatasetVersionIfUploadFailedCallBack(finalCallback);
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      restRequest.getMetricsTracker()
          .injectMetrics(frontendMetrics.putBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false));
      try {
        // Start the callback chain by parsing blob info headers and performing request security processing.
        securityService.processRequest(restRequest, securityProcessRequestCallback());
      } catch (Exception e) {
        finalCallback.onCompletion(null, e);
      }
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.putSecurityProcessRequestMetrics, securityCheckResult -> {
        //make sure this has been called after processRequest(permission check) since it needs to query dataset db.
        BlobInfo blobInfo = getBlobInfoFromRequest();
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback(blobInfo));
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#putBlob} to persist the blob in the
     * storage layer.
     * @param blobInfo the {@link BlobInfo} to make the router call with.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.putSecurityPostProcessRequestMetrics, securityCheckResult -> {
        if (RestUtils.isNamedBlobStitchRequest(restRequest)) {
          if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
            accountAndContainerInjector.injectDatasetForNamedBlob(restRequest);
            frontendMetrics.addDatasetVersionRate.mark();
            addDatasetVersion(blobInfo.getBlobProperties(), restRequest);
          }
          RetainingAsyncWritableChannel channel =
              new RetainingAsyncWritableChannel(frontendConfig.maxJsonRequestSizeBytes);
          restRequest.readInto(channel, fetchStitchRequestBodyCallback(channel, blobInfo));
        } else {
          if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
            accountAndContainerInjector.injectDatasetForNamedBlob(restRequest);
            frontendMetrics.addDatasetVersionRate.mark();
            addDatasetVersion(blobInfo.getBlobProperties(), restRequest);
          }
          PutBlobOptions options = getPutBlobOptionsFromRequest();
          router.putBlob(getPropertiesForRouterUpload(blobInfo), blobInfo.getUserMetadata(), restRequest, options,
              routerPutBlobCallback(blobInfo), QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, true));
        }
      }, uri, LOGGER, deleteDatasetCallback);
    }

    /**
     * After {@link Router#putBlob} finishes, call {@link IdConverter#convert} to store the mapping between blobName and
     * blobId.
     * @param blobInfo the {@link BlobInfo} to use for security checks.
     * @return a {@link Callback} to be used with {@link Router#putBlob}.
     */
    private Callback<String> routerPutBlobCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.putRouterPutBlobMetrics, blobId -> {
        restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, restRequest.getBlobBytesReceived());
        idConverter.convert(restRequest, blobId, blobInfo, idConverterCallback(blobInfo, blobId));
      }, uri, LOGGER, deleteDatasetCallback);
    }

    /**
     * After reading the body of the stitch request, parse the request body,
     * and make a call to {@link Router#stitchBlob}.
     * @param channel the {@link RetainingAsyncWritableChannel} that will contain the request body.
     * @param blobInfo the {@link BlobInfo} to make the router call with.
     * @return a {@link Callback} to be used with {@link RestRequest#readInto}.
     */
    private Callback<Long> fetchStitchRequestBodyCallback(RetainingAsyncWritableChannel channel, BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.putReadStitchRequestMetrics,
          bytesRead -> router.stitchBlob(getPropertiesForRouterUpload(blobInfo), blobInfo.getUserMetadata(),
              getChunksToStitch(blobInfo.getBlobProperties(), readJsonFromChannel(channel)),
              routerStitchBlobCallback(blobInfo), QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, true)),
          uri, LOGGER, deleteDatasetCallback);
    }

    /**
     * After {@link Router#putBlob} finishes, call {@link IdConverter#convert} to convert the returned ID into a format
     * that will be returned in the "Location" header.
     * @param blobInfo the {@link BlobInfo} to use for security checks.
     * @return a {@link Callback} to be used with {@link Router#putBlob}.
     */
    private Callback<String> routerStitchBlobCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.putRouterStitchBlobMetrics,
          blobId -> idConverter.convert(restRequest, blobId, blobInfo, idConverterCallback(blobInfo, blobId)), uri,
          LOGGER, deleteDatasetCallback);
    }

    /**
     * After {@link IdConverter#convert} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @param blobInfo the {@link BlobInfo} to use for security checks.
     * @param blobId the blob ID returned by the router (without decoration or obfuscation by id converter).
     * @return a {@link Callback} to be used with {@link IdConverter#convert}.
     */
    private Callback<String> idConverterCallback(BlobInfo blobInfo, String blobId) {
      return buildCallback(frontendMetrics.putIdConversionMetrics, convertedBlobId -> {
        restResponseChannel.setHeader(RestUtils.Headers.LOCATION, convertedBlobId);
        if (blobInfo.getBlobProperties().getTimeToLiveInSeconds() == Utils.Infinite_Time) {
          // Do ttl update with retryExecutor. Use the blob ID returned from the router instead of the converted ID
          // since the converted ID may be changed by the ID converter.
          String serviceId = blobInfo.getBlobProperties().getServiceId();
          retryExecutor.runWithRetries(retryPolicy,
              callback -> router.updateBlobTtl(blobId, serviceId, Utils.Infinite_Time, callback,
                  QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false)), this::isRetriable,
              routerTtlUpdateCallback(blobInfo, blobId));
        } else {
          if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
            //Make sure to process response after delete finished
            updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount(
                deleteDatasetVersionOutOfRetentionCallback(blobInfo));
          } else {
            securityService.processResponse(restRequest, restResponseChannel, blobInfo,
                securityProcessResponseCallback());
          }
        }
      }, uri, LOGGER, deleteDatasetCallback);
    }

    /**
     * @param throwable the error to check.
     * @return true if the router error is retriable.
     */
    private boolean isRetriable(Throwable throwable) {
      return throwable instanceof RouterException && retriableRouterError.contains(
          ((RouterException) throwable).getErrorCode());
    }

    /**
     * After TTL update finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @param blobInfo the {@link BlobInfo} to use for security checks.
     * @param blobId the {@link String} to use for blob id.
     * @return a {@link Callback} to be used with {@link Router#updateBlobTtl(String, String, long)}.
     */
    private Callback<Void> routerTtlUpdateCallback(BlobInfo blobInfo, String blobId) {
      return buildCallback(frontendMetrics.updateBlobTtlRouterMetrics, convertedBlobId -> {
        // Set the named blob state to be 'READY' after the Ttl update succeed
        if (!restRequest.getArgs().containsKey(RestUtils.InternalKeys.NAMED_BLOB_VERSION)) {
          throw new RestServiceException("Internal key " + RestUtils.InternalKeys.NAMED_BLOB_VERSION
              + " is required in Named Blob TTL update callback!", RestServiceErrorCode.InternalServerError);
        }
        long namedBlobVersion = (long) restRequest.getArgs().get(NAMED_BLOB_VERSION);
        String blobIdClean = RestUtils.stripSlashAndExtensionFromId(blobId);
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(RestUtils.getRequestPath(restRequest), restRequest.getArgs());
        NamedBlobRecord record = new NamedBlobRecord(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
            namedBlobPath.getBlobName(), blobIdClean, Utils.Infinite_Time, namedBlobVersion);
        namedBlobDb.updateBlobTtlAndStateToReady(record).get();
        if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
          //Make sure to process response after delete finished
          updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount(
              deleteDatasetVersionOutOfRetentionCallback(blobInfo));
        } else {
          securityService.processResponse(restRequest, restResponseChannel, blobInfo,
              securityProcessResponseCallback());
        }
      }, uri, LOGGER, deleteDatasetCallback);
    }

    /**
     * After updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount, call {@link SecurityService#processResponse}.
     * @param blobInfo the {@link BlobInfo} to use for security checks.
     * @return a {@link Callback} to be used with updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount.
     */
    private Callback<Void> deleteDatasetVersionOutOfRetentionCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.deleteDatasetOutOfRetentionRequestMetrics,
          deleteResult -> securityService.processResponse(restRequest, restResponseChannel, blobInfo,
              securityProcessResponseCallback()), restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback() {
      return buildCallback(frontendMetrics.putBlobSecurityProcessResponseMetrics,
          securityCheckResult -> finalCallback.onCompletion(null, null), restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * Parse {@link BlobInfo} from the request arguments. This method will also ensure that the correct account and
     * container objects are attached to the request.
     * @return the {@link BlobInfo} parsed from the request arguments.
     * @throws RestServiceException if there is an error while parsing the {@link BlobInfo} arguments.
     */
    private BlobInfo getBlobInfoFromRequest() throws RestServiceException {
      long propsBuildStartTime = System.currentTimeMillis();
      accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, frontendMetrics.putBlobMetricsGroup);
      if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
        accountAndContainerInjector.injectDatasetForNamedBlob(restRequest);
      }
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      if (blobProperties.getTimeToLiveInSeconds() + TimeUnit.MILLISECONDS.toSeconds(
          blobProperties.getCreationTimeInMs()) > Integer.MAX_VALUE) {
        LOGGER.debug("TTL set to very large value in PUT request with BlobProperties {}", blobProperties);
        frontendMetrics.ttlTooLargeError.inc();
      } else if (container.isTtlRequired() && (blobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time
          || blobProperties.getTimeToLiveInSeconds() > frontendConfig.maxAcceptableTtlSecsIfTtlRequired)) {
        String descriptor = RestUtils.getAccountFromArgs(restRequest.getArgs()).getName() + ":" + container.getName();
        if (frontendConfig.failIfTtlRequiredButNotProvided) {
          throw new RestServiceException(
              "TTL < " + frontendConfig.maxAcceptableTtlSecsIfTtlRequired + " is required for upload to " + descriptor,
              RestServiceErrorCode.InvalidArgs);
        } else {
          LOGGER.debug("{} attempted an upload with ttl {} to {}", blobProperties.getServiceId(),
              blobProperties.getTimeToLiveInSeconds(), descriptor);
          frontendMetrics.ttlNotCompliantError.inc();
          restResponseChannel.setHeader(RestUtils.Headers.NON_COMPLIANCE_WARNING,
              "TTL < " + frontendConfig.maxAcceptableTtlSecsIfTtlRequired + " will be required for future uploads");
        }
      }
      // inject encryption frontendMetrics if applicable
      if (blobProperties.isEncrypted()) {
        restRequest.getMetricsTracker()
            .injectMetrics(frontendMetrics.putBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true));
      }
      Map<String, Object> userMetadataFromRequest = new HashMap<>(restRequest.getArgs());
      Map<String, String> userTags = getDatasetUserTags(restRequest);
      if (!userTags.isEmpty()) {
        userTags.forEach((key, value) -> {
          if (!userMetadataFromRequest.containsKey(key)) {
            userMetadataFromRequest.put(key, value);
          }
        });
      }
      byte[] userMetadata = RestUtils.buildUserMetadata(userMetadataFromRequest);
      frontendMetrics.blobPropsBuildForNameBlobPutTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      LOGGER.trace("Blob properties of blob being PUT - {}", blobProperties);
      return new BlobInfo(blobProperties, userMetadata);
    }

    /**
     * Get the user tags at dataset level.
     * @param restRequest the {@link RestRequest} to get dataset user tags.
     * @return the userTags set at dataset level.
     * @throws RestServiceException
     */
    private Map<String, String> getDatasetUserTags(RestRequest restRequest) throws RestServiceException {
      Map<String, String> modifiedUserTags = new HashMap<>();
      if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
        Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
        Map<String, String> userTags = dataset.getUserTags();
        if (userTags != null) {
          for (Map.Entry<String, String> entry : userTags.entrySet()) {
            modifiedUserTags.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + entry.getKey(), entry.getValue());
          }
        }
      }
      return modifiedUserTags;
    }

    /**
     * @return the {@link PutBlobOptions} to use, parsed from the request.
     */
    private PutBlobOptions getPutBlobOptionsFromRequest() throws RestServiceException {
      PutBlobOptionsBuilder builder = new PutBlobOptionsBuilder().chunkUpload(false).restRequest(restRequest);
      Long maxUploadSize = RestUtils.getLongHeader(restRequest.getArgs(), RestUtils.Headers.MAX_UPLOAD_SIZE, false);
      if (maxUploadSize != null) {
        builder.maxUploadSize(maxUploadSize);
      }
      return builder.build();
    }

    /**
     * Parse and verify the signed chunk IDs in the body of a stitch request.
     * @param stitchedBlobProperties the {@link BlobProperties} for the final stitched blob.
     * @param stitchRequestJson the {@link JSONObject} from the stitch request body.
     * @return a list of chunks to stitch that can be provided to the router.
     * @throws RestServiceException
     */
    List<ChunkInfo> getChunksToStitch(BlobProperties stitchedBlobProperties, JSONObject stitchRequestJson)
        throws RestServiceException {
      String reservedMetadataBlobId = null;
      List<String> signedChunkIds = StitchRequestSerDe.fromJson(stitchRequestJson);
      if (signedChunkIds.isEmpty()) {
        throw new RestServiceException("Must provide at least one ID in stitch request",
            RestServiceErrorCode.MissingArgs);
      }
      List<ChunkInfo> chunksToStitch = new ArrayList<>(signedChunkIds.size());
      String expectedSession = null;
      long totalStitchedBlobSize = 0;
      for (String signedChunkId : signedChunkIds) {
        signedChunkId =
            RequestPath.parse(signedChunkId, Collections.emptyMap(), frontendConfig.pathPrefixesToRemove, clusterName)
                .getOperationOrBlobId(false);
        if (!idSigningService.isIdSigned(signedChunkId)) {
          throw new RestServiceException("All chunks IDs must be signed: " + signedChunkId,
              RestServiceErrorCode.BadRequest);
        }
        Pair<String, Map<String, String>> idAndMetadata = idSigningService.parseSignedId(signedChunkId);
        String blobId = idAndMetadata.getFirst();
        Map<String, String> metadata = idAndMetadata.getSecond();

        expectedSession = RestUtils.verifyChunkUploadSession(metadata, expectedSession);
        @SuppressWarnings("ConstantConditions")
        long chunkSizeBytes = RestUtils.getLongHeader(metadata, RestUtils.Headers.BLOB_SIZE, true);

        totalStitchedBlobSize += chunkSizeBytes;
        // Expiration time is sent to the router, but not verified in this handler. The router is responsible for making
        // checks related to internal ambry requirements, like making sure that the chunks do not expire before the
        // metadata blob.
        @SuppressWarnings("ConstantConditions")
        long expirationTimeMs = RestUtils.getLongHeader(metadata, EXPIRATION_TIME_MS_KEY, true);
        verifyChunkAccountAndContainer(blobId, stitchedBlobProperties);
        reservedMetadataBlobId = getAndVerifyReservedMetadataBlobId(metadata, reservedMetadataBlobId, blobId);

        chunksToStitch.add(new ChunkInfo(blobId, chunkSizeBytes, expirationTimeMs, reservedMetadataBlobId));
      }
      //the actual blob size for stitched blob is the sum of all the chunk sizes
      restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, totalStitchedBlobSize);
      return chunksToStitch;
    }

    /**
     * Check that the account and container IDs encoded in a chunk's blob ID matches those in the properties for the
     * stitched blob.
     * @param chunkBlobId the blob ID for the chunk.
     * @param stitchedBlobProperties the {@link BlobProperties} for the stitched blob.
     * @throws RestServiceException if the account or container ID does not match.
     */
    private void verifyChunkAccountAndContainer(String chunkBlobId, BlobProperties stitchedBlobProperties)
        throws RestServiceException {
      Pair<Short, Short> accountAndContainer;
      try {
        accountAndContainer = BlobId.getAccountAndContainerIds(chunkBlobId);
      } catch (Exception e) {
        throw new RestServiceException("Invalid blob ID in signed chunk ID", RestServiceErrorCode.BadRequest);
      }
      if (stitchedBlobProperties.getAccountId() != accountAndContainer.getFirst()
          || stitchedBlobProperties.getContainerId() != accountAndContainer.getSecond()) {
        throw new RestServiceException("Account and container for chunk: (" + accountAndContainer.getFirst() + ", "
            + accountAndContainer.getSecond() + ") does not match account and container for stitched blob: ("
            + stitchedBlobProperties.getAccountId() + ", " + stitchedBlobProperties.getContainerId() + ")",
            RestServiceErrorCode.BadRequest);
      }
    }

    /**
     * Verify that the reserved metadata id for the specified chunkId is same as seen for previous chunks.
     * Also return the chunk's reserved metadata id.
     * @param metadata {@link Map} of metadata set in the signed ids.
     * @param reservedMetadataBlobId Reserved metadata id for the chunks. Can be {@code null}.
     * @param chunkId The chunk id.
     * @return The reserved metadata id.
     * @throws RestServiceException in case of any exception.
     */
    private String getAndVerifyReservedMetadataBlobId(Map<String, String> metadata, String reservedMetadataBlobId,
        String chunkId) throws RestServiceException {
      String chunkReservedMetadataBlobId = RestUtils.getHeader(metadata, RestUtils.Headers.RESERVED_METADATA_ID, false);
      if (chunkReservedMetadataBlobId == null) {
        ReservedMetadataIdMetrics.getReservedMetadataIdMetrics(
            frontendMetrics.getMetricRegistry()).noReservedMetadataForChunkedUploadCount.inc();
        throwRestServiceExceptionIfEnabled(
            new RestServiceException(String.format("No reserved metadata id present in chunk %s signed url", chunkId),
                RestServiceErrorCode.BadRequest), router.getRouterConfig().routerReservedMetadataEnabled);
      }
      if (reservedMetadataBlobId != null && !reservedMetadataBlobId.equals(chunkReservedMetadataBlobId)) {
        ReservedMetadataIdMetrics.getReservedMetadataIdMetrics(
            frontendMetrics.getMetricRegistry()).mismatchedReservedMetadataForChunkedUploadCount.inc();
        throwRestServiceExceptionIfEnabled(new RestServiceException(String.format(
                "Reserved metadata id for the chunks are not same. For chunk: %s the reserved metadata id is %s. But reserved metadata id %s was found earlier.",
                chunkId, chunkReservedMetadataBlobId, reservedMetadataBlobId), RestServiceErrorCode.BadRequest),
            router.getRouterConfig().routerReservedMetadataEnabled);
      }
      return chunkReservedMetadataBlobId;
    }

    /**
     * Create a {@link BlobProperties} for the router upload (putBlob or stitchBlob) with a finite TTL such that
     * orphaned blobs will not be created if the write to the named blob metadata DB fails.
     * @param blobInfoFromRequest the {@link BlobInfo} parsed from the request.
     * @return a {@link BlobProperties} for a TTL-ed initial router call.
     */
    BlobProperties getPropertiesForRouterUpload(BlobInfo blobInfoFromRequest) {
      BlobProperties properties;
      if (blobInfoFromRequest.getBlobProperties().getTimeToLiveInSeconds() == Utils.Infinite_Time) {
        properties = new BlobProperties(blobInfoFromRequest.getBlobProperties());
        // For blob with infinite time, the procedure is putBlob with a TTL, record insert to database with
        // infinite TTL, and ttlUpdate.
        properties.setTimeToLiveInSeconds(frontendConfig.permanentNamedBlobInitialPutTtl);
      } else {
        properties = blobInfoFromRequest.getBlobProperties();
      }
      return properties;
    }

    /**
     * Support add dataset version queries after before the named blob.
     * @param blobProperties The {@link BlobProperties} of this blob.
     * @param restRequest {@link RestRequest} representing the request.
     * @return the {@link Dataset}
     * @throws RestServiceException
     */
    private void addDatasetVersion(BlobProperties blobProperties, RestRequest restRequest) throws RestServiceException {
      long startAddDatasetVersionTime = System.currentTimeMillis();
      String accountName = null;
      String containerName = null;
      String datasetName = null;
      String version = null;
      try {
        Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
        accountName = dataset.getAccountName();
        containerName = dataset.getContainerName();
        datasetName = dataset.getDatasetName();
        version = (String) restRequest.getArgs().get(TARGET_DATASET_VERSION);
        boolean datasetVersionTtlEnabled =
            RestUtils.getBooleanHeader(restRequest.getArgs(), RestUtils.Headers.DATASET_VERSION_TTL_ENABLED, false);
        long expirationTimeMs =
            Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds());
        DatasetVersionRecord datasetVersionRecord =
            accountService.addDatasetVersion(accountName, containerName, datasetName, version,
                blobProperties.getTimeToLiveInSeconds(), blobProperties.getCreationTimeInMs(), datasetVersionTtlEnabled,
                DatasetVersionState.IN_PROGRESS);
        FrontendUtils.replaceRequestPathWithNewOperationOrBlobIdIfNeeded(restRequest, datasetVersionRecord, version);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_NAME, datasetName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_VERSION, datasetVersionRecord.getVersion());
        if (datasetVersionRecord.getExpirationTimeMs() != Utils.Infinite_Time) {
          restResponseChannel.setHeader(RestUtils.Headers.DATASET_EXPIRATION_TIME,
              new Date(datasetVersionRecord.getExpirationTimeMs()));
        }
        long newExpirationTimeMs = datasetVersionRecord.getExpirationTimeMs();
        // expirationTimeMs = ttl + creationTime. If dataset version inherit the retentionTimeInSeconds from dataset level
        // the ttl should be updated.
        if (expirationTimeMs != newExpirationTimeMs) {
          blobProperties.setTimeToLiveInSeconds(
              Utils.getTtlInSecsFromExpiryMs(newExpirationTimeMs, blobProperties.getCreationTimeInMs()));
        }
        frontendMetrics.addDatasetVersionProcessingTimeInMs.update(
            System.currentTimeMillis() - startAddDatasetVersionTime);
      } catch (AccountServiceException ex) {
        LOGGER.error(
            "Dataset version create failed for accountName: " + accountName + " containerName: " + containerName
                + " datasetName: " + datasetName + " version: " + version);
        frontendMetrics.addDatasetVersionError.inc();
        throw new RestServiceException(ex.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }

    /**
     * Support delete the dataset version out of retentionCount
     */
    private void updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount(Callback<Void> callback)
        throws RestServiceException {
      Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
      String accountName = dataset.getAccountName();
      String containerName = dataset.getContainerName();
      String datasetName = dataset.getDatasetName();
      String targetVersion = (String) restResponseChannel.getHeader(Headers.TARGET_DATASET_VERSION);
      try {
        accountService.updateDatasetVersionState(accountName, containerName, datasetName, targetVersion,
            DatasetVersionState.READY);
      } catch (AccountServiceException ex) {
        LOGGER.error(
            "Dataset version update failed for accountName: " + accountName + " containerName: " + containerName
                + " datasetName: " + datasetName + " version: " + targetVersion);
        throw new RestServiceException(ex.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
      try {
        List<DatasetVersionRecord> datasetVersionRecordList =
            accountService.getAllValidVersionsOutOfRetentionCount(accountName, containerName, datasetName);
        if (datasetVersionRecordList.size() > 0) {
          WrappedRestRequest wrappedRestRequest = new WrappedRestRequest(restRequest);
          LOGGER.info(
              "Number of records need to be deleted due to out of retention count: " + datasetVersionRecordList.size()
                  + " for accountName: " + accountName + " containerName: " + containerName + " datasetName: "
                  + datasetName);
          recursiveDeleteDatasetVersions(wrappedRestRequest, datasetVersionRecordList, 0, callback);
        } else {
          callback.onCompletion(null, null);
        }
      } catch (Exception e) {
        frontendMetrics.deleteDatasetVersionOutOfRetentionError.inc();
        LOGGER.error("Failed to delete dataset version out of retention count for this dataset: " + dataset, e);
        //We don't want the exception to be submitted to response.
        callback.onCompletion(null, null);
      }
    }

    /**
     * Recursively delete the dataset version out of retention count at dataset level.
     * @param datasetVersionRecordList the list of the {@link DatasetVersionRecord}
     * @param idx the index of the datasetVersionRecordList
     * @param restRequest the {@link RestRequest}
     * @param processResponseCallback the call back to process response.
     */
    private void recursiveDeleteDatasetVersions(RestRequest restRequest,
        List<DatasetVersionRecord> datasetVersionRecordList, int idx, Callback<Void> processResponseCallback)
        throws Exception {
      if (idx == datasetVersionRecordList.size()) {
        LOGGER.debug("Complete recursive callback for " + idx + " number of dataset version");
        processResponseCallback.onCompletion(null, null);
        return;
      }
      Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
      String accountName = dataset.getAccountName();
      String containerName = dataset.getContainerName();
      String datasetName = dataset.getDatasetName();
      DatasetVersionRecord record = datasetVersionRecordList.get(idx);
      String version = record.getVersion();
      RequestPath requestPath = getRequestPath(restRequest);
      RequestPath newRequestPath =
          new RequestPath(requestPath.getPrefix(), requestPath.getClusterName(), requestPath.getPathAfterPrefixes(),
              NAMED_BLOB_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + datasetName + SLASH + version,
              requestPath.getSubResource(), requestPath.getBlobSegmentIdx());
      LOGGER.debug("New request path : " + newRequestPath);
      // Replace RequestPath in the WrappedRestRequest for delete and call DeleteBlobHandler.handle.
      restRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);
      restRequest.setArg(InternalKeys.TARGET_ACCOUNT_KEY, null);
      restRequest.setArg(InternalKeys.TARGET_CONTAINER_KEY, null);
      //for delete out of retention request, we don't want to set anything to response channel.
      deleteBlobHandler.handle(restRequest, new NoOpResponseChannel(), (r, e) -> {
        if (e != null) {
          frontendMetrics.deleteDatasetVersionOutOfRetentionError.inc();
          LOGGER.error(
              "Failed to delete dataset version due to " + getRequestPath(restRequest) + " failed with exception" + e);
        }
        try {
          recursiveDeleteDatasetVersions(restRequest, datasetVersionRecordList, idx + 1, processResponseCallback);
        } catch (Exception exc) {
          frontendMetrics.deleteDatasetVersionOutOfRetentionError.inc();
          LOGGER.error(
              "Failed to delete dataset version" + getRequestPath(restRequest) + " failed with exception" + exc);
          processResponseCallback.onCompletion(null, null);
        }
      });
    }

    /**
     * When upload named blob failed, we take the best effort to delete the dataset version which create before uploading.
     * @param callback the final callback which submit the response.
     */
    private <T> Callback<T> deleteDatasetVersionIfUploadFailedCallBack(Callback<T> callback) {
      return (r, e) -> {
        try {
          if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
            frontendMetrics.deleteDatasetVersionIfUploadFailCount.inc();
            Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
            String version = (String) restResponseChannel.getHeader(Headers.TARGET_DATASET_VERSION);
            accountService.deleteDatasetVersion(dataset.getAccountName(), dataset.getContainerName(),
                dataset.getDatasetName(), version);
          }
        } catch (Exception ex) {
          LOGGER.error("Best effort to delete the dataset version when upload failed");
          frontendMetrics.deleteDatasetVersionError.inc();
        }
        if (callback != null) {
          callback.onCompletion(r, e);
        }
      };
    }
  }
}
