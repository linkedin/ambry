/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.Container;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


/**
 * Handler for post blob requests. The following request types are handled by {@link PostBlobHandler}:
 * <h2>Direct uploads</h2>
 * Direct upload requests treat the body of the request as the content to upload to Ambry. The request path should be
 * "/". In these requests, the blob properties and user metadata are supplied as headers. See
 * {@link RestUtils#buildBlobProperties(Map)} and {@link RestUtils#buildBlobProperties(Map)} for more details.
 * <h2>Stitched uploads</h2>
 * Stitched upload requests allow clients to stitch together previously uploaded data chunks into a single logical blob.
 * The request path should be "/stitch" ({@link Operations#STITCH}). This request accepts the same headers as direct
 * upload requests for supplying the blob properties and user metadata of the stitched blob, but, instead of the actual
 * blob content, accepts a UTF-8 JSON object that includes the signed IDs for the chunks to stitch.
 * <h3>Request body format</h3>
 * The body of the request should be a JSON object that conforms to the format described in {@link StitchRequestSerDe}.
 */
class PostBlobHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostBlobHandler.class);
  /**
   * Key to represent the time at which a blob will expire in ms. Used within the metadata map in signed IDs.
   */
  static final String EXPIRATION_TIME_MS_KEY = "et";

  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final IdSigningService idSigningService;
  private final Router router;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final Time time;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final String clusterName;

  /**
   * Constructs a handler for handling requests for uploading or stitching blobs.
   * @param securityService the {@link SecurityService} to use.
   * @param idConverter the {@link IdConverter} to use.
   * @param idSigningService the {@link IdSigningService} to use.
   * @param router the {@link Router} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param time the {@link Time} instance to use.
   * @param frontendConfig the {@link FrontendConfig} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param clusterName the name of the storage cluster that the router communicates with
   */
  PostBlobHandler(SecurityService securityService, IdConverter idConverter, IdSigningService idSigningService,
      Router router, AccountAndContainerInjector accountAndContainerInjector, Time time, FrontendConfig frontendConfig,
      FrontendMetrics frontendMetrics, String clusterName) {
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.idSigningService = idSigningService;
    this.router = router;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.time = time;
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.clusterName = clusterName;
  }

  /**
   * Asynchronously post a blob.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final String uri;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> finalCallback;

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
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by parsing the blob info headers in the request and calling
     * {@link SecurityService#processRequest} to perform additional request time security checks.
     */
    private void start() {
      // Metrics initialization. Can potentially be updated after parsing blob properties.
      restRequest.getMetricsTracker()
          .injectMetrics(frontendMetrics.postBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false));
      try {
        // Start the callback chain by parsing blob info headers and performing request security processing.
        BlobInfo blobInfo = getBlobInfoFromRequest();
        checkUploadRequirements(blobInfo.getBlobProperties());
        securityService.processRequest(restRequest, securityProcessRequestCallback(blobInfo));
      } catch (Exception e) {
        finalCallback.onCompletion(null, e);
      }
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @param blobInfo the {@link BlobInfo} to carry to future stages.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.postSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest,
              securityPostProcessRequestCallback(blobInfo)), uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#putBlob} to persist the blob in the
     * storage layer.
     * @param blobInfo the {@link BlobInfo} to make the router call with.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.postSecurityPostProcessRequestMetrics, securityCheckResult -> {
        if (RestUtils.getRequestPath(restRequest).matchesOperation(Operations.STITCH)) {
          RetainingAsyncWritableChannel channel =
              new RetainingAsyncWritableChannel(frontendConfig.maxJsonRequestSizeBytes);
          restRequest.readInto(channel, fetchStitchRequestBodyCallback(channel, blobInfo));
        } else {
          PutBlobOptions options = getPutBlobOptionsFromRequest();
          router.putBlob(blobInfo.getBlobProperties(), blobInfo.getUserMetadata(), restRequest, options,
              routerPutBlobCallback(blobInfo));
        }
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After reading the body of the stitch request
     * @param channel
     * @param blobInfo
     * @return
     */
    private Callback<Long> fetchStitchRequestBodyCallback(RetainingAsyncWritableChannel channel, BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.postReadStitchRequestMetrics,
          bytesRead -> router.stitchBlob(blobInfo.getBlobProperties(), blobInfo.getUserMetadata(),
              getChunksToStitch(blobInfo.getBlobProperties(), readJsonFromChannel(channel)),
              routerStitchBlobCallback(blobInfo)), uri, LOGGER, finalCallback);
    }

    /**
     * After {@link Router#putBlob} finishes, call {@link IdConverter#convert} to convert the returned ID into a format
     * that will be returned in the "Location" header.
     * @param blobInfo the {@link BlobInfo} to make the router call with.
     * @return a {@link Callback} to be used with {@link Router#putBlob}.
     */
    private Callback<String> routerStitchBlobCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.postRouterStitchBlobMetrics,
          blobId -> idConverter.convert(restRequest, blobId, idConverterCallback(blobInfo)), uri, LOGGER,
          finalCallback);
    }

    /**
     * After {@link Router#putBlob} finishes, call {@link IdConverter#convert} to convert the returned ID into a format
     * that will be returned in the "Location" header.
     * @param blobInfo the {@link BlobInfo} to make the router call with.
     * @return a {@link Callback} to be used with {@link Router#putBlob}.
     */
    private Callback<String> routerPutBlobCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.postRouterPutBlobMetrics, blobId -> {
        setSignedIdMetadataAndBlobSize(blobInfo.getBlobProperties());
        idConverter.convert(restRequest, blobId, idConverterCallback(blobInfo));
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link IdConverter#convert} finishes, set the "Location" header and call
     * {@link SecurityService#processResponse}.
     * @param blobInfo the {@link BlobInfo} to use for security checks.
     * @return a {@link Callback} to be used with {@link IdConverter#convert}.
     */
    private Callback<String> idConverterCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.postIdConversionMetrics, convertedBlobId -> {
        restResponseChannel.setHeader(RestUtils.Headers.LOCATION, convertedBlobId);
        securityService.processResponse(restRequest, restResponseChannel, blobInfo, securityProcessResponseCallback());
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback() {
      return buildCallback(frontendMetrics.postSecurityProcessResponseMetrics,
          securityCheckResult -> finalCallback.onCompletion(null, null), uri, LOGGER, finalCallback);
    }

    /**
     * Parse {@link BlobInfo} from the request arguments. This method will also ensure that the correct account and
     * container objects are attached to the request.
     * @return the {@link BlobInfo} parsed from the request arguments.
     * @throws RestServiceException if there is an error while parsing the {@link BlobInfo} arguments.
     */
    private BlobInfo getBlobInfoFromRequest() throws RestServiceException {
      long propsBuildStartTime = System.currentTimeMillis();
      accountAndContainerInjector.injectAccountAndContainerForPostRequest(restRequest,
          frontendMetrics.postBlobMetricsGroup);
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      if (blobProperties.getTimeToLiveInSeconds() + TimeUnit.MILLISECONDS.toSeconds(
          blobProperties.getCreationTimeInMs()) > Integer.MAX_VALUE) {
        LOGGER.debug("TTL set to very large value in POST request with BlobProperties {}", blobProperties);
        frontendMetrics.ttlTooLargeError.inc();
      } else if (container.isTtlRequired() && (blobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time
          || blobProperties.getTimeToLiveInSeconds() > frontendConfig.maxAcceptableTtlSecsIfTtlRequired)) {
        String descriptor = RestUtils.getAccountFromArgs(restRequest.getArgs()).getName() + ":" + container.getName();
        if (frontendConfig.failIfTtlRequiredButNotProvided) {
          throw new RestServiceException(
              "TTL < " + frontendConfig.maxAcceptableTtlSecsIfTtlRequired + " is required for upload to " + descriptor,
              RestServiceErrorCode.InvalidArgs);
        } else {
          LOGGER.debug(
              blobProperties.getServiceId() + " attempted an upload with ttl " + blobProperties.getTimeToLiveInSeconds()
                  + " to " + descriptor);
          frontendMetrics.ttlNotCompliantError.inc();
          restResponseChannel.setHeader(RestUtils.Headers.NON_COMPLIANCE_WARNING,
              "TTL < " + frontendConfig.maxAcceptableTtlSecsIfTtlRequired + " will be required for future uploads");
        }
      }
      // inject encryption frontendMetrics if applicable
      if (blobProperties.isEncrypted()) {
        restRequest.getMetricsTracker()
            .injectMetrics(frontendMetrics.postBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true));
      }
      byte[] userMetadata = RestUtils.buildUserMetadata(restRequest.getArgs());
      frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      LOGGER.trace("Blob properties of blob being POSTed - {}", blobProperties);
      return new BlobInfo(blobProperties, userMetadata);
    }

    /**
     * @return the {@link PutBlobOptions} to use, parsed from the request.
     */
    private PutBlobOptions getPutBlobOptionsFromRequest() throws RestServiceException {
      PutBlobOptionsBuilder builder =
          new PutBlobOptionsBuilder().chunkUpload(RestUtils.isChunkUpload(restRequest.getArgs()));
      Long maxUploadSize = RestUtils.getLongHeader(restRequest.getArgs(), RestUtils.Headers.MAX_UPLOAD_SIZE, false);
      if (maxUploadSize != null) {
        builder.maxUploadSize(maxUploadSize);
      }
      return builder.build();
    }

    /**
     * Attach the metadata to include in a signed ID to the {@link RestRequest} if the request is for a chunk upload.
     * This will tell the ID converter that it needs to produce a signed ID to give back to the client.
     * @param blobProperties the {@link BlobProperties} from the request.
     * @throws RestServiceException
     */
    private void setSignedIdMetadataAndBlobSize(BlobProperties blobProperties) throws RestServiceException {
      if (RestUtils.isChunkUpload(restRequest.getArgs())) {
        Map<String, String> metadata = new HashMap<>(2);
        metadata.put(RestUtils.Headers.BLOB_SIZE, Long.toString(restRequest.getBlobBytesReceived()));
        metadata.put(RestUtils.Headers.SESSION,
            RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.SESSION, true));
        metadata.put(EXPIRATION_TIME_MS_KEY,
            Long.toString(Utils.addSecondsToEpochTime(time.milliseconds(), blobProperties.getTimeToLiveInSeconds())));
        restRequest.setArg(RestUtils.InternalKeys.SIGNED_ID_METADATA_KEY, metadata);
      }
      //the actual blob size is the number of bytes read
      restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, restRequest.getBlobBytesReceived());
    }

    /**
     * Enforce any additional requirements for certain types of uploads like data chunk uploads.
     * @param blobProperties the {@link BlobProperties} parsed from the request.
     * @throws RestServiceException
     */
    private void checkUploadRequirements(BlobProperties blobProperties) throws RestServiceException {
      if (RestUtils.isChunkUpload(restRequest.getArgs())) {
        // ensure that the x-ambry-session header is present.
        RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.SESSION, true);
        // validate that a max chunk size is set.
        RestUtils.getLongHeader(restRequest.getArgs(), RestUtils.Headers.MAX_UPLOAD_SIZE, true);
        // validate that the TTL for the chunk is set correctly.
        long chunkTtl = blobProperties.getTimeToLiveInSeconds();
        if (chunkTtl <= 0 || chunkTtl > frontendConfig.chunkUploadInitialChunkTtlSecs) {
          throw new RestServiceException("Invalid chunk upload TTL: " + chunkTtl, RestServiceErrorCode.InvalidArgs);
        }
      }
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

        expectedSession = verifyChunkUploadSession(metadata, expectedSession);
        @SuppressWarnings("ConstantConditions")
        long chunkSizeBytes = RestUtils.getLongHeader(metadata, RestUtils.Headers.BLOB_SIZE, true);

        totalStitchedBlobSize += chunkSizeBytes;
        // Expiration time is sent to the router, but not verified in this handler. The router is responsible for making
        // checks related to internal ambry requirements, like making sure that the chunks do not expire before the
        // metadata blob.
        @SuppressWarnings("ConstantConditions")
        long expirationTimeMs = RestUtils.getLongHeader(metadata, EXPIRATION_TIME_MS_KEY, true);
        verifyChunkAccountAndContainer(blobId, stitchedBlobProperties);

        chunksToStitch.add(new ChunkInfo(blobId, chunkSizeBytes, expirationTimeMs));
      }
      //the actual blob size for stitched blob is the sum of all the chunk sizes
      restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, totalStitchedBlobSize);
      return chunksToStitch;
    }

    /**
     * Verify that the session ID in the chunk metadata matches the expected session.
     * @param chunkMetadata the metadata map parsed from a signed chunk ID.
     * @param expectedSession the session that the chunk should match. This can be null for the first chunk (where any
     *                        session ID is valid).
     * @return this chunk's session ID
     * @throws RestServiceException if the chunk has a null session ID or it does not match the expected value.
     */
    private String verifyChunkUploadSession(Map<String, String> chunkMetadata, String expectedSession)
        throws RestServiceException {
      String chunkSession = RestUtils.getHeader(chunkMetadata, RestUtils.Headers.SESSION, true);
      if (expectedSession != null && !expectedSession.equals(chunkSession)) {
        throw new RestServiceException("Session IDs differ for chunks in a stitch request",
            RestServiceErrorCode.BadRequest);
      }
      return chunkSession;
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
  }
}
