/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
 *
 */
package com.github.ambry.frontend.s3;

import com.github.ambry.account.Container;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.frontend.AccountAndContainerInjector;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.IdConverter;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.SecurityService;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handles a request for S3 Multipart upload part requests according to the
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">...</a>
 */
public class S3MultipartUploadPartHandler {
  private static final Logger logger = LoggerFactory.getLogger(S3MultipartUploadPartHandler.class);
  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final Router router;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final QuotaManager quotaManager;

  /**
   * Construct a handler for handling S3 POST requests during multipart uploads.
   * @param securityService             the {@link SecurityService} to use.
   * @param idConverter                 the {@link IdConverter} to use.
   * @param router                      the {@link Router} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param frontendConfig              the {@link FrontendConfig} to use.
   * @param frontendMetrics             {@link FrontendMetrics} instance where metrics should be recorded.
   * @param quotaManager                The {@link QuotaManager} class to account for quota usage in serving requests.
   */
  public S3MultipartUploadPartHandler(SecurityService securityService, IdConverter idConverter, Router router,
      AccountAndContainerInjector accountAndContainerInjector, FrontendConfig frontendConfig,
      FrontendMetrics frontendMetrics, QuotaManager quotaManager) {
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.router = router;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.quotaManager = quotaManager;
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
    Callback<ReadableStreamChannel> callback) throws RestServiceException {
    // 1. Set headers required by Ambry. These become the blob properties.
    NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
    String accountName = namedBlobPath.getAccountName();
    restRequest.setArg(Headers.SERVICE_ID, accountName);
    restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, restRequest.getArgs().get(Headers.CONTENT_TYPE));
    restRequest.setArg(Headers.AMBRY_CONTENT_ENCODING, restRequest.getArgs().get(Headers.CONTENT_ENCODING));

    // 2. Set the internal headers session id and chunk-upload. They are used during for multipart part uploads
    String uploadId = RestUtils.getHeader(restRequest.getArgs(), UPLOAD_ID_QUERY_PARAM, true);
    restRequest.setArg(S3_CHUNK_UPLOAD, true);
    restRequest.setArg(SESSION, uploadId);
    // TODO [S3] : set part ttl value
    // restRequest.setArg(TTL, xxx);

    restRequest.setArg(SEND_FAILURE_REASON, Boolean.TRUE);

    // TODO [S3] verifyChunkAccountAndContainer? getAndVerifyReservedMetadataBlobId?
    new S3MultipartUploadPartHandler.CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;
    private final String uri;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      restRequest.getMetricsTracker()
          .injectMetrics(frontendMetrics.putBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false));
      // Start the callback chain by parsing blob info headers and performing request security processing.
      securityService.processRequest(restRequest, securityProcessRequestCallback());
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
      }, uri, logger, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#putBlob} to persist the blob in the
     * storage layer.
     * @param blobInfo the {@link BlobInfo} to make the router call with.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.putSecurityPostProcessRequestMetrics, securityCheckResult -> {
        PutBlobOptions options = getPutBlobOptionsFromRequest();
        router.putBlob(getPropertiesForRouterUpload(blobInfo), blobInfo.getUserMetadata(), restRequest, options,
            routerPutBlobCallback(blobInfo), QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, true));
      }, uri, logger, finalCallback);
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
      }, uri, logger, finalCallback);
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
        securityService.processResponse(restRequest, restResponseChannel, blobInfo, securityProcessResponseCallback());
      }, uri, logger, finalCallback);
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback() {
      return buildCallback(frontendMetrics.putBlobSecurityProcessResponseMetrics, securityCheckResult -> {
        // Set the response status to 200 since Ambry named blob PUT has response as 201.
        if (restResponseChannel.getStatus() == ResponseStatus.Created) {
          restResponseChannel.setStatus(ResponseStatus.Ok);
        }

        // Set S3 ETag header
        String blobId = (String) restResponseChannel.getHeader(LOCATION);
        restResponseChannel.setHeader("ETag", blobId);
        finalCallback.onCompletion(null, null);
      }, restRequest.getUri(), logger, finalCallback);
    }

    /**
     * Parse {@link BlobInfo} from the request arguments. This method will also ensure that the correct account and
     * container objects are attached to the request.
     * @return the {@link BlobInfo} parsed from the request arguments.
     * @throws RestServiceException if there is an error while parsing the {@link BlobInfo} arguments.
     */
    private BlobInfo getBlobInfoFromRequest() throws RestServiceException {
      // TODO [S3] may consolidate and move it to a central place.
      long propsBuildStartTime = System.currentTimeMillis();
      accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, frontendMetrics.putBlobMetricsGroup);
      if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
        accountAndContainerInjector.injectDatasetForNamedBlob(restRequest);
      }
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      if (blobProperties.getTimeToLiveInSeconds() + TimeUnit.MILLISECONDS.toSeconds(
          blobProperties.getCreationTimeInMs()) > Integer.MAX_VALUE) {
        logger.debug("TTL set to very large value in PUT request with BlobProperties {}", blobProperties);
        frontendMetrics.ttlTooLargeError.inc();
      } else if (container.isTtlRequired() && (blobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time
          || blobProperties.getTimeToLiveInSeconds() > frontendConfig.maxAcceptableTtlSecsIfTtlRequired)) {
        String descriptor = RestUtils.getAccountFromArgs(restRequest.getArgs()).getName() + ":" + container.getName();
        if (frontendConfig.failIfTtlRequiredButNotProvided) {
          throw new RestServiceException(
              "TTL < " + frontendConfig.maxAcceptableTtlSecsIfTtlRequired + " is required for upload to " + descriptor,
              RestServiceErrorCode.InvalidArgs);
        } else {
          logger.debug("{} attempted an upload with ttl {} to {}", blobProperties.getServiceId(),
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
      byte[] userMetadata = RestUtils.buildUserMetadata(userMetadataFromRequest);
      frontendMetrics.blobPropsBuildForNameBlobPutTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      logger.trace("Blob properties of blob being PUT - {}", blobProperties);
      return new BlobInfo(blobProperties, userMetadata);
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

      // it's the s3 part upload for the multi-part request, skip creating the composite chunk.
      builder.skipCompositeChunk(true);
      return builder.build();
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
  }
}
