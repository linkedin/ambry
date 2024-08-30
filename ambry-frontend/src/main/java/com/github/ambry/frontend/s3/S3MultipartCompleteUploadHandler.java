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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.commons.RetryExecutor;
import com.github.ambry.commons.RetryPolicies;
import com.github.ambry.commons.RetryPolicy;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.frontend.AccountAndContainerInjector;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.IdConverter;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.SecurityService;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;
import static com.github.ambry.router.RouterErrorCode.*;
import static com.github.ambry.frontend.s3.S3MessagePayload.*;


/**
 * Handles a request for s3 Complete/AbortMultipartUploads according to the
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">...</a>
 * TODO [S3] Add support for Abort multipart uploads.
 */
public class S3MultipartCompleteUploadHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3MultipartCompleteUploadHandler.class);
  private static final ObjectMapper objectMapper = new XmlMapper();
  private final SecurityService securityService;
  private final FrontendMetrics frontendMetrics;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendConfig frontendConfig;
  private final Router router;
  private final IdConverter idConverter;
  private final QuotaManager quotaManager;
  private final RetryExecutor retryExecutor = new RetryExecutor(Executors.newScheduledThreadPool(2));
  private final Set<RouterErrorCode> retriableRouterError =
      EnumSet.of(AmbryUnavailable, ChannelClosed, UnexpectedInternalError, OperationTimedOut);
  private final RetryPolicy retryPolicy = RetryPolicies.defaultPolicy();
  private final NamedBlobDb namedBlobDb;

  /**
   * Construct a handler for handling S3 POST requests during multipart uploads.
   * @param securityService             the {@link SecurityService} to use.
   * @param namedBlobDb                 the {@link NamedBlobDb} to use.
   * @param idConverter                 the {@link IdConverter} to use.
   * @param router                      the {@link Router} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param frontendMetrics             {@link FrontendMetrics} instance where metrics should be recorded.
   * @param frontendConfig              the {@link FrontendConfig} to use.
   * @param quotaManager                The {@link QuotaManager} class to account for quota usage in serving requests.
   */
  public S3MultipartCompleteUploadHandler(SecurityService securityService, NamedBlobDb namedBlobDb,
      IdConverter idConverter, Router router, AccountAndContainerInjector accountAndContainerInjector,
      FrontendMetrics frontendMetrics, FrontendConfig frontendConfig, QuotaManager quotaManager) {
    this.securityService = securityService;
    this.frontendMetrics = frontendMetrics;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.frontendConfig = frontendConfig;
    this.router = router;
    this.idConverter = idConverter;
    this.quotaManager = quotaManager;
    this.namedBlobDb = namedBlobDb;
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    new S3MultipartCompleteUploadHandler.CallbackChain(restRequest, restResponseChannel, callback).start();
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
      try {
        accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest,
            frontendMetrics.postBlobMetricsGroup);
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
        if (frontendConfig.oneHundredContinueEnable && CONTINUE.equals(restRequest.getArgs().get(EXPECT))) {
          restResponseChannel.setStatus(ResponseStatus.Continue);
          //We need to set the content length in order to be a full http response in NettyResponseChannel::maybeWriteResponseMetadata.
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
          finalCallback.onCompletion(null, null);
        } else {
          // 1. Add headers required by Ambry. These become the blob properties.
          NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
          String accountName = namedBlobPath.getAccountName();
          restRequest.setArg(Headers.SERVICE_ID, accountName);
          //Setting default content-type if not provide by S3 client
          if (restRequest.getArgs().get(Headers.CONTENT_TYPE) == null) {
            restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, RestUtils.OCTET_STREAM_CONTENT_TYPE);
          } else {
            restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, restRequest.getArgs().get(Headers.CONTENT_TYPE));
          }
          restRequest.setArg(Headers.AMBRY_CONTENT_ENCODING, restRequest.getArgs().get(Headers.CONTENT_ENCODING));
          BlobInfo blobInfo = getBlobInfoFromRequest();
          securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback(blobInfo));
        }
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, return response for the request.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.putSecurityPostProcessRequestMetrics, securityCheckResult -> {
        RetainingAsyncWritableChannel channel =
            new RetainingAsyncWritableChannel(frontendConfig.maxJsonRequestSizeBytes);
        restRequest.readInto(channel, fetchStitchRequestBodyCallback(channel, blobInfo));
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After reading the body of the stitch request, parse the request body,
     * and make a call to {@link Router#stitchBlob}.
     * @param channel the {@link RetainingAsyncWritableChannel} that will contain the request body.
     * @param blobInfo the {@link BlobInfo} to make the router call with.
     * @return a {@link Callback} to be used with {@link RestRequest#readInto}.
     */
    private Callback<Long> fetchStitchRequestBodyCallback(RetainingAsyncWritableChannel channel, BlobInfo blobInfo) {
      return buildCallback(frontendMetrics.putReadStitchRequestMetrics, bytesRead -> {
        PutBlobOptions options = new PutBlobOptionsBuilder().restRequest(restRequest).build();
        BlobProperties propertiesForRouterUpload = getPropertiesForRouterUpload(blobInfo);
        router.stitchBlob(propertiesForRouterUpload, blobInfo.getUserMetadata(),
            getChunksToStitch(parseCompleteMultipartUploadBody(channel)), options,
            routerStitchBlobCallback(blobInfo, propertiesForRouterUpload),
            QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, true));
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link Router#putBlob} finishes, call {@link IdConverter#convert} to convert the returned ID into a format
     * that will be returned to the "ETag" header.
     * @param blobInfo                       the {@link BlobInfo} to use for security checks.
     * @param propertiesPassedInRouterUpload the {@link BlobProperties} instance that is passed to Router during upload
     * @return a {@link Callback} to be used with {@link Router#putBlob}.
     */
    private Callback<String> routerStitchBlobCallback(BlobInfo blobInfo,
        BlobProperties propertiesPassedInRouterUpload) {
      return buildCallback(frontendMetrics.putRouterStitchBlobMetrics, blobId -> {
        // The actual blob size is now present in the instance of BlobProperties passed to the router.stitchBlob().
        // Update it in the BlobInfo so that IdConverter can add it to the named blob DB
        blobInfo.getBlobProperties().setBlobSize(propertiesPassedInRouterUpload.getBlobSize());
        idConverter.convert(restRequest, blobId, blobInfo, idConverterCallback(blobInfo, blobId));
      }, uri, LOGGER, finalCallback);
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
        restResponseChannel.setHeader(Headers.LOCATION, convertedBlobId);
        // Do ttl update with retryExecutor. Use the blob ID returned from the router instead of the converted ID
        // since the converted ID may be changed by the ID converter.
        String serviceId = blobInfo.getBlobProperties().getServiceId();
        retryExecutor.runWithRetries(retryPolicy,
            callback -> router.updateBlobTtl(blobId, serviceId, Utils.Infinite_Time, callback,
                QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false)), this::isRetriable,
            routerTtlUpdateCallback(blobInfo, blobId));
      }, uri, LOGGER, finalCallback);
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
        if (!restRequest.getArgs().containsKey(NAMED_BLOB_VERSION)) {
          throw new RestServiceException(
              "Internal key " + NAMED_BLOB_VERSION + " is required in Named Blob TTL update callback!",
              RestServiceErrorCode.InternalServerError);
        }
        long namedBlobVersion = (long) restRequest.getArgs().get(NAMED_BLOB_VERSION);
        String blobIdClean = stripSlashAndExtensionFromId(blobId);
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
        NamedBlobRecord record = new NamedBlobRecord(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
            namedBlobPath.getBlobName(), blobIdClean, Utils.Infinite_Time, namedBlobVersion);
        namedBlobDb.updateBlobTtlAndStateToReady(record).get();
        securityService.processResponse(restRequest, restResponseChannel, blobInfo, securityProcessResponseCallback());
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback() {
      return buildCallback(frontendMetrics.putBlobSecurityProcessResponseMetrics, securityCheckResult -> {
        // Create xml response to send to user
        RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(requestPath, restRequest.getArgs());
        String bucket = namedBlobPath.getContainerName();
        String key = namedBlobPath.getBlobName();
        String eTag = (String) restResponseChannel.getHeader(Headers.LOCATION);
        String s3Location = restRequest.getPath();
        CompleteMultipartUploadResult completeMultipartUploadResult =
            new CompleteMultipartUploadResult(bucket, key, s3Location, eTag);
        LOGGER.debug("Sending response for Multipart upload complete {}", completeMultipartUploadResult);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectMapper objectMapper = new XmlMapper();
        objectMapper.writeValue(outputStream, completeMultipartUploadResult);
        ReadableStreamChannel channel =
            new ByteBufferReadableStreamChannel(ByteBuffer.wrap(outputStream.toByteArray()));
        restResponseChannel.setStatus(ResponseStatus.Ok);
        restResponseChannel.setHeader(Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(Headers.CONTENT_TYPE, XML_CONTENT_TYPE);
        restResponseChannel.setHeader(Headers.CONTENT_LENGTH, channel.getSize());
        finalCallback.onCompletion(channel, null);
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    private CompleteMultipartUpload parseCompleteMultipartUploadBody(RetainingAsyncWritableChannel channel)
        throws RestServiceException {
      CompleteMultipartUpload completeMultipartUpload;
      // TODO [s3] sanity check CompleteMultipartUpload, the part number has to be integer.
      try (InputStream inputStream = channel.consumeContentAsInputStream()) {
        completeMultipartUpload = objectMapper.readValue(inputStream, CompleteMultipartUpload.class);
        LOGGER.debug("Parsed CompleteMultipartUpload request {}", completeMultipartUpload);
      } catch (Exception e) {
        throw new RestServiceException("Could not parse xml request body", e, RestServiceErrorCode.BadRequest);
      }
      return completeMultipartUpload;
    }

    /**
     * Parse {@link BlobInfo} from the request arguments. This method will also ensure that the correct account and
     * container objects are attached to the request.
     * @return the {@link BlobInfo} parsed from the request arguments.
     * @throws RestServiceException if there is an error while parsing the {@link BlobInfo} arguments.
     */
    private BlobInfo getBlobInfoFromRequest() throws RestServiceException {
      long propsBuildStartTime = System.currentTimeMillis();
      BlobProperties blobProperties = buildBlobProperties(restRequest.getArgs());
      frontendMetrics.blobPropsBuildForNameBlobPutTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      LOGGER.trace("Blob properties of blob being PUT - {}", blobProperties);
      return new BlobInfo(blobProperties, ByteBuffer.allocate(0).array());
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
     * @param throwable the error to check.
     * @return true if the router error is retriable.
     */
    private boolean isRetriable(Throwable throwable) {
      return throwable instanceof RouterException && retriableRouterError.contains(
          ((RouterException) throwable).getErrorCode());
    }

    /**
     * Parse and verify the part IDs in the body of a CompleteMultipartUpload request.
     * @param completeMultipartUpload {@link CompleteMultipartUpload} request
     * @return a list of chunks to stitch that can be provided to the router.
     */
    List<ChunkInfo> getChunksToStitch(CompleteMultipartUpload completeMultipartUpload) throws RestServiceException {
      // Get parts in order from CompleteMultipartUpload, deserialize each part id to get data chunk ids.
      List<ChunkInfo> chunkInfos = new ArrayList<>();
      try {
        // sort the list in order
        List<Part> sortedParts = Arrays.asList(completeMultipartUpload.getPart());
        Collections.sort(sortedParts, Comparator.comparingInt(Part::getPartNumber));
        String reservedMetadataId = null;
        for (Part part : sortedParts) {
          S3MultipartETag eTag = S3MultipartETag.deserialize(part.geteTag());
          // TODO [S3]: decide the life cycle of S3.
          long expirationTimeInMs = -1;

          if (eTag.getVersion() == S3MultipartETag.VERSION_1) {
            List<Pair<String, Long>> chunks = eTag.getOrderedChunkIdSizeList();
            for (int i = 0; i < chunks.size(); i++) {
              String blobId = chunks.get(i).getFirst();
              long chunkSize = chunks.get(i).getSecond();

              ChunkInfo chunk = new ChunkInfo(blobId, chunkSize, expirationTimeInMs, reservedMetadataId);
              chunkInfos.add(chunk);
            }
          } else {
            String error = "Wrong ETag version " + completeMultipartUpload + " rest request " + restRequest;
            LOGGER.error(error);
            throw new RestServiceException(error, RestServiceErrorCode.BadRequest);
          }
        }
      } catch (IOException e) {
        String error = "Could not parse xml request body " + completeMultipartUpload + " rest request " + restRequest;
        LOGGER.error(error);
        throw new RestServiceException(error, e, RestServiceErrorCode.BadRequest);
      }
      return chunkInfos;
    }
  }
}
