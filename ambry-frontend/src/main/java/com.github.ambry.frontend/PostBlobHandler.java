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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.utils.ThrowingConsumer;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler for post blob requests.
 */
class PostBlobHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostBlobHandler.class);

  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics frontendMetrics;
  private final Router router;

  /**
   * Constructs a handler for handling requests for signed URLs.
   * @param securityService the {@link SecurityService} to use.
   * @param idConverter the {@link IdConverter} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param frontendMetrics {@link FrontendMetrics} instance where frontendMetrics should be recorded.
   * @param router the {@link Router} to use.
   */
  PostBlobHandler(SecurityService securityService, IdConverter idConverter,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics frontendMetrics, Router router) {
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.frontendMetrics = frontendMetrics;
    this.router = router;
  }

  /**
   * Asynchronously post a blob.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    // Metrics initialization. Can potentially be updated after parsing blob properties.
    restRequest.getMetricsTracker()
        .injectMetrics(frontendMetrics.postRequestMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false));
    // Start the callback chain by performing request security pre-processing.
    securityService.preProcessRequest(restRequest,
        securityPreProcessRequestCallback(restRequest, restResponseChannel, callback));
  }

  /**
   * Parse {@link BlobInfo} from the request arguments. This method will also ensure that the correct account and
   * container objects are attached to the request.
   * @param restRequest the {@link RestRequest}
   * @return the {@link BlobInfo} parsed from the request arguments.
   * @throws RestServiceException if there is an error while parsing the {@link BlobInfo} arguments.
   */
  private BlobInfo parseBlobInfoFromRequest(RestRequest restRequest) throws RestServiceException {
    long propsBuildStartTime = System.currentTimeMillis();
    accountAndContainerInjector.injectAccountAndContainerForPostRequest(restRequest);
    BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
    if (blobProperties.getTimeToLiveInSeconds() + TimeUnit.MILLISECONDS.toSeconds(blobProperties.getCreationTimeInMs())
        > Integer.MAX_VALUE) {
      LOGGER.debug("TTL set to very large value in POST request with BlobProperties {}", blobProperties);
      frontendMetrics.ttlTooLargeError.inc();
    }
    // inject encryption frontendMetrics if applicable
    if (blobProperties.isEncrypted()) {
      restRequest.getMetricsTracker()
          .injectMetrics(frontendMetrics.postRequestMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true));
    }
    byte[] userMetadata = RestUtils.buildUserMetadata(restRequest.getArgs());
    frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
    LOGGER.trace("Blob properties of blob being POSTed - {}", blobProperties);
    return new BlobInfo(blobProperties, userMetadata);
  }

  /**
   * After {@link SecurityService#preProcessRequest} finishes, parse the blob info headers in the request and call
   * {@link SecurityService#processRequest} to perform additional request time security checks.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param finalCallback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @return a {@link Callback} to be used with {@link SecurityService#preProcessRequest}.
   */
  private Callback<Void> securityPreProcessRequestCallback(RestRequest restRequest,
      RestResponseChannel restResponseChannel, Callback<ReadableStreamChannel> finalCallback) {
    CallbackTracker callbackTracker =
        new CallbackTracker(restRequest, LOGGER, frontendMetrics.postSecurityPreProcessRequestMetrics);
    ThrowingConsumer<Void> successAction = securityCheckResult -> {
      BlobInfo blobInfo = parseBlobInfoFromRequest(restRequest);
      securityService.processRequest(restRequest,
          securityProcessRequestCallback(restRequest, restResponseChannel, blobInfo, finalCallback));
    };
    return FrontendUtils.managedCallback(callbackTracker, finalCallback, successAction);
  }

  /**
   * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
   * request time security checks that rely on the request being fully parsed and any additional arguments set.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param blobInfo the {@link BlobInfo} to carry to future stages.
   * @param finalCallback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
   */
  private Callback<Void> securityProcessRequestCallback(RestRequest restRequest,
      RestResponseChannel restResponseChannel, BlobInfo blobInfo, Callback<ReadableStreamChannel> finalCallback) {
    CallbackTracker callbackTracker =
        new CallbackTracker(restRequest, LOGGER, frontendMetrics.postSecurityProcessRequestMetrics);
    ThrowingConsumer<Void> successAction = securityCheckResult -> securityService.postProcessRequest(restRequest,
        securityPostProcessRequestCallback(restRequest, restResponseChannel, blobInfo, finalCallback));
    return FrontendUtils.managedCallback(callbackTracker, finalCallback, successAction);
  }

  /**
   * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#putBlob} to persist the blob in the
   * storage layer.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param blobInfo the {@link BlobInfo} to make the router call with.
   * @param finalCallback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
   */
  private Callback<Void> securityPostProcessRequestCallback(RestRequest restRequest,
      RestResponseChannel restResponseChannel, BlobInfo blobInfo, Callback<ReadableStreamChannel> finalCallback) {
    CallbackTracker callbackTracker =
        new CallbackTracker(restRequest, LOGGER, frontendMetrics.postSecurityPostProcessRequestMetrics);
    ThrowingConsumer<Void> successAction =
        securityCheckResult -> router.putBlob(blobInfo.getBlobProperties(), blobInfo.getUserMetadata(), restRequest,
            routerPutBlobCallback(restRequest, restResponseChannel, blobInfo, finalCallback));
    return FrontendUtils.managedCallback(callbackTracker, finalCallback, successAction);
  }

  /**
   * After {@link Router#putBlob} finishes, call {@link IdConverter#convert} to convert the returned ID into a format
   * that will be returned in the "Location" header.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param blobInfo the {@link BlobInfo} to make the router call with.
   * @param finalCallback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @return a {@link Callback} to be used with {@link Router#putBlob}.
   */
  private Callback<String> routerPutBlobCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo, Callback<ReadableStreamChannel> finalCallback) {
    CallbackTracker callbackTracker =
        new CallbackTracker(restRequest, LOGGER, frontendMetrics.postRouterPutBlobMetrics);
    ThrowingConsumer<String> successAction = blobId -> idConverter.convert(restRequest, blobId,
        idConverterCallback(restRequest, restResponseChannel, blobInfo, finalCallback));
    return FrontendUtils.managedCallback(callbackTracker, finalCallback, successAction);
  }

  /**
   * After {@link IdConverter#convert} finishes, set the "Location" header and call
   * {@link SecurityService#processResponse}.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param blobInfo the {@link BlobInfo} to use for security checks.
   * @param finalCallback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
   */
  private Callback<String> idConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobInfo blobInfo, Callback<ReadableStreamChannel> finalCallback) {
    CallbackTracker callbackTracker = new CallbackTracker(restRequest, LOGGER, frontendMetrics.postIdConversionMetrics);
    ThrowingConsumer<String> successAction = convertedBlobId -> {
      restResponseChannel.setHeader(RestUtils.Headers.LOCATION, convertedBlobId);
      securityService.processResponse(restRequest, restResponseChannel, blobInfo,
          securityProcessResponseCallback(restRequest, finalCallback));
    };
    return FrontendUtils.managedCallback(callbackTracker, finalCallback, successAction);
  }

  /**
   * After {@link SecurityService#processResponse}, call the provided callback.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param finalCallback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
   */
  private Callback<Void> securityProcessResponseCallback(RestRequest restRequest,
      Callback<ReadableStreamChannel> finalCallback) {
    CallbackTracker callbackTracker =
        new CallbackTracker(restRequest, LOGGER, frontendMetrics.postSecurityProcessResponseMetrics);
    ThrowingConsumer<Void> successAction = securityCheckResult -> finalCallback.onCompletion(null, null);
    return FrontendUtils.managedCallback(callbackTracker, finalCallback, successAction);
  }
}
