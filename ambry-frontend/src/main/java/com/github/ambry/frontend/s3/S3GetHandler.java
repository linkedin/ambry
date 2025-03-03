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

import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.AccountAndContainerInjector;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.GetBlobHandler;
import com.github.ambry.frontend.SecurityService;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handles S3 Get requests.
 */
public class S3GetHandler extends S3BaseHandler<ReadableStreamChannel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3GetHandler.class);
  private final SecurityService securityService;
  private final FrontendMetrics frontendMetrics;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final S3ListHandler s3ListHandler;
  private final S3MultipartUploadHandler s3MultipartUploadHandler;
  private final GetBlobHandler getBlobHandler;
  public static final String PREFIX_PARAM_NAME = "prefix";
  public static final String EMPTY_PREFIX = "";

  /**
   * Constructs a handler for uploading s3 GET requests.
   * @param s3ListHandler S3ListHandler
   * @param s3MultipartUploadHandler S3MultipartUploadHandler
   * @param getBlobHandler GetBlobHandler
   * @param securityService the {@link SecurityService} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   */
  public S3GetHandler(S3ListHandler s3ListHandler, S3MultipartUploadHandler s3MultipartUploadHandler,
      GetBlobHandler getBlobHandler, SecurityService securityService, FrontendMetrics frontendMetrics,
      AccountAndContainerInjector accountAndContainerInjector) {
    this.securityService = securityService;
    this.frontendMetrics = frontendMetrics;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.s3ListHandler = s3ListHandler;
    this.s3MultipartUploadHandler = s3MultipartUploadHandler;
    this.getBlobHandler = getBlobHandler;
  }

  /**
   * Handles a S3 Get Request.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
    if (S3BaseHandler.isGetObjectLockConfigurationRequest(restRequest)) {
      // We don't support Bucket Object Configuration. So always return NOT_FOUND
      new DefaultCallbackChain(restRequest, securityService, frontendMetrics, accountAndContainerInjector, callback,
          "Object Lock configuration is not set.", RestServiceErrorCode.NotFound).start();
    }  else if (S3BaseHandler.isMultipartListPartRequest(restRequest)) {
      s3MultipartUploadHandler.handle(restRequest, restResponseChannel, callback);
    } else if (S3BaseHandler.isListObjectRequest(restRequest)) {
      restRequest.setArg(PREFIX_PARAM_NAME, EMPTY_PREFIX);
      restRequest.setArg(LIST_REQUEST, "true");
      s3ListHandler.handle(restRequest, restResponseChannel, callback);
    } else {
      getBlobHandler.handle(requestPath, restRequest, restResponseChannel,
          buildCallback(frontendMetrics.s3GetHandleMetrics, (r) -> {
            // Set content-length for S3 requests since S3 java client seems to be looking for it
            long contentLength = Long.parseLong((String) restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
            String contentRangeLength = RestUtils.getHeader(restRequest.getArgs(), CONTENT_RANGE_LENGTH, false);
            if (contentRangeLength != null) {
              // If is a range request, update the content length
              contentLength = Long.parseLong(contentRangeLength);
            }
            restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, contentLength);
            callback.onCompletion(r, null);
          }, restRequest.getUri(), LOGGER, callback));
    }
  }
}
