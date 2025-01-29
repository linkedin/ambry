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
import com.github.ambry.commons.CallbackUtils;
import com.github.ambry.frontend.AccountAndContainerInjector;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.Operations;
import com.github.ambry.frontend.SecurityService;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Base class for all S3 handle classes that provides crosscutting functionality such as
 * logging and response header handling
 *
 * @param <R> the type of the result the object in callbacks. It is {@link Void} for requests that don't
 *            require a response body, otherwise it is {@link ReadableStreamChannel}.
 */
abstract public class S3BaseHandler<R> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(S3BaseHandler.class);

  /**
   * Handles the S3 request and construct the response.
   *
   * @param restRequest the {@link RestRequest} that contains the request headers and body.
   * @param restResponseChannel the {@link RestResponseChannel} that contains the response headers and body.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  abstract protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<R> callback) throws RestServiceException;

  /**
   * Process the request and construct the response, internally the invocation is delegated
   * to method {@link #doHandle(RestRequest, RestResponseChannel, Callback)}, which is
   * implemented by subclasses.
   *
   * @param restRequest the {@link RestRequest} that contains the request headers and body.
   * @param restResponseChannel the {@link RestResponseChannel} that contains the response headers and body.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  public void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<R> callback) {
    try {
      String path = ((RequestPath) restRequest.getArgs().get(REQUEST_PATH)).getOperationOrBlobId(true);
      if (!path.startsWith(Operations.NAMED_BLOB)) {
        throw new RuntimeException("S3 request handler can only handle named blob requests");
      }

      LOGGER.debug("{} {}", restRequest.getRestMethod(), path);

      doHandle(restRequest, restResponseChannel, CallbackUtils.chainCallback(callback, (r) -> {
        removeAmbryHeaders(restResponseChannel);
        callback.onCompletion(r, null);
      }));
    } catch (Throwable t) {
      Exception e = t instanceof Exception ? (Exception) t
          : new RestServiceException(t, RestServiceErrorCode.InternalServerError);
      callback.onCompletion(null, e);
    }
  }

  private void removeAmbryHeaders(RestResponseChannel restResponseChannel) {
    restResponseChannel.getHeaders()
        .stream()
        .filter(header -> header.startsWith(AMBRY_HEADER_PREFIX))
        .forEach(restResponseChannel::removeHeader);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a request to list parts of a completed multipart upload request.
   */
  public static boolean isListObjectRequest(RestRequest restRequest) throws RestServiceException {
    RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
    return restRequest.getRestMethod() == RestMethod.GET && restRequest.getArgs().containsKey(S3_REQUEST)
        && NamedBlobPath.parse(requestPath, restRequest.getArgs()).getBlobName() == null;
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a S3 request to get the Object Lock configuration for a bucket.
   */
  public static boolean isGetObjectLockConfigurationRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.GET && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(OBJECT_LOCK_PARAM);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a creation/initiation of multipart uploads.
   */
  public static boolean isMultipartCreateUploadRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.POST && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOADS_QUERY_PARAM);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a request for batch delete.
   */
  public static boolean isBatchDelete(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.POST && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(BATCH_DELETE_QUERY_PARAM);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a completion/abortion of multipart uploads.
   */
  public static boolean isMultipartCompleteUploadRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.POST && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOAD_ID_QUERY_PARAM);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is an upload part request for multipart uploads.
   */
  public static boolean isMultipartUploadPartRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.PUT && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOAD_ID_QUERY_PARAM);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a request to list parts of a completed multipart upload request.
   */
  public static boolean isMultipartListPartRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.GET && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOAD_ID_QUERY_PARAM);
  }

  public static boolean isMultipartAbortUploadRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.DELETE && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOAD_ID_QUERY_PARAM);
  }

  protected class DefaultCallbackChain {
    private final RestRequest restRequest;
    private final Callback<ReadableStreamChannel> finalCallback;
    private final String uri;
    private final FrontendMetrics metrics;
    private AccountAndContainerInjector accountAndContainerInjector;
    private SecurityService securityService;
    private RestServiceErrorCode returnCode;
    private String returnMessage;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param securityService the {@link SecurityService}.
     * @param metrics the {@link FrontendMetrics}.
     * @param accountAndContainerInjector the {@link AccountAndContainerInjector}.
     * @param finalCallback the {@link Callback} to call on completion.
     * @param returnCode the {@link RestServiceErrorCode}
     * @param returnMessage the error message to return
     */
    protected DefaultCallbackChain(RestRequest restRequest, SecurityService securityService, FrontendMetrics metrics,
        AccountAndContainerInjector accountAndContainerInjector, Callback<ReadableStreamChannel> finalCallback,
        String returnMessage, RestServiceErrorCode returnCode) {
      this.restRequest = restRequest;
      this.metrics = metrics;
      this.accountAndContainerInjector = accountAndContainerInjector;
      this.securityService = securityService;
      this.finalCallback = finalCallback;
      this.returnCode = returnCode;
      this.returnMessage = returnMessage;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    protected void start() {
      try {
        accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, metrics.postBlobMetricsGroup);
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
      return buildCallback(metrics.putSecurityProcessRequestMetrics, securityCheckResult -> {
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback());
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, return response for the request.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(metrics.putSecurityPostProcessRequestMetrics, securityCheckResult -> {
        throw new RestServiceException(returnMessage, returnCode);
      }, uri, LOGGER, finalCallback);
    }
  }
}
