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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.commons.Callback;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.HeadBlobHandler;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.SecurityService;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handler to handle all the S3 HEAD requests
 */
public class S3HeadHandler extends S3BaseHandler<Void> {
  private final S3HeadBucketHandler bucketHandler;
  private final S3HeadObjectHandler objectHandler;

  /**
   * Construct a handler for handling S3 HEAD requests.
   *
   * @param headBlobHandler the generic head request handler delegated to by the underlying head object handler.
   * @param securityService the {@link SecurityService} to use.
   * @param metrics the {@link FrontendMetrics} to use.
   * @param accountService the {@link AccountService} to use.
   */
  public S3HeadHandler(HeadBlobHandler headBlobHandler, SecurityService securityService,
      FrontendMetrics metrics, AccountService accountService) {
    this.bucketHandler = new S3HeadBucketHandler(securityService, metrics, accountService);
    this.objectHandler = new S3HeadObjectHandler(headBlobHandler);
  }

  /**
   * Process the request and construct the response according to the
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html">HeadObject spec</a> and
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html">BucketObject spec</a>
   *
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<Void> callback) throws RestServiceException {
    String path = ((RequestPath) restRequest.getArgs().get(REQUEST_PATH)).getOperationOrBlobId(true);
    if (isHeadBucketRequest(path)) {
      bucketHandler.handle(restRequest, restResponseChannel, callback);
    } else {
      objectHandler.handle(restRequest, restResponseChannel, callback);
    }
  }

  private class S3HeadBucketHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(S3HeadBucketHandler.class);
    private final SecurityService securityService;
    private final FrontendMetrics metrics;
    private final AccountService accountService;

    private S3HeadBucketHandler(SecurityService securityService, FrontendMetrics metrics,
        AccountService accountService) {
      this.securityService = securityService;
      this.metrics = metrics;
      this.accountService = accountService;
    }

    private void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<Void> callback) {
      new CallbackChain(restRequest, restResponseChannel, callback).start();
    }

    private class CallbackChain {
      private final RestRequest restRequest;
      private final RestResponseChannel restResponseChannel;
      private final Callback<Void> finalCallback;

      private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
          Callback<Void> finalCallback) {
        this.restRequest = restRequest;
        this.restResponseChannel = restResponseChannel;
        this.finalCallback = finalCallback;
      }

      private void start() {
        restRequest.setArg(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT, true);
        securityService.processRequest(restRequest, securityProcessRequestCallback());
      }

      private Callback<Void> securityProcessRequestCallback() {
        return buildCallback(metrics.headBlobSecurityProcessRequestMetrics,
            result -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
            restRequest.getUri(), LOGGER, null);
      }

      private Callback<Void> securityPostProcessRequestCallback() {
        return buildCallback(metrics.getAccountsSecurityPostProcessRequestMetrics, securityCheckResult -> {
          RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
          // Build a dummy blob path to get the account and container name
          String dummyBlobPath = requestPath.getOperationOrBlobId(true) + "/dummy";
          NamedBlobPath namedBlobPath = NamedBlobPath.parse(dummyBlobPath, restRequest.getArgs());
          String accountName = namedBlobPath.getAccountName();
          String containerName = namedBlobPath.getContainerName();
          try {
            Container container = accountService.getContainerByName(accountName, containerName);
            if (container == null) {
              throw new RestServiceException(
                  String.format("Container %s in account %s not found", containerName, accountName),
                  RestServiceErrorCode.NotFound);
            }
          } catch (AccountServiceException e) {
            throw new RestServiceException(
                String.format("Failed to get container %s from account %s", containerName, accountName),
                RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
          }
          finalCallback.onCompletion(null, null);
        }, restRequest.getUri(), LOGGER, finalCallback);
      }
    }
  }

  private class S3HeadObjectHandler {
    private final HeadBlobHandler headBlobHandler;
    private S3HeadObjectHandler(HeadBlobHandler headBlobHandler) {
      this.headBlobHandler = headBlobHandler;
    }
    private void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<Void> callback) throws RestServiceException {

      // TODO [S3]: implement PartNumber handling

      headBlobHandler.handle(restRequest, restResponseChannel,
          ((result, exception) -> callback.onCompletion(null, exception)));
    }
  }

  private boolean isHeadBucketRequest(String path) {
    return path.split("/").length <= 3;
  }
}
