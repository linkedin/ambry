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
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.frontend.AccountAndContainerInjector;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.IdConverter;
import com.github.ambry.frontend.SecurityService;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handles requests for s3 multipart uploads.
 */
public class S3MultipartUploadHandler extends S3BaseHandler<ReadableStreamChannel> {
  private final S3CreateMultipartUploadHandler createMultipartUploadHandler;
  private final S3CompleteMultipartUploadHandler completeMultipartUploadHandler;
  private final S3MultipartUploadPartHandler uploadPartHandler;

  /**
   * Construct a handler for handling S3 POST requests during multipart uploads.
   * @param securityService the {@link SecurityService} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param frontendConfig the {@link FrontendConfig} to use.
   * @param namedBlobDb the {@link NamedBlobDb} to use.
   * @param idConverter the {@link IdConverter} to use.
   * @param router the {@link Router} to use.
   * @param quotaManager The {@link QuotaManager} class to account for quota usage in serving requests.
   */
  public S3MultipartUploadHandler(SecurityService securityService, FrontendMetrics frontendMetrics,
      AccountAndContainerInjector accountAndContainerInjector, FrontendConfig frontendConfig, NamedBlobDb namedBlobDb,
      IdConverter idConverter, Router router, QuotaManager quotaManager) {
    this.createMultipartUploadHandler = new S3CreateMultipartUploadHandler(securityService, frontendMetrics);
    this.completeMultipartUploadHandler =
        new S3CompleteMultipartUploadHandler(securityService, namedBlobDb, idConverter, router,
            accountAndContainerInjector, frontendMetrics, frontendConfig, quotaManager);
    this.uploadPartHandler =
        new S3MultipartUploadPartHandler(securityService, idConverter, router, accountAndContainerInjector,
            frontendConfig, frontendMetrics, quotaManager);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    if (isCreateRequest(restRequest)) {
      createMultipartUploadHandler.handle(restRequest, restResponseChannel, callback);
    } else if (isUploadPartRequest(restRequest)) {
      uploadPartHandler.handle(restRequest, restResponseChannel, callback);
    } else if (isCompleteRequest(restRequest)) {
      completeMultipartUploadHandler.handle(restRequest, restResponseChannel, callback);
    } else {
      callback.onCompletion(null,
          new RestServiceException("Invalid S3 Multipart request", RestServiceErrorCode.BadRequest));
    }
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a creation/initiation of multipart uploads.
   */
  public static boolean isCreateRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.POST && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOADS_QUERY_PARAM);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is a completion/abortion of multipart uploads.
   */
  public static boolean isCompleteRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.POST && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOAD_ID_QUERY_PARAM);
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @return {@code True} if it is an upload part request for multipart uploads.
   */
  public static boolean isUploadPartRequest(RestRequest restRequest) {
    return restRequest.getRestMethod() == RestMethod.PUT && restRequest.getArgs().containsKey(S3_REQUEST)
        && restRequest.getArgs().containsKey(UPLOAD_ID_QUERY_PARAM);
  }
}
