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
import com.github.ambry.frontend.DeleteBlobHandler;
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


/**
 * Handler to handle all the S3 DELETE requests
 */
public class S3DeleteHandler extends S3BaseHandler<Void> {

  private final S3DeleteObjectHandler objectHandler;
  private final S3MultipartUploadHandler s3MultipartUploadHandler;
  private final FrontendMetrics metrics;

  /**
   * Construct a handler for handling S3 DELETE requests.
   *
   * @param deleteBlobHandler the generic {@link DeleteBlobHandler} delegated to by the underlying delete object handler.
   */
  public S3DeleteHandler(DeleteBlobHandler deleteBlobHandler, S3MultipartUploadHandler s3MultipartUploadHandler,
      FrontendMetrics metrics) {
    this.metrics = metrics;
    this.objectHandler = new S3DeleteObjectHandler(deleteBlobHandler);
    this.s3MultipartUploadHandler = s3MultipartUploadHandler;
  }

  /**
   * Process the request and construct the response according to the
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html">DeleteObject spec</a> and
   *
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback)
      throws RestServiceException {
    if (S3MultipartUploadHandler.isMultipartAbortUploadRequest(restRequest)) {
      s3MultipartUploadHandler.handle(restRequest, restResponseChannel, callback);
    } else {
      objectHandler.handle(restRequest, restResponseChannel, callback);
    }
  }

  private class S3DeleteObjectHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(S3DeleteObjectHandler.class);
    private final DeleteBlobHandler deleteBlobHandler;

    private S3DeleteObjectHandler(DeleteBlobHandler deleteBlobHandler) {
      this.deleteBlobHandler = deleteBlobHandler;
    }

    private void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> finalCallback)
        throws RestServiceException {
      // Callback for successful case.
      ThrowingConsumer<Void> successAction = (r) -> {
        restResponseChannel.setStatus(ResponseStatus.NoContent);
        finalCallback.onCompletion(null, null);
      };

      // Callback for failure case, Since S3 delete should be idempotent, we should return 204 on 404.
      // TODO: deleteBlobHandler returns 400 if the account or container does not exist, but s3
      // should return 404 with xml error content in the response body. We should return compatible response
      // as s3.
      Callback<Void> failureCallback = (r, e) -> {
        Exception finalException = e;
        try {
          if (e instanceof RestServiceException) {
            RestServiceException restServiceException = (RestServiceException) e;
            if (restServiceException.getErrorCode() == RestServiceErrorCode.NotFound) {
              finalException = null;
              successAction.accept(null);
            }
          }
        } catch (Exception ex) {
          finalException = ex;
        } finally {
          if (finalException != null) {
            finalCallback.onCompletion(r, finalException);
          }
        }
      };

      deleteBlobHandler.handle(restRequest, restResponseChannel,
          buildCallback(metrics.s3DeleteHandleMetrics, successAction, restRequest.getUri(), LOGGER, failureCallback));
    }
  }
}
