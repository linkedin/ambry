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
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler to handle all the S3 DELETE requests
 */
public class S3DeleteHandler extends S3BaseHandler<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3DeleteHandler.class);
  private S3DeleteObjectHandler objectHandler;

  /**
   * Construct a handler for handling S3 DELETE requests.
   *
   * @param deleteBlobHandler the generic {@link DeleteBlobHandler} delegated to by the underlying delete object handler.
   */
  public S3DeleteHandler(DeleteBlobHandler deleteBlobHandler) {
    this.objectHandler = new S3DeleteObjectHandler(deleteBlobHandler);
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
    objectHandler.handle(restRequest, restResponseChannel, callback);
  }

  private class S3DeleteObjectHandler {
    private final DeleteBlobHandler deleteBlobHandler;

    private S3DeleteObjectHandler(DeleteBlobHandler deleteBlobHandler) {
      this.deleteBlobHandler = deleteBlobHandler;
    }

    private void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback)
        throws RestServiceException {
      deleteBlobHandler.handle(restRequest, restResponseChannel, ((result, exception) -> {
        if (exception != null) {
          callback.onCompletion(null, exception);
        } else {
          try {
            restResponseChannel.setStatus(ResponseStatus.NoContent);
            callback.onCompletion(null, null);
          } catch (RestServiceException e) {
            callback.onCompletion(null, e);
          }
        }
      }));
    }
  }
}
