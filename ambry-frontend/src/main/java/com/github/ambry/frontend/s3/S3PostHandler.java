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
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;

import static com.github.ambry.rest.RestUtils.*;


/**
 * Handles S3 POST requests.
 */
public class S3PostHandler extends S3BaseHandler<ReadableStreamChannel> {
  private final S3MultipartUploadHandler multipartPostHandler;

  /**
   * Constructs a handler for uploading s3 POST requests.
   */
  public S3PostHandler(S3MultipartUploadHandler multipartPostHandler) {
    this.multipartPostHandler = multipartPostHandler;
  }

  /**
   * Handles a request for putting a blob.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    if (isMultipartUploadRequest(restRequest)) {
      multipartPostHandler.handle(restRequest, restResponseChannel, callback);
    } else {
      // Placeholder for handling any non-multipart S3 POST requests in the future.
      throw new RestServiceException("Unsupported S3 POST request", RestServiceErrorCode.BadRequest);
    }
  }

  private boolean isMultipartUploadRequest(RestRequest restRequest) {
    return restRequest.getArgs().containsKey(UPLOADS_QUERY_PARAM) || restRequest.getArgs()
        .containsKey(UPLOAD_ID_QUERY_PARAM);
  }
}
