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
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.NamedBlobPutHandler;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;


/**
 * Handles a request for S3 Multipart upload part requests according to the
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">...</a>
 */
public class S3MultipartUploadPartHandler {
  private final NamedBlobPutHandler namedBlobPutHandler;

  /**
   * Construct a handler for handling S3 POST requests during multipart uploads.
   * @param namedBlobPutHandler the {@link NamedBlobPutHandler} to use.
   */
  public S3MultipartUploadPartHandler(NamedBlobPutHandler namedBlobPutHandler) {
    this.namedBlobPutHandler = namedBlobPutHandler;
  }

  /**
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    try {

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

      // 3. Intercept the response callback and make needed changes in response headers.
      Callback<Void> wrappedCallback = (result, exception) -> {
        if (exception != null) {
          callback.onCompletion(null, exception);
          return;
        }
        try {
          // Set the response status to 200 since Ambry named blob PUT has response as 201.
          if (restResponseChannel.getStatus() == ResponseStatus.Created) {
            restResponseChannel.setStatus(ResponseStatus.Ok);
            String blobId = RestUtils.getHeader(restRequest.getArgs(), LOCATION, true);
            restResponseChannel.setHeader("ETag", blobId);
          }
          //TODO [S3]: remove x-ambry- headers
          callback.onCompletion(null, null);
        } catch (RestServiceException e) {
          callback.onCompletion(null, e);
        }
      };

      namedBlobPutHandler.handle(restRequest, restResponseChannel, wrappedCallback);
    } catch (RestServiceException e) {
      callback.onCompletion(null, e);
    }
  }
}
