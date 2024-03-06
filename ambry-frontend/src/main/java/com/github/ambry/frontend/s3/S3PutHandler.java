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
import com.github.ambry.frontend.FrontendMetrics;
import com.github.ambry.frontend.NamedBlobPath;
import com.github.ambry.frontend.NamedBlobPutHandler;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;


/**
 * Handles S3 requests for uploading blobs.
 * API reference: <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">...</a>
 */
public class S3PutHandler extends S3BaseHandler<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3PutHandler.class);
  private final NamedBlobPutHandler namedBlobPutHandler;
  private final S3MultipartUploadHandler multipartUploadHandler;
  private final FrontendMetrics metrics;

  /**
   * Constructs a handler for uploading s3 requests.
   *
   * @param namedBlobPutHandler  the {@link NamedBlobPutHandler} to use.
   * @param multipartUploadHandler the {@link S3MultipartUploadHandler} to use.
   */
  public S3PutHandler(NamedBlobPutHandler namedBlobPutHandler, S3MultipartUploadHandler multipartUploadHandler,
      FrontendMetrics metrics) {
    this.namedBlobPutHandler = namedBlobPutHandler;
    this.multipartUploadHandler = multipartUploadHandler;
    this.metrics = metrics;
  }

  /**
   * Handles a request for putting a blob.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   * @throws RestServiceException exception when the processing fails
   */
  @Override
  protected void doHandle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback)
      throws RestServiceException {
    if (S3MultipartUploadHandler.isUploadPartRequest(restRequest)) {
      multipartUploadHandler.handle(restRequest, restResponseChannel,
          (result, exception) -> callback.onCompletion(null, exception));
      return;
    }

    // 1. Add headers required by Ambry. These become the blob properties.
    NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
    String accountName = namedBlobPath.getAccountName();
    restRequest.setArg(Headers.SERVICE_ID, accountName);
    restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, restRequest.getArgs().get(Headers.CONTENT_TYPE));
    restRequest.setArg(Headers.AMBRY_CONTENT_ENCODING, restRequest.getArgs().get(Headers.CONTENT_ENCODING));

    // 2. Upload the blob by following named blob PUT path
    namedBlobPutHandler.handle(restRequest, restResponseChannel, buildCallback(metrics.s3PutHandleMetrics, (r) -> {
      if (restResponseChannel.getStatus() == ResponseStatus.Created) {
        // Set the response status to 200 since Ambry named blob PUT has response as 201.
        restResponseChannel.setStatus(ResponseStatus.Ok);
        // Set S3 ETag header
        String blobId = (String) restResponseChannel.getHeader(LOCATION);
        blobId = blobId.startsWith("/") ? blobId.substring(1) : blobId;
        restResponseChannel.setHeader("ETag", blobId);
      }
      callback.onCompletion(null, null);
    }, restRequest.getUri(), LOGGER, callback));
  }
}
