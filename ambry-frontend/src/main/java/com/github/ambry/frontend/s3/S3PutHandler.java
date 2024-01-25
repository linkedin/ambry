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
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;

import static com.github.ambry.rest.RestUtils.*;


/**
 * Handles S3 requests for uploading blobs.
 * API reference: <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">...</a>
 */
public class S3PutHandler {
  private final NamedBlobPutHandler namedBlobPutHandler;

  /**
   * Constructs a handler for uploading s3 requests.
   * @param namedBlobPutHandler named blob list handler
   */
  public S3PutHandler(NamedBlobPutHandler namedBlobPutHandler) {
    this.namedBlobPutHandler = namedBlobPutHandler;
  }

  /**
   * Asynchronously get account metadata.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  public void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    try {
      // 1. Add headers needed that are needed by Ambry
      NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
      String accountName = namedBlobPath.getAccountName();
      // Use Account-name as service ID.
      restRequest.setArg(Headers.SERVICE_ID, accountName);
      // Set x-ambry-content-type to content-type http header
      restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, restRequest.getArgs().get(Headers.CONTENT_TYPE));

      // 2. Upload the blob by following named blob put path
      namedBlobPutHandler.handle(restRequest, restResponseChannel, callback);
    } catch (RestServiceException e) {
      callback.onCompletion(null, e);
    }
  }
}
