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

import static com.github.ambry.rest.RestUtils.*;


/**
 * Handles S3 requests for uploading blobs.
 * API reference: <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">...</a>
 */
public class S3PutHandler {
  public static final String AMBRY_PARAMETERS_PREFIX = "x-ambry-";
  private final NamedBlobPutHandler namedBlobPutHandler;

  /**
   * Constructs a handler for uploading s3 requests.
   * @param namedBlobPutHandler named blob put handler
   */
  public S3PutHandler(NamedBlobPutHandler namedBlobPutHandler) {
    this.namedBlobPutHandler = namedBlobPutHandler;
  }

  /**
   * Handles a request for putting a blob.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  public void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {

    // 1. Add headers required by Ambry. These become the blob properties.
    try {
      NamedBlobPath namedBlobPath = NamedBlobPath.parse(getRequestPath(restRequest), restRequest.getArgs());
      String accountName = namedBlobPath.getAccountName();
      restRequest.setArg(Headers.SERVICE_ID, accountName);
      restRequest.setArg(Headers.AMBRY_CONTENT_TYPE, restRequest.getArgs().get(Headers.CONTENT_TYPE));
      restRequest.setArg(Headers.AMBRY_CONTENT_ENCODING, restRequest.getArgs().get(Headers.CONTENT_ENCODING));
    } catch (RestServiceException e) {
      callback.onCompletion(null, e);
      return;
    }

    // 2. Upload the blob by following named blob PUT path
    namedBlobPutHandler.handle(restRequest, restResponseChannel, (result, exception) -> {
      try {
        // Remove any Ambry specific headers in response
        for (String headerName : restResponseChannel.getHeaders()) {
          if (headerName.startsWith(AMBRY_PARAMETERS_PREFIX)) {
            restResponseChannel.removeHeader(headerName);
          }
        }
        if (exception == null && restResponseChannel.getStatus() == ResponseStatus.Created) {
          // Set the response status to 200 since Ambry named blob PUT has response as 201.
          restResponseChannel.setStatus(ResponseStatus.Ok);
        }
        callback.onCompletion(result, exception);
      } catch (RestServiceException e) {
        callback.onCompletion(null, e);
      }
    });
  }
}
