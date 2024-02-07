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
import com.github.ambry.frontend.Operations;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;

/**
 * Base class for all S3 handle classes that provides crosscutting functionality such as
 * logging and response header handling
 *
 * @param <R> the type of the result the object in callbacks. It is {@link Void} for requests that don't
 *           require a response body, otherwise it is {@link ReadableStreamChannel}.
 */
abstract public class S3BaseHandler<R> {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3BaseHandler.class);

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
  public void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<R> callback) throws RestServiceException {
    String path = ((RequestPath) restRequest.getArgs().get(REQUEST_PATH)).getOperationOrBlobId(true);
    if (!path.startsWith(Operations.NAMED_BLOB)) {
      throw new RuntimeException("S3 request handler can only handle named blob requests");
    }

    String action = getClass().getSimpleName()
        .replace("S3", "")
        .replace("Handler", "");
    LOGGER.debug("{} {}", action, path);

    doHandle(restRequest, restResponseChannel, (result, exception) -> {
      if (exception != null) {
        callback.onCompletion(null, exception);
      } else try {
        removeAmbryHeaders(restResponseChannel);
        callback.onCompletion(result, null);
      } catch (Exception e) {
        callback.onCompletion(null, e);
      }
    });
  }

  private void removeAmbryHeaders(RestResponseChannel restResponseChannel) {
    restResponseChannel.getHeaders().stream()
        .filter(header -> header.startsWith(AMBRY_HEADER_PREFIX))
        .forEach(restResponseChannel::removeHeader);
  }
}
