/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
 */
package com.github.ambry.rest;

import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Implementation of {@link RestRequestHandler} and {@link RestResponseHandler} that can be used in tests.
 * <p/>
 * This implementation simply calls the appropriate method (based on the {@link RestMethod} in the request) in the
 * underlying {@link RestRequestService} on being asked to handle a request. If the implementation of
 * {@link RestRequestService} is blocking, then this will be blocking too.
 * <p/>
 * Submitted responses also will be sent out immediately so response handling will block until the response has been
 * sent out completely.
 * <p/>
 * Be advised that this may not work if your test code *needs* the {@link RestRequestHandler} and
 * {@link RestResponseHandler} to be non-blocking. Test code with such assumptions may run into infinite loops.
 */
public class MockRestRequestResponseHandler implements RestRequestHandler, RestResponseHandler {
  public static String RUNTIME_EXCEPTION_ON_HANDLE = "runtime.exception.on.handle";
  public static String REST_EXCEPTION_ON_HANDLE = "rest.exception.on.handle";

  private boolean isRunning = false;
  private VerifiableProperties failureProperties = null;

  private final RestRequestService restRequestService;

  public MockRestRequestResponseHandler(RestRequestService restRequestService) {
    this.restRequestService = restRequestService;
    this.restRequestService.setupResponseHandler(this);
  }

  @Override
  public void start() throws InstantiationException {
    if (restRequestService == null) {
      throw new InstantiationException("RestRequestService not set");
    }
    isRunning = true;
  }

  @Override
  public void shutdown() {
    isRunning = false;
  }

  /**
   * Calls the appropriate method in the {@link RestRequestService}. Non-blocking nature depends on the implementation
   * of the underlying {@link RestRequestService}.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is any error while processing the request.
   */
  @Override
  public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (shouldProceed(restRequest, restResponseChannel)) {
      RestMethod restMethod = restRequest.getRestMethod();
      restRequest.prepare();
      switch (restMethod) {
        case GET:
          restRequestService.handleGet(restRequest, restResponseChannel);
          break;
        case POST:
          restRequestService.handlePost(restRequest, restResponseChannel);
          break;
        case PUT:
          restRequestService.handlePut(restRequest, restResponseChannel);
          break;
        case DELETE:
          restRequestService.handleDelete(restRequest, restResponseChannel);
          break;
        case HEAD:
          restRequestService.handleHead(restRequest, restResponseChannel);
          break;
        default:
          throw new RestServiceException("Unknown rest method - " + restMethod,
              RestServiceErrorCode.UnsupportedRestMethod);
      }
    }
  }

  /**
   * Sends out responses immediately. Returns control only after {@code readableStreamChannel} has been consumed and
   * sent over the {@code restResponseChannel}.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the
   *                                {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   * @throws IllegalArgumentException if either of {@code restRequest} or {@code restResponseChannel} is null.
   * @throws RestServiceException if there is any error while processing the request.
   */
  @Override
  public void handleResponse(RestRequest restRequest, final RestResponseChannel restResponseChannel,
      final ReadableStreamChannel response, Exception exception) throws RestServiceException {
    if (shouldProceed(restRequest, restResponseChannel)) {
      if (exception != null || response == null) {
        onResponseComplete(restResponseChannel, response, exception);
      } else {
        response.readInto(restResponseChannel, new Callback<Long>() {
          private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

          @Override
          public void onCompletion(Long result, Exception exception) {
            if (callbackInvoked.compareAndSet(false, true)) {
              if (exception == null && (result == null || result != response.getSize())) {
                exception = new IllegalStateException("Response write incomplete");
              }
              onResponseComplete(restResponseChannel, response, exception);
            }
          }
        });
      }
    }
  }

  /**
   * Makes the MockRestRequestResponseHandler faulty.
   * @param props failure properties. Defines the faulty behaviour. If null, there is no breakdown.
   */
  public void breakdown(VerifiableProperties props) {
    failureProperties = props;
  }

  /**
   * Fixes the MockRestRequestResponseHandler (not faulty anymore).
   */
  public void fix() {
    failureProperties = null;
  }

  /**
   * If the MockRestRequestResponseHandler is supposed to be breakdown, throws the right exception.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @return {@code true} if the the caller can proceed. {@code false} otherwise.
   * @throws RestServiceException
   */
  private boolean shouldProceed(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    if (restRequest == null) {
      throw new IllegalArgumentException("RestRequest is null");
    } else if (restResponseChannel == null) {
      throw new IllegalArgumentException("RestResponseChannel is null");
    } else if (!isRunning) {
      throw new RestServiceException("MockRestRequestResponseHandler is not running",
          RestServiceErrorCode.ServiceUnavailable);
    } else if (failureProperties != null) {
      if (failureProperties.containsKey(RUNTIME_EXCEPTION_ON_HANDLE) && failureProperties.getBoolean(
          RUNTIME_EXCEPTION_ON_HANDLE)) {
        throw new RuntimeException(RUNTIME_EXCEPTION_ON_HANDLE);
      } else if (failureProperties.containsKey(REST_EXCEPTION_ON_HANDLE)) {
        RestServiceErrorCode errorCode = RestServiceErrorCode.InternalServerError;
        try {
          errorCode = RestServiceErrorCode.valueOf(failureProperties.getString(REST_EXCEPTION_ON_HANDLE));
        } catch (IllegalArgumentException e) {
          // it's alright.
        }
        throw new RestServiceException(REST_EXCEPTION_ON_HANDLE, errorCode);
      }
    }
    return failureProperties == null;
  }

  /**
   * Completes the response.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   */
  private void onResponseComplete(RestResponseChannel restResponseChannel, ReadableStreamChannel response,
      Exception exception) {
    try {
      restResponseChannel.onResponseComplete(exception);
    } finally {
      releaseResources(response);
    }
  }

  /**
   * Cleans up resources.
   */
  private void releaseResources(ReadableStreamChannel response) {
    if (response != null) {
      try {
        response.close();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}



