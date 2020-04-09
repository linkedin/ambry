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

/**
 * RestRequestService defines a service that handles {@link RestRequest}.
 * Typically, a RestRequestService is expected to receive requests and handle them as required and either send a
 * response (if immediately available) or pass control to another component that does further handling and generates a
 * response.
 * Most operations are performed async and responses are therefore queued asynchronously instead of being available at
 * the end of the function call.
 * Implementations are expected to be thread-safe.
 */
public interface RestRequestService {

  /**
   * Setup {@link RestResponseHandler} for this {@link RestRequestService}.
   * This method should be called before {@link RestRequestService#start()}
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses.
   */
  void setupResponseHandler(RestResponseHandler responseHandler);

  /**
   * Does startup tasks for the RestRequestService. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if RestRequestService is unable to start.
   */
  void start() throws InstantiationException;

  /**
   * Does shutdown tasks for the RestRequestService. When the function returns, shutdown is FULLY complete.
   */
  void shutdown();

  /**
   * Handles a GET operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a POST operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a PUT operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  void handlePut(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a DELETE operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles a HEAD operation.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel);

  /**
   * Handles an OPTIONS request.
   * <p/>
   * The {@code restRequest} provided will have both metadata and any content associated with the request. The
   * {@code restResponseChannel} can be used to send responses to the client.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  void handleOptions(RestRequest restRequest, RestResponseChannel restResponseChannel);
}
