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
 * Meant to be the component that handles and routes all incoming requests.
 * <p/>
 * The requests are usually submitted by a {@link NioServer} and handled by a {@link RestRequestService} with the
 * RestRequestHandler forming a bridge between them to provide scaling capabilities and non-blocking behaviour.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestRequestHandler {

  /**
   * Does startup tasks for the RestRequestHandler. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if the RestRequestHandler is unable to start.
   */
  public void start() throws InstantiationException;

  /**
   * Does shutdown tasks for the RestRequestHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * Any requests queued after shutdown is called might be dropped.
   * <p/>
   * The {@link NioServer} is expected to have stopped queueing new requests before this function is called.
   */
  public void shutdown();

  /**
   * Submit a request for handling along with a channel on which a response to the request may be sent.
   * <p/>
   * Depending on the implementation, it is possible that the {@code restRequest} is not immediately handled but
   * en-queued to be handled at a later time.
   * @param restRequest the {@link RestRequest} that needs to be handled.
   * @param restResponseChannel the {@link RestResponseChannel} on which a response to the request may be sent.
   * @throws RestServiceException if there is any error while processing the request.
   */
  public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException;
}
