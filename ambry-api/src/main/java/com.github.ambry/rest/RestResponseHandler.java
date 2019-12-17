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

import com.github.ambry.router.ReadableStreamChannel;


/**
 * Meant to be the component that handles and routes all outgoing responses.
 * <p/>
 * The responses are usually submitted by the {@link RestRequestService} and sent out via the
 * {@link RestResponseChannel} with the RestResponseHandler forming a bridge between them to provide scaling
 * capabilities and non-blocking behaviour.
 * <p/>
 * Implementations are expected to be thread-safe.
 */
public interface RestResponseHandler {
  /**
   * Does startup tasks for the RestResponseHandler. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if the RestResponseHandler is unable to start.
   */
  public void start() throws InstantiationException;

  /**
   * Does shutdown tasks for the RestResponseHandler. When the function returns, shutdown is FULLY complete.
   * <p/>
   * Any responses queued after shutdown is called might be dropped.
   */
  public void shutdown();

  /**
   * Submit a response for a request along with a channel over which the response can be sent. If the response building
   * was unsuccessful for any reason, the details should be included in the {@code exception}.
   * <p/>
   * The bytes consumed from the {@code response} are streamed out (unmodified) through the
   * {@code restResponseChannel}.
   * <p/>
   * Assumed that at least one of {@code response} or {@code exception} is null.
   * <p/>
   * Depending on the implementation, it is possible that the {@code response} is not immediately sent but en-queued to
   * be sent at a later time.
   * @param restRequest the {@link RestRequest} for which the response has been constructed.
   * @param restResponseChannel the {@link RestResponseChannel} to be used to send the response.
   * @param response a {@link ReadableStreamChannel} that represents the response to the {@code restRequest}.
   * @param exception if the response could not be constructed, the reason for the failure.
   * @throws RestServiceException if there is any error while processing the response.
   */
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception) throws RestServiceException;
}
