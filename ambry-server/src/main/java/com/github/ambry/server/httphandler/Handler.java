/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server.httphandler;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;


/**
 * Http handler interface. Any http handler has to implement this interface and register
 * itself in AmbryServerRequests in order to handle the incoming http requests.
 *
 * {@link #supportedMethod()} returns the HttpMethod this handler supports. This handler
 * would be registered to only handle http requests at this {@link HttpMethod}. If this
 * method returns {@link HttpMethod#GET}, then this handler would only handle Get requests.
 *
 * {@link #match} returns true if handler wants to handle the given {@link FullHttpRequest}.
 * The simplest way is to check the {@link FullHttpRequest#uri()}.
 *
 * {@link #handle} method handles the given http request. This method also has a default
 * response passed into. If you don't have any content to return, then just se the status
 * and headers in the default response and return the default response back. If you have
 * content to return, then use {@link FullHttpResponse#replace} to get a new response from
 * default response.
 */
public interface Handler {
  /**
   * handle given http request and return a http response.
   * @param request The given http request.
   * @param defaultResponse The default http response.
   * @return A {@link FullHttpResponse}.
   */
  FullHttpResponse handle(FullHttpRequest request, FullHttpResponse defaultResponse);

  /**
   * The {@link HttpMethod} this handler supports
   * @return The {@link HttpMethod}.
   */
  HttpMethod supportedMethod();

  /**
   * {@code true} if handler wants to handle this request.
   * @param request The given request
   * @return True if handler want to handle this request.
   */
  boolean match(FullHttpRequest request);
}
