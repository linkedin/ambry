/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class HttpMultiplexer {
  private final Map<RestMethod, List<Pair<RestRequestMatcher, HttpHandler>>> methodsToListOfHandler = new HashMap<>();

  public HttpMultiplexer() {
    for (RestMethod method : RestMethod.values()) {
      methodsToListOfHandler.put(method, new ArrayList<>());
    }
  }

  void register(RestMethod method, RestRequestMatcher matcher, HttpHandler handler) {
    List<Pair<RestRequestMatcher, HttpHandler>> list = methodsToListOfHandler.get(method);
    if (list == null) {
      throw new IllegalStateException("Unsupported method: " + method);
    }
    if (!list.isEmpty()) {
      RestRequestMatcher lastMatcher = list.get(list.size() - 1).getFirst();
      if (lastMatcher == RestRequestMatcher.ALL_MATCHER) {
        throw new IllegalStateException("Last matcher is already an all matcher, this matcher will never be reached");
      }
    }
    list.add(new Pair<>(matcher, handler));
  }

  public void get(RestRequestMatcher matcher, HttpHandler handler) {
    register(RestMethod.GET, matcher, handler);
  }

  public void post(RestRequestMatcher matcher, HttpHandler handler) {
    register(RestMethod.POST, matcher, handler);
  }

  public void put(RestRequestMatcher matcher, HttpHandler handler) {
    register(RestMethod.PUT, matcher, handler);
  }

  public void delete(RestRequestMatcher matcher, HttpHandler handler) {
    register(RestMethod.DELETE, matcher, handler);
  }

  public void head(RestRequestMatcher matcher, HttpHandler handler) {
    register(RestMethod.HEAD, matcher, handler);
  }

  public void options(RestRequestMatcher matcher, HttpHandler handler) {
    register(RestMethod.OPTIONS, matcher, handler);
  }

  public void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    RestMethod restMethod = restRequest.getRestMethod();
    List<Pair<RestRequestMatcher, HttpHandler>> list = methodsToListOfHandler.get(restMethod);
    if (list == null) {
      throw new RestServiceException("Unsupported REST method: " + restMethod,
          RestServiceErrorCode.UnsupportedRestMethod);
    }
    HttpHandler httpHandler = null;
    for (Pair<RestRequestMatcher, HttpHandler> pair : list) {
      if (pair.getFirst().match(restRequest)) {
        httpHandler = pair.getSecond();
        break;
      }
    }
    if (httpHandler != null) {
      httpHandler.handleRequest(restRequest, restResponseChannel, callback);
    } else {
      throw new RestServiceException(
          "Unrecognized method + operation: " + restMethod.toString() + " " + restRequest.getUri(),
          RestServiceErrorCode.BadRequest);
    }
  }
}
