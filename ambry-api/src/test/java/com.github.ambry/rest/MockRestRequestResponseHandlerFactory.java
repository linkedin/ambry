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
 * Implementation of {@link RestRequestResponseHandlerFactory} that can be used in tests.
 * Sets up all the supporting cast required for {@link MockRestRequestResponseHandler}. Maintains a single instance of
 * {@link MockRestRequestResponseHandler} and returns the same instance on any call to {@link #getRestRequestHandler()} or
 * {@link #getRestResponseHandler()}.
 */
public class MockRestRequestResponseHandlerFactory implements RestRequestResponseHandlerFactory {
  private MockRestRequestResponseHandler handler;

  public MockRestRequestResponseHandlerFactory(Object handlerCount, Object metricRegistry,
      RestRequestService restRequestService) {
    handler = new MockRestRequestResponseHandler(restRequestService);
  }

  /**
   * @return an instance of {@link MockRestRequestResponseHandler}.
   */
  @Override
  public RestRequestHandler getRestRequestHandler() throws InstantiationException {
    return handler;
  }

  /**
   * @return an instance of {@link MockRestRequestResponseHandler}.
   */
  @Override
  public RestResponseHandler getRestResponseHandler() throws InstantiationException {
    return handler;
  }
}
