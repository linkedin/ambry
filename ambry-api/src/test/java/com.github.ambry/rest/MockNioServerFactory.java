/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;


/**
 * Implementation of {@link NioServerFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link MockNioServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class MockNioServerFactory implements NioServerFactory {
  public static final String IS_FAULTY_KEY = "mock.nio.server.isFaulty";

  private final boolean isFaulty;

  public MockNioServerFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      Object restRequestHandler, PublicAccessLogger publicAccessLogger, RestServerState restServerState) {
    isFaulty = verifiableProperties.getBoolean(IS_FAULTY_KEY, false);
  }

  /**
   * Returns a new instance of {@link MockNioServer}.
   * @return a new instance of {@link MockNioServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new MockNioServer(isFaulty);
  }
}
