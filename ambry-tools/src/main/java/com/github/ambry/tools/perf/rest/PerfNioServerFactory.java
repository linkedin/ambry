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
package com.github.ambry.tools.perf.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.NioServerFactory;
import com.github.ambry.rest.PublicAccessLogger;
import com.github.ambry.rest.RestRequestHandler;
import com.github.ambry.rest.RestServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Perf specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link PerfNioServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class PerfNioServerFactory implements NioServerFactory {
  private final PerfConfig perfConfig;
  private final PerfNioServerMetrics perfNioServerMetrics;
  private final RestRequestHandler requestHandler;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates a new instance of PerfNioServerFactory.
   * @param verifiableProperties the in-memory {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param requestHandler the {@link RestRequestHandler} that can be used to submit requests that need to be handled.
   * @param publicAccessLogger the {@link PublicAccessLogger} that can be used for public access logging (can be
   *                           {@code null}).
   * @param restServerState the {@link RestServerState} that can be used to check the health of the system (can be
   *                        {@code null}).
   * @throws IllegalArgumentException if any of the required arguments are null.
   */
  public PerfNioServerFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestHandler requestHandler, PublicAccessLogger publicAccessLogger, RestServerState restServerState,
      SSLFactory sslFactory) {
    if (verifiableProperties == null || metricRegistry == null || requestHandler == null) {
      throw new IllegalArgumentException("One of the arguments is null");
    } else {
      this.requestHandler = requestHandler;
      perfConfig = new PerfConfig(verifiableProperties);
      perfNioServerMetrics = new PerfNioServerMetrics(metricRegistry);
      logger.trace("Instantiated PerfNioServerFactory");
    }
  }

  /**
   * Returns a new instance of {@link PerfNioServer}.
   * @return a new instance of {@link PerfNioServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new PerfNioServer(perfConfig, perfNioServerMetrics, requestHandler);
  }
}

/**
 * Metrics related to the {@link PerfNioServer}.
 */
class PerfNioServerMetrics {
  public final Meter requestRate;

  public final Histogram requestRoundTripTimeInMs;

  public final Counter requestResponseError;

  /**
   * Creates an instance of PerfNioServerMetrics.
   * @param metricRegistry the {@link MetricRegistry} instance to use.
   */
  PerfNioServerMetrics(MetricRegistry metricRegistry) {
    requestRate = metricRegistry.meter(MetricRegistry.name(PerfNioServer.class, "RequestRate"));

    requestRoundTripTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(PerfNioServer.class, "RequestRoundTripTimeInMs"));

    requestResponseError = metricRegistry.counter(MetricRegistry.name(PerfNioServer.class, "RequestResponseError"));
  }
}
