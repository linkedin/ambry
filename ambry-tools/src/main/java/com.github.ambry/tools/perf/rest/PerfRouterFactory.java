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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link PerfRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link PerfRouter} and returns an instance on
 * {@link #getRouter()}.
 */
public class PerfRouterFactory implements RouterFactory {
  private final PerfConfig perfConfig;
  private final PerfRouterMetrics perfRouterMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates an instance of PerfRouterFactory.
   * @param verifiableProperties the in-memory properties for constructing configurations.
   * @param clusterMap the {@link ClusterMap} whose {@link MetricRegistry} needs to be used.
   * @param notificationSystem the {@link NotificationSystem} to use (can be {@code null}).
   * @param sslFactory the {@link SSLFactory} to use (can be {@code null}).
   * @param accountService the {@link AccountService} to use. Can be {@code null}.
   * @throws IllegalArgumentException if any of the required arguments are null.
   */
  public PerfRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem, SSLFactory sslFactory, AccountService accountService) {
    if (verifiableProperties == null || clusterMap == null) {
      throw new IllegalArgumentException("One of the arguments is null");
    } else {
      perfConfig = new PerfConfig(verifiableProperties);
      perfRouterMetrics = new PerfRouterMetrics(clusterMap.getMetricRegistry());
      logger.trace("Instantiated PerfRouterFactory");
    }
  }

  @Override
  public Router getRouter() {
    return new PerfRouter(perfConfig, perfRouterMetrics);
  }
}

/**
 * Metrics related to the {@link PerfRouter}.
 */
class PerfRouterMetrics {
  public final Histogram putSizeInBytes;
  public final Histogram putContentConsumeTimeInMs;

  /**
   * Creates an instance of PerfRouterMetrics.
   * @param metricRegistry the {@link MetricRegistry} instance to use.
   */
  PerfRouterMetrics(MetricRegistry metricRegistry) {
    putSizeInBytes = metricRegistry.histogram(MetricRegistry.name(PerfRouter.class, "PostSizeInBytes"));
    putContentConsumeTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(PerfRouter.class, "PostContentConsumeTimeInMs"));
  }
}

