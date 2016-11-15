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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link NonBlockingRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NonBlockingRouter} and returns a new
 * instance on {@link #getRouter()}.
 */
public class NonBlockingRouterFactory implements RouterFactory {
  private final RouterConfig routerConfig;
  private final NonBlockingRouterMetrics routerMetrics;
  private final ClusterMap clusterMap;
  private final NetworkConfig networkConfig;
  private final NetworkMetrics networkMetrics;
  private final SSLFactory sslFactory;
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final NetworkClientFactory networkClientFactory;
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouterFactory.class);

  /**
   * Creates an instance of NonBlockingRouterFactory with the given {@code verifiableProperties},
   * {@code clusterMap} and {@code notificationSystem}.
   * @param verifiableProperties the in-memory properties to use to construct configurations.
   * @param clusterMap the {@link ClusterMap} to use to determine where operations should go.
   * @param notificationSystem the {@link NotificationSystem} to use to log operations.
   * @throws IllegalArgumentException if any of the arguments are null.
   * @throws IOException if the SSL configs could not be initialized.
   */
  public NonBlockingRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem) throws GeneralSecurityException, IOException {
    if (verifiableProperties != null && clusterMap != null && notificationSystem != null) {
      routerConfig = new RouterConfig(verifiableProperties);
      if (!clusterMap.hasDatacenter(routerConfig.routerDatacenterName)) {
        throw new IllegalStateException(
            "Router datacenter " + routerConfig.routerDatacenterName + " is not part of the clustermap");
      }
      MetricRegistry registry = clusterMap.getMetricRegistry();
      routerMetrics = new NonBlockingRouterMetrics(clusterMap);
      this.clusterMap = clusterMap;
      this.notificationSystem = notificationSystem;
      networkConfig = new NetworkConfig(verifiableProperties);
      networkMetrics = new NetworkMetrics(registry);
      SSLConfig sslConfig = new SSLConfig(verifiableProperties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
      sslFactory = clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0 ? new SSLFactory(sslConfig) : null;
      this.time = SystemTime.getInstance();
      networkClientFactory = new NetworkClientFactory(networkMetrics, networkConfig, sslFactory,
          routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
          routerConfig.routerScalingUnitMaxConnectionsPerPortSsl, routerConfig.routerConnectionCheckoutTimeoutMs, time);
    } else {
      throw new IllegalArgumentException("Null argument passed in");
    }
    logger.trace("Instantiated NonBlockingRouterFactory");
  }

  /**
   * Construct and return a {@link NonBlockingRouter}
   * @return a {@link NonBlockingRouter}
   * @throws InstantiationException
   */
  @Override
  public Router getRouter() throws InstantiationException {
    try {
      return new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, notificationSystem, clusterMap,
          time);
    } catch (IOException e) {
      throw new InstantiationException("Error instantiating NonBlocking Router" + e.getMessage());
    }
  }
}

