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
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
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
  private final NotificationSystem notificationSystem;
  private final Time time;
  private final NetworkClientFactory networkClientFactory;
  private final KeyManagementService kms;
  private final CryptoService cryptoService;
  private final CryptoJobHandler cryptoJobHandler;
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouterFactory.class);

  /**
   * Creates an instance of NonBlockingRouterFactory with the given {@code verifiableProperties},
   * {@code clusterMap} and {@code notificationSystem}.
   * @param verifiableProperties the in-memory properties to use to construct configurations.
   * @param clusterMap the {@link ClusterMap} to use to determine where operations should go.
   * @param notificationSystem the {@link NotificationSystem} to use to log operations.
   * @param sslFactory the {@link SSLFactory} to support SSL transmissions. Required if SSL is enabled for any
   *                   datacenters.
   * @throws Exception if any of the arguments are null or if instantiation of KMS or CryptoService fails
   */
  public NonBlockingRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem, SSLFactory sslFactory) throws Exception {
    if (verifiableProperties == null || clusterMap == null || notificationSystem == null) {
      throw new IllegalArgumentException("Null argument passed in");
    }
    if (sslFactory == null && new ClusterMapConfig(verifiableProperties).clusterMapSslEnabledDatacenters.length() > 0) {
      throw new IllegalArgumentException("NonBlockingRouter requires SSL, but sslFactory is null");
    }
    routerConfig = new RouterConfig(verifiableProperties);
    if (!clusterMap.hasDatacenter(routerConfig.routerDatacenterName)) {
      throw new IllegalStateException(
          "Router datacenter " + routerConfig.routerDatacenterName + " is not part of the clustermap");
    }
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
    MetricRegistry registry = clusterMap.getMetricRegistry();
    routerMetrics = new NonBlockingRouterMetrics(clusterMap);
    networkConfig = new NetworkConfig(verifiableProperties);
    networkMetrics = new NetworkMetrics(registry);
    time = SystemTime.getInstance();
    networkClientFactory = new NetworkClientFactory(networkMetrics, networkConfig, sslFactory,
        routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
        routerConfig.routerScalingUnitMaxConnectionsPerPortSsl, routerConfig.routerConnectionCheckoutTimeoutMs, time);
    KeyManagementServiceFactory kmsFactory =
        Utils.getObj(routerConfig.routerKeyManagementServiceFactory, verifiableProperties,
            new ClusterMapConfig(verifiableProperties).clusterMapClusterName, registry);
    kms = kmsFactory.getKeyManagementService();
    CryptoServiceFactory cryptoServiceFactory =
        Utils.getObj(routerConfig.routerCryptoServiceFactory, verifiableProperties, registry);
    cryptoService = cryptoServiceFactory.getCryptoService();
    cryptoJobHandler =
        new CryptoJobHandler(routerConfig.routerCryptoJobsWorkerCount, routerConfig.routerCryptoJobsTimeoutMs, time);
    logger.trace("Instantiated NonBlockingRouterFactory");
  }

  /**
   * Construct and return a {@link NonBlockingRouter}
   * @return a {@link NonBlockingRouter}
   */
  @Override
  public Router getRouter() {
    try {
      return new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, notificationSystem, clusterMap,
          kms, cryptoService, cryptoJobHandler, time);
    } catch (IOException e) {
      throw new IllegalStateException("Error instantiating NonBlocking Router ", e);
    }
  }
}

