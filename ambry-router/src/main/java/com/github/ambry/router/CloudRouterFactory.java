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
import com.github.ambry.account.AccountService;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudStorageManager;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.CompositeNetworkClientFactory;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.SocketNetworkClientFactory;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.EnumMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link RouterFactory} that creates routers that work with cloud storage.
 * This is somewhat experimental, written to enable Ambry frontend to talk to Azure.  Eventually
 * the router will need to work with both cloud and data stores.
 */
public class CloudRouterFactory implements RouterFactory {
  private static final Logger logger = LoggerFactory.getLogger(CloudRouterFactory.class);
  private final ClusterMapConfig clusterMapConfig;
  private final RouterConfig routerConfig;
  private final NetworkConfig networkConfig;
  private final NonBlockingRouterMetrics routerMetrics;
  private final NetworkMetrics networkMetrics;
  private final ClusterMap clusterMap;
  private final NotificationSystem notificationSystem;
  private final SSLFactory sslFactory;
  private final AccountService accountService;
  private final Time time;
  private final KeyManagementService kms;
  private final CryptoService cryptoService;
  private final CryptoJobHandler cryptoJobHandler;
  private final String defaultPartitionClass;
  private final VerifiableProperties verifiableProperties;

  /**
   * Creates an instance of NonBlockingRouterFactory with the given {@code verifiableProperties},
   * {@code clusterMap} and {@code notificationSystem}.
   * @param verifiableProperties the in-memory properties to use to construct configurations.
   * @param clusterMap the {@link ClusterMap} to use to determine where operations should go.
   * @param notificationSystem the {@link NotificationSystem} to use to log operations.
   * @param sslFactory the {@link SSLFactory} to support SSL transmissions. Required if SSL is enabled for any
   *                   datacenters.
   * @param accountService the {@link AccountService} to use.
   * @throws Exception if any of the arguments are null or if instantiation of KMS or CryptoService fails
   */
  public CloudRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem, SSLFactory sslFactory, AccountService accountService) throws Exception {
    if (verifiableProperties == null || clusterMap == null || notificationSystem == null) {
      throw new IllegalArgumentException("Null argument passed in");
    }
    this.verifiableProperties = verifiableProperties;
    clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    if (sslFactory == null && clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0) {
      throw new IllegalArgumentException("NonBlockingRouter requires SSL, but sslFactory is null");
    }
    this.sslFactory = sslFactory;
    routerConfig = new RouterConfig(verifiableProperties);
    if (!clusterMap.hasDatacenter(routerConfig.routerDatacenterName)) {
      throw new IllegalStateException(
          "Router datacenter " + routerConfig.routerDatacenterName + " is not part of the clustermap");
    }
    networkConfig = new NetworkConfig(verifiableProperties);
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
    this.accountService = accountService;
    MetricRegistry registry = clusterMap.getMetricRegistry();
    routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    networkMetrics = new NetworkMetrics(registry);
    time = SystemTime.getInstance();
    KeyManagementServiceFactory kmsFactory =
        Utils.getObj(routerConfig.routerKeyManagementServiceFactory, verifiableProperties,
            new ClusterMapConfig(verifiableProperties).clusterMapClusterName, registry);
    kms = kmsFactory.getKeyManagementService();
    CryptoServiceFactory cryptoServiceFactory =
        Utils.getObj(routerConfig.routerCryptoServiceFactory, verifiableProperties, registry);
    cryptoService = cryptoServiceFactory.getCryptoService();
    cryptoJobHandler = new CryptoJobHandler(routerConfig.routerCryptoJobsWorkerCount);
    defaultPartitionClass = clusterMapConfig.clusterMapDefaultPartitionClass;
    logger.info("Instantiated CloudRouterFactory");
  }

  /**
   * Construct and return a {@link NonBlockingRouter} that works with cloud storage.
   * @return a {@link NonBlockingRouter}
   */
  @Override
  public Router getRouter() throws InstantiationException {
    try {
      MetricRegistry registry = clusterMap.getMetricRegistry();
      CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
      CloudDestinationFactory cloudDestinationFactory =
          Utils.getObj(cloudConfig.cloudDestinationFactoryClass, verifiableProperties, registry);
      CloudDestination cloudDestination = cloudDestinationFactory.getCloudDestination();
      RequestHandlerPool requestHandlerPool =
          getRequestHandlerPool(verifiableProperties, clusterMap, cloudDestination, cloudConfig);
      CompositeNetworkClientFactory networkClientFactory = getCompositeNetworkClientFactory(requestHandlerPool);
      NonBlockingRouter router =
          new NonBlockingRouter(routerConfig, routerMetrics, networkClientFactory, notificationSystem, clusterMap, kms,
              cryptoService, cryptoJobHandler, accountService, time, defaultPartitionClass);
      // Make sure requestHandlerPool is shut down properly
      router.addResourceToClose(requestHandlerPool);
      router.addResourceToClose(cloudDestination);
      logger.info("Instantiated NonBlockingRouter");
      return router;
    } catch (Exception e) {
      logger.error("Error instantiating NonBlocking Router", e);
      throw new InstantiationException("Error instantiating NonBlocking Router: " + e.toString());
    }
  }

  /**
   * Utility method to build  a {@link RequestHandlerPool}.
   * @param verifiableProperties the properties to use.
   * @param clusterMap the {@link ClusterMap} to use.
   * @return the constructed {@link RequestHandlerPool}.
   * @throws Exception if the construction fails.
   */
  static RequestHandlerPool getRequestHandlerPool(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      CloudDestination cloudDestination, CloudConfig cloudConfig) throws Exception {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    // TODO: move properties into maybe routerConfig, frontendConfig
    ServerConfig serverConfig = new ServerConfig(verifiableProperties);
    MetricRegistry registry = clusterMap.getMetricRegistry();

    DataNodeId nodeId = new CloudDataNode(cloudConfig, clusterMapConfig);
    VcrMetrics vcrMetrics = new VcrMetrics(registry);
    StoreManager cloudStorageManager =
        new CloudStorageManager(verifiableProperties, vcrMetrics, cloudDestination, clusterMap);
    LocalRequestResponseChannel channel = new LocalRequestResponseChannel();
    ServerMetrics serverMetrics = new ServerMetrics(registry, AmbryRequests.class);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    StoreKeyConverterFactory storeKeyConverterFactory =
        Utils.getObj(serverConfig.serverStoreKeyConverterFactory, verifiableProperties, registry);
    // A null notification system is passed into AmbryRequests so that replication events are not emitted from a
    // frontend.
    AmbryRequests requests =
        new AmbryRequests(cloudStorageManager, channel, clusterMap, nodeId, registry, serverMetrics, null, null, null,
            storeKeyFactory, serverConfig.serverEnableStoreDataPrefetch, storeKeyConverterFactory);
    return new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads, channel, requests);
  }

  /**
   * @param requestHandlerPool the pool to connect to {@link LocalNetworkClientFactory}.
   * @return a {@link CompositeNetworkClientFactory} that can be used to talk to cloud managed services or ambry server
   *         nodes.
   */
  private CompositeNetworkClientFactory getCompositeNetworkClientFactory(RequestHandlerPool requestHandlerPool) {
    NetworkClientFactory cloudNetworkClientFactory =
        new LocalNetworkClientFactory((LocalRequestResponseChannel) requestHandlerPool.getChannel(), networkConfig,
            networkMetrics, time);
    NetworkClientFactory diskNetworkClientFactory =
        new SocketNetworkClientFactory(networkMetrics, networkConfig, sslFactory,
            routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
            routerConfig.routerScalingUnitMaxConnectionsPerPortSsl, routerConfig.routerConnectionCheckoutTimeoutMs,
            time);

    Map<ReplicaType, NetworkClientFactory> childFactories = new EnumMap<>(ReplicaType.class);
    childFactories.put(ReplicaType.CLOUD_BACKED, cloudNetworkClientFactory);
    childFactories.put(ReplicaType.DISK_BACKED, diskNetworkClientFactory);

    return new CompositeNetworkClientFactory(childFactories);
  }
}

