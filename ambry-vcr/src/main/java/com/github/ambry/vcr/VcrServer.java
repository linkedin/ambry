/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.vcr;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceFactory;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudStorageManager;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.cloud.VcrReplicationManager;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationFactory;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.VcrClusterAgentsFactory;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.commons.NettyInternalMetrics;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.ServerRequestResponseHelper;
import com.github.ambry.network.SocketNetworkClientFactory;
import com.github.ambry.network.SocketServer;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClientFactory;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.rest.NettyMetrics;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.NioServerFactory;
import com.github.ambry.rest.ServerSecurityService;
import com.github.ambry.rest.ServerSecurityServiceFactory;
import com.github.ambry.rest.StorageServerNettyFactory;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * Virtual Cloud Replicator server
 */
public class VcrServer {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private ScheduledExecutorService scheduler = null;
  private VcrReplicationManager vcrReplicationManager = null;
  private StoreManager cloudStorageManager = null;
  private static final Logger logger = LoggerFactory.getLogger(VcrServer.class);
  private final VerifiableProperties properties;
  private final ClusterAgentsFactory clusterAgentsFactory;
  private ClusterMap clusterMap;
  private VcrClusterParticipant vcrClusterParticipant;
  private MetricRegistry registry = null;
  private JmxReporter reporter = null;
  private final NotificationSystem notificationSystem;
  private final Function<MetricRegistry, JmxReporter> reporterFactory;
  private CloudDestination cloudDestination;
  private ServerSecurityService serverSecurityService;
  private ServerMetrics serverMetrics;
  private NettyInternalMetrics nettyInternalMetrics;

  /**
   * VcrServer constructor.
   * @param properties the config properties to use.
   * @param clusterAgentsFactory the {@link ClusterAgentsFactory} to use.
   * @param notificationSystem the {@link NotificationSystem} to use.
   * @param reporterFactory if non-null, use this function to set up a {@link JmxReporter} with custom settings. If this
   *                        option is null the default settings for the reporter will be used.
   */
  public VcrServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, Function<MetricRegistry, JmxReporter> reporterFactory) {
    this.properties = properties;
    this.clusterAgentsFactory = clusterAgentsFactory;
    this.notificationSystem = notificationSystem;
    this.reporterFactory = reporterFactory;
  }

  /**
   * Test constructor.
   * @param properties the config properties to use.
   * @param clusterAgentsFactory the {@link ClusterAgentsFactory} to use.
   * @param notificationSystem the {@link NotificationSystem} to use.
   * @param cloudDestinationFactory the {@link CloudDestinationFactory} to use.
   * @param reporterFactory if non-null, use this function to set up a {@link JmxReporter} with custom settings. If this
   *                        option is null the default settings for the reporter will be used.
   */
  public VcrServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, CloudDestinationFactory cloudDestinationFactory,
      Function<MetricRegistry, JmxReporter> reporterFactory) {
    this(properties, clusterAgentsFactory, notificationSystem, reporterFactory);
  }

  /**
   * Start the VCR Server.
   * @throws InstantiationException if an error was encountered during startup.
   */
  public void startup() throws InstantiationException {
    try {
      logger.info("[Startup] Starting VCR Server");
      ServerConfig serverConfig = new ServerConfig(properties);
      ServerSecurityServiceFactory serverSecurityServiceFactory =
          Utils.getObj(serverConfig.serverSecurityServiceFactory, properties, serverMetrics, registry);
      serverSecurityService = serverSecurityServiceFactory.getServerSecurityService();

      clusterMap = clusterAgentsFactory.getClusterMap();
      registry = clusterMap.getMetricRegistry();
      serverMetrics = new ServerMetrics(registry, VcrServer.class, VcrServer.class);
      nettyInternalMetrics = new NettyInternalMetrics(registry, new NettyConfig(properties));
      nettyInternalMetrics.start();

      long startTime = SystemTime.getInstance().milliseconds();
      registry = clusterMap.getMetricRegistry();
      reporter = reporterFactory != null ? reporterFactory.apply(registry) : JmxReporter.forRegistry(registry).build();
      reporter.start();

      AzureCloudConfig azureCloudConfig = new AzureCloudConfig(properties);
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      CloudConfig cloudConfig = new CloudConfig(properties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
      SSLConfig sslConfig = new SSLConfig(properties);
      properties.verify();
      // These are required to be at these values going forward for backups.
      if (!azureCloudConfig.azureBlobContainerStrategy.equalsIgnoreCase(
          String.valueOf(AzureBlobLayoutStrategy.BlobContainerStrategy.PARTITION))) {
        throw new InstantiationException("azureBlobContainerStrategy must be PARTITION but is "
            + azureCloudConfig.azureBlobContainerStrategy);
      }
      if (azureCloudConfig.azureNameSchemeVersion != 1)  {
        throw new InstantiationException("azureNameSchemeVersion must be 1 but is "
            + azureCloudConfig.azureNameSchemeVersion);
      }
      if (cloudConfig.ambryBackupVersion != CloudConfig.AMBRY_BACKUP_VERSION_2) {
        throw new InstantiationException("ambryBackupVersion must be 2.0 but is "
            + cloudConfig.ambryBackupVersion);
      }

      // TODO Make sure that config.updaterPollingIntervalMs value is large (~one day) for VCR.
      AccountServiceFactory accountServiceFactory =
          Utils.getObj(serverConfig.serverAccountServiceFactory, properties, registry);
      AccountService accountService = accountServiceFactory.getAccountService();

      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);

      SSLFactory sslHttp2Factory = new NettySslHttp2Factory(sslConfig);
      NetworkClientFactory networkClientFactory;
      Time time = SystemTime.getInstance();
      if (clusterMapConfig.clusterMapEnableHttp2Replication) {
        Http2ClientMetrics http2ClientMetrics = new Http2ClientMetrics(registry);
        Http2ClientConfig http2ClientConfig = new Http2ClientConfig(properties);
        networkClientFactory =
            new Http2NetworkClientFactory(http2ClientMetrics, http2ClientConfig, sslHttp2Factory, time);
        logger.info("Using http2 for VCR replication.");
      } else {
        SSLFactory sslSocketFactory =
            clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0 ? SSLFactory.getNewInstance(sslConfig) : null;
        networkClientFactory =
            new SocketNetworkClientFactory(new NetworkMetrics(registry), networkConfig, sslSocketFactory, 20, 20, 50000,
                time);
        logger.info("Using socket channel for VCR replication.");
      }

      StoreKeyConverterFactory storeKeyConverterFactory =
          Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties, registry);
      VcrMetrics vcrMetrics = new VcrMetrics(registry);

      AzureCloudDestinationFactory cloudDestinationFactory = Utils.getObj(cloudConfig.cloudDestinationFactoryClass,
          properties, registry, clusterMap, accountService);
      cloudDestination = cloudDestinationFactory.getCloudDestination();
      vcrClusterParticipant =
          ((VcrClusterAgentsFactory) Utils.getObj(cloudConfig.vcrClusterAgentsFactoryClass, cloudConfig,
              clusterMapConfig, clusterMap, accountService, storeConfig, cloudDestination,
              registry)).getVcrClusterParticipant();
      cloudStorageManager = new CloudStorageManager(properties, vcrMetrics, cloudDestination, clusterMap);
      vcrReplicationManager = new VcrReplicationManager(properties, cloudStorageManager, storeKeyFactory, clusterMap,
          vcrClusterParticipant, cloudDestination, scheduler, networkClientFactory, notificationSystem,
          storeKeyConverterFactory);
      vcrReplicationManager.start();
      logger.info("[Startup] Started VCR server in {}ms", SystemTime.getInstance().milliseconds() - startTime);
    } catch (Throwable e) {
      logger.error("[Startup] Failed to start VCR server due to {}", e.getMessage());
      e.printStackTrace();
      throw new InstantiationException();
    }
  }

  /**
   * This method is expected to be called in the exit path as long as the AmbryServer instance construction was
   * successful. This is expected to be called even if {@link #startup()} did not succeed.
   */
  public void shutdown() {
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      logger.info("VCR shutdown started");
      if (vcrReplicationManager != null) {
        vcrReplicationManager.shutdown();
      }
      if (reporter != null) {
        reporter.stop();
      }
      if (notificationSystem != null) {
        try {
          notificationSystem.close();
        } catch (IOException e) {
          logger.error("Error while closing notification system.", e);
        }
      }
      if (clusterMap != null) {
        clusterMap.close();
      }
      if (vcrClusterParticipant != null) {
        vcrClusterParticipant.close();
      }
      if (cloudDestination != null) {
        cloudDestination.close();
      }
      if (nettyInternalMetrics != null) {
        nettyInternalMetrics.stop();
      }
      logger.info("VCR shutdown completed");
    } catch (Exception e) {
      logger.error("Error while shutting down VCR", e);
    } finally {
      shutdownLatch.countDown();
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      logger.info("VCR shutdown time in Ms {}", processingTime);
    }
  }

  public boolean awaitShutdown(int timeoutMs) throws InterruptedException {
    return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
  }

  public VcrClusterParticipant getVcrClusterParticipant() {
    return vcrClusterParticipant;
  }

  public VcrReplicationManager getVcrReplicationManager() {
    return vcrReplicationManager;
  }
}
