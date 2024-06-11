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
  private NioServer nettyHttp2Server;
  private NetworkServer networkServer = null;
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
  private CloudDestinationFactory cloudDestinationFactory;
  private RequestHandlerPool requestHandlerPool;
  private RequestHandlerPool requestHandlerPoolForHttp2;
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
    this.cloudDestinationFactory = cloudDestinationFactory;
  }

  /**
   * Start the VCR Server.
   * @throws InstantiationException if an error was encountered during startup.
   */
  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      ServerConfig serverConfig = new ServerConfig(properties);
      ServerSecurityServiceFactory serverSecurityServiceFactory =
          Utils.getObj(serverConfig.serverSecurityServiceFactory, properties, serverMetrics, registry);
      serverSecurityService = serverSecurityServiceFactory.getServerSecurityService();

      clusterMap = clusterAgentsFactory.getClusterMap();
      logger.info("Initialized clusterMap");
      registry = clusterMap.getMetricRegistry();
      serverMetrics = new ServerMetrics(registry, VcrServer.class, VcrServer.class);
      nettyInternalMetrics = new NettyInternalMetrics(registry, new NettyConfig(properties));
      nettyInternalMetrics.start();

      logger.info("Setting up JMX.");
      long startTime = SystemTime.getInstance().milliseconds();
      registry = clusterMap.getMetricRegistry();
      reporter = reporterFactory != null ? reporterFactory.apply(registry) : JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      ReplicationConfig replicationConfig = new ReplicationConfig(properties);
      CloudConfig cloudConfig = new CloudConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
      SSLConfig sslConfig = new SSLConfig(properties);
      // verify the configs
      properties.verify();

      // TODO Make sure that config.updaterPollingIntervalMs value is large (~one day) for VCR.
      AccountServiceFactory accountServiceFactory =
          Utils.getObj(serverConfig.serverAccountServiceFactory, properties, registry);
      AccountService accountService = accountServiceFactory.getAccountService();

      scheduler = Utils.newScheduler(serverConfig.serverSchedulerNumOfthreads, false);
      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);

      SSLFactory sslHttp2Factory = new NettySslHttp2Factory(sslConfig);
      NetworkClientFactory networkClientFactory = null;
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

      logger.info("Ambry backup version = {}", cloudConfig.ambryBackupVersion);
      if (cloudDestinationFactory == null) {
        cloudDestinationFactory =
            Utils.getObj(cloudConfig.cloudDestinationFactoryClass, properties, registry, clusterMap, accountService);
      }
      cloudDestination = cloudDestinationFactory.getCloudDestination();
      vcrClusterParticipant =
          ((VcrClusterAgentsFactory) Utils.getObj(cloudConfig.vcrClusterAgentsFactoryClass, cloudConfig,
              clusterMapConfig, clusterMap, accountService, storeConfig, cloudDestination,
              registry)).getVcrClusterParticipant();
      cloudStorageManager = new CloudStorageManager(properties, vcrMetrics, cloudDestination, clusterMap);
      vcrReplicationManager = new VcrReplicationManager(properties, cloudStorageManager, storeKeyFactory, clusterMap, vcrClusterParticipant, cloudDestination, scheduler,
          networkClientFactory, notificationSystem, storeKeyConverterFactory);
      vcrReplicationManager.start();

      DataNodeId currentNode = vcrClusterParticipant.getCurrentDataNodeId();
      ArrayList<Port> ports = new ArrayList<Port>();
      ports.add(new Port(networkConfig.port, PortType.PLAINTEXT));
      if (currentNode.hasSSLPort()) {
        ports.add(new Port(cloudConfig.vcrSslPort, PortType.SSL));
      }
      networkServer = new SocketServer(networkConfig, sslConfig, registry, ports);

      //todo fix enableDataPrefetch
      ServerMetrics serverMetrics = new ServerMetrics(registry, VcrRequests.class, VcrServer.class);
      VcrRequests requests =
          new VcrRequests(cloudStorageManager, networkServer.getRequestResponseChannel(), clusterMap, currentNode,
              registry, serverMetrics, new FindTokenHelper(storeKeyFactory, replicationConfig), notificationSystem,
              vcrReplicationManager, storeKeyFactory, storeKeyConverterFactory);

      requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumSocketServerThreads,
          networkServer.getRequestResponseChannel(), requests);

      networkServer.start();

      // Start netty http2 server
      if (currentNode.hasHttp2Port()) {
        logger.info("Http2 port {} is enabled. Starting HTTP/2 service.", currentNode.getHttp2Port());
        NettyConfig nettyConfig = new NettyConfig(properties);
        NettyMetrics nettyMetrics = new NettyMetrics(registry);
        Http2ServerMetrics http2ServerMetrics = new Http2ServerMetrics(registry);
        Http2ClientConfig http2ClientConfig = new Http2ClientConfig(properties);
        FindTokenHelper findTokenHelper = new FindTokenHelper(storeKeyFactory, replicationConfig);
        NettyServerRequestResponseChannel requestResponseChannel =
            new NettyServerRequestResponseChannel(networkConfig, http2ServerMetrics, serverMetrics,
                new ServerRequestResponseHelper(clusterMap, findTokenHelper));
        VcrRequests vcrRequestsForHttp2 =
            new VcrRequests(cloudStorageManager, requestResponseChannel, clusterMap, currentNode, registry,
                serverMetrics, findTokenHelper, notificationSystem, vcrReplicationManager, storeKeyFactory,
                storeKeyConverterFactory);
        requestHandlerPoolForHttp2 =
            new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads, requestResponseChannel,
                vcrRequestsForHttp2);

        NioServerFactory nioServerFactory =
            new StorageServerNettyFactory(currentNode.getHttp2Port(), requestResponseChannel, sslHttp2Factory,
                nettyConfig, http2ClientConfig, serverMetrics, nettyMetrics, http2ServerMetrics, serverSecurityService);
        nettyHttp2Server = nioServerFactory.getNioServer();
        nettyHttp2Server.start();
      }

      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      logger.info("VCR startup time in Ms {}", processingTime);
    } catch (Exception e) {
      logger.error("Error during VCR startup", e);
      throw new InstantiationException("failure during VCR startup " + e);
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
      if (scheduler != null) {
        shutDownExecutorService(scheduler, 5, TimeUnit.MINUTES);
      }
      if (requestHandlerPool != null) {
        requestHandlerPool.shutdown();
      }
      if (networkServer != null) {
        networkServer.shutdown();
      }
      if (requestHandlerPoolForHttp2 != null) {
        requestHandlerPoolForHttp2.shutdown();
      }
      if (nettyHttp2Server != null) {
        nettyHttp2Server.shutdown();
      }
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
