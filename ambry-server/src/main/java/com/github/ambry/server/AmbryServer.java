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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.ClusterSpectator;
import com.github.ambry.clustermap.ClusterSpectatorFactory;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.RestServerConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreHardDelete;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SocketServer;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.replication.CloudToStoreReplicationManager;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.NioServerFactory;
import com.github.ambry.rest.RestRequestHandler;
import com.github.ambry.rest.RestRequestResponseHandlerFactory;
import com.github.ambry.rest.RestRequestService;
import com.github.ambry.rest.StorageServerNettyFactory;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * Ambry server
 */
public class AmbryServer {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private NetworkServer networkServer = null;
  private AmbryRequests requests = null;
  private RequestHandlerPool requestHandlerPool = null;
  private ScheduledExecutorService scheduler = null;
  private StorageManager storageManager = null;
  private StatsManager statsManager = null;
  private ReplicationManager replicationManager = null;
  private CloudToStoreReplicationManager cloudToStoreReplicationManager = null;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final VerifiableProperties properties;
  private final ClusterAgentsFactory clusterAgentsFactory;
  private final ClusterSpectatorFactory clusterSpectatorFactory;
  private ClusterMap clusterMap;
  private List<ClusterParticipant> clusterParticipants;
  private ClusterSpectator vcrClusterSpectator;
  private MetricRegistry registry = null;
  private JmxReporter reporter = null;
  private ConnectionPool connectionPool = null;
  private final NotificationSystem notificationSystem;
  private ServerMetrics metrics = null;
  private Time time;
  private RequestHandlerPool requestHandlerPoolForHttp2;
  private RestRequestHandler restRequestHandlerForHttp2;
  private NioServer nettyHttp2Server;

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      ClusterSpectatorFactory clusterSpectatorFactory, Time time) {
    this(properties, clusterAgentsFactory, clusterSpectatorFactory, new LoggingNotificationSystem(), time);
  }

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, Time time) {
    this(properties, clusterAgentsFactory, null, notificationSystem, time);
  }

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      ClusterSpectatorFactory clusterSpectatorFactory, NotificationSystem notificationSystem, Time time) {
    this.properties = properties;
    this.clusterAgentsFactory = clusterAgentsFactory;
    this.clusterSpectatorFactory = clusterSpectatorFactory;
    this.notificationSystem = notificationSystem;
    this.time = time;
  }

  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      clusterMap = clusterAgentsFactory.getClusterMap();
      logger.info("Initialized clusterMap");
      clusterParticipants = clusterAgentsFactory.getClusterParticipants();
      logger.info("Setting up JMX.");
      long startTime = SystemTime.getInstance().milliseconds();
      registry = clusterMap.getMetricRegistry();
      this.metrics = new ServerMetrics(registry, AmbryRequests.class, AmbryServer.class);
      reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      DiskManagerConfig diskManagerConfig = new DiskManagerConfig(properties);
      ServerConfig serverConfig = new ServerConfig(properties);
      ReplicationConfig replicationConfig = new ReplicationConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      SSLConfig sslConfig = new SSLConfig(properties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
      StatsManagerConfig statsConfig = new StatsManagerConfig(properties);
      CloudConfig cloudConfig = new CloudConfig(properties);
      // verify the configs
      properties.verify();

      scheduler = Utils.newScheduler(serverConfig.serverSchedulerNumOfthreads, false);
      logger.info("check if node exist in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
      DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
      if (nodeId == null) {
        throw new IllegalArgumentException("The node " + networkConfig.hostName + ":" + networkConfig.port
            + "is not present in the clustermap. Failing to start the datanode");
      }

      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      // TODO make StorageManager, ReplicationManager, CloudToStoreReplicationManager and StatsManager support multiple participants
      // For now, we assume there is only one element in clusterParticipants list.
      storageManager =
          new StorageManager(storeConfig, diskManagerConfig, scheduler, registry, storeKeyFactory, clusterMap, nodeId,
              new BlobStoreHardDelete(), clusterParticipants.get(0), time, new BlobStoreRecovery());
      storageManager.start();

      connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, registry);
      connectionPool.start();

      StoreKeyConverterFactory storeKeyConverterFactory =
          Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties, registry);

      replicationManager =
          new ReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
              clusterMap, scheduler, nodeId, connectionPool, registry, notificationSystem, storeKeyConverterFactory,
              serverConfig.serverMessageTransformer, clusterParticipants.get(0));
      replicationManager.start();

      if (replicationConfig.replicationEnabledWithVcrCluster) {
        logger.info("Creating Helix cluster spectator for cloud to store replication.");
        vcrClusterSpectator = clusterSpectatorFactory.getClusterSpectator(cloudConfig, clusterMapConfig);
        cloudToStoreReplicationManager =
            new CloudToStoreReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager,
                storeKeyFactory, clusterMap, scheduler, nodeId, connectionPool, registry, notificationSystem,
                storeKeyConverterFactory, serverConfig.serverMessageTransformer, vcrClusterSpectator,
                clusterParticipants.get(0));
        cloudToStoreReplicationManager.start();
      }

      logger.info("Creating StatsManager to publish stats");
      statsManager = new StatsManager(storageManager, clusterMap.getReplicaIds(nodeId), registry, statsConfig, time,
          clusterParticipants.get(0));
      if (serverConfig.serverStatsPublishLocalEnabled) {
        statsManager.start();
      }

      ArrayList<Port> ports = new ArrayList<Port>();
      ports.add(new Port(networkConfig.port, PortType.PLAINTEXT));
      if (nodeId.hasSSLPort()) {
        ports.add(new Port(nodeId.getSSLPort(), PortType.SSL));
      }
      networkServer = new SocketServer(networkConfig, sslConfig, registry, ports);
      FindTokenHelper findTokenHelper = new FindTokenHelper(storeKeyFactory, replicationConfig);
      ServerMetrics serverMetrics = new ServerMetrics(registry, AmbryRequests.class, AmbryServer.class);
      requests = new AmbryServerRequests(storageManager, networkServer.getRequestResponseChannel(), clusterMap, nodeId,
          registry, serverMetrics, findTokenHelper, notificationSystem, replicationManager, storeKeyFactory,
          serverConfig, storeKeyConverterFactory, statsManager);
      requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads,
          networkServer.getRequestResponseChannel(), requests);
      networkServer.start();

      // Start netty http2 server
      if (nodeId.hasHttp2Port()) {
        logger.info("Http2 port {} is enabled. Starting HTTP/2 service. ", nodeId.getHttp2Port());
        RestServerConfig restServerConfig = new RestServerConfig(properties);
        SSLFactory sslFactory = new NettySslHttp2Factory(sslConfig);
        NettyServerRequestResponseChannel requestResponseChannel = new NettyServerRequestResponseChannel(32);
        RestRequestService restRequestService = new StorageRestRequestService(requestResponseChannel);

        AmbryServerRequests ambryServerRequestsForHttp2 =
            new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, nodeId, registry, serverMetrics,
                findTokenHelper, notificationSystem, replicationManager, storeKeyFactory, serverConfig,
                storeKeyConverterFactory, statsManager);
        requestHandlerPoolForHttp2 =
            new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads, requestResponseChannel,
                ambryServerRequestsForHttp2);

        RestRequestResponseHandlerFactory restRequestHandlerFactory =
            Utils.getObj(restServerConfig.restServerRequestResponseHandlerFactory,
                restServerConfig.restServerRequestHandlerScalingUnitCount, registry, restRequestService);
        restRequestHandlerForHttp2 = restRequestHandlerFactory.getRestRequestHandler();
        restRequestHandlerForHttp2.start();

        NioServerFactory nioServerFactory =
            new StorageServerNettyFactory(nodeId.getHttp2Port(), properties, registry, restRequestHandlerForHttp2,
                sslFactory);
        nettyHttp2Server = nioServerFactory.getNioServer();
        nettyHttp2Server.start();
      }

      // Other code
      List<AmbryHealthReport> ambryHealthReports = new ArrayList<>();
      Set<String> validStatsTypes = new HashSet<>();
      for (StatsReportType type : StatsReportType.values()) {
        validStatsTypes.add(type.toString());
      }
      if (serverConfig.serverStatsPublishHealthReportEnabled) {
        serverConfig.serverStatsReportsToPublish.forEach(e -> {
          if (validStatsTypes.contains(e)) {
            ambryHealthReports.add(
                new AmbryStatsReport(statsManager, serverConfig.serverQuotaStatsAggregateIntervalInMinutes,
                    StatsReportType.valueOf(e)));
          }
        });
      }

      if (vcrClusterSpectator != null) {
        vcrClusterSpectator.spectate();
      }
      for (ClusterParticipant clusterParticipant : clusterParticipants) {
        clusterParticipant.participate(ambryHealthReports);
      }

      logger.info("started");
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.serverStartTimeInMs.update(processingTime);
      logger.info("Server startup time in Ms " + processingTime);
    } catch (Exception e) {
      logger.error("Error during startup", e);
      throw new InstantiationException("failure during startup " + e);
    }
  }

  /**
   * This method is expected to be called in the exit path as long as the AmbryServer instance construction was
   * successful. This is expected to be called even if {@link #startup()} did not succeed.
   */
  public void shutdown() {
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      logger.info("shutdown started");
      if (clusterParticipants != null) {
        clusterParticipants.forEach(ClusterParticipant::close);
      }
      if (scheduler != null) {
        shutDownExecutorService(scheduler, 5, TimeUnit.MINUTES);
      }
      if (statsManager != null) {
        statsManager.shutdown();
      }
      if (networkServer != null) {
        networkServer.shutdown();
      }
      if (nettyHttp2Server != null) {
        nettyHttp2Server.shutdown();
      }
      if (restRequestHandlerForHttp2 != null) {
        restRequestHandlerForHttp2.shutdown();
      }
      if (requestHandlerPoolForHttp2 != null) {
        requestHandlerPoolForHttp2.shutdown();
      }
      if (requestHandlerPool != null) {
        requestHandlerPool.shutdown();
      }
      if (cloudToStoreReplicationManager != null) {
        cloudToStoreReplicationManager.shutdown();
      }
      if (replicationManager != null) {
        replicationManager.shutdown();
      }
      if (storageManager != null) {
        storageManager.shutdown();
      }
      if (connectionPool != null) {
        connectionPool.shutdown();
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
      logger.info("shutdown completed");
    } catch (Exception e) {
      logger.error("Error while shutting down server", e);
    } finally {
      shutdownLatch.countDown();
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.serverShutdownTimeInMs.update(processingTime);
      logger.info("Server shutdown time in Ms " + processingTime);
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}
