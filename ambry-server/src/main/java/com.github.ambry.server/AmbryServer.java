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

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreHardDelete;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SocketServer;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StatsManager;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ambry server
 */
class AmbryServer {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private NetworkServer networkServer = null;
  private AmbryRequests requests = null;
  private RequestHandlerPool requestHandlerPool = null;
  private ScheduledExecutorService scheduler = null;
  private StorageManager storageManager = null;
  private StatsManager statsManager = null;
  private ReplicationManager replicationManager = null;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final VerifiableProperties properties;
  private final ClusterAgentsFactory clusterAgentsFactory;
  private ClusterMap clusterMap;
  private ClusterParticipant clusterParticipant;
  private MetricRegistry registry = null;
  private JmxReporter reporter = null;
  private ConnectionPool connectionPool = null;
  private final NotificationSystem notificationSystem;
  private ServerMetrics metrics = null;
  private Time time;

  AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory, Time time)
      throws IOException {
    this(properties, clusterAgentsFactory, new LoggingNotificationSystem(), time);
  }

  AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, Time time) throws IOException {
    this.properties = properties;
    this.clusterAgentsFactory = clusterAgentsFactory;
    this.notificationSystem = notificationSystem;
    this.time = time;
  }

  void startup() throws InstantiationException {
    try {
      clusterMap = clusterAgentsFactory.getClusterMap();
      clusterParticipant = clusterAgentsFactory.getClusterParticipant();
      logger.info("starting");
      logger.info("Setting up JMX.");
      long startTime = SystemTime.getInstance().milliseconds();
      registry = clusterMap.getMetricRegistry();
      this.metrics = new ServerMetrics(registry);
      reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      ServerConfig serverConfig = new ServerConfig(properties);
      ReplicationConfig replicationConfig = new ReplicationConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      SSLConfig sslConfig = new SSLConfig(properties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
      StatsManagerConfig statsConfig = new StatsManagerConfig(properties);
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
      FindTokenFactory findTokenFactory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
      storageManager =
          new StorageManager(storeConfig, scheduler, registry, clusterMap.getReplicaIds(nodeId), storeKeyFactory,
              new BlobStoreRecovery(), new BlobStoreHardDelete(), time);
      storageManager.start();

      connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, registry);
      connectionPool.start();

      replicationManager =
          new ReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
              clusterMap, scheduler, nodeId, connectionPool, registry, notificationSystem);
      replicationManager.start();

      ArrayList<Port> ports = new ArrayList<Port>();
      ports.add(new Port(networkConfig.port, PortType.PLAINTEXT));
      if (nodeId.hasSSLPort()) {
        ports.add(new Port(nodeId.getSSLPort(), PortType.SSL));
      }

      networkServer = new SocketServer(networkConfig, sslConfig, registry, ports);
      requests =
          new AmbryRequests(storageManager, networkServer.getRequestResponseChannel(), clusterMap, nodeId, registry,
              findTokenFactory, notificationSystem, replicationManager, storeKeyFactory);
      requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads,
          networkServer.getRequestResponseChannel(), requests);
      networkServer.start();

      if (statsConfig.publishEnabled) {
        List<PartitionId> partitionIds = new ArrayList<>();
        for (ReplicaId replicaId : clusterMap.getReplicaIds(nodeId)) {
          partitionIds.add(replicaId.getPartitionId());
        }
        statsManager = new StatsManager(storageManager, partitionIds, registry, statsConfig, time);
        statsManager.start();
      }
      clusterParticipant.initialize(networkConfig.hostName, networkConfig.port);

      logger.info("started");
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.serverStartTimeInMs.update(processingTime);
      logger.info("Server startup time in Ms " + processingTime);
    } catch (Exception e) {
      logger.error("Error during startup", e);
      throw new InstantiationException("failure during startup " + e);
    }
  }

  void shutdown() {
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      logger.info("shutdown started");
      clusterParticipant.terminate();
      if (scheduler != null) {
        scheduler.shutdown();
        if (!scheduler.awaitTermination(5, TimeUnit.MINUTES)) {
          logger.error("Could not terminate all tasks after scheduler shutdown");
        }
      }
      if (statsManager != null) {
        statsManager.shutdown();
      }
      if (networkServer != null) {
        networkServer.shutdown();
      }
      if (requestHandlerPool != null) {
        requestHandlerPool.shutdown();
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

  void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}
