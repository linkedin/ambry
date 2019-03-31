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
package com.github.ambry.cloud;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SocketServer;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * Virtual Cloud Replicator server
 */
public class VcrServer {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private NetworkServer networkServer = null;
  private ScheduledExecutorService scheduler = null;
  private CloudBackupManager cloudBackupManager = null;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final VerifiableProperties properties;
  private final ClusterAgentsFactory clusterAgentsFactory;
  private ClusterMap clusterMap;
  private VirtualReplicatorCluster virtualReplicatorCluster;
  private MetricRegistry registry = null;
  private JmxReporter reporter = null;
  private ConnectionPool connectionPool = null;
  private final NotificationSystem notificationSystem;
  private CloudDestinationFactory cloudDestinationFactory;
  private SSLConfig sslConfig;
  private VcrMetrics metrics = null;

  /**
   * VcrServer constructor.
   * @param properties the config properties to use.
   * @param clusterAgentsFactory the {@link ClusterAgentsFactory} to use.
   * @param notificationSystem the {@link NotificationSystem} to use.
   */
  public VcrServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem) {
    this.properties = properties;
    this.clusterAgentsFactory = clusterAgentsFactory;
    this.notificationSystem = notificationSystem;
  }

  /**
   * Test constructor.
   * @param properties the config properties to use.
   * @param clusterAgentsFactory the {@link ClusterAgentsFactory} to use.
   * @param notificationSystem the {@link NotificationSystem} to use.
   * @param cloudDestinationFactory the {@link CloudDestinationFactory} to use.
   * @param virtualReplicatorCluster the {@link VirtualReplicatorCluster} to use.
   * @param sslConfig the {@link SSLConfig} to use.
   */
  VcrServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, CloudDestinationFactory cloudDestinationFactory,
      VirtualReplicatorCluster virtualReplicatorCluster, SSLConfig sslConfig) {
    this(properties, clusterAgentsFactory, notificationSystem);
    this.cloudDestinationFactory = cloudDestinationFactory;
    this.virtualReplicatorCluster = virtualReplicatorCluster;
    this.sslConfig = sslConfig;
  }

  /**
   * Start the VCR Server.
   * @throws InstantiationException if an error was encountered during startup.
   */
  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      clusterMap = clusterAgentsFactory.getClusterMap();
      logger.info("Initialized clusterMap");

      logger.info("Setting up JMX.");
      long startTime = SystemTime.getInstance().milliseconds();
      registry = clusterMap.getMetricRegistry();
      this.metrics = new VcrMetrics(registry);
      reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      ServerConfig serverConfig = new ServerConfig(properties);
      ReplicationConfig replicationConfig = new ReplicationConfig(properties);
      CloudConfig cloudConfig = new CloudConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
      if (sslConfig == null) {
        sslConfig = new SSLConfig(properties);
      }
      // verify the configs
      properties.verify();

      // Note: using static cluster until Helix version implemented
      if (virtualReplicatorCluster == null) {
        // TODO: obtain factory from config property
        // Participate in vcr cluster?
        virtualReplicatorCluster = new StaticVcrCluster(cloudConfig, clusterMapConfig, clusterMap);
      }

      // initialize cloud destination
      if (cloudDestinationFactory == null) {
        cloudDestinationFactory = Utils.getObj(cloudConfig.cloudDestinationFactoryClass, properties, registry);
      }

      scheduler = Utils.newScheduler(serverConfig.serverSchedulerNumOfthreads, false);
      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      FindTokenFactory findTokenFactory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);

      connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, registry);
      connectionPool.start();

      StoreKeyConverterFactory storeKeyConverterFactory =
          Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties, registry);

      cloudBackupManager =
          new CloudBackupManager(cloudConfig, replicationConfig, clusterMapConfig, storeConfig, storeKeyFactory,
              clusterMap, virtualReplicatorCluster, cloudDestinationFactory, scheduler, connectionPool, registry,
              notificationSystem, storeKeyConverterFactory, serverConfig.serverMessageTransformer);
      cloudBackupManager.start();

      DataNodeId currentNode = virtualReplicatorCluster.getCurrentDataNodeId();
      ArrayList<Port> ports = new ArrayList<Port>();
      ports.add(new Port(networkConfig.port, PortType.PLAINTEXT));
      if (currentNode.hasSSLPort()) {
        ports.add(new Port(cloudConfig.vcrSslPort, PortType.SSL));
      }
      networkServer = new SocketServer(networkConfig, sslConfig, registry, ports);
      networkServer.start();

      // TODO: for recovery, need AmbryRequests and RequestHandlerPool

      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.vcrStartTimeInMs.update(processingTime);
      logger.info("VCR startup time in Ms " + processingTime);
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
      logger.info("shutdown started");
      if (scheduler != null) {
        shutDownExecutorService(scheduler, 5, TimeUnit.MINUTES);
      }
      if (networkServer != null) {
        networkServer.shutdown();
      }
      if (cloudBackupManager != null) {
        cloudBackupManager.shutdown();
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
      if (virtualReplicatorCluster != null) {
        virtualReplicatorCluster.close();
      }
      logger.info("VCR shutdown completed");
    } catch (Exception e) {
      logger.error("Error while shutting down VCR", e);
    } finally {
      shutdownLatch.countDown();
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.vcrShutdownTimeInMs.update(processingTime);
      logger.info("VCR shutdown time in Ms " + processingTime);
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}
