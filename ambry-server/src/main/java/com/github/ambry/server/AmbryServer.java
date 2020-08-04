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
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceCallback;
import com.github.ambry.account.AccountServiceFactory;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.ClusterSpectator;
import com.github.ambry.clustermap.ClusterSpectatorFactory;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettyInternalMetrics;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
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
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SocketServer;
import com.github.ambry.network.http2.Http2BlockingChannelPool;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.replication.CloudToStoreReplicationManager;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationSkipPredicate;
import com.github.ambry.rest.NettyMetrics;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.NioServerFactory;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.commons.Callback;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
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
  private static final Logger logger = LoggerFactory.getLogger(AmbryServer.class);
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
  private NioServer nettyHttp2Server;
  private ParticipantsConsistencyChecker consistencyChecker = null;
  private ScheduledFuture<?> consistencyCheckerTask = null;
  private ScheduledExecutorService consistencyCheckerScheduler = null;
  private ServerSecurityService serverSecurityService;
  private final NettyInternalMetrics nettyInternalMetrics;

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      ClusterSpectatorFactory clusterSpectatorFactory, Time time) throws InstantiationException {
    this(properties, clusterAgentsFactory, clusterSpectatorFactory, new LoggingNotificationSystem(), time);
  }

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, Time time) throws InstantiationException {
    this(properties, clusterAgentsFactory, null, notificationSystem, time);
  }

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      ClusterSpectatorFactory clusterSpectatorFactory, NotificationSystem notificationSystem, Time time)
      throws InstantiationException {
    this.properties = properties;
    this.clusterAgentsFactory = clusterAgentsFactory;
    this.clusterSpectatorFactory = clusterSpectatorFactory;
    this.notificationSystem = notificationSystem;
    this.time = time;

    try {
      clusterMap = clusterAgentsFactory.getClusterMap();
      logger.info("Initialized clusterMap");
      registry = clusterMap.getMetricRegistry();
      this.metrics = new ServerMetrics(registry, AmbryRequests.class, AmbryServer.class);
      ServerConfig serverConfig = new ServerConfig(properties);
      ServerSecurityServiceFactory serverSecurityServiceFactory =
          Utils.getObj(serverConfig.serverSecurityServiceFactory, properties, metrics, registry);
      serverSecurityService = serverSecurityServiceFactory.getServerSecurityService();
      nettyInternalMetrics = new NettyInternalMetrics(registry, new NettyConfig(properties));
    } catch (Exception e) {
      logger.error("Error during bootup", e);
      throw new InstantiationException("failure during bootup " + e);
    }
  }

  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      clusterParticipants = clusterAgentsFactory.getClusterParticipants();
      logger.info("Setting up JMX.");

      long startTime = SystemTime.getInstance().milliseconds();
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
      // if there are more than one participants on local node, we create a consistency checker to monitor and alert any
      // mismatch in sealed/stopped replica lists that maintained by each participant.
      if (clusterParticipants != null && clusterParticipants.size() > 1
          && serverConfig.serverParticipantsConsistencyCheckerPeriodSec > 0) {
        consistencyChecker = new ParticipantsConsistencyChecker(clusterParticipants, metrics);
        logger.info("Scheduling participants consistency checker with a period of {} secs",
            serverConfig.serverParticipantsConsistencyCheckerPeriodSec);
        consistencyCheckerScheduler = Utils.newScheduler(1, "consistency-checker-", false);
        consistencyCheckerTask = consistencyCheckerScheduler.scheduleAtFixedRate(consistencyChecker, 0,
            serverConfig.serverParticipantsConsistencyCheckerPeriodSec, TimeUnit.SECONDS);
      }
      logger.info("checking if node exists in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
      DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
      if (nodeId == null) {
        throw new IllegalArgumentException("The node " + networkConfig.hostName + ":" + networkConfig.port
            + "is not present in the clustermap. Failing to start the datanode");
      }

      AccountServiceFactory accountServiceFactory =
          Utils.getObj(serverConfig.serverAccountServiceFactory, properties, registry);
      AccountService accountService = accountServiceFactory.getAccountService();

      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      // In most cases, there should be only one participant in the clusterParticipants list. If there are more than one
      // and some components require sole participant, the first one in the list will be primary participant.
      storageManager =
          new StorageManager(storeConfig, diskManagerConfig, scheduler, registry, storeKeyFactory, clusterMap, nodeId,
              new BlobStoreHardDelete(), clusterParticipants, time, new BlobStoreRecovery(), accountService);
      storageManager.start();

      SSLFactory sslFactory = new NettySslHttp2Factory(sslConfig);

      if (replicationConfig.replicationEnableHttp2) {
        connectionPool = new Http2BlockingChannelPool(sslFactory, new Http2ClientConfig(properties),
            new Http2ClientMetrics(registry));
      } else {
        connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, registry);
      }
      connectionPool.start();

      StoreKeyConverterFactory storeKeyConverterFactory =
          Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties, registry);

      Predicate<MessageInfo> skipPredicate = new ReplicationSkipPredicate(accountService, replicationConfig);
      replicationManager =
          new ReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
              clusterMap, scheduler, nodeId, connectionPool, registry, notificationSystem, storeKeyConverterFactory,
              serverConfig.serverMessageTransformer, clusterParticipants.get(0), skipPredicate);
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
      requests = new AmbryServerRequests(storageManager, networkServer.getRequestResponseChannel(), clusterMap, nodeId,
          registry, metrics, findTokenHelper, notificationSystem, replicationManager, storeKeyFactory, serverConfig,
          storeKeyConverterFactory, statsManager, clusterParticipants.get(0));
      requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads,
          networkServer.getRequestResponseChannel(), requests);
      networkServer.start();

      // Start netty http2 server
      if (nodeId.hasHttp2Port()) {
        NettyConfig nettyConfig = new NettyConfig(properties);
        NettyMetrics nettyMetrics = new NettyMetrics(registry);
        Http2ServerMetrics http2ServerMetrics = new Http2ServerMetrics(registry);
        Http2ClientConfig http2ClientConfig = new Http2ClientConfig(properties);

        logger.info("Http2 port {} is enabled. Starting HTTP/2 service.", nodeId.getHttp2Port());
        NettyServerRequestResponseChannel requestResponseChannel =
            new NettyServerRequestResponseChannel(networkConfig.queuedMaxRequests, http2ServerMetrics);

        AmbryServerRequests ambryServerRequestsForHttp2 =
            new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, nodeId, registry, metrics,
                findTokenHelper, notificationSystem, replicationManager, storeKeyFactory, serverConfig,
                storeKeyConverterFactory, statsManager, clusterParticipants.get(0));
        requestHandlerPoolForHttp2 =
            new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads, requestResponseChannel,
                ambryServerRequestsForHttp2);

        NioServerFactory nioServerFactory =
            new StorageServerNettyFactory(nodeId.getHttp2Port(), requestResponseChannel, sslFactory, nettyConfig,
                http2ClientConfig, metrics, nettyMetrics, http2ServerMetrics, serverSecurityService);
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
      Callback<StatsSnapshot> accountServiceCallback = new AccountServiceCallback(accountService);
      for (ClusterParticipant clusterParticipant : clusterParticipants) {
        clusterParticipant.participate(ambryHealthReports, accountServiceCallback);
      }

      if (nettyInternalMetrics != null) {
        nettyInternalMetrics.start();
        logger.info("NettyInternalMetric starts");
      }
      logger.info("started");
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.serverStartTimeInMs.update(processingTime);
      logger.info("Server startup time in Ms {}", processingTime);
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
      if (nettyInternalMetrics != null) {
        nettyInternalMetrics.stop();
      }
      if (consistencyCheckerTask != null) {
        consistencyCheckerTask.cancel(false);
      }
      if (clusterParticipants != null) {
        clusterParticipants.forEach(ClusterParticipant::close);
      }
      if (consistencyCheckerScheduler != null) {
        shutDownExecutorService(consistencyCheckerScheduler, 5, TimeUnit.MINUTES);
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
      if (serverSecurityService != null) {
        serverSecurityService.close();
      }

      logger.info("shutdown completed");
    } catch (Exception e) {
      logger.error("Error while shutting down server", e);
    } finally {
      shutdownLatch.countDown();
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.serverShutdownTimeInMs.update(processingTime);
      logger.info("Server shutdown time in Ms {}", processingTime);
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  /**
   * Exposed for testing.
   * @return {@link ServerMetrics}
   */
  ServerMetrics getServerMetrics() {
    return metrics;
  }
}
