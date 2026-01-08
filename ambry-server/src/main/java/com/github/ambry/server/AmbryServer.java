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
import com.github.ambry.accountstats.AccountStatsMySqlStore;
import com.github.ambry.accountstats.AccountStatsMySqlStoreFactory;
import com.github.ambry.cloud.BackupIntegrityMonitor;
import com.github.ambry.cloud.RecoveryManager;
import com.github.ambry.cloud.RecoveryNetworkClientFactory;
import com.github.ambry.clustermap.AmbryServerDataNode;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.CompositeClusterManager;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.StaticClusterManager;
import com.github.ambry.clustermap.VcrClusterAgentsFactory;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettyInternalMetrics;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicaPrioritizationConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.ServerExecutionMode;
import com.github.ambry.config.ServerReplicationMode;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.filetransfer.FileCopyBasedReplicationManager;
import com.github.ambry.filetransfer.FileCopyBasedReplicationSchedulerFactory;
import com.github.ambry.filetransfer.FileCopyBasedReplicationSchedulerFactoryImpl;
import com.github.ambry.filetransfer.FileCopyMetrics;
import com.github.ambry.filetransfer.handler.FileCopyHandlerFactory;
import com.github.ambry.filetransfer.handler.StoreFileCopyHandlerFactory;
import com.github.ambry.messageformat.BlobStoreHardDelete;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.ServerRequestResponseHelper;
import com.github.ambry.network.SocketNetworkClientFactory;
import com.github.ambry.network.SocketServer;
import com.github.ambry.network.http2.Http2BlockingChannelPool;
import com.github.ambry.network.http2.Http2ClientMetrics;
import com.github.ambry.network.http2.Http2NetworkClientFactory;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.repair.RepairRequestsDb;
import com.github.ambry.repair.RepairRequestsDbFactory;
import com.github.ambry.replica.prioritization.FCFSPrioritizationManager;
import com.github.ambry.replica.prioritization.FileBasedReplicationPrioritizationManagerFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.replica.prioritization.PrioritizationManagerFactory;
import com.github.ambry.replica.prioritization.ReplicationPrioritizationManager;
import com.github.ambry.replica.prioritization.disruption.DisruptionService;
import com.github.ambry.replica.prioritization.disruption.factory.DisruptionServiceFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationSkipPredicate;
import com.github.ambry.rest.NettyMetrics;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.NioServerFactory;
import com.github.ambry.rest.ServerSecurityService;
import com.github.ambry.rest.ServerSecurityServiceFactory;
import com.github.ambry.rest.StorageServerNettyFactory;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.store.MessageInfo;
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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * Ambry server
 */
public class AmbryServer {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final CountDownLatch dataNodeLatch = new CountDownLatch(1);
  private NetworkServer networkServer = null;
  private AmbryRequests requests = null;
  private RequestHandlerPool requestHandlerPool = null;
  private ScheduledExecutorService scheduler = null;
  private StorageManager storageManager = null;
  private FileCopyBasedReplicationManager fileCopyBasedReplicationManager = null;
  private PrioritizationManager prioritizationManager = null;
  private StatsManager statsManager = null;
  private ReplicationManager replicationManager = null;
  private RecoveryManager recoveryManager = null;
  private static final Logger logger = LoggerFactory.getLogger(AmbryServer.class);
  private final VerifiableProperties properties;
  private final ClusterAgentsFactory clusterAgentsFactory;
  private final Function<MetricRegistry, JmxReporter> reporterFactory;
  private ClusterMap clusterMap;
  private ClusterMapConfig clusterMapConfig;
  private List<ClusterParticipant> clusterParticipants;
  private MetricRegistry registry = null;
  private JmxReporter reporter = null;
  private ConnectionPool connectionPool = null;
  private NetworkClientFactory networkClientFactory;
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
  private AccountStatsMySqlStore accountStatsMySqlStore = null;

  // variables to handle repair requests.
  private LocalRequestResponseChannel localChannel = null;
  private RequestHandlerPool repairHandlerPool = null;
  private RepairRequestsSender repairRequestsSender = null;
  private Thread repairThread = null;
  private RepairRequestsDb repairRequestsDb = null;
  private BackupIntegrityMonitor backupIntegrityMonitor = null;

  // Replication Prioritization Manager
  private ReplicationPrioritizationManager replicationPrioritizationManager = null;

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      VcrClusterAgentsFactory vcrClusterAgentsFactory, Time time) throws InstantiationException {
    this(properties, clusterAgentsFactory, vcrClusterAgentsFactory, new LoggingNotificationSystem(), time, null);
  }

  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, Time time) throws InstantiationException {
    this(properties, clusterAgentsFactory, null, notificationSystem, time, null);
  }

  /**
   * @param properties {@link VerifiableProperties} object containing configs for the server.
   * @param clusterAgentsFactory The {@link ClusterAgentsFactory} to use.
   * @param vcrClusterAgentsFactory a {@link VcrClusterAgentsFactory} instance. Required if replicating from a VCR.
   * @param notificationSystem the {@link NotificationSystem} to use.
   * @param time The {@link Time} instance to use.
   * @param reporterFactory if non-null, use this function to set up a {@link JmxReporter} with custom settings. If this
   *                        option is null the default settings for the reporter will be used.
   * @throws InstantiationException if there was an error during startup.
   */
  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      VcrClusterAgentsFactory vcrClusterAgentsFactory, NotificationSystem notificationSystem, Time time,
      Function<MetricRegistry, JmxReporter> reporterFactory) throws InstantiationException {
    this.properties = properties;
    this.clusterAgentsFactory = clusterAgentsFactory;
    this.notificationSystem = notificationSystem;
    this.time = time;
    this.reporterFactory = reporterFactory;

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
      this.clusterMapConfig = new ClusterMapConfig(properties);
      ClusterMapChangeListener clusterMapListener = new AmbryServerClusterMapChangeListenerImpl(
          ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort),
          dataNodeLatch);
      clusterMap.registerClusterMapListener(clusterMapListener);
    } catch (Exception e) {
      logger.error("Error during bootup", e);
      throw new InstantiationException("failure during bootup " + e);
    }
  }

  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      clusterParticipants = clusterAgentsFactory.getClusterParticipants();
      if (clusterParticipants == null || clusterParticipants.isEmpty()) {
        logger.info("Cluster participant list is null or empty");
      }
      ClusterParticipant clusterParticipant = clusterParticipants == null || clusterParticipants.isEmpty() ? null : clusterParticipants.get(0);
      logger.info("Setting up JMX.");

      long startTime = SystemTime.getInstance().milliseconds();
      reporter = reporterFactory != null ? reporterFactory.apply(registry) : JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      DiskManagerConfig diskManagerConfig = new DiskManagerConfig(properties);
      ServerConfig serverConfig = new ServerConfig(properties);
      ReplicationConfig replicationConfig = new ReplicationConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      SSLConfig sslConfig = new SSLConfig(properties);
      StatsManagerConfig statsConfig = new StatsManagerConfig(properties);
      FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig = new FileCopyBasedReplicationConfig(properties);
      ReplicaPrioritizationConfig replicaPrioritizationConfig = new ReplicaPrioritizationConfig(properties);
      // verify the configs
      properties.verify();

      AccountServiceFactory accountServiceFactory =
          Utils.getObj(serverConfig.serverAccountServiceFactory, properties, registry);
      AccountService accountService = accountServiceFactory.getAccountService();

      SSLFactory sslHttp2Factory = new NettySslHttp2Factory(sslConfig);
      StoreKeyConverterFactory storeKeyConverterFactory =
          Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties, registry);

      if (serverConfig.serverExecutionMode.equals(ServerExecutionMode.DATA_RECOVERY_MODE)) {
        logger.info("Server execution mode is DATA_RECOVERY_MODE");
        /**
         * Recovery from cloud. When the server is restoring a backup from cloud, it will not replicate from peers.
         * We need to use RecoveryManager for one reason. It will add one replica for the server to replicate from
         * which is the Cloud. The regular ReplicationManager will add 3 replicas, and as a result 3 threads will try
         * to fetch the same data from Azure. Although this can be avoided with a minor change to ReplicationManager,
         * having a separate RecoveryManager for cloud offers the flexibility to override methods and add more
         * suited to cloud.
         */
        StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
        DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
        if (nodeId == null) {
          throw new IllegalArgumentException(String.format("Node %s absent in cluster-map", networkConfig.hostName));
        }
        // In most cases, there should be only one participant in the clusterParticipants list. If there are more than one
        // and some components require sole participant, the first one in the list will be primary participant.
        scheduler = Utils.newScheduler(1, true);
        storageManager =
            new StorageManager(storeConfig, diskManagerConfig, scheduler, registry, storeKeyFactory, clusterMap, nodeId,
                new BlobStoreHardDelete(), clusterParticipants, time, new BlobStoreRecovery(), accountService);
        storageManager.start();
        networkClientFactory =
            new RecoveryNetworkClientFactory(properties, registry, clusterMap, storageManager, accountService);
        recoveryManager =
            new RecoveryManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
                clusterMap, scheduler, nodeId, networkClientFactory, registry, notificationSystem,
                storeKeyConverterFactory, serverConfig.serverMessageTransformer, null, null);
        recoveryManager.start();
      } else if (serverConfig.serverExecutionMode.equals(ServerExecutionMode.DATA_VERIFICATION_MODE)) {
        logger.info("Server execution mode is DATA_VERIFICATION_MODE");
        /**
         * We need composite cluster manager because it has both static and helix cluster manager.
         * We use helix-cluster-map to discover replicas of a partition.
         * We use static-cluster-map to discover local disks on the host.
         * The static map returns objects used for static maps. We need to convert them to be suitable for helix map.
         */
        if (!(clusterMap instanceof CompositeClusterManager)) {
          throw new InstantiationException("Cluster manager must be of type CompositeClusterManager");
        }
        CompositeClusterManager compositeClusterManager = (CompositeClusterManager) clusterMap;
        HelixClusterManager helixClusterManager = compositeClusterManager.getHelixClusterManager();
        StaticClusterManager staticClusterManager = compositeClusterManager.getStaticClusterManager();
        DataNodeId nodeId = staticClusterManager.getDataNodeId(networkConfig.hostName, networkConfig.port);
        if (nodeId == null) {
          throw new IllegalArgumentException(String.format("Node %s absent in cluster-map", networkConfig.hostName));
        }
        // Store key factory is instantiated in two places : here and in internal fork. Careful!
        StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, helixClusterManager);
        scheduler = Utils.newScheduler(1, true);
        storageManager =
            new StorageManager(storeConfig, diskManagerConfig, scheduler, registry, storeKeyFactory, staticClusterManager, nodeId,
                new BlobStoreHardDelete(), clusterParticipants, time, new BlobStoreRecovery(), accountService);
        storageManager.start();
        /**
         * Backup integrity monitor here because vcr does not have code to store to disk. Only server does.
         * DataNodeId -> AmbryDataNode -> AmbryServerDataNode : for helix
         * DataNodeId -> DataNode : for static
         */
        AmbryServerDataNode helixNode = new AmbryServerDataNode(nodeId, clusterMapConfig);
        recoveryManager =
            new RecoveryManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
                helixClusterManager, null, helixNode,
                new RecoveryNetworkClientFactory(properties, registry, helixClusterManager, storageManager, accountService),
                registry, null, storeKeyConverterFactory, serverConfig.serverMessageTransformer,
                null, null);
        replicationManager =
            new ReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
                helixClusterManager, null, helixNode,
                new Http2NetworkClientFactory(new Http2ClientMetrics(registry), new Http2ClientConfig(properties), sslHttp2Factory, time),
                registry, null, storeKeyConverterFactory, serverConfig.serverMessageTransformer,
                null, null);
        backupIntegrityMonitor = new BackupIntegrityMonitor(recoveryManager, replicationManager,
            compositeClusterManager, storageManager, helixNode, properties);
        backupIntegrityMonitor.start();
      } else if (serverConfig.serverExecutionMode.equals(ServerExecutionMode.DATA_SERVING_MODE)) {
        logger.info("Server execution mode is DATA_SERVING_MODE");
        scheduler = Utils.newScheduler(serverConfig.serverSchedulerNumOfthreads, false);
        accountStatsMySqlStore =
            statsConfig.enableMysqlReport ? (AccountStatsMySqlStore) new AccountStatsMySqlStoreFactory(properties,
                clusterMapConfig, registry).getAccountStatsStore() : null;

        StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
        FindTokenHelper findTokenHelper = new FindTokenHelper(storeKeyFactory, replicationConfig);
        Callback<AggregatedAccountStorageStats> accountServiceCallback = new AccountServiceCallback(accountService);

        logger.info("checking if node exists in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
        DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
        // Flow for new node ADD
        // Instance config is populated with auto-registration
        // Once DataNode config is populated, Storage, Replication and Stats Manager are created
        // State Machine Model and other tasks are not registered until then to the HelixParticipant
        List<AmbryStatsReport> ambryStatsReports = getAmbryStatsReports(serverConfig);

        for (ClusterParticipant participant : clusterParticipants) {
          participant.participateAndBlockStateTransition(ambryStatsReports, accountStatsMySqlStore, accountServiceCallback);
        }

        // wait for dataNode to be populated
        if (nodeId == null) {
          if (clusterParticipant != null && !clusterParticipant.populateDataNodeConfig()) {
            logger.error("Failed to populate data node config to property store for instance: {}", networkConfig.hostName);
          }
          logger.info("Waiting on dataNode config to be populated...");
          if(!dataNodeLatch.await(serverConfig.serverDatanodeConfigTimeout, TimeUnit.SECONDS)) {
            throw new IllegalArgumentException("Startup timed out waiting for data node config to be populated");
          }
          nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
          if (nodeId == null) {
            throw new IllegalArgumentException(String.format("Node %s absent in cluster-map", networkConfig.hostName));
          }
          logger.info("DataNode config is populated");
        }

        // In most cases, there should be only one participant in the clusterParticipants list. If there are more than one
        // and some components require sole participant, the first one in the list will be primary participant.
        logger.info("Creating Storage Manager");
        storageManager =
            new StorageManager(storeConfig, diskManagerConfig, scheduler, registry, storeKeyFactory, clusterMap, nodeId,
                new BlobStoreHardDelete(), clusterParticipants, time, new BlobStoreRecovery(), accountService);
        storageManager.start();

        logger.info("Creating StatsManager to publish stats");
        statsManager =
            new StatsManager(storageManager, clusterMap, clusterMap.getReplicaIds(nodeId),
                registry, statsConfig, time, clusterParticipant, accountStatsMySqlStore, accountService, nodeId);
        statsManager.start();

        // if there are more than one participant on local node, we create a consistency checker to monitor and alert any
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

        if (clusterMapConfig.clusterMapEnableHttp2Replication) {
          Http2ClientMetrics http2ClientMetrics = new Http2ClientMetrics(registry);
          Http2ClientConfig http2ClientConfig = new Http2ClientConfig(properties);
          networkClientFactory =
              new Http2NetworkClientFactory(http2ClientMetrics, http2ClientConfig, sslHttp2Factory, time);
          connectionPool = new Http2BlockingChannelPool(sslHttp2Factory, http2ClientConfig, http2ClientMetrics);
        } else {
          SSLFactory sslSocketFactory =
              clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0 ? SSLFactory.getNewInstance(sslConfig) : null;
          networkClientFactory =
              new SocketNetworkClientFactory(new NetworkMetrics(registry), networkConfig, sslSocketFactory, 20, 20, 50000,
                  time);
          connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, registry);
        }
        Predicate<MessageInfo> skipPredicate = new ReplicationSkipPredicate(accountService, replicationConfig);
        replicationManager =
            new ReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
                clusterMap, scheduler, nodeId, networkClientFactory, registry, notificationSystem,
                storeKeyConverterFactory, serverConfig.serverMessageTransformer, clusterParticipant,
                skipPredicate);
        replicationManager.start();

        DisruptionService disruptionService = null;
        if (serverConfig.serverReplicationProtocolForHydration.equals(ServerReplicationMode.FILE_BASED)
            && clusterMap instanceof HelixClusterManager) {
          DisruptionServiceFactory disruptionServiceFactory =
              Utils.getObj(replicationConfig.disruptionServiceFactory, properties, nodeId.getDatacenterName());
          disruptionService = disruptionServiceFactory.getDisruptionService();

          FileCopyMetrics fileCopyMetrics =
              new FileCopyMetrics(registry, fileCopyBasedReplicationConfig.fileCopyMetricsReservoirTimeWindowMs);
          FileCopyHandlerFactory fileCopyHandlerFactory =
              new StoreFileCopyHandlerFactory(connectionPool, storageManager, clusterMap,
                  fileCopyBasedReplicationConfig, storeConfig, fileCopyMetrics);

          PrioritizationManagerFactory prioritizationManagerFactory =
              new FileBasedReplicationPrioritizationManagerFactory(disruptionService,
                  ((HelixClusterManager) clusterMap).getManagerQueryHelper(), nodeId.getDatacenterName());
          prioritizationManager = prioritizationManagerFactory.getPrioritizationManager(
              replicaPrioritizationConfig.replicaPrioritizationStrategy);
          prioritizationManager.start();
          FileCopyBasedReplicationSchedulerFactory fileCopyBasedReplicationSchedulerFactory =
              new FileCopyBasedReplicationSchedulerFactoryImpl(fileCopyHandlerFactory, fileCopyBasedReplicationConfig,
                  clusterMap, prioritizationManager, storageManager, storeConfig, nodeId, clusterParticipant,
                  fileCopyMetrics);
          fileCopyBasedReplicationManager =
              new FileCopyBasedReplicationManager(fileCopyBasedReplicationConfig, clusterMapConfig, storageManager,
                  clusterMap, networkClientFactory, fileCopyMetrics, clusterParticipant,
                  fileCopyBasedReplicationSchedulerFactory, fileCopyHandlerFactory, prioritizationManager, storeConfig,
                  replicaPrioritizationConfig);
          fileCopyBasedReplicationManager.start();
        }
        // unblock state transition
        logger.info("Unblocking state transition");
        for (ClusterParticipant participant : clusterParticipants) {
          participant.unblockStateTransition();
        }

        ArrayList<Port> ports = new ArrayList<Port>();
        ports.add(new Port(networkConfig.port, PortType.PLAINTEXT));
        if (nodeId.hasSSLPort()) {
          ports.add(new Port(nodeId.getSSLPort(), PortType.SSL));
        }
        networkServer = new SocketServer(networkConfig, sslConfig, registry, ports);
        requests = new AmbryServerRequests(storageManager, networkServer.getRequestResponseChannel(), clusterMap, nodeId,
            registry, metrics, findTokenHelper, notificationSystem, replicationManager, storeKeyFactory, serverConfig,
            diskManagerConfig, storeKeyConverterFactory, statsManager, clusterParticipant);
        requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumSocketServerThreads,
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
              new NettyServerRequestResponseChannel(networkConfig, http2ServerMetrics, metrics,
                  new ServerRequestResponseHelper(clusterMap, findTokenHelper));
          AmbryServerRequests ambryServerRequestsForHttp2 =
              new AmbryServerRequests(storageManager, requestResponseChannel, clusterMap, nodeId, registry, metrics,
                  findTokenHelper, notificationSystem, replicationManager, storeKeyFactory, serverConfig,
                  diskManagerConfig, storeKeyConverterFactory, statsManager, clusterParticipant, connectionPool);
          requestHandlerPoolForHttp2 =
              new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads, requestResponseChannel,
                  ambryServerRequestsForHttp2);

          NioServerFactory nioServerFactory =
              new StorageServerNettyFactory(nodeId.getHttp2Port(), requestResponseChannel, sslHttp2Factory, nettyConfig,
                  http2ClientConfig, metrics, nettyMetrics, http2ServerMetrics, serverSecurityService);
          nettyHttp2Server = nioServerFactory.getNioServer();
          nettyHttp2Server.start();
        }

        if (serverConfig.serverRepairRequestsDbFactory != null) {
          // if we have a RepairRequestsDB, start the threads to fix the partially failed requests.
          try {
            RepairRequestsDbFactory factory =
                Utils.getObj(serverConfig.serverRepairRequestsDbFactory, properties, registry, nodeId.getDatacenterName(),
                    time);
            repairRequestsDb = factory.getRepairRequestsDb();

            localChannel = new LocalRequestResponseChannel();
            LocalNetworkClientFactory localClientFactory =
                new LocalNetworkClientFactory(localChannel, networkConfig, new NetworkMetrics(registry), time);
            AmbryRequests repairRequests =
                new AmbryServerRequests(storageManager, localChannel, clusterMap, nodeId, registry, metrics,
                    findTokenHelper, notificationSystem, replicationManager, storeKeyFactory, serverConfig,
                    diskManagerConfig, storeKeyConverterFactory, statsManager, clusterParticipant,
                    connectionPool);
            // Right now we only open single thread for the repairHandlerPool
            repairHandlerPool = new RequestHandlerPool(1, localChannel, repairRequests, "Repair-");
            // start the repairRequestSender. It sends requests through the localChannel to the repairHandlerPool
            repairRequestsSender =
                new RepairRequestsSender(localChannel, localClientFactory, clusterMap, nodeId, repairRequestsDb,
                    clusterParticipant, registry, storageManager);
            repairThread = Utils.daemonThread("Repair-Sender", repairRequestsSender);
            repairThread.start();
            logger.info("RepairRequests: open the db and started the handling thread {}.", repairRequestsDb);
          } catch (Exception e) {
            logger.error("RepairRequests: Cannot connect to the RepairRequestsDB. ", e);
          }
        }

        if (replicationConfig.enableReplicationPrioritization && clusterMap instanceof HelixClusterManager) {
          HelixClusterManager helixClusterManager = (HelixClusterManager) clusterMap;
          if (disruptionService == null) {
            DisruptionServiceFactory disruptionServiceFactory =
                Utils.getObj(replicationConfig.disruptionServiceFactory, properties, nodeId.getDatacenterName());
            disruptionService = disruptionServiceFactory.getDisruptionService();
          }
          ScheduledExecutorService scheduledExecutorService = Utils.newScheduler(1, "ambry-prioritization", false);
          replicationPrioritizationManager =
              new ReplicationPrioritizationManager(replicationManager, clusterMap, nodeId, scheduledExecutorService,
                  storageManager, replicationConfig, helixClusterManager.getManagerQueryHelper(), disruptionService,
                  registry);
        }

      } else {
        throw new IllegalArgumentException("Unknown server execution mode");
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
      if (repairRequestsSender != null) {
        repairRequestsSender.shutdown();
      }
      if (repairHandlerPool != null) {
        repairHandlerPool.shutdown();
      }
      if (repairThread != null) {
        repairThread.join();
      }
      if (repairRequestsDb != null) {
        repairRequestsDb.close();
      }

      if (localChannel != null) {
        localChannel.shutdown();
      }
      if (backupIntegrityMonitor != null) {
        backupIntegrityMonitor.shutdown();
      }
      if (recoveryManager != null) {
        recoveryManager.shutdown();
      }
      if (replicationManager != null) {
        replicationManager.shutdown();
      }
      if (storageManager != null) {
        storageManager.shutdown();
      }
      if (fileCopyBasedReplicationManager != null) {
       fileCopyBasedReplicationManager.shutdown();
      }
      if(prioritizationManager != null){
        prioritizationManager.shutdown();
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

      if (replicationPrioritizationManager != null) {
        replicationPrioritizationManager.shutdown();
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

  /**
   * Retrieves a list of AmbryStatsReport objects based on the server configuration.
   *
   * @param serverConfig The {@link ServerConfig} containing the server's configuration.
   * @return A list of {@link AmbryStatsReport} objects to be published.
   */
  private List<AmbryStatsReport> getAmbryStatsReports(ServerConfig serverConfig) {
    // Other code
    List<AmbryStatsReport> ambryStatsReports = new ArrayList<>();
    Set<String> validStatsTypes = new HashSet<>();
    for (StatsReportType type : StatsReportType.values()) {
      validStatsTypes.add(type.toString());
    }

    // StatsManager would publish account stats to AccountStatsStore, and optionally publish partition class stats
    // as well. So if serverStatsReportsToPublish contains PARTITION_CLASS_REPORT, then be sure to enable partition
    // class stats with StatsManagerConfig.publishPartitionClassReportPeriodInSecs.
    // Also, since StatsManager is tightly coupled with mysql database right now, if variable accountStatsMySqlStore
    // is null, there is no report to aggregate and publish.
    if (!serverConfig.serverStatsReportsToPublish.isEmpty() && accountStatsMySqlStore != null) {
      serverConfig.serverStatsReportsToPublish.forEach(e -> {
        if (validStatsTypes.contains(e)) {
          ambryStatsReports.add(new AmbryStatsReportImpl(serverConfig.serverQuotaStatsAggregateIntervalInMinutes,
              StatsReportType.valueOf(e)));
        }
      });
    }

    return ambryStatsReports;
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
