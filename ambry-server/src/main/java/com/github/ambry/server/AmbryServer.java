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
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.CompositeClusterManager;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StaticClusterManager;
import com.github.ambry.clustermap.VcrClusterAgentsFactory;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettyInternalMetrics;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.ServerExecutionMode;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreHardDelete;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
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
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.repair.RepairRequestsDb;
import com.github.ambry.repair.RepairRequestsDbFactory;
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
import com.github.ambry.store.FileInfo;
import com.github.ambry.store.FileStore;
import com.github.ambry.store.LogInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
  private NetworkServer networkServer = null;
  private AmbryRequests requests = null;
  private RequestHandlerPool requestHandlerPool = null;
  private ScheduledExecutorService scheduler = null;
  private StorageManager storageManager = null;
  private StatsManager statsManager = null;
  private ReplicationManager replicationManager = null;
  private RecoveryManager recoveryManager = null;
  private static final Logger logger = LoggerFactory.getLogger(AmbryServer.class);
  private final VerifiableProperties properties;
  private final ClusterAgentsFactory clusterAgentsFactory;
  private final Function<MetricRegistry, JmxReporter> reporterFactory;
  private ClusterMap clusterMap;
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
  private DataNodeId nodeId;
  private FileStore fileStore;

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
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
      StatsManagerConfig statsConfig = new StatsManagerConfig(properties);
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
        this.nodeId = nodeId;
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
        logger.info("checking if node exists in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
        DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
        this.nodeId = nodeId;
        if (nodeId == null) {
          throw new IllegalArgumentException("The node " + networkConfig.hostName + ":" + networkConfig.port
              + "is not present in the clustermap. Failing to start the datanode");
        }
        StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
        FindTokenHelper findTokenHelper = new FindTokenHelper(storeKeyFactory, replicationConfig);
        // In most cases, there should be only one participant in the clusterParticipants list. If there are more than one
        // and some components require sole participant, the first one in the list will be primary participant.
        storageManager =
            new StorageManager(storeConfig, diskManagerConfig, scheduler, registry, storeKeyFactory, clusterMap, nodeId,
                new BlobStoreHardDelete(), clusterParticipants, time, new BlobStoreRecovery(), accountService);
        storageManager.start();

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


        logger.info("Creating StatsManager to publish stats");

        accountStatsMySqlStore =
            statsConfig.enableMysqlReport ? (AccountStatsMySqlStore) new AccountStatsMySqlStoreFactory(properties,
                clusterMapConfig, registry).getAccountStatsStore() : null;
        statsManager =
            new StatsManager(storageManager, clusterMap, clusterMap.getReplicaIds(nodeId), registry, statsConfig, time,
                clusterParticipant, accountStatsMySqlStore, accountService, nodeId);
        statsManager.start();

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

        Callback<AggregatedAccountStorageStats> accountServiceCallback = new AccountServiceCallback(accountService);
        for (ClusterParticipant participant : clusterParticipants) {
          participant.participate(ambryStatsReports, accountStatsMySqlStore, accountServiceCallback);
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

      FileCopyConfig fileCopyConfig = new FileCopyConfig(properties);
      this.fileStore = new FileStore("dataDir", fileCopyConfig);
      fileStore.start();

      long startTimeMs = System.currentTimeMillis();
      testE2EFlow();
      logger.info("Demo: E2E flow took {} ms", System.currentTimeMillis() - startTimeMs);

//      testFileChunkAggregationForFileCopy();
//      testStateBuildPostFileCopy();
//      testFileStoreUtils();
    } catch (Exception e) {
      logger.error("Error during startup", e);
      throw new InstantiationException("failure during startup " + e);
    }
  }

  private void testE2EFlow() {
    Optional<PartitionId> optional = storageManager.getLocalPartitions().stream().filter(p -> p.getId() == 146).findFirst();
    PartitionId partitionId;
    if (optional.isPresent()) {
      partitionId = optional.get();
    } else {
      logger.info("Demo: Partition not found");
      return;
    }
    Optional<? extends ReplicaId> targetReplica = partitionId.getReplicaIds().stream().filter(replicaId ->
        replicaId.getDataNodeId().getHostname().equals("ltx1-app3645.stg.linkedin.com")).findFirst();
    ReplicaId targetReplicaId;
    if (targetReplica.isPresent()) {
      targetReplicaId = targetReplica.get();
    } else {
      logger.info("Demo: Target Replica not found");
      return;
    }
    Optional<? extends ReplicaId> sourceReplica = partitionId.getReplicaIds().stream().filter(replicaId1 ->
        replicaId1.getDataNodeId().getHostname().equals("ltx1-app3602.stg.linkedin.com")).findFirst();
    ReplicaId sourceReplicaId;
    if (sourceReplica.isPresent()) {
      sourceReplicaId = sourceReplica.get();
    } else {
      logger.info("Demo: Source Replica not found");
      return;
    }

    if (nodeId.getHostname().equals("ltx1-app3602.stg.linkedin.com")) {
      logger.info("Demo: Source host. Initiating file copy based bootstrap...");

      File directory = new File(sourceReplicaId.getReplicaPath());
      if (directory.exists() && directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.isFile()) {
            file.delete();
          }
        }
        logger.info("Demo: All files deleted.");
      } else {
        logger.info("Demo: Directory does not exist.");
      }
      try {
        long startTimeMs = System.currentTimeMillis();
        new FileCopyHandler(connectionPool, fileStore, clusterMap).copy(partitionId, sourceReplicaId, targetReplicaId);
        logger.info("Demo: FileCopyHandler took {} ms", System.currentTimeMillis() - startTimeMs);

        startTimeMs = System.currentTimeMillis();
        storageManager.buildStateForFileCopy(sourceReplicaId);
        logger.info("Demo: buildStateForFileCopy took {} ms", System.currentTimeMillis() - startTimeMs);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else if (nodeId.getHostname().equals("ltx1-app3679.stg.linkedin.com")) {
      logger.info("Demo: Tester host. Making GetBlob request...");
      while (true) {
        try {
          ConnectedChannel connectedChannel =
              connectionPool.checkOutConnection(sourceReplicaId.getDataNodeId().getHostname(), sourceReplicaId.getDataNodeId().getPortToConnectTo(), 99999);

          long startTimeMs = System.currentTimeMillis();
          GetResponse response = getBlob(new BlobId("AAYAAgBoAAMAAQAAAAAAAACSsveRrYTJQfW46D8DJ6T0Pw", clusterMap), connectedChannel);
          logger.info("Demo: TestGetBlob took {} ms", System.currentTimeMillis() - startTimeMs);

          logger.info("Demo: TestGetBlob response: {}", response);
          response.getPartitionResponseInfoList().forEach(partitionResponseInfo -> {
            partitionResponseInfo.getMessageInfoList().forEach(messageInfo -> {
              logger.info("Demo: TestGetBlob message: {}", messageInfo);
            });
          });
          if (response.getError().equals(ServerErrorCode.No_Error)) {
            break;
          }
        } catch (Exception e) {
          logger.error("Demo: TestGetBlob: Exception occurred", e);
        } finally {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {}
        }
      }
    }
  }

  private void testFileStoreUtils() throws StoreException, IOException {
    // Testing FileStore utils
    FileCopyConfig fileCopyConfig = new FileCopyConfig(properties);
    FileStore fileStore = new FileStore("test", fileCopyConfig);
    fileStore.start();
    List<LogInfo> logInfoList = Collections.singletonList(new LogInfo(new FileInfo("0_log", 20000L),
        Collections.singletonList(new FileInfo("0_index", 100L)),
        Collections.singletonList(new FileInfo("0_bloom", 50L))));
    System.out.println("Persisting metadata" + logInfoList + " to file");
    fileStore.persistMetaDataToFile("/tmp/0/", logInfoList);
    System.out.println("Reading metadata" + fileStore.readMetaDataFromFile("/tmp/0/") + " from file");
    String chunkPath = "/tmp/0/test_chunk";
    try (FileInputStream inputStream = new FileInputStream(chunkPath)) {
      System.out.println("Trying putChunkToFile for chunk at " + chunkPath);
      fileStore.putChunkToFile("/tmp/0/0_log", inputStream);
    } catch (IOException e) {
      System.err.println("An error occurred: " + e.getMessage());
    }

    int offset = 10;
    int size = 10;
    ByteBuffer byteBuffer = fileStore.readChunkForFileCopy("/tmp/0/", "0_log", offset, size);
    System.out.println("Parsed log file contents read for offset=" + offset + ", size=" + size + " is: " + StandardCharsets.UTF_8.decode(byteBuffer));
  }

  private void testFileChunkAggregationForFileCopy() throws IOException {
    String chunkPath = "/tmp/0/test_chunk";     // The path to the chunk file
    String logFilePath = "/tmp/0/0_log";        // The path to the log file where chunks are written
    String outputFilePath = "/tmp/0/output_log_copy"; // New file where the log data will be copied

    System.out.println("Testing file chunk aggregation for filecopy with Filestore");

    int numChunksToWrite = 10;   // Number of times the chunk should be written to the log file
    int chunkSize = (int)Files.size(Paths.get(chunkPath));  // Size of the chunk

    Path path = Paths.get(logFilePath);
    if (Files.exists(path)) {
      // If the file exists, delete it
      System.out.println("File exists. Deleting the file: " + logFilePath);
      Files.delete(path);  // Delete the existing file
    }
    System.out.println("Creating a new file: " + logFilePath);
    Files.createFile(path);  // Create a new file

    // Step 1: Write the chunk to the logFilePath multiple times
    for (int i = 0; i < numChunksToWrite; i++) {
      try (FileInputStream inputStream = new FileInputStream(chunkPath)) {
        System.out.println("Trying to put chunk to file for chunk at " + chunkPath);
        // Assuming fileStore.putChunkToFile() writes data from the input stream to the log file
        fileStore.putChunkToFile(logFilePath, inputStream);
        System.out.println("Written chunk " + (i + 1) + " to " + logFilePath);
      } catch (IOException e) {
        System.err.println("An error occurred while reading or writing the chunk: " + e.getMessage());
      }
    }


    // Step 2: Read from logFilePath chunk by chunk and write to a new file in the same directory
    int offset = 0;
    try (FileInputStream logInputStream = new FileInputStream(logFilePath);
        FileOutputStream outputStream = new FileOutputStream(outputFilePath)) {

      byte[] buffer = new byte[chunkSize];
      int bytesRead;
      while ((bytesRead = logInputStream.read(buffer)) != -1) {
        // Write the chunk to the new file
        outputStream.write(buffer, 0, bytesRead);
        offset += bytesRead;

        System.out.println("Writing chunk to new file at offset " + offset);
      }

      // Verify if contents of both files are same
      byte[] content1 = Files.readAllBytes(Paths.get(logFilePath));
      byte[] content2 = Files.readAllBytes(Paths.get(outputFilePath));
      // Compare the byte arrays
      if (Arrays.equals(content1, content2)) {
        System.out.println("Input and output files are identical.");
      } else {
        System.out.println("Input and output files differ.");
      }
      System.out.println("File copy completed. Data written to " + outputFilePath);
    } catch (IOException e) {
      System.err.println("An error occurred while reading or writing the log file: " + e.getMessage());
    }
  }


  private void testStateBuildPostFileCopy() throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    String partitionName = "803";
    String logFilePath = "/tmp/803/14_0_log";        // The path to the log file where chunks are written
    String outputFilePath = "/tmp/803/15_0_log"; // New file where the log data will be copied
    int chunkSize = 100*1024*1024;  // Size of the chunk
    System.out.println("Testing state build post filecopy for partitionId " + partitionName);

    // Step 2: Read from logFilePath chunk by chunk and write to a new file in the same directory
    int offset = 0;
    try (FileInputStream logInputStream = new FileInputStream(logFilePath);
        FileOutputStream outputStream = new FileOutputStream(outputFilePath)) {

      byte[] buffer = new byte[chunkSize];
      int bytesRead;
      while ((bytesRead = logInputStream.read(buffer)) != -1) {
        // Write the chunk to the new file
        outputStream.write(buffer, 0, bytesRead);
        offset += bytesRead;

        System.out.println("Writing chunk to new file at offset " + offset);
      }

      // Verify if contents of both files are same
      byte[] content1 = Files.readAllBytes(Paths.get(logFilePath));
      byte[] content2 = Files.readAllBytes(Paths.get(outputFilePath));
      // Compare the byte arrays
      if (Arrays.equals(content1, content2)) {
        System.out.println("Input and output files are identical.");
      } else {
        System.out.println("Input and output files differ.");
      }
      System.out.println("File copy completed. Data written to " + outputFilePath);
    } catch (IOException e) {
      System.err.println("An error occurred while reading or writing the log file: " + e.getMessage());
    }

    // Run state build on the aggregated output file by replacing original log file with copied file to see if the
    // state is built correctly post filecopy
    Files.delete(Paths.get(logFilePath));
    Files.move(Paths.get(outputFilePath), Paths.get(logFilePath));

    System.out.println("Renamed log file: " + outputFilePath + " to " + logFilePath );

    storageManager.buildStateForFileCopy(storageManager.getReplica(partitionName));
    System.out.println("State build successfully for partitionId: " + partitionName);

    // Perform getBlob operations on few blobs to verify is state is built correctly.

    List<BlobId> blobIdList = new ArrayList<>(2);
    blobIdList.add(new BlobId("AAYQAgZEAAgAAQAAAAAAAAMja1b_H6RbSG2fzSHaZem-SA", clusterMap));
    blobIdList.add(new BlobId("AAYQAgZEAAgAAQAAAAAAAAMj48QxOzSxRoKbgGiP59OZFw", clusterMap));
    ConnectedChannel connectedChannel =
        connectionPool.checkOutConnection("localhost", new Port(clusterMap.getDataNodeIds().get(0).getPort(), PortType.PLAINTEXT),
            5000);
    for (BlobId blobId : blobIdList) {
      System.out.println("Trying getBlob operation for blobId: " + blobId.getID());
      GetResponse getResponse = getBlob(blobId, connectedChannel);
      System.out.println("BlobId: " + blobId.getID() + " found with GetResponse: " + getResponse);
    }
  }


  /**
   * Fetches a single blob from ambry server node
   * @param blobId the {@link BlobId} that needs to be fetched
   * @param connectedChannel the {@link ConnectedChannel} to use to send and receive data
   * @throws IOException
   */
  GetResponse getBlob(BlobId blobId, ConnectedChannel connectedChannel) throws IOException {
    List<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<>();
    partitionRequestInfoList.add(new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId)));
    GetRequest getRequest = new GetRequest(1, "client1", MessageFormatFlags.All, partitionRequestInfoList, GetOption.Include_All);
    return GetResponse.readFrom(connectedChannel.sendAndReceive(getRequest).getInputStream(), clusterMap);
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
