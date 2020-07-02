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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class to handle replication for given partitions. Responsible for the following
 * 1. Initialize class members.
 * 2. Create replica threads and distribute partitions amongst the threads
 * 3. Set up replica token persistor used to recover from shutdown/crash
 * 4. Initialize and shutdown all the components required to perform replication
 */
public abstract class ReplicationEngine implements ReplicationAPI {

  protected final ReplicationConfig replicationConfig;
  protected final ClusterMap clusterMap;
  protected final ScheduledExecutorService scheduler;
  private final AtomicInteger correlationIdGenerator;
  private final ConnectionPool connectionPool;
  private final NotificationSystem notification;
  // RemoteReplicaInfo are managed by replicaThread.
  protected final Map<String, List<ReplicaThread>> replicaThreadPoolByDc;
  protected final Map<DataNodeId, ReplicaThread> dataNodeIdToReplicaThread;
  protected final Map<String, AtomicInteger> nextReplicaThreadIndexByDc;
  private final StoreKeyFactory storeKeyFactory;
  private final List<String> sslEnabledDatacenters;
  protected final StoreKeyConverterFactory storeKeyConverterFactory;
  private final String transformerClassName;
  private final Predicate<MessageInfo> skipPredicate;
  protected final DataNodeId dataNodeId;
  protected final MetricRegistry metricRegistry;
  protected final ReplicationMetrics replicationMetrics;
  protected final FindTokenHelper tokenHelper;
  // non static logger so that extensions of this class have their own class as the logger name.
  // little impact since ReplicationEngines are long-lived objects.
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final Map<PartitionId, PartitionInfo> partitionToPartitionInfo;
  protected final Map<String, Set<PartitionInfo>> mountPathToPartitionInfos;
  protected final ReplicaSyncUpManager replicaSyncUpManager;
  protected final StoreManager storeManager;
  protected ReplicaTokenPersistor persistor = null;
  protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  protected static final short Replication_Delay_Multiplier = 5;
  protected static final String replicaTokenFileName = "replicaTokens";
  private final Time time;
  protected LeaderBasedReplicationAdmin leaderBasedReplicationAdmin = null;

  public ReplicationEngine(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, ScheduledExecutorService scheduler, DataNodeId dataNode,
      List<? extends ReplicaId> replicaIds, ConnectionPool connectionPool, MetricRegistry metricRegistry,
      NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory,
      String transformerClassName, ClusterParticipant clusterParticipant, StoreManager storeManager,
      Predicate<MessageInfo> skipPredicate) throws ReplicationException {
    this(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler, dataNode, replicaIds,
        connectionPool, metricRegistry, requestNotification, storeKeyConverterFactory, transformerClassName,
        clusterParticipant, storeManager, skipPredicate, null, SystemTime.getInstance());
  }

  public ReplicationEngine(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, ScheduledExecutorService scheduler, DataNodeId dataNode,
      List<? extends ReplicaId> replicaIds, ConnectionPool connectionPool, MetricRegistry metricRegistry,
      NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory,
      String transformerClassName, ClusterParticipant clusterParticipant, StoreManager storeManager,
      Predicate<MessageInfo> skipPredicate, FindTokenHelper findTokenHelper, Time time) throws ReplicationException {
    this.replicationConfig = replicationConfig;
    this.storeKeyFactory = storeKeyFactory;
    if (findTokenHelper == null) {
      try {
        this.tokenHelper = new FindTokenHelper(this.storeKeyFactory, this.replicationConfig);
      } catch (ReflectiveOperationException roe) {
        logger.error("Error on getting ReplicaTokenHelper", roe);
        throw new ReplicationException("Error on getting ReplicaTokenHelper");
      }
    } else {
      this.tokenHelper = findTokenHelper;
    }
    this.replicaThreadPoolByDc = new ConcurrentHashMap<>();
    this.replicationMetrics = new ReplicationMetrics(metricRegistry, replicaIds);
    this.mountPathToPartitionInfos = new ConcurrentHashMap<>();
    this.partitionToPartitionInfo = new ConcurrentHashMap<>();
    this.clusterMap = clusterMap;
    this.scheduler = scheduler;
    this.correlationIdGenerator = new AtomicInteger(0);
    this.dataNodeId = dataNode;
    this.connectionPool = connectionPool;
    this.notification = requestNotification;
    this.metricRegistry = metricRegistry;
    this.dataNodeIdToReplicaThread = new ConcurrentHashMap<>();
    this.nextReplicaThreadIndexByDc = new ConcurrentHashMap<>();
    this.sslEnabledDatacenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    this.storeKeyConverterFactory = storeKeyConverterFactory;
    this.transformerClassName = transformerClassName;
    this.storeManager = storeManager;
    this.skipPredicate = skipPredicate;
    replicaSyncUpManager = clusterParticipant == null ? null : clusterParticipant.getReplicaSyncUpManager();
    this.time = time;
  }

  /**
   * Starts up the replication engine.
   * @throws ReplicationException
   */
  public abstract void start() throws ReplicationException;

  @Override
  public boolean controlReplicationForPartitions(Collection<PartitionId> ids, List<String> origins, boolean enable) {
    if (origins.isEmpty()) {
      origins = new ArrayList<>(replicaThreadPoolByDc.keySet());
    }
    if (!replicaThreadPoolByDc.keySet().containsAll(origins)) {
      return false;
    }
    for (String origin : origins) {
      for (ReplicaThread replicaThread : replicaThreadPoolByDc.get(origin)) {
        replicaThread.controlReplicationForPartitions(ids, enable);
      }
    }
    return true;
  }

  @Override
  public void updateTotalBytesReadByRemoteReplica(PartitionId partitionId, String hostName, String replicaPath,
      long totalBytesRead) throws StoreException {
    RemoteReplicaInfo remoteReplicaInfo = getRemoteReplicaInfo(partitionId, hostName, replicaPath);
    if (remoteReplicaInfo != null) {
      ReplicaId localReplica = remoteReplicaInfo.getLocalReplicaId();
      remoteReplicaInfo.setTotalBytesReadFromLocalStore(totalBytesRead);
      // update replication lag in ReplicaSyncUpManager
      if (replicaSyncUpManager != null) {
        Store localStore = storeManager.getStore(partitionId);
        if (localStore.getCurrentState() == ReplicaState.INACTIVE) {
          // if local store is in INACTIVE state, that means deactivation process is initiated and in progress on this
          // replica. We update SyncUpManager by peer's lag from last PUT offset in local store.
          // it's ok if deactivation has completed and subsequent metadata request attempts to update lag of same replica
          // again. The reason is, in previous request, onDeactivationComplete method should have removed local replica
          // from SyncUpManager and it should be no-op and "updated" should be false when calling updateLagBetweenReplicas()
          // for subsequent request. Hence, it won't call onDeactivationComplete() twice.
          boolean updated =
              replicaSyncUpManager.updateLagBetweenReplicas(localReplica, remoteReplicaInfo.getReplicaId(),
                  localStore.getEndPositionOfLastPut() - totalBytesRead);
          // if updated = false, it means the replica is not present in SyncUpManager and therefore doesn't need to
          // complete transition. We don't throw exception here because Standby-To-Inactive transition may already be
          // complete but local store's state is still INACTIVE since it will be updated at the beginning of
          // Inactive-To-Offline transition.
          if (updated && replicaSyncUpManager.isSyncUpComplete(localReplica)) {
            replicaSyncUpManager.onDeactivationComplete(localReplica);
          }
        } else if (localStore.getCurrentState() == ReplicaState.OFFLINE && localStore.isDecommissionInProgress()) {
          // if local store is in OFFLINE state, we need more info to determine if replica is really in Inactive-To-Offline
          // transition. So we check if decommission file is present. If present, we update SyncUpManager by peer's lag
          // from end offset in local store.
          boolean updated =
              replicaSyncUpManager.updateLagBetweenReplicas(localReplica, remoteReplicaInfo.getReplicaId(),
                  localStore.getSizeInBytes() - totalBytesRead);
          if (updated && replicaSyncUpManager.isSyncUpComplete(localReplica)) {
            replicaSyncUpManager.onDisconnectionComplete(localReplica);
          }
        }
      }
    }
  }

  @Override
  public long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath) {
    RemoteReplicaInfo remoteReplicaInfo = getRemoteReplicaInfo(partitionId, hostName, replicaPath);
    if (remoteReplicaInfo != null) {
      return remoteReplicaInfo.getRemoteLagFromLocalInBytes();
    }
    return 0;
  }

  /**
   * Gets the replica info for the remote peer replica identified by PartitionId, ReplicaPath and Hostname
   * @param partitionId PartitionId to which the replica belongs to
   * @param hostName hostname of the remote peer replica
   * @param replicaPath replica path on the remote peer replica
   * @return RemoteReplicaInfo
   */
  protected RemoteReplicaInfo getRemoteReplicaInfo(PartitionId partitionId, String hostName, String replicaPath) {
    RemoteReplicaInfo foundRemoteReplicaInfo = null;

    PartitionInfo partitionInfo = partitionToPartitionInfo.get(partitionId);
    for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfos()) {
      if (remoteReplicaInfo.getReplicaId().getReplicaPath().equals(replicaPath) && remoteReplicaInfo.getReplicaId()
          .getDataNodeId()
          .getHostname()
          .equals(hostName)) {
        foundRemoteReplicaInfo = remoteReplicaInfo;
        break;
      }
    }
    if (foundRemoteReplicaInfo == null && !replicaPath.startsWith(CloudReplica.Cloud_Replica_Keyword)) {
      replicationMetrics.unknownRemoteReplicaRequestCount.inc();
      logger.error("ReplicaMetaDataRequest from unknown Replica {}, with path {}", hostName, replicaPath);
    }
    return foundRemoteReplicaInfo;
  }

  /**
   * Shuts down the replication engine. Shuts down the individual replica threads and
   * then persists all the replica tokens
   * @throws ReplicationException
   */
  public void shutdown() throws ReplicationException {
    try {
      // stop all replica threads
      for (Map.Entry<String, List<ReplicaThread>> replicaThreads : replicaThreadPoolByDc.entrySet()) {
        for (ReplicaThread replicaThread : replicaThreads.getValue()) {
          replicaThread.shutdown();
        }
      }

      if (replicationConfig.replicationPersistTokenOnShutdownOrReplicaRemove) {
        // persist replica tokens
        persistor.write(true);
      }
    } catch (Exception e) {
      logger.error("Error shutting down replica manager", e);
      throw new ReplicationException("Error shutting down replica manager");
    }
  }

  /**
   * Remove a list of {@link RemoteReplicaInfo} from each's {@link ReplicaThread}.
   * @param remoteReplicaInfos List of {@link RemoteReplicaInfo} to remote.
   */
  protected void removeRemoteReplicaInfoFromReplicaThread(List<RemoteReplicaInfo> remoteReplicaInfos) {
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
      // Thread safe with addRemoteReplicaInfoToReplicaThread.
      // For ReplicationManger, removeRemoteReplicaInfo() ensures lock is held by only one thread at any time.
      // For CloudBackUpManager with HelixVcrCluster, Helix requires acknowledgement before next message for the same
      // resource, which means methods in HelixVcrStateModel will be executed sequentially for same partition.
      // So do listener actions in addPartition() and removePartition().
      if (remoteReplicaInfo.getReplicaThread() != null) {
        remoteReplicaInfo.getReplicaThread().removeRemoteReplicaInfo(remoteReplicaInfo);
        remoteReplicaInfo.setReplicaThread(null);
      }
    }
  }

  /**
   * Assign {@link RemoteReplicaInfo} to a {@link ReplicaThread} for replication.
   * The assignment is based on {@link DataNodeId}. If no {@link ReplicaThread} responsible for a {@link DataNodeId},
   * a {@link ReplicaThread} will be selected by {@link ReplicationEngine#getReplicaThreadIndexToUse(String)}.
   * Create threads pool for a DC if not exists.
   * @param remoteReplicaInfos List of {@link RemoteReplicaInfo} to add.
   * @param startThread if threads need to be started when create.
   */
  protected void addRemoteReplicaInfoToReplicaThread(List<RemoteReplicaInfo> remoteReplicaInfos, boolean startThread) {
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
      DataNodeId dataNodeIdToReplicate = remoteReplicaInfo.getReplicaId().getDataNodeId();
      String datacenter = dataNodeIdToReplicate.getDatacenterName();
      List<ReplicaThread> replicaThreads = getOrCreateThreadPoolIfNecessary(datacenter, startThread);
      if (replicaThreads == null) {
        logger.warn("Number of replica threads is smaller or equal to 0, not starting any replica threads for {}.",
            datacenter);
        continue;
      }
      ReplicaThread replicaThread = dataNodeIdToReplicaThread.computeIfAbsent(dataNodeIdToReplicate,
          key -> replicaThreads.get(getReplicaThreadIndexToUse(datacenter)));
      replicaThread.addRemoteReplicaInfo(remoteReplicaInfo);
      remoteReplicaInfo.setReplicaThread(replicaThread);
    }
  }

  /**
   * Select next available {@link ReplicaThread} in given datacenter.
   * @param datacenter the datacenter String.
   */
  private int getReplicaThreadIndexToUse(String datacenter) {
    return nextReplicaThreadIndexByDc.get(datacenter).getAndIncrement() % replicaThreadPoolByDc.get(datacenter).size();
  }

  /**
   * Get thread pool for given datacenter. Create thread pool for a datacenter if its thread pool doesn't exist.
   * @param datacenter The datacenter String.
   * @param startThread If thread needs to be started when create.
   * @return List of {@link ReplicaThread}s. Return null if number of replication thread in config is 0 for this DC.
   */
  private List<ReplicaThread> getOrCreateThreadPoolIfNecessary(String datacenter, boolean startThread) {
    int numOfThreadsInPool =
        datacenter.equals(dataNodeId.getDatacenterName()) ? replicationConfig.replicationNumOfIntraDCReplicaThreads
            : replicationConfig.replicationNumOfInterDCReplicaThreads;
    if (numOfThreadsInPool <= 0) {
      return null;
    }
    return replicaThreadPoolByDc.computeIfAbsent(datacenter,
        key -> createThreadPool(datacenter, numOfThreadsInPool, startThread));
  }

  /**
   * Create thread pool for a datacenter.
   * @param datacenter The datacenter String.
   * @param numberOfThreads Number of threads to create for the thread pool.
   * @param startThread If thread needs to be started when create.
   */
  private List<ReplicaThread> createThreadPool(String datacenter, int numberOfThreads, boolean startThread) {
    nextReplicaThreadIndexByDc.put(datacenter, new AtomicInteger(0));
    List<ReplicaThread> replicaThreads = new ArrayList<>();
    logger.info("Number of replica threads to replicate from {}: {}", datacenter, numberOfThreads);
    ResponseHandler responseHandler = new ResponseHandler(clusterMap);
    for (int i = 0; i < numberOfThreads; i++) {
      boolean replicatingOverSsl = sslEnabledDatacenters.contains(datacenter);
      String threadIdentity =
          (startThread ? "Vcr" : "") + "ReplicaThread-" + (dataNodeId.getDatacenterName().equals(datacenter) ? "Intra-"
              : "Inter-") + i + "-" + datacenter;
      try {
        StoreKeyConverter threadSpecificKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
        Transformer threadSpecificTransformer =
            Utils.getObj(transformerClassName, storeKeyFactory, threadSpecificKeyConverter);
        ReplicaThread replicaThread =
            new ReplicaThread(threadIdentity, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId,
                connectionPool, replicationConfig, replicationMetrics, notification, threadSpecificKeyConverter,
                threadSpecificTransformer, metricRegistry, replicatingOverSsl, datacenter, responseHandler, time,
                replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
        replicaThreads.add(replicaThread);
        if (startThread) {
          Thread thread = Utils.newThread(replicaThread.getName(), replicaThread, false);
          thread.start();
          logger.info("Started replica thread {}", thread.getName());
        }
      } catch (Exception e) {
        throw new RuntimeException("Encountered exception instantiating ReplicaThread", e);
      }
    }
    replicationMetrics.trackLiveThreadsCount(replicaThreads, datacenter);
    replicationMetrics.populateSingleColoMetrics(datacenter);
    return replicaThreads;
  }

  /**
   * Reads the replica tokens from storage, populates the Remote replica info,
   * and re-persists the tokens if they have been reset.
   * @param mountPath The mount path where the replica tokens are stored
   * @throws ReplicationException
   * @throws IOException
   */
  public void retrieveReplicaTokensAndPersistIfNecessary(String mountPath) throws ReplicationException, IOException {
    boolean tokenWasReset = false;
    long readStartTimeMs = time.milliseconds();
    List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfoList = persistor.retrieve(mountPath);

    for (RemoteReplicaInfo.ReplicaTokenInfo tokenInfo : tokenInfoList) {
      String hostname = tokenInfo.getHostname();
      int port = tokenInfo.getPort();
      PartitionId partitionId = tokenInfo.getPartitionId();
      FindToken token = tokenInfo.getReplicaToken();
      // update token
      PartitionInfo partitionInfo = partitionToPartitionInfo.get(tokenInfo.getPartitionId());
      if (partitionInfo != null) {
        boolean updatedToken = false;
        for (RemoteReplicaInfo remoteReplicaInfo : partitionInfo.getRemoteReplicaInfos()) {
          if (isTokenForRemoteReplicaInfo(remoteReplicaInfo, tokenInfo)) {
            logger.info("Read token for partition {} remote host {} port {} token {}", partitionId, hostname, port,
                token);
            if (!partitionInfo.getStore().isEmpty()) {
              remoteReplicaInfo.initializeTokens(token);
              remoteReplicaInfo.setTotalBytesReadFromLocalStore(tokenInfo.getTotalBytesReadFromLocalStore());
            } else {
              // if the local replica is empty, it could have been newly created. In this case, the offset in
              // every peer replica which the local replica lags from should be set to 0, so that the local
              // replica starts fetching from the beginning of the peer. The totalBytes the peer read from the
              // local replica should also be set to 0. During initialization these values are already set to 0,
              // so we let them be.
              tokenWasReset = true;
              logTokenReset(partitionId, hostname, port, token);
            }
            updatedToken = true;
            break;
          }
        }
        if (!updatedToken) {
          logger.warn("Persisted remote replica host {} and port {} not present in new cluster ", hostname, port);
        }
      } else {
        // If this partition was not found in partitionToPartitionInfo, it means that the local store corresponding
        // to this partition could not be started. In such a case, the tokens for its remote replicas should be
        // reset.
        tokenWasReset = true;
        logTokenReset(partitionId, hostname, port, token);
      }
    }
    replicationMetrics.remoteReplicaTokensRestoreTime.update(time.milliseconds() - readStartTimeMs);

    if (tokenWasReset) {
      // We must ensure that the the token file is persisted if any of the tokens in the file got reset. We need to do
      // this before an associated store takes any writes, to avoid the case where a store takes writes and persists it,
      // before the replica token file is persisted after the reset.
      if (persistor != null) {
        persistor.write(mountPath, false);
      } else {
        logger.warn("Unable to persist after token reset, persistor is null");
      }
    }
  }

  /**
   * Update metrics and print a log message when a replica token is reset.
   * @param partitionId The replica's partition.
   * @param hostname The replica's hostname.
   * @param port The replica's port.
   * @param token The {@link FindToken} for the replica.
   */
  private void logTokenReset(PartitionId partitionId, String hostname, int port, FindToken token) {
    replicationMetrics.replicationTokenResetCount.inc();
    logger.info("Resetting token for partition {} remote host {} port {}, persisted token {}", partitionId, hostname,
        port, token);
  }

  /**
   * Check if a token is for the given {@link RemoteReplicaInfo} based on hostname, port and replicaPath.
   * @param remoteReplicaInfo The remoteReplicaInfo to check.
   * @param tokenInfo The tokenInfo to check.
   * @return true if hostname, port and replicaPath match.
   */
  protected boolean isTokenForRemoteReplicaInfo(RemoteReplicaInfo remoteReplicaInfo,
      RemoteReplicaInfo.ReplicaTokenInfo tokenInfo) {
    DataNodeId dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
    return dataNodeId.getHostname().equalsIgnoreCase(tokenInfo.getHostname())
        && dataNodeId.getPort() == tokenInfo.getPort() && remoteReplicaInfo.getReplicaId()
        .getReplicaPath()
        .equals(tokenInfo.getReplicaPath());
  }

  /**
   * Reload replication token if exist for all remote replicas of {@code localReplicaId}.
   * @param localReplicaId local {@link ReplicaId} for which to reload tokens.
   * @param remoteReplicaInfos {@link List} of {@link RemoteReplicaInfo} of the {@code localReplicaId}.
   * @return Number of remote replicas for which reload failed.
   */
  protected int reloadReplicationTokenIfExists(ReplicaId localReplicaId, List<RemoteReplicaInfo> remoteReplicaInfos)
      throws ReplicationException {
    int tokenReloadFailureCount = 0;
    List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfos = persistor.retrieve(localReplicaId.getMountPath());
    if (tokenInfos.size() != 0) {
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
        boolean tokenReloaded = false;
        for (RemoteReplicaInfo.ReplicaTokenInfo tokenInfo : tokenInfos) {
          if (isTokenForRemoteReplicaInfo(remoteReplicaInfo, tokenInfo)) {
            logger.info("Read token for partition {} remote host {} port {} token {}", localReplicaId.getPartitionId(),
                tokenInfo.getHostname(), tokenInfo.getPort(), tokenInfo.getReplicaToken());
            tokenReloaded = true;
            remoteReplicaInfo.initializeTokens(tokenInfo.getReplicaToken());
            remoteReplicaInfo.setTotalBytesReadFromLocalStore(tokenInfo.getTotalBytesReadFromLocalStore());
            break;
          }
        }
        if (!tokenReloaded) {
          // This may happen on clusterMap update: replica removed or added.
          // Or error on token persist/retrieve.
          logger.warn("Token not found or reload failed. remoteReplicaInfo: {} tokenInfos: {}", remoteReplicaInfo,
              tokenInfos);
          tokenReloadFailureCount++;
        }
      }
    }
    return tokenReloadFailureCount;
  }

  /**
   * Stop replication for the specified {@link PartitionId}.
   * @param partitionId {@link PartitionId} for which replication should be removed.
   */
  protected void stopPartitionReplication(PartitionId partitionId) {
    PartitionInfo partitionInfo = partitionToPartitionInfo.remove(partitionId);
    if (partitionInfo == null) {
      logger.warn("Partition {} doesn't exist when removing it from {}. ", partitionId, dataNodeId);
      return;
    }
    removeRemoteReplicaInfoFromReplicaThread(partitionInfo.getRemoteReplicaInfos());
    if (replicationConfig.replicationPersistTokenOnShutdownOrReplicaRemove) {
      try {
        persistor.write(partitionInfo.getLocalReplicaId().getMountPath(), false);
      } catch (IOException | ReplicationException e) {
        logger.error("Exception {} on token write when removing Partition {} from: {}", e, partitionId, dataNodeId);
      }
    }
  }

  /**
   * To co-ordinate replication between leader and standby replicas of a partition during leader based replication.
   */
  class LeaderBasedReplicationAdmin {

    //Maintains the list of leader partitions on local node and their corresponding peer leaders in remote data centers
    private final Map<String, Set<ReplicaId>> leaderPartitionToPeerLeaderReplicas = new HashMap<>();
    private final ReadWriteLock rwLockForLeaderReplicaUpdates = new ReentrantReadWriteLock();

    LeaderBasedReplicationAdmin() {
      // We can't initialize the leaderPartitionToPeerLeaderReplicas map on startup because we don't know the leader partitions
      // on local server until it has finished participating with Helix. The map will be updated after server participates
      // with Helix and receives LEADER transition notifications via onPartitionBecomeLeaderFromStandby().
    }

    /**
     * Go through remote replicas for this partition and compare messages written to local store with the missing messages
     * found during previous meta data exchange. If there are matching messages (based on store key), remove them from the missing message set.
     * This is used during leader-based replication to update token for standby replicas. Standby replicas store the
     * missing messages in metadata exchange, track them through intra-dc replication and update token when all the
     * missing messages are written to store.
     * @param partitionId partition ID of the messages written to store
     * @param messageInfoList list of messages written to store
     */
    void onMessageWriteForPartition(PartitionId partitionId, List<MessageInfo> messageInfoList) {
      partitionToPartitionInfo.computeIfPresent(partitionId, (k, v) -> {
        v.updateReplicaInfosOnMessageWrite(messageInfoList);
        return v;
      });
    }

    /**
     * Add a leader partition and its set of peer leader replicas. This method is thread safe.
     * @param partitionName name of the partition to be added
     */
    public void addLeaderPartition(String partitionName) {

      // Read-write lock avoids contention from threads removing old leader partitions (removePartition()) and
      // threads updating existing leader partitions (refreshPeerLeadersForAllPartitions())
      rwLockForLeaderReplicaUpdates.writeLock().lock();
      try {
        // Get the peer leader replicas from all data centers for this partition
        Set<ReplicaId> peerLeaderReplicas = getPeerLeaderReplicaSet(partitionName);
        logger.info("Adding leader Partition {} and list of peer leader replicas {} on node {} to an in-memory map",
            partitionName, peerLeaderReplicas, dataNodeId);
        leaderPartitionToPeerLeaderReplicas.put(partitionName, peerLeaderReplicas);
      } finally {
        rwLockForLeaderReplicaUpdates.writeLock().unlock();
      }
    }

    /**
     * Remove a partition from the map of leader partitions. This method is thread safe.
     * @param partitionName name of the partition to be removed
     */
    public void removeLeaderPartition(String partitionName) {
      // Read-write lock avoids contention from threads adding new leaders (addPartition()) and
      // threads updating existing leader partitions (refreshPeerLeadersForAllPartitions())
      rwLockForLeaderReplicaUpdates.writeLock().lock();
      try {
        logger.info("Removing leader Partition {} on node {} from an in-memory map", partitionName, dataNodeId);
        leaderPartitionToPeerLeaderReplicas.remove(partitionName);
      } finally {
        rwLockForLeaderReplicaUpdates.writeLock().unlock();
      }
    }

    /**
     * Refreshes the list of remote leaders for all leader partitions by querying the latest information from
     * RoutingTableSnapshots of all data centers. This method is thread safe.
     */
    public void refreshPeerLeadersForLeaderPartitions() {
      // Read-write lock usage: Avoids contention between threads doing the following activities:
      // 1. Adding new leaders (in addPeerLeadersByPartition())
      // 2. Removing old leaders (in removePartition())
      // 3. Refreshing remote leader set for existing leaders (current method).
      // Explanation for point 3: Multiple threads from different cluster change handlers (we have one cluster change handler for each DC)
      // can trigger onRoutingTableUpdate() in parallel which calls this method to refresh leader partitions.
      // We need to make sure that the sequence of gathering remote leaders (from RoutingTableSnapshot of each DC) and updating the map is an atomic operation.

      rwLockForLeaderReplicaUpdates.writeLock().lock();
      try {
        for (Map.Entry<String, Set<ReplicaId>> entry : leaderPartitionToPeerLeaderReplicas.entrySet()) {
          String partitionName = entry.getKey();
          Set<ReplicaId> previousPeerLeaderReplicas = entry.getValue();
          Set<ReplicaId> currentPeerLeaderReplicas = getPeerLeaderReplicaSet(partitionName);
          if (!previousPeerLeaderReplicas.equals(currentPeerLeaderReplicas)) {
            logger.info(
                "Refreshing leader Partition {} and list of peer leader replicas {} on node {} in an in-memory map",
                partitionName, currentPeerLeaderReplicas, dataNodeId);
            leaderPartitionToPeerLeaderReplicas.put(partitionName, currentPeerLeaderReplicas);
          }
        }
      } finally {
        rwLockForLeaderReplicaUpdates.writeLock().unlock();
      }
    }

    /**
     * Get a map of partitions to their sets of peer leader replicas (this method is only by ReplicationTest for now)
     * @return an unmodifiable map of peer leader replicas stored by partition
     */
    public Map<String, Set<ReplicaId>> getLeaderPartitionToPeerLeaderReplicas() {
      return Collections.unmodifiableMap(leaderPartitionToPeerLeaderReplicas);
    }

    /**
     * Checks if provided local and remote replicas are leaders of their partition. I.e. checks if partition corresponding
     * to local replica is a leader partition on local node (present in leaderPartitionToPeerLeaderReplicas map) and the leader
     * partition contains remote replica in its list of peer leader replicas.
     * For example: Say if the partition corresponding to provided local/remote replica is 100, and if local replica is
     * a leader of this partition, this map would have an entry {"100": List of remote leaders}. If remote replica is a
     * leader as well, it would be present in the entry value (list of remote leaders). In that case, we return true.
     * Else, we return false.
     * @param localReplica local replica
     * @param remoteReplica remote replica
     * @return true if local and remote replica are leaders of their partition.
     */
    public boolean isLeaderPair(ReplicaId localReplica, ReplicaId remoteReplica) {
      rwLockForLeaderReplicaUpdates.readLock().lock();
      try {
        String partitionName = localReplica.getPartitionId().toPathString();
        return leaderPartitionToPeerLeaderReplicas.getOrDefault(partitionName, Collections.emptySet())
            .contains(remoteReplica);
      } finally {
        rwLockForLeaderReplicaUpdates.readLock().unlock();
      }
    }

    /**
     * Retrieves the set of peer leader replicas (in remote data centers) for the given leader partition on this node
     * @param leaderPartitionName leader partition name
     * @return set of peer leader replicas
     */
    Set<ReplicaId> getPeerLeaderReplicaSet(String leaderPartitionName) {
      ReplicaId localReplica = storeManager.getReplica(leaderPartitionName);
      PartitionId leaderPartition = localReplica.getPartitionId();
      Set<ReplicaId> peerLeaderReplicaSet =
          new HashSet<>(leaderPartition.getReplicaIdsByState(ReplicaState.LEADER, null));
      peerLeaderReplicaSet.remove(localReplica);
      return peerLeaderReplicaSet;
    }
  }
}
