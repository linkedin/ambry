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
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final StoreKeyConverterFactory storeKeyConverterFactory;
  private final String transformerClassName;

  protected final DataNodeId dataNodeId;
  protected final MetricRegistry metricRegistry;
  protected final ReplicationMetrics replicationMetrics;
  protected final FindTokenHelper tokenHelper;
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final Map<PartitionId, PartitionInfo> partitionToPartitionInfo;
  protected final Map<String, Set<PartitionInfo>> mountPathToPartitionInfos;
  protected final Map<String, List<ReplicaId>> peerLeaderReplicasByPartition;
  protected final ReplicaSyncUpManager replicaSyncUpManager;
  protected final StoreManager storeManager;
  protected ReplicaTokenPersistor persistor = null;

  protected static final short Replication_Delay_Multiplier = 5;
  protected static final String replicaTokenFileName = "replicaTokens";

  public ReplicationEngine(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, ScheduledExecutorService scheduler, DataNodeId dataNode,
      List<? extends ReplicaId> replicaIds, ConnectionPool connectionPool, MetricRegistry metricRegistry,
      NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory,
      String transformerClassName, ClusterParticipant clusterParticipant, StoreManager storeManager)
      throws ReplicationException {
    this.replicationConfig = replicationConfig;
    this.storeKeyFactory = storeKeyFactory;
    try {
      this.tokenHelper = new FindTokenHelper(this.storeKeyFactory, this.replicationConfig);
    } catch (ReflectiveOperationException roe) {
      logger.error("Error on getting ReplicaTokenHelper", roe);
      throw new ReplicationException("Error on getting ReplicaTokenHelper");
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
    replicaSyncUpManager = clusterParticipant == null ? null : clusterParticipant.getReplicaSyncUpManager();
    peerLeaderReplicasByPartition = new ConcurrentHashMap<>();
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
                threadSpecificTransformer, metricRegistry, replicatingOverSsl, datacenter, responseHandler,
                SystemTime.getInstance(), replicaSyncUpManager);
        replicaThreads.add(replicaThread);
        if (startThread) {
          Thread thread = Utils.newThread(replicaThread.getName(), replicaThread, false);
          thread.start();
          logger.info("Started replica thread " + thread.getName());
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
    long readStartTimeMs = SystemTime.getInstance().milliseconds();
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
    replicationMetrics.remoteReplicaTokensRestoreTime.update(SystemTime.getInstance().milliseconds() - readStartTimeMs);

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
        logger.error(
            "Exception " + e + " on token write when removing Partition " + partitionId + " from: " + dataNodeId);
      }
    }
  }
}
