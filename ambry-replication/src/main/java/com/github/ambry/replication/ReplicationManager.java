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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


/**
 * Set up replicas based on {@link ReplicationEngine} and do replication across all data centers.
 */
public class ReplicationManager extends ReplicationEngine {
  protected CountDownLatch startupLatch = new CountDownLatch(1);
  protected boolean started = false;
  private final StoreConfig storeConfig;
  private final DataNodeId currentNode;
  private final boolean trackPerPartitionLagInMetric;

  public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, ConnectionPool connectionPool,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      ClusterParticipant clusterParticipant, Predicate<MessageInfo> skipPredicate) throws ReplicationException {
    this(replicationConfig, clusterMapConfig, storeConfig, storeManager, storeKeyFactory, clusterMap, scheduler,
        dataNode, connectionPool, metricRegistry, requestNotification, storeKeyConverterFactory, transformerClassName,
        clusterParticipant, skipPredicate, null, SystemTime.getInstance());
  }

  public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, ConnectionPool connectionPool,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      ClusterParticipant clusterParticipant, Predicate<MessageInfo> skipPredicate, FindTokenHelper findTokenHelper,
      Time time) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler, dataNode,
        clusterMap.getReplicaIds(dataNode), connectionPool, metricRegistry, requestNotification,
        storeKeyConverterFactory, transformerClassName, clusterParticipant, storeManager, skipPredicate,
        findTokenHelper, time);
    this.storeConfig = storeConfig;
    this.currentNode = dataNode;
    trackPerPartitionLagInMetric = replicationConfig.replicationTrackPerDatacenterLagFromLocal;
    clusterMap.registerClusterMapListener(new ClusterMapChangeListenerImpl());
    // make sure leaderBasedReplicationAdmin is constructed before creating ReplicaThreads since it is passed to them
    if (replicationConfig.replicationModelAcrossDatacenters.equals(ReplicationModelType.LEADER_BASED)) {
      logger.info("Leader-based cross colo replication model is being used");
      leaderBasedReplicationAdmin = new LeaderBasedReplicationAdmin();
    }
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNode);
    // initialize all partitions
    for (ReplicaId replicaId : replicaIds) {
      PartitionId partition = replicaId.getPartitionId();
      Store store = storeManager.getStore(partition);
      if (store != null) {
        List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        if (peerReplicas != null) {
          List<RemoteReplicaInfo> remoteReplicas = createRemoteReplicaInfos(peerReplicas, replicaId);
          updatePartitionInfoMaps(remoteReplicas, replicaId);
          addRemoteReplicaInfoToReplicaThread(remoteReplicas, false);
        }
      } else {
        logger.error("Not replicating to partition {} because an initialized store could not be found", partition);
      }
    }
    // register replication manager's state change listener if clusterParticipant is not null
    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.ReplicationManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("Replication manager's state change listener registered!");
    }
    persistor = new DiskTokenPersistor(replicaTokenFileName, mountPathToPartitionInfos, replicationMetrics, clusterMap,
        tokenHelper, storeManager);
  }

  @Override
  public void start() throws ReplicationException {
    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : mountPathToPartitionInfos.keySet()) {
        retrieveReplicaTokensAndPersistIfNecessary(mountPath);
      }
      if (replicaThreadPoolByDc.size() == 0) {
        logger.warn("Number of data centers to replicate from is 0, not starting any replica threads");
        return;
      }
      // valid for replication manager.
      replicationMetrics.trackReplicationDisabledPartitions(replicaThreadPoolByDc);

      // start all replica threads
      for (List<ReplicaThread> replicaThreads : replicaThreadPoolByDc.values()) {
        for (ReplicaThread thread : replicaThreads) {
          Thread replicaThread = Utils.newThread(thread.getName(), thread, false);
          logger.info("Starting replica thread {}", thread.getName());
          replicaThread.start();
        }
      }

      // start scheduler thread to persist replica token in the background
      if (persistor != null) {
        this.scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
            replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
      }
      started = true;
    } catch (IOException e) {
      logger.error("IO error while starting replication", e);
    } finally {
      startupLatch.countDown();
    }
  }

  /**
   * Add given replica into replication manager.
   * @param replicaId the replica to add
   * @return {@code true} if addition succeeded, {@code false} failed to add replica because it already exists.
   */
  public boolean addReplica(ReplicaId replicaId) {
    if (partitionToPartitionInfo.containsKey(replicaId.getPartitionId())) {
      logger.warn("Partition {} already exists in replication manager, rejecting adding replica request.",
          replicaId.getPartitionId());
      return false;
    }
    rwLock.writeLock().lock();
    try {
      List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
      List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
      if (!peerReplicas.isEmpty()) {
        remoteReplicaInfos = createRemoteReplicaInfos(peerReplicas, replicaId);
        updatePartitionInfoMaps(remoteReplicaInfos, replicaId);
      }
      logger.info("Assigning thread for {}", replicaId.getPartitionId());
      addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
      // No need to update persistor to explicitly persist tokens for new replica because background persistor will
      // periodically persist all tokens including new added replica's
      logger.info("{} is successfully added into replication manager", replicaId.getPartitionId());
    } finally {
      rwLock.writeLock().unlock();
    }
    return true;
  }

  /**
   * Remove replica from replication manager
   * @param replicaId the replica to remove
   * @return {@code true} if replica is successfully removed. {@code false} otherwise
   */
  public boolean removeReplica(ReplicaId replicaId) {
    if (!partitionToPartitionInfo.containsKey(replicaId.getPartitionId())) {
      logger.error("Partition {} doesn't exist in replication manager, skipping removing replica request.",
          replicaId.getPartitionId());
      return false;
    }
    rwLock.writeLock().lock();
    try {
      PartitionInfo partitionInfo = partitionToPartitionInfo.get(replicaId.getPartitionId());
      List<RemoteReplicaInfo> remoteReplicaInfos = partitionInfo.getRemoteReplicaInfos();
      logger.info("Removing remote replicas of {} from replica threads", replicaId.getPartitionId());
      removeRemoteReplicaInfoFromReplicaThread(remoteReplicaInfos);
      mountPathToPartitionInfos.computeIfPresent(replicaId.getMountPath(), (k, v) -> {
        v.remove(partitionInfo);
        return v;
      });
      partitionToPartitionInfo.remove(replicaId.getPartitionId());
      if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
        replicationMetrics.removeLagMetricForPartition(replicaId.getPartitionId());
      }
      logger.info("{} is successfully removed from replication manager", replicaId.getPartitionId());
    } finally {
      rwLock.writeLock().unlock();
    }
    return true;
  }

  /**
   * Create {@link RemoteReplicaInfo}(s) that associates with given local replica.
   * @param peerReplicas the list peer replicas of given local replica
   * @param replicaId the local replica
   * @return list of {@link RemoteReplicaInfo} associated with local replica.
   */
  private List<RemoteReplicaInfo> createRemoteReplicaInfos(List<? extends ReplicaId> peerReplicas,
      ReplicaId replicaId) {
    List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
    PartitionId partition = replicaId.getPartitionId();
    Store store = storeManager.getStore(partition);
    for (ReplicaId remoteReplica : peerReplicas) {
      // We need to ensure that a replica token gets persisted only after the corresponding data in the
      // store gets flushed to disk. We use the store flush interval multiplied by a constant factor
      // to determine the token flush interval
      FindToken findToken =
          this.tokenHelper.getFindTokenFactoryFromReplicaType(remoteReplica.getReplicaType()).getNewFindToken();
      RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(remoteReplica, replicaId, store, findToken,
          TimeUnit.SECONDS.toMillis(storeConfig.storeDataFlushIntervalSeconds) * Replication_Delay_Multiplier,
          SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());

      replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo, trackPerPartitionLagInMetric);
      remoteReplicaInfos.add(remoteReplicaInfo);
    }
    if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
      replicationMetrics.addLagMetricForPartition(partition);
    }
    return remoteReplicaInfos;
  }

  /**
   * Update {@link PartitionInfo} related maps including {@link ReplicationEngine#partitionToPartitionInfo} and
   * {@link ReplicationEngine#mountPathToPartitionInfos}
   * @param remoteReplicaInfos the {@link RemoteReplicaInfo}(s) of the local {@link ReplicaId}
   * @param replicaId the local replica
   */
  private void updatePartitionInfoMaps(List<RemoteReplicaInfo> remoteReplicaInfos, ReplicaId replicaId) {
    PartitionId partition = replicaId.getPartitionId();
    PartitionInfo partitionInfo =
        new PartitionInfo(remoteReplicaInfos, partition, storeManager.getStore(partition), replicaId);
    partitionToPartitionInfo.put(partition, partitionInfo);
    mountPathToPartitionInfos.computeIfAbsent(replicaId.getMountPath(), key -> ConcurrentHashMap.newKeySet())
        .add(partitionInfo);
  }

  /**
   * Implementation of {@link ClusterMapChangeListener} that helps replication manager react to cluster map changes.
   */
  class ClusterMapChangeListenerImpl implements ClusterMapChangeListener {
    /**
     * {@inheritDoc}
     * Note that, this method should be thread-safe because multiple threads (from different cluster change handlers) may
     * concurrently update remote replica infos.
     */
    @Override
    public void onReplicaAddedOrRemoved(List<ReplicaId> addedReplicas, List<ReplicaId> removedReplicas) {
      // 1. wait for start() to complete
      try {
        startupLatch.await();
      } catch (InterruptedException e) {
        logger.warn("Waiting for startup is interrupted.");
        throw new IllegalStateException("Replication manager startup is interrupted while updating remote replicas");
      }
      if (started) {
        // Read-write lock avoids contention between addReplica()/removeReplica() and onReplicaAddedOrRemoved() methods.
        // Read lock for current method should suffice because multiple threads from cluster change handlers should be able
        // to access partitionToPartitionInfo map. Each thead only updates PartitionInfo of certain partition and synchronization
        // is only required within PartitionInfo. Also, addRemoteReplicaInfoToReplicaThread() is thread-safe which allows
        // several threads from cluster change handlers to add remoteReplicaInfo
        rwLock.readLock().lock();
        try {
          // 2. determine if added/removed replicas have peer replica on local node.
          //    We skip the replica on current node because it should already be added/removed by state transition thread.
          Set<ReplicaId> addedPeerReplicas = addedReplicas.stream()
              .filter(r -> partitionToPartitionInfo.containsKey(r.getPartitionId()) && r.getDataNodeId() != currentNode)
              .collect(Collectors.toSet());
          Set<ReplicaId> removedPeerReplicas = removedReplicas.stream()
              .filter(r -> partitionToPartitionInfo.containsKey(r.getPartitionId()) && r.getDataNodeId() != currentNode)
              .collect(Collectors.toSet());

          // No additional synchronization is required because cluster change handler of each dc only updates replica-threads
          // belonging to certain dc. Hence, there is only one thread adding/removing remote replicas within a certain dc.

          // 3. create replicaInfo for new remote replicas and assign them to replica-threads.
          List<RemoteReplicaInfo> replicaInfosToAdd = new ArrayList<>();
          for (ReplicaId remoteReplica : addedPeerReplicas) {
            PartitionInfo partitionInfo = partitionToPartitionInfo.get(remoteReplica.getPartitionId());
            // create findToken, remoteReplicaInfo
            FindToken findToken =
                tokenHelper.getFindTokenFactoryFromReplicaType(remoteReplica.getReplicaType()).getNewFindToken();
            RemoteReplicaInfo remoteReplicaInfo =
                new RemoteReplicaInfo(remoteReplica, partitionInfo.getLocalReplicaId(), partitionInfo.getStore(),
                    findToken,
                    TimeUnit.SECONDS.toMillis(storeConfig.storeDataFlushIntervalSeconds) * Replication_Delay_Multiplier,
                    SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());
            logger.info("Adding remote replica {} on {} to partition info.", remoteReplica.getReplicaPath(),
                remoteReplica.getDataNodeId());
            if (partitionInfo.addReplicaInfoIfAbsent(remoteReplicaInfo)) {
              replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo, trackPerPartitionLagInMetric);
              replicaInfosToAdd.add(remoteReplicaInfo);
            }
          }
          addRemoteReplicaInfoToReplicaThread(replicaInfosToAdd, true);

          // 4. remove replicaInfo from existing partitionInfo and replica-threads
          List<RemoteReplicaInfo> replicaInfosToRemove = new ArrayList<>();
          for (ReplicaId remoteReplica : removedPeerReplicas) {
            PartitionInfo partitionInfo = partitionToPartitionInfo.get(remoteReplica.getPartitionId());
            RemoteReplicaInfo removedReplicaInfo = partitionInfo.removeReplicaInfoIfPresent(remoteReplica);
            if (removedReplicaInfo != null) {
              replicationMetrics.removeMetricsForRemoteReplicaInfo(removedReplicaInfo);
              logger.info("Removing remote replica {} on {} from replica threads.", remoteReplica.getReplicaPath(),
                  remoteReplica.getDataNodeId());
              replicaInfosToRemove.add(removedReplicaInfo);
            }
          }
          removeRemoteReplicaInfoFromReplicaThread(replicaInfosToRemove);
        } finally {
          rwLock.readLock().unlock();
        }
      }
    }

    /**
     * {@inheritDoc}
     * Note that, this method should be thread-safe because multiple threads (from different cluster change handlers) may
     * concurrently call this method.
     */
    @Override
    public void onRoutingTableChange() {

      // wait for start() to complete
      try {
        startupLatch.await();
      } catch (InterruptedException e) {
        logger.warn("Waiting for startup is interrupted.");
        throw new IllegalStateException(
            "Replication manager startup is interrupted while handling routing table change");
      }

      if (leaderBasedReplicationAdmin != null) {
        // Refreshes the remote leader information for all local leader partitions maintained in the in-mem structure in LeaderBasedReplicationAdmin.
        // Thread safety is ensured in LeaderBasedReplicationAdmin::refreshPeerLeadersForAllPartitions().
        leaderBasedReplicationAdmin.refreshPeerLeadersForLeaderPartitions();
      }
    }
  }

  /**
   * {@link PartitionStateChangeListener} to capture changes in partition state.
   */
  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      // check if partition exists
      ReplicaId replica = storeManager.getReplica(partitionName);
      if (replica == null) {
        // no matter this is an existing replica or new added one, it should be present in storage manager because new
        // replica is added into storage manager first.
        throw new StateTransitionException("Replica " + partitionName + " is not found on current node",
            ReplicaNotFound);
      }

      if (!partitionToPartitionInfo.containsKey(replica.getPartitionId())) {
        // if partition is not present in partitionToPartitionInfo map, it means this partition was just added in storage
        // manager and next step is to add it into replication manager
        logger.info("Didn't find replica {} in replication manager, starting to add it.", partitionName);
        if (!addReplica(replica)) {
          throw new StateTransitionException("Failed to add new replica " + partitionName + " into replication manager",
              ReplicaOperationFailure);
        }
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      // if code arrives here, it means local replica has completed OFFLINE -> BOOTSTRAP transition. We don't have to
      // check if local replica exists or not.
      ReplicaId localReplica = storeManager.getReplica(partitionName);
      Store store = storeManager.getStore(localReplica.getPartitionId());
      // 1. check if store is started
      if (store == null) {
        throw new StateTransitionException(
            "Store " + partitionName + " is not started during Bootstrap-To-Standby transition", StoreNotStarted);
      }
      // 2. check if store is new added and needs to catch up with peer replicas.
      if (store.isBootstrapInProgress()) {
        store.setCurrentState(ReplicaState.BOOTSTRAP);
        // store state will updated to STANDBY in ReplicaThread when bootstrap is complete
        replicaSyncUpManager.initiateBootstrap(localReplica);
      } else {
        // if this is existing replica, then directly set state to STANDBY
        store.setCurrentState(ReplicaState.STANDBY);
      }
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
      logger.info("Partition state change notification from Standby to Leader received for partition {}",
          partitionName);
      if (leaderBasedReplicationAdmin != null) {
        // Add the leader partition (and its peer leaders) information to the in-mem structure maintained in
        // leaderBasedReplicationAdmin. LeaderBasedReplicationAdmin::addLeaderPartition is thread safe.
        leaderBasedReplicationAdmin.addLeaderPartition(partitionName);
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
      logger.info("Partition state change notification from Leader to Standby received for partition {}",
          partitionName);
      if (leaderBasedReplicationAdmin != null) {
        // Remove the leader partition from the in-mem structure maintained in leaderBasedReplicationAdmin.
        // LeaderBasedReplicationAdmin::removeLeaderPartition is thread safe.
        leaderBasedReplicationAdmin.removeLeaderPartition(partitionName);
      }
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      // if code arrives here, we don't need to check if replica exists, it has been checked in StorageManager
      ReplicaId localReplica = storeManager.getReplica(partitionName);
      Store store = storeManager.getStore(localReplica.getPartitionId());
      // 1. check if store is started
      if (store == null) {
        throw new StateTransitionException(
            "Store " + partitionName + " is not started during Standby-To-Inactive transition", StoreNotStarted);
      }
      replicaSyncUpManager.initiateDeactivation(localReplica);
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      // we have to check existence of local replica because replication manager is invoked first in INACTIVE -> OFFLINE
      // transition and we cannot guarantee there is no change after STANDBY -> INACTIVE is complete
      ReplicaId localReplica = storeManager.getReplica(partitionName);
      // check if local replica exists
      if (localReplica == null) {
        throw new StateTransitionException("Replica " + partitionName + " is not found on current node",
            ReplicaNotFound);
      }
      // check if store is started
      Store store = storeManager.getStore(localReplica.getPartitionId());
      if (store == null) {
        throw new StateTransitionException(
            "Store " + partitionName + " is not started during Inactive-To-Offline transition", StoreNotStarted);
      }
      // set local store state to OFFLINE and initiate disconnection
      store.setCurrentState(ReplicaState.OFFLINE);
      replicaSyncUpManager.initiateDisconnection(localReplica);
    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {
      ReplicaId replica = storeManager.getReplica(partitionName);
      // if code arrives here, we don't need to check if replica exists, it has been checked in StatsManager
      // here we attempt to remove replica from replication manager. If replica doesn't exist, log info but don't fail
      // the transition
      removeReplica(replica);
    }
  }
}
