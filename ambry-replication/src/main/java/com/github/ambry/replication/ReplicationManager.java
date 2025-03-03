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
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replica.prioritization.PartitionPriorityTier;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


/**
 * Set up replicas based on {@link ReplicationEngine} and do replication across all data centers.
 * Store-> Store
 */
public class ReplicationManager extends ReplicationEngine {
  private final boolean trackPerPartitionLagInMetric;
  private final boolean trackPerReplicaReplicationBytes;

  private final PriorityBlockingQueue<PartitionInfo> replicationQueue;
  private final ScheduledExecutorService replicationQueueScheduler;
  private final ReentrantLock queueLock;
  private final AtomicLong batchStartTimeMs;


  public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, NetworkClientFactory networkClientFactory,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      ClusterParticipant clusterParticipant, Predicate<MessageInfo> skipPredicate) throws ReplicationException {
    this(replicationConfig, clusterMapConfig, storeConfig, storeManager, storeKeyFactory, clusterMap, scheduler,
        dataNode, networkClientFactory, metricRegistry, requestNotification, storeKeyConverterFactory,
        transformerClassName, clusterParticipant, skipPredicate, null, SystemTime.getInstance());
  }

  public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, NetworkClientFactory networkClientFactory,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName,
      ClusterParticipant clusterParticipant, Predicate<MessageInfo> skipPredicate, FindTokenHelper findTokenHelper,
      Time time) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeConfig, storeKeyFactory, clusterMap, scheduler, dataNode,
        clusterMap.getReplicaIds(dataNode), networkClientFactory, metricRegistry, requestNotification,
        storeKeyConverterFactory, transformerClassName, clusterParticipant, storeManager, skipPredicate,
        findTokenHelper, time, true);
    trackPerPartitionLagInMetric = replicationConfig.replicationTrackPerDatacenterLagFromLocal;
    trackPerReplicaReplicationBytes = replicationConfig.replicationTrackPerReplicaReplicationBytes;
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
    replicationQueue = new PriorityBlockingQueue<>(500, (o1, o2) -> o2.getPriority() - o1.getPriority());
    replicationQueueScheduler = Utils.newScheduler(1, false);
    queueLock = new ReentrantLock();
    batchStartTimeMs = new AtomicLong(0);
  }

  /**
   * Returns replication-thread object, not a thread
   * @param threadName
   * @return
   */
  public BackupCheckerThread getBackupCheckerThread(String threadName) {
    try {
      String dc = dataNodeId.getDatacenterName();
      StoreKeyConverter storeKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
      Transformer transformer = Utils.getObj(transformerClassName, storeKeyFactory, storeKeyConverter);
      return new BackupCheckerThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId,
          networkClientFactory.getNetworkClient(), replicationConfig, replicationMetrics, notification,
          storeKeyConverter, storeKeyFactory, transformer, metricRegistry, sslEnabledDatacenters.contains(dc), dc,
          new ResponseHandler(clusterMap), time, replicaSyncUpManager, skipPredicate, leaderBasedReplicationAdmin);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns replication thread
   */
  @Override
  protected ReplicaThread getReplicaThread(String threadName, FindTokenHelper findTokenHelper, ClusterMap clusterMap,
      AtomicInteger correlationIdGenerator, DataNodeId dataNodeId, NetworkClient networkClient,
      ReplicationConfig replicationConfig, ReplicationMetrics replicationMetrics, NotificationSystem notification,
      StoreKeyConverter storeKeyConverter, Transformer transformer, MetricRegistry metricRegistry,
      boolean replicatingOverSsl, String datacenterName, ResponseHandler responseHandler, Time time,
      ReplicaSyncUpManager replicaSyncUpManager, Predicate<MessageInfo> skipPredicate,
      ReplicationManager.LeaderBasedReplicationAdmin leaderBasedReplicationAdmin) {
    return new ReplicaThread(threadName, tokenHelper, clusterMap, correlationIdGenerator, dataNodeId, networkClient,
        replicationConfig, replicationMetrics, notification, storeKeyConverter, transformer, metricRegistry,
        replicatingOverSsl, datacenterName, responseHandler, time, replicaSyncUpManager, skipPredicate,
        leaderBasedReplicationAdmin);
  }

  @Override
  public void start() throws ReplicationException {
    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : mountPathToPartitionInfos.keySet()) {
        retrieveReplicaTokensAndPersistIfNecessary(mountPath);
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
      replicationQueueScheduler.scheduleAtFixedRate(this::scheduledReplicationTasks, 15, 15, TimeUnit.SECONDS);
      started = true;
    } catch (IOException e) {
      logger.error("IO error while starting replication", e);
    } finally {
      startupLatch.countDown();
    }
  }

  private void scheduledReplicationTasks() {
    if (!replicaSyncUpManager.getBatchInProgress().compareAndSet(false, true)) {
      logger.info("Previous batch of prioritized replication is still in progress, skipping this round of replication");
      return;
    }

    try {

      if (!replicaSyncUpManager.getActivePartitionsInBatch().isEmpty()) {
        logger.info("Active partition set is not empty, skipping this round of replication");
        return;
      }

      //batchStartTimeMs.set(System.currentTimeMillis());

      Map<PartitionPriorityTier, List<PartitionInfo>> partitionInfoByPriorityTier = getNextBatchByTier();

      if (partitionInfoByPriorityTier.isEmpty()) {
        logger.info("No partitions in queue to process");
        replicaSyncUpManager.getBatchInProgress().set(false);
        return;
      }

      int totalBatchSize = partitionInfoByPriorityTier.values().stream().mapToInt(List::size).sum();
      logger.info("Processing {} partitions in this batch across {} tiers", totalBatchSize, partitionInfoByPriorityTier.size());


      for (PartitionPriorityTier tier : PartitionPriorityTier.values()) {
        if (partitionInfoByPriorityTier.containsKey(tier) && !partitionInfoByPriorityTier.get(tier).isEmpty()) {
          List<PartitionInfo> partitionInfos = partitionInfoByPriorityTier.get(tier);
          logger.info("Processing {} partitions in tier {}", partitionInfos.size(), tier);
          for (PartitionInfo partitionInfo : partitionInfos) {
            replicaSyncUpManager.getActivePartitionsInBatch().add(partitionInfo.getPartitionId());
            replicaSyncUpManager.getActivePartitionsInBatchByTier().get(tier).add(partitionInfo.getPartitionId());
            assignPartitionToReplicaThread(partitionInfo);
          }

          //replicationMetrics.batchesByTier.get(tier).inc();
          //replicationMetrics.partitionsProcessedByTier.get(tier).inc(partitionInfos.size());
          //replicationMetrics
        }
      }

    } catch (Exception e) {
      logger.error("Exception during scheduled replication tasks", e);
    } finally {
      replicaSyncUpManager.getBatchInProgress().set(false);
    }
  }

  /**
   * Get the next batch of partitions grouped by priority tier
   * @return Map of priority tier to list of partitions
   */
  private Map<PartitionPriorityTier, List<PartitionInfo>> getNextBatchByTier() {
    Map<PartitionPriorityTier, List<PartitionInfo>> result = new HashMap<>();

    // First determine optimal batch size based on current conditions
    int optimalBatchSize = replicationConfig.maxPartitionsPerBatch;

    queueLock.lock();
    try {
      int remainingBatchSize = optimalBatchSize;

      // Always process CRITICAL and HIGH priority partitions first
      for (PartitionPriorityTier tier : PartitionPriorityTier.values()) {
        if (remainingBatchSize <= 0) break;

        List<PartitionInfo> tierPartitions = new ArrayList<>();

        // Collect partitions for this tier
        for (PartitionInfo info : replicationQueue) {
          if (PartitionPriorityTier.fromPriority(info.getPriority()) == tier) {
            tierPartitions.add(info);
          }
        }

        // Sort by priority then arrival time
        tierPartitions.sort(
            Comparator.comparing(PartitionInfo::getPriority).reversed().thenComparing(PartitionInfo::getArrivalTime));

        // Take up to the remaining batch size for this tier
        int tierSize = Math.min(tierPartitions.size(), remainingBatchSize);
        if (tierSize > 0) {
          List<PartitionInfo> selectedPartitions = tierPartitions.subList(0, tierSize);
          result.put(tier, new ArrayList<>(selectedPartitions));

          // Remove these partitions from the queue
          replicationQueue.removeAll(selectedPartitions);
          // Add metrics to track the number of partitions in the queue
          //replicationMetrics.partitionQueueSize.dec(selectedPartitions.size());

          remainingBatchSize -= tierSize;
        }
      }
    } finally {
      queueLock.unlock();
    }

    return result;
  }

  public void assignPartitionToReplicaThread(PartitionInfo partitionInfo) {
    try {
      logger.info("Assigning thread for {}", partitionInfo.getPartitionId());
      addRemoteReplicaInfoToReplicaThread(partitionInfo.getRemoteReplicaInfos(), true);
      // No need to update persistor to explicitly persist tokens for new replica because background persistor will
      // periodically persist all tokens including new added replica's
      logger.info("{} is successfully added into replication manager", partitionInfo.getPartitionId());
    } catch (Exception e) {
      logger.error("Error while assigning partition to replica thread", e);
    }
  }

  /**
   * Checks if replication manager started successfully. Only used in tests
   * @return {@code true} if replication manager started successfully.
   */
  boolean isStarted() {
    return started;
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
      }
      int priority = calculatePartitionPriority(replicaId.getPartitionId());
      long partitionArrivalTime = System.currentTimeMillis();
      PartitionPriorityTier tier = PartitionPriorityTier.fromPriority(priority);
      updatePartitionInfoMaps(remoteReplicaInfos, replicaId);
      PartitionInfo partitionInfo = partitionToPartitionInfo.get(replicaId.getPartitionId());
      partitionInfo.setPriority(priority);
      partitionInfo.setArrivalTime(partitionArrivalTime);
      queueLock.lock();
      replicationQueue.add(partitionInfo);
      logger.info("{} is successfully added into replication queue", replicaId.getPartitionId());
      //replicationMetrics.replicationQueueSize.inc();
      //replicationMetrics.queuedPartitionsByTier.get(tier).inc();
    } finally {
      rwLock.writeLock().unlock();
      queueLock.unlock();
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
      // We are removing this replica from replica thread, we don't need to keep "disabled" information in replica thread
      // anymore. So passing true to enable would remove any "disabled" information.
      controlReplicationForPartitions(Collections.singleton(replicaId.getPartitionId()), Collections.emptyList(), true);
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
  public List<RemoteReplicaInfo> createRemoteReplicaInfos(List<? extends ReplicaId> peerReplicas,
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
      RemoteReplicaInfo remoteReplicaInfo =
          new RemoteReplicaInfo(remoteReplica, replicaId, store, findToken,
              TimeUnit.SECONDS.toMillis(storeConfig.storeDataFlushIntervalSeconds) * Replication_Delay_Multiplier,
              SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());

      replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo, trackPerPartitionLagInMetric,
          trackPerReplicaReplicationBytes);
      remoteReplicaInfos.add(remoteReplicaInfo);
    }
    replicationMetrics.addLagMetricForPartition(partition, replicationConfig.replicationTrackPerPartitionLagFromRemote);
    return remoteReplicaInfos;
  }

  /**
   * Calculate priority for a partition based on scheduled disruptions and other factors
   * @param partitionId The partition to calculate priority for
   * @return Priority value (higher = more important)
   */
  private int calculatePartitionPriority(PartitionId partitionId) {
    int priority = 0;

    if (replicationConfig.enableBatchPrioritization) {
      long currentTimeMs = System.currentTimeMillis();
      //long timeToDisruption = poller.getTimeUntilDisruption(partitionId, currentTimeMs);
      long timeToDisruption = 0;
      // if this partitionId has AR=2
      boolean belowMinActiveReplica = false;
      if (belowMinActiveReplica || timeToDisruption < (long) replicationConfig.partitionPriorityWindowMS) {
        // Base priority for disruption-scheduled partitions
        //priority = 70; // Start at HIGH tier

        // Adjust based on time to disruption
        if (timeToDisruption <= 2 * 60 * 60 * 1000) { // Less than 2 hours
          priority = 95; // CRITICAL tier
        } else if (timeToDisruption <= 6 * 60 * 60 * 1000) { // Less than 6 hours
          priority = 85; // HIGH tier
        } else if (timeToDisruption <= 12 * 60 * 60 * 1000) { // Less than 12 hours
          priority = 75; // HIGH tier
        }
      }
    }

    return priority;
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
      logger.info("Partition state change notification from Bootstrap to Standby received for partition {}",
          partitionName);
      // if code arrives here, it means local replica has completed OFFLINE -> BOOTSTRAP transition. We don't have to
      // check if local replica exists or not.
      ReplicaId localReplica = storeManager.getReplica(partitionName);
      Store store = storeManager.getStore(localReplica.getPartitionId());
      // 1. check if store is started
      if (store == null) {
        throw new StateTransitionException(
            "Store " + partitionName + " is not started during Bootstrap-To-Standby transition", StoreNotStarted);
      }
      logger.info("Partition = {}, replicaId = {}, storeId = {}", partitionName, localReplica, store);
      // 2. check if store is new added and needs to catch up with peer replicas.
      ReplicaState currentState = store.getCurrentState();
      if (store.isBootstrapInProgress()) {
        logger.info("Partition {} bootstrap in progress, current state = {}", partitionName, currentState);
        store.setCurrentState(ReplicaState.BOOTSTRAP);
        // store state will updated to STANDBY in ReplicaThread when bootstrap is complete
        replicaSyncUpManager.initiateBootstrap(localReplica);
      } else {
        logger.info("Partition {} bootstrap complete, current state = {}", partitionName, currentState);
        // if this is existing replica, then directly set state to STANDBY
        store.setCurrentState(ReplicaState.STANDBY);
      }
      logger.info("Partition {} has changed state from {} to {}", partitionName, currentState, store.getCurrentState());
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
      if (!store.isEmpty()) {
        // Wait for peers to sync up if this replica is not empty
        replicaSyncUpManager.initiateDeactivation(localReplica);
      }
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
      if (!store.isEmpty()) {
        // Wait for peers to sync up if this replica is not empty
        replicaSyncUpManager.initiateDisconnection(localReplica);
      }
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
