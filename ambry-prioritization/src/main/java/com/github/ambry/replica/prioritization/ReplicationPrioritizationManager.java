/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.AmbryDataNode;
import com.github.ambry.clustermap.AmbryDisk;
import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.AmbryReplica;
import com.github.ambry.clustermap.ClusterManagerQueryHelper;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.store.StorageManager;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


/**
 * The ReplicationPrioritizationManager prioritizes partitions for replication based on planned disruptions and replica count.
 * It periodically polls a DisruptionAPI to get information about planned disruptions and uses that information
 * to prioritize partitions with upcoming disruptions and low replica counts.
 * This implementation integrates with StorageManager to track actual replication progress and adjust
 * the priority set in real-time as partitions complete replication.
 */
public class ReplicationPrioritizationManager implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(ReplicationPrioritizationManager.class);


  private final ReplicationEngine replicationEngine;
  private final ClusterMap clusterMap;
  private final DataNodeId dataNodeId;
  private final long prioritizationWindowMs; // How far in advance to prioritize disruptions
  private final ReadWriteLock rwLock;
  private final Set<PartitionId> currentlyReplicatingPartitions;
  private final Set<PartitionId> disabledReplicationPartitions;
  private Set<PartitionId> allBootstrappingPartitions;
  private final String datacenterName;
  private final int lowReplicaThreshold;
  private final int minBatchSizeForHighPriorityPartitions;
  private final AtomicBoolean isHighPriorityReplicationRunning;
  private final long replicationTimeoutMs;
  private final Set<PartitionId> completedPartitions;
  private volatile long lastReplicationActivityMs;
  private final StorageManager storageManager;
  private final Time time;
  private final HelixClusterManager helixClusterManager;
  private final long scheduleIntervalMinutes;
  private final ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode>
      clusterManagerQueryHelper;
  /**
   * Creates a new ReplicationPrioritizationManager.
   * @param replicationEngine The ReplicationEngine to control partition replication.
   * @param clusterMap The ClusterMap to get partition and replica information.
   * @param dataNodeId The DataNodeId of the local node.
   * @param scheduler The scheduler to run periodic tasks.
   * @param prioritizationWindowHours How far in advance to prioritize disruptions (in hours).
   * @param datacenterName The name of the local datacenter.
   * @param lowReplicaThreshold The threshold for considering a partition to have low replica count.
   * @param minBatchSizeForHighPriorityPartitions Minimum number of partitions to include in high priority batch.
   * @param replicationTimeoutHours Maximum time without progress to allow before resetting.
   */

  ReplicationPrioritizationManager(ReplicationEngine replicationEngine, ClusterMap clusterMap, DataNodeId dataNodeId,
      ScheduledExecutorService scheduler, int prioritizationWindowHours, String datacenterName,
      int lowReplicaThreshold, int minBatchSizeForHighPriorityPartitions,
      int replicationTimeoutHours, StorageManager storageManager,
      ReplicationConfig replicationConfig, HelixClusterManager helixClusterManager, ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode>
      clusterManagerQueryHelper) {
    this.replicationEngine = replicationEngine;
    this.clusterMap = clusterMap;
    this.dataNodeId = dataNodeId;
    this.prioritizationWindowMs = replicationConfig.diruptionReadinessWindowInMS;
    this.scheduleIntervalMinutes = replicationConfig.scheduledIntervalMinutes;
    this.rwLock = new ReentrantReadWriteLock();
    this.currentlyReplicatingPartitions = ConcurrentHashMap.newKeySet();
    this.disabledReplicationPartitions = ConcurrentHashMap.newKeySet();
    this.allBootstrappingPartitions = ConcurrentHashMap.newKeySet();
    this.datacenterName = datacenterName;
    this.lowReplicaThreshold = lowReplicaThreshold;
    this.minBatchSizeForHighPriorityPartitions = minBatchSizeForHighPriorityPartitions;
    this.isHighPriorityReplicationRunning = new AtomicBoolean(false);
    this.replicationTimeoutMs = TimeUnit.HOURS.toMillis(replicationTimeoutHours);
    this.completedPartitions = ConcurrentHashMap.newKeySet();
    this.lastReplicationActivityMs = SystemTime.getInstance().milliseconds();
    this.storageManager = storageManager;
    this.time = SystemTime.getInstance();
    this.helixClusterManager = helixClusterManager;
    this.clusterManagerQueryHelper = clusterManagerQueryHelper;


    // Schedule periodic runs for disruption checking
    scheduler.scheduleAtFixedRate(this, 0, scheduleIntervalMinutes, TimeUnit.MINUTES);

    logger.info("ReplicationPrioritizationManager initialized with prioritization window of {} hours, schedule interval of {} minutes, " +
            "and min batch size of {} partitions", prioritizationWindowHours, scheduleIntervalMinutes,
        minBatchSizeForHighPriorityPartitions);
  }


  @Override
  public void run() {
    start();
  }

  /**
   * Starts the replication prioritization manager.
   * 1. Looks at existing high priority partitions and checks if they have completed replication.
   * 2. Identifies bootstrapping partitions from local store
   * 3. From these bootstrapping partitions filter partitions which have count <= MIN_ACTIVE_REPLICAS
   * 4. Fetches disruptions
   * 5. Filters for upcoming disruptions within our window
   * 6. Get affected partitions
   * 7. Control replication thread based on the high priority partitions
   */
  private void start() {
    try {
      logger.info("Starting partition prioritization run");

      // Process any completed partitions
      processCompletedPartitions();

      // 1. Get all bootstrapping partitions from StorageManager
      allBootstrappingPartitions = getAllBootstrappingPartitionsForNode();
      Set<PartitionId> partitionIds = new HashSet<>(allBootstrappingPartitions);
      partitionIds.removeAll(currentlyReplicatingPartitions);

      if (partitionIds.isEmpty()) {
        logger.info("Bootstrapping partition list is empty");
        return;
      }

      // 3. Analyze replica counts and create prioritized list
      Set<PartitionId> highPriorityPartitions = new HashSet<>();

      for (PartitionId partition : partitionIds) {
        boolean isCountBelowMinActiveReplica = isBelowMinActiveReplica(partition);

        // Check if this partition has low replica count
        if (isCountBelowMinActiveReplica) {
          highPriorityPartitions.add(partition);
          logger.info("Identifying high-priority partition {} with count below minActiveReplica",
              partition.toPathString());
        }
      }

// TODO: Implement disruption fetching
//      4. Fetch Disruptions
//            logger.info("Fetched {} planned disruptions", disruptions.size());
//
//      // 5.. Filter for upcoming disruptions within our window
//      long currentTimeMs = time.milliseconds();
//      List<Disruption> upcomingDisruptions = disruptions.stream()
//          .filter(d -> d.getScheduledTimeMs() > currentTimeMs && d.getScheduledTimeMs() - currentTimeMs >= prioritizationWindowMs)
//          .collect(Collectors.toList());
//
//      logger.info("Found {} disruptions scheduled within the next {} hours",
//          upcomingDisruptions.size(), TimeUnit.MILLISECONDS.toHours(prioritizationWindowMs));

//      // 6. Get affected partitions
//      Set<PartitionId> partitionsToAnalyze = new HashSet<>();
//
//      // Add partitions with upcoming disruptions
//      for (Disruption disruption : upcomingDisruptions) {
//        PartitionId partitionId = getPartitionFromString(disruption.getPartitionId());
//        if (partitionId != null) {
//          partitionsToAnalyze.add(partitionId);
//        }
//      }

      // 5. Update replication priorities
      updateReplicationSet(highPriorityPartitions);
    } catch (Exception e) {
      logger.error("Error in partition prioritization task", e);
    }
  }

  /**
   * Process any partitions that have completed replication.
   */
  private void processCompletedPartitions() {
    currentlyReplicatingPartitions.stream().filter(this::hasCompletedReplication).forEach(completedPartitions::add);

    // Remove completed partitions from current set
    currentlyReplicatingPartitions.removeAll(completedPartitions);
    logger.info("Removed {} completed partitions from replication set", completedPartitions.size());
  }

  /**
   * Updates the replication set based on the high priority partitions.
   * @param highPriorityPartitions The set of partitions that should be prioritized.
   */
  private void updateReplicationSet(Set<PartitionId> highPriorityPartitions) {
    if (highPriorityPartitions.isEmpty()) {
      return;
    }

    rwLock.writeLock().lock();
    try {
      // Identify truly new high-priority partitions that aren't already being replicated
      Set<PartitionId> newHighPriorityPartitions = new HashSet<>(highPriorityPartitions);
      newHighPriorityPartitions.removeAll(currentlyReplicatingPartitions);

      if (newHighPriorityPartitions.isEmpty() && isHighPriorityReplicationRunning.get()) {
        logger.info("No new high-priority partitions identified, continuing with current replication set");
        return;
      }

      // If we're not already in high-priority mode, start it
      if (!isHighPriorityReplicationRunning.get()) {
        // Start high-priority replication with all identified high-priority partitions
        startHighPriorityReplication(highPriorityPartitions);
      } else {
        // We're already in high-priority mode, just add the new partitions
        if (!newHighPriorityPartitions.isEmpty()) {
          logger.info("Adding {} new high-priority partitions to existing replication set",
              newHighPriorityPartitions.size());

          // Enable replication for the new high-priority partitions
          currentlyReplicatingPartitions.addAll(newHighPriorityPartitions);

          // Ensure we have at least the minimum batch size
          if (currentlyReplicatingPartitions.size() < minBatchSizeForHighPriorityPartitions) {
            addNewPartitions();
          }

          // Update the priority settings
          controlReplicationThreads();

          // Record activity timestamp
          lastReplicationActivityMs = time.milliseconds();
        }
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Starts high priority replication for the given partitions.
   * @param highPriorityPartitions The set of partitions that should be prioritized.
   */
  private void startHighPriorityReplication(Set<PartitionId> highPriorityPartitions) {
    rwLock.writeLock().lock();
    try {
      // Initialize the set of replicating partitions with the high-priority ones
      currentlyReplicatingPartitions.clear();
      currentlyReplicatingPartitions.addAll(highPriorityPartitions);

      // Ensure we have at least the minimum batch size
      if (currentlyReplicatingPartitions.size() < minBatchSizeForHighPriorityPartitions) {
        addNewPartitions();
      }

      // Apply the replication priorities
      controlReplicationThreads();

      // Update state
      isHighPriorityReplicationRunning.set(true);
      lastReplicationActivityMs = time.milliseconds();

      logger.info("Started high-priority replication for {} partitions", currentlyReplicatingPartitions.size());
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Add additional partitions to meet the minimum batch size requirement.
   */
  private void addNewPartitions() {
    int additionalPartitionsNeeded = minBatchSizeForHighPriorityPartitions - currentlyReplicatingPartitions.size();

    if (additionalPartitionsNeeded <= 0) {
      return;
    }

    logger.info("Adding {} additional normal partitions to reach minimum batch size of {}",
        additionalPartitionsNeeded, minBatchSizeForHighPriorityPartitions);

    // Get all partitions for this node
    Set<PartitionId> allPartitions = new HashSet<>(allBootstrappingPartitions);
    // Remove already prioritized partitions
    allPartitions.removeAll(currentlyReplicatingPartitions);

    // Add additional normal partitions up to the minimum batch size
    if (!allPartitions.isEmpty()) {
      int partitionsToAdd = Math.min(additionalPartitionsNeeded, allPartitions.size());
      currentlyReplicatingPartitions.addAll(allPartitions.stream().limit(partitionsToAdd).collect(Collectors.toSet()));
    }
  }

  /**
   * Update the replication threads to reflect the current priority set.
   */
  private void controlReplicationThreads() {
    // Get all partitions that aren't currently replicating
    Set<PartitionId> allPartitions = new HashSet<>(allBootstrappingPartitions);
    Set<PartitionId> partitionsToDisable = new HashSet<>(allPartitions);
    partitionsToDisable.removeAll(currentlyReplicatingPartitions);

    // Disable non-priority partitions
    if (!partitionsToDisable.isEmpty()) {
      logger.info("Disabling replication for {} non-priority partitions", partitionsToDisable.size());
      replicationEngine.controlReplicationForPartitions(partitionsToDisable, Collections.emptyList(), false);
      disabledReplicationPartitions.addAll(partitionsToDisable);
    }

    // Enable high-priority partitions
    logger.info("Enabling replication for {} high-priority partitions", currentlyReplicatingPartitions.size());
    replicationEngine.controlReplicationForPartitions(currentlyReplicatingPartitions, Collections.emptyList(), true);
  }


  /**
   * Manually check if a specific partition has completed replication.
   * This uses storageManager to verify status.
   * @param partitionId The partition to check.
   * @return true if replication is complete (bootstrap finished)
   */
  public boolean hasCompletedReplication(PartitionId partitionId) {

    // Check if replica is in STANDBY or LEADER state, which means bootstrap is complete
    ReplicaState state = storageManager.getStore(partitionId).getCurrentState();
    return state == ReplicaState.STANDBY || state == ReplicaState.LEADER;
  }


  /**
   * Gets all partitions for the current node.
   * @return A list of all partitions this node is responsible for.
   */
  private Set<PartitionId> getAllBootstrappingPartitionsForNode() {
    return storageManager.getLocalPartitions().
        stream().filter(partitionId -> !hasCompletedReplication(partitionId)).collect(Collectors.toSet());
  }

  /**
   * Gets the number of replicas for a partition in the local datacenter.
   * @param partition The partition to check.
   * @return The number of replicas in the local datacenter.
   */
  private boolean isBelowMinActiveReplica(PartitionId partition) {

    int minActiveReplicaCount = clusterManagerQueryHelper.getMinActiveReplicas(partition);
    Set<ReplicaState> states = new HashSet<>(Arrays.asList(ReplicaState.LEADER, ReplicaState.STANDBY));
    Map<ReplicaState, List<AmbryReplica>> localDCReplicas =
        (Map<ReplicaState, List<AmbryReplica>>) partition.getReplicaIdsByStates(states, datacenterName);
    logger.info("Found {} local replicas for partition {}", localDCReplicas.values().stream().mapToInt(List::size).sum(),
        partition.toPathString());
    return localDCReplicas.values().stream().mapToInt(List::size).sum() <= minActiveReplicaCount;
  }
}