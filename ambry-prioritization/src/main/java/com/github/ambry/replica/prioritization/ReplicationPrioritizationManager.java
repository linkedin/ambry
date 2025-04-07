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
import com.github.ambry.replica.prioritization.disruption.DisruptionService;
import com.github.ambry.replica.prioritization.disruption.Operation;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.store.StorageManager;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
  private final Set<PartitionId> currentlyReplicatingPriorityPartitions;
  private final Set<PartitionId> disabledReplicationPartitions;
  private Set<PartitionId> allBootstrappingPartitions;
  private final String datacenterName;
  private final int minBatchSizeForHighPriorityPartitions;
  private final AtomicBoolean isHighPriorityReplicationRunning;
  private final Set<PartitionId> completedPartitions;
  private volatile long lastReplicationActivityMs;
  private final StorageManager storageManager;
  private final Time time;
  private final long scheduleIntervalMinutes;
  private final ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode>
      clusterManagerQueryHelper;
  private final DisruptionService disruptionService;
  private Map<PriorityTier, Set<PartitionId>> prioritizedPartitions;
  private final ScheduledExecutorService scheduler;
  /**
   * Creates a new ReplicationPrioritizationManager.
   * @param replicationEngine The ReplicationEngine to control partition replication.
   * @param clusterMap The ClusterMap to get partition and replica information.
   * @param dataNodeId The DataNodeId of the local node.
   * @param scheduler The scheduler to run periodic tasks.
   * @param datacenterName The name of the local datacenter.
   */

  public ReplicationPrioritizationManager(ReplicationEngine replicationEngine, ClusterMap clusterMap, DataNodeId dataNodeId,
      ScheduledExecutorService scheduler, String datacenterName, StorageManager storageManager,
      ReplicationConfig replicationConfig, ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode>
      clusterManagerQueryHelper, DisruptionService disruptionService) {
    this.replicationEngine = replicationEngine;
    this.clusterMap = clusterMap;
    this.dataNodeId = dataNodeId;
    this.prioritizationWindowMs = replicationConfig.diruptionReadinessWindowInMS;
    this.scheduleIntervalMinutes = replicationConfig.scheduledIntervalMinutes;
    this.rwLock = new ReentrantReadWriteLock();
    this.currentlyReplicatingPriorityPartitions = ConcurrentHashMap.newKeySet();
    this.disabledReplicationPartitions = ConcurrentHashMap.newKeySet();
    this.allBootstrappingPartitions = ConcurrentHashMap.newKeySet();
    this.datacenterName = datacenterName;
    this.minBatchSizeForHighPriorityPartitions = replicationConfig.highPriorityPartitionsBatchSize;
    this.isHighPriorityReplicationRunning = new AtomicBoolean(false);
    this.completedPartitions = ConcurrentHashMap.newKeySet();
    this.lastReplicationActivityMs = SystemTime.getInstance().milliseconds();
    this.storageManager = storageManager;
    this.time = SystemTime.getInstance();
    this.clusterManagerQueryHelper = clusterManagerQueryHelper;
    this.disruptionService = disruptionService;
    this.prioritizedPartitions = new EnumMap<>(PriorityTier.class);
    this.scheduler = scheduler;

    // Schedule periodic runs for prioritization run
    this.scheduler.scheduleAtFixedRate(this, 0, scheduleIntervalMinutes, TimeUnit.MINUTES);

    logger.info("ReplicationPrioritizationManager initialized with prioritization window of {} hours, schedule interval of {} minutes, " +
            "and min batch size of {} partitions", prioritizationWindowMs, scheduleIntervalMinutes,
        minBatchSizeForHighPriorityPartitions);
  }


  public Set<PartitionId> getCurrentlyReplicatingPriorityPartitions() {
    return currentlyReplicatingPriorityPartitions;
  }

  /**
   * Main entry point for the replication prioritization manager.
   */
  @Override
  public void run() {
    startPrioritizationCycle();
  }

  public void shutdown() {
    if (!scheduler.isTerminated()) {
      logger.info("Shutting down ReplicationPrioritizationManager");
      Utils.shutDownExecutorService(scheduler, 5, TimeUnit.SECONDS);
      // clear all maps
      currentlyReplicatingPriorityPartitions.clear();
      disabledReplicationPartitions.clear();
      allBootstrappingPartitions.clear();
      completedPartitions.clear();
      prioritizedPartitions.clear();
      isHighPriorityReplicationRunning.set(false);
    } else {
      logger.info("ReplicationPrioritizationManager already shut down");
    }
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
  void startPrioritizationCycle() {
    try {
      logger.info("Starting partition prioritization run");

      // 1. Get all bootstrapping partitions from StorageManager
      allBootstrappingPartitions = getAllBootstrappingPartitionsForNode();

      // Process any completed partitions
      processCompletedPartitions();

      Set<PartitionId> partitionIds = new HashSet<>(allBootstrappingPartitions);
      if (partitionIds.isEmpty()) {
        logger.info("Bootstrapping partition list from StorageManager is empty");
        return;
      }

      partitionIds.removeAll(currentlyReplicatingPriorityPartitions);

      if (partitionIds.isEmpty()) {
        logger.info("All bootstrapping partitions are already being replicated");
      }

      Map<PartitionId, List<Operation>> disruptionsByPartition = fetchDisruptions(new ArrayList<>(partitionIds));

      if (disruptionsByPartition == null || disruptionsByPartition.isEmpty()) {
        logger.info("No disruptions found for any partitions");
      } else {
        logger.info("Fetched disruptions for {} partitions", disruptionsByPartition.size());
      }
      // 4. Create prioritized partition lists
      prioritizedPartitions = prioritizePartitions(partitionIds, disruptionsByPartition);

      prioritizedPartitions.keySet().forEach(priorityTier -> logger.info("Found {} partitions in {} category", prioritizedPartitions.get(priorityTier).size(), priorityTier));

      // 5. Update replication priorities
      if (prioritizedPartitions.entrySet().stream().
          filter(entry -> entry.getKey() != PriorityTier.NORMAL).
          allMatch(entry -> entry.getValue().isEmpty())
          && !isHighPriorityReplicationRunning.get()) {
        logger.info("No new high-priority partitions identified "
            + "and no existing high priority run, enabling replication for disabled partitions");

        if (disabledReplicationPartitions.isEmpty()) {
          logger.info("No disabled partitions to enable");
        } else {
          logger.info("Found {} disabled partitions to enable", disabledReplicationPartitions.size());
          resetToNormalReplication();
        }
      } else {
        // Add partitions according to priority order
        Set<PartitionId> highPriorityPartitions = new HashSet<>();

        for (PriorityTier priorityTier : PriorityTier.values()) {
          Set<PartitionId> partitions = prioritizedPartitions.get(priorityTier);
          if (!partitions.isEmpty()) {
            int partitionsToAdd = Math.min(minBatchSizeForHighPriorityPartitions - highPriorityPartitions.size(), partitions.size());
            highPriorityPartitions.addAll(partitions.stream().limit(partitionsToAdd).collect(Collectors.toSet()));

            if (highPriorityPartitions.size() >= minBatchSizeForHighPriorityPartitions) {
              break;
            }
          }
        }

        logger.info("Identified {} high-priority partitions for replication", highPriorityPartitions.size());
        updateReplicationSet(highPriorityPartitions);
      }

    } catch (Exception e) {
      logger.error("Error in partition prioritization task", e);
    }
  }

  /**
   * Fetches disruptions for the given partitions.
   *
   * @param partitionIds List of partitions to fetch disruptions for
   * @return Map of PartitionId to list of disruptions
   */
  private Map<PartitionId, List<Operation>> fetchDisruptions(List<PartitionId> partitionIds) {
    Map<PartitionId, List<Operation>> disruptionsByPartition = new HashMap<>();
    try {
      // 3. Fetch disruptions for all partitions
      disruptionsByPartition =
          disruptionService.batchDisruptionsByPartition(new ArrayList<>(partitionIds));
    } catch (Exception e) {
      logger.error("Error fetching disruptions from DisruptionService", e);
      logger.info("Proceeding with prioritization based on MIN_ACTIVE_REPLICA", e);
    }

    return disruptionsByPartition;
  }

  /**
   * Prioritizes partitions based on replica count and disruption status.
   *
   * @param partitionIds Set of all partitions to evaluate
   * @param disruptionsByPartition Map of partitions to their disruptions
   * @return Map of priority categories to sets of partitions
   */
  private Map<PriorityTier, Set<PartitionId>> prioritizePartitions(Set<PartitionId> partitionIds, Map<PartitionId, List<Operation>> disruptionsByPartition) {
    // Filter disruptions within our window
    Map<PartitionId, Boolean> hasUpcomingDisruption = populateDisruptionData(disruptionsByPartition);

    // Categorize all partitions
    Map<PriorityTier, Set<PartitionId>> result = assignPriorityTierToPartitions(partitionIds, hasUpcomingDisruption);

    return result;
  }

  /**
   * Assigns priority tiers to partitions based on their replica count and disruption status.
   *
   * @param partitionIds Set of all partitions to evaluate
   * @param hasUpcomingDisruption Map of partitions to their disruption status
   * @return Map of priority categories to sets of partitions
   */
  private Map<PriorityTier, Set<PartitionId>> assignPriorityTierToPartitions(Set<PartitionId> partitionIds, Map<PartitionId, Boolean> hasUpcomingDisruption) {
    Map<PriorityTier, Set<PartitionId>> result = new EnumMap<>(PriorityTier.class);
    // Initialize all priority categories with empty sets
    for (PriorityTier category : PriorityTier.values()) {
      result.put(category, new HashSet<>());
    }

    for (PartitionId partition : partitionIds) {
      int localReplicaCount = calculateLocalReplicaCount(partition);
      int minActiveReplica = getMinActiveReplicas(partition);
      boolean isBelowMinActiveReplica = false;
      boolean isAtMinActiveReplica = false;
      if (localReplicaCount < minActiveReplica) {
        isBelowMinActiveReplica = true;
      } else if (localReplicaCount == minActiveReplica) {
        isAtMinActiveReplica = true;
      }

      boolean hasDisruption = hasUpcomingDisruption.getOrDefault(partition, false);

      if (isBelowMinActiveReplica && hasDisruption) {
        result.get(PriorityTier.BELOW_MIN_REPLICA_WITH_DISRUPTION).add(partition);
        logger.info("High-priority partition {} with count below minActiveReplica and upcoming disruption",
            partition.toPathString());
      } else if (isBelowMinActiveReplica) {
        result.get(PriorityTier.BELOW_MIN_REPLICA_NO_DISRUPTION).add(partition);
        logger.info("High-priority partition {} with count below minActiveReplica",
            partition.toPathString());
      } else if (isAtMinActiveReplica && hasDisruption) {
        result.get(PriorityTier.MIN_REPLICA_WITH_DISRUPTION).add(partition);
        logger.info("High-priority partition {} with count at minActiveReplica and upcoming disruption",
            partition.toPathString());
      } else if (isAtMinActiveReplica) {
        result.get(PriorityTier.MIN_REPLICA_NO_DISRUPTION).add(partition);
        logger.info("High-priority partition {} with count at minActiveReplica",
            partition.toPathString());
      } else {
        result.get(PriorityTier.NORMAL).add(partition);
      }
    }

    return result;
  }


  /**
   * Populates disruption data for each partition.
   *
   * @param disruptionsByPartition Map of partitions to their disruptions
   * @return Map of partitions to their disruption status
   */
  private Map<PartitionId, Boolean> populateDisruptionData(Map<PartitionId, List<Operation>> disruptionsByPartition) {
    Map<PartitionId, Boolean> hasUpcomingDisruption = new HashMap<>();
    long currentTimeMs = time.milliseconds();
    if (disruptionsByPartition != null) {
      for (Map.Entry<PartitionId, List<Operation>> entry : disruptionsByPartition.entrySet()) {
        PartitionId partitionId = entry.getKey();
        List<Operation> operations = entry.getValue();

        // Check if any operation is within our prioritization window
        boolean hasDisruptionInWindow = operations.stream().anyMatch(op -> {
          long scheduledTime = op.getStartTime();
          return scheduledTime > currentTimeMs && scheduledTime - currentTimeMs >= prioritizationWindowMs;
        });

        hasUpcomingDisruption.put(partitionId, hasDisruptionInWindow);
      }
    }

    return hasUpcomingDisruption;
  }


  /**
   * Resets replication to normal operation, enabling all partitions.
   */
  private void resetToNormalReplication() {
    try {
      logger.info("Restoring normal replication for disabled partitions");

      // Helps the mock replication engine
      Set<PartitionId> copyOfDisabledReplicationPartitions = new HashSet<>(disabledReplicationPartitions);
      // Enable replication for previously disabled partitions
      replicationEngine.controlReplicationForPartitions(copyOfDisabledReplicationPartitions, Collections.emptyList(),
          true);
      disabledReplicationPartitions.clear();
      logger.debug("Disabling high priority partition flag isHighPriorityReplicationRunning={}",
          isHighPriorityReplicationRunning.get());
    } catch (Exception e) {
      logger.error("Error resetting to normal replication", e);
    }
  }

  /**
   * Process any partitions that have completed replication.
   */
  private void processCompletedPartitions() {
    currentlyReplicatingPriorityPartitions.stream().filter(this::hasCompletedReplication).forEach(completedPartitions::add);

    // Remove completed partitions from current set
    currentlyReplicatingPriorityPartitions.removeAll(completedPartitions);
    logger.info("Removed {} completed partitions from replication set", completedPartitions.size());

    if (currentlyReplicatingPriorityPartitions.isEmpty()) {
      logger.info("No currently replicating partitions, disabling high-priority replication");
      isHighPriorityReplicationRunning.set(false);
    }
  }

  /**
   * Updates the replication set based on the high priority partitions.
   * @param highPriorityPartitions The set of partitions that should be prioritized.
   */
  private void updateReplicationSet(Set<PartitionId> highPriorityPartitions) {

    if (highPriorityPartitions.isEmpty()) {
      return;
    }

    try {
      // Identify truly new high-priority partitions that aren't already being replicated
      Set<PartitionId> newHighPriorityPartitions = new HashSet<>(highPriorityPartitions);
      newHighPriorityPartitions.removeAll(currentlyReplicatingPriorityPartitions);

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
          currentlyReplicatingPriorityPartitions.addAll(newHighPriorityPartitions);

          // Ensure we have at least the minimum batch size
          if (currentlyReplicatingPriorityPartitions.size() < minBatchSizeForHighPriorityPartitions) {
            addNewPartitions();
          }

          // Update the priority settings
          controlReplicationThreads();

          // Record activity timestamp
          lastReplicationActivityMs = time.milliseconds();
        }
      }
    } catch (Exception e) {
      logger.error("Error updating replication set", e);
    }
  }

  /**
   * Starts high priority replication for the given partitions.
   * @param highPriorityPartitions The set of partitions that should be prioritized.
   */
  private void startHighPriorityReplication(Set<PartitionId> highPriorityPartitions) {
    try {
      // Initialize the set of replicating partitions with the high-priority ones
      currentlyReplicatingPriorityPartitions.clear();
      currentlyReplicatingPriorityPartitions.addAll(highPriorityPartitions);

      // Ensure we have at least the minimum batch size
      if (currentlyReplicatingPriorityPartitions.size() < minBatchSizeForHighPriorityPartitions) {
        addNewPartitions();
      }

      // Apply the replication priorities
      controlReplicationThreads();

      // Update state
      isHighPriorityReplicationRunning.set(true);
      lastReplicationActivityMs = time.milliseconds();

      logger.info("Started high-priority replication for {} partitions", currentlyReplicatingPriorityPartitions.size());
    } catch (Exception e) {
      logger.error("Error starting high-priority replication", e);
    }
  }

  /**
   * Add additional partitions to meet the minimum batch size requirement.
   * Now follows the priority ordering when adding partitions.
   */
  private void addNewPartitions() {
    int additionalPartitionsNeeded = minBatchSizeForHighPriorityPartitions - currentlyReplicatingPriorityPartitions.size();

    if (additionalPartitionsNeeded <= 0) {
      return;
    }

    logger.info("Adding {} additional partitions to reach minimum batch size of {}",
        additionalPartitionsNeeded, minBatchSizeForHighPriorityPartitions);

    // Get all partitions for this node
    Set<PartitionId> remainingPartitions = new HashSet<>(allBootstrappingPartitions);
    // Remove already prioritized partitions
    remainingPartitions.removeAll(currentlyReplicatingPriorityPartitions);

    if (remainingPartitions.isEmpty()) {
      logger.info("No additional partitions available to add to batch");
      return;
    }

    // Prioritize the remaining partitions
    // Add partitions in priority order until we reach the minimum batch size
    fillUptoBatchSize(additionalPartitionsNeeded, remainingPartitions);
  }

  /**
   * Fill the currently replicating partitions up to the minimum batch size.
   * @param additionalPartitionsNeeded addition partitions needed
   * @param remainingPartitions remaining partitions to choose from
   */
  private void fillUptoBatchSize(int additionalPartitionsNeeded, Set<PartitionId> remainingPartitions) {
    int added = 0;
    for (PriorityTier priorityTier : PriorityTier.values()) {
      Set<PartitionId> partitions = prioritizedPartitions.get(priorityTier);
      if (!partitions.isEmpty()) {
        int toAdd = Math.min(additionalPartitionsNeeded - added, partitions.size());
        Set<PartitionId> partitionsToAdd = partitions.stream().filter(remainingPartitions::contains).limit(toAdd).collect(Collectors.toSet());
        currentlyReplicatingPriorityPartitions.addAll(partitionsToAdd);
        added += partitionsToAdd.size();
        logger.info("Added {} partitions from {} category", partitionsToAdd.size(), priorityTier);

        if (added >= additionalPartitionsNeeded) {
          break;
        }
      }
    }
  }

  /**
   * Update the replication threads to reflect the current priority set.
   */
  private void controlReplicationThreads() {
    // Get all partitions that aren't currently replicating
    Set<PartitionId> allPartitions = new HashSet<>(allBootstrappingPartitions);
    Set<PartitionId> partitionsToDisable = new HashSet<>(allPartitions);
    partitionsToDisable.removeAll(currentlyReplicatingPriorityPartitions);

    // Disable non-priority partitions
    if (!partitionsToDisable.isEmpty()) {
      logger.info("Disabling replication for {} non-priority partitions", partitionsToDisable.size());
      replicationEngine.controlReplicationForPartitions(partitionsToDisable, Collections.emptyList(), false);
      disabledReplicationPartitions.addAll(partitionsToDisable);
    }

    // Enable high-priority partitions
    logger.info("Enabling replication for {} high-priority partitions", currentlyReplicatingPriorityPartitions.size());
    replicationEngine.controlReplicationForPartitions(currentlyReplicatingPriorityPartitions, Collections.emptyList(), true);
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
  private int calculateLocalReplicaCount(PartitionId partition) {
    Set<ReplicaState> states = new HashSet<>(Arrays.asList(ReplicaState.LEADER, ReplicaState.STANDBY));
    Map<ReplicaState, List<AmbryReplica>> localDCReplicas =
        (Map<ReplicaState, List<AmbryReplica>>) partition.getReplicaIdsByStates(states, datacenterName);
    logger.info("Found {} local replicas for partition {}", localDCReplicas.values().stream().mapToInt(List::size).sum(),
        partition.toPathString());
    return localDCReplicas.values().stream().mapToInt(List::size).sum();
  }

  private int getMinActiveReplicas(PartitionId partitionId) {
    return clusterManagerQueryHelper.getMinActiveReplicas(partitionId);
  }
}
