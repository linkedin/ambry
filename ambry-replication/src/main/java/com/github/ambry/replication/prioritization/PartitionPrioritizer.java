/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.replication.prioritization;

import com.github.ambry.clustermap.ClusterManagerQueryHelper;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages priorities for partitions during replication. This class provides mechanisms to:
 * 1. Set and get priorities for partitions
 * 2. Automatically adjust priorities based on age (time since last prioritized)
 * 3. Adjust priorities based on active replica count
 * 4. Promote partitions to higher priority when others complete bootstrap
 */
public class PartitionPrioritizer {
  private static final Logger logger = LoggerFactory.getLogger(PartitionPrioritizer.class);


  /**
   * Priority levels for partitions
   */
  // TODO: Adding scoring for each priority level. Once integration with disruption poller is completed
  public enum Priority {
    HIGH,   // Highest priority, processed first
    MEDIUM, // Medium priority, processed after HIGH
    LOW     // Lowest priority, processed last
  }

  // Maps partitions to their priorities
  private final Map<PartitionId, Priority> partitionPriorities = new ConcurrentHashMap<>();

  // Records when a partition was last demoted from HIGH priority
  private final Map<PartitionId, Long> partitionDemotionTimeMs = new ConcurrentHashMap<>();

  private final ReplicationConfig replicationConfig;

  // Configuration parameters
  private final long agingThresholdMs;
  private final int minActiveReplicaCount;
  private final int maxPrioritizedPartitions;
  private final Time time;

  // Circuit breaker
  //private final PrioritizationCircuitBreaker circuitBreaker;

  // Metrics
  private final PrioritizationMetrics metrics;

  /**
   * Creates a new PartitionPrioritizer with the given configuration
   * @param replicationConfig Configuration for replication
   * @param metricRegistry Registry for recording metrics
   * @param time Time instance for tracking time
   */
  public PartitionPrioritizer(ReplicationConfig replicationConfig,
      com.codahale.metrics.MetricRegistry metricRegistry,
      Time time) {
    this.time = time;
    this.replicationConfig = replicationConfig;
    this.agingThresholdMs = replicationConfig.replicationPartitionAgingThresholdMs;
    // this should be dynamic based on cluster and if it is home DC or remote DC
    this.minActiveReplicaCount = 2;
    this.maxPrioritizedPartitions = replicationConfig.replicationMaxPrioritizedPartitions;

    // Initialize metrics
    this.metrics = new PrioritizationMetrics(metricRegistry);

    // Initialize circuit breaker
    //this.circuitBreaker = new PrioritizationCircuitBreaker(replicationConfig, metrics, time);

    logger.info("Initialized PartitionPrioritizer with agingThresholdMs={}, minActiveReplicaCount={}, maxPrioritizedPartitions={}",
        agingThresholdMs, minActiveReplicaCount, maxPrioritizedPartitions);
  }

  /**
   * Sets the priority of a partition
   * @param partitionId The partition to set priority for
   * @param priority The priority to set
   */
  public void setPriority(PartitionId partitionId, Priority priority) {
    Priority oldPriority = partitionPriorities.put(partitionId, priority);

    // If this was a demotion from HIGH, record the time
    if (oldPriority == Priority.HIGH && priority != Priority.HIGH) {
      partitionDemotionTimeMs.put(partitionId, time.milliseconds());
      logger.info("Partition {} demoted from HIGH to {} priority", partitionId, priority);
      metrics.incrementManualPriorityChangeCount();

      if (priority == Priority.MEDIUM) {
        metrics.incrementBootstrapDemotionCount();
      }
    } else if (priority == Priority.HIGH) {
      // If promoted to HIGH, remove demotion time
      partitionDemotionTimeMs.remove(partitionId);
      logger.info("Partition {} promoted to HIGH priority", partitionId);
      metrics.incrementManualPriorityChangeCount();
    }

    // Update metrics
    updatePriorityMetrics();
  }

  /**
   * Gets the priority of a partition
   * @param partitionId The partition to get priority for
   * @return The priority of the partition, or LOW if not explicitly set
   */
  public Priority getPriority(PartitionId partitionId) {
    return partitionPriorities.getOrDefault(partitionId, Priority.LOW);
  }

  public int getActiveReplicaCount(ClusterManagerQueryHelper<ReplicaId, ?, PartitionId, ?> queryHelper,
      PartitionId partition, String dcName) {
    List<ReplicaId> leaderReplicas = queryHelper.getReplicaIdsByState(partition, ReplicaState.LEADER, dcName);
    List<ReplicaId> standbyReplicas = queryHelper.getReplicaIdsByState(partition, ReplicaState.STANDBY, dcName);
    return leaderReplicas.size() + standbyReplicas.size();
  }

  /**
   * Updates priorities based on aging and active replica count
   * @param allPartitions All partitions to consider
   * @param activeReplicaCounts Map that returns active replica count for a partition
   */
  public void updatePriorities(Collection<PartitionId> allPartitions,
      Map<PartitionId, Integer> activeReplicaCounts) {
    // Check if prioritization is enabled
    if (!isPrioritizationEnabled()) {
      logger.debug("Prioritization is currently disabled by circuit breaker");
      return;
    }

    activeReplicaCounts.forEach((partition, count) -> {

      if (count < 0) {
        logger.warn("Invalid active replica count {} for partition {}", count, partition);
      }
    });

    // TODO: Figure out this part
    //getActiveReplicaCount(new HelixClusterManager.HelixClusterManagerQueryHelper(), partition, dcName);

    long currentTimeMs = time.milliseconds();
    List<PartitionId> agingPromotions = new ArrayList<>();
    List<PartitionId> replicaCountPromotions = new ArrayList<>();

    // First pass: identify partitions that need promotion
    for (PartitionId partitionId : allPartitions) {
      Priority currentPriority = getPriority(partitionId);
      if (currentPriority != Priority.HIGH) {
        // Check for aging promotion
        // check this logic again?
        Long demotionTimeMs = partitionDemotionTimeMs.get(partitionId);
        if (demotionTimeMs != null && currentTimeMs - demotionTimeMs > agingThresholdMs) {
          agingPromotions.add(partitionId);
        }

        // Check for active replica count promotion
        Integer activeReplicas = activeReplicaCounts.get(partitionId);
        if (activeReplicas != null) {
          // Record active replica count in metrics
          metrics.updateActiveReplicaCount(activeReplicas);

          if (activeReplicas < minActiveReplicaCount) {
            replicaCountPromotions.add(partitionId);
          }
        }
      }
    }

    // Log promotions
    if (!agingPromotions.isEmpty()) {
      logger.info("Promoting {} partitions due to aging: {}", agingPromotions.size(), agingPromotions);
      metrics.incrementAgingPromotionCount(agingPromotions.size());
    }
    if (!replicaCountPromotions.isEmpty()) {
      logger.info("Promoting {} partitions due to low active replica count: {}",
          replicaCountPromotions.size(), replicaCountPromotions);
      metrics.incrementReplicaCountPromotionCount(replicaCountPromotions.size());
    }

    // Apply promotions, respecting the max prioritized partitions limit
    Set<PartitionId> currentHighPriority = getPartitionsByPriority(allPartitions, Priority.HIGH);
    int slotsAvailable = maxPrioritizedPartitions - currentHighPriority.size();

    // Prioritize replica count promotions first (they're more critical)
    if (slotsAvailable > 0 && !replicaCountPromotions.isEmpty()) {
      int toPromote = Math.min(slotsAvailable, replicaCountPromotions.size());
      for (int i = 0; i < toPromote; i++) {
        setPriority(replicaCountPromotions.get(i), Priority.HIGH);
      }
      slotsAvailable -= toPromote;
    }

    // Then aging promotions if slots are still available
    if (slotsAvailable > 0 && !agingPromotions.isEmpty()) {
      int toPromote = Math.min(slotsAvailable, agingPromotions.size());
      for (int i = 0; i < toPromote; i++) {
        setPriority(agingPromotions.get(i), Priority.HIGH);
      }
    }

    // Update priority metrics
    updatePriorityMetrics();
  }

  /**
   * Gets partitions with the specified priority
   * @param allPartitions All partitions to consider
   * @param priority The priority to filter by
   * @return Set of partitions with the specified priority
   */
  public Set<PartitionId> getPartitionsByPriority(Collection<PartitionId> allPartitions, Priority priority) {
    return allPartitions.stream()
        .filter(p -> getPriority(p) == priority)
        .collect(Collectors.toSet());
  }

  /**
   * Called when a partition completes bootstrap to manage priority changes
   * @param bootstrappedPartition The partition that completed bootstrap
   * @param allPartitions All partitions to consider for promotion
   */
  public void onPartitionBootstrapComplete(PartitionId bootstrappedPartition, Collection<PartitionId> allPartitions) {
    // Demote the bootstrapped partition to MEDIUM priority
    setPriority(bootstrappedPartition, Priority.MEDIUM);

    // Check if prioritization is enabled
    if (!isPrioritizationEnabled()) {
      logger.debug("Prioritization is currently disabled");
      return;
    }

    // Check if we need to promote additional partitions
    Set<PartitionId> highPriorityPartitions = getPartitionsByPriority(allPartitions, Priority.HIGH);
    if (highPriorityPartitions.size() < maxPrioritizedPartitions) {
      promoteNextPartitions(allPartitions, maxPrioritizedPartitions - highPriorityPartitions.size());
    }
  }

  /**
   * Promotes the next batch of partitions to HIGH priority
   * @param allPartitions All partitions to consider
   * @param slotsAvailable Number of slots available for promotion
   */
  private void promoteNextPartitions(Collection<PartitionId> allPartitions, int slotsAvailable) {
    if (slotsAvailable <= 0) {
      return;
    }

    // Try to promote MEDIUM priority partitions first
    Set<PartitionId> mediumPriorityPartitions = getPartitionsByPriority(allPartitions, Priority.MEDIUM);

    // Sort by demotion time if available (oldest first)
    List<PartitionId> sortedPartitions = new ArrayList<>(mediumPriorityPartitions);
    sortedPartitions.sort((p1, p2) -> {
      Long t1 = partitionDemotionTimeMs.get(p1);
      Long t2 = partitionDemotionTimeMs.get(p2);
      if (t1 == null && t2 == null) return 0;
      if (t1 == null) return 1;
      if (t2 == null) return -1;
      return Long.compare(t1, t2);
    });

    int promotedCount = 0;
    for (PartitionId partition : sortedPartitions) {
      if (promotedCount >= slotsAvailable) break;
      setPriority(partition, Priority.HIGH);
      promotedCount++;
    }

    // If we still have slots and no MEDIUM partitions left, try LOW priority
    if (promotedCount < slotsAvailable) {
      Set<PartitionId> lowPriorityPartitions = getPartitionsByPriority(allPartitions, Priority.LOW);

      // Sort by demotion time if available (oldest first)
      sortedPartitions = new ArrayList<>(lowPriorityPartitions);
      sortedPartitions.sort((p1, p2) -> {
        Long t1 = partitionDemotionTimeMs.get(p1);
        Long t2 = partitionDemotionTimeMs.get(p2);
        if (t1 == null && t2 == null) return 0;
        if (t1 == null) return 1;
        if (t2 == null) return -1;
        return Long.compare(t1, t2);
      });

      for (PartitionId partition : sortedPartitions) {
        if (promotedCount >= slotsAvailable) break;
        setPriority(partition, Priority.HIGH);
        promotedCount++;
      }
    }

    if (promotedCount > 0) {
      logger.info("Promoted {} partitions to HIGH priority after bootstrap completion", promotedCount);
    }
  }

  /**
   * Gets all partition priorities for logging or debugging
   * @return Map of partitions to their priorities
   */
  public Map<PartitionId, Priority> getAllPriorities() {
    return new HashMap<>(partitionPriorities);
  }

  /**
   * Records the completion of a normal (unprioritized) replication cycle
   * @param cycleTimeMs Time in milliseconds taken for the cycle
   */
//  public void recordNormalCycleTime(long cycleTimeMs) {
//    metrics.recordNormalCycleTime(cycleTimeMs);
//  }

  /**
   * Records the completion of a prioritized replication cycle
   * @param priority The priority level of the cycle
   * @param cycleTimeMs Time in milliseconds taken for the cycle
   */
  public void recordPrioritizedCycleTime(Priority priority, long cycleTimeMs) {
    metrics.recordReplicationCycleTime(priority, cycleTimeMs);
  }

  /**
   * Checks if prioritization is currently enabled (not disabled by circuit breaker)
   * @return true if prioritization should be used, false otherwise
   */
  public boolean isPrioritizationEnabled() {
    return replicationConfig.isReplicationPrioritizationEnabled;
  }

  /**
   * Updates the priority metrics based on current state
   */
  private void updatePriorityMetrics() {
    int highCount = 0;
    int mediumCount = 0;
    int lowCount = 0;

    for (Priority priority : partitionPriorities.values()) {
      switch (priority) {
        case HIGH:
          highCount++;
          break;
        case MEDIUM:
          mediumCount++;
          break;
        case LOW:
          lowCount++;
          break;
      }
    }

    metrics.updatePriorityLevelCounts(highCount, mediumCount, lowCount);
  }
}