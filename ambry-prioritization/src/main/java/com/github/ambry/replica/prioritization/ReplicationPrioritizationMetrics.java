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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.PartitionId;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class tracks metrics related to replication prioritization.
 * It provides visibility into the behavior and effectiveness of the ReplicationPrioritizationManager.
 */
public class ReplicationPrioritizationMetrics {
  // Metric Registry
  private final MetricRegistry metricRegistry;

  // Gauges to track partition counts in different priority tiers
  private final Gauge<Integer> belowMinReplicaWithDisruptionCount;
  private final Gauge<Integer> belowMinReplicaNoDisruptionCount;
  private final Gauge<Integer> minReplicaWithDisruptionCount;
  private final Gauge<Integer> minReplicaNoDisruptionCount;
  private final Gauge<Integer> normalPriorityCount;

  public Gauge<Integer> getCurrentlyReplicatingPriorityPartitionsCount() {
    return currentlyReplicatingPriorityPartitionsCount;
  }

  public Gauge<Integer> getMinReplicaNoDisruptionCount() {
    return minReplicaNoDisruptionCount;
  }

  public Gauge<Integer> getMinReplicaWithDisruptionCount() {
    return minReplicaWithDisruptionCount;
  }

  public Gauge<Integer> getBelowMinReplicaNoDisruptionCount() {
    return belowMinReplicaNoDisruptionCount;
  }

  public Gauge<Integer> getBelowMinReplicaWithDisruptionCount() {
    return belowMinReplicaWithDisruptionCount;
  }

  public Gauge<Integer> getDisabledReplicationPartitionsCount() {
    return disabledReplicationPartitionsCount;
  }

  public Gauge<Integer> getAllBootstrappingPartitionsCount() {
    return allBootstrappingPartitionsCount;
  }

  public Gauge<Integer> getNormalPriorityCount() {
    return normalPriorityCount;
  }

  public Counter getPartitionsAddedToHighPriorityCount() {
    return partitionsAddedToHighPriorityCount;
  }

  public Counter getNormalToHighPriorityTransitionCount() {
    return normalToHighPriorityTransitionCount;
  }

  public Histogram getHighPriorityBatchSizeHistogram() {
    return highPriorityBatchSizeHistogram;
  }

  // Gauge to track currently replicating priority partitions
  private final Gauge<Integer> currentlyReplicatingPriorityPartitionsCount;

  // Gauge to track disabled replication partitions
  private final Gauge<Integer> disabledReplicationPartitionsCount;

  // Gauge for all bootstrapping partitions
  private final Gauge<Integer> allBootstrappingPartitionsCount;

  // Performance metrics
  private final Timer prioritizationCycleTime;
  private final Timer fetchDisruptionsTime;
  private final Timer controlReplicationThreadsTime;

  // Counters for partition state transitions
  private final Counter partitionsCompletedCount;
  private final Counter partitionsAddedToHighPriorityCount;
  private final Counter normalToHighPriorityTransitionCount;

  // Histogram for batch sizes
  private final Histogram highPriorityBatchSizeHistogram;

  // Maps to track per-partition metrics
  private final Map<String, AtomicLong> partitionReplicationTimeMs; // Key: partitionId.toPathString()
  private final Map<String, AtomicLong> partitionPriorityChangeCount; // Key: partitionId.toPathString()

  // Reference to latest prioritized partitions
  private volatile Map<PriorityTier, Set<PartitionId>> prioritizedPartitions;
  private volatile Set<PartitionId> currentlyReplicatingPriorityPartitions;
  private volatile Set<PartitionId> disabledReplicationPartitions;
  private volatile Set<PartitionId> allBootstrappingPartitions;

  /**
   * Creates a new ReplicationPrioritizationMetrics instance.
   * @param metricRegistry The MetricRegistry to register metrics with.
   */
  public ReplicationPrioritizationMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;

    // Initialize maps for per-partition metrics
    this.partitionReplicationTimeMs = new ConcurrentHashMap<>();
    this.partitionPriorityChangeCount = new ConcurrentHashMap<>();

    // Register gauge metrics
    this.belowMinReplicaWithDisruptionCount = () -> prioritizedPartitions != null ?
        prioritizedPartitions.getOrDefault(PriorityTier.BELOW_MIN_REPLICA_WITH_DISRUPTION, new HashSet<>()).size() : 0;

    this.belowMinReplicaNoDisruptionCount = () -> prioritizedPartitions != null ?
        prioritizedPartitions.getOrDefault(PriorityTier.BELOW_MIN_REPLICA_NO_DISRUPTION, new HashSet<>()).size() : 0;

    this.minReplicaWithDisruptionCount = () -> prioritizedPartitions != null ?
        prioritizedPartitions.getOrDefault(PriorityTier.MIN_REPLICA_WITH_DISRUPTION, new HashSet<>()).size() : 0;

    this.minReplicaNoDisruptionCount = () -> prioritizedPartitions != null ?
        prioritizedPartitions.getOrDefault(PriorityTier.MIN_REPLICA_NO_DISRUPTION, new HashSet<>()).size() : 0;

    this.normalPriorityCount = () -> prioritizedPartitions != null ?
        prioritizedPartitions.getOrDefault(PriorityTier.NORMAL, new HashSet<>()).size() : 0;

    this.currentlyReplicatingPriorityPartitionsCount = () -> currentlyReplicatingPriorityPartitions != null ?
        currentlyReplicatingPriorityPartitions.size() : 0;

    this.disabledReplicationPartitionsCount = () -> disabledReplicationPartitions != null ?
        disabledReplicationPartitions.size() : 0;

    this.allBootstrappingPartitionsCount = () -> allBootstrappingPartitions != null ?
        allBootstrappingPartitions.size() : 0;

    // Register all gauge metrics to the registry
    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "BelowMinReplicaWithDisruptionCount"),
        belowMinReplicaWithDisruptionCount);

    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "BelowMinReplicaNoDisruptionCount"),
        belowMinReplicaNoDisruptionCount);

    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "MinReplicaWithDisruptionCount"),
        minReplicaWithDisruptionCount);

    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "MinReplicaNoDisruptionCount"),
        minReplicaNoDisruptionCount);

    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "NormalPriorityCount"),
        normalPriorityCount);

    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "CurrentlyReplicatingPriorityPartitionsCount"),
        currentlyReplicatingPriorityPartitionsCount);

    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "DisabledReplicationPartitionsCount"),
        disabledReplicationPartitionsCount);

    metricRegistry.register(MetricRegistry.name(ReplicationPrioritizationManager.class, "AllBootstrappingPartitionsCount"),
        allBootstrappingPartitionsCount);

    // Create timer metrics
    this.prioritizationCycleTime = metricRegistry.timer(
        MetricRegistry.name(ReplicationPrioritizationManager.class, "PrioritizationCycleTimeMs"));

    this.fetchDisruptionsTime = metricRegistry.timer(
        MetricRegistry.name(ReplicationPrioritizationManager.class, "FetchDisruptionsTimeMs"));

    this.controlReplicationThreadsTime = metricRegistry.timer(
        MetricRegistry.name(ReplicationPrioritizationManager.class, "ControlReplicationThreadsTimeMs"));

    // Create counter metrics
    this.partitionsCompletedCount = metricRegistry.counter(
        MetricRegistry.name(ReplicationPrioritizationManager.class, "PartitionsCompletedCount"));

    this.partitionsAddedToHighPriorityCount = metricRegistry.counter(
        MetricRegistry.name(ReplicationPrioritizationManager.class, "PartitionsAddedToHighPriorityCount"));

    this.normalToHighPriorityTransitionCount = metricRegistry.counter(
        MetricRegistry.name(ReplicationPrioritizationManager.class, "NormalToHighPriorityTransitionCount"));

    // Create histogram metrics
    this.highPriorityBatchSizeHistogram = metricRegistry.histogram(
        MetricRegistry.name(ReplicationPrioritizationManager.class, "HighPriorityBatchSizeHistogram"));
  }

  /**
   * Update references to the current priority partitions.
   * @param prioritizedPartitions Map of priority tiers to partitions.
   */
  public void updatePrioritizedPartitions(Map<PriorityTier, Set<PartitionId>> prioritizedPartitions) {
    this.prioritizedPartitions = prioritizedPartitions;
  }

  /**
   * Update references to currently replicating priority partitions.
   * @param currentlyReplicatingPriorityPartitions Set of currently replicating priority partitions.
   */
  public void updateCurrentlyReplicatingPriorityPartitions(Set<PartitionId> currentlyReplicatingPriorityPartitions) {
    this.currentlyReplicatingPriorityPartitions = currentlyReplicatingPriorityPartitions;
    this.highPriorityBatchSizeHistogram.update(currentlyReplicatingPriorityPartitions.size());
  }

  /**
   * Update references to disabled replication partitions.
   * @param disabledReplicationPartitions Set of disabled partitions.
   */
  public void updateDisabledReplicationPartitions(Set<PartitionId> disabledReplicationPartitions) {
    this.disabledReplicationPartitions = disabledReplicationPartitions;
  }

  /**
   * Update references to all bootstrapping partitions.
   * @param allBootstrappingPartitions Set of all bootstrapping partitions.
   */
  public void updateAllBootstrappingPartitions(Set<PartitionId> allBootstrappingPartitions) {
    this.allBootstrappingPartitions = allBootstrappingPartitions;
  }

  /**
   * Record completion of replication for a partition.
   */
  public void recordPartitionReplicationComplete() {
    partitionsCompletedCount.inc();
  }

  /**
   * Record completion of replication for a partition.
   */
  public void recordPartitionReplicationComplete(int numPartitions) {
    partitionsCompletedCount.inc(numPartitions);
  }

  /**
   * Record addition of partitions to high priority replication.
   * @param newHighPriorityPartitions The partitions added to high priority.
   */
  public void recordPartitionsAddedToHighPriority(Set<PartitionId> newHighPriorityPartitions) {
    partitionsAddedToHighPriorityCount.inc(newHighPriorityPartitions.size());
  }

  /**
   * Record a transition from normal replication to high priority replication.
   */
  public void recordNormalToHighPriorityTransition() {
    normalToHighPriorityTransitionCount.inc();
  }
}
