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
package com.github.ambry.replication.prioritization;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Metrics for the partition prioritization system.
 */
public class PrioritizationMetrics {
  // Gauges to track current count of partitions at each priority level
  private final Gauge<Integer> highPriorityPartitionCount;
  private final Gauge<Integer> mediumPriorityPartitionCount;
  private final Gauge<Integer> lowPriorityPartitionCount;

  // Counters for partition priority changes
  private final Counter agingPromotionCount;
  private final Counter replicaCountPromotionCount;
  private final Counter bootstrapDemotionCount;
  private final Counter manualPriorityChangeCount;

  // Histograms for active replica distribution
  private final Histogram partitionActiveReplicaCount;

  // Performance metrics
  private final Map<PartitionPrioritizer.Priority, Timer> replicationCycleTimeByPriority;

  // Circuit breaker metrics
  private final Counter circuitBreakerTripsCount;
  private final Gauge<Boolean> circuitBreakerTripped;
  private final Histogram normalReplicationCycleTime;
  private final Histogram prioritizedReplicationCycleTime;

  // Internal state trackers for gauges
  private final AtomicInteger highPriorityCount = new AtomicInteger(0);
  private final AtomicInteger mediumPriorityCount = new AtomicInteger(0);
  private final AtomicInteger lowPriorityCount = new AtomicInteger(0);
  private volatile boolean isCircuitBreakerTripped = false;

  /**
   * Constructor for PrioritizationMetrics
   * @param registry The metric registry to register metrics with
   */
  public PrioritizationMetrics(MetricRegistry registry) {
    // Register gauges for priority level counts
    highPriorityPartitionCount = registry.register(
        MetricRegistry.name(PartitionPrioritizer.class, "highPriorityPartitionCount"), highPriorityCount::get);

    mediumPriorityPartitionCount = registry.register(
        MetricRegistry.name(PartitionPrioritizer.class, "mediumPriorityPartitionCount"), mediumPriorityCount::get);

    lowPriorityPartitionCount = registry.register(
        MetricRegistry.name(PartitionPrioritizer.class, "lowPriorityPartitionCount"), lowPriorityCount::get);

    // Initialize counters
    agingPromotionCount = registry.counter(
        MetricRegistry.name(PartitionPrioritizer.class, "agingPromotionCount"));

    replicaCountPromotionCount = registry.counter(
        MetricRegistry.name(PartitionPrioritizer.class, "replicaCountPromotionCount"));

    bootstrapDemotionCount = registry.counter(
        MetricRegistry.name(PartitionPrioritizer.class, "bootstrapDemotionCount"));

    manualPriorityChangeCount = registry.counter(
        MetricRegistry.name(PartitionPrioritizer.class, "manualPriorityChangeCount"));

    // Initialize histograms
    partitionActiveReplicaCount = registry.histogram(
        MetricRegistry.name(PartitionPrioritizer.class, "partitionActiveReplicaCount"));

    // Performance metrics by priority
    replicationCycleTimeByPriority = new ConcurrentHashMap<>();
    for (PartitionPrioritizer.Priority priority : PartitionPrioritizer.Priority.values()) {
      replicationCycleTimeByPriority.put(
          priority,
          registry.timer(MetricRegistry.name(PartitionPrioritizer.class,
              "replicationCycleTime", priority.name())));
    }

    // Circuit breaker metrics
    circuitBreakerTripsCount = registry.counter(
        MetricRegistry.name(PartitionPrioritizer.class, "circuitBreakerTripsCount"));

    circuitBreakerTripped = registry.register(
        MetricRegistry.name(PartitionPrioritizer.class, "circuitBreakerTripped"),
        () -> isCircuitBreakerTripped);

    normalReplicationCycleTime = registry.histogram(
        MetricRegistry.name(PartitionPrioritizer.class, "normalReplicationCycleTime"));

    prioritizedReplicationCycleTime = registry.histogram(
        MetricRegistry.name(PartitionPrioritizer.class, "prioritizedReplicationCycleTime"));
  }

  /**
   * Updates the count of partitions at each priority level
   * @param highCount Count of HIGH priority partitions
   * @param mediumCount Count of MEDIUM priority partitions
   * @param lowCount Count of LOW priority partitions
   */
  public void updatePriorityLevelCounts(int highCount, int mediumCount, int lowCount) {
    highPriorityCount.set(highCount);
    mediumPriorityCount.set(mediumCount);
    lowPriorityCount.set(lowCount);
  }

  /**
   * Increment the count of aging-based priority promotions
   * @param count The number to increment by
   */
  public void incrementAgingPromotionCount(int count) {
    agingPromotionCount.inc(count);
  }

  /**
   * Increment the count of replica-count-based priority promotions
   * @param count The number to increment by
   */
  public void incrementReplicaCountPromotionCount(int count) {
    replicaCountPromotionCount.inc(count);
  }

  /**
   * Increment the count of bootstrap demotions
   */
  public void incrementBootstrapDemotionCount() {
    bootstrapDemotionCount.inc();
  }

  /**
   * Increment the count of manual priority changes
   */
  public void incrementManualPriorityChangeCount() {
    manualPriorityChangeCount.inc();
  }

  /**
   * Update the histogram of active replica counts
   * @param activeReplicaCount The active replica count to record
   */
  public void updateActiveReplicaCount(int activeReplicaCount) {
    partitionActiveReplicaCount.update(activeReplicaCount);
  }

  /**
   * Record replication cycle time for a specific priority
   * @param priority The priority level
   * @param durationMs The cycle duration in milliseconds
   */
  public void recordReplicationCycleTime(PartitionPrioritizer.Priority priority, long durationMs) {
    Timer timer = replicationCycleTimeByPriority.get(priority);
    if (timer != null) {
      timer.update(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Update metrics when the circuit breaker trips
   */
  public void onCircuitBreakerTrip() {
    circuitBreakerTripsCount.inc();
    isCircuitBreakerTripped = true;
  }

  /**
   * Update metrics when the circuit breaker resets
   */
  public void onCircuitBreakerReset() {
    isCircuitBreakerTripped = false;
  }

  /**
   * Record cycle time for normal (unprioritized) replication
   * @param durationMs The cycle duration in milliseconds
   */
  public void recordNormalReplicationCycleTime(long durationMs) {
    normalReplicationCycleTime.update(durationMs);
  }

  /**
   * Record cycle time for prioritized replication
   * @param durationMs The cycle duration in milliseconds
   */
  public void recordPrioritizedReplicationCycleTime(long durationMs) {
    prioritizedReplicationCycleTime.update(durationMs);
  }
}