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
package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Metrics for all of the stores on a node.
 */
public class StorageManagerMetrics {
  private final MetricRegistry registry;
  public final Histogram storageManagerStartTimeMs;
  public final Histogram storageManagerShutdownTimeMs;
  public final Histogram diskStartTimeMs;
  public final Histogram diskShutdownTimeMs;
  public final Counter totalStoreStartFailures;
  public final Counter totalStoreShutdownFailures;
  public final Counter diskMountPathFailures;
  public final Counter diskDownCount;
  public final Counter unexpectedDirsOnDisk;
  public final Counter resumeDecommissionErrorCount;

  // DiskSpaceAllocator related metrics
  public final Histogram diskSpaceAllocatorStartTimeMs;
  public final Histogram diskSpaceAllocatorAllocTimeMs;
  public final Histogram diskSpaceAllocatorFreeTimeMs;
  public final Counter diskSpaceAllocatorInitFailureCount;
  public final Counter diskSpaceAllocatorSegmentNotFoundCount;
  public final Counter diskSpaceAllocatorAllocBeforeInitCount;
  public final Counter diskSpaceAllocatorFreeBeforeInitCount;

  // CompactionManager related metrics
  public final Counter compactionManagerTerminateErrorCount;
  public final Counter compactionErrorCount;
  public final Counter compactionExecutorErrorCount;

  private final Counter compactionCount;
  private final AtomicLong compactionsInProgress = new AtomicLong(0);

  /**
   * Create a {@link StorageManagerMetrics} object for handling metrics related to the stores on a node.
   * @param registry the {@link MetricRegistry} to use.
   */
  public StorageManagerMetrics(MetricRegistry registry) {
    this.registry = registry;
    storageManagerStartTimeMs =
        registry.histogram(MetricRegistry.name(StorageManager.class, "StorageManagerStartTimeMs"));
    storageManagerShutdownTimeMs =
        registry.histogram(MetricRegistry.name(StorageManager.class, "StorageManagerShutdownTimeMs"));
    resumeDecommissionErrorCount =
        registry.counter(MetricRegistry.name(StorageManager.class, "ResumeDecommissionErrorCount"));
    diskStartTimeMs = registry.histogram(MetricRegistry.name(DiskManager.class, "DiskStartTimeMs"));
    diskShutdownTimeMs = registry.histogram(MetricRegistry.name(DiskManager.class, "DiskShutdownTimeMs"));
    totalStoreStartFailures = registry.counter(MetricRegistry.name(DiskManager.class, "TotalStoreStartFailures"));
    totalStoreShutdownFailures = registry.counter(MetricRegistry.name(DiskManager.class, "TotalStoreShutdownFailures"));
    diskMountPathFailures = registry.counter(MetricRegistry.name(DiskManager.class, "DiskMountPathFailures"));
    diskDownCount = registry.counter(MetricRegistry.name(DiskManager.class, "DiskDownCount"));
    unexpectedDirsOnDisk = registry.counter(MetricRegistry.name(DiskManager.class, "UnexpectedDirsOnDisk"));

    diskSpaceAllocatorStartTimeMs =
        registry.histogram(MetricRegistry.name(DiskSpaceAllocator.class, "DiskSpaceAllocatorStartTimeMs"));
    diskSpaceAllocatorAllocTimeMs =
        registry.histogram(MetricRegistry.name(DiskSpaceAllocator.class, "DiskSpaceAllocatorAllocTimeMs"));
    diskSpaceAllocatorFreeTimeMs =
        registry.histogram(MetricRegistry.name(DiskSpaceAllocator.class, "DiskSpaceAllocatorFreeTimeMs"));
    diskSpaceAllocatorInitFailureCount =
        registry.counter(MetricRegistry.name(DiskSpaceAllocator.class, "DiskSpaceAllocatorInitFailureCount"));
    diskSpaceAllocatorSegmentNotFoundCount =
        registry.counter(MetricRegistry.name(DiskSpaceAllocator.class, "DiskSpaceAllocatorSegmentNotFoundCount"));
    diskSpaceAllocatorAllocBeforeInitCount =
        registry.counter(MetricRegistry.name(DiskSpaceAllocator.class, "DiskSpaceAllocatorAllocBeforeInitCount"));
    diskSpaceAllocatorFreeBeforeInitCount =
        registry.counter(MetricRegistry.name(DiskSpaceAllocator.class, "DiskSpaceAllocatorFreeBeforeInitCount"));
    compactionCount = registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionCount"));
    compactionManagerTerminateErrorCount =
        registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionManagerTerminateErrorCount"));
    compactionErrorCount = registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionErrorCount"));
    compactionExecutorErrorCount =
        registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionExecutorErrorCount"));

    Gauge<Long> compactionsInProgressGauge = compactionsInProgress::longValue;
    registry.register(MetricRegistry.name(CompactionManager.class, "CompactionsInProgress"),
        compactionsInProgressGauge);
  }

  /**
   * Initializes gauges that track the compaction thread counts.
   * @param storageManager the {@link StorageManager} instance to use to obtain values.
   * @param diskCount the number of disks that the {@link StorageManager} handles.
   */
  void initializeCompactionThreadsTracker(final StorageManager storageManager, final int diskCount) {
    Gauge<Integer> compactionThreadsCountGauge = storageManager::getCompactionThreadCount;
    registry.register(MetricRegistry.name(StorageManager.class, "CompactionThreadsAlive"), compactionThreadsCountGauge);
    Gauge<Integer> compactionHealthGauge = () -> storageManager.getCompactionThreadCount() == diskCount ? 1 : 0;
    registry.register(MetricRegistry.name(StorageManager.class, "CompactionHealth"), compactionHealthGauge);
  }

  /**
   * Deregister the Metrics related to the compaction thread.
   */
  void deregisterCompactionThreadsTracker() {
    registry.remove(MetricRegistry.name(CompactionManager.class, "CompactionsInProgress"));
    registry.remove(MetricRegistry.name(StorageManager.class, "CompactionThreadsAlive"));
    registry.remove(MetricRegistry.name(StorageManager.class, "CompactionHealth"));
  }

  /**
   * Marks the beginning of a compaction.
   * @param incrementUniqueCompactionsCount {@code true} if this is a new compaction and not the resume of a suspended
   *                                                    one. {@code false otherwise}
   */
  void markCompactionStart(boolean incrementUniqueCompactionsCount) {
    if (incrementUniqueCompactionsCount) {
      compactionCount.inc();
    }
    compactionsInProgress.incrementAndGet();
  }

  /**
   * Marks the end/suspension of a compaction.
   */
  void markCompactionStop() {
    compactionsInProgress.decrementAndGet();
  }
}
