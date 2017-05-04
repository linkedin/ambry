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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Metrics for all of the stores on a node.
 */
public class StorageManagerMetrics {
  private final MetricRegistry registry;

  public final Timer storageManagerStartTime;
  public final Timer storageManagerShutdownTime;
  public final Timer diskStartTime;
  public final Timer diskShutdownTime;
  public final Counter totalStoreStartFailures;
  public final Counter totalStoreShutdownFailures;
  public final Counter diskMountPathFailures;

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
    storageManagerStartTime = registry.timer(MetricRegistry.name(StorageManager.class, "StorageManagerStartTime"));
    storageManagerShutdownTime =
        registry.timer(MetricRegistry.name(StorageManager.class, "StorageManagerShutdownTime"));
    diskStartTime = registry.timer(MetricRegistry.name(DiskManager.class, "DiskStartTime"));
    diskShutdownTime = registry.timer(MetricRegistry.name(DiskManager.class, "DiskShutdownTime"));
    totalStoreStartFailures = registry.counter(MetricRegistry.name(DiskManager.class, "TotalStoreStartFailures"));
    totalStoreShutdownFailures = registry.counter(MetricRegistry.name(DiskManager.class, "TotalStoreShutdownFailures"));
    diskMountPathFailures = registry.counter(MetricRegistry.name(DiskManager.class, "DiskMountPathFailures"));

    compactionCount = registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionCount"));
    compactionManagerTerminateErrorCount =
        registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionManagerTerminateErrorCount"));
    compactionErrorCount = registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionErrorCount"));
    compactionExecutorErrorCount =
        registry.counter(MetricRegistry.name(CompactionManager.class, "CompactionExecutorErrorCount"));

    Gauge<Long> compactionsInProgressGauge = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return compactionsInProgress.longValue();
      }
    };
    registry.register(MetricRegistry.name(CompactionManager.class, "CompactionsInProgress"),
        compactionsInProgressGauge);
  }

  /**
   * Initializes gauges that track the compaction thread counts.
   * @param storageManager the {@link StorageManager} instance to use to obtain values.
   * @param diskCount the number of disks that the {@link StorageManager} handles.
   */
  void initializeCompactionThreadsTracker(final StorageManager storageManager, final int diskCount) {
    Gauge<Integer> compactionThreadsCountGauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return storageManager.getCompactionThreadCount();
      }
    };
    registry.register(MetricRegistry.name(StorageManager.class, "CompactionThreadsAlive"), compactionThreadsCountGauge);
    Gauge<Integer> compactionHealthGauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return storageManager.getCompactionThreadCount() == diskCount ? 1 : 0;
      }
    };
    registry.register(MetricRegistry.name(StorageManager.class, "CompactionHealth"), compactionHealthGauge);
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

  /**
   * Create a {@link StoreMetrics} object for handling metrics related to a specific store.
   * @param storeId the name of the store.
   * @return the {@link StoreMetrics}
   */
  public StoreMetrics createStoreMetrics(String storeId) {
    return new StoreMetrics(storeId, registry);
  }
}
