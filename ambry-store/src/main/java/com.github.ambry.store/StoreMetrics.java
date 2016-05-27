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
import com.codahale.metrics.Timer;


/**
 * Metrics for the store
 */
public class StoreMetrics {

  public final Timer getResponse;
  public final Timer putResponse;
  public final Timer deleteResponse;
  public final Timer findEntriesSinceResponse;
  public final Timer findMissingKeysResponse;
  public final Timer isKeyDeletedResponse;
  public final Timer storeStartTime;
  public final Counter overflowWriteError;
  public final Counter overflowReadError;
  public final Timer recoveryTime;
  public final Timer findTime;
  public final Timer indexFlushTime;
  public final Timer cleanupTokenFlushTime;
  public final Timer hardDeleteTime;
  public final Counter nonzeroMessageRecovery;
  public final Counter bloomPositiveCount;
  public final Counter bloomFalsePositiveCount;
  public final Counter keySizeMismatchCount;
  public final Counter hardDeleteDoneCount;
  public final Counter hardDeleteFailedCount;
  public final Counter hardDeleteIncompleteRecoveryCount;
  public final Counter hardDeleteExceptionsCount;
  public Gauge<Long> currentCapacityUsed;
  public Gauge<Long> currentHardDeleteProgress;
  public Gauge<Long> hardDeleteThreadRunning;
  public Gauge<Long> hardDeleteCaughtUp;
  public final Histogram segmentSizeForExists;
  public Gauge<Double> percentageUsedCapacity;
  public Gauge<Double> percentageHardDeleteCompleted;
  private final MetricRegistry registry;
  private final String name;

  public StoreMetrics(String storeId, MetricRegistry registry) {
    this.registry = registry;
    name = storeId + ".";
    getResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreGetResponse"));
    putResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StorePutResponse"));
    deleteResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreDeleteResponse"));
    findEntriesSinceResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindEntriesSinceResponse"));
    findMissingKeysResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindMissingKeyResponse"));
    isKeyDeletedResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "IsKeyDeletedResponse"));
    storeStartTime = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreStartTime"));
    overflowWriteError = registry.counter(MetricRegistry.name(Log.class, name + "OverflowWriteError"));
    overflowReadError = registry.counter(MetricRegistry.name(Log.class, name + "OverflowReadError"));
    recoveryTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexRecoveryTime"));
    findTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexFindTime"));
    indexFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexFlushTime"));
    cleanupTokenFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "CleanupTokenFlushTime"));
    hardDeleteTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteTime"));
    nonzeroMessageRecovery =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "NonZeroMessageRecovery"));
    bloomPositiveCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomPositiveCount"));
    bloomFalsePositiveCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomFalsePositiveCount"));
    keySizeMismatchCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "KeySizeMismatchCount"));
    hardDeleteDoneCount = registry.counter(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteDoneCount"));
    hardDeleteFailedCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteFailedCount"));
    hardDeleteIncompleteRecoveryCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteIncompleteRecoveryCount"));
    hardDeleteExceptionsCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteExceptionsCount"));
    segmentSizeForExists = registry.histogram(MetricRegistry.name(IndexSegment.class, name + "SegmentSizeForExists"));
  }

  public void initializeCapacityUsedMetric(final Log log, final long capacityInBytes) {
    currentCapacityUsed = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return log.getLogEndOffset();
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "CurrentCapacityUsed"), currentCapacityUsed);
    percentageUsedCapacity = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return ((double) log.getLogEndOffset() / capacityInBytes) * 100;
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "PercentageUsedCapacity"), percentageUsedCapacity);
  }

  public void initializeHardDeleteMetric(final PersistentIndex index, final Log log) {
    currentHardDeleteProgress = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return index.getHardDeleteProgress();
      }
    };
    registry.register(MetricRegistry.name(PersistentIndex.class, name + "CurrentHardDeleteProgress"),
        currentHardDeleteProgress);

    percentageHardDeleteCompleted = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return ((double) index.getHardDeleteProgress() / log.getLogEndOffset()) * 100;
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "PercentageHardDeleteCompleted"),
        percentageHardDeleteCompleted);

    hardDeleteThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return index.hardDeleteThreadRunning() ? 1L : 0L;
      }
    };
    registry.register(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteThreadRunning"),
        hardDeleteThreadRunning);

    hardDeleteCaughtUp = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return index.hardDeleteCaughtUp() ? 1L : 0L;
      }
    };
    registry.register(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteCaughtUp"), hardDeleteCaughtUp);
  }
}
