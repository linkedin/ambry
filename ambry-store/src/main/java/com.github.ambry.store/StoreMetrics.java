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
 * Metrics for a specific store.
 */
public class StoreMetrics {
  public final Timer getResponse;
  public final Timer putResponse;
  public final Timer deleteResponse;
  public final Timer findEntriesSinceResponse;
  public final Timer findMissingKeysResponse;
  public final Timer isKeyDeletedResponse;
  public final Timer storeStartTime;
  public final Counter storeStartFailure;
  public final Counter overflowWriteError;
  public final Counter overflowReadError;
  public final Timer recoveryTime;
  public final Timer findTime;
  public final Timer indexFlushTime;
  public final Timer cleanupTokenFlushTime;
  public final Timer hardDeleteTime;
  public final Counter putEntryDeletedInfoMismatchCount;
  public final Counter nonzeroMessageRecovery;
  public final Counter blobFoundInActiveSegmentCount;
  public final Counter bloomAccessedCount;
  public final Counter bloomPositiveCount;
  public final Counter bloomFalsePositiveCount;
  public final Counter keySizeMismatchCount;
  public final Counter hardDeleteDoneCount;
  public final Counter hardDeleteFailedCount;
  public final Counter hardDeleteIncompleteRecoveryCount;
  public final Counter hardDeleteExceptionsCount;
  public final Histogram segmentSizeForExists;
  public final Histogram segmentsAccessedPerBlobCount;
  public final Counter identicalPutAttemptCount;

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
    storeStartFailure = registry.counter(MetricRegistry.name(BlobStore.class, name + "StoreStartFailure"));
    overflowWriteError = registry.counter(MetricRegistry.name(Log.class, name + "OverflowWriteError"));
    overflowReadError = registry.counter(MetricRegistry.name(Log.class, name + "OverflowReadError"));
    recoveryTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexRecoveryTime"));
    findTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexFindTime"));
    indexFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexFlushTime"));
    cleanupTokenFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "CleanupTokenFlushTime"));
    hardDeleteTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteTime"));
    putEntryDeletedInfoMismatchCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "PutEntryDeletedInfoMismatchCount"));
    nonzeroMessageRecovery =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "NonZeroMessageRecovery"));
    blobFoundInActiveSegmentCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "BlobFoundInActiveSegmentCount"));
    bloomAccessedCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomAccessedCount"));
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
    segmentsAccessedPerBlobCount =
        registry.histogram(MetricRegistry.name(IndexSegment.class, name + "SegmentsAccessedPerBlobCount"));
    identicalPutAttemptCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "IdenticalPutAttemptCount"));
  }

  void initializeIndexGauges(final PersistentIndex index, final long capacityInBytes) {
    Gauge<Long> currentCapacityUsed = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return index.getLogUsedCapacity();
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "CurrentCapacityUsed"), currentCapacityUsed);
    Gauge<Double> percentageUsedCapacity = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return ((double) index.getLogUsedCapacity() / capacityInBytes) * 100;
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "PercentageUsedCapacity"), percentageUsedCapacity);
    Gauge<Long> currentSegmentCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return index.getLogSegmentCount();
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "CurrentSegmentCount"), currentSegmentCount);
  }

  void initializeHardDeleteMetric(final HardDeleter hardDeleter, final PersistentIndex index) {
    Gauge<Long> currentHardDeleteProgress = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return hardDeleter.getProgress();
      }
    };
    registry.register(MetricRegistry.name(PersistentIndex.class, name + "CurrentHardDeleteProgress"),
        currentHardDeleteProgress);

    Gauge<Double> percentageHardDeleteCompleted = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return ((double) hardDeleter.getProgress() / index.getLogUsedCapacity()) * 100;
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "PercentageHardDeleteCompleted"),
        percentageHardDeleteCompleted);

    Gauge<Long> hardDeleteThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return hardDeleter.isRunning() ? 1L : 0L;
      }
    };
    registry.register(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteThreadRunning"),
        hardDeleteThreadRunning);

    Gauge<Long> hardDeleteCaughtUp = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return hardDeleter.isCaughtUp() ? 1L : 0L;
      }
    };
    registry.register(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteCaughtUp"), hardDeleteCaughtUp);
  }
}
