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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Metrics for store operations
 */
public class StoreMetrics {
  private static final String SEPERATOR = ".";

  public final Timer getResponse;
  public final Timer putResponse;
  public final Timer deleteResponse;
  public final Timer findEntriesSinceResponse;
  public final Timer findMissingKeysResponse;
  public final Timer isKeyDeletedResponse;
  public final Timer storeStartTime;
  public final Histogram storeShutdownTimeInMs;
  public final Histogram indexShutdownTimeInMs;
  public final Histogram hardDeleteShutdownTimeInMs;
  public final Counter storeStartFailure;
  public final Counter overflowWriteError;
  public final Counter overflowReadError;
  public final Counter sealSetError;
  public final Counter unsealSetError;
  public final Counter sealDoneCount;
  public final Counter unsealDoneCount;
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
  public final Counter getAuthorizationFailureCount;
  public final Counter deleteAuthorizationFailureCount;
  public final Counter keyInFindEntriesAbsent;

  // Compaction related metrics
  public final Counter compactionFixStateCount;
  public final Meter compactionCopyRateInBytes;
  public final Counter compactionBytesReclaimedCount;

  // BlobStoreStats metrics
  public final Counter blobStoreStatsIndexScannerErrorCount;
  public final Counter blobStoreStatsQueueProcessorErrorCount;
  public final Timer statsOnDemandScanTotalTimeMs;
  public final Timer statsOnDemandScanTimePerIndexSegmentMs;
  public final Timer statsBucketingScanTotalTimeMs;
  public final Timer statsBucketingScanTimePerIndexSegmentMs;
  public final Timer statsRecentEntryQueueProcessTimeMs;
  public final Histogram statsRecentEntryQueueSize;
  public final Histogram statsForwardScanEntryCount;

  private final MetricRegistry registry;

  public StoreMetrics(MetricRegistry registry) {
    this("", registry);
  }

  public StoreMetrics(String prefix, MetricRegistry registry) {
    this.registry = registry;
    String name = !prefix.isEmpty() ? prefix + SEPERATOR : "";
    getResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreGetResponse"));
    putResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StorePutResponse"));
    deleteResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreDeleteResponse"));
    findEntriesSinceResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindEntriesSinceResponse"));
    findMissingKeysResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindMissingKeyResponse"));
    isKeyDeletedResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "IsKeyDeletedResponse"));
    storeStartTime = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreStartTime"));
    storeShutdownTimeInMs = registry.histogram(MetricRegistry.name(BlobStore.class, name + "StoreShutdownTimeInMs"));
    indexShutdownTimeInMs =
        registry.histogram(MetricRegistry.name(PersistentIndex.class, name + "IndexShutdownTimeInMs"));
    hardDeleteShutdownTimeInMs =
        registry.histogram(MetricRegistry.name(HardDeleter.class, name + "HardDeleteShutdownTimeInMs"));
    storeStartFailure = registry.counter(MetricRegistry.name(BlobStore.class, name + "StoreStartFailure"));
    overflowWriteError = registry.counter(MetricRegistry.name(Log.class, name + "OverflowWriteError"));
    overflowReadError = registry.counter(MetricRegistry.name(Log.class, name + "OverflowReadError"));
    sealSetError = registry.counter(MetricRegistry.name(BlobStore.class, name + "SealSetError"));
    unsealSetError = registry.counter(MetricRegistry.name(BlobStore.class, name + "UnsealSetError"));
    sealDoneCount = registry.counter(MetricRegistry.name(BlobStore.class, name + "SealDoneCount"));
    unsealDoneCount = registry.counter(MetricRegistry.name(BlobStore.class, name + "UnsealDoneCount"));
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
    getAuthorizationFailureCount =
        registry.counter(MetricRegistry.name(BlobStore.class, name + "GetAuthorizationFailureCount"));
    deleteAuthorizationFailureCount =
        registry.counter(MetricRegistry.name(BlobStore.class, name + "DeleteAuthorizationFailureCount"));
    keyInFindEntriesAbsent = registry.counter(MetricRegistry.name(BlobStore.class, name + "KeyInFindEntriesAbsent"));
    compactionFixStateCount = registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "FixStateCount"));
    compactionCopyRateInBytes = registry.meter(MetricRegistry.name(BlobStoreCompactor.class, name + "CopyRateInBytes"));
    compactionBytesReclaimedCount =
        registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionBytesReclaimedCount"));
    blobStoreStatsIndexScannerErrorCount =
        registry.counter(MetricRegistry.name(BlobStoreStats.class, name + "BlobStoreStatsIndexScannerErrorCount"));
    blobStoreStatsQueueProcessorErrorCount =
        registry.counter(MetricRegistry.name(BlobStoreStats.class, name + "BlobStoreStatsQueueProcessorErrorCount"));
    statsOnDemandScanTotalTimeMs =
        registry.timer(MetricRegistry.name(BlobStoreStats.class, name + "StatsOnDemandScanTotalTimeMs"));
    statsOnDemandScanTimePerIndexSegmentMs =
        registry.timer(MetricRegistry.name(BlobStoreStats.class, name + "StatsOnDemandScanTimePerIndexSegmentMs"));
    statsBucketingScanTotalTimeMs =
        registry.timer(MetricRegistry.name(BlobStoreStats.class, name + "StatsBucketingScanTotalTimeMs"));
    statsBucketingScanTimePerIndexSegmentMs =
        registry.timer(MetricRegistry.name(BlobStoreStats.class, name + "StatsBucketingScanTimePerIndexSegmentMs"));
    statsRecentEntryQueueProcessTimeMs =
        registry.timer(MetricRegistry.name(BlobStoreStats.class, name + "StatsRecentEntryQueueProcessTimeMs"));
    statsRecentEntryQueueSize =
        registry.histogram(MetricRegistry.name(BlobStoreStats.class, name + "StatsRecentEntryQueueSize"));
    statsForwardScanEntryCount =
        registry.histogram(MetricRegistry.name(BlobStoreStats.class, name + "StatsForwardScanEntryCount"));
  }

  void initializeIndexGauges(String storeId, final PersistentIndex index, final long capacityInBytes) {
    String prefix = storeId + SEPERATOR;
    Gauge<Long> currentCapacityUsed = index::getLogUsedCapacity;
    registry.register(MetricRegistry.name(Log.class, prefix + "CurrentCapacityUsed"), currentCapacityUsed);
    Gauge<Double> percentageUsedCapacity = () -> ((double) index.getLogUsedCapacity() / capacityInBytes) * 100;
    registry.register(MetricRegistry.name(Log.class, prefix + "PercentageUsedCapacity"), percentageUsedCapacity);
    Gauge<Long> currentSegmentCount = index::getLogSegmentCount;
    registry.register(MetricRegistry.name(Log.class, prefix + "CurrentSegmentCount"), currentSegmentCount);
  }

  /**
   * Deregister the IndexGauges for given {@code store}.
   * @param storeId the {@link BlobStore} for which the IndexGauges should be deregistered.
   * @return {@code true} if deregistration was successful. {@code false} if not.
   */
  private boolean deregisterIndexGauges(String storeId) {
    String prefix = storeId + SEPERATOR;
    return registry.remove(MetricRegistry.name(Log.class, prefix + "CurrentCapacityUsed")) && registry.remove(
        MetricRegistry.name(Log.class, prefix + "PercentageUsedCapacity")) && registry.remove(
        MetricRegistry.name(Log.class, prefix + "CurrentSegmentCount"));
  }

  void initializeHardDeleteMetric(String storeId, final HardDeleter hardDeleter, final PersistentIndex index) {
    String prefix = storeId + SEPERATOR;
    Gauge<Long> currentHardDeleteProgress = hardDeleter::getProgress;
    registry.register(MetricRegistry.name(PersistentIndex.class, prefix + "CurrentHardDeleteProgress"),
        currentHardDeleteProgress);

    Gauge<Double> percentageHardDeleteCompleted =
        () -> ((double) hardDeleter.getProgress() / index.getLogUsedCapacity()) * 100;
    registry.register(MetricRegistry.name(Log.class, prefix + "PercentageHardDeleteCompleted"),
        percentageHardDeleteCompleted);

    Gauge<Long> hardDeleteThreadRunning = () -> hardDeleter.isRunning() ? 1L : 0L;
    registry.register(MetricRegistry.name(PersistentIndex.class, prefix + "HardDeleteThreadRunning"),
        hardDeleteThreadRunning);

    Gauge<Long> hardDeleteCaughtUp = () -> hardDeleter.isCaughtUp() ? 1L : 0L;
    registry.register(MetricRegistry.name(PersistentIndex.class, prefix + "HardDeleteCaughtUp"), hardDeleteCaughtUp);
  }

  /**
   * Deregister the HardDeleteMetric for given {@code store}.
   * @param storeId the {@link BlobStore} for which the HardDeleteMetric should be deregistered.
   * @return {@code true} if deregistration was successful. {@code false} if not.
   */
  private boolean deregisterHardDeleteMetric(String storeId) {
    String prefix = storeId + SEPERATOR;
    return registry.remove(MetricRegistry.name(PersistentIndex.class, prefix + "CurrentHardDeleteProgress")) && registry
        .remove(MetricRegistry.name(Log.class, prefix + "PercentageHardDeleteCompleted")) && registry.remove(
        MetricRegistry.name(PersistentIndex.class, prefix + "HardDeleteThreadRunning")) && registry.remove(
        MetricRegistry.name(PersistentIndex.class, prefix + "HardDeleteCaughtUp"));
  }

  void initializeCompactorGauges(String storeId, final AtomicBoolean compactionInProgress) {
    String prefix = storeId + SEPERATOR;
    Gauge<Long> compactionInProgressGauge = () -> compactionInProgress.get() ? 1L : 0L;
    registry.register(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionInProgress"),
        compactionInProgressGauge);
  }

  /**
   * Deregister the CompactorGauges for given {@code store}.
   * @param storeId the {@link BlobStore} for which the CompactorGauges should be deregistered.
   * @return {@code true} if deregistration was successful. {@code false} if not.
   */
  private boolean deregisterCompactorGauges(String storeId) {
    String prefix = storeId + SEPERATOR;
    return registry.remove(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionInProgress"));
  }

  /**
   * Deregister the Metrics related to the given {@code store}.
   * @param storeId the {@link BlobStore} for which some Metrics should be deregistered.
   * @return {@code true} if deregistration was successful. {@code false} if not.
   */
  boolean deregisterMetrics(String storeId) {
    return deregisterIndexGauges(storeId) && deregisterHardDeleteMetric(storeId) && deregisterCompactorGauges(storeId);
  }
}
