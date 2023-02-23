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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Metrics for store operations
 */
public class StoreMetrics {
  private static final String SEPARATOR = ".";
  private static final Logger logger = LoggerFactory.getLogger(StoreMetrics.class);
  public final Timer getResponse;
  public final Timer putResponse;
  public final Timer deleteResponse;
  public final Timer ttlUpdateResponse;
  public final Timer undeleteResponse;
  public final Timer findEntriesSinceResponse;
  public final Timer findMissingKeysResponse;
  public final Timer findKeyResponse;
  public final Timer findAllMessageInfosResponse;
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
  public final Counter partialSealSetError;
  public final Counter sealDoneCount;
  public final Counter unsealDoneCount;
  public final Counter partialSealDoneCount;
  public final Counter indexBasedTokenResetCount;
  public final Counter journalBasedTokenResetCount;
  public final Counter resetKeyFoundInCurrentIndex;
  public final Counter generateTokenBasedOnCompactionHistoryCount;
  public final Counter offsetFoundInBeforeAfterMapCount;
  public final Counter beforeAndAfterOffsetSanityCheckFailureCount;
  public final Timer recoveryTime;
  public final Timer findTime;
  public final Timer indexFlushTime;
  public final Timer cleanupTokenFlushTime;
  public final Timer hardDeleteTime;
  public final Counter nonzeroMessageRecovery;
  public final Counter blobFoundInMemSegmentCount;
  public final Counter bloomAccessedCount;
  public final Counter bloomPositiveCount;
  public final Counter bloomFalsePositiveCount;
  public final Counter bloomRebuildOnLoadFailureCount;
  public final Counter bloomPersistFailureCount;
  public final Counter mappedSegmentIsLoadedDuringFindCount;
  public final Counter mappedSegmentIsNotLoadedDuringFindCount;
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
  public final Counter ttlUpdateAuthorizationFailureCount;
  public final Counter undeleteAuthorizationFailureCount;
  public final Counter keyInFindEntriesAbsent;
  public final Counter duplicateKeysInBatch;
  public final Counter storeIoErrorTriggeredShutdownCount;
  public final Counter blobStoreRecoverCompactionPolicySwitchInfoErrorCount;

  // Compaction related metrics
  public final Counter compactionFixStateCount;
  public final Meter compactionCopyRateInBytes;
  public final Counter compactionBytesReclaimedCount;
  public final Counter compactionBundleReadBufferNotFitIn;
  public final Counter compactionBundleReadBufferUsed;
  public final Counter compactionBundleReadBufferIoCount;
  public final Counter compactionTargetIndexDuplicateOnNonRecoveryCount;
  public final Counter permanentDeleteTombstonePurgeCount;
  public final Timer compactionCopyRecordTimeInMs;
  public final Timer compactionCopyDataByIndexSegmentTimeInMs;
  public final Timer compactionCopyDataByLogSegmentTimeInMs;

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

  private final ConcurrentHashMap<String, PersistentIndex> indexes = new ConcurrentHashMap<>();

  private final MetricRegistry registry;

  public StoreMetrics(MetricRegistry registry) {
    this("", registry);
  }

  public MetricRegistry getRegistry() {
    return registry;
  }

  public StoreMetrics(String prefix, MetricRegistry registry) {
    this.registry = registry;
    String name = !prefix.isEmpty() ? prefix + SEPARATOR : "";
    getResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreGetResponse"));
    putResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StorePutResponse"));
    deleteResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreDeleteResponse"));
    ttlUpdateResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreTtlUpdateResponse"));
    undeleteResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreUndeleteResponse"));
    findEntriesSinceResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindEntriesSinceResponse"));
    findMissingKeysResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindMissingKeyResponse"));
    findKeyResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindKeyResponse"));
    findAllMessageInfosResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "StoreFindAllMessageInfosResponse"));
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
    partialSealSetError = registry.counter(MetricRegistry.name(BlobStore.class, name + "PartialSealSetError"));
    sealDoneCount = registry.counter(MetricRegistry.name(BlobStore.class, name + "SealDoneCount"));
    unsealDoneCount = registry.counter(MetricRegistry.name(BlobStore.class, name + "UnsealDoneCount"));
    partialSealDoneCount = registry.counter(MetricRegistry.name(BlobStore.class, name + "PartialSealDoneCount"));
    indexBasedTokenResetCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "IndexBasedTokenResetCount"));
    journalBasedTokenResetCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "JournalBasedTokenResetCount"));
    resetKeyFoundInCurrentIndex =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "ResetKeyFoundInCurrentIndex"));
    generateTokenBasedOnCompactionHistoryCount = registry.counter(
        MetricRegistry.name(PersistentIndex.class, name + "GenerateTokenBasedOnCompactionHistoryCount"));
    offsetFoundInBeforeAfterMapCount =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "OffsetFoundInBeforeAfterMapCount"));
    beforeAndAfterOffsetSanityCheckFailureCount = registry.counter(
        MetricRegistry.name(PersistentIndex.class, name + "BeforeAndAfterOffsetSanityCheckFailureCount"));
    recoveryTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexRecoveryTime"));
    findTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexFindTime"));
    indexFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "IndexFlushTime"));
    cleanupTokenFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "CleanupTokenFlushTime"));
    hardDeleteTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "HardDeleteTime"));
    nonzeroMessageRecovery =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "NonZeroMessageRecovery"));
    blobFoundInMemSegmentCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "BlobFoundInMemSegmentCount"));
    bloomAccessedCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomAccessedCount"));
    bloomPositiveCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomPositiveCount"));
    bloomFalsePositiveCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomFalsePositiveCount"));
    bloomRebuildOnLoadFailureCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomRebuildOnLoadFailureCount"));
    bloomPersistFailureCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "BloomPersistFailureCount"));
    mappedSegmentIsLoadedDuringFindCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "MappedSegmentIsLoadedDuringFindCount"));
    mappedSegmentIsNotLoadedDuringFindCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "MappedSegmentIsNotLoadedDuringFindCount"));
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
    ttlUpdateAuthorizationFailureCount =
        registry.counter(MetricRegistry.name(BlobStore.class, name + "TtlUpdateAuthorizationFailureCount"));
    undeleteAuthorizationFailureCount =
        registry.counter(MetricRegistry.name(BlobStore.class, name + "UndeleteAuthorizationFailureCount"));
    keyInFindEntriesAbsent = registry.counter(MetricRegistry.name(BlobStore.class, name + "KeyInFindEntriesAbsent"));
    duplicateKeysInBatch = registry.counter(MetricRegistry.name(BlobStore.class, name + "DuplicateKeysInBatch"));
    storeIoErrorTriggeredShutdownCount =
        registry.counter(MetricRegistry.name(BlobStore.class, name + "StoreIoErrorTriggeredShutdownCount"));
    compactionFixStateCount = registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "FixStateCount"));
    compactionCopyRateInBytes = registry.meter(MetricRegistry.name(BlobStoreCompactor.class, name + "CopyRateInBytes"));
    compactionBytesReclaimedCount =
        registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionBytesReclaimedCount"));
    compactionBundleReadBufferNotFitIn =
        registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionBundleReadBufferNotFitIn"));
    compactionBundleReadBufferUsed =
        registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionBundleReadBufferUsed"));
    compactionBundleReadBufferIoCount =
        registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionBundleReadBufferIoCount"));
    compactionTargetIndexDuplicateOnNonRecoveryCount = registry.counter(
        MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionTargetIndexDuplicateOnNonRecoveryCount"));
    permanentDeleteTombstonePurgeCount =
        registry.counter(MetricRegistry.name(BlobStoreCompactor.class, name + "PermanentDeleteTombstonePurgeCount"));
    compactionCopyRecordTimeInMs =
        registry.timer(MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionCopyRecordTimeInMs"));
    compactionCopyDataByIndexSegmentTimeInMs = registry.timer(
        MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionCopyDataByIndexSegmentTimeInMs"));
    compactionCopyDataByLogSegmentTimeInMs =
        registry.timer(MetricRegistry.name(BlobStoreCompactor.class, name + "CompactionCopyDataByLogSegmentTimeInMs"));
    blobStoreStatsIndexScannerErrorCount =
        registry.counter(MetricRegistry.name(BlobStoreStats.class, name + "BlobStoreStatsIndexScannerErrorCount"));
    blobStoreRecoverCompactionPolicySwitchInfoErrorCount = registry.counter(
        MetricRegistry.name(BlobStoreStats.class, name + "BlobStoreRecoverCompactionPolicySwitchInfoErrorCount"));
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
    Gauge<Integer> byteBufferForAppendTotalCountGauge = LogSegment.byteBufferForAppendTotalCount::get;
    registry.register(MetricRegistry.name(Log.class, name + "ByteBufferForAppendTotalCount"),
        byteBufferForAppendTotalCountGauge);
  }

  void initializeIndexGauges(String storeId, final PersistentIndex index, final long capacityInBytes,
      BlobStoreStats blobStoreStats, boolean enableStoreCurrentInvalidSize,
      boolean enableStoreIndexDirectMemoryUsageMetric) {
    String prefix = storeId + SEPARATOR;
    Gauge<Long> currentCapacityUsed = index::getLogUsedCapacity;
    registry.register(MetricRegistry.name(Log.class, prefix + "CurrentCapacityUsed"), currentCapacityUsed);
    Gauge<Double> percentageUsedCapacity = () -> ((double) index.getLogUsedCapacity() / capacityInBytes) * 100;
    registry.register(MetricRegistry.name(Log.class, prefix + "PercentageUsedCapacity"), percentageUsedCapacity);
    Gauge<Long> currentSegmentCount = index::getLogSegmentCount;
    registry.register(MetricRegistry.name(Log.class, prefix + "CurrentSegmentCount"), currentSegmentCount);
    if (enableStoreCurrentInvalidSize) {
      Gauge<Long> currentInvalidDataSize =
          () -> index.getLogUsedCapacity() - blobStoreStats.getCachedValidSize().getSecond();
      registry.register(MetricRegistry.name(Log.class, prefix + "CurrentInvalidDataSize"), currentInvalidDataSize);
    }
    if (enableStoreIndexDirectMemoryUsageMetric) {
      indexes.putIfAbsent(storeId, index);
      Gauge<Long> indexDirectMemoryUsage =
          () -> indexes.values().stream().mapToLong(PersistentIndex::getDirectMemoryUsage).sum();
      registry.gauge(MetricRegistry.name(BlobStore.class, "IndexDirectMemoryUsage"), () -> indexDirectMemoryUsage);
    }
  }

  /**
   * Deregister the IndexGauges for given {@code store}.
   * @param storeId the {@link BlobStore} for which the IndexGauges should be deregistered.
   */
  private void deregisterIndexGauges(String storeId) {
    String prefix = storeId + SEPARATOR;
    registry.remove(MetricRegistry.name(Log.class, prefix + "CurrentCapacityUsed"));
    registry.remove(MetricRegistry.name(Log.class, prefix + "PercentageUsedCapacity"));
    registry.remove(MetricRegistry.name(Log.class, prefix + "CurrentSegmentCount"));
    registry.remove(MetricRegistry.name(Log.class, prefix + "CurrentInvalidDataSize"));
    registry.remove(MetricRegistry.name(Log.class, "ByteBufferForAppendTotalCount"));
    registry.remove(MetricRegistry.name(Log.class, "UnderCompaction" + SEPARATOR + "ByteBufferForAppendTotalCount"));
  }

  void initializeHardDeleteMetric(String storeId, final HardDeleter hardDeleter, final PersistentIndex index) {
    String prefix = storeId + SEPARATOR;
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
   */
  private void deregisterHardDeleteMetric(String storeId) {
    String prefix = storeId + SEPARATOR;
    registry.remove(MetricRegistry.name(PersistentIndex.class, prefix + "CurrentHardDeleteProgress"));
    registry.remove(MetricRegistry.name(Log.class, prefix + "PercentageHardDeleteCompleted"));
    registry.remove(MetricRegistry.name(PersistentIndex.class, prefix + "HardDeleteThreadRunning"));
    registry.remove(MetricRegistry.name(PersistentIndex.class, prefix + "HardDeleteCaughtUp"));
  }

  void initializeCompactorGauges(String storeId, final AtomicBoolean compactionInProgress,
      AtomicReference<CompactionDetails> compactionDetailsAtomicReference, AtomicInteger compactedLogCount,
      AtomicInteger logSegmentCount) {
    String prefix = storeId + SEPARATOR;
    Gauge<Long> compactionInProgressGauge = () -> compactionInProgress.get() ? 1L : 0L;
    registry.register(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionInProgress"),
        compactionInProgressGauge);
    Gauge<Long> compactionCost = () -> {
      CompactionDetails compactionDetails = compactionDetailsAtomicReference.get();
      if (compactionDetails == null || compactionDetails.getCostBenefitInfo() == null) {
        return -1L;
      } else {
        return compactionDetails.getCostBenefitInfo().getCost();
      }
    };
    registry.register(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionCost"), compactionCost);
    Gauge<Integer> compactionBenefit = () -> {
      CompactionDetails compactionDetails = compactionDetailsAtomicReference.get();
      if (compactionDetails == null || compactionDetails.getCostBenefitInfo() == null) {
        return -1;
      } else {
        return compactionDetails.getCostBenefitInfo().getBenefit();
      }
    };
    registry.register(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionBenefit"), compactionBenefit);
    Gauge<Integer> compactedLogCountGauge = compactedLogCount::get;
    registry.gauge(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactedLogCount"),
        () -> compactedLogCountGauge);
    Gauge<Integer> logSegmentCountGauge = logSegmentCount::get;
    registry.gauge(MetricRegistry.name(BlobStoreCompactor.class, prefix + "LogSegmentCount"),
        () -> logSegmentCountGauge);
  }

  /**
   * Deregister the CompactorGauges for given {@code store}.
   * @param storeId the {@link BlobStore} for which the CompactorGauges should be deregistered.
   */
  private void deregisterCompactorGauges(String storeId) {
    String prefix = storeId + SEPARATOR;
    registry.remove(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionInProgress"));
    registry.remove(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionCost"));
    registry.remove(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionBenefit"));
  }

  /**
   * Deregister the Metrics related to the given {@code store}.
   * @param storeId the {@link BlobStore} for which some Metrics should be deregistered.
   */
  void deregisterMetrics(String storeId) {
    deregisterIndexGauges(storeId);
    deregisterHardDeleteMetric(storeId);
    deregisterCompactorGauges(storeId);
  }
}
