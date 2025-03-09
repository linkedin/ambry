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
package com.github.ambry.config;

import com.github.ambry.store.IndexMemState;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The configs for the store
 */

public class StoreConfig {
  private static final Logger logger = LoggerFactory.getLogger(StoreConfig.class);

  /**
   * The factory class the store uses to creates its keys
   */
  @Config("store.key.factory")
  @Default("com.github.ambry.commons.BlobIdFactory")
  public final String storeKeyFactory;

  /**
   * The frequency at which the data gets flushed to disk
   */
  @Config("store.data.flush.interval.seconds")
  @Default("60")
  public final long storeDataFlushIntervalSeconds;

  /**
   * The max size of the index that can reside in memory in bytes for a single store
   */
  @Config("store.index.max.memory.size.bytes")
  @Default("20971520")
  public final int storeIndexMaxMemorySizeBytes;

  /**
   * The delay after which the data flush thread starts on startup
   */
  @Config("store.data.flush.delay.seconds")
  @Default("5")
  public final int storeDataFlushDelaySeconds;

  /**
   * The max number of the elements in the index that can be in memory for a single store
   */
  @Config("store.index.max.number.of.inmem.elements")
  @Default("10000")
  public final int storeIndexMaxNumberOfInmemElements;

  /**
   * The max number of entries that the journal will return each time it is queried for entries
   */
  @Config("store.max.number.of.entries.to.return.from.journal")
  @Default("5000")
  public final int storeMaxNumberOfEntriesToReturnFromJournal;

  /**
   * The max probability of a false positive for the index bloom filter
   */
  @Config("store.index.bloom.max.false.positive.probability")
  @Default("0.01")
  public final double storeIndexBloomMaxFalsePositiveProbability;

  /**
   * How long (in hours) a key must be in deleted state before it is hard deleted. Minimum value: 1 hour.
   */
  @Config("store.deleted.message.retention.hours")
  @Default("168")
  public final int storeDeletedMessageRetentionHours;

  /**
   * How long (in minutes) a key must be in deleted state before it is hard deleted.
   */
  @Config("store.deleted.message.retention.minutes")
  @Default("-1")
  public final int storeDeletedMessageRetentionMinutes;

  /**
   * How often the HybridCompactionPolicy switch from StatsBasedCompactionPolicy to CompactAllPolicy based on timestamp.
   */
  @Config("store.compaction.policy.switch.timestamp.days")
  @Default("6")
  public final int storeCompactionPolicySwitchTimestampDays;

  /**
   * The maximum stagger delay (in hours) for full compaction of a store after it is eligible for compaction.
   * In other words, when a store is eligible for full compaction, the start of full compaction will be delayed by a max
   * of this value.
   * This config is needed to ensure that all the stores on a disk do not start full compaction at the same time.
   */
  @Config("store.hybrid.compaction.full.compaction.stagger.limit.in.hours")
  @Default("0")
  public final int storeHybridCompactionFullCompactionStaggerLimitInHours;

  /**
   * True to filter out all leading and trailing log segments whose data is 100% valid in compact all policy.
   */
  @Config(storeCompactAllPolicyFilterOutAllValidSegmentName)
  @Default("false")
  public final boolean storeCompactAllPolicyFilterOutAllValidSegment;
  public static final String storeCompactAllPolicyFilterOutAllValidSegmentName =
      "store.compact.all.policy.filter.out.all.valid.segment";

  /**
   * How long (in days) a container must be in DELETE_IN_PROGRESS state before it's been deleted during compaction.
   */
  @Config("store.container.deletion.retention.days")
  @Default("14")
  public final int storeContainerDeletionRetentionDays;

  /**
   * The rate of I/O allowed per disk for hard deletes.
   */
  @Config("store.hard.delete.operations.bytes.per.sec")
  @Default("100*1024")
  public final int storeHardDeleteOperationsBytesPerSec;

  /**
   * The max rate of I/O allowed per disk for compaction.
   */
  @Config("store.compaction.operations.bytes.per.sec")
  @Default("1*1024*1024")
  public final int storeCompactionOperationsBytesPerSec;

  /**
   * The minimum  I/O rate allowed per disk for compaction.
   */
  @Config("store.compaction.min.operations.bytes.per.sec")
  @Default("512*1024")
  public final int storeCompactionMinOperationsBytesPerSec;

  /**
   * The adjustment coefficient to use when compaction calculate desired copy rate.
   */
  @Config("store.compaction.operations.adjust.k")
  @Default("1")
  public final double storeCompactionOperationsAdjustK;

  /**
   * The check interval used in compaction rate throttler. -1 means we check rate in every call.
   */
  @Config("store.compaction.throttler.check.interval.ms")
  @Default("-1")
  public final int storeCompactionThrottlerCheckIntervalMs;

  /**
   * Whether direct IO are to be enable or not for compaction.
   * This is only supported on > Linux 2.6
   */
  @Config("store.compaction.enable.direct.io")
  @Default("false")
  public final boolean storeCompactionEnableDirectIO;

  /**
   * The buffer size when using direct io in compaction to write to the target log segments.
   * 0 means no buffer for direct io. And buffer size has to be a multiple of file system blob size.
   */
  @Config(storeCompactionDirectIOBufferSizeName)
  @Default("0")
  public final int storeCompactionDirectIOBufferSize;
  public static final String storeCompactionDirectIOBufferSizeName = "store.compaction.direct.io.buffer.size";

  /**
   * Whether to purge expired delete tombstone in compaction.
   */
  @Config("store.compaction.purge.delete.tombstone")
  @Default("false")
  public final boolean storeCompactionPurgeDeleteTombstone;

  /**
   * When a peer went offline for more than this amount of days, we would ignore this peer's remote token when checking
   * if a delete tombstone is valid or not. If the value for this configuration is 0, then this feature is disabled.
   */
  @Config(storeCompactionIgnorePeersUnavailableForDaysName)
  @Default("0")
  public final int storeCompactionIgnorePeersUnavailableForDays;
  public final static String storeCompactionIgnorePeersUnavailableForDaysName =
      "store.compaction.ignore.peers.unavailable.for.days";

  /**
   * The minimum buffer size for compaction copy phase.
   */
  @Config("store.compaction.min.buffer.size")
  @Default("10*1024*1024")
  public final int storeCompactionMinBufferSize;

  /**
   *The IndexSegmentValidEntryFilter type to use for compaction.
   */
  @Config("store.compaction.filter")
  @Default("IndexSegmentValidEntryFilterWithoutUndelete")
  public final String storeCompactionFilter;

  /**
   * Whether hard deletes are to be enabled or not
   */
  @Config("store.enable.hard.delete")
  @Default("false")
  public final boolean storeEnableHardDelete;

  /**
   * The size of a single segment in the log. Only relevant for first startup of a {@link com.github.ambry.store.Store}.
   */
  @Config("store.segment.size.in.bytes")
  @Default("9223372036854775807")
  public final long storeSegmentSizeInBytes;

  /**
   * Comma separated list of the compaction triggers that should be enabled. If this config is an empty string,
   * compaction will not be enabled.
   * The valid triggers are: Periodic,Admin
   */
  @Config("store.compaction.triggers")
  @Default("")
  public final String[] storeCompactionTriggers;

  /**
   * The frequency (in hours) at which a store is checked to see whether it is ready for compaction.
   */
  @Config("store.compaction.check.frequency.in.hours")
  @Default("7*24")
  public final int storeCompactionCheckFrequencyInHours;

  /**
   * The minimum capacity that has to be used (as a percentage of the total capacity) for the store to trigger
   * compaction
   */
  @Config("store.min.used.capacity.to.trigger.compaction.in.percentage")
  @Default("50")
  public final int storeMinUsedCapacityToTriggerCompactionInPercentage;

  /**
   * The factory class used to get the compaction policy
   */
  @Config("store.compaction.policy.factory")
  @Default("com.github.ambry.store.CompactAllPolicyFactory")
  public final String storeCompactionPolicyFactory;

  /**
   * The minimum number of log segments to be reclaimed to trigger compaction.
   * It is up to the compaction policy implementation to honor this config if need be.
   */
  @Config("store.min.log.segment.count.to.reclaim.to.trigger.compaction")
  @Default("1")
  public final int storeMinLogSegmentCountToReclaimToTriggerCompaction;

  /**
   * Only the log segment whose valid data percentage is less or equal than the specified number
   * will be qualified for compaction.
   */
  @Config("store.max.log.segment.valid.data.percentage.to.qualify.compaction")
  public final double storeMaxLogSegmentValidDataPercentageToQualifyCompaction;

  /**
   * In the stats based compaction, set the min cost for each log segment.
   * If the IO effort is less than it, still count the cost as this min cost.
   */
  @Config("store.stats.based.compaction.min.cost.in.percentage")
  public final double storeStatsBasedCompactionMinCostInPercentage;

  /**
   * the time interval to run the "middle range compaction" in the stats based compaction.
   * the interval is in milliseconds.
   */
  @Config("store.stats.based.middle.range.compaction.interval.in.ms")
  public final long storeStatsBasedMiddleRangeCompactionIntervalInMs;

  /**
   * if true, stats based compaction weights more on benefit instead of IO cost.
   */
  @Config("store.stats.based.weight.on.benefit.enabled")
  public final boolean storeStatsBasedWeightOnBenefitEnabled;

  /**
   * The number of buckets for stats bucketing, a value of 0 will disable bucketing.
   */
  @Config("store.stats.bucket.count")
  @Default("0")
  public final int storeStatsBucketCount;

  /**
   * The time span of each bucket in minutes.
   */
  @Config("store.stats.bucket.span.in.minutes")
  @Default("60")
  public final long storeStatsBucketSpanInMinutes;

  /**
   * Period in minutes to specify how frequent is the queue processor executed.
   */
  @Config("store.stats.recent.entry.processing.interval.in.minutes")
  @Default("2")
  public final long storeStatsRecentEntryProcessingIntervalInMinutes;

  /**
   * Period in seconds to specify how frequent the validDataSizeCollector executed.
   */
  @Config("store.get.valid.size.interval.in.secs")
  @Default("2*60")
  public final long storeGetValidSizeIntervalInSecs;

  /**
   * The upper limit in seconds for requests to wait for a ongoing construction of buckets (that contains the answer)
   * to complete.
   */
  @Config("store.stats.wait.timeout.in.secs")
  @Default("2*60")
  public final long storeStatsWaitTimeoutInSecs;

  /**
   * Specifies the number of index entries that can be read per second for stats collection.
   */
  @Config("store.stats.index.entries.per.second")
  @Default("240000")
  public final int storeStatsIndexEntriesPerSecond;

  /**
   * Specifies the minimum size that index entries should occupy when they get persisted. If the number of bytes for
   * constituting keys and values fall short of this size, the entries will be padded with dummy bytes to amount to this
   * number.
   * Setting this value to N bytes ensures that even if the size of keys put to the store changes at runtime, as long as
   * the total entry size is still N bytes, the key size change will not cause the active index segment to roll over.
   */
  @Config("store.index.persisted.entry.min.bytes")
  @Default("115")
  public final int storeIndexPersistedEntryMinBytes;

  /**
   * The minimum bytes to determine in current log segment only contains the header info.
   */
  @Config("store.determine.log.segment.only.contains.header.min.bytes")
  @Default("20")
  public final int storeDetermineLogSegmentOnlyContainsHeaderMinBytes;

  /**
   * Whether to rebuild replication token (if it's been invalidated) based on reset key. If {@code false}, the token
   * will be reset to the very beginning of whole log.
   */
  @Config("store.rebuild.token.based.on.reset.key")
  @Default("false")
  public final boolean storeRebuildTokenBasedOnResetKey;

  /**
   * Enables or disables accountId and containerId validation for GET/DELETE request.
   */
  @Config("store.validate.authorization")
  @Default("false")
  public final boolean storeValidateAuthorization;

  /**
   * Enables or disables ReplicaStatusDelegate to dynamically set the replica sealed/stopped status
   */
  @Config(storeReplicaStatusDelegateEnableName)
  @Default("false")
  public final boolean storeReplicaStatusDelegateEnable;
  public static final String storeReplicaStatusDelegateEnableName = "store.replica.status.delegate.enable";

  /**
   * Specifies the size threshold (as percentage of maximum size) of a store for converting the store to RO from RW
   */
  @Config(storeReadOnlyEnableSizeThresholdPercentageName)
  @Default("95")
  public final int storeReadOnlyEnableSizeThresholdPercentage;
  public static final String storeReadOnlyEnableSizeThresholdPercentageName =
      "store.read.only.enable.size.threshold.percentage";

  /**
   * Specifies the size threshold delta below {@link #storeReadOnlyEnableSizeThresholdPercentageName} that a store will
   * be converted from RO to Partially Writable.
   */
  @Config(storeReadOnlyToPartialWriteEnableSizeThresholdPercentageDeltaName)
  @Default("5")
  public final int storeReadOnlyToPartialWriteEnableSizeThresholdPercentageDelta;
  public static final String storeReadOnlyToPartialWriteEnableSizeThresholdPercentageDeltaName =
      "store.read.only.to.partial.write.enable.size.threshold.percentage.delta";

  /**
   * Specifies the size threshold (as percentage of maximum size) of a store for converting the replica and partition to
   * partially writable from RW.
   */
  @Config(storePartialWriteEnableSizeThresholdPercentageName)
  @Default("95")
  public final int storePartialWriteEnableSizeThresholdPercentage;
  public static final String storePartialWriteEnableSizeThresholdPercentageName =
      "store.partial.write.enable.size.threshold.percentage";

  /**
   * Specifies the size threshold delta below {@link #storePartialWriteEnableSizeThresholdPercentageName} that a store will
   * be converted from partially writable to RW
   */
  @Config(storePartialWriteToReadWriteEnableSizeThresholdPercentageDeltaName)
  @Default("5")
  public final int storePartialWriteToReadWriteEnableSizeThresholdPercentageDelta;
  public static final String storePartialWriteToReadWriteEnableSizeThresholdPercentageDeltaName =
      "store.partial.write.to.read.write.enable.size.threshold.percentage.delta";

  /**
   * Specifies the minimum number of seconds before a blob's current expiry time (creation time + TTL) that the current
   * time has to be in order for a TTL update operation on the blob to succeed.
   */
  @Config(storeTtlUpdateBufferTimeSecondsName)
  @Default("60 * 60 * 24")
  public final int storeTtlUpdateBufferTimeSeconds;
  public static final String storeTtlUpdateBufferTimeSecondsName = "store.ttl.update.buffer.time.seconds";

  /**
   * Provides a hint for how indexes should be treated w.r.t memory
   */
  @Config(storeIndexMemStateName)
  @Default("MMAP_WITHOUT_FORCE_LOAD")
  public final IndexMemState storeIndexMemState;
  public static final String storeIndexMemStateName = "store.index.mem.state";

  /**
   * Specifies the threshold I/O error count of store to trigger shutdown operation on the store.
   */
  @Config("store.io.error.count.to.trigger.shutdown")
  @Default("Integer.MAX_VALUE")
  public final int storeIoErrorCountToTriggerShutdown;

  /**
   * Whether to set certain permissions for files in store.
   */
  @Config("store.set.file.permission.enabled")
  @Default("false")
  public final boolean storeSetFilePermissionEnabled;

  /**
   * Whether to enable auto close last log segment during compaction.
   */
  @Config(storeAutoCloseLastLogSegmentEnabledName)
  @Default("false")
  public final boolean storeAutoCloseLastLogSegmentEnabled;
  public static final String storeAutoCloseLastLogSegmentEnabledName = "store.auto.close.last.log.segment.enabled";

  /**
   * Specifies the minimum value of Bytes for maxLagForPartition(max lag refers to local replica is lagging behind remote peers)
   * which can unseal replica.
   */
  @Config(storeUnsealReplicaMinimumLagBytesName)
  @Default("0")
  public final long storeUnsealReplicaMinimumLagBytes;
  public static final String storeUnsealReplicaMinimumLagBytesName = "store.unseal.replica.minimum.lag.bytes";

  /**
   * Specifies the permissions for data files in store. (Data files are user data related files for example, log segment,
   * index segment and bloom filter etc)
   */
  @Config("store.data.file.permission")
  @Default("rw-rw----")
  public final Set<PosixFilePermission> storeDataFilePermission;

  /**
   * Specifies the permissions for operation files in store. (Operation files are usually generated by ambry to keep track
   * of store state like compaction log, clean shutdown file, etc)
   */
  @Config("store.operation.file.permission")
  @Default("rw-rw-r--")
  public final Set<PosixFilePermission> storeOperationFilePermission;

  /**
   * Whether to populate bloom filter with UUID only for index segment.
   */
  @Config("store.uuid.based.bloom.filter.enabled")
  @Default("false")
  public final boolean storeUuidBasedBloomFilterEnabled;

  /**
   * Whether to rebuild index bloom filter during startup. If true, store will cleanup existing bloom files and rebuild
   * them based on index segments when server restarts.
   */
  @Config("store.index.rebuild.bloom.filter.enabled")
  @Default("false")
  public final boolean storeIndexRebuildBloomFilterEnabled;

  /**
   * Maximum page count to invalidate some corrupted bloom files that may compute super large value for number of pages
   * and cause OutOfMemory issue. If computed page count is larger this value, an exception will be thrown to either
   * rebuild bloom file or terminate store startup.
   */
  @Config("store.bloom.filter.maximum.page.count")
  @Default("128")
  public final int storeBloomFilterMaximumPageCount;

  /**
   * True to enable container deletion in store.
   */
  @Config("store.container.deletion.enabled")
  @Default("false")
  public final boolean storeContainerDeletionEnabled;

  /**
   * Whether to set local partition state through InstanceConfig in Helix. If true, store is allowed to enable/disable
   * partition on local node by calling InstanceConfig API.
   */
  @Config("store.set.local.partition.state.enabled")
  @Default("false")
  public final boolean storeSetLocalPartitionStateEnabled;

  /**
   * True to enable bucket reports for log segment. This is only effective when the bucket count is greater than 0.
   */
  @Config("store.enable.bucket.for.log.segment.reports")
  @Default("false")
  public final boolean storeEnableBucketForLogSegmentReports;

  @Config("store.enable.current.invalid.size.metric")
  @Default("false")
  public final boolean storeEnableCurrentInvalidSizeMetric;

  @Config(storeEnableIndexDirectMemoryUsageMetricName)
  @Default("false")
  public final boolean storeEnableIndexDirectMemoryUsageMetric;
  public static final String storeEnableIndexDirectMemoryUsageMetricName =
      "store.enable.index.direct.memory.usage.metric";

  /**
   * A normalized disk IO read latency threshold(per MB). If actual normalized disk read latency is higher than the
   * threshold, we need to decrease compaction speed.
   */
  @Config("store.compaction.io.per.mb.read.latency.threshold.ms")
  @Default("20")
  public final int storeCompactionIoPerMbReadLatencyThresholdMs;

  /**
   * A normalized disk IO write latency threshold(per MB). If actual normalized disk write latency is higher than the
   * threshold, we need to decrease compaction speed.
   */
  @Config("store.compaction.io.per.mb.write.latency.threshold.ms")
  @Default("20")
  public final int storeCompactionIoPerMbWriteLatencyThresholdMs;

  /**
   * The per disk histogram's reservoir time window in millisecond.
   */
  @Config("store.disk.io.reservoir.time.window.ms")
  @Default("200")
  public final int storeDiskIoReservoirTimeWindowMs;

  /**
   * How many days of compactionlogs we have to read from disk to build the compaction history
   */
  @Config(storeCompactionHistoryInDayName)
  @Default("21")
  public final int storeCompactionHistoryInDay;
  public static final String storeCompactionHistoryInDayName = "store.compaction.history.in.day";

  /**
   * True to enable rebuilding replication token based on compaction history for all partitions in this host.
   */
  @Config(storeRebuildTokenBasedOnCompactionHistoryName)
  @Default("false")
  public final boolean storeRebuildTokenBasedOnCompactionHistory;
  public static final String storeRebuildTokenBasedOnCompactionHistoryName =
      "store.rebuild.token.based.on.compaction.history";

  /**
   * If storePersistRemoteTokenIntervalInSeconds > 0, persist the remote token every storePersistRemoteTokenIntervalInSeconds seconds.
   */
  @Config(storePersistRemoteTokenIntervalInSecondsName)
  public final int storePersistRemoteTokenIntervalInSeconds;
  public static final String storePersistRemoteTokenIntervalInSecondsName =
      "store.persist.remote.token.interval.in.seconds";

  /**
   * True to enable disk failure handler
   */
  @Config(storeDiskFailureHandlerEnabledName)
  public final boolean storeDiskFailureHandlerEnabled;
  public static final String storeDiskFailureHandlerEnabledName = "store.disk.failure.handler.enabled";

  /**
   * The interval to run disk failures in a periodical schedule.
   */
  @Config(storeDiskFailureHandlerTaskIntervalInSecondsName)
  public final int storeDiskFailureHandlerTaskIntervalInSeconds;
  public static final String storeDiskFailureHandlerTaskIntervalInSecondsName =
      "store.disk.failure.handler.task.interval.in.seconds";

  /**
   * The backoff time in seconds to retry acquiring lock in disk failure handler.
   */
  @Config(storeDiskFailureHandlerRetryLockBackoffTimeInSecondsName)
  public final int storeDiskFailureHandlerRetryLockBackoffTimeInSeconds;
  public static final String storeDiskFailureHandlerRetryLockBackoffTimeInSecondsName =
      "store.disk.failure.handler.retry.lock.backoff.time.in.seconds";

  /**
   * The percentage of real disk capacity to report to helix in disk failure handler. In Disk failure handler, we will
   * update the disk capacity to helix. We will reserve some disk space in host so this percentage should be less than
   * 100.
   */
  @Config(storeDiskCapacityReportingPercentageName)
  public final int storeDiskCapacityReportingPercentage;
  public static final String storeDiskCapacityReportingPercentageName = "store.disk.capacity.reporting.percentage";

  /**
   * The threshold of disk failures to terminate the process. If we have 10 disks and the threshold is 0.8, then when
   * there are 9, or 10 disks failed to start, or marked as unavailable, we would fail the initialization of the
   * storage manager, thus, kill the process. By default, the threshold is 1, which means we don't fail the process even
   * when all the disks failed.
   */
  @Config(storeThresholdOfDiskFailuresToTerminateName)
  public final float storeThresholdOfDiskFailuresToTerminate;
  public static final String storeThresholdOfDiskFailuresToTerminateName =
      "store.threshold.of.disk.failures.to.terminate";

  /**
   * True to remove all the unexpected directories when the current node is in FULL AUTO.
   */
  @Config(storeRemoveUnexpectedDirsInFullAutoName)
  public final boolean storeRemoveUnexpectedDirsInFullAuto;
  public static final String storeRemoveUnexpectedDirsInFullAutoName = "store.remove.unexpected.dirs.in.full.auto";

  /**
   * True to remove all the files under the partition directory and restart the blob store when blob store fails to start.
   */
  @Config(storeRemoveDirectoryAndRestartBlobStoreName)
  public final boolean storeRemoveDirectoryAndRestartBlobStore;
  public static final String storeRemoveDirectoryAndRestartBlobStoreName =
      "store.remove.directory.and.restart.blob.store";

  /**
   * True to enable partial log segment recovery. If in the last log segment, there are invalid messages, then as long
   * as these invalid messages are smaller than the given threshold, we should continue recovery and truncate the file.
   */
  @Config(storeEnablePartialLogSegmentRecoveryName)
  public final boolean storeEnablePartialLogSegmentRecovery;
  public static final String storeEnablePartialLogSegmentRecoveryName = "store.enable.partial.log.segment.recovery";

  /**
   * The threshold for partial log segment recovery. If there are more bytes in the remaining log segment than the given
   * threshold, then don't recover. The default value is 5MB, which is a bit larger than the largest put blob.
   */
  @Config(storePartialLogSegmentRecoveryRemainingDataSizeThresholdName)
  public final long storePartialLogSegmentRecoveryRemainingDataSizeThreshold;
  public static final String storePartialLogSegmentRecoveryRemainingDataSizeThresholdName =
      "store.partial.log.segment.recovery.remaining.data.size.threshold";

  /**
   * True to restore disk's availability on data node config when an unavailable disk is fixed in FULL AUTO mode.
   */
  @Config(storeRestoreUnavailableDiskInFullAutoName)
  public final boolean storeRestoreUnavailableDiskInFullAuto;
  public static final String storeRestoreUnavailableDiskInFullAutoName = "store.restore.unavailable.disk.in.full.auto";

  /**
   * True to proactively test storage availability of all blob stores on a disk when any blob store on this disk has
   * io errors.
   */
  @Config(storeProactivelyTestStorageAvailabilityName)
  public final boolean storeProactivelyTestStorageAvailability;
  public static final String storeProactivelyTestStorageAvailabilityName =
      "store.proactively.test.storage.availability";

  /**
   * Only when {@link #storeProactivelyTestStorageAvailability} is true does this configuration works. We will set some
   * delay to run the test logic so the replication or frontend requests would be able to shut down blob stores.
   */
  @Config(storeProactiveTestDelayInSecondsName)
  public final int storeProactiveTestDelayInSeconds;
  public static final String storeProactiveTestDelayInSecondsName = "store.proactive.test.delay.in.seconds";

  /**
   * If the store hasn't been up for X days, the store is stale.
   */
  @Config(storeStaleTimeInDaysName)
  public final int storeStaleTimeInDays;
  public final static String storeStaleTimeInDaysName = "store.stale.time.in.days";

  /**
   * If the store is stale and storeBlockStaleBlobStoreToStart is true, don't start the BlobStore.
   */
  @Config(storeBlockStaleBlobStoreToStartName)
  public final boolean storeBlockStaleBlobStoreToStart;
  public final static String storeBlockStaleBlobStoreToStartName = "store.block.stale.blob.store.to.start";

  /**
   * Whether to attempt reshuffling of reordered disks and subsequent process termination.
   */
  @Config("store.reshuffle.disks.on.reorder")
  @Default("false")
  public final boolean storeReshuffleDisksOnReorder;

  public final static String storeReshuffleDisksOnReorderName = "store.reshuffle.disks.on.reorder";

  /**
   * Name Of file to Store Sealed Segments Progress Status
   */
  public static final String STORE_FILE_COPY_IN_PROGRESS_FILE_NAME = "store.file.copy.in.progress.file.name";
  @Config(STORE_FILE_COPY_IN_PROGRESS_FILE_NAME)
  @Default("file_copy_in_progress")
  public final String storeFileCopyInProgressFileName;

  /**
   * Name of file to Store Sealed Segments Completion Status
   */
  public static final String STORE_FILE_COPY_COMPLETED_FILE_NAME = "store.file.copy.completed.file.name";
  @Config(STORE_FILE_COPY_COMPLETED_FILE_NAME)
  @Default("file_copy_completed")
  public final String storeFileCopyCompletedFileName;

  /**
   * Name Of file to Store Bootstrap Progress Status
   */
  public static final String STORE_BOOTSTRAP_IN_PROGRESS_FILE = "store.bootstrap.in.progress.file.name";
  @Config(STORE_BOOTSTRAP_IN_PROGRESS_FILE)
  @Default("bootstrap_in_progress")
  public final String storeBootstrapInProgressFile;

  public StoreConfig(VerifiableProperties verifiableProperties) {
    storeKeyFactory = verifiableProperties.getString("store.key.factory", "com.github.ambry.commons.BlobIdFactory");
    storeDataFlushIntervalSeconds = verifiableProperties.getLong("store.data.flush.interval.seconds", 60);
    storeIndexMaxMemorySizeBytes = verifiableProperties.getInt("store.index.max.memory.size.bytes", 20 * 1024 * 1024);
    storeDataFlushDelaySeconds = verifiableProperties.getInt("store.data.flush.delay.seconds", 5);
    storeIndexMaxNumberOfInmemElements = verifiableProperties.getInt("store.index.max.number.of.inmem.elements", 10000);
    storeIndexBloomMaxFalsePositiveProbability =
        verifiableProperties.getDoubleInRange("store.index.bloom.max.false.positive.probability", 0.01, 0.0, 1.0);
    storeMaxNumberOfEntriesToReturnFromJournal =
        verifiableProperties.getIntInRange("store.max.number.of.entries.to.return.from.journal", 5000, 1, 10000);
    storeDeletedMessageRetentionHours =
        verifiableProperties.getIntInRange("store.deleted.message.retention.hours", 168, 0, Integer.MAX_VALUE);
    storeCompactionPolicySwitchTimestampDays =
        verifiableProperties.getIntInRange("store.compaction.policy.switch.timestamp.days", 6, 1, 14);
    storeHybridCompactionFullCompactionStaggerLimitInHours =
        verifiableProperties.getIntInRange("store.hybrid.compaction.full.compaction.stagger.limit.in.hours", 0, 0,
            Integer.MAX_VALUE);
    storeCompactAllPolicyFilterOutAllValidSegment =
        verifiableProperties.getBoolean(storeCompactAllPolicyFilterOutAllValidSegmentName, false);
    storeContainerDeletionRetentionDays = verifiableProperties.getInt("store.container.deletion.retention.days", 14);
    storeHardDeleteOperationsBytesPerSec =
        verifiableProperties.getIntInRange("store.hard.delete.operations.bytes.per.sec", 100 * 1024, 1,
            Integer.MAX_VALUE);
    storeCompactionOperationsBytesPerSec =
        verifiableProperties.getIntInRange("store.compaction.operations.bytes.per.sec", 1 * 1024 * 1024, 1,
            Integer.MAX_VALUE);
    storeCompactionMinOperationsBytesPerSec =
        verifiableProperties.getIntInRange("store.compaction.min.operations.bytes.per.sec", 512 * 1024, 1,
            Integer.MAX_VALUE);
    storeCompactionOperationsAdjustK =
        verifiableProperties.getDoubleInRange("store.compaction.operations.adjust.k", 1.0, -100.0, 100.0);
    storeCompactionThrottlerCheckIntervalMs =
        verifiableProperties.getIntInRange("store.compaction.throttler.check.interval.ms", -1, -1, Integer.MAX_VALUE);
    storeCompactionEnableDirectIO = verifiableProperties.getBoolean("store.compaction.enable.direct.io", false);
    storeCompactionDirectIOBufferSize =
        verifiableProperties.getIntInRange(storeCompactionDirectIOBufferSizeName, 0, 0, 100 * 1024 * 1024);
    storeCompactionPurgeDeleteTombstone =
        verifiableProperties.getBoolean("store.compaction.purge.delete.tombstone", false);

    storeCompactionMinBufferSize =
        verifiableProperties.getIntInRange("store.compaction.min.buffer.size", 10 * 1024 * 1024, 0, Integer.MAX_VALUE);
    storeCompactionFilter =
        verifiableProperties.getString("store.compaction.filter", "IndexSegmentValidEntryFilterWithoutUndelete");
    storeEnableHardDelete = verifiableProperties.getBoolean("store.enable.hard.delete", false);
    storeSegmentSizeInBytes =
        verifiableProperties.getLongInRange("store.segment.size.in.bytes", Long.MAX_VALUE, 1, Long.MAX_VALUE);
    storeMinUsedCapacityToTriggerCompactionInPercentage =
        verifiableProperties.getInt("store.min.used.capacity.to.trigger.compaction.in.percentage", 50);
    storeCompactionTriggers = verifiableProperties.getString("store.compaction.triggers", "").split(",");
    storeCompactionCheckFrequencyInHours =
        verifiableProperties.getIntInRange("store.compaction.check.frequency.in.hours", 7 * 24, 1, 365 * 24);
    storeCompactionPolicyFactory = verifiableProperties.getString("store.compaction.policy.factory",
        "com.github.ambry.store.CompactAllPolicyFactory");
    storeMinLogSegmentCountToReclaimToTriggerCompaction =
        verifiableProperties.getIntInRange("store.min.log.segment.count.to.reclaim.to.trigger.compaction", 1, 1, 1000);
    storeStatsBasedMiddleRangeCompactionIntervalInMs =
        verifiableProperties.getLongInRange("store.stats.based.middle.range.compaction.interval.in.ms", 0, 0,
            Long.MAX_VALUE);
    storeStatsBasedWeightOnBenefitEnabled =
        verifiableProperties.getBoolean("store.stats.based.weight.on.benefit.enabled", false);
    storeMaxLogSegmentValidDataPercentageToQualifyCompaction =
        verifiableProperties.getDoubleInRange("store.max.log.segment.valid.data.percentage.to.qualify.compaction", 0.30,
            0.0, 1.0);
    storeStatsBasedCompactionMinCostInPercentage =
        verifiableProperties.getDoubleInRange("store.stats.based.compaction.min.cost.in.percentage", 0.06, 0.0, 1.0);
    storeStatsBucketCount = verifiableProperties.getIntInRange("store.stats.bucket.count", 0, 0, 10000);
    storeStatsBucketSpanInMinutes =
        verifiableProperties.getLongInRange("store.stats.bucket.span.in.minutes", 60, 1, 10000);
    storeStatsRecentEntryProcessingIntervalInMinutes =
        verifiableProperties.getLongInRange("store.stats.recent.entry.processing.interval.in.minutes", 2, 1, 60);
    storeGetValidSizeIntervalInSecs =
        verifiableProperties.getLongInRange("store.get.valid.size.interval.in.secs", 2 * 60, 1, 60 * 60);
    storeStatsWaitTimeoutInSecs =
        verifiableProperties.getLongInRange("store.stats.wait.timeout.in.secs", 2 * 60, 0, 30 * 60);
    storeStatsIndexEntriesPerSecond =
        verifiableProperties.getIntInRange("store.stats.index.entries.per.second", 240000, 1, Integer.MAX_VALUE);
    storeIndexPersistedEntryMinBytes = verifiableProperties.getInt("store.index.persisted.entry.min.bytes", 115);
    storeDetermineLogSegmentOnlyContainsHeaderMinBytes =
        verifiableProperties.getIntInRange("store.determine.log.segment.only.contains.header.min.bytes", 20, 1, 1000);
    storeReplicaStatusDelegateEnable = verifiableProperties.getBoolean(storeReplicaStatusDelegateEnableName, false);
    storeReadOnlyEnableSizeThresholdPercentage =
        verifiableProperties.getIntInRange(storeReadOnlyEnableSizeThresholdPercentageName, 95, 0, 100);
    storeReadOnlyToPartialWriteEnableSizeThresholdPercentageDelta =
        verifiableProperties.getIntInRange(storeReadOnlyToPartialWriteEnableSizeThresholdPercentageDeltaName, 5, 0,
            storeReadOnlyEnableSizeThresholdPercentage);
    storePartialWriteEnableSizeThresholdPercentage =
        verifiableProperties.getIntInRange(storePartialWriteEnableSizeThresholdPercentageName, 50, 0, 100);
    storePartialWriteToReadWriteEnableSizeThresholdPercentageDelta =
        verifiableProperties.getIntInRange(storePartialWriteToReadWriteEnableSizeThresholdPercentageDeltaName, 5, 0,
            storePartialWriteEnableSizeThresholdPercentage);
    storeValidateAuthorization = verifiableProperties.getBoolean("store.validate.authorization", false);
    storeTtlUpdateBufferTimeSeconds =
        verifiableProperties.getIntInRange(storeTtlUpdateBufferTimeSecondsName, 60 * 60 * 24, 0, Integer.MAX_VALUE);
    storeIndexMemState = IndexMemState.valueOf(
        verifiableProperties.getString(storeIndexMemStateName, IndexMemState.MMAP_WITHOUT_FORCE_LOAD.name()));
    storeIoErrorCountToTriggerShutdown =
        verifiableProperties.getIntInRange("store.io.error.count.to.trigger.shutdown", Integer.MAX_VALUE, 1,
            Integer.MAX_VALUE);
    storeSetFilePermissionEnabled = verifiableProperties.getBoolean("store.set.file.permission.enabled", false);
    storeAutoCloseLastLogSegmentEnabled =
        verifiableProperties.getBoolean(storeAutoCloseLastLogSegmentEnabledName, false);
    storeUnsealReplicaMinimumLagBytes =
        verifiableProperties.getLongInRange(storeUnsealReplicaMinimumLagBytesName, 0, 0, Long.MAX_VALUE);
    String storeDataFilePermissionStr = verifiableProperties.getString("store.data.file.permission", "rw-rw----");
    storeDataFilePermission = PosixFilePermissions.fromString(storeDataFilePermissionStr);
    String storeOperationFilePermissionStr =
        verifiableProperties.getString("store.operation.file.permission", "rw-rw-r--");
    storeOperationFilePermission = PosixFilePermissions.fromString(storeOperationFilePermissionStr);
    storeUuidBasedBloomFilterEnabled = verifiableProperties.getBoolean("store.uuid.based.bloom.filter.enabled", false);
    storeIndexRebuildBloomFilterEnabled =
        verifiableProperties.getBoolean("store.index.rebuild.bloom.filter.enabled", false);
    storeBloomFilterMaximumPageCount =
        verifiableProperties.getIntInRange("store.bloom.filter.maximum.page.count", 128, 1, Integer.MAX_VALUE);
    storeContainerDeletionEnabled = verifiableProperties.getBoolean("store.container.deletion.enabled", false);
    storeSetLocalPartitionStateEnabled =
        verifiableProperties.getBoolean("store.set.local.partition.state.enabled", false);
    storeEnableBucketForLogSegmentReports =
        verifiableProperties.getBoolean("store.enable.bucket.for.log.segment.reports", false);
    storeEnableCurrentInvalidSizeMetric =
        verifiableProperties.getBoolean("store.enable.current.invalid.size.metric", false);
    storeEnableIndexDirectMemoryUsageMetric =
        verifiableProperties.getBoolean(storeEnableIndexDirectMemoryUsageMetricName, false);
    storeRebuildTokenBasedOnResetKey = verifiableProperties.getBoolean("store.rebuild.token.based.on.reset.key", false);
    storeCompactionIoPerMbReadLatencyThresholdMs =
        verifiableProperties.getIntInRange("store.compaction.io.per.mb.read.latency.threshold.ms", 20, 0,
            Integer.MAX_VALUE);
    storeCompactionIoPerMbWriteLatencyThresholdMs =
        verifiableProperties.getIntInRange("store.compaction.io.per.mb.write.latency.threshold.ms", 20, 0,
            Integer.MAX_VALUE);
    storeDiskIoReservoirTimeWindowMs =
        verifiableProperties.getIntInRange("store.disk.io.reservoir.time.window.ms", 200, 0, Integer.MAX_VALUE);
    storeCompactionHistoryInDay = verifiableProperties.getIntInRange(storeCompactionHistoryInDayName, 21, 1, 365);
    storeRebuildTokenBasedOnCompactionHistory =
        verifiableProperties.getBoolean(storeRebuildTokenBasedOnCompactionHistoryName, false);
    // persistRemoteToken has to be on all the time. It's a signal that the blobStore is alive.
    storePersistRemoteTokenIntervalInSeconds =
        verifiableProperties.getIntInRange(storePersistRemoteTokenIntervalInSecondsName, 600, 5, 60 * 60 * 24);
    // storePersistentRemoteTokenInterval has to be valid value (enabled) before enabling this feature.
    storeCompactionIgnorePeersUnavailableForDays =
        verifiableProperties.getIntInRange(storeCompactionIgnorePeersUnavailableForDaysName, 0, 0, Integer.MAX_VALUE);
    // While making transition from StoreConfig#storeDeletedMessageRetentionHours to StoreConfig#storeDeletedMessageRetentionMinutes
    // we need to make sure that the storeDeletedMessageRetentionHours isn't set by any hidden config that's missed.
    int deletedMessageRetentionMinutes =
        verifiableProperties.getIntInRange("store.deleted.message.retention.minutes", -1, -1, Integer.MAX_VALUE);
    if (deletedMessageRetentionMinutes == -1 && storeDeletedMessageRetentionHours != 168) {
      logger.warn("storeDeletedMessageRetentionHours config is overridden from default value.");
    }
    storeDeletedMessageRetentionMinutes =
        (deletedMessageRetentionMinutes == -1) ? storeDeletedMessageRetentionHours * 60
            : deletedMessageRetentionMinutes;
    storeDiskFailureHandlerEnabled = verifiableProperties.getBoolean(storeDiskFailureHandlerEnabledName, false);
    storeDiskFailureHandlerTaskIntervalInSeconds =
        verifiableProperties.getIntInRange(storeDiskFailureHandlerTaskIntervalInSecondsName, 10 * 60, 1,
            Integer.MAX_VALUE);
    storeDiskFailureHandlerRetryLockBackoffTimeInSeconds =
        verifiableProperties.getIntInRange(storeDiskFailureHandlerRetryLockBackoffTimeInSecondsName, 30, 0,
            Integer.MAX_VALUE);
    if (storeDiskFailureHandlerRetryLockBackoffTimeInSeconds > storeDiskFailureHandlerTaskIntervalInSeconds) {
      throw new IllegalStateException("Retry lock backoff time should be shorter than task interval: "
          + storeDiskFailureHandlerRetryLockBackoffTimeInSeconds + " < "
          + storeDiskFailureHandlerTaskIntervalInSeconds);
    }
    storeDiskCapacityReportingPercentage =
        verifiableProperties.getIntInRange(storeDiskCapacityReportingPercentageName, 95, 0, 100);
    storeThresholdOfDiskFailuresToTerminate =
        verifiableProperties.getFloatInRange(storeThresholdOfDiskFailuresToTerminateName, 1.0f, 0.0f, 1.0f);
    storeRemoveUnexpectedDirsInFullAuto =
        verifiableProperties.getBoolean(storeRemoveUnexpectedDirsInFullAutoName, false);
    storeRemoveDirectoryAndRestartBlobStore =
        verifiableProperties.getBoolean(storeRemoveDirectoryAndRestartBlobStoreName, false);
    storeEnablePartialLogSegmentRecovery =
        verifiableProperties.getBoolean(storeEnablePartialLogSegmentRecoveryName, false);
    storePartialLogSegmentRecoveryRemainingDataSizeThreshold =
        verifiableProperties.getIntInRange(storePartialLogSegmentRecoveryRemainingDataSizeThresholdName,
            5 * 1024 * 1024, 0, 1024 * 1024 * 1024);
    storeRestoreUnavailableDiskInFullAuto =
        verifiableProperties.getBoolean(storeRestoreUnavailableDiskInFullAutoName, false);
    storeProactivelyTestStorageAvailability =
        verifiableProperties.getBoolean(storeProactivelyTestStorageAvailabilityName, false);
    storeProactiveTestDelayInSeconds =
        verifiableProperties.getIntInRange(storeProactiveTestDelayInSecondsName, 60, 0, Integer.MAX_VALUE);
    storeStaleTimeInDays = verifiableProperties.getIntInRange(storeStaleTimeInDaysName, 7, 0, Integer.MAX_VALUE);
    storeBlockStaleBlobStoreToStart = verifiableProperties.getBoolean(storeBlockStaleBlobStoreToStartName, false);
    storeReshuffleDisksOnReorder = verifiableProperties.getBoolean(storeReshuffleDisksOnReorderName, false);

    storeFileCopyInProgressFileName = verifiableProperties.getString(STORE_FILE_COPY_IN_PROGRESS_FILE_NAME, "file_copy_in_progress");
    storeBootstrapInProgressFile = verifiableProperties.getString(STORE_BOOTSTRAP_IN_PROGRESS_FILE, "bootstrap_in_progress");
    storeFileCopyCompletedFileName = verifiableProperties.getString(STORE_FILE_COPY_COMPLETED_FILE_NAME, "file_copy_completed");
  }
}