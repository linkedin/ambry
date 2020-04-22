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


/**
 * The configs for the store
 */

public class StoreConfig {

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
   * How long (in days) a key must be in deleted state before it is hard deleted.
   */
  @Config("store.deleted.message.retention.days")
  @Default("7")
  public final int storeDeletedMessageRetentionDays;

  /**
   * The rate of I/O allowed per disk for hard deletes.
   */
  @Config("store.hard.delete.operations.bytes.per.sec")
  @Default("100*1024")
  public final int storeHardDeleteOperationsBytesPerSec;

  /**
   * The rate of I/O allowed per disk for compaction.
   */
  @Config("store.compaction.operations.bytes.per.sec")
  @Default("1*1024*1024")
  public final int storeCompactionOperationsBytesPerSec;

  /**
   * Whether direct IO are to be enable or not for compaction.
   * This is only supported on > Linux 2.6
   */
  @Config("store.compaction.enable.direct.io")
  @Default("false")
  public final boolean storeCompactionEnableDirectIO;

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
   * Specifies the size threshold (as percentage of maximum size) of a store for converting the chunk to RO from RW
   */
  @Config(storeReadOnlyEnableSizeThresholdPercentageName)
  @Default("95")
  public final int storeReadOnlyEnableSizeThresholdPercentage;
  public static final String storeReadOnlyEnableSizeThresholdPercentageName =
      "store.read.only.enable.size.threshold.percentage";

  /**
   * Specifies the size threshold delta below {@link #storeReadOnlyEnableSizeThresholdPercentageName} that a store will
   * be converted from RO to RW
   */
  @Config(storeReadWriteEnableSizeThresholdPercentageDeltaName)
  @Default("5")
  public final int storeReadWriteEnableSizeThresholdPercentageDelta;
  public static final String storeReadWriteEnableSizeThresholdPercentageDeltaName =
      "store.read.write.enable.size.threshold.percentage.delta";

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
    storeDeletedMessageRetentionDays = verifiableProperties.getInt("store.deleted.message.retention.days", 7);
    storeHardDeleteOperationsBytesPerSec =
        verifiableProperties.getIntInRange("store.hard.delete.operations.bytes.per.sec", 100 * 1024, 1,
            Integer.MAX_VALUE);
    storeCompactionOperationsBytesPerSec =
        verifiableProperties.getIntInRange("store.compaction.operations.bytes.per.sec", 1 * 1024 * 1024, 1,
            Integer.MAX_VALUE);
    storeCompactionEnableDirectIO = verifiableProperties.getBoolean("store.compaction.enable.direct.io", false);
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
    storeStatsBucketCount = verifiableProperties.getIntInRange("store.stats.bucket.count", 0, 0, 10000);
    storeStatsBucketSpanInMinutes =
        verifiableProperties.getLongInRange("store.stats.bucket.span.in.minutes", 60, 1, 10000);
    storeStatsRecentEntryProcessingIntervalInMinutes =
        verifiableProperties.getLongInRange("store.stats.recent.entry.processing.interval.in.minutes", 2, 1, 60);
    storeStatsWaitTimeoutInSecs =
        verifiableProperties.getLongInRange("store.stats.wait.timeout.in.secs", 2 * 60, 0, 30 * 60);
    storeStatsIndexEntriesPerSecond =
        verifiableProperties.getIntInRange("store.stats.index.entries.per.second", 240000, 1, Integer.MAX_VALUE);
    storeIndexPersistedEntryMinBytes = verifiableProperties.getInt("store.index.persisted.entry.min.bytes", 115);
    storeReplicaStatusDelegateEnable = verifiableProperties.getBoolean(storeReplicaStatusDelegateEnableName, false);
    storeReadOnlyEnableSizeThresholdPercentage =
        verifiableProperties.getIntInRange(storeReadOnlyEnableSizeThresholdPercentageName, 95, 0, 100);
    storeReadWriteEnableSizeThresholdPercentageDelta =
        verifiableProperties.getIntInRange(storeReadWriteEnableSizeThresholdPercentageDeltaName, 5, 0,
            storeReadOnlyEnableSizeThresholdPercentage);
    storeValidateAuthorization = verifiableProperties.getBoolean("store.validate.authorization", false);
    storeTtlUpdateBufferTimeSeconds =
        verifiableProperties.getIntInRange(storeTtlUpdateBufferTimeSecondsName, 60 * 60 * 24, 0, Integer.MAX_VALUE);
    storeIndexMemState = IndexMemState.valueOf(
        verifiableProperties.getString(storeIndexMemStateName, IndexMemState.MMAP_WITHOUT_FORCE_LOAD.name()));
    storeIoErrorCountToTriggerShutdown =
        verifiableProperties.getIntInRange("store.io.error.count.to.trigger.shutdown", Integer.MAX_VALUE, 1,
            Integer.MAX_VALUE);
    storeSetFilePermissionEnabled = verifiableProperties.getBoolean("store.set.file.permission.enabled", false);
    String storeDataFilePermissionStr = verifiableProperties.getString("store.data.file.permission", "rw-rw----");
    storeDataFilePermission = PosixFilePermissions.fromString(storeDataFilePermissionStr);
    String storeOperationFilePermissionStr =
        verifiableProperties.getString("store.operation.file.permission", "rw-rw-r--");
    storeOperationFilePermission = PosixFilePermissions.fromString(storeOperationFilePermissionStr);
    storeUuidBasedBloomFilterEnabled = verifiableProperties.getBoolean("store.uuid.based.bloom.filter.enabled", false);
    storeIndexRebuildBloomFilterEnabled =
        verifiableProperties.getBoolean("store.index.rebuild.bloom.filter.enabled", false);
  }
}

