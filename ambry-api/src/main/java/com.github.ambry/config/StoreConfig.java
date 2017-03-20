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
   * The rate of I/O allowed for hard deletes.
   */
  @Config("store.hard.delete.bytes.per.sec")
  @Default("1*1024*1024")
  public final int storeHardDeleteBytesPerSec;

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
   * The minimum capacity that has to be used (as a percentage of the total capacity) for the store to trigger
   * compaction
   */
  @Config("store.min.used.capacity.to.trigger.compaction.in.percentage")
  @Default("50")
  public final int storeMinUsedCapacityToTriggerCompactionInPercentage;

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
    storeHardDeleteBytesPerSec = verifiableProperties.getInt("store.hard.delete.bytes.per.sec", 1 * 1024 * 1024);
    storeEnableHardDelete = verifiableProperties.getBoolean("store.enable.hard.delete", false);
    storeSegmentSizeInBytes =
        verifiableProperties.getLongInRange("store.segment.size.in.bytes", Long.MAX_VALUE, 1, Long.MAX_VALUE);
    storeMinUsedCapacityToTriggerCompactionInPercentage =
        verifiableProperties.getInt("store.min.used.capacity.to.trigger.compaction.in.percentage", 50);
  }
}

