/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Factory class to get {@link CompactAllPolicy}
 */
public class CompactAllPolicyFactory implements CompactionPolicyFactory {

  private final StoreConfig storeConfig;
  private final Time time;

  public CompactAllPolicyFactory(StoreConfig storeConfig, Time time) {
    this.storeConfig = storeConfig;
    this.time = time;
  }

  public CompactionPolicy getCompactionPolicy() {
    return new CompactAllPolicy(storeConfig, time);
  }
}

/**
 * CompactAllPolicy returns the entire list of all log segments non overlapping with {@link Journal} as a candidate
 * to be compacted for {@link #getCompactionDetails(long, long, long, long, List, BlobStoreStats)}
 */
class CompactAllPolicy implements CompactionPolicy {

  private final StoreConfig storeConfig;
  private final Time time;
  private final long messageRetentionTimeInMs;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  CompactAllPolicy(StoreConfig storeConfig, Time time) {
    this.storeConfig = storeConfig;
    this.time = time;
    this.messageRetentionTimeInMs = TimeUnit.DAYS.toMillis(storeConfig.storeDeletedMessageRetentionDays);
  }

  @Override
  public CompactionDetails getCompactionDetails(long totalCapacity, long usedCapacity, long segmentCapacity,
      long segmentHeaderSize, List<String> logSegmentsNotInJournal, BlobStoreStats blobStoreStats) {
    CompactionDetails details = null;
    logger.trace("UsedCapacity {} vs TotalCapacity {}", usedCapacity, totalCapacity);
    if (usedCapacity >= (storeConfig.storeMinUsedCapacityToTriggerCompactionInPercentage / 100.0) * totalCapacity) {
      if (logSegmentsNotInJournal != null) {
        details = new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, logSegmentsNotInJournal);
        logger.info("Generating CompactionDetails {} using CompactAllPolicy", details);
      }
    }
    return details;
  }
}
