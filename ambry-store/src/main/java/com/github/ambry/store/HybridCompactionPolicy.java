/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HybridCompactionPolicy will run compaction with StatsBasedCompactionPolicy more frequently and with CompactAllPolicy
 * one time out of storeCompactionPolicySwitchPeriod.
 */
public class HybridCompactionPolicy implements CompactionPolicy {
  private final Time time;
  private final StoreConfig storeConfig;
  private static final Logger logger = LoggerFactory.getLogger(HybridCompactionPolicy.class);

  HybridCompactionPolicy(StoreConfig storeConfig, Time time) {
    this.storeConfig = storeConfig;
    this.time = time;
  }

  @Override
  public CompactionDetails getCompactionDetails(long totalCapacity, long usedCapacity, long segmentCapacity,
      long segmentHeaderSize, List<String> logSegmentsNotInJournal, BlobStoreStats blobStoreStats,
      SafeCounterWithoutLock safeCounterWithoutLock) throws StoreException {
    CompactionPolicy compactionPolicy = selectCompactionPolicy(safeCounterWithoutLock);
    logger.info("Current compaction policy is : {}", compactionPolicy);
    return compactionPolicy.getCompactionDetails(totalCapacity, usedCapacity, segmentCapacity, segmentHeaderSize,
        logSegmentsNotInJournal, blobStoreStats, safeCounterWithoutLock);
  }

  /**
   * Selects which compaction policy to use for current compaction cycle.
   * @return CompactAllPolicy if the round number of compaction reach to storeConfig.storeCompactionPolicySwitchPeriod.
   * Othewise @return StatsBasedCompactionPolicy.
   */
  CompactionPolicy selectCompactionPolicy(SafeCounterWithoutLock safeCounterWithoutLock) {
    if (safeCounterWithoutLock != null && safeCounterWithoutLock.getValue() == 0) {
      logger.trace("Return CompactAllPolicy this round");
      return new CompactAllPolicy(storeConfig, time);
    } else {
      if (safeCounterWithoutLock == null) {
        logger.trace("Counter is null");
      } else {
        logger.trace("Return StatsBasedCompactionPolicy this round");
      }
      return new StatsBasedCompactionPolicy(storeConfig, time);
    }
  }
}


