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
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HybridCompactionPolicy will run compaction with StatsBasedCompactionPolicy more frequently and with CompactAllPolicy
 * one time out of storeCompactionPolicySwitchPeriod.
 */
public class HybridCompactionPolicy implements CompactionPolicy{
  private final Time time;
  private final StoreConfig storeConfig;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final SafeCounterWithoutLock counter;

  HybridCompactionPolicy(StoreConfig storeConfig, Time time) {
    this.storeConfig = storeConfig;
    this.time = time;
    counter = new SafeCounterWithoutLock();
  }

  @Override
  public CompactionDetails getCompactionDetails(long totalCapacity, long usedCapacity, long segmentCapacity,
      long segmentHeaderSize, List<String> logSegmentsNotInJournal, BlobStoreStats blobStoreStats)
      throws StoreException {
    return selectCompactionPolicy().getCompactionDetails(totalCapacity, usedCapacity, segmentCapacity,
        segmentHeaderSize, logSegmentsNotInJournal, blobStoreStats);
  }

  /**
   * Selects which compaction policy needs to be used for current compaction cycle.
   * @return CompactAllPolicy if the round number of compaction reach to storeConfig.storeCompactionPolicySwitchPeriod.
   * Othewise @return StatsBasedCompactionPolicy.
   */
  CompactionPolicy selectCompactionPolicy(){
    if (counter.incrementAndGet() == 0) {
      logger.info("Return CompactAllPolicy this round");
      return new CompactAllPolicy(storeConfig, time);
    } else {
      logger.info("Return StatsBasedCompactionPolicy this round");
      return new StatsBasedCompactionPolicy(storeConfig, time);
    }
  }

  private class SafeCounterWithoutLock {
    private final AtomicInteger counter = new AtomicInteger(0);
    public int getValue() {
      return counter.get();
    }
    public int incrementAndGet() {
      while (true) {
        int existingValue = getValue();
        int newValue = (existingValue + 1) % storeConfig.storeCompactionPolicySwitchPeriod;
        if (counter.compareAndSet(existingValue, newValue)) {
          return newValue;
        }
      }
    }
  }
}


