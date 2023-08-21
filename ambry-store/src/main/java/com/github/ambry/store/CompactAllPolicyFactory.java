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
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.List;
import java.util.NavigableMap;
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
 * to be compacted for {@link CompactionPolicy#getCompactionDetails(long, long, long, long, List, BlobStoreStats, String)}
 */
class CompactAllPolicy implements CompactionPolicy {

  final static long ERROR_MARGIN_MS = 1000 * 60 * 60;
  private final StoreConfig storeConfig;
  private final Time time;
  private final long messageRetentionTimeInMs;
  private static final Logger logger = LoggerFactory.getLogger(CompactAllPolicy.class);

  CompactAllPolicy(StoreConfig storeConfig, Time time) {
    this.storeConfig = storeConfig;
    this.time = time;
    this.messageRetentionTimeInMs = TimeUnit.MINUTES.toMillis(storeConfig.storeDeletedMessageRetentionMinutes);
  }

  @Override
  public CompactionDetails getCompactionDetails(long totalCapacity, long usedCapacity, long segmentCapacity,
      long segmentHeaderSize, List<LogSegmentName> logSegmentsNotInJournal, BlobStoreStats blobStoreStats,
      String dataDir) throws StoreException {
    CompactionDetails details = null;
    logger.trace("UsedCapacity {} vs TotalCapacity {}", usedCapacity, totalCapacity);
    if (usedCapacity >= (storeConfig.storeMinUsedCapacityToTriggerCompactionInPercentage / 100.0) * totalCapacity) {
      if (logSegmentsNotInJournal != null) {
        details = new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, logSegmentsNotInJournal, null);
        logger.info("Generating CompactionDetails {} using CompactAllPolicy", details);

        Pair<Long, NavigableMap<LogSegmentName, Long>> validDataSizeByLogSegment =
            blobStoreStats.getValidDataSizeByLogSegment(
                new TimeRange(time.milliseconds() - messageRetentionTimeInMs - ERROR_MARGIN_MS, ERROR_MARGIN_MS));
        if (validDataSizeByLogSegment != null) {
          final StringBuilder sizeLog = new StringBuilder(
              "Valid data size for " + dataDir + " from BlobStoreStats " + validDataSizeByLogSegment.getFirst()
                  + " segments: ");
          validDataSizeByLogSegment.getSecond().forEach((logSegmentName, validDataSize) -> {
            sizeLog.append(
                logSegmentName + " " + validDataSize / 1000 / 1000 / 1000.0 + "GB " + validDataSize / segmentCapacity
                    + "%;");
          });
          logger.info(sizeLog.toString());
        }
      }
    }
    return details;
  }
}
