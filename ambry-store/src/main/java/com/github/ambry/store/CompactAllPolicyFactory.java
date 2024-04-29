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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
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
        Pair<Long, NavigableMap<LogSegmentName, Long>> validDataSizeByLogSegment =
            blobStoreStats.getValidDataSizeByLogSegment(
                new TimeRange(time.milliseconds() - messageRetentionTimeInMs - ERROR_MARGIN_MS, ERROR_MARGIN_MS));
        String sizeLog = "";
        if (validDataSizeByLogSegment != null) {
          sizeLog = blobStoreStats.dumpLogSegmentSize(validDataSizeByLogSegment.getSecond(), segmentCapacity, dataDir);
        }
        //@formatter:off
        List<LogSegmentName> finalLogSegmentNames =
            storeConfig.storeCompactAllPolicyFilterOutAllValidSegment && validDataSizeByLogSegment != null
            ?filterOutAllValidLogSegment(logSegmentsNotInJournal, blobStoreStats.getLogSegmentSizeProvider(), validDataSizeByLogSegment.getSecond())
            :logSegmentsNotInJournal;
        //@formatter:on
        if (finalLogSegmentNames == null || finalLogSegmentNames.isEmpty()) {
          return null;
        }
        details =
            new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, finalLogSegmentNames, null, true);

        logger.info("[CAPACITY] Generating CompactionDetails {} using CompactAllPolicy for {}. Stats {}", details,
            dataDir, sizeLog);
      }
    }
    return details;
  }

  /**
   * Filter out all the leading and trailing log segments whose data is 100% valid.
   * For instance, if segment_1, segment_2, segment_3 all have 100% valid data, this method would return a null.
   * I segment_1, segment_3 has 100% valid data, this method would return segment_2.
   * @param logSegmentsNotInJournal The list of {@link LogSegmentName}s that are not in journal.
   * @param sizeProvider The {@link LogSegmentSizeProvider}.
   * @param validDataSizeByLogSegment The map from {@link LogSegmentName} to valid data size.
   * @return A list of {@link LogSegmentName}s.
   */
  List<LogSegmentName> filterOutAllValidLogSegment(List<LogSegmentName> logSegmentsNotInJournal,
      LogSegmentSizeProvider sizeProvider, NavigableMap<LogSegmentName, Long> validDataSizeByLogSegment) {
    List<LogSegmentName> allValidLogSegments = new ArrayList<>();

    Predicate<LogSegmentName> isLogSegmentDataAllValid = segmentName -> {
      long segmentTotalSize = sizeProvider.getLogSegmentSize(segmentName);
      long segmentValidSize = validDataSizeByLogSegment.getOrDefault(segmentName, 0L);
      if (segmentValidSize == segmentTotalSize) {
        logger.info("All LogSegment {}'s data is valid, total size is {}, skip this log segment in compaction",
            segmentName, segmentTotalSize);
        allValidLogSegments.add(segmentName);
        return true;
      }
      return false;
    };
    for (LogSegmentName segmentName : logSegmentsNotInJournal) {
      if (!isLogSegmentDataAllValid.test(segmentName)) {
        break;
      }
    }
    if (allValidLogSegments.size() == logSegmentsNotInJournal.size()) {
      logger.info("All LogSegments have 100% of valid data, skip compaction");
      return null;
    }

    for (int i = logSegmentsNotInJournal.size() - 1; i >= 0; i--) {
      if (!isLogSegmentDataAllValid.test(logSegmentsNotInJournal.get(i))) {
        break;
      }
    }

    List<LogSegmentName> finalLogSegmentNames = new ArrayList<>(logSegmentsNotInJournal);
    finalLogSegmentNames.removeAll(allValidLogSegments);
    return finalLogSegmentNames;
  }
}
