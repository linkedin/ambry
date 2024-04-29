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
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.junit.Test;


public class CompactAllPolicyTest {

  // the properties that will used to generate a StoreConfig. Clear before use if required.
  private final Properties properties = new Properties();
  private final Time time = new MockTime();
  private StoreConfig config;
  private MockBlobStore blobStore;
  private long messageRetentionTimeInMs;
  private CompactionPolicy compactionPolicy = null;

  /**
   * Instantiates {@link CompactionPolicyTest} with the required cast
   * @throws InterruptedException
   */
  public CompactAllPolicyTest() throws InterruptedException {
    CompactionPolicyTest.StoreConfigWrapper wrapper = new CompactionPolicyTest.StoreConfigWrapper();
    wrapper.compactAllFilterAllValidSegment = true;
    Pair<MockBlobStore, StoreConfig> initState = CompactionPolicyTest.initializeBlobStore(properties, time, wrapper);
    config = initState.getSecond();
    messageRetentionTimeInMs = TimeUnit.MINUTES.toMillis(config.storeDeletedMessageRetentionMinutes);
    blobStore = initState.getFirst();
    compactionPolicy = new CompactAllPolicy(config, time);
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)}
   * @throws StoreException
   * @throws InterruptedException
   */
  @Test
  public void testGetCompactionDetailsTest() throws StoreException {
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName(1);
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.logSegmentsNotInJournal, null),
        blobStore, compactionPolicy);

    // random no of valid logSegments
    for (int i = 0; i < 3; i++) {
      int logSegmentCount = TestUtils.RANDOM.nextInt(10) + 1;
      blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName(logSegmentCount);
      CompactionPolicyTest.verifyCompactionDetails(
          new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.logSegmentsNotInJournal,
              null), blobStore, compactionPolicy);
    }
  }

  /**
   * Test compact all policy when there are log segments who has 100% valid data.
   * @throws Exception
   */
  @Test
  public void testFilter() throws Exception {
    // case 1. all log segments have 100% valid data
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName(5);
    MockBlobStoreStats blobStoreStats = blobStore.getBlobStoreStats();
    NavigableMap<LogSegmentName, Long> validDataSize = new TreeMap<>();
    Map<LogSegmentName, Long> allDataSize = new HashMap<>();
    long maxDataSize = blobStore.segmentCapacity - blobStore.segmentHeaderSize;
    for (LogSegmentName segmentName : blobStore.logSegmentsNotInJournal) {
      validDataSize.put(segmentName, maxDataSize);
      allDataSize.put(segmentName, maxDataSize);
    }
    blobStoreStats.validDataSizeByLogSegments = validDataSize;
    blobStoreStats.setLogSegmentSizeProvider(new MapLogSegmentSizeProvider(allDataSize));
    CompactionPolicyTest.verifyCompactionDetails(null, blobStore, compactionPolicy);

    // case 2, only leading segments have 100% valid data
    List<LogSegmentName> logSegmentToCompact = new ArrayList<>();
    for (int i = 0; i < blobStore.logSegmentsNotInJournal.size(); i++) {
      LogSegmentName segmentName = blobStore.logSegmentsNotInJournal.get(i);
      if (i < 2) {
        validDataSize.put(segmentName, allDataSize.get(segmentName));
      } else {
        validDataSize.put(segmentName, allDataSize.get(segmentName) / 2);
        logSegmentToCompact.add(segmentName);
      }
    }
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, logSegmentToCompact, null), blobStore,
        compactionPolicy);

    // case 3. only trailing segments have 100% valid data
    logSegmentToCompact = new ArrayList<>();
    for (int i = 0; i < blobStore.logSegmentsNotInJournal.size(); i++) {
      LogSegmentName segmentName = blobStore.logSegmentsNotInJournal.get(i);
      if (i > 2) {
        validDataSize.put(segmentName, allDataSize.get(segmentName));
      } else {
        validDataSize.put(segmentName, allDataSize.get(segmentName) / 2);
        logSegmentToCompact.add(segmentName);
      }
    }
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, logSegmentToCompact, null), blobStore,
        compactionPolicy);

    // case 4: leading and trailing segments both have 100% valid data
    logSegmentToCompact = new ArrayList<>();
    for (int i = 0; i < blobStore.logSegmentsNotInJournal.size(); i++) {
      LogSegmentName segmentName = blobStore.logSegmentsNotInJournal.get(i);
      if (i == 2) {
        validDataSize.put(segmentName, allDataSize.get(segmentName) / 2);
        logSegmentToCompact.add(segmentName);
      } else {
        validDataSize.put(segmentName, allDataSize.get(segmentName));
      }
    }
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, logSegmentToCompact, null), blobStore,
        compactionPolicy);

    // case 5: none of the log segment has 100% valid data
    logSegmentToCompact = new ArrayList<>();
    for (int i = 0; i < blobStore.logSegmentsNotInJournal.size(); i++) {
      LogSegmentName segmentName = blobStore.logSegmentsNotInJournal.get(i);
      validDataSize.put(segmentName, allDataSize.get(segmentName) / 2);
      logSegmentToCompact.add(segmentName);
    }
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, logSegmentToCompact, null), blobStore,
        compactionPolicy);
  }
}
