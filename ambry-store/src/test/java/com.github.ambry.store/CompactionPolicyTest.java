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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Unit tests different {@link CompactionPolicy}
 */
@RunWith(Parameterized.class)
public class CompactionPolicyTest {

  final static long CAPACITY_IN_BYTES = 10 * 1024 * 1024;
  private static final long SEGMENT_CAPACITY_IN_BYTES = CAPACITY_IN_BYTES / 10;
  private static final long DEFAULT_USED_CAPACITY_IN_BYTES = CAPACITY_IN_BYTES * 6 / 10;
  private static final long DEFAULT_MAX_BLOB_SIZE = CAPACITY_IN_BYTES / 100;
  private static final long SEGMENT_HEADER_SIZE = CAPACITY_IN_BYTES / 50;

  // the properties that will used to generate a StoreConfig. Clear before use if required.
  private final Properties properties = new Properties();
  private Time time;
  private MockBlobStore blobStore;
  private StoreConfig config;
  private long messageRetentionTimeInMs;
  private MockBlobStoreStats mockBlobStoreStats;
  private CompactionPolicy compactionPolicy;

  /**
   * Running for both CompactAll and Default Compaction Policy
   * @return an array with both {@code CompactionPolicyType.DEFAULT} and {@code CompactionPolicyType.COMPACT_ALL}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{StatsBasedCompactionPolicyFactory.class.getName()}, {CompactAllPolicyFactory.class.getName()}});
  }

  /**
   * Instantiates {@link CompactionPolicyTest} with the required cast
   * @throws InterruptedException
   */
  public CompactionPolicyTest(String compactionPolicyFactoryStr) throws Exception {
    time = new MockTime();
    properties.setProperty("store.compaction.policy.factory", compactionPolicyFactoryStr);
    Pair<MockBlobStore, StoreConfig> initState = initializeBlobStore(properties, time, -1, -1, DEFAULT_MAX_BLOB_SIZE);
    config = initState.getSecond();
    messageRetentionTimeInMs = TimeUnit.DAYS.toMillis(config.storeDeletedMessageRetentionDays);
    blobStore = initState.getFirst();
    mockBlobStoreStats = blobStore.getBlobStoreStats();
    CompactionPolicyFactory compactionPolicyFactory = Utils.getObj(compactionPolicyFactoryStr, config, time);
    compactionPolicy = compactionPolicyFactory.getCompactionPolicy();
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for {@link BlobStore}'s
   * used capacity
   * @throws StoreException
   */
  @Test
  public void testDifferentUsedCapacities() throws StoreException {
    List<String> bestCandidates = null;
    if (compactionPolicy instanceof StatsBasedCompactionPolicy) {
      bestCandidates = setUpStateForStatsBasedCompactionPolicy(blobStore, mockBlobStoreStats);
    } else if (compactionPolicy instanceof CompactAllPolicy) {
      blobStore.logSegmentsNotInJournal = generateRandomLogSegmentName(3);
      bestCandidates = blobStore.logSegmentsNotInJournal;
    }

    // if used capacity is <= 60%, compaction details will be null. If not, logSegmentsNotInJournal needs to be returned.
    Long[] usedCapacities =
        new Long[]{CAPACITY_IN_BYTES * 2 / 10, (CAPACITY_IN_BYTES * 4 / 10), CAPACITY_IN_BYTES * 5 / 10,
            CAPACITY_IN_BYTES * 51 / 100, (CAPACITY_IN_BYTES * 6 / 10), CAPACITY_IN_BYTES * 7 / 10,
            CAPACITY_IN_BYTES * 9 / 10};
    for (Long usedCapacity : usedCapacities) {
      blobStore.usedCapacity = usedCapacity;
      if (blobStore.usedCapacity < (config.storeMinUsedCapacityToTriggerCompactionInPercentage / 100.0
          * blobStore.capacityInBytes)) {
        verifyCompactionDetails(null, blobStore, compactionPolicy);
      } else {
        verifyCompactionDetails(new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates),
            blobStore, compactionPolicy);
      }
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for
   * {@link StoreConfig#storeMinUsedCapacityToTriggerCompactionInPercentage}
   */
  @Test
  public void testDifferentThresholdsForMinLogSizeToCompact() throws StoreException, InterruptedException {

    int[] minLogSizeToTriggerCompactionInPercentages = new int[]{10, 20, 35, 40, 50, 59, 60, 61, 65, 70, 80, 95};
    // when used capacity(60%) is <= (minLogSize) % of total capacity, compactionDetails is expected to be null.
    // If not, logSegmentsNotInJournal needs to be returned
    List<String> bestCandidates = null;
    for (int minLogSize : minLogSizeToTriggerCompactionInPercentages) {
      initializeBlobStore(properties, time, minLogSize, -1, DEFAULT_MAX_BLOB_SIZE);
      if (compactionPolicy instanceof StatsBasedCompactionPolicy) {
        bestCandidates = setUpStateForStatsBasedCompactionPolicy(blobStore, mockBlobStoreStats);
      } else if (compactionPolicy instanceof CompactAllPolicy) {
        blobStore.logSegmentsNotInJournal = generateRandomLogSegmentName(3);
        bestCandidates = blobStore.logSegmentsNotInJournal;
      }
      if (blobStore.usedCapacity < (config.storeMinUsedCapacityToTriggerCompactionInPercentage / 100.0
          * blobStore.capacityInBytes)) {
        verifyCompactionDetails(null, blobStore, compactionPolicy);
      } else {
        verifyCompactionDetails(new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates),
            blobStore, compactionPolicy);
      }
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for
   * {@link StoreConfig#storeDeletedMessageRetentionDays}
   */
  @Test
  public void testDifferentMessageRetentionDays() throws StoreException, InterruptedException {
    List<String> bestCandidates = null;
    int[] messageRetentionDayValues = new int[]{1, 2, 3, 6, 9};
    for (int messageRetentionDays : messageRetentionDayValues) {
      time = new MockTime();
      Pair<MockBlobStore, StoreConfig> initState =
          initializeBlobStore(properties, time, -1, messageRetentionDays, DEFAULT_MAX_BLOB_SIZE);
      if (compactionPolicy instanceof StatsBasedCompactionPolicy) {
        bestCandidates = setUpStateForStatsBasedCompactionPolicy(blobStore, mockBlobStoreStats);
        compactionPolicy = new StatsBasedCompactionPolicy(initState.getSecond(), time);
      } else if (compactionPolicy instanceof CompactAllPolicy) {
        blobStore.logSegmentsNotInJournal = generateRandomLogSegmentName(3);
        bestCandidates = blobStore.logSegmentsNotInJournal;
        compactionPolicy = new CompactAllPolicy(initState.getSecond(), time);
      }
      verifyCompactionDetails(
          new CompactionDetails(time.milliseconds() - TimeUnit.DAYS.toMillis(messageRetentionDays), bestCandidates),
          blobStore, compactionPolicy);
    }
  }

  // helper methods

  /**
   * Initializes {@link BlobStore}
   * @param minLogSizeToTriggerCompactionInPercentage Property value to be set for
   * {@link StoreConfig#storeMinUsedCapacityToTriggerCompactionInPercentage}
   * @param messageRetentionInDays Property value to be set for {@link StoreConfig#storeDeletedMessageRetentionDays
   * @throws InterruptedException
   */
  static Pair<MockBlobStore, StoreConfig> initializeBlobStore(Properties properties, Time time,
      int minLogSizeToTriggerCompactionInPercentage, int messageRetentionInDays, long maxBlobSize)
      throws InterruptedException {

    if (minLogSizeToTriggerCompactionInPercentage != -1) {
      properties.setProperty("store.min.log.size.to.trigger.compaction.in.percent",
          String.valueOf(minLogSizeToTriggerCompactionInPercentage));
    }
    if (messageRetentionInDays != -1) {
      properties.setProperty("store.deleted.message.retention.days", String.valueOf(messageRetentionInDays));
    }
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    time.sleep(2 * TimeUnit.DAYS.toMillis(config.storeDeletedMessageRetentionDays));
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    MockBlobStoreStats mockBlobStoreStats = new MockBlobStoreStats(maxBlobSize);
    MockBlobStore blobStore =
        new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, SEGMENT_CAPACITY_IN_BYTES, SEGMENT_HEADER_SIZE,
            DEFAULT_USED_CAPACITY_IN_BYTES, mockBlobStoreStats);
    return new Pair<>(blobStore, config);
  }

  /**
   * Sets up the state (like logSegemntsNotInJournal and valid data size map) for
   * {@link StatsBasedCompactionPolicy}
   * @return a {@link List} of log segment names referring to the best candidate to compact
   */
  private List<String> setUpStateForStatsBasedCompactionPolicy(MockBlobStore blobStore,
      MockBlobStoreStats mockBlobStoreStats) {
    long maxLogSegmentCapacity =
        blobStore.segmentCapacity - blobStore.segmentHeaderSize - mockBlobStoreStats.getMaxBlobSize();
    long logSegmentCount = blobStore.capacityInBytes / blobStore.segmentCapacity;
    blobStore.logSegmentsNotInJournal = generateRandomLogSegmentName((int) logSegmentCount);

    int bestCandidateRange = 3 + TestUtils.RANDOM.nextInt((int) logSegmentCount - 2);
    int bestCandidateStartIndex = TestUtils.RANDOM.nextInt((int) logSegmentCount - bestCandidateRange + 1);
    int bestCandidateEndIndex = bestCandidateStartIndex + bestCandidateRange - 1;

    List<String> bestCandidates =
        blobStore.logSegmentsNotInJournal.subList(bestCandidateStartIndex, bestCandidateEndIndex + 1);
    long bestCost = maxLogSegmentCapacity / bestCandidates.size();
    // this best cost is to ensure that no of segments reclaimed will be "bestCandidates" count - 1
    NavigableMap<String, Long> validDataSize =
        generateValidDataSize(blobStore.logSegmentsNotInJournal, bestCandidates, bestCost, maxLogSegmentCapacity);
    mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
    return bestCandidates;
  }

  /**
   * Generates random log segment names
   * @param count the total number of random log segment names that needs to be generated
   * @return a {@link List} of random log segment name of size {@code count}
   */
  static List<String> generateRandomLogSegmentName(int count) {
    List<String> randomLogSegmentNames = new ArrayList<>();
    while (randomLogSegmentNames.size() < count) {
      String logSegmentName =
          LogSegmentNameHelper.getName(TestUtils.RANDOM.nextInt(count * 1000), TestUtils.RANDOM.nextInt(count * 1000));
      if (!randomLogSegmentNames.contains(logSegmentName)) {
        randomLogSegmentNames.add(logSegmentName);
      }
    }
    Collections.sort(randomLogSegmentNames, LogSegmentNameHelper.COMPARATOR);
    return randomLogSegmentNames;
  }

  /**
   * Generates valid data size map for given segments
   * @param logSegmentNames {@link List<String>} of log segment names
   * @param bestCandidates {@link List<String>} of best candidates
   * @param validDataSizeForBest valid data size to be set for best candidate
   * @return a {@link NavigableMap} of log segment name to valid data size
   */
  static NavigableMap<String, Long> generateValidDataSize(List<String> logSegmentNames, List<String> bestCandidates,
      long validDataSizeForBest, long maxLogSegmentCapacity) {
    NavigableMap<String, Long> validDataSize = new TreeMap<>(LogSegmentNameHelper.COMPARATOR);
    for (String logSegmentName : logSegmentNames) {
      if (bestCandidates.contains(logSegmentName)) {
        validDataSize.put(logSegmentName, validDataSizeForBest);
      } else {
        validDataSize.put(logSegmentName, maxLogSegmentCapacity);
      }
    }
    return validDataSize;
  }

  /**
   * Update validDataSize for some log segments
   * @param validDataSizePerLogSegment {@link NavigableMap<String,Long>} containing the valid data size
   * @param logSegmentsToUpdate log segments to be updated
   * @param newValidDataSize new valid data size that needs to be updated
   */
  static void updateValidDataSize(NavigableMap<String, Long> validDataSizePerLogSegment,
      List<String> logSegmentsToUpdate, long newValidDataSize) {
    for (String logSegmentName : logSegmentsToUpdate) {
      validDataSizePerLogSegment.put(logSegmentName, newValidDataSize);
    }
  }

  // verification helper methods

  /**
   * Verifies {@link BlobStore#getCompactionDetails(CompactionPolicy)} returns expected values i.e. {@code expectedCompactionDetails}
   * @throws StoreException
   */
  static void verifyCompactionDetails(CompactionDetails expectedCompactionDetails, BlobStore blobStore,
      CompactionPolicy compactionPolicy) throws StoreException {
    CompactionDetails compactionDetails = blobStore.getCompactionDetails(compactionPolicy);
    if (expectedCompactionDetails == null) {
      assertNull("CompactionDetails expected to be null ", compactionDetails);
    } else {
      assertEquals("Returned invalid time ", expectedCompactionDetails.getReferenceTimeMs(),
          compactionDetails.getReferenceTimeMs());
      assertEquals("Compaction range mismatch ", expectedCompactionDetails.getLogSegmentsUnderCompaction(),
          compactionDetails.getLogSegmentsUnderCompaction());
    }
  }
}

/**
 * MockBlobStore to assist in testing {@link CompactionManager}
 */
class MockBlobStore extends BlobStore {
  long usedCapacity;
  long segmentCapacity;
  long segmentHeaderSize;
  long capacityInBytes;
  List<String> logSegmentsNotInJournal = null;
  MockBlobStoreStats mockBlobStoreStats;

  MockBlobStore(StoreConfig config, StoreMetrics metrics, Time time, long capacityInBytes, long segmentCapacity,
      long segmentHeaderSize, long usedCapacity, MockBlobStoreStats mockBlobStoreStats) {
    super(StoreTestUtils.createMockReplicaId("", 0, null), config, null, null, null, null, metrics, metrics, null, null,
        null, null, time);
    this.capacityInBytes = capacityInBytes;
    this.segmentCapacity = segmentCapacity;
    this.segmentHeaderSize = segmentHeaderSize;
    this.usedCapacity = usedCapacity;
    this.mockBlobStoreStats = mockBlobStoreStats;
  }

  @Override
  CompactionDetails getCompactionDetails(CompactionPolicy compactionPolicy) throws StoreException {
    return compactionPolicy.getCompactionDetails(capacityInBytes, usedCapacity, segmentCapacity, segmentHeaderSize,
        logSegmentsNotInJournal, mockBlobStoreStats);
  }

  MockBlobStoreStats getBlobStoreStats() {
    return mockBlobStoreStats;
  }
}

/**
 * Mock {@link BlobStoreStats} for test purposes
 */
class MockBlobStoreStats extends BlobStoreStats {

  NavigableMap<String, Long> validDataSizeByLogSegments;
  private long maxBlobSize;

  MockBlobStoreStats(long maxBlobSize) {
    super("", null, 0, 0, 0, 0, 0, null, null, null, null, null);
    this.maxBlobSize = maxBlobSize;
  }

  @Override
  Pair<Long, NavigableMap<String, Long>> getValidDataSizeByLogSegment(TimeRange timeRange) throws StoreException {
    long deleteReferenceTimeInMs = timeRange.getEndTimeInMs();
    if (validDataSizeByLogSegments != null) {
      return new Pair<>(deleteReferenceTimeInMs, validDataSizeByLogSegments);
    } else {
      return null;
    }
  }

  @Override
  long getMaxBlobSize() {
    return maxBlobSize;
  }
}
