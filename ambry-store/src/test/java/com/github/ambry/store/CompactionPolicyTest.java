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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final String MOUNT_PATH = "/tmp/";
  private final Path dataDirPath;
  private static final Logger logger = LoggerFactory.getLogger(CompactionPolicyTest.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String COMPACT_POLICY_INFO_FILE_NAME_V2 = "compactionPolicyInfoV2.json";

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
    messageRetentionTimeInMs = TimeUnit.HOURS.toMillis(config.storeDeletedMessageRetentionHours);
    blobStore = initState.getFirst();
    mockBlobStoreStats = blobStore.getBlobStoreStats();
    CompactionPolicyFactory compactionPolicyFactory = Utils.getObj(compactionPolicyFactoryStr, config, time);
    compactionPolicy = compactionPolicyFactory.getCompactionPolicy();
    dataDirPath = Paths.get(MOUNT_PATH).toAbsolutePath();
  }

  /**
   * Tests HybridCompactionPolicy which runs {@link StatsBasedCompactionPolicy} more frequently and {@link CompactAllPolicy} regularly.
   * @throws StoreException
   */
  @Test
  public void testHybridCompactionPolicy() throws StoreException, IOException {
    cleanupBackupFiles();
    // with compaction enabled.
    properties.setProperty("store.compaction.triggers", "Periodic");
    properties.setProperty("store.compaction.policy.switch.timestamp.days", "6");
    properties.setProperty("store.compaction.policy.factory", "com.github.ambry.store.HybridCompactionPolicyFactory");
    config = new StoreConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    CompactionManager compactionManager = new CompactionManager(MOUNT_PATH, config, Collections.singleton(blobStore),
        new StorageManagerMetrics(metricRegistry), time);
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    MockBlobStoreStats mockBlobStoreStats = new MockBlobStoreStats(DEFAULT_MAX_BLOB_SIZE, 1);
    MockBlobStore blobStore =
        new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, SEGMENT_CAPACITY_IN_BYTES, SEGMENT_HEADER_SIZE,
            DEFAULT_USED_CAPACITY_IN_BYTES, mockBlobStoreStats);
    String blobId = mockBlobStoreStats.getStoreId();
    File tmpDir = new File(MOUNT_PATH + blobId);
    if (!tmpDir.exists()) {
      tmpDir.mkdir();
    }
    compactionManager.getCompactionDetails(blobStore);
    File compactionPolicyInfoFile = new File(tmpDir.toString(), COMPACT_POLICY_INFO_FILE_NAME_V2);
    CompactionPolicySwitchInfo compactionPolicySwitchInfo =
        objectMapper.readValue(compactionPolicyInfoFile, CompactionPolicySwitchInfo.class);
    assertFalse("Next round of compaction is not compactAll", compactionPolicySwitchInfo.isNextRoundCompactAllPolicy());
    //set last compactAll kick off time to current time to trigger compactAllPolicy
    compactionPolicySwitchInfo.setLastCompactAllTime(
        compactionPolicySwitchInfo.getLastCompactAllTime() - TimeUnit.DAYS.toMillis(
            config.storeCompactionPolicySwitchTimestampDays));
    ((HybridCompactionPolicy) compactionManager.getCompactionPolicy()).getBlobToCompactionPolicySwitchInfoMap()
        .put(mockBlobStoreStats.getStoreId(), compactionPolicySwitchInfo);
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(compactionPolicyInfoFile, compactionPolicySwitchInfo);
    compactionManager.getCompactionDetails(blobStore);
    compactionPolicySwitchInfo = objectMapper.readValue(compactionPolicyInfoFile, CompactionPolicySwitchInfo.class);
    assertTrue("Next round of compaction is not compactAll", compactionPolicySwitchInfo.isNextRoundCompactAllPolicy());

    //add one more round to check if compaction will be recovered from backup file
    ((HybridCompactionPolicy) compactionManager.getCompactionPolicy()).getBlobToCompactionPolicySwitchInfoMap().clear();
    compactionManager.getCompactionDetails(blobStore);
    compactionPolicySwitchInfo = objectMapper.readValue(compactionPolicyInfoFile, CompactionPolicySwitchInfo.class);
    assertFalse("Next round of compaction is not compactAll", compactionPolicySwitchInfo.isNextRoundCompactAllPolicy());
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for {@link BlobStore}'s
   * used capacity
   * @throws StoreException
   */
  @Test
  public void testDifferentUsedCapacities() throws StoreException {
    List<LogSegmentName> bestCandidates = null;
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
        verifyCompactionDetails(
            new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates, null), blobStore,
            compactionPolicy);
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
    List<LogSegmentName> bestCandidates = null;
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
        verifyCompactionDetails(
            new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates, null), blobStore,
            compactionPolicy);
      }
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for
   * {@link StoreConfig#storeDeletedMessageRetentionHours}
   */
  @Test
  public void testDifferentMessageRetentionDays() throws StoreException, InterruptedException {
    List<LogSegmentName> bestCandidates = null;
    int[] messageRetentionHoursValues = new int[]{1, 2, 3, 6, 9};
    for (int messageRetentionHours : messageRetentionHoursValues) {
      time = new MockTime();
      Pair<MockBlobStore, StoreConfig> initState =
          initializeBlobStore(properties, time, -1, messageRetentionHours, DEFAULT_MAX_BLOB_SIZE);
      if (compactionPolicy instanceof StatsBasedCompactionPolicy) {
        bestCandidates = setUpStateForStatsBasedCompactionPolicy(blobStore, mockBlobStoreStats);
        compactionPolicy = new StatsBasedCompactionPolicy(initState.getSecond(), time);
      } else if (compactionPolicy instanceof CompactAllPolicy) {
        blobStore.logSegmentsNotInJournal = generateRandomLogSegmentName(3);
        bestCandidates = blobStore.logSegmentsNotInJournal;
        compactionPolicy = new CompactAllPolicy(initState.getSecond(), time);
      }
      verifyCompactionDetails(
          new CompactionDetails(time.milliseconds() - TimeUnit.HOURS.toMillis(messageRetentionHours), bestCandidates,
              null), blobStore, compactionPolicy);
    }
  }

  // helper methods

  /**
   * Initializes {@link BlobStore}
   * @param minLogSizeToTriggerCompactionInPercentage Property value to be set for
   * {@link StoreConfig#storeMinUsedCapacityToTriggerCompactionInPercentage}
   * @param messageRetentionInHours Property value to be set for {@link StoreConfig#storeDeletedMessageRetentionHours
   * @throws InterruptedException
   */
  static Pair<MockBlobStore, StoreConfig> initializeBlobStore(Properties properties, Time time,
      int minLogSizeToTriggerCompactionInPercentage, int messageRetentionInHours, long maxBlobSize)
      throws InterruptedException {

    if (minLogSizeToTriggerCompactionInPercentage != -1) {
      properties.setProperty("store.min.log.size.to.trigger.compaction.in.percent",
          String.valueOf(minLogSizeToTriggerCompactionInPercentage));
    }
    if (messageRetentionInHours != -1) {
      properties.setProperty("store.deleted.message.retention.hours", String.valueOf(messageRetentionInHours));
    }
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    time.sleep(10 * TimeUnit.HOURS.toMillis(config.storeDeletedMessageRetentionHours));
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    MockBlobStoreStats mockBlobStoreStats = new MockBlobStoreStats(maxBlobSize);
    MockBlobStore blobStore =
        new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, SEGMENT_CAPACITY_IN_BYTES, SEGMENT_HEADER_SIZE,
            DEFAULT_USED_CAPACITY_IN_BYTES, mockBlobStoreStats);
    return new Pair<>(blobStore, config);
  }

  /**
   * Clean up the backup files.
   */
  private void cleanupBackupFiles() {
    if (dataDirPath == null) {
      return;
    }
    File backupDir = dataDirPath.toFile();

    // First get all the file with temp file suffix and remove all of them
    FileFilter tempFileFilter = (File pathname) -> pathname.getName().startsWith("storeId");
    File[] files = backupDir.listFiles(tempFileFilter);
    if (files != null) {
      for (File file : files) {
        File deleteFile = new File(MOUNT_PATH + file.getName(), "compactionPolicyInfo.json");
        logger.trace("Delete temp file {}", deleteFile.getName());
        deleteFile(deleteFile.toPath());
      }
    }
  }

  /**
   * Delete the files.
   * @param toDelete files need to be deleted.
   */
  private void deleteFile(Path toDelete) {
    try {
      Files.delete(toDelete);
    } catch (NoSuchFileException e) {
      logger.error("File doesn't exist while deleting: {}", toDelete.toString(), e);
    } catch (IOException e) {
      logger.error("Encounter an I/O error while deleting file: {}", toDelete.toString(), e);
    } catch (Exception e) {
      logger.error("Encounter an unexpected error while deleting file: {}", toDelete.toString(), e);
    }
  }

  /**
   * Sets up the state (like logSegemntsNotInJournal and valid data size map) for
   * {@link StatsBasedCompactionPolicy}
   * @return a {@link List} of log segment names referring to the best candidate to compact
   */
  private List<LogSegmentName> setUpStateForStatsBasedCompactionPolicy(MockBlobStore blobStore,
      MockBlobStoreStats mockBlobStoreStats) {
    long maxLogSegmentCapacity =
        blobStore.segmentCapacity - blobStore.segmentHeaderSize - mockBlobStoreStats.getMaxBlobSize();
    long logSegmentCount = blobStore.capacityInBytes / blobStore.segmentCapacity;
    blobStore.logSegmentsNotInJournal = generateRandomLogSegmentName((int) logSegmentCount);

    int bestCandidateRange = 3 + TestUtils.RANDOM.nextInt((int) logSegmentCount - 2);
    int bestCandidateStartIndex = TestUtils.RANDOM.nextInt((int) logSegmentCount - bestCandidateRange + 1);
    int bestCandidateEndIndex = bestCandidateStartIndex + bestCandidateRange - 1;

    List<LogSegmentName> bestCandidates =
        blobStore.logSegmentsNotInJournal.subList(bestCandidateStartIndex, bestCandidateEndIndex + 1);
    long bestCost = maxLogSegmentCapacity / bestCandidates.size();
    // this best cost is to ensure that no of segments reclaimed will be "bestCandidates" count - 1
    NavigableMap<LogSegmentName, Long> validDataSize =
        generateValidDataSize(blobStore.logSegmentsNotInJournal, bestCandidates, bestCost, maxLogSegmentCapacity);
    mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
    return bestCandidates;
  }

  /**
   * Generates random log segment names
   * @param count the total number of random log segment names that needs to be generated
   * @return a {@link List} of random log segment name of size {@code count}
   */
  static List<LogSegmentName> generateRandomLogSegmentName(int count) {
    List<LogSegmentName> randomLogSegmentNames = new ArrayList<>();
    while (randomLogSegmentNames.size() < count) {
      LogSegmentName logSegmentName = StoreTestUtils.getRandomLogSegmentName(randomLogSegmentNames);
      randomLogSegmentNames.add(logSegmentName);
    }
    Collections.sort(randomLogSegmentNames);
    return randomLogSegmentNames;
  }

  /**
   * Generates valid data size map for given segments
   * @param logSegmentNames {@link List<String>} of log segment names
   * @param bestCandidates {@link List<String>} of best candidates
   * @param validDataSizeForBest valid data size to be set for best candidate
   * @return a {@link NavigableMap} of log segment name to valid data size
   */
  static NavigableMap<LogSegmentName, Long> generateValidDataSize(List<LogSegmentName> logSegmentNames,
      List<LogSegmentName> bestCandidates, long validDataSizeForBest, long maxLogSegmentCapacity) {
    NavigableMap<LogSegmentName, Long> validDataSize = new TreeMap<>();
    for (LogSegmentName logSegmentName : logSegmentNames) {
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
  static void updateValidDataSize(NavigableMap<LogSegmentName, Long> validDataSizePerLogSegment,
      List<LogSegmentName> logSegmentsToUpdate, long newValidDataSize) {
    for (LogSegmentName logSegmentName : logSegmentsToUpdate) {
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
  List<LogSegmentName> logSegmentsNotInJournal = null;
  MockBlobStoreStats mockBlobStoreStats;

  MockBlobStore(StoreConfig config, StoreMetrics metrics, Time time, long capacityInBytes, long segmentCapacity,
      long segmentHeaderSize, long usedCapacity, MockBlobStoreStats mockBlobStoreStats) {
    super(StoreTestUtils.createMockReplicaId(mockBlobStoreStats.getStoreId(), 0,
        "/tmp/" + mockBlobStoreStats.getStoreId() + "/"), config, null, null, null, null, metrics, metrics, null, null,
        null, null, time, new InMemAccountService(false, false), null);
    this.capacityInBytes = capacityInBytes;
    this.segmentCapacity = segmentCapacity;
    this.segmentHeaderSize = segmentHeaderSize;
    this.usedCapacity = usedCapacity;
    this.mockBlobStoreStats = mockBlobStoreStats;
  }

  @Override
  CompactionDetails getCompactionDetails(CompactionPolicy compactionPolicy) throws StoreException {
    return compactionPolicy.getCompactionDetails(capacityInBytes, usedCapacity, segmentCapacity, segmentHeaderSize,
        logSegmentsNotInJournal, mockBlobStoreStats, "/tmp/" + mockBlobStoreStats.getStoreId() + "/");
  }

  MockBlobStoreStats getBlobStoreStats() {
    return mockBlobStoreStats;
  }
}

/**
 * Mock {@link BlobStoreStats} for test purposes
 */
class MockBlobStoreStats extends BlobStoreStats {
  private final long maxBlobSize;

  NavigableMap<LogSegmentName, Long> validDataSizeByLogSegments;
  private String storeId = "";

  MockBlobStoreStats(long maxBlobSize) {
    super("", null, 0, 0, 0, 0, 0, true, true, null, null, null, null, null, 1, false);
    this.maxBlobSize = maxBlobSize;
  }

  MockBlobStoreStats(long maxBlobSize, int i) {
    super("storeId" + i, null, 0, 0, 0, 0, 0, true, true, null, null, null, null, null, 1, false);
    this.storeId = "storeId" + i;
    this.maxBlobSize = maxBlobSize;
  }

  @Override
  public long getMaxBlobSize() {
    return maxBlobSize;
  }

  @Override
  Pair<Long, NavigableMap<LogSegmentName, Long>> getValidDataSizeByLogSegment(TimeRange timeRange) {
    long deleteReferenceTimeInMs = timeRange.getEndTimeInMs();
    if (validDataSizeByLogSegments != null) {
      return new Pair<Long, NavigableMap<LogSegmentName, Long>>(deleteReferenceTimeInMs, validDataSizeByLogSegments);
    } else {
      return null;
    }
  }

  String getStoreId() {
    return this.storeId;
  }
}
