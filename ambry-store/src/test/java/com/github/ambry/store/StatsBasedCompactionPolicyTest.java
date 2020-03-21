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
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static com.github.ambry.store.CompactionPolicyTest.*;


public class StatsBasedCompactionPolicyTest {

  private static long DEFAULT_MAX_BLOB_SIZE = CAPACITY_IN_BYTES / 100;
  // the properties that will used to generate a StoreConfig. Clear before use if required.
  private final Properties properties = new Properties();
  private final Time time = new MockTime();
  private StoreConfig config;
  private MockBlobStore blobStore;
  private long messageRetentionTimeInMs;
  private MockBlobStoreStats mockBlobStoreStats;
  private CompactionPolicy compactionPolicy = null;

  /**
   * Instantiates {@link CompactionPolicyTest} with the required cast
   * @throws InterruptedException
   */
  public StatsBasedCompactionPolicyTest() throws InterruptedException {
    Pair<MockBlobStore, StoreConfig> initState =
        CompactionPolicyTest.initializeBlobStore(properties, time, -1, -1, DEFAULT_MAX_BLOB_SIZE);
    config = initState.getSecond();
    blobStore = initState.getFirst();
    mockBlobStoreStats = blobStore.getBlobStoreStats();
    messageRetentionTimeInMs = TimeUnit.DAYS.toMillis(config.storeDeletedMessageRetentionDays);
    compactionPolicy = new StatsBasedCompactionPolicy(config, time);
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)}
   * @throws StoreException
   * @throws InterruptedException
   */
  @Test
  public void testGetCompactionDetailsTest() throws StoreException, InterruptedException {
    // null logSegmentsNotInJournal to mimic all log segments overlapping with journal
    blobStore.logSegmentsNotInJournal = null;
    CompactionPolicyTest.verifyCompactionDetails(null, blobStore, compactionPolicy);

    // normal case where in some log segments are non overlapping with journal
    long maxLogSegmentCapacity = blobStore.segmentCapacity - blobStore.segmentHeaderSize - DEFAULT_MAX_BLOB_SIZE;
    long logSegmentCount = blobStore.capacityInBytes / blobStore.segmentCapacity;
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName((int) logSegmentCount);
    for (int k = 0; k < 5; k++) {
      for (int i = 0; i < logSegmentCount; i++) {
        for (int j = i; j < logSegmentCount; j++) {
          List<String> bestCandidates = blobStore.logSegmentsNotInJournal.subList(i, j + 1);
          long bestCost = maxLogSegmentCapacity / bestCandidates.size();
          // this best cost is to ensure that no of segments reclaimed will be "bestCandidates" count - 1
          NavigableMap<String, Long> validDataSize =
              CompactionPolicyTest.generateValidDataSize(blobStore.logSegmentsNotInJournal, bestCandidates, bestCost,
                  maxLogSegmentCapacity);
          mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
          if ((j - i) >= config.storeMinLogSegmentCountToReclaimToTriggerCompaction) {
            CompactionPolicyTest.verifyCompactionDetails(
                new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates), blobStore,
                compactionPolicy);
            Pair<Integer, Integer> bestCandidateIndexes = new Pair<>(i, j);
            Pair<Integer, Integer> secondBestCandidateIndexes =
                findNonOverLappingCandidate(bestCandidateIndexes, (int) logSegmentCount - 1);
            if (secondBestCandidateIndexes != null) {
              // set cost benefit of 2nd best candidate to 1 more than best candidate's cost benefit ratio.
              CompactionPolicyTest.updateValidDataSize(validDataSize,
                  blobStore.logSegmentsNotInJournal.subList(secondBestCandidateIndexes.getFirst(),
                      secondBestCandidateIndexes.getSecond() + 1), bestCost + 1);
              List<String> secondBestCandidate =
                  blobStore.logSegmentsNotInJournal.subList(secondBestCandidateIndexes.getFirst(),
                      secondBestCandidateIndexes.getSecond() + 1);
              mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
              // ensure that the best remains the best as it has the lowest cost benefit ratio
              CompactionPolicyTest.verifyCompactionDetails(
                  new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates), blobStore,
                  compactionPolicy);
              // set cost benefit of 2nd best to 1 less than the best candidate's cost benefit ratio
              CompactionPolicyTest.updateValidDataSize(validDataSize,
                  blobStore.logSegmentsNotInJournal.subList(secondBestCandidateIndexes.getFirst(),
                      secondBestCandidateIndexes.getSecond() + 1), bestCost - 1);
              mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
              // ensure that the 2nd best is now the best candidate to compact
              CompactionPolicyTest.verifyCompactionDetails(
                  new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, secondBestCandidate), blobStore,
                  compactionPolicy);
            }
          } else {
            CompactionPolicyTest.verifyCompactionDetails(null, blobStore, compactionPolicy);
          }
        }
      }
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} with 0 cost for the best candidate
   * @throws StoreException
   * @throws InterruptedException
   */
  @Test
  public void testGetCompactionDetailsZeroCostTest() throws StoreException, InterruptedException {
    long maxLogSegmentCapacity = blobStore.segmentCapacity - blobStore.segmentHeaderSize - DEFAULT_MAX_BLOB_SIZE;
    long logSegmentCount = blobStore.capacityInBytes / blobStore.segmentCapacity;
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName((int) logSegmentCount);
    for (int i = 0; i < logSegmentCount; i++) {
      for (int j = i; j < logSegmentCount; j++) {
        List<String> bestCandidates = blobStore.logSegmentsNotInJournal.subList(i, j + 1);
        NavigableMap<String, Long> validDataSize =
            CompactionPolicyTest.generateValidDataSize(blobStore.logSegmentsNotInJournal, bestCandidates, 0,
                maxLogSegmentCapacity);
        mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
        CompactionPolicyTest.verifyCompactionDetails(
            new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates), blobStore,
            compactionPolicy);
      }
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different cases for
   * logSegmentsNotInJournal
   */
  @Test
  public void testLogSegmentsNotInJournal() throws StoreException {
    long maxLogSegmentCapacity = blobStore.segmentCapacity - blobStore.segmentHeaderSize - DEFAULT_MAX_BLOB_SIZE;
    long logSegmentCount = blobStore.capacityInBytes / blobStore.segmentCapacity;
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName((int) logSegmentCount);

    // case 1: 1st best(index_0 to index_3) is in journal, 2nd not in journal
    List<String> bestCandidates = blobStore.logSegmentsNotInJournal.subList(0, 4);
    long bestCost = maxLogSegmentCapacity / bestCandidates.size();
    // this best cost is to ensure that no of segments reclaimed will be "bestCandidates" count - 1
    NavigableMap<String, Long> validDataSize =
        CompactionPolicyTest.generateValidDataSize(blobStore.logSegmentsNotInJournal, bestCandidates, bestCost,
            maxLogSegmentCapacity);
    mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates), blobStore,
        compactionPolicy);
    Pair<Integer, Integer> bestCandidateIndexes = new Pair<>(0, 3);
    Pair<Integer, Integer> secondBestCandidateIndexes =
        findNonOverLappingCandidate(bestCandidateIndexes, (int) logSegmentCount - 1);
    // set cost benefit of 2nd best to 1 less than the best candidate's cost benefit ratio
    CompactionPolicyTest.updateValidDataSize(validDataSize,
        blobStore.logSegmentsNotInJournal.subList(secondBestCandidateIndexes.getFirst(),
            secondBestCandidateIndexes.getSecond() + 1), bestCost - 1);
    mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
    // ensure 2nd best's log segments are in journal
    blobStore.logSegmentsNotInJournal = bestCandidates;
    // ensure that the 1st best is now the best candidate to compact even though 2nd best's cost benefit ratio
    // is the lowest
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates), blobStore,
        compactionPolicy);

    // case 2: only one potential candidate(index_5 to index_7) to compact which is overlaps with the journal
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName((int) logSegmentCount);
    bestCandidates = blobStore.logSegmentsNotInJournal.subList(5, 8);
    bestCost = maxLogSegmentCapacity / bestCandidates.size();
    // this best cost is to ensure that no of segments reclaimed will be "bestCandidates" count - 1
    validDataSize =
        CompactionPolicyTest.generateValidDataSize(blobStore.logSegmentsNotInJournal, bestCandidates, bestCost,
            maxLogSegmentCapacity);
    mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
    blobStore.logSegmentsNotInJournal = blobStore.logSegmentsNotInJournal.subList(0, 4);
    CompactionPolicyTest.verifyCompactionDetails(null, blobStore, compactionPolicy);

    // case 3: best candidate's first few segments(index_2 to index7) are non overlapping and the rest are overlapping
    // with journal
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName((int) logSegmentCount);
    bestCandidates = blobStore.logSegmentsNotInJournal.subList(2, 8);
    bestCost = maxLogSegmentCapacity / bestCandidates.size();
    validDataSize =
        CompactionPolicyTest.generateValidDataSize(blobStore.logSegmentsNotInJournal, bestCandidates, bestCost,
            maxLogSegmentCapacity);
    mockBlobStoreStats.validDataSizeByLogSegments = validDataSize;
    blobStore.logSegmentsNotInJournal = blobStore.logSegmentsNotInJournal.subList(0, 5);
    // expected best candidate = index2 to index_4 which is first 3 entries in the actual best candidate
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, bestCandidates.subList(0, 3)), blobStore,
        compactionPolicy);
  }

  /**
   * Find non overlapping candidates with the given candidate
   * @param candidateIndexes candidate indexes to compare against to find the non overlapping indexes
   * @param maxIndex maximum index of the non overlapping candidate to find
   * @return a {@link Pair} of Integers representing the indexes of the candidate that doesn't overlap with the
   * passed in {@code candidate}
   */
  private Pair<Integer, Integer> findNonOverLappingCandidate(Pair<Integer, Integer> candidateIndexes, int maxIndex) {
    int range = candidateIndexes.getSecond() - candidateIndexes.getFirst() + 1;
    // ensure best and 2nd best have atleast 1 segment between them. If not, both of them put together will become the best candidate
    if (candidateIndexes.getSecond() + range + 1 > maxIndex) {
      return null;
    } else {
      int maxStartIndex = maxIndex - range + 1;
      int minStartIndex = candidateIndexes.getSecond() + 2;
      int newStartIndex = minStartIndex + TestUtils.RANDOM.nextInt(maxStartIndex - minStartIndex + 1);
      return new Pair<>(newStartIndex, newStartIndex + range - 1);
    }
  }
}
