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

import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests for {@link BlobStoreStats}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class BlobStoreStatsTest {
  private static final long TEST_TIME_INTERVAL_IN_MS = CuratedLogIndexState.DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS / 2;
  private static final DiskIOScheduler DISK_IO_SCHEDULER = new DiskIOScheduler(null);
  private final CuratedLogIndexState state;
  private final File tempDir;
  private final BlobStoreStats blobStoreStats;

  /**
   * Running for both segmented and non-segmented log.
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Creates a temporary directory and sets up some test state.
   * @throws IOException
   */
  public BlobStoreStatsTest(boolean isLogSegmented) throws InterruptedException, IOException, StoreException {
    tempDir = StoreTestUtils.createTempDirectory("blobStoreStatsDir-" + UtilsTest.getRandomString(10));
    state = new CuratedLogIndexState(isLogSegmented, tempDir);
    blobStoreStats = new BlobStoreStats(state.index, state.time, DISK_IO_SCHEDULER);
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    state.destroy();
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Basic test to verify reported valid size information per container by BlobStoreStats.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testContainerValidDataSize() throws InterruptedException, StoreException, IOException {
    verifyAndGetContainerValidSize();
    // advance time
    state.advanceTime(TEST_TIME_INTERVAL_IN_MS);
    verifyAndGetContainerValidSize();
  }

  /**
   * Basic test to verify reported valid size information per log segment by BlobStoreStats.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testLogSegmentValidDataSize() throws InterruptedException, StoreException, IOException {
    for (long i = 0; i <= state.time.milliseconds() + TEST_TIME_INTERVAL_IN_MS; i += TEST_TIME_INTERVAL_IN_MS) {
      TimeRange timeRange = new TimeRange(i, 0L);
      verifyAndGetLogSegmentValidSize(timeRange);
    }
  }

  /**
   * Tests to verify the correctness of reported stats after new puts via the following steps:
   * 1. Verify reported stats and record the total valid size prior to adding the new puts.
   * 2. Add new puts.
   * 3. Verify reported stats and record the total valid size after new puts are added.
   * 4. Verify the delta of total valid size prior to adding the new puts and after matches with the expected delta.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterPuts() throws InterruptedException, StoreException, IOException {
    // advance time to the next second for deletes/expiration to take effect
    advanceTimeToNextSecond();
    long timeInMsBeforePuts = state.time.milliseconds();
    long totalLogSegmentValidSizeBeforePuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsBeforePuts, 0L));
    long totalContainerValidSizeBeforePuts = verifyAndGetContainerValidSize();

    // 3 puts
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    long timeInMsAfterPuts = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterPuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterPuts, 0L));
    long totalContainerValidSizeAfterPuts = verifyAndGetContainerValidSize();
    long expectedIncrement = 3 * CuratedLogIndexState.PUT_RECORD_SIZE;
    assertEquals("Put entries are not properly counted for log segment valid size", totalLogSegmentValidSizeAfterPuts,
        totalLogSegmentValidSizeBeforePuts + expectedIncrement);
    assertEquals("Put entries are not properly counted for container valid size", totalContainerValidSizeAfterPuts,
        totalContainerValidSizeBeforePuts + expectedIncrement);
  }

  /**
   * Tests to verify the correctness of reported stats with puts that is going to expire via the following steps:
   * 1. Verify reported stats and record the total valid size before adding the new expiring puts.
   * 2. Add new expiring and non-expiring puts.
   * 3. Verify the new puts are being reported correctly.
   * 4. Advance time to let the expiration take effect.
   * 5. Verify reported stats and record the total valid size after new puts are expired.
   * 6. Verify the reported total valid size difference before the new puts and after.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterExpiration() throws InterruptedException, StoreException, IOException {
    // advance time to the next second for previous deletes/expiration to take effect
    advanceTimeToNextSecond();
    long timeInMsBeforePuts = state.time.milliseconds();
    long totalLogSegmentValidSizeBeforePuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsBeforePuts, 0L));
    long totalContainerValidSizeBeforePuts = verifyAndGetContainerValidSize();

    // 1 put with no expiry
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 3 puts that will expire in 20 seconds (note the two puts should be in the same index segment)
    long expiresAtInMs = state.time.milliseconds() + 20 * Time.MsPerSec;
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, expiresAtInMs);

    // advance time to exactly the time of expiration, all new puts should still be valid
    state.advanceTime(expiresAtInMs - state.time.milliseconds());

    long expectedDeltaAfterPut = 4 * CuratedLogIndexState.PUT_RECORD_SIZE;
    long timeInMsAfterPuts = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterPuts = verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterPuts, 0L));
    long totalContainerValidSizeAfterPuts = verifyAndGetContainerValidSize();
    assertEquals("Put entries with expiry are not properly counted for log segment valid size",
        totalLogSegmentValidSizeAfterPuts, totalLogSegmentValidSizeBeforePuts + expectedDeltaAfterPut);
    assertEquals("Put entries with expiry are not properly counted for container valid size",
        totalContainerValidSizeAfterPuts, totalContainerValidSizeBeforePuts + expectedDeltaAfterPut);

    // advance time to the next second for expiration to take effect
    advanceTimeToNextSecond();

    long expectedDeltaAfterExpiration = CuratedLogIndexState.PUT_RECORD_SIZE;
    long timeInMsAfterExpiration = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterExpiration =
        verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterExpiration, 0L));
    long totalContainerValidSizeAfterExpiration = verifyAndGetContainerValidSize();
    assertEquals("Expired put entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeAfterExpiration, totalLogSegmentValidSizeBeforePuts + expectedDeltaAfterExpiration);
    assertEquals("Expired put entries are not properly counted for container valid size",
        totalContainerValidSizeAfterExpiration, totalContainerValidSizeBeforePuts + expectedDeltaAfterExpiration);
  }

  /**
   * Tests to verify the correctness of reported stats with after new deletes via the following steps:
   * 1. Add new puts that are going to be deleted later.
   * 2. Verify reported stats and record the total valid size before new deletes.
   * 3. Perform the deletes.
   * 4. Verify reported stats after the deletes but at a time point before the deletes are relevant.
   * 5. Verify reported stats and record the total valid size after the deletes.
   * 6. Verify the delta of total valid size prior to the new deletes and after matches with the expected delta.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterDeletes() throws InterruptedException, StoreException, IOException {
    // 5 puts to be deleted
    state.addPutEntries(5, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    TimeRange timeBeforeDeletes = new TimeRange(state.time.milliseconds(), 0L);
    long totalLogSegmentValidSizeBeforeDeletes = verifyAndGetLogSegmentValidSize(timeBeforeDeletes);
    long totalContainerValidSizeBeforeDeletes = verifyAndGetContainerValidSize();

    // advance time to the next seconds before adding the deletes
    advanceTimeToNextSecond();
    // 2 deletes from the last index segment
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));

    long expectedDeltaBeforeDeletesRelevant = 2 * CuratedLogIndexState.DELETE_RECORD_SIZE;
    long totalLogSegmentValidSizeBeforeDeletesRelevant = verifyAndGetLogSegmentValidSize(timeBeforeDeletes);
    long totalContainerValidSizeBeforeDeletesRelevant = verifyAndGetContainerValidSize();
    assertEquals("Delete entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeBeforeDeletesRelevant,
        totalLogSegmentValidSizeBeforeDeletes + expectedDeltaBeforeDeletesRelevant);
    assertEquals("Delete entries are not properly counted for container valid size",
        totalContainerValidSizeBeforeDeletesRelevant, totalContainerValidSizeBeforeDeletes);

    // advance time to the next second for deletes/expiration to take effect
    advanceTimeToNextSecond();

    long timeInMsAfterDeletes = state.time.milliseconds();
    long totalLogSegmentValidSizeAfterDeletes =
        verifyAndGetLogSegmentValidSize(new TimeRange(timeInMsAfterDeletes, 0L));
    long totalContainerValidSizeAfterDeletes = verifyAndGetContainerValidSize();
    long expectedLogSegmentDecrement =
        2 * (CuratedLogIndexState.PUT_RECORD_SIZE - CuratedLogIndexState.DELETE_RECORD_SIZE);
    long expectedContainerDecrement = 2 * CuratedLogIndexState.PUT_RECORD_SIZE;
    assertEquals("Delete entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeAfterDeletes, totalLogSegmentValidSizeBeforeDeletes - expectedLogSegmentDecrement);
    assertEquals("Delete entries are not properly counted for container valid size",
        totalContainerValidSizeAfterDeletes, totalContainerValidSizeBeforeDeletes - expectedContainerDecrement);
  }

  /**
   * Advance the time to the next nearest second. That is, 1 sec to 2 sec or 1001 ms to 2000ms.
   */
  private void advanceTimeToNextSecond() throws InterruptedException {
    long currentTimeInMs = state.time.milliseconds();
    state.advanceTime(Time.MsPerSec - currentTimeInMs % Time.MsPerSec);
  }

  /**
   * Verify the correctness of valid data size information per container returned by BlobStoreStats and return the
   * total valid data size of all containers.
   * @return the total valid data size of all containers (from all serviceIds)
   */
  private long verifyAndGetContainerValidSize() throws StoreException {
    long deleteAndExpirationRefTimeInMs = state.time.milliseconds();
    Map<String, Map<String, Long>> actualContainerValidSizeMap = blobStoreStats.getValidDataSizeByContainer();
    Map<String, Map<String, Long>> expectedContainerValidSizeMap =
        getValidSizeByContainer(deleteAndExpirationRefTimeInMs);
    long totalValidSize = 0L;

    for (Map.Entry<String, Map<String, Long>> expectedContainerValidSizeEntry : expectedContainerValidSizeMap.entrySet()) {
      String serviceId = expectedContainerValidSizeEntry.getKey();
      assertTrue("Expected serviceId: " + serviceId + " not found", actualContainerValidSizeMap.containsKey(serviceId));
      Map<String, Long> innerMap = expectedContainerValidSizeEntry.getValue();
      for (Map.Entry<String, Long> innerEntry : innerMap.entrySet()) {
        String containerId = innerEntry.getKey();
        assertTrue("Expected containerId: " + containerId + " not found in serviceId: " + serviceId,
            innerMap.containsKey(containerId));
        long expectedContainerValidSize = innerEntry.getValue();
        long actualContainerValidSize = actualContainerValidSizeMap.get(serviceId).get(containerId);
        assertEquals("Valid data size mismatch for serviceId: " + serviceId + " containerId: " + containerId,
            expectedContainerValidSize, actualContainerValidSize);
        totalValidSize += expectedContainerValidSize;
      }
      assertEquals("Mismatch in number of containerIds in serviceId: " + serviceId, innerMap.size(),
          actualContainerValidSizeMap.get(serviceId).size());
    }
    assertEquals("Mismatch in number of serviceIds", expectedContainerValidSizeMap.size(),
        actualContainerValidSizeMap.size());

    return totalValidSize;
  }

  /**
   * Verify the correctness of valid data size information per log segment returned by BlobStoreStats for a given
   * {@link TimeRange} and return the total valid data size of all log segments.
   * @param timeRange the {@link TimeRange} to be used for the verification
   * @return the total valid data size of all log segments
   */
  private long verifyAndGetLogSegmentValidSize(TimeRange timeRange) throws StoreException {
    Pair<Long, NavigableMap<String, Long>> actualLogSegmentValidSizeMap =
        blobStoreStats.getValidDataSizeByLogSegment(timeRange);
    long expiryReferenceTimeMs = state.time.milliseconds();
    assertTrue("Valid data size collection time should be in the range",
        timeRange.getStartTimeInMs() <= actualLogSegmentValidSizeMap.getFirst()
            && timeRange.getEndTimeInMs() >= actualLogSegmentValidSizeMap.getFirst());

    int expectedNumberOfLogSegments = 0;
    long expectedTotalLogSegmentValidSize = 0L;
    LogSegment logSegment = state.log.getFirstSegment();

    while (logSegment != null) {
      String logSegmentName = logSegment.getName();
      assertTrue("Log segment: " + logSegmentName + " not found",
          actualLogSegmentValidSizeMap.getSecond().containsKey(logSegmentName));

      long expectedLogSegmentValidSize =
          state.getValidDataSizeForLogSegment(logSegment, timeRange.getEndTimeInMs(), expiryReferenceTimeMs);
      long actualLogSegmentValidSize = actualLogSegmentValidSizeMap.getSecond().get(logSegmentName);
      assertEquals("Valid data size mismatch for log segment: " + logSegmentName, expectedLogSegmentValidSize,
          actualLogSegmentValidSize);

      expectedTotalLogSegmentValidSize += expectedLogSegmentValidSize;
      expectedNumberOfLogSegments++;
      logSegment = state.log.getNextSegment(logSegment);
    }

    assertEquals("Mismatch in number of log segments", expectedNumberOfLogSegments,
        actualLogSegmentValidSizeMap.getSecond().size());

    Pair<Long, Long> actualTotalValidSize = blobStoreStats.getValidSize(timeRange);
    assertTrue("Valid data size collection time should be in the range",
        timeRange.getStartTimeInMs() <= actualTotalValidSize.getFirst()
            && timeRange.getEndTimeInMs() >= actualLogSegmentValidSizeMap.getFirst());
    assertEquals("Total valid data size of all log segments mismatch", expectedTotalLogSegmentValidSize,
        actualTotalValidSize.getSecond().longValue());

    return actualTotalValidSize.getSecond();
  }

  /**
   * Go over the referenceIndex to collect valid data size information per container. The result is used for
   * verification purposes.
   * @param deleteAndExpirationRefTimeInMs the reference time in ms until which deletes and expiration are relevant
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> getValidSizeByContainer(long deleteAndExpirationRefTimeInMs) {
    Map<String, Map<String, Long>> containerValidSizeMap = new HashMap<>();
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> segmentEntry : state.referenceIndex.entrySet()) {
      List<IndexEntry> validEntries =
          state.getValidIndexEntriesForIndexSegment(segmentEntry.getKey(), deleteAndExpirationRefTimeInMs,
              deleteAndExpirationRefTimeInMs);
      for (IndexEntry indexEntry : validEntries) {
        IndexValue indexValue = indexEntry.getValue();
        if (!indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
          updateNestedMapHelper(containerValidSizeMap, String.valueOf(indexValue.getServiceId()),
              String.valueOf(indexValue.getContainerId()), indexValue.getSize());
        }
      }
    }
    return containerValidSizeMap;
  }

  /**
   * Helper function to update nested map data structure.
   * @param nestedMap nested {@link Map} to be updated
   * @param firstKey of the nested map
   * @param secondKey of the nested map
   * @param value the value to be added at the corresponding entry
   */
  private void updateNestedMapHelper(Map<String, Map<String, Long>> nestedMap, String firstKey, String secondKey,
      Long value) {
    if (!nestedMap.containsKey(firstKey)) {
      nestedMap.put(firstKey, new HashMap<String, Long>());
    }
    Map<String, Long> innerMap = nestedMap.get(firstKey);
    Long newValue = innerMap.containsKey(secondKey) ? innerMap.get(secondKey) + value : value;
    innerMap.put(secondKey, newValue);
  }
}
