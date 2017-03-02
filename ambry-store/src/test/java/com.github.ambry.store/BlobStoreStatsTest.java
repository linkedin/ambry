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
import java.util.TreeMap;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests for {@link PersistentIndex}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class BlobStoreStatsTest {
  private static final long TEST_TIME_INTERVAL_IN_SEC = CuratedLogIndexState.DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS / 2;
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
    tempDir = StoreTestUtils.createTempDirectory("indexDir-" + UtilsTest.getRandomString(10));
    state = new CuratedLogIndexState(isLogSegmented, tempDir);
    DiskIOScheduler diskIOScheduler = new DiskIOScheduler(null);
    blobStoreStats = new BlobStoreStats(state.index, state.time, diskIOScheduler);
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

  @Test(expected = IllegalArgumentException.class)
  public void testTimeRange() {
    // construct a TimeRange with illegal arguments
    TimeRange timeRange = new TimeRange(-1, -1);
  }

  /**
   * Basic test to verify reported valid size information per container by BlobStoreStats.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testContainerValidDataSize() throws InterruptedException, StoreException, IOException {
    verifyContainerValidSize();
  }

  /**
   * Basic test to verify reported valid size information per log segment by BlobStoreStats.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testLogSegmentValidDataSize() throws InterruptedException, StoreException, IOException {
    for (int i = 0; i <= state.time.seconds(); i += TEST_TIME_INTERVAL_IN_SEC) {
      TimeRange timeRange = new TimeRange(i, 0L);
      verifyLogSegmentValidSize(timeRange);
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
    // advance time by 1 second
    state.advanceTime(Time.MsPerSec);
    long timeInSecsBeforePuts = state.time.seconds();
    long totalLogSegmentValidSizeBeforePuts = verifyLogSegmentValidSize(new TimeRange(timeInSecsBeforePuts, 0L));
    long totalContainerValidSizeBeforePuts = verifyContainerValidSize();

    // 3 puts
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // advance time by 1 second
    state.advanceTime(Time.MsPerSec);

    long timeInSecsAfterPuts = state.time.seconds();
    verifyLogSegmentValidSize(new TimeRange(timeInSecsAfterPuts + 1, 0L));
    long totalLogSegmentValidSizeAfterPuts = verifyLogSegmentValidSize(new TimeRange(timeInSecsAfterPuts, 0L));
    long totalContainerValidSizeAfterPuts = verifyContainerValidSize();
    long expectedIncrement = 3 * CuratedLogIndexState.PUT_RECORD_SIZE;
    assertEquals("Put entries are not properly counted for log segment valid size", totalLogSegmentValidSizeAfterPuts,
        totalLogSegmentValidSizeBeforePuts + expectedIncrement);
    assertEquals("Put entries are not properly counted for container valid size", totalContainerValidSizeAfterPuts,
        totalContainerValidSizeBeforePuts + expectedIncrement);
  }

  /**
   * Tests to verify the correctness of reported stats with puts that is going to expire via the following steps:
   * 1. Verify reported stats and record the total valid size before adding the new expiring puts.
   * 2. Add new expiring puts and advance time to let the puts expire.
   * 3. Verify reported stats and record the total valid size after new puts are expired.
   * 4. Verify that the total valid size before the new puts and after is the same.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterExpiration() throws InterruptedException, StoreException, IOException {
    // advance time by 1 second
    state.advanceTime(Time.MsPerSec);
    long timeInSecsBeforeExpiringPuts = state.time.seconds();
    long totalLogSegmentValidSizeBeforePuts =
        verifyLogSegmentValidSize(new TimeRange(timeInSecsBeforeExpiringPuts, 0L));
    long totalContainerValidSizeBeforePuts = verifyContainerValidSize();

    // 3 puts that will expire in 20 seconds
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, (timeInSecsBeforeExpiringPuts + 20) * Time.MsPerSec);
    // advance time by 21 seconds
    state.advanceTime(21 * Time.MsPerSec);

    long timeInSecsAfterExpiringPuts = state.time.seconds();
    long totalLogSegmentValidSizeAfterPuts = verifyLogSegmentValidSize(new TimeRange(timeInSecsAfterExpiringPuts, 0L));
    long totalContainerValidSizeAfterPuts = verifyContainerValidSize();
    assertEquals("Expired put entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeBeforePuts, totalLogSegmentValidSizeAfterPuts);
    assertEquals("Expired put entries are not properly counted for container valid size",
        totalContainerValidSizeBeforePuts, totalContainerValidSizeAfterPuts);
  }

  /**
   * Tests to verify the correctness of reported stats with after new deletes via the following steps:
   * 1. Add new puts that are going to be deleted later.
   * 2. Verify reported stats and record the total valid size before new deletes.
   * 3. Perform the deletes.
   * 4. Verify reported stats and record the total valid size after the deletes.
   * 5. Verify the delta of total valid size prior to the new deletes and after matches with the expected delta.
   * @throws InterruptedException
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testValidDataSizeAfterDeletes() throws InterruptedException, StoreException, IOException {
    // 5 puts to be deleted
    state.addPutEntries(5, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    // advance time by 1 second
    state.advanceTime(Time.MsPerSec);
    long timeInSecsBeforeDeletes = state.time.seconds();
    long totalLogSegmentValidSizeBeforeDeletes = verifyLogSegmentValidSize(new TimeRange(timeInSecsBeforeDeletes, 0L));
    long totalContainerValidSizeBeforeDeletes = verifyContainerValidSize();

    // 2 deletes from the last index segment
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    // advance time by 1 second
    state.advanceTime(Time.MsPerSec);

    long timeInSecsAfterDeletes = state.time.seconds();
    long totalLogSegmentValidSizeAfterDeletes = verifyLogSegmentValidSize(new TimeRange(timeInSecsAfterDeletes, 0L));
    long totalContainerValidSizeAfterDeletes = verifyContainerValidSize();
    long expectedLogSegmentDecrement =
        2 * (CuratedLogIndexState.PUT_RECORD_SIZE - CuratedLogIndexState.DELETE_RECORD_SIZE);
    long expectedContainerDecrement = 2 * CuratedLogIndexState.PUT_RECORD_SIZE;
    assertEquals("Delete entries are not properly counted for log segment valid size",
        totalLogSegmentValidSizeAfterDeletes, totalLogSegmentValidSizeBeforeDeletes - expectedLogSegmentDecrement);
    assertEquals("Delete entries are not properly counted for container valid size",
        totalContainerValidSizeAfterDeletes, totalContainerValidSizeBeforeDeletes - expectedContainerDecrement);
  }

  /**
   * Verify the correctness of valid data size information per container returned by BlobStoreStats.
   * @return the total valid data size of all containers (from all serviceIds)
   */
  private long verifyContainerValidSize() throws StoreException {
    long referenceTimeInMs = state.time.milliseconds();
    Map<String, Map<String, Long>> actualContainerValidSizeMap = blobStoreStats.getValidDataSize();
    Map<String, Map<String, Long>> expectedContainerValidSizeMap = getValidSizeByContainer(referenceTimeInMs);
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
    }

    return totalValidSize;
  }

  /**
   * Verify the correctness of valid data size information per log segment returned by BlobStoreStats for a given {@link TimeRange}.
   * @param timeRange the {@link TimeRange} to be used for the verification
   * @return the total valid data size of all log segments
   */
  private long verifyLogSegmentValidSize(TimeRange timeRange) throws StoreException {
    Pair<Long, Map<String, Long>> actualLogSegmentValidSizeMap = blobStoreStats.getValidDataSizeByLogSegment(timeRange);
    assertTrue("Valid data size collection time should be in the range",
        timeRange.getStartTimeInSecs() <= actualLogSegmentValidSizeMap.getFirst()
            && timeRange.getEndTimeInSecs() >= actualLogSegmentValidSizeMap.getFirst());

    Map<String, Long> expectedLogSegmentValidSizeMap =
        state.getValidSizeByLogSegment(timeRange.getStartTimeInSecs() * Time.MsPerSec);
    assertEquals("Mismatch in number of log segments", expectedLogSegmentValidSizeMap.size(),
        actualLogSegmentValidSizeMap.getSecond().size());
    long expectedTotalValidSize = 0L;

    for (Map.Entry<String, Long> expectedLogSegmentValidSizeEntry : expectedLogSegmentValidSizeMap.entrySet()) {
      String logSegmentName = expectedLogSegmentValidSizeEntry.getKey();
      assertTrue("Log segment: " + logSegmentName + " not found",
          actualLogSegmentValidSizeMap.getSecond().containsKey(logSegmentName));
      long expectedSegmentValidSize = expectedLogSegmentValidSizeEntry.getValue();
      long actualSegmentValidSize = actualLogSegmentValidSizeMap.getSecond().get(logSegmentName);
      assertEquals("Valid data size mismatch for log segment: " + logSegmentName, expectedSegmentValidSize,
          actualSegmentValidSize);
      expectedTotalValidSize += expectedLogSegmentValidSizeEntry.getValue();
    }

    Pair<Long, Long> actualTotalValidSize = blobStoreStats.getValidDataSize(timeRange);
    assertTrue("Valid data size collection time should be in the range",
        timeRange.getStartTimeInSecs() <= actualTotalValidSize.getFirst()
            && timeRange.getEndTimeInSecs() >= actualLogSegmentValidSizeMap.getFirst());
    assertEquals("Total valid data size of all log segments mismatch", expectedTotalValidSize,
        actualTotalValidSize.getSecond().longValue());

    return actualTotalValidSize.getSecond();
  }

  /**
   * Go over the entire referenceIndex to collect valid data size information per container. The result is used for
   * verification purposes.
   * @param referenceTimeInMs the reference time to be used to decide whether or not a blob is valid
   * @return a nested {@link Map} of serviceId to containerId to valid data size
   */
  private Map<String, Map<String, Long>> getValidSizeByContainer(long referenceTimeInMs) {
    Map<String, Map<String, Long>> containerValidSizeMap = new HashMap<>();
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> segmentEntry : state.referenceIndex.entrySet()) {
      for (Map.Entry<MockId, IndexValue> indexEntry : segmentEntry.getValue().entrySet()) {
        MockId key = indexEntry.getKey();
        IndexValue value = indexEntry.getValue();
        if (!value.isFlagSet(IndexValue.Flags.Delete_Index) && (value.getExpiresAtMs() == Utils.Infinite_Time
            || value.getExpiresAtMs() >= referenceTimeInMs) && !state.deletedKeys.contains(key)) {
          // non expired, non deleted PUT
          updateNestedMapHelper(containerValidSizeMap, BlobStoreStats.getServiceId(key),
              BlobStoreStats.getContainerId(key), value.getSize());
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
    Long existingValue = innerMap.get(secondKey);
    if (existingValue == null) {
      existingValue = value;
    } else {
      existingValue += value;
    }
    innerMap.put(secondKey, existingValue);
  }
}
