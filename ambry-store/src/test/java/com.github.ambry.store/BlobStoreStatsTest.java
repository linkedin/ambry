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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
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
  private static final StoreKeyFactory STORE_KEY_FACTORY;
  private static final byte[] RECOVERY_INFO = new byte[100];

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
      Arrays.fill(RECOVERY_INFO, (byte) 0);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static final int BUCKET_COUNT = 200;
  private static final long BUCKET_TIME_SPAN = 1000;
  private static final long SEGMENT_SCAN_OFFSET = 100000;

  private final CuratedLogIndexState state;
  // Variables that represent the folder where the data resides
  private final File tempDir;

  // the BlobStoreStats that exposes statistics about the blobStore
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
    blobStoreStats = new BlobStoreStats(state.log, state.index, BUCKET_COUNT, BUCKET_TIME_SPAN, SEGMENT_SCAN_OFFSET,
        false, state.time);
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

  @Test
  public void testTotalCapacity() throws IOException, StoreException, InterruptedException {
    // Verify total capacity
    assertEquals("Total capacity mismatch ", state.getTotalCapcity(), blobStoreStats.getTotalCapacity());
    // Total capacity should be unaffected by any new writes to the store
    state.addPutEntries(10, state.PUT_RECORD_SIZE, Utils.Infinite_Time);
    assertEquals("Total capacity mismatch ", state.getTotalCapcity(), blobStoreStats.getTotalCapacity());
  }

  @Test
  public void testUsedCapacity() throws IOException, StoreException, InterruptedException {
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment());

    // 1 DELETE for a PUT in the 1th index segment
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.index.indexes.firstKey()));
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment());

    // 2 DELETEs for a PUT in last index segment
    state.addPutEntries(2, state.PUT_RECORD_SIZE, Utils.Infinite_Time);
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment());

    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.index.indexes.lastKey()));
    state.addPutEntries(2, state.PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.index.indexes.lastKey()));
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment());
  }

  @Test
  public void testValidCapacity() throws IOException, StoreException, InterruptedException {
    // verify valid data size
    blobStoreStats.start();

    checkAndPopulateValidDataSize();
    for (IndexSegment segment : state.index.indexes.values()) {
      TimeRange timeRange = new TimeRange(segment.getLastModifiedTime() + 1, 0);
      verifySegmentValidDataSizeOfStore(blobStoreStats, timeRange);
      verifyValidDataSizeBySegment(blobStoreStats, timeRange);
    }

    verifyValidDataSizeByContainer(blobStoreStats);
    for (int i = 0; i <= 5; i++) {
      state.advanceTime(Time.MsPerSec * 10 + Time.MsPerSec);
      verifyValidDataSizeByContainer(blobStoreStats);
    }
  }

  @Test
  public void testNewWritesAfterScan() throws IOException, StoreException, InterruptedException {
    blobStoreStats.start();

    checkAndPopulateValidDataSize();
    verifyValidDataSizeByContainer(blobStoreStats);

    ArrayList<MessageInfo> messageInfos = new ArrayList<>();
    ArrayList<IndexEntry> indexEntries = new ArrayList<>();
    // 3 PUT that will expire in 20 seconds
    indexEntries.addAll(state.addPutEntries(
        3, state.PUT_RECORD_SIZE, state.time.milliseconds() + 20 * Time.MsPerSec).getSecond());
    // 5 PUTs without expiry
    indexEntries.addAll(state.addPutEntries(5, state.PUT_RECORD_SIZE, Utils.Infinite_Time).getSecond());

    for (IndexEntry indexEntry : indexEntries) {
      messageInfos.add(new MessageInfo(indexEntry.getKey(), indexEntry.getValue().getSize(), indexEntry.getValue().getExpiresAtMs()));
    }
    blobStoreStats.processNewPutEntries(messageInfos, indexEntries);

    // 1 DELETE of a new non-expiring PUT record.
    addNewDeleteEntry(indexEntries.get(3).getKey());
    // 1 DELETE of a new expiring PUT record.
    addNewDeleteEntry(indexEntries.get(1).getKey());

    checkAndPopulateValidDataSize();
    TimeRange timeRange = new TimeRange(state.time.seconds() + 1, 0);
    verifySegmentValidDataSizeOfStore(blobStoreStats, timeRange);
    verifyValidDataSizeBySegment(blobStoreStats, timeRange);
    verifyValidDataSizeByContainer(blobStoreStats);
  }

  @Test
  public void testOutOfBoundRequests() throws IOException, StoreException, InterruptedException {
    BlobStoreStats handicappedBlobStoreStats = new BlobStoreStats(state.log, state.index, 1, 1000, 1000,
        false, state.time);
    handicappedBlobStoreStats.start();

    checkAndPopulateValidDataSize();
    for (IndexSegment segment : state.index.indexes.values()) {
      TimeRange timeRange = new TimeRange(segment.getLastModifiedTime() + 1, 0);
      verifySegmentValidDataSizeOfStore(handicappedBlobStoreStats, timeRange);
      verifyValidDataSizeBySegment(handicappedBlobStoreStats, timeRange);
    }

    verifyValidDataSizeByContainer(handicappedBlobStoreStats);
    for (int i = 0; i <= 5; i++) {
      state.advanceTime(Time.MsPerSec * 10 + Time.MsPerSec);
      verifyValidDataSizeByContainer(handicappedBlobStoreStats);
    }
  }

  private void addNewDeleteEntry(StoreKey storeKey) throws IOException, StoreException {
    MockId deleteCandidate = new MockId(storeKey.getID());
    if (state.liveKeys.contains(deleteCandidate)) {
      state.deletedKeys.add(deleteCandidate);
      state.liveKeys.remove(deleteCandidate);
      IndexValue indexValue = state.allKeys.get(deleteCandidate).getFirst();
      MessageInfo messageInfo = new MessageInfo(deleteCandidate, state.DELETE_RECORD_SIZE, true, Utils.Infinite_Time);
      state.addDeleteEntry(deleteCandidate);
      IndexValue deleteIndexValue = state.allKeys.get(deleteCandidate).getSecond();
      blobStoreStats.processNewDeleteEntry(messageInfo, indexValue, deleteIndexValue.getOffset());
    }
  }

  /**
   * Verifies used capacity of the store
   * @param actualUsedCapacity actual used capacity of the store
   */
  private void verifyUsedCapacity(long actualUsedCapacity) {
    assertEquals("Used capacity mismatch ", state.log.getCapacityInBytes(), actualUsedCapacity);
  }

  /**
   * Verifies Used capacity of the store by segments
   * @param actualUsedCapacity actual used capacity by segment of the store
   */
  private void verifyUsedCapacityBySegment(SortedMap<String, Long> actualUsedCapacity) {
    Iterator<Map.Entry<String, Long>> usedCapacityIterator = actualUsedCapacity.entrySet().iterator();
    LogSegment expectedLogSegment = state.log.getFirstSegment();
    while (usedCapacityIterator.hasNext()) {
      Map.Entry<String, Long> segmentNameCapacityPair = usedCapacityIterator.next();
      assertEquals("Log segment name mismatch while fetching Used capacity", expectedLogSegment.getName(),
          segmentNameCapacityPair.getKey());
      assertEquals("Used capacity mismatch for  " + expectedLogSegment.getName(), expectedLogSegment.getEndOffset(),
          segmentNameCapacityPair.getValue().longValue());
      expectedLogSegment = state.log.getNextSegment(expectedLogSegment);
    }
    assertNull("No of segments mismatch ", expectedLogSegment);
  }

  /**
   * Verifies the total segment valid data size of the store
   * @param timeRange the {@link TimeRange} that needs to be used while fetching valid data size
   * @throws IOException
   * @throws StoreException
   */
  private void verifySegmentValidDataSizeOfStore(BlobStoreStats selectedBlobStoreStats, TimeRange timeRange) throws IOException, StoreException {
    HashMap<String, Long> expectedSegmentValidDataSizeMap =
        state.validDataSizePerSegment.floorEntry(timeRange.getStart()).getValue();
    long expectedSegmentValidDataSize = 0;
    for (Map.Entry<String, Long> validDataSizePerSegment : expectedSegmentValidDataSizeMap.entrySet()) {
      expectedSegmentValidDataSize += validDataSizePerSegment.getValue();
    }

    Pair<Long, Long> actualValidDataSizeOfStore = selectedBlobStoreStats.getValidDataSize(timeRange);
    assertTrue("Valid data size collection time should be greater than start time in the range",
        timeRange.getStart() <= actualValidDataSizeOfStore.getFirst());
    assertTrue("Valid data size collection time should be lesser than end time in the range",
        actualValidDataSizeOfStore.getFirst() <= timeRange.getEnd());
    assertEquals("Valid data size of the store mismatch", expectedSegmentValidDataSize,
        actualValidDataSizeOfStore.getSecond().longValue());
  }

  /**
   * Verifies the valid data size of individual segments in the store
   * @param timeRange the {@link TimeRange} that needs to be used while fetching valid data size
   * @throws IOException
   * @throws StoreException
   */
  private void verifyValidDataSizeBySegment(BlobStoreStats selectedBlobStoreStats, TimeRange timeRange) throws IOException, StoreException {
    HashMap<String, Long> expectedSegmentValidDataSizeMap =
        state.validDataSizePerSegment.floorEntry(timeRange.getStart()).getValue();

    Pair<Long, SortedMap<String, Long>> actualValidDataSizePerSegment =
        selectedBlobStoreStats.getValidDataSizeBySegment(timeRange);
    assertTrue("Valid data size collection time should be greater than start time in the range",
        timeRange.getStart() <= actualValidDataSizePerSegment.getFirst());
    assertTrue("Valid data size collection time should be lesser than end time in the range",
        actualValidDataSizePerSegment.getFirst() <= timeRange.getEnd());

    for (Map.Entry<String, Long> validSegmentSize : actualValidDataSizePerSegment.getSecond().entrySet()) {
      assertTrue("Segment not found in expectedSegmentValidDataSizeMap " + validSegmentSize.getKey(),
          expectedSegmentValidDataSizeMap.containsKey(validSegmentSize.getKey()));
      assertEquals("Segment's valid size mismatch ", expectedSegmentValidDataSizeMap.get(validSegmentSize.getKey()),
          validSegmentSize.getValue());
      expectedSegmentValidDataSizeMap.remove(validSegmentSize.getKey());
    }
    assertEquals("Mismatch in total segments returned by ValidDataSizePerSegment(). Missing entries are : "
        + expectedSegmentValidDataSizeMap.entrySet(), 0, expectedSegmentValidDataSizeMap.size());
  }

  /**
   * Verifies the valid data size of containers
   */
  private void verifyValidDataSizeByContainer(BlobStoreStats selectedBlobStoreStats) {
    HashMap<String, HashMap<String, Long>> actualValidDataSizePerContainer = selectedBlobStoreStats.getValidDataSize();
    HashMap<String, HashMap<String, Long>> expectedValidDataSizePerContainer = new HashMap<>();
    for (Map.Entry<String, HashMap<String, Long>> entry : state.validDataSizePerContainer.entrySet()) {
      expectedValidDataSizePerContainer.put(entry.getKey(), new HashMap<>(entry.getValue()));
    }
    for (MessageInfo expirableInfo : state.expirableMap.values()) {
      if (expirableInfo.getExpirationTimeInMs() <= state.time.milliseconds()) {
        Long currentValue = expectedValidDataSizePerContainer.get(
            expirableInfo.getServiceId()).get(expirableInfo.getContainerId());
        expectedValidDataSizePerContainer.get(expirableInfo.getServiceId()).put(expirableInfo.getContainerId(),
            currentValue - expirableInfo.getSize());
      }
    }
    for (Map.Entry<String, HashMap<String, Long>> entry : actualValidDataSizePerContainer.entrySet()) {
      assertTrue("ServiceId not found in expectedValidDataSizePerContainer " + entry.getKey(),
          expectedValidDataSizePerContainer.containsKey(entry.getKey()));
      for (Map.Entry<String, Long> innerEntry : entry.getValue().entrySet()) {
        assertTrue("ContainerId not found in expectedValidDataSizePerContainer " + entry.getKey() + " " +
            innerEntry.getKey(), expectedValidDataSizePerContainer.get(entry.getKey()).containsKey(innerEntry.getKey()));
        Long expectedContainerValue = expectedValidDataSizePerContainer.get(entry.getKey()).get(innerEntry.getKey());
        assertEquals("Container's valid data size mismatch " + entry.getKey() + " " + innerEntry.getKey(),
            expectedContainerValue, actualValidDataSizePerContainer.get(entry.getKey()).get(innerEntry.getKey()));
        expectedValidDataSizePerContainer.get(entry.getKey()).remove(innerEntry.getKey());
      }
    }
    for (HashMap<String, Long> remainingContainer : expectedValidDataSizePerContainer.values()) {
      for (Long value : remainingContainer.values()) {
        assertEquals("Mismatch in number of containers with valid data returned by getValidDataSize",
            0L, value.longValue());
      }
    }
  }

  /**
   * Check if there is an active segment that hasn't been processed for valid data size yet.
   * @throws IOException
   * @throws StoreException
   */
  private void checkAndPopulateValidDataSize() throws IOException, StoreException {
    if (state.populateValidDataSizeCheckPoint == null ||
        state.populateValidDataSizeCheckPoint.compareTo(state.index.getCurrentEndOffset()) != 0) {
      state.populateValidDataSize(state.index.indexes.lastEntry().getValue());
    }
  }
}
