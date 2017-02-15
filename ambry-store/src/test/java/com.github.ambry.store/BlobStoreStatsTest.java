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
  private final Object notifyObject = new Object();

  // the StatsEngine that builds the data structure used by BlobStoreStats
  private final StatsEngine statsEngine;
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
    statsEngine = new StatsEngine(state.log, state.index, state.getTotalCapcity(), BUCKET_COUNT, BUCKET_TIME_SPAN,
        SEGMENT_SCAN_OFFSET, state.time, notifyObject);
    blobStoreStats = statsEngine.getBlobStoreStats();
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
  public void testValidCapacity() throws IOException, StoreException {
    // verify valid data size
    statsEngine.start();
    synchronized (notifyObject) {
      try {
        notifyObject.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    // stop the scheduler from triggering more scans
    statsEngine.close();
    Iterator<Map.Entry<Offset, IndexSegment>> indexesIterator = state.index.indexes.entrySet().iterator();
    while (indexesIterator.hasNext()) {
      Map.Entry<Offset, IndexSegment> indexEntry = indexesIterator.next();
      TimeRange timeRange = new TimeRange(indexEntry.getValue().getLastModifiedTime() + 1, 0);
      verifySegmentValidDataSizeOfStore(timeRange);
      verifyValidDataSizeBySegment(timeRange);
    }

    for (int i = 0; i <= 6; i++) {
      try {
        state.advanceTime(Time.MsPerSec + i * Time.MsPerSec * 10);
        verifyValidDataSizeByContainer();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
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
  private void verifySegmentValidDataSizeOfStore(TimeRange timeRange) throws IOException, StoreException {
    checkActiveIndexSegment();
    HashMap<String, Long> expectedSegmentValidDataSizeMap =
        state.validDataSizePerSegment.floorEntry(timeRange.getStart()).getValue();
    long expectedSegmentValidDataSize = 0;
    for (Map.Entry<String, Long> validDataSizePerSegment : expectedSegmentValidDataSizeMap.entrySet()) {
      expectedSegmentValidDataSize += validDataSizePerSegment.getValue();
    }

    Pair<Long, Long> actualValidDataSizeOfStore = blobStoreStats.getValidDataSize(timeRange);
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
  private void verifyValidDataSizeBySegment(TimeRange timeRange) throws IOException, StoreException {
    checkActiveIndexSegment();
    HashMap<String, Long> expectedSegmentValidDataSizeMap =
        state.validDataSizePerSegment.floorEntry(timeRange.getStart()).getValue();

    Pair<Long, SortedMap<String, Long>> actualValidDataSizePerSegment =
        blobStoreStats.getValidDataSizeBySegment(timeRange);
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
   * Verifies the  valid data size of containers and serviceIds
   */
  private void verifyValidDataSizeByContainer() {
    HashMap<String, ArrayList<String>> serviceIdContainerIdMap = new HashMap<>();
    for (String containerKey : state.validDataSizePerContainer.keySet()) {
      String[] ids = containerKey.split("-");
      ArrayList<String> containers = serviceIdContainerIdMap.get(ids[0]);
      if (containers == null) {
        containers = new ArrayList<>();
        containers.add(ids[1]);
        serviceIdContainerIdMap.put(ids[0], containers);
      } else {
        containers.add(ids[1]);
      }
    }

    for (Map.Entry<String, ArrayList<String>> idMapEntry : serviceIdContainerIdMap.entrySet()) {
      SortedMap<String, Long> actualValidDataSizePerContainer = blobStoreStats.getValidDataSizeByContainer(
          idMapEntry.getKey(), idMapEntry.getValue());
      Long actualValidDataSizePerServiceId = blobStoreStats.getValidDataSize(idMapEntry.getKey());
      Long expectedServiceValue = 0L;

      for (Map.Entry<String, Long> containerValidDataSize : actualValidDataSizePerContainer.entrySet()) {
        String containerKey = idMapEntry.getKey().concat("-").concat(containerValidDataSize.getKey());
        assertTrue("Container not found in validDataSizePerContainer" + containerKey,
            state.validDataSizePerContainer.containsKey(containerKey));
        Long expectedContainerValue =
            state.validDataSizePerContainer.get(containerKey);
        for (Map.Entry<StoreKey, MessageInfo> expirableInfo : state.expirableMap.entrySet()) {
          String expirableContainerKey =
              expirableInfo.getValue().getServiceId().concat("-").concat(expirableInfo.getValue().getContainerId());
          if (containerKey.equals(expirableContainerKey) &&
              expirableInfo.getValue().getExpirationTimeInMs() <= state.time.milliseconds()) {
            expectedContainerValue -= expirableInfo.getValue().getSize();
          }
        }
        assertEquals("Container's valid data size mismatch",
            expectedContainerValue, containerValidDataSize.getValue());
        expectedServiceValue += expectedContainerValue;
      }
      assertEquals("Mismatch in number of containers returned by getValidDataSizeByContainer",
          idMapEntry.getValue().size(), actualValidDataSizePerContainer.size());
      assertEquals("ServiceId's valid data size mismatch " + idMapEntry.getKey(),
          expectedServiceValue, actualValidDataSizePerServiceId);
    }
  }

  /**
   * Check if there is an active segment that hasn't been processed for valid data size yet.
   * @throws IOException
   * @throws StoreException
   */
  private void checkActiveIndexSegment() throws IOException, StoreException {
    if (state.validDataSizePerSegment.size() < state.index.indexes.size()) {
      state.populateValidDataSize(state.index.indexes.lastEntry().getValue());
    }
  }
}