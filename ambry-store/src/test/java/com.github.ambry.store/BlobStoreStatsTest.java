/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
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

  private BlobStoreTestUtils blobStoreTestUtils;
  private static final Set<MockId> EMPTY_SET = new HashSet<>();

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
    blobStoreTestUtils = new BlobStoreTestUtils(isLogSegmented);
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    blobStoreTestUtils.cleanup();
  }

  /**
   * Tests {@link BlobStore#getBlobStoreStats()}
   */
  @Test
  public void testBlobStoreStats() throws IOException, StoreException, InterruptedException {
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());

    // put data and verify
    blobStoreTestUtils.idsByLogSegment.get(blobStoreTestUtils.idsByLogSegment.size() - 1)
        .addAll(blobStoreTestUtils.put(2, blobStoreTestUtils.PUT_RECORD_SIZE, Utils.Infinite_Time));
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
    // put data that is expired and verify
    MockId addedId =
        blobStoreTestUtils.put(1, blobStoreTestUtils.PUT_RECORD_SIZE, blobStoreTestUtils.time.milliseconds() + 1)
            .get(0);
    blobStoreTestUtils.idsByLogSegment.get(blobStoreTestUtils.idsByLogSegment.size() - 1).add(addedId);
    blobStoreTestUtils.time.sleep(2);
    blobStoreTestUtils.liveKeys.remove(addedId);
    blobStoreTestUtils.expiredKeys.add(addedId);
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
    // failed puts shouldn't result in any change in stats
    blobStoreTestUtils.verifyPutFailure(blobStoreTestUtils.liveKeys.iterator().next(), StoreErrorCodes.Already_Exist);
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
    // delete data and verify
    testBlobStoreStatsWithDeletes(0);

    if (blobStoreTestUtils.isLogSegmented) {
      int[] logSegmentIndexToDelete = new int[]{1, blobStoreTestUtils.idsByLogSegment.size() - 1};
      for (int index : logSegmentIndexToDelete) {
        testBlobStoreStatsWithDeletes(index);
      }
      // fill the current active segment to its capacity
      long usedCapacityOfLastSegment = getUsedCapacityOfLogSegment(blobStoreTestUtils.idsByLogSegment.size() - 1);
      MockId id = blobStoreTestUtils.put(1, blobStoreTestUtils.SEGMENT_CAPACITY - usedCapacityOfLastSegment,
          Utils.Infinite_Time).get(0);
      blobStoreTestUtils.idsByLogSegment.get(blobStoreTestUtils.idsByLogSegment.size() - 1).add(id);
      // add data to new segment and verify
      blobStoreTestUtils.idsByLogSegment.add(EMPTY_SET);
      blobStoreTestUtils.idsByLogSegment.get(blobStoreTestUtils.idsByLogSegment.size() - 1)
          .addAll(blobStoreTestUtils.put(2, blobStoreTestUtils.PUT_RECORD_SIZE, Utils.Infinite_Time));
      verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
      // delete data from a sealed segment and verify
      testBlobStoreStatsWithDeletes(2);
      // delete data from the active segment and verify
      testBlobStoreStatsWithDeletes(blobStoreTestUtils.idsByLogSegment.size() - 1);
    }

    blobStoreTestUtils.reloadStore();
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
  }

  // blob Store stats test helpers

  /**
   * Verifies {@link BlobStoreStats}
   * @param blobStoreStats the {@link BlobStoreStats} that needs to be verified
   * @throws IOException
   * @throws StoreException
   */
  private void verifyBlobStoreStats(BlobStoreStats blobStoreStats) throws IOException, StoreException {
    // verify used capacity
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment().getSecond());
    // verify valid data size
    verifyValidDataSize(blobStoreStats.getValidDataSize().getSecond().longValue());
    verifyValidDataSizeBySegment(blobStoreStats.getValidDataSizeBySegment().getSecond());
    // verify total capacity
    verifyTotalCapacity(blobStoreStats.getTotalCapacity());
  }

  /**
   * Tests {@link BlobStoreStats}  with some delete operations
   * @param deleteLogSegmentIndex log segment index from which a blob has to be deleted
   * @throws StoreException
   */
  private void testBlobStoreStatsWithDeletes(int deleteLogSegmentIndex) throws StoreException, IOException {
    // delete a blob in the given segment
    MockId idToDelete = blobStoreTestUtils.getIdToDelete(blobStoreTestUtils.idsByLogSegment.get(deleteLogSegmentIndex));
    blobStoreTestUtils.delete(idToDelete);
    blobStoreTestUtils.idsByLogSegment.get(blobStoreTestUtils.idsByLogSegment.size() - 1).add(idToDelete);
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
    blobStoreTestUtils.verifyDeleteFailure(idToDelete, StoreErrorCodes.ID_Deleted);
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
    blobStoreTestUtils.verifyDeleteFailure(blobStoreTestUtils.getUniqueId(), StoreErrorCodes.ID_Not_Found);
    verifyBlobStoreStats(blobStoreTestUtils.store.getBlobStoreStats());
  }

  /**
   * Verifies used capacity of the store
   * @param usedCapacity used capacity of the store
   */
  private void verifyUsedCapacity(long usedCapacity) {
    long segmentCapacity =
        blobStoreTestUtils.isLogSegmented ? blobStoreTestUtils.SEGMENT_CAPACITY : blobStoreTestUtils.LOG_CAPACITY;
    long usedCapacityOfSealedSegments = (blobStoreTestUtils.idsByLogSegment.size() - 1) * segmentCapacity;
    long lastSegmentSize = getUsedCapacityOfLogSegment(blobStoreTestUtils.idsByLogSegment.size() - 1);
    assertEquals("Used capacity mismatch ", usedCapacityOfSealedSegments + lastSegmentSize, usedCapacity);
  }

  /**
   * Verify Used capacity of the store
   * @param actualUsedCapacity actual used capacity by segment of the store
   */
  private void verifyUsedCapacityBySegment(SortedMap<String, Long> actualUsedCapacity) {
    Iterator<Map.Entry<String, Long>> usedCapacityIterator = actualUsedCapacity.entrySet().iterator();
    int count = 0;
    while (usedCapacityIterator.hasNext()) {
      Map.Entry<String, Long> segmentNameCapacityPair = usedCapacityIterator.next();
      assertEquals("Used capacity mismatch for  " + segmentNameCapacityPair.getKey(),
          getUsedCapacityOfLogSegment(count++), segmentNameCapacityPair.getValue().longValue());
    }
    assertTrue("No of segments mismatch ", (count == actualUsedCapacity.size()));
  }

  /**
   * Verifies valid data size of the {@link BlobStore}
   * @param validDataSize valid data size of the {@link BlobStore}
   */
  private void verifyValidDataSize(long validDataSize) {
    assertEquals("Expected valid data size mismatch ", getValidDataSizeForKeys(blobStoreTestUtils.liveKeys),
        validDataSize);
  }

  /**
   * Verify valid data size by segment of the store
   * @param validDataSizeBySegments valid data size of the segments
   * @throws IOException
   * @throws StoreException
   */
  private void verifyValidDataSizeBySegment(SortedMap<String, Long> validDataSizeBySegments)
      throws IOException, StoreException {
    Iterator<Map.Entry<String, Long>> iterator = validDataSizeBySegments.entrySet().iterator();
    int count = 0;
    while (iterator.hasNext()) {
      Map.Entry<String, Long> validSizeOfSegment = iterator.next();
      assertEquals("Valid data size mismatch for " + validSizeOfSegment.getKey(), getValidDataSizeOfLogSegment(count++),
          validSizeOfSegment.getValue().longValue());
    }
    assertEquals("Store returned more segments for valid data size per segment ", count,
        blobStoreTestUtils.idsByLogSegment.size());
  }

  /**
   * Get Valid data size of the given log segment
   * @param logSegmentIndex the log segment index for which the valid data size has to be determined
   * @return the valid data size of the given log segment
   */
  private long getValidDataSizeOfLogSegment(int logSegmentIndex) {
    long validDataSize = 0;
    for (MockId key : blobStoreTestUtils.idsByLogSegment.get(logSegmentIndex)) {
      if (blobStoreTestUtils.liveKeys.contains(key)) {
        MessageInfo msgInfo = blobStoreTestUtils.allKeys.get(key).msgInfo;
        validDataSize += msgInfo.getSize();
      }
    }
    return validDataSize;
  }

  /**
   * Get the valid data size of all the keys passed in
   * @param keys set of {@link MockId}s for which valid data size has to be determined
   * @return the valid data size of all the keys passed in
   */
  private long getValidDataSizeForKeys(Set<MockId> keys) {
    long validDataSize = 0;
    for (MockId key : keys) {
      MessageInfo msgInfo = blobStoreTestUtils.allKeys.get(key).msgInfo;
      validDataSize += msgInfo.getSize();
    }
    return validDataSize;
  }

  /**
   * Get used capacity of a log segment
   * @param logSegmentIndex the log segment index for which the used capacity has to be determined
   * @return the used capacity of a log segment
   */
  private long getUsedCapacityOfLogSegment(int logSegmentIndex) {
    long usedSegmentSize = 0;
    Set<MockId> mockIds = blobStoreTestUtils.idsByLogSegment.get(logSegmentIndex);
    for (MockId id : mockIds) {
      if (blobStoreTestUtils.deletedKeys.contains(id)
          && blobStoreTestUtils.allKeys.get(id).logSegmentIndexforDelete == logSegmentIndex) {
        usedSegmentSize += (blobStoreTestUtils.DELETE_RECORD_SIZE);
        if (blobStoreTestUtils.allKeys.get(id).logSegmentIndexForPut == logSegmentIndex) {
          usedSegmentSize += blobStoreTestUtils.allKeys.get(id).msgInfo.getSize();
        }
      } else {
        usedSegmentSize += blobStoreTestUtils.allKeys.get(id).msgInfo.getSize();
      }
    }
    if (blobStoreTestUtils.isLogSegmented) {
      usedSegmentSize += LogSegment.HEADER_SIZE;
    }
    return usedSegmentSize;
  }

  /**
   * Verifies total capacity of the {@link BlobStore}
   * @param totalCapacity the total capacity of the {@link BlobStore}
   */
  private void verifyTotalCapacity(long totalCapacity) {
    assertEquals("Total capacity mismatch ", blobStoreTestUtils.LOG_CAPACITY, totalCapacity);
  }
}
