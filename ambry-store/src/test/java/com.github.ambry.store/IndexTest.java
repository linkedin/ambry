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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.store.CuratedLogIndexState.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link PersistentIndex}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class IndexTest {

  private final boolean isLogSegmented;
  private final File tempDir;
  private final CuratedLogIndexState state;

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
  public IndexTest(boolean isLogSegmented) throws InterruptedException, IOException, StoreException {
    this.isLogSegmented = isLogSegmented;
    tempDir = StoreTestUtils.createTempDirectory("indexDir-" + UtilsTest.getRandomString(10));
    state = new CuratedLogIndexState(isLogSegmented, tempDir);
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    state.destroy();
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Tests for {@link PersistentIndex#findKey(StoreKey)}.
   * Cases:
   * 1. Live keys
   * 2. Expired keys
   * 3. Deleted keys
   * 4. Non existent keys
   * @throws StoreException
   */
  @Test
  public void findKeyTest() throws StoreException {
    for (MockId id : state.allKeys.keySet()) {
      IndexValue value = state.index.findKey(id);
      verifyValue(id, value);
    }
    // search for a non existent key
    MockId nonExistentId = state.getUniqueId();
    verifyValue(nonExistentId, state.index.findKey(nonExistentId));
  }

  /**
   * Tests for {@link PersistentIndex#findKey(StoreKey, FileSpan)}.
   * Cases:
   * 1. FileSpan exactly that of the message
   * 2. FileSpan before and after message including it on the boundary
   * 3. FileSpan that includes the message.
   * 4. FileSpan before and after message not including it
   * @throws StoreException
   */
  @Test
  public void findKeyWithFileSpanTest() throws StoreException {
    // using only liveKeys to simplify the test - behavior does not differ based on the type of entry.
    for (MockId id : state.liveKeys) {
      IndexValue expectedValue = state.getExpectedValue(id, true);
      FileSpan fileSpanForMessage = state.log.getFileSpanForMessage(expectedValue.getOffset(), expectedValue.getSize());

      // FileSpan that is exactly that of the message
      verifyValue(id, state.index.findKey(id, fileSpanForMessage));

      Offset indexSegmentStartOffset = state.indexSegmentStartOffsets.get(id).getFirst();
      Offset lowerSegmentStartOffset = state.referenceIndex.lowerKey(indexSegmentStartOffset);
      Offset higherSegmentStartOffset = state.referenceIndex.higherKey(indexSegmentStartOffset);

      // FileSpan from start offset of message to index end offset
      FileSpan fileSpan = new FileSpan(fileSpanForMessage.getStartOffset(), state.index.getCurrentEndOffset());
      verifyValue(id, state.index.findKey(id, fileSpan));

      // FileSpan from start offset of index to end offset of message
      fileSpan = new FileSpan(state.index.getStartOffset(), fileSpanForMessage.getEndOffset());
      verifyValue(id, state.index.findKey(id, fileSpan));

      // FileSpan that includes the message
      Offset startOffset = lowerSegmentStartOffset == null ? indexSegmentStartOffset : lowerSegmentStartOffset;
      Offset endOffset =
          higherSegmentStartOffset == null ? state.index.getCurrentEndOffset() : higherSegmentStartOffset;
      fileSpan = new FileSpan(startOffset, endOffset);
      verifyValue(id, state.index.findKey(id, fileSpan));

      if (higherSegmentStartOffset != null) {
        // FileSpan higher than the entry (does not include entry)
        fileSpan = new FileSpan(higherSegmentStartOffset, state.log.getEndOffset());
        assertNull("There should have been no value returned", state.index.findKey(id, fileSpan));
      }

      if (lowerSegmentStartOffset != null) {
        // FileSpan lower than the entry (does not include entry)
        fileSpan = new FileSpan(state.index.getStartOffset(), lowerSegmentStartOffset);
        assertNull("There should have been no value returned", state.index.findKey(id, fileSpan));
      }
    }
  }

  /**
   * Tests {@link PersistentIndex#getBlobReadInfo(StoreKey, EnumSet)}.
   * Cases (all cases on all types of keys - live, expired, deleted):
   * 1. With no options
   * 2. With combinations of parameters in {@link StoreGetOptions}.
   * </p>
   * Also tests non existent keys.
   * @throws StoreException
   */
  @Test
  public void getBlobReadInfoTest() throws StoreException {
    final AtomicReference<MockId> idRequested = new AtomicReference<>();
    state.hardDelete = new MessageStoreHardDelete() {
      @Override
      public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory,
          List<byte[]> recoveryInfoList) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory factory) throws IOException {
        MockId id = idRequested.get();
        if (id == null) {
          throw new IllegalStateException("No ID was set before making a call to getBlobReadInfo()");
        }
        IndexValue value = state.getExpectedValue(id, true);
        return new MessageInfo(id, value.getSize(), value.getExpiresAtMs());
      }
    };
    state.reloadIndex(true, false);
    List<EnumSet<StoreGetOptions>> allCombos = new ArrayList<>();
    allCombos.add(EnumSet.noneOf(StoreGetOptions.class));
    allCombos.add(EnumSet.of(StoreGetOptions.Store_Include_Expired));
    allCombos.add(EnumSet.of(StoreGetOptions.Store_Include_Deleted));
    allCombos.add(EnumSet.allOf(StoreGetOptions.class));
    for (MockId id : state.allKeys.keySet()) {
      idRequested.set(id);
      for (EnumSet<StoreGetOptions> getOptions : allCombos) {
        if (state.liveKeys.contains(id)) {
          verifyBlobReadOptions(id, getOptions, null);
        } else if (state.expiredKeys.contains(id)) {
          StoreErrorCodes expectedErrorCode =
              getOptions.contains(StoreGetOptions.Store_Include_Expired) ? null : StoreErrorCodes.TTL_Expired;
          verifyBlobReadOptions(id, getOptions, expectedErrorCode);
        } else if (state.deletedKeys.contains(id)) {
          StoreErrorCodes expectedErrorCode =
              getOptions.contains(StoreGetOptions.Store_Include_Deleted) ? null : StoreErrorCodes.ID_Deleted;
          verifyBlobReadOptions(id, getOptions, expectedErrorCode);
        }
      }
    }
    // try to get BlobReadOption for a non existent key
    MockId nonExistentId = state.getUniqueId();
    verifyBlobReadOptions(nonExistentId, EnumSet.allOf(StoreGetOptions.class), StoreErrorCodes.ID_Not_Found);
  }

  /**
   * Tests {@link PersistentIndex#findMissingKeys(List)}.
   * @throws StoreException
   */
  @Test
  public void findMissingKeysTest() throws StoreException {
    List<StoreKey> idsToProvide = new ArrayList<StoreKey>(state.allKeys.keySet());
    Set<StoreKey> nonExistentIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      nonExistentIds.add(state.getUniqueId());
    }
    idsToProvide.addAll(nonExistentIds);
    Collections.shuffle(idsToProvide);
    Set<StoreKey> missingKeys = state.index.findMissingKeys(idsToProvide);
    assertEquals("Set of missing keys not as expected", nonExistentIds, missingKeys);
  }

  /**
   * Tests error cases for {@link PersistentIndex#addToIndex(IndexEntry, FileSpan)}.
   * Cases:
   * 1. FileSpan end offset < currentIndexEndOffset
   * 2. FileSpan is across segments
   * @throws StoreException
   */
  @Test
  public void addEntryBadInputTest() throws StoreException {
    // FileSpan end offset < currentIndexEndOffset
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getStartOffset(), 1);
    IndexValue value =
        new IndexValue(1, state.index.getStartOffset(), IndexValue.FLAGS_DEFAULT_VALUE, Utils.Infinite_Time,
            Utils.Infinite_Time);
    try {
      state.index.addToIndex(new IndexEntry(state.getUniqueId(), value), fileSpan);
      fail("Should have failed because filespan provided < currentIndexEndOffset");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    if (isLogSegmented) {
      // FileSpan spans across segments
      Offset startOffset = state.index.getCurrentEndOffset();
      String nextLogSegmentName = LogSegmentNameHelper.getNextPositionName(startOffset.getName());
      Offset endOffset = new Offset(nextLogSegmentName, 0);
      fileSpan = new FileSpan(startOffset, endOffset);
      try {
        state.index.addToIndex(new IndexEntry(state.getUniqueId(), value), fileSpan);
        fail("Should have failed because fileSpan provided spanned across segments");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }
  }

  /**
   * Tests error cases for {@link PersistentIndex#markAsDeleted(StoreKey, FileSpan)}.
   * Cases
   * 1. FileSpan end offset < currentIndexEndOffset
   * 2. FileSpan is across segments
   * 3. ID does not exist
   * 4. ID already deleted
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void markAsDeletedBadInputTest() throws IOException, StoreException {
    // FileSpan end offset < currentIndexEndOffset
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getStartOffset(), 1);
    try {
      state.index.markAsDeleted(state.liveKeys.iterator().next(), fileSpan);
      fail("Should have failed because filespan provided < currentIndexEndOffset");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    if (isLogSegmented) {
      // FileSpan spans across segments
      Offset startOffset = state.index.getCurrentEndOffset();
      String nextLogSegmentName = LogSegmentNameHelper.getNextPositionName(startOffset.getName());
      Offset endOffset = new Offset(nextLogSegmentName, 0);
      fileSpan = new FileSpan(startOffset, endOffset);
      try {
        state.index.markAsDeleted(state.liveKeys.iterator().next(), fileSpan);
        fail("Should have failed because fileSpan provided spanned across segments");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }

    state.appendToLog(5);
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), 5);
    // ID does not exist
    try {
      state.index.markAsDeleted(state.getUniqueId(), fileSpan);
      fail("Should have failed because ID provided for delete does not exist");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Not_Found, e.getErrorCode());
    }

    // ID already deleted
    try {
      state.index.markAsDeleted(state.deletedKeys.iterator().next(), fileSpan);
      fail("Should have failed because ID provided for delete is already deleted");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Deleted, e.getErrorCode());
    }
  }

  /**
   * Tests that hard delete is kicked off by the index.
   * @throws StoreException
   */
  @Test
  public void hardDeleteKickOffTest() throws StoreException {
    assertFalse("HardDelete should not be enabled", state.index.hardDeleter.isRunning());
    state.properties.put("store.enable.hard.delete", "true");
    state.reloadIndex(true, false);
    assertTrue("HardDelete is not enabled", state.index.hardDeleter.isRunning());
  }

  /**
   * Tests that hard delete zeros out blobs correctly and makes progress as expected.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void hardDeleteTest() throws InterruptedException, IOException, StoreException {
    state.properties.put("store.deleted.message.retention.days", Integer.toString(1));
    state.properties.put("store.hard.delete.bytes.per.sec", Integer.toString(Integer.MAX_VALUE / 10));
    state.reloadIndex(true, false);
    state.index.hardDeleter.enabled.set(true);
    assertFalse("Hard delete did work even though no message is past retention time",
        state.index.hardDeleter.hardDelete());
    // IndexSegment still uses real time so advance time so that it goes 2 days past the real time.
    state.advanceTime(SystemTime.getInstance().milliseconds() + 2 * Time.MsPerSec * Time.SecsPerDay);
    assertTrue("Hard delete did not do any work", state.index.hardDeleter.hardDelete());
    long expectedProgress = state.index.getAbsolutePositionInLogForOffset(state.logOrder.lastKey());
    assertEquals("Hard delete did not make expected progress", expectedProgress, state.index.hardDeleter.getProgress());
    state.verifyEntriesForHardDeletes(state.deletedKeys);
  }

  /**
   * Tests that hard delete pause and resume where in hard deletes is done by the daemon thread and not explicitly
   * invoked by the tests
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void hardDeletePauseResumeTest() throws InterruptedException, IOException, StoreException {
    testHardDeletePauseResume(false);
  }

  /**
   * Tests that hard delete pause and resume with reloading of index, where in hard deletes is done by the daemon thread
   * and not explicitly invoked by the tests
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void hardDeletePauseResumeRestartTest() throws InterruptedException, IOException, StoreException {
    testHardDeletePauseResume(true);
  }

  /**
   * Tests that expired values are correctly handled.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void expirationTest() throws InterruptedException, IOException, StoreException {

    // add a PUT entry that will expire if time advances by 1 second
    // advance time so that time moves to whole second with no residual milliseconds
    state.time.sleep(Time.MsPerSec - state.time.milliseconds());
    state.addPutEntries(1, 1, state.time.milliseconds());
    MockId id = state.logOrder.lastEntry().getValue().getFirst();
    verifyBlobReadOptions(id, EnumSet.noneOf(StoreGetOptions.class), null);
    state.advanceTime(Time.MsPerSec);
    verifyBlobReadOptions(id, EnumSet.noneOf(StoreGetOptions.class), StoreErrorCodes.TTL_Expired);
  }

  /**
   * Tests that end offsets are set correctly in log segments when the index is created.
   * Cases
   * 1. Current end offsets have been set correctly
   * 2. Add data to log but not index, restart and check that end offsets have been reset.
   * 3. Add data to log such that a new log segment is created but not to index, restart and check that the new log
   * segment is gone.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void setEndOffsetsTest() throws IOException, StoreException {
    // check that current end offsets set are correct
    LogSegment segment = state.log.getFirstSegment();
    while (segment != null) {
      Offset lastRecordStartOffset = getLastRecordOffset(segment);
      long size = state.logOrder.get(lastRecordStartOffset).getSecond().indexValue.getSize();
      Offset expectedEndOffset = state.log.getFileSpanForMessage(lastRecordStartOffset, size).getEndOffset();
      assertEquals("End offset of segment not as expected", expectedEndOffset.getOffset(), segment.getEndOffset());
      segment = state.log.getNextSegment(segment);
    }

    // write some data to the log but not the index, check that end offset of the segment has changed
    // reload the index and check the end offset has been reset
    LogSegment activeSegment = state.log.getSegment(state.index.getCurrentEndOffset().getName());
    long offsetBeforeAppend = activeSegment.getEndOffset();
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
    assertEquals("End offset of active segment did not change",
        offsetBeforeAppend + CuratedLogIndexState.PUT_RECORD_SIZE, activeSegment.getEndOffset());
    state.reloadIndex(true, false);
    assertEquals("End offset of active segment should have been reset", offsetBeforeAppend,
        activeSegment.getEndOffset());

    if (isLogSegmented) {
      // this test works under the assumption that log segments are not allocated until they are required
      // this is a fair assumption because the PersistentIndex works under the same assumption and would break if it
      // were not true (which this test failing would indicate).

      // write some data to the log but not the index such that new segment is created, check that end offset of the
      // segment has changed and a new segment created, reload the index and check the end offset has been reset and
      // the new segment does not exist.
      activeSegment = state.log.getSegment(state.index.getCurrentEndOffset().getName());
      offsetBeforeAppend = activeSegment.getEndOffset();
      // fill up this segment
      state.appendToLog(activeSegment.getCapacityInBytes() - activeSegment.getEndOffset());
      assertEquals("End offset of active segment did not change", activeSegment.getCapacityInBytes(),
          activeSegment.getEndOffset());
      // write a little more so that a new segment is created
      state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
      LogSegment nextActiveSegment = state.log.getNextSegment(activeSegment);
      assertNotNull("New segment has not been created", nextActiveSegment);
      assertEquals("Unexpected end offset for new segment", CuratedLogIndexState.PUT_RECORD_SIZE,
          nextActiveSegment.getEndOffset() - nextActiveSegment.getStartOffset());
      state.reloadIndex(true, false);
      // there should no longer be a "next" segment to the old active segment
      assertNull("There should have been no more segments", state.log.getNextSegment(activeSegment));
      assertEquals("End offset of active segment should have been reset", offsetBeforeAppend,
          activeSegment.getEndOffset());
    }
  }

  /**
   * Tests {@link PersistentIndex#changeIndexSegments(List, Set)} for good and bad cases.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void changeIndexSegmentsTest() throws IOException, StoreException {
    ConcurrentSkipListMap<Offset, IndexSegment> indexes = state.index.getIndexSegments();
    Set<Offset> saved = indexes.clone().keySet();

    Map<Offset, IndexSegment> toRemoveOne = new HashMap<>();
    Set<Offset> toRetainOne = new HashSet<>();

    partitionIndexSegments(toRemoveOne, toRetainOne);

    // remove the first batch without adding anything
    state.index.changeIndexSegments(Collections.EMPTY_LIST, toRemoveOne.keySet());
    assertEquals("Offsets in index do not match expected", toRetainOne, state.index.getIndexSegments().keySet());
    // make sure persist does not throw errors
    state.index.persistIndex();

    Map<Offset, IndexSegment> toRemoveTwo = new HashMap<>();
    Set<Offset> toRetainTwo = new HashSet<>();

    partitionIndexSegments(toRemoveTwo, toRetainTwo);

    // remove the second batch and add the batch that was removed earlier
    toRetainTwo.addAll(toRemoveOne.keySet());
    List<File> filesToAdd = new ArrayList<>();
    for (IndexSegment indexSegment : toRemoveOne.values()) {
      filesToAdd.add(indexSegment.getFile());
    }
    state.index.changeIndexSegments(filesToAdd, toRemoveTwo.keySet());
    assertEquals("Offsets in index do not match expected", toRetainTwo, state.index.getIndexSegments().keySet());
    // make sure persist does not throw errors
    state.index.persistIndex();

    // add the second batch that was removed
    filesToAdd.clear();
    for (IndexSegment indexSegment : toRemoveTwo.values()) {
      filesToAdd.add(indexSegment.getFile());
    }
    state.index.changeIndexSegments(filesToAdd, Collections.EMPTY_SET);
    assertEquals("Offsets in index do not match expected", saved, state.index.getIndexSegments().keySet());
    // make sure persist does not throw errors
    state.index.persistIndex();

    // error case
    // try to remove the last segment (its offset is in the journal)
    try {
      state.index.changeIndexSegments(Collections.EMPTY_LIST, Collections.singleton(state.referenceIndex.lastKey()));
      fail("Should have failed to remove index segment because start offset is past the first offset in the journal");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // add an entry into the journal for the very first offset in the index
    state.index.journal.addEntry(state.logOrder.firstKey(), state.logOrder.firstEntry().getValue().getFirst());
    // remove the first index segment
    IndexSegment firstSegment = state.index.getIndexSegments().remove(state.index.getIndexSegments().firstKey());
    // try to add it back and it should result in an error
    try {
      state.index.changeIndexSegments(Collections.singletonList(firstSegment.getFile()), Collections.EMPTY_SET);
      fail("Should have failed to add index segment because its end offset is past the first offset in the journal");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Test that verifies that there are no concurrency issues with the execution of
   * {@link PersistentIndex#addToIndex(IndexEntry, FileSpan)} and {@link PersistentIndex#changeIndexSegments(List, Set)}
   * @throws Exception
   */
  @Test
  public void addToIndexAndChangeIndexSegmentsConcurrencyTest() throws Exception {
    long ITERATIONS = 500;
    final Set<Offset> indexSegmentStartOffsets = new HashSet<>(state.referenceIndex.keySet());
    final AtomicReference<Offset> logEndOffset = new AtomicReference<>(state.log.getEndOffset());
    final AtomicReference<Exception> exception = new AtomicReference<>(null);
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    // make sure rollover occurs on every index entry
    state.properties.put("store.index.max.number.of.inmem.elements", "1");
    state.reloadIndex(true, false);
    state.appendToLog(ITERATIONS);

    final Set<IndexEntry> entriesAdded = Collections.newSetFromMap(new ConcurrentHashMap<IndexEntry, Boolean>());
    Runnable adder = new Runnable() {
      @Override
      public void run() {
        try {
          FileSpan fileSpan = state.log.getFileSpanForMessage(logEndOffset.get(), 1);
          IndexValue value = new IndexValue(1, fileSpan.getStartOffset(), Utils.Infinite_Time);
          IndexEntry entry = new IndexEntry(state.getUniqueId(), value);
          state.index.addToIndex(entry, fileSpan);
          logEndOffset.set(fileSpan.getEndOffset());
          entriesAdded.add(entry);
          indexSegmentStartOffsets.add(fileSpan.getStartOffset());
        } catch (Exception e) {
          exception.set(e);
        } finally {
          latch.get().countDown();
        }
      }
    };
    final List<IndexSegment> candidateIndexSegments = new ArrayList<>();
    for (IndexSegment indexSegment : state.index.getIndexSegments().values()) {
      if (indexSegment.getEndOffset().compareTo(state.index.journal.getFirstOffset()) < 0) {
        candidateIndexSegments.add(indexSegment);
      }
    }
    Runnable changer = new Runnable() {
      @Override
      public void run() {
        try {
          int idx = TestUtils.RANDOM.nextInt(candidateIndexSegments.size());
          IndexSegment segmentToUse = candidateIndexSegments.get(idx);
          // remove the index segment
          state.index.changeIndexSegments(Collections.EMPTY_LIST, Collections.singleton(segmentToUse.getStartOffset()));
          // ensure that the relevant index segment is gone.
          if (state.index.getIndexSegments().containsKey(segmentToUse.getStartOffset())) {
            throw new IllegalStateException(
                "Segment with offset " + segmentToUse.getStartOffset() + " should have been" + " removed.");
          }
          // add it back
          state.index.changeIndexSegments(Collections.singletonList(segmentToUse.getFile()), Collections.EMPTY_SET);
          // ensure that the relevant index segment is back.
          if (!state.index.getIndexSegments().containsKey(segmentToUse.getStartOffset())) {
            throw new IllegalStateException(
                "Segment with offset " + segmentToUse.getStartOffset() + " should have been" + " present.");
          }
        } catch (Exception e) {
          exception.set(e);
        } finally {
          latch.get().countDown();
        }
      }
    };

    for (int i = 0; i < ITERATIONS; i++) {
      latch.set(new CountDownLatch(2));
      executorService.submit(adder);
      executorService.submit(changer);
      assertTrue("Took too long to add/change index segments", latch.get().await(1, TimeUnit.SECONDS));
      if (exception.get() != null) {
        throw exception.get();
      }
      assertEquals("Index segment start offsets do not match expected", indexSegmentStartOffsets,
          state.index.getIndexSegments().keySet());
    }
  }

  /**
   * Tests success cases for recovery.
   * Cases
   * 1. Single segment recovery
   * 2. Multiple segment recovery
   * 3. Recovery after index is completely lost
   * In all cases, the tests also verify that end offsets are set correctly.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void recoverySuccessTest() throws InterruptedException, IOException, StoreException {
    state.advanceTime(1);
    singleSegmentRecoveryTest();
    if (isLogSegmented) {
      multipleSegmentRecoveryTest();
    }
    totalIndexLossRecoveryTest();
  }

  /**
   * Tests recovery failure cases.
   * Cases
   * 1. Recovery info contains a PUT for a key that already exists
   * 2. Recovery info contains a PUT for a key that has been deleted
   * 3. Recovery info contains a DELETE for a key that has been deleted
   * 4. Recovery info that contains a DELETE for a key that has no PUT record
   * 5. Recovery info that contains a PUT beyond the end offset of the log segment
   */
  @Test
  public void recoveryFailureTest() {
    // recovery info contains a PUT for a key that already exists
    MessageInfo info = new MessageInfo(state.liveKeys.iterator().next(), CuratedLogIndexState.PUT_RECORD_SIZE);
    doRecoveryFailureTest(info, StoreErrorCodes.Initialization_Error);
    // recovery info contains a PUT for a key that has been deleted
    info = new MessageInfo(state.deletedKeys.iterator().next(), CuratedLogIndexState.PUT_RECORD_SIZE);
    doRecoveryFailureTest(info, StoreErrorCodes.Initialization_Error);
    // recovery info contains a DELETE for a key that has been deleted
    info = new MessageInfo(state.deletedKeys.iterator().next(), CuratedLogIndexState.DELETE_RECORD_SIZE, true);
    doRecoveryFailureTest(info, StoreErrorCodes.ID_Deleted);
    // recovery info that contains a DELETE for a key that has no PUT record
    info = new MessageInfo(state.getUniqueId(), CuratedLogIndexState.DELETE_RECORD_SIZE, true);
    doRecoveryFailureTest(info, StoreErrorCodes.ID_Not_Found);
    // recovery info that contains a PUT beyond the end offset of the log segment
    info = new MessageInfo(state.getUniqueId(), CuratedLogIndexState.PUT_RECORD_SIZE);
    doRecoveryFailureTest(info, StoreErrorCodes.Index_Creation_Failure);
  }

  /**
   * Tests {@link PersistentIndex#findEntriesSince(FindToken, long)} for various cases
   * 1. All cases that result in getting an index based token
   * 2. All cases that result in getting a journal based token
   * 3. Getting entries one by one
   * 4. Getting entries using an index based token for an offset in the journal
   * 5. Error case - trying to findEntriesSince() using an index based token that contains the last index segment
   * 6. Using findEntriesSince() in an empty index
   * 7. Token that has the log end offset
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceTest() throws InterruptedException, IOException, StoreException {
    // add some more entries so that the journal gets entries across segments and doesn't start at the beginning
    // of an index segment.
    state.addPutEntries(7, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));

    // token with log end offset should not return anything
    StoreFindToken token = new StoreFindToken(state.log.getEndOffset(), state.sessionId, state.incarnationId, false);
    token.setBytesRead(state.index.getLogUsedCapacity());
    doFindEntriesSinceTest(token, Long.MAX_VALUE, Collections.EMPTY_SET, token);

    findEntriesSinceToIndexBasedTest();
    findEntriesSinceToJournalBasedTest();
    findEntriesSinceOneByOneTest();
    findEntriesSinceIndexBasedTokenForOffsetInJournalTest();

    // error case - can never have provided an index based token that is contains the offset of the last segment
    token = new StoreFindToken(state.referenceIndex.lastEntry().getValue().firstKey(), state.referenceIndex.lastKey(),
        state.sessionId, state.incarnationId);
    doFindEntriesSinceFailureTest(token, StoreErrorCodes.Unknown_Error);

    findEntriesSinceInEmptyIndexTest(false);
  }

  /**
   * Tests behaviour of {@link PersistentIndex#findEntriesSince(FindToken, long)} on crash-restart of index and some
   * recovery. Specifically tests cases where tokens have been handed out before the "crash" failure.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceOnRestartTest() throws IOException, StoreException {
    Offset lastRecordOffset = state.index.journal.getLastOffset();
    state.appendToLog(2 * CuratedLogIndexState.PUT_RECORD_SIZE);
    // this record will be recovered.
    FileSpan firstRecordFileSpan =
        state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), CuratedLogIndexState.PUT_RECORD_SIZE);
    // this record will not be recovered.
    FileSpan secondRecordFileSpan =
        state.log.getFileSpanForMessage(firstRecordFileSpan.getEndOffset(), CuratedLogIndexState.PUT_RECORD_SIZE);

    // if there is no bad shutdown but the store token is past the index end offset, it is an error state
    StoreFindToken startToken =
        new StoreFindToken(secondRecordFileSpan.getStartOffset(), new UUID(1, 1), state.incarnationId, false);
    doFindEntriesSinceFailureTest(startToken, StoreErrorCodes.Unknown_Error);

    UUID oldSessionId = state.sessionId;
    final MockId newId = state.getUniqueId();
    // add to allKeys() so that doFindEntriesSinceTest() works correctly.
    state.allKeys.put(newId, new Pair<IndexValue, IndexValue>(
        new IndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, firstRecordFileSpan.getStartOffset(), Utils.Infinite_Time),
        null));
    state.recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        return Collections.singletonList(new MessageInfo(newId, CuratedLogIndexState.PUT_RECORD_SIZE));
      }
    };
    state.reloadIndex(true, true);

    // If there is no incarnationId in the incoming token, for backwards compatibility purposes we consider it as valid
    // and proceed with session id validation and so on.
    UUID[] incarnationIds = new UUID[]{state.incarnationId, null};
    for (UUID incarnationIdToTest : incarnationIds) {
      long bytesRead = state.index.getAbsolutePositionInLogForOffset(firstRecordFileSpan.getEndOffset());
      // create a token that will be past the index end offset on startup after recovery.
      if (incarnationIdToTest == null) {
        startToken = getTokenWithNullIncarnationId(
            new StoreFindToken(secondRecordFileSpan.getEndOffset(), oldSessionId, state.incarnationId, false));
        assertNull("IncarnationId is expected to be null ", startToken.getIncarnationId());
      } else {
        startToken = new StoreFindToken(secondRecordFileSpan.getEndOffset(), oldSessionId, incarnationIdToTest, false);
      }
      // token should get reset internally, no keys should be returned and the returned token should be correct (offset
      // in it will be the current log end offset = firstRecordFileSpan.getEndOffset()).
      StoreFindToken expectedEndToken =
          new StoreFindToken(firstRecordFileSpan.getEndOffset(), state.sessionId, state.incarnationId, true);
      expectedEndToken.setBytesRead(bytesRead);
      doFindEntriesSinceTest(startToken, Long.MAX_VALUE, Collections.EMPTY_SET, expectedEndToken);

      // create a token that is not past the index end offset on startup after recovery. Should work as expected
      if (incarnationIdToTest == null) {
        startToken = getTokenWithNullIncarnationId(
            new StoreFindToken(lastRecordOffset, oldSessionId, state.incarnationId, false));
        assertNull("IncarnationId is expected to be null ", startToken.getIncarnationId());
      } else {
        startToken = new StoreFindToken(lastRecordOffset, oldSessionId, incarnationIdToTest, false);
      }
      expectedEndToken =
          new StoreFindToken(firstRecordFileSpan.getStartOffset(), state.sessionId, state.incarnationId, false);
      expectedEndToken.setBytesRead(bytesRead);
      doFindEntriesSinceTest(startToken, Long.MAX_VALUE, Collections.singleton(newId), expectedEndToken);
    }
  }

  /**
   * Generates token in {@link StoreFindToken#VERSION_1} so that incarnationId is null
   * @param token the {@link StoreFindToken} that needs be parsed to generate the token with null incarnationId
   * @return the {@link StoreFindToken} with null incarnationId
   * @throws IOException
   */
  private StoreFindToken getTokenWithNullIncarnationId(StoreFindToken token) throws IOException {
    return StoreFindToken.fromBytes(StoreFindTokenTest.getSerializedStream(token, StoreFindToken.VERSION_1),
        STORE_KEY_FACTORY);
  }

  /**
   * Tests behaviour of {@link PersistentIndex#findEntriesSince(FindToken, long)} on crash restart for the following
   * scenario
   * After restart, lets say no new writes have gone into the store.
   * For a findEntriesSince(offset beyond logEndOffsetOnStartup with different session id) call, index will reset the
   * token to logEndOffsetOnStartup and returns the same.
   * On the subsequent findEntriesSince() call, the index should start returning entries
   * starting from that offset and should not consider that token as non-inclusive
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceOnCrashRestartTest() throws InterruptedException, IOException, StoreException {
    UUID oldSessionId = state.sessionId;
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.reloadIndex(true, true);
    // create a token that will be past the log end offset on start up after restart.
    StoreFindToken startToken = new StoreFindToken(new Offset(state.log.getEndOffset().getName(),
        (state.log.getEndOffset().getOffset() + (2 * CuratedLogIndexState.PUT_RECORD_SIZE))), oldSessionId,
        state.incarnationId, false);
    // end token should point to log end offset on startup
    long bytesRead = state.index.getAbsolutePositionInLogForOffset(state.log.getEndOffset());
    StoreFindToken expectedEndToken =
        new StoreFindToken(state.log.getEndOffset(), state.sessionId, state.incarnationId, true);
    expectedEndToken.setBytesRead(bytesRead);
    // Fetch the FindToken returned from findEntriesSince
    FindInfo findInfo = state.index.findEntriesSince(startToken, Long.MAX_VALUE);
    assertEquals("EndToken mismatch ", expectedEndToken, findInfo.getFindToken());
    assertEquals("No entries should have been returned ", 0, findInfo.getMessageEntries().size());

    // add 2 entries to index and log
    Set<MockId> expectedEntries = new HashSet<>();
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    expectedEntries.add(state.logOrder.lastEntry().getValue().getFirst());
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    expectedEntries.add(state.logOrder.lastEntry().getValue().getFirst());
    bytesRead = state.index.getAbsolutePositionInLogForOffset(state.index.getCurrentEndOffset());
    expectedEndToken =
        new StoreFindToken(state.index.journal.getLastOffset(), state.sessionId, state.incarnationId, false);
    expectedEndToken.setBytesRead(bytesRead);
    doFindEntriesSinceTest((StoreFindToken) findInfo.getFindToken(), Long.MAX_VALUE, expectedEntries, expectedEndToken);
  }

  /**
   * Tests behaviour of {@link PersistentIndex#findEntriesSince(FindToken, long)} relating to incarnationId
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceIncarnationIdTest() throws IOException, StoreException {
    Offset lastRecordOffset = state.index.journal.getLastOffset();
    state.appendToLog(2 * CuratedLogIndexState.PUT_RECORD_SIZE);
    // will be recovered
    FileSpan firstRecordFileSpan =
        state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), CuratedLogIndexState.PUT_RECORD_SIZE);
    // will not be recovered
    FileSpan secondRecordFileSpan =
        state.log.getFileSpanForMessage(firstRecordFileSpan.getEndOffset(), CuratedLogIndexState.PUT_RECORD_SIZE);

    UUID oldSessionId = state.sessionId;
    UUID oldIncarnationId = state.incarnationId;
    final MockId newId = state.getUniqueId();
    // add to allKeys() so that doFindEntriesSinceTest() works correctly.
    state.allKeys.put(newId, new Pair<IndexValue, IndexValue>(
        new IndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, firstRecordFileSpan.getStartOffset(), Utils.Infinite_Time),
        null));
    state.recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        return Collections.singletonList(new MessageInfo(newId, CuratedLogIndexState.PUT_RECORD_SIZE));
      }
    };
    // change in incarnationId
    state.incarnationId = UUID.randomUUID();
    state.reloadIndex(true, true);

    long bytesRead = state.index.getAbsolutePositionInLogForOffset(firstRecordFileSpan.getEndOffset());
    // create a token that will be past the index end offset on startup after recovery with old incarnationId
    StoreFindToken startToken =
        new StoreFindToken(secondRecordFileSpan.getEndOffset(), oldSessionId, oldIncarnationId, false);
    // token should get reset internally, all keys should be returned and the returned token should be pointing to
    // start offset of firstRecordFileSpan.
    StoreFindToken expectedEndToken =
        new StoreFindToken(firstRecordFileSpan.getStartOffset(), state.sessionId, state.incarnationId, false);
    expectedEndToken.setBytesRead(bytesRead);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, state.allKeys.keySet(), expectedEndToken);

    // create a token that is not past the index end offset on startup after recovery with old incarnationId.
    // token should get reset internally, all keys should be returned and the returned token should be be pointing to
    // start offset of firstRecordFileSpan.
    startToken = new StoreFindToken(lastRecordOffset, oldSessionId, oldIncarnationId, false);
    expectedEndToken =
        new StoreFindToken(firstRecordFileSpan.getStartOffset(), state.sessionId, state.incarnationId, false);
    expectedEndToken.setBytesRead(bytesRead);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, state.allKeys.keySet(), expectedEndToken);
  }

  /**
   * Tests {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} for various cases
   * 1. All cases that result in getting an index based token
   * 2. All cases that result in getting a journal based token
   * 3. Getting entries one by one
   * 4. Getting entries using an index based token for an offset in the journal
   * 5. Using findDeletedEntriesSince() in an empty index
   * 6. Token that has the log end offset
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void findDeletedEntriesSinceTest() throws InterruptedException, IOException, StoreException {
    // add some more entries so that the journal gets entries across segments and doesn't start at the beginning
    // of an index segment.
    state.addPutEntries(7, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    MockId idToDelete = state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey());
    state.addDeleteEntry(idToDelete);

    // token with log end offset should not return anything
    StoreFindToken token = new StoreFindToken(state.log.getEndOffset(), state.sessionId, state.incarnationId, false);
    doFindDeletedEntriesSinceTest(token, Long.MAX_VALUE, Collections.EMPTY_SET, token);

    findDeletedEntriesSinceToIndexBasedTest();
    findDeletedEntriesSinceToJournalBasedTest();
    findDeletedEntriesSinceOneByOneTest();
    findDeletedEntriesSinceIndexBasedTokenForOffsetInJournalTest();

    findEntriesSinceInEmptyIndexTest(true);
  }

  /**
   * Tests the index segment roll over when there is a change in IndexValue size. With introduction of
   * {@link PersistentIndex#VERSION_1} there is a change in IndexValue
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testIndexSegmentRollOverNewIndexSegmentVersion() throws StoreException, IOException {
    indexSegmentRollOverTest(true);
    indexSegmentRollOverTest(false);
  }

  /**
   * Tests the Index persistor for all cases
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void indexPersistorTest() throws InterruptedException, IOException, StoreException {
    // add new entries which may not be persisted
    // call persist and reload the index (index.close() is never called)
    Offset indexEndOffset = state.index.getCurrentEndOffset();
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    MockId id = state.logOrder.lastEntry().getValue().getFirst();
    state.index.persistIndex();
    state.reloadIndex(false, false);
    assertEquals("Index End offset mismatch ",
        new Offset(indexEndOffset.getName(), indexEndOffset.getOffset() + CuratedLogIndexState.PUT_RECORD_SIZE),
        state.index.getCurrentEndOffset());
    verifyValue(id, state.index.findKey(id));

    // add entries to index alone.
    // index.persist() should throw an exception since log end offset is < index end offset
    FileSpan fileSpan = new FileSpan(state.index.getCurrentEndOffset(),
        new Offset(state.index.getCurrentEndOffset().getName(),
            state.index.getCurrentEndOffset().getOffset() + CuratedLogIndexState.PUT_RECORD_SIZE));
    IndexValue value =
        new IndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, fileSpan.getStartOffset(), Utils.Infinite_Time);
    MockId newId = state.getUniqueId();
    state.index.addToIndex(new IndexEntry(newId, value), fileSpan);
    try {
      state.index.persistIndex();
      fail("Should have thrown exception since index has entries which log does not have");
    } catch (StoreException e) {
      assertEquals("StoreException error code mismatch ", StoreErrorCodes.Illegal_Index_State, e.getErrorCode());
    }
    // append to log so that log and index are in sync with each other
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
  }

  /**
   * Tests {@link PersistentIndex#getAbsolutePositionInLogForOffset(Offset)}.
   */
  @Test
  public void getAbsolutePositionForOffsetTest() {
    List<LogSegment> logSegments = new ArrayList<>();
    LogSegment segment = state.log.getFirstSegment();
    while (segment != null) {
      logSegments.add(segment);
      segment = state.log.getNextSegment(segment);
    }

    long numLogSegmentsPreceding = 0;
    for (LogSegment logSegment : logSegments) {
      verifyAbsolutePosition(new Offset(logSegment.getName(), 0L), numLogSegmentsPreceding);
      verifyAbsolutePosition(new Offset(logSegment.getName(), logSegment.getStartOffset()), numLogSegmentsPreceding);
      long randomPosRange = logSegment.getEndOffset() - logSegment.getStartOffset() - 1;
      long randomPos = Utils.getRandomLong(TestUtils.RANDOM, randomPosRange) + logSegment.getStartOffset() + 1;
      verifyAbsolutePosition(new Offset(logSegment.getName(), randomPos), numLogSegmentsPreceding);
      verifyAbsolutePosition(new Offset(logSegment.getName(), logSegment.getEndOffset()), numLogSegmentsPreceding);
      numLogSegmentsPreceding++;
    }
  }

  /**
   * Tests {@link PersistentIndex#getAbsolutePositionInLogForOffset(Offset)} with bad arguments.
   */
  @Test
  public void getAbsolutePositionForOffsetBadArgsTest() {
    Offset badSegmentOffset = new Offset(LogSegmentNameHelper.getName(state.index.getLogSegmentCount() + 1, 0), 0);
    Offset badOffsetOffset =
        new Offset(state.log.getFirstSegment().getName(), state.log.getFirstSegment().getCapacityInBytes() + 1);
    List<Offset> offsetsToCheck = new ArrayList<>();
    offsetsToCheck.add(badSegmentOffset);
    offsetsToCheck.add(badOffsetOffset);

    for (Offset offset : offsetsToCheck) {
      try {
        state.index.getAbsolutePositionInLogForOffset(offset);
        fail("Should have failed to get absolute position for invalid offset input: " + offset);
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }
  }

  /**
   * Test that verifies that everything is ok even if {@link MessageStoreHardDelete} instance provided is null.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void hardDeleteNullTest() throws InterruptedException, IOException, StoreException {
    state.hardDelete = null;
    state.reloadIndex(true, false);
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    MockId idToCheck = state.logOrder.lastEntry().getValue().getFirst();
    state.reloadIndex(true, false);
    verifyValue(idToCheck, state.index.findKey(idToCheck));
  }

  /**
   * Tests correctness of {@link PersistentIndex#getIndexSegmentFilesForLogSegment(String, String)} and makes sure
   * it picks up all the files.
   */
  @Test
  public void getIndexSegmentFilesForLogSegmentTest() {
    LogSegment logSegment = state.log.getFirstSegment();
    while (logSegment != null) {
      LogSegment nextSegment = state.log.getNextSegment(logSegment);
      File[] indexSegmentFiles =
          PersistentIndex.getIndexSegmentFilesForLogSegment(tempDir.getAbsolutePath(), logSegment.getName());
      Arrays.sort(indexSegmentFiles, PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
      Offset from = new Offset(logSegment.getName(), logSegment.getStartOffset());
      Offset to = new Offset(logSegment.getName(), logSegment.getEndOffset());
      Set<Offset> offsets = state.referenceIndex.subMap(from, true, to, true).keySet();
      assertEquals("Number of index segment files inconsistent", offsets.size(), indexSegmentFiles.length);
      int i = 0;
      for (Offset offset : offsets) {
        assertEquals("Index segment file inconsistent with offset expected", offset,
            IndexSegment.getIndexSegmentStartOffset(indexSegmentFiles[i].getName()));
        i++;
      }
      logSegment = nextSegment;
    }
  }

  /**
   * Tests {@link PersistentIndex#cleanupIndexSegmentFilesForLogSegment(String, String)} and makes sure it deletes all
   * the relevant files and no more.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void cleanupIndexSegmentFilesForLogSegmentTest() throws IOException, StoreException {
    state.index.close();
    LogSegment logSegment = state.log.getFirstSegment();
    while (logSegment != null) {
      LogSegment nextSegment = state.log.getNextSegment(logSegment);
      final String logSegmentName = logSegment.getName();
      int totalFilesInDir = tempDir.listFiles().length;
      File[] filesToCheck = tempDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(logSegmentName) && (name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX)
              || name.endsWith(IndexSegment.BLOOM_FILE_NAME_SUFFIX));
        }
      });
      Offset from = new Offset(logSegment.getName(), logSegment.getStartOffset());
      Offset to = new Offset(logSegment.getName(), logSegment.getEndOffset());
      int expectedNumFilesToDelete = state.referenceIndex.subMap(from, true, to, true).size() * 2;
      if (nextSegment == null) {
        // latest index segment does not have a bloom file
        expectedNumFilesToDelete--;
      }
      assertEquals("Number of files to check does not match expectation", expectedNumFilesToDelete,
          filesToCheck.length);
      PersistentIndex.cleanupIndexSegmentFilesForLogSegment(tempDir.getAbsolutePath(), logSegmentName);
      for (File fileToCheck : filesToCheck) {
        assertFalse(fileToCheck + " should have been deleted", fileToCheck.exists());
      }
      assertEquals("More than the expected number of files were deleted", totalFilesInDir - filesToCheck.length,
          tempDir.listFiles().length);
      logSegment = nextSegment;
    }
  }

  // helpers

  // general

  /**
   * Verifies that the {@link BlobReadOptions} returned from {@link PersistentIndex#getBlobReadInfo(StoreKey, EnumSet)}
   * matches the expected value.
   * @param id the {@link MockId} to use
   * @param storeGetOptions the {@link StoreGetOptions} to use.
   * @param expectedErrorCode if this operation is expected to fail, the {@link StoreErrorCodes} expected.
   * @throws StoreException
   */
  private void verifyBlobReadOptions(MockId id, EnumSet<StoreGetOptions> storeGetOptions,
      StoreErrorCodes expectedErrorCode) throws StoreException {
    try {
      BlobReadOptions options = state.index.getBlobReadInfo(id, storeGetOptions);
      if (expectedErrorCode != null) {
        fail("Should have failed because a StoreException is expected");
      }
      IndexValue putEntryValue = state.getExpectedValue(id, true);
      assertEquals("StoreKey not as expected", id, options.getStoreKey());
      assertEquals("Log Segment Name not as expected", putEntryValue.getOffset().getName(),
          options.getLogSegmentName());
      assertEquals("Offset not as expected", putEntryValue.getOffset().getOffset(), options.getOffset());
      assertEquals("Size not as expected", putEntryValue.getSize(), options.getSize());
      assertEquals("ExpiresAtMs not as expected", putEntryValue.getExpiresAtMs(), options.getExpiresAtMs());
    } catch (StoreException e) {
      if (expectedErrorCode == null) {
        throw e;
      } else {
        assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
      }
    }
  }

  /**
   * Compares two tokens to ensure their equality test passes and that they have the same session ID.
   * @param reference the reference {@link StoreFindToken}
   * @param toCheck the {@link StoreFindToken} to check
   */
  private void compareTokens(StoreFindToken reference, StoreFindToken toCheck) {
    assertEquals("Tokens do not match", reference, toCheck);
    assertEquals("SessionId does not match", reference.getSessionId(), toCheck.getSessionId());
  }

  // findKey test helpers

  /**
   * Verifies that {@code valueFromFind} matches the expected value from {@link CuratedLogIndexState#referenceIndex}.
   * @param id the {@link MockId} whose value is required.
   * @param valueFromFind the {@link IndexValue} that needs to be verified.
   */
  private void verifyValue(MockId id, IndexValue valueFromFind) {
    if (state.allKeys.containsKey(id)) {
      assertNotNull("Value should be successfully fetched", valueFromFind);
      IndexValue expectedValue = state.getExpectedValue(id, !state.deletedKeys.contains(id));
      assertEquals("Offset in value from index not as expected", expectedValue.getOffset(), valueFromFind.getOffset());
      assertEquals("Bytes from value from index not as expected", expectedValue.getBytes(), valueFromFind.getBytes());
    } else {
      assertNull("There should have been no value returned", valueFromFind);
    }
  }

  // setEndOffsetsTest() helpers

  /**
   * Gets the offset of the last record in the given log {@code segment}.
   * @param segment the {@link LogSegment} whose last record offset is required.
   * @return the offset of the last record in the given log {@code segment}.
   */
  private Offset getLastRecordOffset(LogSegment segment) {
    Offset lastOffset;
    LogSegment nextSegment = state.log.getNextSegment(segment);
    if (nextSegment == null) {
      lastOffset = state.logOrder.lastKey();
    } else {
      Offset nextSegmentStartOffset = new Offset(nextSegment.getName(), nextSegment.getStartOffset());
      lastOffset = state.logOrder.lowerKey(nextSegmentStartOffset);
    }
    return lastOffset;
  }

  // hardDeletePauseAndResume() helpers

  /**
   * Utility to test hard delete pause and resume with or without reload between pause and resume
   * @param reloadIndex {@code true} if index has to be reloaded inbetween pause and resume. {@code false} otherwise
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void testHardDeletePauseResume(boolean reloadIndex) throws InterruptedException, IOException, StoreException {
    state.properties.put("store.deleted.message.retention.days", Integer.toString(1));
    state.properties.put("store.hard.delete.bytes.per.sec", Integer.toString(Integer.MAX_VALUE / 10));
    state.properties.setProperty("store.data.flush.interval.seconds", "3600");
    // enable daemon thread to run hard deletes
    state.properties.put("store.enable.hard.delete", "true");
    state.reloadIndex(true, false);
    assertTrue("Hard delete is not enabled", state.index.hardDeleter.isRunning());
    // IndexSegment still uses real time so advance time so that it goes 2 days past the real time.
    state.advanceTime(2 * Time.MsPerSec * Time.SecsPerDay);
    long expectedProgress = state.index.getAbsolutePositionInLogForOffset(state.logOrder.lastKey());
    // give it some time so that hard delete completes one cycle
    waitUntilExpectedProgress(expectedProgress, 5000);
    state.verifyEntriesForHardDeletes(state.deletedKeys);
    Set<MockId> idsDeleted = new HashSet<>();
    // delete two entries
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    Set<MockId> idsToDelete = new HashSet<>();
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    for (MockId id : idsToDelete) {
      state.addDeleteEntry(id);
    }
    idsDeleted.addAll(idsToDelete);
    // pause hard delete
    state.index.hardDeleter.pause();
    assertTrue("Hard deletes should have been paused ", state.index.hardDeleter.isPaused());
    waitUntilExpectedState(Thread.State.WAITING, HardDeleter.HARD_DELETE_SLEEP_TIME_ON_CAUGHT_UP_MS + 1, 10);

    // delete two entries
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    idsToDelete.clear();
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
    for (MockId id : idsToDelete) {
      state.addDeleteEntry(id);
    }
    idsDeleted.addAll(idsToDelete);

    if (reloadIndex) {
      state.reloadIndex(true, true);
      waitUntilExpectedState(Thread.State.WAITING, HardDeleter.HARD_DELETE_SLEEP_TIME_ON_CAUGHT_UP_MS + 1, 10);
      idsToDelete.clear();
      state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
      state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey()));
      for (MockId id : idsToDelete) {
        state.addDeleteEntry(id);
      }
      idsDeleted.addAll(idsToDelete);
    }

    // advance time so that deleted entries becomes eligible to be hard deleted
    state.advanceTime(2 * Time.MsPerSec * Time.SecsPerDay);
    // resume and verify new entries have been hard deleted
    state.index.hardDeleter.resume();
    assertFalse("Hard deletes should have been resumed ", state.index.hardDeleter.isPaused());
    expectedProgress = state.index.getAbsolutePositionInLogForOffset(state.logOrder.lastKey());
    // after resuming. hard deletes should progress. Give it some time to hard delete next range
    waitUntilExpectedProgress(expectedProgress, 5000);
    state.verifyEntriesForHardDeletes(idsDeleted);
  }

  // recoverySuccessTest() helpers

  /**
   * Test recovery of a single segment.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void singleSegmentRecoveryTest() throws InterruptedException, IOException, StoreException {
    Offset indexEndOffsetBeforeRecovery = state.index.getCurrentEndOffset();
    MockId idToCreateAndDelete = state.getUniqueId();
    // recover a few messages in a single segment
    final List<MessageInfo> infos = getCuratedSingleSegmentRecoveryInfos(idToCreateAndDelete);
    final AtomicInteger returnTracker = new AtomicInteger(0);
    state.recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        switch (returnTracker.getAndIncrement()) {
          case 0:
            return infos;
          default:
            throw new IllegalStateException("This function should not have been called more than once");
        }
      }
    };
    // This test relies on log segment not spilling over. If that happens, this test will fail.
    LogSegment activeSegment = state.log.getSegment(indexEndOffsetBeforeRecovery.getName());
    long expectedSegmentEndOffset = activeSegment.getEndOffset();
    // write a little "extra" data
    state.appendToLog(2 * CuratedLogIndexState.PUT_RECORD_SIZE);

    state.reloadIndex(true, false);
    assertEquals("End offset not as expected", expectedSegmentEndOffset, activeSegment.getEndOffset());
    checkInfos(infos, idToCreateAndDelete, indexEndOffsetBeforeRecovery);
  }

  /**
   * Tests recovery of more than one segment.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void multipleSegmentRecoveryTest() throws InterruptedException, IOException, StoreException {
    Offset indexEndOffsetBeforeRecovery = state.index.getCurrentEndOffset();
    LogSegment activeSegment = state.log.getSegment(indexEndOffsetBeforeRecovery.getName());
    // recover a few messages across segments
    final List<MessageInfo> activeSegmentInfos = new ArrayList<>();
    // 1 PUT record that will be deleted in the next segment
    MockId idToCreateAndDeleteAcrossSegments = state.getUniqueId();
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
    activeSegmentInfos.add(new MessageInfo(idToCreateAndDeleteAcrossSegments, CuratedLogIndexState.PUT_RECORD_SIZE));
    // 1 PUT record that will remain and covers almost the rest of the active segment.
    long size =
        activeSegment.getCapacityInBytes() - activeSegment.getEndOffset() - (CuratedLogIndexState.DELETE_RECORD_SIZE
            - 1);
    state.appendToLog(size);
    activeSegmentInfos.add(new MessageInfo(state.getUniqueId(), size));
    MockId idToCreateAndDeleteInSameSegment = state.getUniqueId();
    final List<MessageInfo> nextSegmentInfos = getCuratedSingleSegmentRecoveryInfos(idToCreateAndDeleteInSameSegment);
    // 1 DELETE record for the PUT in the previous segment
    state.appendToLog(CuratedLogIndexState.DELETE_RECORD_SIZE);
    nextSegmentInfos.add(
        new MessageInfo(idToCreateAndDeleteAcrossSegments, CuratedLogIndexState.DELETE_RECORD_SIZE, true));
    final AtomicInteger returnTracker = new AtomicInteger(0);
    state.recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        switch (returnTracker.getAndIncrement()) {
          case 0:
            return activeSegmentInfos;
          case 1:
            return nextSegmentInfos;
          default:
            throw new IllegalStateException("This function should not have been called more than two times");
        }
      }
    };
    long activeSegmentExpectedEndOffset = activeSegment.getEndOffset();
    long nextSegmentExpectedEndOffset = state.log.getNextSegment(activeSegment).getEndOffset();
    // write a little "extra" data
    state.appendToLog(2 * CuratedLogIndexState.PUT_RECORD_SIZE);

    state.reloadIndex(true, false);
    assertEquals("End offset of former active segment not as expected", activeSegmentExpectedEndOffset,
        activeSegment.getEndOffset());
    activeSegment = state.log.getNextSegment(activeSegment);
    assertNotNull("A new segment has not been created", activeSegment);
    assertEquals("End offset active segment not as expected", nextSegmentExpectedEndOffset,
        activeSegment.getEndOffset());
    checkInfos(activeSegmentInfos, idToCreateAndDeleteAcrossSegments, indexEndOffsetBeforeRecovery);
    checkInfos(nextSegmentInfos, idToCreateAndDeleteInSameSegment,
        new Offset(activeSegment.getName(), activeSegment.getStartOffset()));
  }

  /**
   * Creates a few curated recovery entries. For understanding the created entries, please read the source code which is
   * annotated with comments.
   * @param idToCreateAndDelete the {@link MockId} that will have both a PUT and DELETE entry.
   * @return curated revovery entries.
   * @throws IOException
   */
  private List<MessageInfo> getCuratedSingleSegmentRecoveryInfos(MockId idToCreateAndDelete) throws IOException {
    List<MessageInfo> infos = new ArrayList<>();
    state.appendToLog(2 * CuratedLogIndexState.DELETE_RECORD_SIZE + 4 * CuratedLogIndexState.PUT_RECORD_SIZE);
    // 1 DELETE for a PUT not in the infos
    infos.add(new MessageInfo(state.getIdToDeleteFromLogSegment(state.log.getFirstSegment()),
        CuratedLogIndexState.DELETE_RECORD_SIZE, true));
    // 3 PUT
    infos.add(new MessageInfo(idToCreateAndDelete, CuratedLogIndexState.PUT_RECORD_SIZE));
    infos.add(new MessageInfo(state.getUniqueId(), CuratedLogIndexState.PUT_RECORD_SIZE));
    infos.add(new MessageInfo(state.getUniqueId(), CuratedLogIndexState.PUT_RECORD_SIZE));
    // 1 DELETE for a PUT in the infos
    infos.add(new MessageInfo(idToCreateAndDelete, CuratedLogIndexState.DELETE_RECORD_SIZE, true));
    // 1 expired PUT
    infos.add(new MessageInfo(state.getUniqueId(), CuratedLogIndexState.PUT_RECORD_SIZE, 0));
    return infos;
  }

  /**
   * Checks that the provided {@code infos} is present in the index.
   * @param infos the {@link List} of {@link MessageInfo} whose presence needs to be checked in the index.
   * @param putRecordIdToIgnore the {@link MockId} whose PUT {@link MessageInfo} has to be ignored.
   * @param indexEndOffsetBeforeRecovery the end offset of the {@link PersistentIndex} before recovery.
   * @throws StoreException
   */
  private void checkInfos(List<MessageInfo> infos, MockId putRecordIdToIgnore, Offset indexEndOffsetBeforeRecovery)
      throws StoreException {
    Offset currCheckOffset = indexEndOffsetBeforeRecovery;
    for (MessageInfo info : infos) {
      FileSpan expectedFileSpan = state.log.getFileSpanForMessage(currCheckOffset, info.getSize());
      if (!info.getStoreKey().equals(putRecordIdToIgnore) || info.isDeleted()) {
        IndexValue value = state.index.findKey(info.getStoreKey());
        assertEquals("Incorrect value for start offset", currCheckOffset, value.getOffset());
        assertEquals("Inconsistent size", info.getSize(), value.getSize());
        assertEquals("Inconsistent delete state ", info.isDeleted(), value.isFlagSet(IndexValue.Flags.Delete_Index));
        assertEquals("Inconsistent expiresAtMs", info.getExpirationTimeInMs(), value.getExpiresAtMs());
      }
      currCheckOffset = expectedFileSpan.getEndOffset();
    }
  }

  /**
   * Tests the case where the index is lost completely and needs to be recovered from scratch.
   * @throws StoreException
   */
  private void totalIndexLossRecoveryTest() throws StoreException {
    state.closeAndClearIndex();
    final AtomicInteger returnTracker = new AtomicInteger(0);
    state.recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        switch (returnTracker.getAndIncrement()) {
          case 0:
            return Collections.singletonList(
                new MessageInfo(state.getUniqueId(), CuratedLogIndexState.PUT_RECORD_SIZE));
          default:
            return Collections.emptyList();
        }
      }
    };
    state.reloadIndex(true, false);
    assertEquals("Incorrect log segment count", 1, state.index.getLogSegmentCount());
    assertEquals("Index should contain exactly one index segment", 1, state.index.getIndexSegments().size());
    LogSegment segment = state.log.getFirstSegment();
    assertEquals("End offset not as expected",
        new Offset(segment.getName(), segment.getStartOffset() + CuratedLogIndexState.PUT_RECORD_SIZE),
        state.index.getCurrentEndOffset());
  }

  // recoveryFailureTest() helpers

  /**
   * Tests that recovery fails for {@code info}.
   * @param info the {@link MessageInfo} which will cause recovery to fail.
   * @param expectedErrorCode the {@link StoreErrorCodes} expected for the failure.
   */
  private void doRecoveryFailureTest(final MessageInfo info, StoreErrorCodes expectedErrorCode) {
    state.recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        return Collections.singletonList(info);
      }
    };
    try {
      state.reloadIndex(true, false);
      fail("Loading index should have failed because recovery contains invalid info");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
    }
  }

  // findEntriesSinceTest() helpers

  /**
   * Tests all cases of {@link PersistentIndex#findEntriesSince(FindToken, long)} that result in an index based
   * {@link StoreFindToken} being returned.
   * 1. Uninited -> Index
   * 2. Index -> Index
   * 3. Journal -> Index
   * @throws StoreException
   */
  private void findEntriesSinceToIndexBasedTest() throws StoreException {
    Offset logAbsoluteZero = new Offset(state.log.getFirstSegment().getName(), 0);

    // ------------------
    // 1. Index -> Index
    Offset firstIndexSegmentStartOffset = state.referenceIndex.firstKey();
    Offset secondIndexSegmentStartOffset = state.referenceIndex.higherKey(firstIndexSegmentStartOffset);
    MockId firstId = state.referenceIndex.get(firstIndexSegmentStartOffset).firstKey();
    // All elements from first index segment and two from the second to be returned (because of size restrictions)
    Set<MockId> expectedKeys = new HashSet<>();
    long maxTotalSizeOfEntries = 0;
    for (Map.Entry<MockId, IndexValue> segmentEntry : state.referenceIndex.get(firstIndexSegmentStartOffset)
        .entrySet()) {
      if (!segmentEntry.getKey().equals(firstId)) {
        expectedKeys.add(segmentEntry.getKey());
        maxTotalSizeOfEntries += segmentEntry.getValue().getSize();
      }
    }
    TreeMap<MockId, IndexValue> secondIndexSegment = state.referenceIndex.get(secondIndexSegmentStartOffset);
    Map.Entry<MockId, IndexValue> secondIndexSegmentEntry = secondIndexSegment.firstEntry();
    expectedKeys.add(secondIndexSegmentEntry.getKey());
    maxTotalSizeOfEntries += secondIndexSegmentEntry.getValue().getSize();
    secondIndexSegmentEntry = secondIndexSegment.higherEntry(secondIndexSegmentEntry.getKey());
    expectedKeys.add(secondIndexSegmentEntry.getKey());
    maxTotalSizeOfEntries += secondIndexSegmentEntry.getValue().getSize();

    StoreFindToken startToken =
        new StoreFindToken(firstId, firstIndexSegmentStartOffset, state.sessionId, state.incarnationId);
    StoreFindToken expectedEndToken =
        new StoreFindToken(secondIndexSegmentEntry.getKey(), secondIndexSegmentStartOffset, state.sessionId,
            state.incarnationId);
    expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(secondIndexSegmentStartOffset));
    doFindEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 2. Uninitialized -> Index
    // add firstStoreKey and its size
    expectedKeys.add(firstId);
    maxTotalSizeOfEntries += state.allKeys.get(firstId).getFirst().getSize();
    doFindEntriesSinceTest(new StoreFindToken(), maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 3. Journal -> Index
    // create a journal based token for an offset that isn't in the journal
    startToken = new StoreFindToken(state.logOrder.firstKey(), state.sessionId, state.incarnationId, false);
    doFindEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);
  }

  /**
   * Tests all cases of {@link PersistentIndex#findEntriesSince(FindToken, long)} that result in an journal based
   * {@link StoreFindToken} being returned.
   * 1. Uninited -> Journal
   * 2. Index -> Journal
   * 3. Journal -> Journal
   * 4. No movement.
   * @throws StoreException
   */
  private void findEntriesSinceToJournalBasedTest() throws StoreException {
    StoreFindToken absoluteEndToken =
        new StoreFindToken(state.logOrder.lastKey(), state.sessionId, state.incarnationId, false);
    absoluteEndToken.setBytesRead(state.index.getLogUsedCapacity());

    // ------------------
    // 1. Uninitialized -> Journal
    doFindEntriesSinceTest(new StoreFindToken(), Long.MAX_VALUE, state.allKeys.keySet(), absoluteEndToken);

    // ------------------
    // 2. Index -> Journal
    Offset firstIndexSegmentStartOffset = state.referenceIndex.firstKey();
    StoreKey firstStoreKey = state.referenceIndex.get(firstIndexSegmentStartOffset).firstKey();
    StoreFindToken startToken =
        new StoreFindToken(firstStoreKey, firstIndexSegmentStartOffset, state.sessionId, state.incarnationId);
    Set<MockId> expectedKeys = new HashSet<>(state.allKeys.keySet());
    if (!state.deletedKeys.contains(firstStoreKey)) {
      // if firstStoreKey has not been deleted, it will not show up in findEntries since its PUT record is ignored
      expectedKeys.remove(firstStoreKey);
    }
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 3. Journal -> Journal
    // a. Token no longer in journal
    startToken = new StoreFindToken(state.logOrder.firstKey(), state.sessionId, state.incarnationId, false);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, state.allKeys.keySet(), absoluteEndToken);

    // b. Token still in journal
    startToken = new StoreFindToken(state.index.journal.getFirstOffset(), state.sessionId, state.incarnationId, false);
    expectedKeys = new HashSet<>();
    for (Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> entry : state.logOrder.tailMap(
        startToken.getOffset(), false).entrySet()) {
      expectedKeys.add(entry.getValue().getFirst());
    }
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // c. Token still in journal with inclusiveness set to true
    startToken = new StoreFindToken(state.index.journal.getFirstOffset(), state.sessionId, state.incarnationId, true);
    expectedKeys.add(state.logOrder.tailMap(startToken.getOffset(), true).firstEntry().getValue().getFirst());
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 4. Journal no change
    doFindEntriesSinceTest(absoluteEndToken, Long.MAX_VALUE, Collections.EMPTY_SET, absoluteEndToken);
  }

  /**
   * Uses {@link PersistentIndex#findEntriesSince(FindToken, long)} to get entries one by one.
   * @throws StoreException
   */
  private void findEntriesSinceOneByOneTest() throws StoreException {
    Offset logAbsoluteZero = new Offset(state.log.getFirstSegment().getName(), 0);
    Offset journalStartOffset = state.index.journal.getFirstOffset();
    StoreFindToken startToken = new StoreFindToken();
    Offset stoppedAt = null;
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry : state.referenceIndex.entrySet()) {
      Offset indexSegmentStartOffset = indexEntry.getKey();
      // We get index based tokens as long as
      // 1. The original token is index based
      // 2. The size of entries being obtained is <= the size of records in the current index segment
      if (indexSegmentStartOffset.compareTo(journalStartOffset) >= 0) {
        stoppedAt = indexSegmentStartOffset;
        break;
      }
      for (Map.Entry<MockId, IndexValue> indexSegmentEntry : indexEntry.getValue().entrySet()) {
        MockId id = indexSegmentEntry.getKey();
        StoreFindToken expectedEndToken =
            new StoreFindToken(id, indexSegmentStartOffset, state.sessionId, state.incarnationId);
        expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(indexSegmentStartOffset));
        doFindEntriesSinceTest(startToken, indexSegmentEntry.getValue().getSize(), Collections.singleton(id),
            expectedEndToken);
        startToken = expectedEndToken;
      }
    }

    Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> logEntry = state.logOrder.floorEntry(stoppedAt);
    while (logEntry != null) {
      Offset startOffset = logEntry.getKey();
      MockId id = logEntry.getValue().getFirst();
      Pair<IndexValue, IndexValue> putDelete = state.allKeys.get(id);
      // size returned is the size of the delete if the key has been deleted.
      long size = putDelete.getSecond() != null ? putDelete.getSecond().getSize() : putDelete.getFirst().getSize();
      StoreFindToken expectedEndToken = new StoreFindToken(startOffset, state.sessionId, state.incarnationId, false);
      Offset endOffset = state.log.getFileSpanForMessage(startOffset, size).getEndOffset();
      expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(endOffset));
      doFindEntriesSinceTest(startToken, size, Collections.singleton(id), expectedEndToken);
      startToken = expectedEndToken;
      logEntry = state.logOrder.higherEntry(logEntry.getKey());
    }
  }

  /**
   * Tests {@link PersistentIndex#findEntriesSince(FindToken, long)} when an index based {@link StoreFindToken} has been
   * given out for an offset in the {@link Journal}.
   * @throws StoreException
   */
  private void findEntriesSinceIndexBasedTokenForOffsetInJournalTest() throws StoreException {
    Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry =
        state.referenceIndex.floorEntry(state.index.journal.getFirstOffset());
    Offset nextIndexSegmentStartOffset = state.referenceIndex.higherKey(indexEntry.getKey());
    MockId firstIdInSegment = indexEntry.getValue().firstKey();
    StoreFindToken startToken =
        new StoreFindToken(firstIdInSegment, indexEntry.getKey(), state.sessionId, state.incarnationId);
    long maxSize = 0;
    Set<MockId> expectedKeys = new HashSet<>();
    for (Map.Entry<MockId, IndexValue> indexSegmentEntry : indexEntry.getValue().entrySet()) {
      if (!firstIdInSegment.equals(indexSegmentEntry.getKey())) {
        expectedKeys.add(indexSegmentEntry.getKey());
        maxSize += indexSegmentEntry.getValue().getSize();
      }
    }
    MockId logId = state.logOrder.get(nextIndexSegmentStartOffset).getFirst();
    expectedKeys.add(logId);
    long size = state.deletedKeys.contains(logId) ? CuratedLogIndexState.DELETE_RECORD_SIZE
        : state.logOrder.get(nextIndexSegmentStartOffset).getSecond().indexValue.getSize();
    maxSize += size;

    Offset endOffset = state.log.getFileSpanForMessage(nextIndexSegmentStartOffset, size).getEndOffset();
    StoreFindToken expectedEndToken =
        new StoreFindToken(nextIndexSegmentStartOffset, state.sessionId, state.incarnationId, false);
    expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(endOffset));
    doFindEntriesSinceTest(startToken, maxSize, expectedKeys, expectedEndToken);
  }

  /**
   * Does the test for {@link PersistentIndex#findEntriesSince(FindToken, long)} and compares the token received to
   * the expected token. It also verifies that the index entries obtained are as expected.
   * @param startToken the {@link StoreFindToken} to provide to the function.
   * @param maxTotalSizeOfEntries the total size of entries to fetch
   * @param expectedKeys the keys expected in the returned entries.
   * @param expectedEndToken the {@link StoreFindToken} expected to be returned.
   * @throws StoreException
   */
  private void doFindEntriesSinceTest(StoreFindToken startToken, long maxTotalSizeOfEntries, Set<MockId> expectedKeys,
      StoreFindToken expectedEndToken) throws StoreException {
    FindInfo findInfo = state.index.findEntriesSince(startToken, maxTotalSizeOfEntries);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    compareTokens(expectedEndToken, token);
    assertEquals("Returned token should have the right number of bytes read", expectedEndToken.getBytesRead(),
        token.getBytesRead());
    List<MessageInfo> infos = findInfo.getMessageEntries();
    Set<StoreKey> keysExamined = new HashSet<>();
    for (MessageInfo info : infos) {
      Pair<IndexValue, IndexValue> putDelete = state.allKeys.get(info.getStoreKey());
      IndexValue value = putDelete.getFirst();
      // size returned is hard to predict if the key has been deleted - it depends on the locations of the PUT and
      // DELETE entries and whether both or one of them is present in the return list. It is not useful to recompute the
      // situations here and check since there isn't currently a reason to depend on the size if the record has been
      // deleted.
      if (putDelete.getSecond() == null) {
        assertEquals("Inconsistent size", value.getSize(), info.getSize());
      }
      // if a key is deleted, it doesn't matter if we reached the delete record or not, the delete state will be
      // the one that is returned.
      assertEquals("Inconsistent delete state ", putDelete.getSecond() != null, info.isDeleted());
      assertEquals("Inconsistent expiresAtMs", value.getExpiresAtMs(), info.getExpirationTimeInMs());
      keysExamined.add(info.getStoreKey());
    }
    assertEquals("All keys should be present", expectedKeys, keysExamined);
  }

  // findEntriesSinceTest() and findEntriesSinceOnRestartTest() helpers

  /**
   * Tests for failure of {@link PersistentIndex#findEntriesSince(FindToken, long)}.
   * @param token the {@link StoreFindToken} to provide to the function.
   * @param expectedCode the expected {@link StoreErrorCodes}.
   */
  private void doFindEntriesSinceFailureTest(StoreFindToken token, StoreErrorCodes expectedCode) {
    try {
      state.index.findEntriesSince(token, Long.MAX_VALUE);
      fail("Should have failed because token is beyond the end offset in the index");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedCode, e.getErrorCode());
    }
  }

  // findEntriesSinceTest(), findDeletedEntriesSinceTest() and findEntriesSinceOnRestartTest() helpers

  /**
   * Tests the case where {@link PersistentIndex#findEntriesSince(FindToken, long)} or
   * {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} is run on an empty index.
   * @param deletedEntries if {@code true}, the test is on
   * {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)}.
   * @throws StoreException
   */
  private void findEntriesSinceInEmptyIndexTest(boolean deletedEntries) throws StoreException {
    state.closeAndClearIndex();
    state.reloadIndex(true, false);
    StoreFindToken token = new StoreFindToken();
    token.setBytesRead(0);
    if (deletedEntries) {
      doFindDeletedEntriesSinceTest(token, Long.MAX_VALUE, Collections.EMPTY_SET, token);
    } else {
      doFindEntriesSinceTest(token, Long.MAX_VALUE, Collections.EMPTY_SET, token);
    }
  }

  // findDeletedEntriesSinceTest() helpers

  /**
   * Tests all cases of {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} that result in an index
   * based {@link StoreFindToken} being returned.
   * 1. Uninited -> Index
   * 2. Index -> Index
   * 3. Journal -> Index
   * @throws StoreException
   */
  private void findDeletedEntriesSinceToIndexBasedTest() throws StoreException {
    // ------------------
    // 1. Index -> Index
    Offset secondIndexSegmentStartOffset = state.referenceIndex.higherKey(state.referenceIndex.firstKey());
    Map.Entry<MockId, IndexValue> firstSegmentEntry =
        state.referenceIndex.get(secondIndexSegmentStartOffset).firstEntry();
    // Most elements from the second to be returned (because of size restrictions)
    Set<MockId> expectedKeys = new HashSet<>();
    long maxTotalSizeOfEntries = 0;
    MockId lastKey = null;
    for (Map.Entry<MockId, IndexValue> segmentEntry : state.referenceIndex.get(secondIndexSegmentStartOffset)
        .entrySet()) {
      if (!segmentEntry.equals(firstSegmentEntry)) {
        if (segmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
          expectedKeys.add(segmentEntry.getKey());
        }
        maxTotalSizeOfEntries += segmentEntry.getValue().getSize();
      }
      lastKey = segmentEntry.getKey();
    }
    StoreFindToken startToken =
        new StoreFindToken(firstSegmentEntry.getKey(), secondIndexSegmentStartOffset, state.sessionId,
            state.incarnationId);
    StoreFindToken expectedEndToken =
        new StoreFindToken(lastKey, secondIndexSegmentStartOffset, state.sessionId, state.incarnationId);
    doFindDeletedEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 2. Uninitialized -> Index
    // add size of values and any keys that are supposed to be returned from the first index segment
    for (Map.Entry<MockId, IndexValue> segmentEntry : state.referenceIndex.firstEntry().getValue().entrySet()) {
      if (segmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
        expectedKeys.add(segmentEntry.getKey());
      }
      maxTotalSizeOfEntries += segmentEntry.getValue().getSize();
    }
    // add size of value of firstIdInSegment
    maxTotalSizeOfEntries += firstSegmentEntry.getValue().getSize();
    if (firstSegmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
      expectedKeys.add(firstSegmentEntry.getKey());
    }
    doFindDeletedEntriesSinceTest(new StoreFindToken(), maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 3. Journal -> Index
    // create a journal based token for an offset that isn't in the journal
    startToken = new StoreFindToken(state.logOrder.firstKey(), state.sessionId, state.incarnationId, false);
    doFindDeletedEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);
  }

  /**
   * Tests all cases of {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} that result in an journal
   * based {@link StoreFindToken} being returned.
   * 1. Uninited -> Journal
   * 2. Index -> Journal
   * 3. Journal -> Journal
   * 4. No movement.
   * @throws StoreException
   */
  private void findDeletedEntriesSinceToJournalBasedTest() throws StoreException {
    StoreFindToken absoluteEndToken =
        new StoreFindToken(state.logOrder.lastKey(), state.sessionId, state.incarnationId, false);

    // ------------------
    // 1. Uninitialized -> Journal
    doFindDeletedEntriesSinceTest(new StoreFindToken(), Long.MAX_VALUE, state.deletedKeys, absoluteEndToken);

    // ------------------
    // 2. Index -> Journal
    Offset secondIndexSegmentStartOffset = state.referenceIndex.higherKey(state.referenceIndex.firstKey());
    // second index segment contains the first delete entry
    StoreKey firstDeletedKey = getDeletedKeyFromIndexSegment(secondIndexSegmentStartOffset);
    StoreFindToken startToken =
        new StoreFindToken(firstDeletedKey, secondIndexSegmentStartOffset, state.sessionId, state.incarnationId);
    Set<MockId> expectedKeys = new HashSet<>(state.deletedKeys);
    expectedKeys.remove(firstDeletedKey);
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 3. Journal -> Journal
    // a. Token no longer in journal
    startToken = new StoreFindToken(state.allKeys.get(firstDeletedKey).getSecond().getOffset(), state.sessionId,
        state.incarnationId, false);
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, state.deletedKeys, absoluteEndToken);

    // b. Token still in journal
    startToken = new StoreFindToken(state.index.journal.getFirstOffset(), state.sessionId, state.incarnationId, false);
    expectedKeys.clear();
    for (Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> entry : state.logOrder.tailMap(
        startToken.getOffset(), false).entrySet()) {
      if (entry.getValue().getSecond().indexValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
        expectedKeys.add(entry.getValue().getFirst());
      }
    }
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 4. Journal no change
    doFindDeletedEntriesSinceTest(absoluteEndToken, Long.MAX_VALUE, Collections.EMPTY_SET, absoluteEndToken);
  }

  /**
   * Uses {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} to get entries one by one.
   * @throws StoreException
   */
  private void findDeletedEntriesSinceOneByOneTest() throws StoreException {
    Offset journalStartOffset = state.index.journal.getFirstOffset();
    StoreFindToken startToken = new StoreFindToken();
    Offset stoppedAt = null;
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry : state.referenceIndex.entrySet()) {
      Offset indexSegmentStartOffset = indexEntry.getKey();
      // We get index based tokens as long as
      // 1. The original token is index based
      // 2. The size of entries being obtained is <= the size of records in the current index segment
      if (indexSegmentStartOffset.compareTo(journalStartOffset) >= 0) {
        stoppedAt = indexSegmentStartOffset;
        break;
      }
      for (Map.Entry<MockId, IndexValue> indexSegmentEntry : indexEntry.getValue().entrySet()) {
        MockId id = indexSegmentEntry.getKey();
        boolean isDeleted = indexSegmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index);
        StoreFindToken expectedEndToken =
            new StoreFindToken(id, indexSegmentStartOffset, state.sessionId, state.incarnationId);
        long size = indexSegmentEntry.getValue().getSize();
        doFindDeletedEntriesSinceTest(startToken, size, isDeleted ? Collections.singleton(id) : Collections.EMPTY_SET,
            expectedEndToken);
        startToken = expectedEndToken;
      }
    }

    Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> logEntry = state.logOrder.floorEntry(stoppedAt);
    while (logEntry != null) {
      Offset startOffset = logEntry.getKey();
      MockId id = logEntry.getValue().getFirst();
      Pair<IndexValue, IndexValue> putDelete = state.allKeys.get(id);
      boolean isDeleted = putDelete.getSecond() != null;
      // size returned is the size of the delete if the key has been deleted.
      long size = isDeleted ? putDelete.getSecond().getSize() : putDelete.getFirst().getSize();
      StoreFindToken expectedEndToken = new StoreFindToken(startOffset, state.sessionId, state.incarnationId, false);
      doFindDeletedEntriesSinceTest(startToken, size, isDeleted ? Collections.singleton(id) : Collections.EMPTY_SET,
          expectedEndToken);
      startToken = expectedEndToken;
      logEntry = state.logOrder.higherEntry(logEntry.getKey());
    }
  }

  /**
   * Tests {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} when an index based
   * {@link StoreFindToken} has been given out for an offset in the {@link Journal}.
   * @throws StoreException
   */
  private void findDeletedEntriesSinceIndexBasedTokenForOffsetInJournalTest() throws StoreException {
    Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry =
        state.referenceIndex.floorEntry(state.index.journal.getFirstOffset());
    Offset nextIndexSegmentStartOffset = state.referenceIndex.higherKey(indexEntry.getKey());
    MockId firstIdInSegment = indexEntry.getValue().firstKey();
    StoreFindToken startToken =
        new StoreFindToken(firstIdInSegment, indexEntry.getKey(), state.sessionId, state.incarnationId);
    long maxSize = 0;
    Set<MockId> expectedKeys = new HashSet<>();
    for (Map.Entry<MockId, IndexValue> indexSegmentEntry : indexEntry.getValue().entrySet()) {
      if (!firstIdInSegment.equals(indexSegmentEntry.getKey())) {
        if (indexSegmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
          expectedKeys.add(indexSegmentEntry.getKey());
        }
        maxSize += indexSegmentEntry.getValue().getSize();
      }
    }
    MockId logId = state.logOrder.get(nextIndexSegmentStartOffset).getFirst();
    long size = state.logOrder.get(nextIndexSegmentStartOffset).getSecond().indexValue.getSize();
    if (state.deletedKeys.contains(logId)) {
      expectedKeys.add(logId);
      size = CuratedLogIndexState.DELETE_RECORD_SIZE;
    }
    maxSize += size;

    StoreFindToken expectedEndToken =
        new StoreFindToken(nextIndexSegmentStartOffset, state.sessionId, state.incarnationId, false);
    doFindDeletedEntriesSinceTest(startToken, maxSize, expectedKeys, expectedEndToken);
  }

  /**
   * Does the test for {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)}  and compares the token
   * received to the expected token. It also verifies that the index entries obtained are as expected.
   * @param startToken the {@link StoreFindToken} to provide to the function.
   * @param maxTotalSizeOfEntries the total size of entries to fetch
   * @param expectedKeys the keys expected in the returned entries.
   * @param expectedEndToken the {@link StoreFindToken} expected to be returned.
   * @throws StoreException
   */
  private void doFindDeletedEntriesSinceTest(StoreFindToken startToken, long maxTotalSizeOfEntries,
      Set<MockId> expectedKeys, StoreFindToken expectedEndToken) throws StoreException {
    FindInfo findInfo = state.index.findDeletedEntriesSince(startToken, maxTotalSizeOfEntries, Long.MAX_VALUE);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    compareTokens(expectedEndToken, token);
    List<MessageInfo> infos = findInfo.getMessageEntries();
    Set<StoreKey> keysExamined = new HashSet<>();
    for (MessageInfo info : infos) {
      IndexValue value = state.allKeys.get(info.getStoreKey()).getSecond();
      assertEquals("Inconsistent size", value.getSize(), info.getSize());
      assertTrue("Not deleted", info.isDeleted());
      assertEquals("Inconsistent expiresAtMs", value.getExpiresAtMs(), info.getExpirationTimeInMs());
      keysExamined.add(info.getStoreKey());
    }
    assertEquals("All keys should be present", expectedKeys, keysExamined);
  }

  /**
   * Gets a deleted key from the index segment with start offset {@code indexSegmentStartOffset}.
   * @param indexSegmentStartOffset the start {@link Offset} of the index segment from which a deleted key is required
   * @return a deleted key from the index segment with start offset {@code indexSegmentStartOffset}.
   */
  private MockId getDeletedKeyFromIndexSegment(Offset indexSegmentStartOffset) {
    MockId deletedId = null;
    for (Map.Entry<MockId, IndexValue> indexSegmentEntry : state.referenceIndex.get(indexSegmentStartOffset)
        .entrySet()) {
      if (indexSegmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
        deletedId = indexSegmentEntry.getKey();
        break;
      }
    }
    return deletedId;
  }

  // getAbsolutePositionForOffsetTest() helpers

  /**
   * Verifies that the absolute position returned for {@code offset} is correct.
   * @param offset the {@link Offset} whose absolute position needs to be verified.
   * @param numLogSegmentsPreceding number of log segments that precede the log segment that contains {@code offset}.
   */
  private void verifyAbsolutePosition(Offset offset, long numLogSegmentsPreceding) {
    long expectedPosition = numLogSegmentsPreceding * state.log.getSegmentCapacity() + offset.getOffset();
    assertEquals("Position not as expected", expectedPosition, state.index.getAbsolutePositionInLogForOffset(offset));
  }

  // hardDelete tests helpers

  /**
   * Sleeps in intervals of 10ms until expected progress is made or {@code maxTimeToCheck} is reached
   * @param expectedProgress expected progress value
   * @param maxTimeToCheck max time to check if expected progress is reached
   * @throws InterruptedException
   */
  private void waitUntilExpectedProgress(long expectedProgress, int maxTimeToCheck) throws InterruptedException {
    int sleptSoFar = 0;
    while (expectedProgress != state.index.hardDeleter.getProgress()) {
      sleptSoFar += 5;
      Thread.sleep(5);
      if (sleptSoFar >= maxTimeToCheck) {
        fail("HardDelete failed to catch up in " + maxTimeToCheck + ". Expected " + expectedProgress + ", actual "
            + state.index.hardDeleter.getProgress());
      }
      state.index.hardDeleter.preLogFlush();
      state.index.hardDeleter.postLogFlush();
    }
  }

  /**
   * Waits until the HardDeleter thread is in the {@code expectedState} for the specified {@code timeToCheckInMs} time.
   * @param expectedState Expected HardDeleter thread state
   * @param timeToCheckInMs time for which the state has to be checked for
   * @param intervalToCheckInMs interval at which the check has to be done
   */
  private void waitUntilExpectedState(Thread.State expectedState, long timeToCheckInMs, long intervalToCheckInMs)
      throws InterruptedException {
    long timeSoFar = 0;
    while (expectedState != state.index.hardDeleteThread.getState()) {
      Thread.sleep(intervalToCheckInMs);
      timeSoFar += intervalToCheckInMs;
      if (timeSoFar >= timeToCheckInMs) {
        fail("Hard Deleter thread state failed to move to " + Thread.State.WAITING + " in " + timeToCheckInMs);
      }
    }
  }

  // changeIndexSegmentsTest() helpers

  /**
   * Partitions all the available index segments in {@link CuratedLogIndexState#index} into ones that will be removed
   * and ones that will be retained.
   * @param toRemove a {@link Map} that will contain all the {@link Offset} and {@link IndexSegment} instances that need
   *                 to be removed.
   * @param toRetain a {@link Set} that will contain all the {@link Offset} of {@link IndexSegment} instances that need
   *                 to be retained.
   */
  private void partitionIndexSegments(Map<Offset, IndexSegment> toRemove, Set<Offset> toRetain) {
    int i = 0;
    for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : state.index.getIndexSegments().entrySet()) {
      if (indexSegmentEntry.getValue().getEndOffset().compareTo(state.index.journal.getFirstOffset()) >= 0
          || i % 3 != 0) {
        toRetain.add(indexSegmentEntry.getKey());
      } else {
        toRemove.put(indexSegmentEntry.getKey(), indexSegmentEntry.getValue());
      }
      i++;
    }
  }

  // Index roll over test helpers

  /**
   * Tests that the index segment rolls over when there is a version change in the index value or index segment
   * @param rollOverWithPutRecord {@code true} if the entry that causes rollover should be a put record,
   *                              {@code false} if the entry that causes rollover should be a delete record
   * @throws StoreException
   * @throws IOException
   */
  private void indexSegmentRollOverTest(boolean rollOverWithPutRecord) throws StoreException, IOException {
    state.closeAndClearIndex();
    Offset currentEndOffset = state.index.getCurrentEndOffset();

    List<IndexEntry> indexEntries = new ArrayList<>();
    // create an index entry in Version_0
    IndexEntry entry = new IndexEntry(state.getUniqueId(),
        IndexValueTest.getIndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, currentEndOffset, state.time.milliseconds(),
            PersistentIndex.VERSION_0));
    // create Index Segment in PersistentIndex.Version_0
    IndexSegment indexSegment = generateIndexSegmentV0(entry.getValue().getOffset(), entry.getKey().sizeInBytes(),
        entry.getValue().getBytes().capacity());
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
    FileSpan fileSpan = state.log.getFileSpanForMessage(currentEndOffset, CuratedLogIndexState.PUT_RECORD_SIZE);
    indexSegment.addEntry(entry, fileSpan.getEndOffset());
    indexEntries.add(entry);
    // add more entries to the segment
    indexEntries.addAll(addPutEntries(fileSpan.getEndOffset(), indexSegment, 2, CuratedLogIndexState.PUT_RECORD_SIZE,
        Utils.Infinite_Time));
    // persist the index segment of version 0
    indexSegment.writeIndexSegmentToFile(indexSegment.getEndOffset());

    state.reloadIndex(false, false);
    int indexCount = state.index.getIndexSegments().size();
    // add an entry and verify if roll over happened
    currentEndOffset = state.index.getCurrentEndOffset();
    if (rollOverWithPutRecord) {
      indexEntries.addAll(
          addPutEntries(currentEndOffset, null, 1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time));
    } else {
      IndexEntry entryToDelete = indexEntries.get(TestUtils.RANDOM.nextInt(indexEntries.size()));
      state.appendToLog(state.DELETE_RECORD_SIZE);
      fileSpan = state.log.getFileSpanForMessage(currentEndOffset, CuratedLogIndexState.DELETE_RECORD_SIZE);
      state.index.markAsDeleted(entryToDelete.getKey(), fileSpan);
      // remove entryToDelete from indexEntries as it will be part of latest index segment
      indexEntries.remove(entryToDelete);
    }
    assertEquals("Index roll over should have happened ", indexCount + 1, state.index.getIndexSegments().size());
    currentEndOffset = state.index.getCurrentEndOffset();
    indexEntries.addAll(
        addPutEntries(currentEndOffset, null, 2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time));
    assertEquals("Index roll over should not have happened ", indexCount + 1, state.index.getIndexSegments().size());
    // verify index values
    verifyIndexValues(indexEntries);
  }

  /**
   * Generate {@link IndexSegment} of version {@link PersistentIndex#VERSION_0}
   * @param startOffset the start offset of the {@link IndexSegment}
   * @param keySize The key size that this segment supports
   * @param valueSize The value size that this segment supports
   * @return the {@link IndexSegment} created of version {@link PersistentIndex#VERSION_0}
   */
  private IndexSegment generateIndexSegmentV0(Offset startOffset, int keySize, int valueSize) {
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(tempDir.getAbsolutePath(), metricRegistry);
    StoreConfig config = new StoreConfig(new VerifiableProperties(state.properties));
    return new MockIndexSegmentV0(tempDir.getAbsolutePath(), startOffset, state.STORE_KEY_FACTORY, keySize, valueSize,
        config, metrics, state.time);
  }

  /**
   * Adds {@link IndexEntry}s to the given {@link IndexSegment} for {@link IndexValue}s of version
   * {@link PersistentIndex#VERSION_0} or to the actual {@link PersistentIndex} for {@link IndexValue}s of version
   * {@link PersistentIndex#CURRENT_VERSION}
   * @param prevEntryEndOffset end offset of last {@link IndexEntry}
   * @param indexSegment the {@link IndexSegment} to which put entries need to be added. If {@code null},
   *                     {@link IndexEntry}s will be added to the {@link PersistentIndex}
   * @param count total count of put entries to be added
   * @param size size of put entries
   * @param expiresAtMs expiration value at ms for the put entries
   * @return a list {@link IndexEntry}s added to the {@link PersistentIndex}
   * @throws IOException
   * @throws StoreException
   */
  private List<IndexEntry> addPutEntries(Offset prevEntryEndOffset, IndexSegment indexSegment, int count, long size,
      long expiresAtMs) throws IOException, StoreException {
    List<IndexEntry> indexEntries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      state.appendToLog(size);
      FileSpan fileSpan = state.log.getFileSpanForMessage(prevEntryEndOffset, size);
      IndexEntry entry;
      if (indexSegment != null) {
        entry = new IndexEntry(state.getUniqueId(),
            IndexValueTest.getIndexValue(size, prevEntryEndOffset, expiresAtMs, state.time.milliseconds(),
                IndexValue.SERVICE_CONTAINER_ID_DEFAULT_VALUE, IndexValue.SERVICE_CONTAINER_ID_DEFAULT_VALUE,
                indexSegment.getVersion()));
        indexSegment.addEntry(entry, fileSpan.getEndOffset());
      } else {
        entry = new IndexEntry(state.getUniqueId(),
            new IndexValue(size, prevEntryEndOffset, (byte) 0, expiresAtMs, state.time.milliseconds()));
        state.index.addToIndex(entry, fileSpan);
      }
      indexEntries.add(entry);
      prevEntryEndOffset = fileSpan.getEndOffset();
    }
    return indexEntries;
  }

  /**
   * Verifies that {@link IndexValue}s matches with those from the {@link PersistentIndex}
   * @param indexEntries {@link List} of {@link IndexEntry}s to be verified
   * @throws StoreException
   */
  private void verifyIndexValues(List<IndexEntry> indexEntries) throws StoreException {
    for (IndexEntry entry : indexEntries) {
      IndexValue value = state.index.findKey(entry.getKey(), null);
      IndexValue expectedValue = entry.getValue();
      assertEquals("Offset mismatch for " + entry.getKey(), expectedValue.getOffset(), value.getOffset());
      assertEquals("Size mismatch for " + entry.getKey(), expectedValue.getSize(), value.getSize());
      assertEquals("Expiration value mismatch for " + entry.getKey(), expectedValue.getExpiresAtMs(),
          value.getExpiresAtMs());
      assertEquals("Flags mismatch for " + entry.getKey(), expectedValue.getFlags(), value.getFlags());
      assertEquals("OriginalMessageOffset mismatch for " + entry.getKey(), expectedValue.getOriginalMessageOffset(),
          value.getOriginalMessageOffset());
      assertEquals("OperationTime mismatch for " + entry.getKey(), expectedValue.getOperationTimeInMs(),
          value.getOperationTimeInMs());
      assertEquals("ServiceId mismatch for " + entry.getKey(), expectedValue.getServiceId(), value.getServiceId());
      assertEquals("ContainerId mismatch for " + entry.getKey(), expectedValue.getContainerId(),
          value.getContainerId());
    }
  }
}
