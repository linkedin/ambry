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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests for {@link PersistentIndex}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class IndexTest {
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

  // setupTestState() is coupled to these numbers. Changing them *will* cause setting test state or tests to fail.
  private static final long LOG_CAPACITY = 10000;
  private static final long SEGMENT_CAPACITY = 2000;
  private static final int MAX_IN_MEM_ELEMENTS = 5;
  // deliberately do not divide the capacities perfectly.
  private static final long PUT_RECORD_SIZE = 53;
  private static final long DELETE_RECORD_SIZE = 29;
  private static final long HARD_DELETE_START_OFFSET = 11;
  private static final long HARD_DELETE_LAST_PART_SIZE = 13;

  // used by getUniqueId() to make sure keys are never regenerated in a single test run.
  private final Set<MockId> generatedKeys = new HashSet<>();
  // The reference index to compare against. Key is index segment start Offset, Value is the reference index segment.
  // This reflects exactly how PersistentIndex is supposed to look.
  private final TreeMap<Offset, TreeMap<MockId, IndexValue>> referenceIndex = new TreeMap<>();
  // A map of all the keys. The key is the MockId and the value is a pair of index segment start Offsets.
  // first Offset represents the index segment start offset of the PUT entry.
  // second Offset represents the index segment start offset of the DELETE entry (null if the key has not been deleted).
  private final Map<MockId, Pair<Offset, Offset>> indexSegmentStartOffsets = new HashMap<>();
  // A map of all the keys. The key is the MockId and the value is a pair of IndexValues.
  // The first IndexValue represents the value of the PUT entry.
  // The second IndexValue represents the value of the DELETE entry (null if the key has not been deleted).
  private final Map<MockId, Pair<IndexValue, IndexValue>> allKeys = new HashMap<>();
  // Set of all deleted keys
  private final Set<MockId> deletedKeys = new HashSet<>();
  // Set of all expired keys
  private final Set<MockId> expiredKeys = new HashSet<>();
  // Set of all keys that are not deleted/expired
  private final Set<MockId> liveKeys = new HashSet<>();
  // The keys in offset order as they appear in the log.
  private final TreeMap<Offset, Pair<MockId, IndexValue>> logOrder = new TreeMap<>();

  // Indicates whether the log is segmented
  private final boolean isLogSegmented;
  // Variables that represent the folder where the data resides
  private final File tempDir;
  private final String tempDirStr;
  // the time instance that will be used in the index
  private final Time time = new MockTime();

  private final ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
  private final Properties properties = new Properties();

  // The Log which has the data
  private Log log;
  // The MessageStoreRecovery that is used with the index
  private MessageStoreRecovery recovery = new DummyMessageStoreRecovery();
  // The MessageStoreHardDelete that is used with the index
  private MessageStoreHardDelete hardDelete = new MockMessageStoreHardDelete();
  // The MetricRegistry that is used with the index
  private MetricRegistry metricRegistry;
  // The index of the log
  private PersistentIndex index;
  // The session ID associated with the index
  private UUID sessionId;

  /**
   * Mock implementation of {@link MessageStoreHardDelete} that returns {@link MessageInfo} appropriately and
   * zeroes out a well defined section of any offered blobs.
   */
  private class MockMessageStoreHardDelete implements MessageStoreHardDelete {

    @Override
    public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory,
        List<byte[]> recoveryInfoList) {
      /*
       * Returns hard delete messages that zero out well known parts of the offered blobs.
       */
      class MockMessageStoreHardDeleteIterator implements Iterator<HardDeleteInfo> {
        private final MessageReadSet readSet;
        private int count = 0;

        private MockMessageStoreHardDeleteIterator(MessageReadSet readSet) {
          this.readSet = readSet;
        }

        @Override
        public boolean hasNext() {
          return count < readSet.count();
        }

        @Override
        public HardDeleteInfo next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          count++;
          long size = readSet.sizeInBytes(count - 1) - HARD_DELETE_START_OFFSET - HARD_DELETE_LAST_PART_SIZE;
          ByteBuffer buf = ByteBuffer.allocate((int) size);
          Arrays.fill(buf.array(), (byte) 0);
          ByteBufferInputStream stream = new ByteBufferInputStream(buf);
          ReadableByteChannel channel = Channels.newChannel(stream);
          return new HardDeleteInfo(channel, buf.capacity(), 100, RECOVERY_INFO);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      }

      return new MockMessageStoreHardDeleteIterator(readSet);
    }

    @Override
    public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory factory) {
      String segmentName = ((LogSegment) read).getName();
      Pair<MockId, IndexValue> idAndValue = logOrder.get(new Offset(segmentName, offset));
      IndexValue value = idAndValue.getSecond();
      return new MessageInfo(idAndValue.getFirst(), value.getSize(), value.getExpiresAtMs());
    }
  }

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
    tempDirStr = tempDir.getAbsolutePath();
    setupTestState();
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    index.close();
    log.close();
    scheduler.shutdown();
    assertTrue(scheduler.awaitTermination(1, TimeUnit.SECONDS));
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
    for (MockId id : allKeys.keySet()) {
      IndexValue value = index.findKey(id);
      verifyValue(id, value);
    }
    // search for a non existent key
    MockId nonExistentId = getUniqueId();
    verifyValue(nonExistentId, index.findKey(nonExistentId));
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
    for (MockId id : liveKeys) {
      IndexValue expectedValue = getExpectedValue(id, true);
      FileSpan fileSpanForMessage = log.getFileSpanForMessage(expectedValue.getOffset(), expectedValue.getSize());

      // FileSpan that is exactly that of the message
      verifyValue(id, index.findKey(id, fileSpanForMessage));

      Offset indexSegmentStartOffset = indexSegmentStartOffsets.get(id).getFirst();
      Offset lowerSegmentStartOffset = referenceIndex.lowerKey(indexSegmentStartOffset);
      Offset higherSegmentStartOffset = referenceIndex.higherKey(indexSegmentStartOffset);

      // FileSpan from start offset of message to index end offset
      FileSpan fileSpan = new FileSpan(fileSpanForMessage.getStartOffset(), index.getCurrentEndOffset());
      verifyValue(id, index.findKey(id, fileSpan));

      // FileSpan from start offset of log to end offset of message
      fileSpan = new FileSpan(log.getStartOffset(), fileSpanForMessage.getEndOffset());
      verifyValue(id, index.findKey(id, fileSpan));

      // FileSpan that includes the message
      Offset startOffset = lowerSegmentStartOffset == null ? indexSegmentStartOffset : lowerSegmentStartOffset;
      Offset endOffset = higherSegmentStartOffset == null ? index.getCurrentEndOffset() : higherSegmentStartOffset;
      fileSpan = new FileSpan(startOffset, endOffset);
      verifyValue(id, index.findKey(id, fileSpan));

      if (higherSegmentStartOffset != null) {
        // FileSpan higher than the entry (does not include entry)
        fileSpan = new FileSpan(higherSegmentStartOffset, log.getEndOffset());
        assertNull("There should have been no value returned", index.findKey(id, fileSpan));
      }

      if (lowerSegmentStartOffset != null) {
        // FileSpan lower than the entry (does not include entry)
        fileSpan = new FileSpan(log.getStartOffset(), lowerSegmentStartOffset);
        assertNull("There should have been no value returned", index.findKey(id, fileSpan));
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
    hardDelete = new MessageStoreHardDelete() {
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
        IndexValue value = getExpectedValue(id, true);
        return new MessageInfo(id, value.getSize(), value.getExpiresAtMs());
      }
    };
    reloadIndex(false);
    List<EnumSet<StoreGetOptions>> allCombos = new ArrayList<>();
    allCombos.add(EnumSet.noneOf(StoreGetOptions.class));
    allCombos.add(EnumSet.of(StoreGetOptions.Store_Include_Expired));
    allCombos.add(EnumSet.of(StoreGetOptions.Store_Include_Deleted));
    allCombos.add(EnumSet.allOf(StoreGetOptions.class));
    for (MockId id : allKeys.keySet()) {
      idRequested.set(id);
      for (EnumSet<StoreGetOptions> getOptions : allCombos) {
        if (liveKeys.contains(id)) {
          verifyBlobReadOptions(id, getOptions, null);
        } else if (expiredKeys.contains(id)) {
          StoreErrorCodes expectedErrorCode =
              getOptions.contains(StoreGetOptions.Store_Include_Expired) ? null : StoreErrorCodes.TTL_Expired;
          verifyBlobReadOptions(id, getOptions, expectedErrorCode);
        } else if (deletedKeys.contains(id)) {
          StoreErrorCodes expectedErrorCode =
              getOptions.contains(StoreGetOptions.Store_Include_Deleted) ? null : StoreErrorCodes.ID_Deleted;
          verifyBlobReadOptions(id, getOptions, expectedErrorCode);
        }
      }
    }
    // try to get BlobReadOption for a non existent key
    MockId nonExistentId = getUniqueId();
    verifyBlobReadOptions(nonExistentId, EnumSet.allOf(StoreGetOptions.class), StoreErrorCodes.ID_Not_Found);
  }

  /**
   * Tests {@link PersistentIndex#findMissingKeys(List)}.
   * @throws StoreException
   */
  @Test
  public void findMissingKeysTest() throws StoreException {
    List<StoreKey> idsToProvide = new ArrayList<StoreKey>(allKeys.keySet());
    Set<StoreKey> nonExistentIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      nonExistentIds.add(getUniqueId());
    }
    idsToProvide.addAll(nonExistentIds);
    Collections.shuffle(idsToProvide);
    Set<StoreKey> missingKeys = index.findMissingKeys(idsToProvide);
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
    FileSpan fileSpan = log.getFileSpanForMessage(log.getStartOffset(), 1);
    IndexValue value = new IndexValue(1, log.getStartOffset());
    try {
      index.addToIndex(new IndexEntry(getUniqueId(), value), fileSpan);
      fail("Should have failed because filespan provided < currentIndexEndOffset");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    if (isLogSegmented) {
      // FileSpan spans across segments
      Offset startOffset = index.getCurrentEndOffset();
      String nextLogSegmentName = LogSegmentNameHelper.getNextPositionName(startOffset.getName());
      Offset endOffset = new Offset(nextLogSegmentName, 0);
      fileSpan = new FileSpan(startOffset, endOffset);
      try {
        index.addToIndex(new IndexEntry(getUniqueId(), value), fileSpan);
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
    FileSpan fileSpan = log.getFileSpanForMessage(log.getStartOffset(), 1);
    try {
      index.markAsDeleted(liveKeys.iterator().next(), fileSpan);
      fail("Should have failed because filespan provided < currentIndexEndOffset");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    if (isLogSegmented) {
      // FileSpan spans across segments
      Offset startOffset = index.getCurrentEndOffset();
      String nextLogSegmentName = LogSegmentNameHelper.getNextPositionName(startOffset.getName());
      Offset endOffset = new Offset(nextLogSegmentName, 0);
      fileSpan = new FileSpan(startOffset, endOffset);
      try {
        index.markAsDeleted(liveKeys.iterator().next(), fileSpan);
        fail("Should have failed because fileSpan provided spanned across segments");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }

    appendToLog(5);
    fileSpan = log.getFileSpanForMessage(index.getCurrentEndOffset(), 5);
    // ID does not exist
    try {
      index.markAsDeleted(getUniqueId(), fileSpan);
      fail("Should have failed because ID provided for delete does not exist");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Not_Found, e.getErrorCode());
    }

    // ID already deleted
    try {
      index.markAsDeleted(deletedKeys.iterator().next(), fileSpan);
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
    assertFalse("HardDelete should not be running", index.hardDeleter.isRunning());
    properties.put("store.enable.hard.delete", "true");
    reloadIndex(false);
    assertTrue("HardDelete is not running", index.hardDeleter.isRunning());
  }

  /**
   * Tests that hard delete zeros out blobs correctly and makes progress as expected.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void hardDeleteTest() throws InterruptedException, IOException, StoreException {
    properties.put("store.deleted.message.retention.days", Integer.toString(1));
    properties.put("store.hard.delete.bytes.per.sec", Integer.toString(Integer.MAX_VALUE / 10));
    reloadIndex(false);
    index.hardDeleter.running.set(true);
    assertFalse("Hard delete did work even though no message is past retention time", index.hardDeleter.hardDelete());
    // IndexSegment still uses real time so advance time so that it goes 2 days past the real time.
    advanceTime(SystemTime.getInstance().milliseconds() + 2 * Time.MsPerSec * Time.SecsPerDay);
    assertTrue("Hard delete did not do any work", index.hardDeleter.hardDelete());
    for (MockId id : deletedKeys) {
      IndexValue putValue = allKeys.get(id).getFirst();
      Offset offset = putValue.getOffset();
      LogSegment segment = log.getSegment(offset.getName());
      long size = putValue.getSize() - HARD_DELETE_START_OFFSET - HARD_DELETE_LAST_PART_SIZE;
      ByteBuffer readBuf = ByteBuffer.allocate((int) size);
      segment.readInto(readBuf, offset.getOffset() + HARD_DELETE_START_OFFSET);
      readBuf.flip();
      while (readBuf.hasRemaining()) {
        assertEquals("Hard delete has not zeroed out the data", (byte) 0, readBuf.get());
      }
    }
    index.hardDeleter.preLogFlush();
    index.hardDeleter.postLogFlush();
    long expectedProgress = log.getDifference(logOrder.lastKey(), new Offset(log.getFirstSegment().getName(), 0));
    assertEquals("Hard delete has not processed all keys", expectedProgress, index.hardDeleter.getProgress());
  }

  /**
   * Tests that expired values are correctly handled.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void expirationTest() throws InterruptedException, IOException, StoreException {
    // add a PUT entry that will expire if time advances by a millisecond
    addPutEntries(1, 1, time.milliseconds());
    MockId id = logOrder.lastEntry().getValue().getFirst();
    verifyBlobReadOptions(id, EnumSet.noneOf(StoreGetOptions.class), null);
    advanceTime(1);
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
    LogSegment segment = log.getFirstSegment();
    while (segment != null) {
      Offset lastRecordStartOffset = getLastRecordOffset(segment);
      long size = logOrder.get(lastRecordStartOffset).getSecond().getSize();
      Offset expectedEndOffset = log.getFileSpanForMessage(lastRecordStartOffset, size).getEndOffset();
      assertEquals("End offset of segment not as expected", expectedEndOffset.getOffset(), segment.getEndOffset());
      segment = log.getNextSegment(segment);
    }

    // write some data to the log but not the index, check that end offset of the segment has changed
    // reload the index and check the end offset has been reset
    LogSegment activeSegment = log.getSegment(index.getCurrentEndOffset().getName());
    long offsetBeforeAppend = activeSegment.getEndOffset();
    appendToLog(PUT_RECORD_SIZE);
    assertEquals("End offset of active segment did not change", offsetBeforeAppend + PUT_RECORD_SIZE,
        activeSegment.getEndOffset());
    reloadIndex(false);
    assertEquals("End offset of active segment should have been reset", offsetBeforeAppend,
        activeSegment.getEndOffset());

    if (isLogSegmented) {
      // this test works under the assumption that log segments are not allocated until they are required
      // this is a fair assumption because the PersistentIndex works under the same assumption and would break if it
      // were not true (which this test failing would indicate).

      // write some data to the log but not the index such that new segment is created, check that end offset of the
      // segment has changed and a new segment created, reload the index and check the end offset has been reset and
      // the new segment does not exist.
      activeSegment = log.getSegment(index.getCurrentEndOffset().getName());
      offsetBeforeAppend = activeSegment.getEndOffset();
      // fill up this segment
      appendToLog(activeSegment.getCapacityInBytes() - activeSegment.getEndOffset());
      assertEquals("End offset of active segment did not change", activeSegment.getCapacityInBytes(),
          activeSegment.getEndOffset());
      // write a little more so that a new segment is created
      appendToLog(PUT_RECORD_SIZE);
      LogSegment nextActiveSegment = log.getNextSegment(activeSegment);
      assertNotNull("New segment has not been created", nextActiveSegment);
      assertEquals("Unexpected end offset for new segment", PUT_RECORD_SIZE,
          nextActiveSegment.getEndOffset() - nextActiveSegment.getStartOffset());
      reloadIndex(false);
      // there should no longer be a "next" segment to the old active segment
      assertNull("There should have been no more segments", log.getNextSegment(activeSegment));
      assertEquals("End offset of active segment should have been reset", offsetBeforeAppend,
          activeSegment.getEndOffset());
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
    advanceTime(1);
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
    MessageInfo info = new MessageInfo(liveKeys.iterator().next(), PUT_RECORD_SIZE);
    doRecoveryFailureTest(info, StoreErrorCodes.Initialization_Error);
    // recovery info contains a PUT for a key that has been deleted
    info = new MessageInfo(deletedKeys.iterator().next(), PUT_RECORD_SIZE);
    doRecoveryFailureTest(info, StoreErrorCodes.Initialization_Error);
    // recovery info contains a DELETE for a key that has been deleted
    info = new MessageInfo(deletedKeys.iterator().next(), DELETE_RECORD_SIZE, true);
    doRecoveryFailureTest(info, StoreErrorCodes.ID_Deleted);
    // recovery info that contains a DELETE for a key that has no PUT record
    info = new MessageInfo(getUniqueId(), DELETE_RECORD_SIZE, true);
    doRecoveryFailureTest(info, StoreErrorCodes.ID_Not_Found);
    // recovery info that contains a PUT beyond the end offset of the log segment
    info = new MessageInfo(getUniqueId(), PUT_RECORD_SIZE);
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
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceTest() throws IOException, StoreException {
    // add some more entries so that the journal gets entries across segments and doesn't start at the beginning
    // of an index segment.
    addPutEntries(7, PUT_RECORD_SIZE, Utils.Infinite_Time);
    addDeleteEntry(getIdToDeleteFromIndexSegment(referenceIndex.lastKey()));

    // token with log end offset should not return anything
    StoreFindToken token = new StoreFindToken(log.getEndOffset(), sessionId);
    token.setBytesRead(log.getUsedCapacity());
    doFindEntriesSinceTest(token, Long.MAX_VALUE, Collections.EMPTY_SET, token);

    findEntriesSinceToIndexBasedTest();
    findEntriesSinceToJournalBasedTest();
    findEntriesSinceOneByOneTest();
    findEntriesSinceIndexBasedTokenForOffsetInJournalTest();

    // error case - can never have provided an index based token that is contains the offset of the last segment
    token = new StoreFindToken(referenceIndex.lastEntry().getValue().firstKey(), referenceIndex.lastKey(), sessionId);
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
    Offset lastRecordOffset = index.journal.getLastOffset();
    appendToLog(2 * PUT_RECORD_SIZE);
    // this record will be recovered.
    FileSpan firstRecordFileSpan = log.getFileSpanForMessage(index.getCurrentEndOffset(), PUT_RECORD_SIZE);
    // this record will not be recovered.
    FileSpan secondRecordFileSpan = log.getFileSpanForMessage(firstRecordFileSpan.getEndOffset(), PUT_RECORD_SIZE);

    // if there is no bad shutdown but the store token is past the index end offset, it is an error state
    StoreFindToken startToken = new StoreFindToken(secondRecordFileSpan.getStartOffset(), new UUID(1, 1));
    doFindEntriesSinceFailureTest(startToken, StoreErrorCodes.Unknown_Error);

    UUID oldSessionId = sessionId;
    final MockId newId = getUniqueId();
    // add to allKeys() so that doFindEntriesSinceTest() works correctly.
    allKeys.put(newId,
        new Pair<IndexValue, IndexValue>(new IndexValue(PUT_RECORD_SIZE, firstRecordFileSpan.getStartOffset()), null));
    recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        return Collections.singletonList(new MessageInfo(newId, PUT_RECORD_SIZE));
      }
    };
    reloadIndex(true);

    long bytesRead =
        log.getDifference(firstRecordFileSpan.getEndOffset(), new Offset(log.getFirstSegment().getName(), 0));
    // create a token that will be past the index end offset on startup after recovery.
    startToken = new StoreFindToken(secondRecordFileSpan.getEndOffset(), oldSessionId);
    // token should get reset internally, no keys should be returned and the returned token should be correct (offset in
    // it will be the current log end offset = firstRecordFileSpan.getEndOffset()).
    StoreFindToken expectedEndToken = new StoreFindToken(firstRecordFileSpan.getEndOffset(), sessionId);
    expectedEndToken.setBytesRead(bytesRead);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, Collections.EMPTY_SET, expectedEndToken);

    // create a token that is not past the index end offset on startup after recovery. Should work as expected
    startToken = new StoreFindToken(lastRecordOffset, oldSessionId);
    expectedEndToken = new StoreFindToken(firstRecordFileSpan.getStartOffset(), sessionId);
    expectedEndToken.setBytesRead(bytesRead);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, Collections.singleton(newId), expectedEndToken);
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
    addPutEntries(7, PUT_RECORD_SIZE, Utils.Infinite_Time);
    MockId idToDelete = getIdToDeleteFromIndexSegment(referenceIndex.lastKey());
    addDeleteEntry(idToDelete);

    // token with log end offset should not return anything
    StoreFindToken token = new StoreFindToken(log.getEndOffset(), sessionId);
    doFindDeletedEntriesSinceTest(token, Long.MAX_VALUE, Collections.EMPTY_SET, token);

    findDeletedEntriesSinceToIndexBasedTest();
    findDeletedEntriesSinceToJournalBasedTest();
    findDeletedEntriesSinceOneByOneTest();
    findDeletedEntriesSinceIndexBasedTokenForOffsetInJournalTest();

    findEntriesSinceInEmptyIndexTest(true);
  }

  // helpers

  // general

  /**
   * @return a {@link MockId} that is unique and has not been generated before in this run.
   */
  private MockId getUniqueId() {
    MockId id;
    do {
      id = new MockId(UtilsTest.getRandomString(10));
    } while (generatedKeys.contains(id));
    generatedKeys.add(id);
    return id;
  }

  /**
   * Appends random data of size {@code size} to the {@link #log}.
   * @param size the size of data that needs to be appeneded.
   * @throws IOException
   */
  private void appendToLog(long size) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate((int) size);
    ReadableByteChannel channel = Channels.newChannel(new ByteBufferInputStream(buffer));
    log.appendFrom(channel, buffer.capacity());
  }

  /**
   * Given an offset, generates the start offset of the index segment that the record at that offset has to go to.
   * <p/>
   * Use only for the latest record - does not work for offsets that are below the current index end offset.
   * @param recordOffset the offset of the record being added to the index.
   * @return the index segment start offset of the index segment that the record belongs to.
   */
  private Offset generateReferenceIndexSegmentStartOffset(Offset recordOffset) {
    if (referenceIndex.size() == 0) {
      return recordOffset;
    }

    Map.Entry<Offset, TreeMap<MockId, IndexValue>> lastEntry = referenceIndex.lastEntry();
    Offset indexSegmentStartOffset = lastEntry.getKey();
    if (!indexSegmentStartOffset.getName().equals(recordOffset.getName())
        || lastEntry.getValue().size() == MAX_IN_MEM_ELEMENTS) {
      indexSegmentStartOffset = recordOffset;
    }
    return indexSegmentStartOffset;
  }

  /**
   * Gets an ID to delete from the index segment with start offset {@code indexSegmentStartOffset}. The returned ID will
   * have been removed from {@link #liveKeys} and added to {@link #deletedKeys}.
   * @param indexSegmentStartOffset the start offset of the index segment from which an ID is required.
   * @return an ID to delete from the index segment with start offset {@code indexSegmentStartOffset}.
   */
  private MockId getIdToDeleteFromIndexSegment(Offset indexSegmentStartOffset) {
    MockId deleteCandidate = null;
    TreeMap<MockId, IndexValue> indexSegment = referenceIndex.get(indexSegmentStartOffset);
    for (Map.Entry<MockId, IndexValue> entry : indexSegment.entrySet()) {
      MockId id = entry.getKey();
      if (liveKeys.contains(id)) {
        deleteCandidate = id;
        break;
      }
    }
    if (deleteCandidate != null) {
      deletedKeys.add(deleteCandidate);
      liveKeys.remove(deleteCandidate);
    }
    return deleteCandidate;
  }

  /**
   * Gets an ID to delete from the given log segment. The returned ID will have been removed from {@link #liveKeys} and
   * added to {@link #deletedKeys}.
   * @param segment the {@link LogSegment} from which an ID is required.
   * @return the ID to delete.
   */
  private MockId getIdToDeleteFromLogSegment(LogSegment segment) {
    MockId deleteCandidate;
    Offset indexSegmentStartOffset = new Offset(segment.getName(), segment.getStartOffset());
    do {
      deleteCandidate = getIdToDeleteFromIndexSegment(indexSegmentStartOffset);
      indexSegmentStartOffset = referenceIndex.higherKey(indexSegmentStartOffset);
      if (indexSegmentStartOffset == null || !indexSegmentStartOffset.getName().equals(segment.getName())) {
        break;
      }
    } while (deleteCandidate == null);
    return deleteCandidate;
  }

  /**
   * Gets the value that is expected to obtained from the {@link PersistentIndex}.
   * @param id the {@link MockId} whose value is required.
   * @param wantPut {@code true} if the {@link IndexValue} of the PUT entry is required.
   * @return the value that is expected to obtained from the {@link PersistentIndex}
   */
  private IndexValue getExpectedValue(MockId id, boolean wantPut) {
    Pair<IndexValue, IndexValue> indexValues = allKeys.get(id);
    return wantPut ? indexValues.getFirst() : indexValues.getSecond();
  }

  /**
   * Adds {@code count} number of put entries each of size {@code size} and that expire at {@code expiresAtMs} to the
   * index (both real and reference).
   * @param count the number of PUT entries to add.
   * @param size the size of each PUT entry.
   * @param expiresAtMs the time at which each of the PUT entries expires.
   * @return the {@link FileSpan} of the added entries.
   * @throws IOException
   * @throws StoreException
   */
  private FileSpan addPutEntries(int count, long size, long expiresAtMs) throws IOException, StoreException {
    if (count <= 0) {
      throw new IllegalArgumentException("Number of put entries to add cannot be <= 0");
    }
    ArrayList<IndexEntry> indexEntries = new ArrayList<>(count);
    Offset expectedJournalLastOffset = null;
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    for (int i = 0; i < count; i++) {
      appendToLog(size);
      FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, size);
      IndexValue value = new IndexValue(size, fileSpan.getStartOffset(), expiresAtMs);
      MockId id = getUniqueId();
      logOrder.put(fileSpan.getStartOffset(), new Pair<>(id, value));
      indexEntries.add(new IndexEntry(id, value));
      Offset indexSegmentStartOffset = generateReferenceIndexSegmentStartOffset(fileSpan.getStartOffset());
      indexSegmentStartOffsets.put(id, new Pair<Offset, Offset>(indexSegmentStartOffset, null));
      allKeys.put(id, new Pair<IndexValue, IndexValue>(value, null));
      if (!referenceIndex.containsKey(indexSegmentStartOffset)) {
        referenceIndex.put(indexSegmentStartOffset, new TreeMap<MockId, IndexValue>());
      }
      referenceIndex.get(indexSegmentStartOffset).put(id, value);
      if (expiresAtMs != Utils.Infinite_Time && expiresAtMs < time.milliseconds()) {
        expiredKeys.add(id);
      } else {
        liveKeys.add(id);
      }
      expectedJournalLastOffset = fileSpan.getStartOffset();
      endOffsetOfPrevMsg = fileSpan.getEndOffset();
    }
    FileSpan fileSpan = new FileSpan(indexEntries.get(0).getValue().getOffset(), endOffsetOfPrevMsg);
    index.addToIndex(indexEntries, fileSpan);
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", expectedJournalLastOffset, index.journal.getLastOffset());
    return fileSpan;
  }

  /**
   * Adds a delete entry in the index (real and reference) for {@code idToDelete}.
   * @param idToDelete the id to be deleted.
   * @return the {@link FileSpan} of the added entries.
   * @throws IOException
   * @throws StoreException
   */
  private FileSpan addDeleteEntry(MockId idToDelete) throws IOException, StoreException {
    appendToLog(DELETE_RECORD_SIZE);
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, DELETE_RECORD_SIZE);
    index.markAsDeleted(idToDelete, fileSpan);

    IndexValue value = getExpectedValue(idToDelete, true);
    IndexValue newValue = new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs());
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(fileSpan.getStartOffset());
    newValue.setNewSize(DELETE_RECORD_SIZE);

    logOrder.put(fileSpan.getStartOffset(), new Pair<>(idToDelete, newValue));
    Offset indexSegmentStartOffset = generateReferenceIndexSegmentStartOffset(fileSpan.getStartOffset());
    Pair<Offset, Offset> keyLocations = indexSegmentStartOffsets.get(idToDelete);
    indexSegmentStartOffsets.put(idToDelete, new Pair<>(keyLocations.getFirst(), indexSegmentStartOffset));
    Pair<IndexValue, IndexValue> keyValues = allKeys.get(idToDelete);
    allKeys.put(idToDelete, new Pair<>(keyValues.getFirst(), newValue));
    if (!referenceIndex.containsKey(indexSegmentStartOffset)) {
      referenceIndex.put(indexSegmentStartOffset, new TreeMap<MockId, IndexValue>());
    }
    referenceIndex.get(indexSegmentStartOffset).put(idToDelete, newValue);
    endOffsetOfPrevMsg = fileSpan.getEndOffset();
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", fileSpan.getStartOffset(), index.journal.getLastOffset());
    return fileSpan;
  }

  /**
   * Verifies that the number of log segments is as expected.
   * @param expectedCount the number of log segments expected.
   */
  private void verifyLogSegmentCount(int expectedCount) {
    // this function works under the assumption that log segments are not allocated until they are required
    // this is a fair assumption because the PersistentIndex works under the same assumption and would break if it were
    // not true.
    LogSegment segment = log.getFirstSegment();
    int logSegmentCount = 0;
    while (segment != null) {
      logSegmentCount++;
      segment = log.getNextSegment(segment);
    }
    assertEquals("Unexpected number of log segments", expectedCount, logSegmentCount);
  }

  /**
   * Reloads the index. Uses the class variables as parameters. For e.g, if a particular implementation of
   * {@link MessageStoreRecovery} is desired, it can be set to {@link #recovery} and this function called. The newly
   * created index will use that implementation of {@link MessageStoreRecovery}.
   * @param deleteCleanShutdownFile {@code true} if the clean shutdown file should be deleted to mimic unclean shutdown
   * @throws StoreException
   */
  private void reloadIndex(boolean deleteCleanShutdownFile) throws StoreException {
    index.close();
    if (deleteCleanShutdownFile) {
      assertTrue("The clean shutdown file could not be deleted",
          new File(tempDir, PersistentIndex.CLEAN_SHUTDOWN_FILENAME).delete());
    }
    metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(tempDirStr, metricRegistry);
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    index =
        new PersistentIndex(tempDirStr, scheduler, log, config, STORE_KEY_FACTORY, recovery, hardDelete, metrics, time);
    sessionId =
        ((StoreFindToken) index.findEntriesSince(new StoreFindToken(), Long.MAX_VALUE).getFindToken()).getSessionId();
  }

  /**
   * Closes the index and clears all the index files essentially creating a new index.
   * @throws StoreException
   */
  private void closeAndClearIndex() throws StoreException {
    index.close();
    // delete all index files
    File[] indexSegmentFiles = tempDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(PersistentIndex.INDEX_SEGMENT_FILE_NAME_SUFFIX) || name.endsWith(
            PersistentIndex.BLOOM_FILE_NAME_SUFFIX);
      }
    });
    assertNotNull("Could not load index segment files", indexSegmentFiles);
    for (File indexSegmentFile : indexSegmentFiles) {
      assertTrue("Could not deleted index segment file", indexSegmentFile.delete());
    }
  }

  /**
   * Advances time by {@code ms} and adjusts {@link #liveKeys} if any of the keys in it expire.
   * @param ms the amount in ms to advance.
   * @throws InterruptedException
   */
  private void advanceTime(long ms) throws InterruptedException {
    time.sleep(ms);
    Iterator<MockId> liveKeysIterator = liveKeys.iterator();
    while (liveKeysIterator.hasNext()) {
      MockId id = liveKeysIterator.next();
      IndexValue value = allKeys.get(id).getFirst();
      if (value.getExpiresAtMs() != Utils.Infinite_Time && value.getExpiresAtMs() < time.milliseconds()) {
        expiredKeys.add(id);
        liveKeysIterator.remove();
      }
    }
  }

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
      BlobReadOptions options = index.getBlobReadInfo(id, storeGetOptions);
      if (expectedErrorCode != null) {
        fail("Should have failed because a StoreException is expected");
      }
      IndexValue putEntryValue = getExpectedValue(id, true);
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

  // test setup

  /**
   * Sets up some state in order to make sure all cases are represented and the tests don't need to do any setup
   * individually. For understanding the created index, please read the source code which is annotated with comments.
   * <p/>
   * Also tests critical functionality of {@link PersistentIndex} and behaviour of the {@link Journal} in the index.
   * Also verifies that the state in {@link #referenceIndex} matches the state in the real index.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void setupTestState() throws InterruptedException, IOException, StoreException {
    long segmentCapacity = isLogSegmented ? SEGMENT_CAPACITY : LOG_CAPACITY;
    metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(tempDirStr, metricRegistry);
    properties.put("store.index.max.number.of.inmem.elements", Integer.toString(MAX_IN_MEM_ELEMENTS));
    // the segment capacity property is never used, so it is not set.
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    log = new Log(tempDirStr, LOG_CAPACITY, segmentCapacity, metrics);
    index =
        new PersistentIndex(tempDirStr, scheduler, log, config, STORE_KEY_FACTORY, recovery, hardDelete, metrics, time);
    assertEquals("End Offset of index not as expected", log.getStartOffset(), index.getCurrentEndOffset());

    // advance time by a millisecond in order to be able to add expired keys and to avoid keys that are expired from
    // being picked for delete.
    time.sleep(1);
    verifyLogSegmentCount(1);
    if (!isLogSegmented) {
      // log is filled about ~50%.
      addCuratedIndexEntriesToLogSegment(segmentCapacity / 2, 1);
    } else {
      // first log segment is filled to capacity.
      addCuratedIndexEntriesToLogSegment(segmentCapacity, 1);

      // second log segment is filled but has some space at the end (free space has to be less than the lesser of the
      // standard delete and put record sizes so that the next write causes a roll over of log segments).
      addCuratedIndexEntriesToLogSegment(segmentCapacity - (DELETE_RECORD_SIZE - 1), 2);

      // third log segment is partially filled and is left as the "active" segment
      // First Index Segment
      // 1 PUT entry
      addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
      // DELETE for a key in the first log segment
      LogSegment segment = log.getFirstSegment();
      MockId idToDelete = getIdToDeleteFromLogSegment(segment);
      addDeleteEntry(idToDelete);
      verifyLogSegmentCount(3);
      // DELETE for a key in the second segment
      segment = log.getNextSegment(segment);
      idToDelete = getIdToDeleteFromLogSegment(segment);
      addDeleteEntry(idToDelete);
      // 1 DELETE for the PUT in the same segment
      idToDelete = getIdToDeleteFromIndexSegment(referenceIndex.lastKey());
      addDeleteEntry(idToDelete);
      // 1 PUT entry that spans the rest of the data in the segment (upto a third of the segment size)
      long size = segmentCapacity / 3 - index.getCurrentEndOffset().getOffset();
      addPutEntries(1, size, Utils.Infinite_Time);

      // fourth and fifth log segment are free.
    }
    // make sure all indexes are written to disk and mapped as required (forcing IndexPersistor to run).
    log.flush();
    reloadIndex(false);
    verifyState();
  }

  /**
   * Adds some curated entries into the index in order to ensure a good mix for testing. For understanding the created
   * index, please read the source code which is annotated with comments.
   * @param sizeToMakeIndexEntriesFor the size to make index entries for.
   * @param expectedLogSegmentCount the number of log segments that are expected to assist after the addition of the
   *                                first entry and at the end of the addition of all entries.
   * @throws IOException
   * @throws StoreException
   */
  private void addCuratedIndexEntriesToLogSegment(long sizeToMakeIndexEntriesFor, int expectedLogSegmentCount)
      throws IOException, StoreException {
    // First Index Segment
    // 1 PUT
    Offset firstJournalEntryAddedNow = addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).getStartOffset();
    verifyLogSegmentCount(expectedLogSegmentCount);
    // 2 more PUT
    addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 2 PUT EXPIRED
    addPutEntries(2, PUT_RECORD_SIZE, 0);
    // 5 entries were added - firstJournalEntryAddedNow should still be a part of the journal
    List<JournalEntry> entries = index.journal.getEntriesSince(firstJournalEntryAddedNow, true);
    assertEquals("There should have been exactly 5 entries returned from the journal", 5, entries.size());

    // Second Index Segment
    // 4 PUT
    addPutEntries(4, PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 1 DELETE for a PUT in the same index segment
    MockId idToDelete = getIdToDeleteFromIndexSegment(referenceIndex.lastKey());
    addDeleteEntry(idToDelete);
    // 5 more entries (for a total of 10) were added - firstJournalEntryAddedNow should still be a part of the journal
    entries = index.journal.getEntriesSince(firstJournalEntryAddedNow, true);
    assertEquals("There should have been exactly 10 entries returned from the journal", 10, entries.size());
    // 1 DELETE for a PUT in the first index segment
    Offset firstIndexSegmentStartOffset = referenceIndex.lowerKey(referenceIndex.lastKey());
    idToDelete = getIdToDeleteFromIndexSegment(firstIndexSegmentStartOffset);
    addDeleteEntry(idToDelete);
    // 1 more entry (for a total of 11) was added - firstJournalEntryAddedNow should no longer be a part of the journal
    assertNull("There should no entries returned from the journal",
        index.journal.getEntriesSince(firstJournalEntryAddedNow, true));

    // Third and Fourth Index Segment
    for (int seg = 0; seg < 2; seg++) {
      // 3 PUT
      addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);
      // 1 PUT for an expired blob
      addPutEntries(1, PUT_RECORD_SIZE, 0);
      // 1 DELETE for the expired PUT
      MockId expiredId = logOrder.lastEntry().getValue().getFirst();
      addDeleteEntry(expiredId);
      deletedKeys.add(expiredId);
      expiredKeys.remove(expiredId);
      // 1 PUT
      addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    }

    Offset fourthIndexSegmentStartOffset = referenceIndex.lastKey();
    Offset thirdIndexSegmentStartOffset = referenceIndex.lowerKey(fourthIndexSegmentStartOffset);
    // Fifth Index Segment
    // 1 PUT entry
    addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 1 DELETE for a PUT in each of the third and fourth segments
    idToDelete = getIdToDeleteFromIndexSegment(thirdIndexSegmentStartOffset);
    addDeleteEntry(idToDelete);
    idToDelete = getIdToDeleteFromIndexSegment(fourthIndexSegmentStartOffset);
    addDeleteEntry(idToDelete);
    // 1 DELETE for the PUT in the same segment
    idToDelete = getIdToDeleteFromIndexSegment(referenceIndex.lastKey());
    addDeleteEntry(idToDelete);
    // 1 PUT entry that spans the rest of the data in the segment
    long size = sizeToMakeIndexEntriesFor - index.getCurrentEndOffset().getOffset();
    addPutEntries(1, size, Utils.Infinite_Time);
    verifyLogSegmentCount(expectedLogSegmentCount);
  }

  /**
   * Verifies that the state in {@link PersistentIndex} is the same as the one in {@link #referenceIndex}.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyState() throws IOException, StoreException {
    verifyLogSegmentCount(isLogSegmented ? 3 : 1);
    NavigableMap<Offset, IndexSegment> realIndex = index.indexes;
    assertEquals("Number of index segments does not match expected", referenceIndex.size(), realIndex.size());
    Map.Entry<Offset, IndexSegment> realIndexEntry = realIndex.firstEntry();
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> referenceIndexEntry : referenceIndex.entrySet()) {
      assertEquals("Offset of index segment does not match expected", referenceIndexEntry.getKey(),
          realIndexEntry.getKey());
      TreeMap<MockId, IndexValue> referenceIndexSegment = referenceIndexEntry.getValue();
      IndexSegment realIndexSegment = realIndexEntry.getValue();
      List<MessageInfo> messageInfos = new ArrayList<>();
      FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE);
      assertTrue("There should have been entries returned from the index segment",
          realIndexSegment.getEntriesSince(null, condition, messageInfos, new AtomicLong(0)));
      assertEquals("Size of index segment differs from expected", referenceIndexSegment.size(), messageInfos.size());
      for (Map.Entry<MockId, IndexValue> referenceIndexSegmentEntry : referenceIndexSegment.entrySet()) {
        IndexValue value = realIndexSegment.find(referenceIndexSegmentEntry.getKey());
        IndexValue referenceValue = referenceIndexSegmentEntry.getValue();
        assertEquals("Value from IndexSegment does not match expected", referenceValue.getBytes(), value.getBytes());
      }
      realIndexEntry = realIndex.higherEntry(realIndexEntry.getKey());
    }
    assertNull("There should no more index segments left", realIndexEntry);
    // all the elements in the last segment should be in the journal
    assertNotNull("There is no offset in the log that corresponds to the last index segment start offset",
        logOrder.floorEntry(referenceIndex.lastKey()));
    Map.Entry<Offset, Pair<MockId, IndexValue>> logEntry = logOrder.floorEntry(referenceIndex.lastKey());
    List<JournalEntry> entries = index.journal.getEntriesSince(referenceIndex.lastKey(), true);
    for (JournalEntry entry : entries) {
      assertNotNull("There are no more entries in the reference log but there are entries in the journal", logEntry);
      assertEquals("Offset in journal not as expected", logEntry.getKey(), entry.getOffset());
      assertEquals("Key in journal not as expected", logEntry.getValue().getFirst(), entry.getKey());
      logEntry = logOrder.higherEntry(logEntry.getKey());
    }
    assertNull("There should be no more entries in the reference log", logEntry);
  }

  // findKey test helpers

  /**
   * Verifies that {@code valueFromFind} matches the expected value from {@link #referenceIndex}.
   * @param id the {@link MockId} whose value is required.
   * @param valueFromFind the {@link IndexValue} that needs to be verified.
   */
  private void verifyValue(MockId id, IndexValue valueFromFind) {
    if (allKeys.containsKey(id)) {
      assertNotNull("Value should be successfully fetched", valueFromFind);
      IndexValue expectedValue = getExpectedValue(id, !deletedKeys.contains(id));
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
    LogSegment nextSegment = log.getNextSegment(segment);
    if (nextSegment == null) {
      lastOffset = logOrder.lastKey();
    } else {
      Offset nextSegmentStartOffset = new Offset(nextSegment.getName(), nextSegment.getStartOffset());
      lastOffset = logOrder.lowerKey(nextSegmentStartOffset);
    }
    return lastOffset;
  }

  // recoverySuccessTest() helpers

  /**
   * Test recovery of a single segment.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void singleSegmentRecoveryTest() throws InterruptedException, IOException, StoreException {
    Offset indexEndOffsetBeforeRecovery = index.getCurrentEndOffset();
    MockId idToCreateAndDelete = getUniqueId();
    // recover a few messages in a single segment
    final List<MessageInfo> infos = getCuratedSingleSegmentRecoveryInfos(idToCreateAndDelete);
    final AtomicInteger returnTracker = new AtomicInteger(0);
    recovery = new MessageStoreRecovery() {
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
    LogSegment activeSegment = log.getSegment(indexEndOffsetBeforeRecovery.getName());
    long expectedSegmentEndOffset = activeSegment.getEndOffset();
    // write a little "extra" data
    appendToLog(2 * PUT_RECORD_SIZE);

    reloadIndex(false);
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
    Offset indexEndOffsetBeforeRecovery = index.getCurrentEndOffset();
    LogSegment activeSegment = log.getSegment(indexEndOffsetBeforeRecovery.getName());
    // recover a few messages across segments
    final List<MessageInfo> activeSegmentInfos = new ArrayList<>();
    // 1 PUT record that will be deleted in the next segment
    MockId idToCreateAndDeleteAcrossSegments = getUniqueId();
    appendToLog(PUT_RECORD_SIZE);
    activeSegmentInfos.add(new MessageInfo(idToCreateAndDeleteAcrossSegments, PUT_RECORD_SIZE));
    // 1 PUT record that will remain and covers almost the rest of the active segment.
    long size = activeSegment.getCapacityInBytes() - activeSegment.getEndOffset() - (DELETE_RECORD_SIZE - 1);
    appendToLog(size);
    activeSegmentInfos.add(new MessageInfo(getUniqueId(), size));
    MockId idToCreateAndDeleteInSameSegment = getUniqueId();
    final List<MessageInfo> nextSegmentInfos = getCuratedSingleSegmentRecoveryInfos(idToCreateAndDeleteInSameSegment);
    // 1 DELETE record for the PUT in the previous segment
    appendToLog(DELETE_RECORD_SIZE);
    nextSegmentInfos.add(new MessageInfo(idToCreateAndDeleteAcrossSegments, DELETE_RECORD_SIZE, true));
    final AtomicInteger returnTracker = new AtomicInteger(0);
    recovery = new MessageStoreRecovery() {
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
    long nextSegmentExpectedEndOffset = log.getNextSegment(activeSegment).getEndOffset();
    // write a little "extra" data
    appendToLog(2 * PUT_RECORD_SIZE);

    reloadIndex(false);
    assertEquals("End offset of former active segment not as expected", activeSegmentExpectedEndOffset,
        activeSegment.getEndOffset());
    activeSegment = log.getNextSegment(activeSegment);
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
    appendToLog(2 * DELETE_RECORD_SIZE + 4 * PUT_RECORD_SIZE);
    // 1 DELETE for a PUT not in the infos
    infos.add(new MessageInfo(getIdToDeleteFromLogSegment(log.getFirstSegment()), DELETE_RECORD_SIZE, true));
    // 3 PUT
    infos.add(new MessageInfo(idToCreateAndDelete, PUT_RECORD_SIZE));
    infos.add(new MessageInfo(getUniqueId(), PUT_RECORD_SIZE));
    infos.add(new MessageInfo(getUniqueId(), PUT_RECORD_SIZE));
    // 1 DELETE for a PUT in the infos
    infos.add(new MessageInfo(idToCreateAndDelete, DELETE_RECORD_SIZE, true));
    // 1 expired PUT
    infos.add(new MessageInfo(getUniqueId(), PUT_RECORD_SIZE, 0));
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
      FileSpan expectedFileSpan = log.getFileSpanForMessage(currCheckOffset, info.getSize());
      if (!info.getStoreKey().equals(putRecordIdToIgnore) || info.isDeleted()) {
        IndexValue value = index.findKey(info.getStoreKey());
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
    closeAndClearIndex();
    final AtomicInteger returnTracker = new AtomicInteger(0);
    recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        switch (returnTracker.getAndIncrement()) {
          case 0:
            return Collections.singletonList(new MessageInfo(getUniqueId(), PUT_RECORD_SIZE));
          default:
            return Collections.emptyList();
        }
      }
    };
    reloadIndex(false);
    verifyLogSegmentCount(1);
    assertEquals("Index should contain exactly one index segment", 1, index.indexes.size());
    LogSegment segment = log.getFirstSegment();
    assertEquals("End offset not as expected",
        new Offset(segment.getName(), segment.getStartOffset() + PUT_RECORD_SIZE), index.getCurrentEndOffset());
  }

  // recoveryFailureTest() helpers

  /**
   * Tests that recovery fails for {@code info}.
   * @param info the {@link MessageInfo} which will cause recovery to fail.
   * @param expectedErrorCode the {@link StoreErrorCodes} expected for the failure.
   */
  private void doRecoveryFailureTest(final MessageInfo info, StoreErrorCodes expectedErrorCode) {
    recovery = new MessageStoreRecovery() {
      @Override
      public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
          throws IOException {
        return Collections.singletonList(info);
      }
    };
    try {
      reloadIndex(false);
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
    Offset logAbsoluteZero = new Offset(log.getFirstSegment().getName(), 0);

    // ------------------
    // 1. Index -> Index
    Offset firstIndexSegmentStartOffset = referenceIndex.firstKey();
    Offset secondIndexSegmentStartOffset = referenceIndex.higherKey(firstIndexSegmentStartOffset);
    MockId firstId = referenceIndex.get(firstIndexSegmentStartOffset).firstKey();
    // All elements from first index segment and two from the second to be returned (because of size restrictions)
    Set<MockId> expectedKeys = new HashSet<>();
    long maxTotalSizeOfEntries = 0;
    for (Map.Entry<MockId, IndexValue> segmentEntry : referenceIndex.get(firstIndexSegmentStartOffset).entrySet()) {
      if (!segmentEntry.getKey().equals(firstId)) {
        expectedKeys.add(segmentEntry.getKey());
        maxTotalSizeOfEntries += segmentEntry.getValue().getSize();
      }
    }
    TreeMap<MockId, IndexValue> secondIndexSegment = referenceIndex.get(secondIndexSegmentStartOffset);
    Map.Entry<MockId, IndexValue> secondIndexSegmentEntry = secondIndexSegment.firstEntry();
    expectedKeys.add(secondIndexSegmentEntry.getKey());
    maxTotalSizeOfEntries += secondIndexSegmentEntry.getValue().getSize();
    secondIndexSegmentEntry = secondIndexSegment.higherEntry(secondIndexSegmentEntry.getKey());
    expectedKeys.add(secondIndexSegmentEntry.getKey());
    maxTotalSizeOfEntries += secondIndexSegmentEntry.getValue().getSize();

    StoreFindToken startToken = new StoreFindToken(firstId, firstIndexSegmentStartOffset, sessionId);
    StoreFindToken expectedEndToken =
        new StoreFindToken(secondIndexSegmentEntry.getKey(), secondIndexSegmentStartOffset, sessionId);
    expectedEndToken.setBytesRead(log.getDifference(secondIndexSegmentStartOffset, logAbsoluteZero));
    doFindEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 2. Uninitialized -> Index
    // add firstStoreKey and its size
    expectedKeys.add(firstId);
    maxTotalSizeOfEntries += allKeys.get(firstId).getFirst().getSize();
    doFindEntriesSinceTest(new StoreFindToken(), maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 3. Journal -> Index
    // create a journal based token for an offset that isn't in the journal
    startToken = new StoreFindToken(logOrder.firstKey(), sessionId);
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
    StoreFindToken absoluteEndToken = new StoreFindToken(logOrder.lastKey(), sessionId);
    absoluteEndToken.setBytesRead(log.getUsedCapacity());

    // ------------------
    // 1. Uninitialized -> Journal
    doFindEntriesSinceTest(new StoreFindToken(), Long.MAX_VALUE, allKeys.keySet(), absoluteEndToken);

    // ------------------
    // 2. Index -> Journal
    Offset firstIndexSegmentStartOffset = referenceIndex.firstKey();
    StoreKey firstStoreKey = referenceIndex.get(firstIndexSegmentStartOffset).firstKey();
    StoreFindToken startToken = new StoreFindToken(firstStoreKey, firstIndexSegmentStartOffset, sessionId);
    Set<MockId> expectedKeys = new HashSet<>(allKeys.keySet());
    if (!deletedKeys.contains(firstStoreKey)) {
      // if firstStoreKey has not been deleted, it will not show up in findEntries since its PUT record is ignored
      expectedKeys.remove(firstStoreKey);
    }
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 3. Journal -> Journal
    // a. Token no longer in journal
    startToken = new StoreFindToken(logOrder.firstKey(), sessionId);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, allKeys.keySet(), absoluteEndToken);

    // b. Token still in journal
    startToken = new StoreFindToken(index.journal.getFirstOffset(), sessionId);
    expectedKeys = new HashSet<>();
    for (Map.Entry<Offset, Pair<MockId, IndexValue>> entry : logOrder.tailMap(startToken.getOffset(), false)
        .entrySet()) {
      expectedKeys.add(entry.getValue().getFirst());
    }
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
    Offset logAbsoluteZero = new Offset(log.getFirstSegment().getName(), 0);
    Offset journalStartOffset = index.journal.getFirstOffset();
    StoreFindToken startToken = new StoreFindToken();
    Offset stoppedAt = null;
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry : referenceIndex.entrySet()) {
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
        StoreFindToken expectedEndToken = new StoreFindToken(id, indexSegmentStartOffset, sessionId);
        expectedEndToken.setBytesRead(log.getDifference(indexSegmentStartOffset, logAbsoluteZero));
        doFindEntriesSinceTest(startToken, indexSegmentEntry.getValue().getSize(), Collections.singleton(id),
            expectedEndToken);
        startToken = expectedEndToken;
      }
    }

    Map.Entry<Offset, Pair<MockId, IndexValue>> logEntry = logOrder.floorEntry(stoppedAt);
    while (logEntry != null) {
      Offset startOffset = logEntry.getKey();
      MockId id = logEntry.getValue().getFirst();
      Pair<IndexValue, IndexValue> putDelete = allKeys.get(id);
      // size returned is the size of the delete if the key has been deleted.
      long size = putDelete.getSecond() != null ? putDelete.getSecond().getSize() : putDelete.getFirst().getSize();
      StoreFindToken expectedEndToken = new StoreFindToken(startOffset, sessionId);
      Offset endOffset = log.getFileSpanForMessage(startOffset, size).getEndOffset();
      expectedEndToken.setBytesRead(log.getDifference(endOffset, logAbsoluteZero));
      doFindEntriesSinceTest(startToken, size, Collections.singleton(id), expectedEndToken);
      startToken = expectedEndToken;
      logEntry = logOrder.higherEntry(logEntry.getKey());
    }
  }

  /**
   * Tests {@link PersistentIndex#findEntriesSince(FindToken, long)} when an index based {@link StoreFindToken} has been
   * given out for an offset in the {@link Journal}.
   * @throws StoreException
   */
  private void findEntriesSinceIndexBasedTokenForOffsetInJournalTest() throws StoreException {
    Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry =
        referenceIndex.floorEntry(index.journal.getFirstOffset());
    Offset nextIndexSegmentStartOffset = referenceIndex.higherKey(indexEntry.getKey());
    MockId firstIdInSegment = indexEntry.getValue().firstKey();
    StoreFindToken startToken = new StoreFindToken(firstIdInSegment, indexEntry.getKey(), sessionId);
    long maxSize = 0;
    Set<MockId> expectedKeys = new HashSet<>();
    for (Map.Entry<MockId, IndexValue> indexSegmentEntry : indexEntry.getValue().entrySet()) {
      if (!firstIdInSegment.equals(indexSegmentEntry.getKey())) {
        expectedKeys.add(indexSegmentEntry.getKey());
        maxSize += indexSegmentEntry.getValue().getSize();
      }
    }
    MockId logId = logOrder.get(nextIndexSegmentStartOffset).getFirst();
    expectedKeys.add(logId);
    long size = deletedKeys.contains(logId) ? DELETE_RECORD_SIZE
        : logOrder.get(nextIndexSegmentStartOffset).getSecond().getSize();
    maxSize += size;

    Offset endOffset = log.getFileSpanForMessage(nextIndexSegmentStartOffset, size).getEndOffset();
    StoreFindToken expectedEndToken = new StoreFindToken(nextIndexSegmentStartOffset, sessionId);
    expectedEndToken.setBytesRead(log.getDifference(endOffset, new Offset(log.getFirstSegment().getName(), 0)));
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
    FindInfo findInfo = index.findEntriesSince(startToken, maxTotalSizeOfEntries);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    compareTokens(expectedEndToken, token);
    assertEquals("Returned token should have the right number of bytes read", expectedEndToken.getBytesRead(),
        token.getBytesRead());
    List<MessageInfo> infos = findInfo.getMessageEntries();
    Set<StoreKey> keysExamined = new HashSet<>();
    for (MessageInfo info : infos) {
      Pair<IndexValue, IndexValue> putDelete = allKeys.get(info.getStoreKey());
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
      index.findEntriesSince(token, Long.MAX_VALUE);
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
    closeAndClearIndex();
    reloadIndex(false);
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
    Offset secondIndexSegmentStartOffset = referenceIndex.higherKey(referenceIndex.firstKey());
    Map.Entry<MockId, IndexValue> firstSegmentEntry = referenceIndex.get(secondIndexSegmentStartOffset).firstEntry();
    // Most elements from the second to be returned (because of size restrictions)
    Set<MockId> expectedKeys = new HashSet<>();
    long maxTotalSizeOfEntries = 0;
    MockId lastKey = null;
    for (Map.Entry<MockId, IndexValue> segmentEntry : referenceIndex.get(secondIndexSegmentStartOffset).entrySet()) {
      if (!segmentEntry.equals(firstSegmentEntry)) {
        if (segmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
          expectedKeys.add(segmentEntry.getKey());
        }
        maxTotalSizeOfEntries += segmentEntry.getValue().getSize();
      }
      lastKey = segmentEntry.getKey();
    }
    StoreFindToken startToken =
        new StoreFindToken(firstSegmentEntry.getKey(), secondIndexSegmentStartOffset, sessionId);
    StoreFindToken expectedEndToken = new StoreFindToken(lastKey, secondIndexSegmentStartOffset, sessionId);
    doFindDeletedEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 2. Uninitialized -> Index
    // add size of values and any keys that are supposed to be returned from the first index segment
    for (Map.Entry<MockId, IndexValue> segmentEntry : referenceIndex.firstEntry().getValue().entrySet()) {
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
    startToken = new StoreFindToken(logOrder.firstKey(), sessionId);
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
    StoreFindToken absoluteEndToken = new StoreFindToken(logOrder.lastKey(), sessionId);

    // ------------------
    // 1. Uninitialized -> Journal
    doFindDeletedEntriesSinceTest(new StoreFindToken(), Long.MAX_VALUE, deletedKeys, absoluteEndToken);

    // ------------------
    // 2. Index -> Journal
    Offset secondIndexSegmentStartOffset = referenceIndex.higherKey(referenceIndex.firstKey());
    // second index segment contains the first delete entry
    StoreKey firstDeletedKey = getDeletedKeyFromIndexSegment(secondIndexSegmentStartOffset);
    StoreFindToken startToken = new StoreFindToken(firstDeletedKey, secondIndexSegmentStartOffset, sessionId);
    Set<MockId> expectedKeys = new HashSet<>(deletedKeys);
    expectedKeys.remove(firstDeletedKey);
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 3. Journal -> Journal
    // a. Token no longer in journal
    startToken = new StoreFindToken(allKeys.get(firstDeletedKey).getSecond().getOffset(), sessionId);
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, deletedKeys, absoluteEndToken);

    // b. Token still in journal
    startToken = new StoreFindToken(index.journal.getFirstOffset(), sessionId);
    expectedKeys.clear();
    for (Map.Entry<Offset, Pair<MockId, IndexValue>> entry : logOrder.tailMap(startToken.getOffset(), false)
        .entrySet()) {
      if (entry.getValue().getSecond().isFlagSet(IndexValue.Flags.Delete_Index)) {
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
    Offset journalStartOffset = index.journal.getFirstOffset();
    StoreFindToken startToken = new StoreFindToken();
    Offset stoppedAt = null;
    for (Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry : referenceIndex.entrySet()) {
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
        StoreFindToken expectedEndToken = new StoreFindToken(id, indexSegmentStartOffset, sessionId);
        long size = indexSegmentEntry.getValue().getSize();
        doFindDeletedEntriesSinceTest(startToken, size, isDeleted ? Collections.singleton(id) : Collections.EMPTY_SET,
            expectedEndToken);
        startToken = expectedEndToken;
      }
    }

    Map.Entry<Offset, Pair<MockId, IndexValue>> logEntry = logOrder.floorEntry(stoppedAt);
    while (logEntry != null) {
      Offset startOffset = logEntry.getKey();
      MockId id = logEntry.getValue().getFirst();
      Pair<IndexValue, IndexValue> putDelete = allKeys.get(id);
      boolean isDeleted = putDelete.getSecond() != null;
      // size returned is the size of the delete if the key has been deleted.
      long size = isDeleted ? putDelete.getSecond().getSize() : putDelete.getFirst().getSize();
      StoreFindToken expectedEndToken = new StoreFindToken(startOffset, sessionId);
      doFindDeletedEntriesSinceTest(startToken, size, isDeleted ? Collections.singleton(id) : Collections.EMPTY_SET,
          expectedEndToken);
      startToken = expectedEndToken;
      logEntry = logOrder.higherEntry(logEntry.getKey());
    }
  }

  /**
   * Tests {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} when an index based
   * {@link StoreFindToken} has been given out for an offset in the {@link Journal}.
   * @throws StoreException
   */
  private void findDeletedEntriesSinceIndexBasedTokenForOffsetInJournalTest() throws StoreException {
    Map.Entry<Offset, TreeMap<MockId, IndexValue>> indexEntry =
        referenceIndex.floorEntry(index.journal.getFirstOffset());
    Offset nextIndexSegmentStartOffset = referenceIndex.higherKey(indexEntry.getKey());
    MockId firstIdInSegment = indexEntry.getValue().firstKey();
    StoreFindToken startToken = new StoreFindToken(firstIdInSegment, indexEntry.getKey(), sessionId);
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
    MockId logId = logOrder.get(nextIndexSegmentStartOffset).getFirst();
    long size = logOrder.get(nextIndexSegmentStartOffset).getSecond().getSize();
    if (deletedKeys.contains(logId)) {
      expectedKeys.add(logId);
      size = DELETE_RECORD_SIZE;
    }
    maxSize += size;

    StoreFindToken expectedEndToken = new StoreFindToken(nextIndexSegmentStartOffset, sessionId);
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
    FindInfo findInfo = index.findDeletedEntriesSince(startToken, maxTotalSizeOfEntries, Long.MAX_VALUE);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    compareTokens(expectedEndToken, token);
    List<MessageInfo> infos = findInfo.getMessageEntries();
    Set<StoreKey> keysExamined = new HashSet<>();
    for (MessageInfo info : infos) {
      IndexValue value = allKeys.get(info.getStoreKey()).getSecond();
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
    for (Map.Entry<MockId, IndexValue> indexSegmentEntry : referenceIndex.get(indexSegmentStartOffset).entrySet()) {
      if (indexSegmentEntry.getValue().isFlagSet(IndexValue.Flags.Delete_Index)) {
        deletedId = indexSegmentEntry.getKey();
        break;
      }
    }
    return deletedId;
  }
}
