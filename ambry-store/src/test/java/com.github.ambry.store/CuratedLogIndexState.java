/*
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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;


/**
 * Represents a carefully built state that includes {@link Log} and {@link PersistentIndex} instances along with
 * data structures that represent the source of truth for data those instances must contain. Allows some manipulation
 * of state and some useful functionality.
 */
class CuratedLogIndexState {
  private static final byte[] RECOVERY_INFO = new byte[100];
  // setupTestState() is coupled to these numbers. Changing them *will* cause setting test state or tests to fail.
  private static final long LOG_CAPACITY = 10000;
  private static final long SEGMENT_CAPACITY = 2000;
  private static final int MAX_IN_MEM_ELEMENTS = 5;
  private static final long HARD_DELETE_START_OFFSET = 11;
  private static final long HARD_DELETE_LAST_PART_SIZE = 13;

  static final StoreKeyFactory STORE_KEY_FACTORY;
  // deliberately do not divide the capacities perfectly.
  static final long PUT_RECORD_SIZE = 53;
  static final long DELETE_RECORD_SIZE = 29;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
      Arrays.fill(CuratedLogIndexState.RECOVERY_INFO, (byte) 0);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  // The reference index to compare against. Key is index segment start Offset, Value is the reference index segment.
  // This reflects exactly how PersistentIndex is supposed to look.
  final TreeMap<Offset, TreeMap<MockId, IndexValue>> referenceIndex = new TreeMap<>();
  // A map of all the keys. The key is the MockId and the value is a pair of index segment start Offsets.
  // first Offset represents the index segment start offset of the PUT entry.
  // second Offset represents the index segment start offset of the DELETE entry (null if the key has not been deleted).
  final Map<MockId, Pair<Offset, Offset>> indexSegmentStartOffsets = new HashMap<>();
  // A map of all the keys. The key is the MockId and the value is a pair of IndexValues.
  // The first IndexValue represents the value of the PUT entry.
  // The second IndexValue represents the value of the DELETE entry (null if the key has not been deleted).
  final Map<MockId, Pair<IndexValue, IndexValue>> allKeys = new HashMap<>();
  // Set of all deleted keys
  final Set<MockId> deletedKeys = new HashSet<>();
  // Set of all expired keys
  final Set<MockId> expiredKeys = new HashSet<>();
  // Set of all keys that are not deleted/expired
  final Set<MockId> liveKeys = new HashSet<>();
  // The keys in offset order as they appear in the log.
  final TreeMap<Offset, Pair<MockId, LogEntry>> logOrder = new TreeMap<>();
  // the properties that will used to generate a StoreConfig. Clear before use if required.
  final Properties properties = new Properties();
  // The Log which has the data
  final Log log;
  // the time instance that will be used in the index
  final Time time = new MockTime();
  // the scheduler used in the index
  final ScheduledExecutorService scheduler = Utils.newScheduler(1, false);

  // The MessageStoreRecovery that is used with the index
  MessageStoreRecovery recovery = new DummyMessageStoreRecovery();
  // The MessageStoreHardDelete that is used with the index
  MessageStoreHardDelete hardDelete = new MockMessageStoreHardDelete();
  // The index of the log
  PersistentIndex index;
  // The session ID associated with the index
  UUID sessionId = UUID.randomUUID();
  // the incarnationId associated with the store
  UUID incarnationId = UUID.randomUUID();

  // Variables that represent the folder where the data resides
  private final File tempDir;
  private final String tempDirStr;
  // used by getUniqueId() to make sure keys are never regenerated in a single test run.
  private final Set<MockId> generatedKeys = new HashSet<>();
  // The MetricRegistry that is used with the index
  private MetricRegistry metricRegistry = new MetricRegistry();

  /**
   * Creates state in order to make sure all cases are represented and log-index tests don't need to do any setup
   * individually. For understanding the created index, please read the source code which is annotated with comments.
   * <p/>
   * Also verifies that the state in {@link #referenceIndex} matches the state in the real index (which implicitly
   * verifies some functionality of {@link PersistentIndex} and behaviour of the {@link Journal} in the index).
   * @param isLogSegmented {@code true} if segmented. {@code false} otherwise.
   * @param tempDir the directory where the log and index files should be created.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  CuratedLogIndexState(boolean isLogSegmented, File tempDir) throws InterruptedException, IOException, StoreException {
    this.tempDir = tempDir;
    tempDirStr = tempDir.getAbsolutePath();
    long segmentCapacity = isLogSegmented ? CuratedLogIndexState.SEGMENT_CAPACITY : CuratedLogIndexState.LOG_CAPACITY;
    StoreMetrics metrics = new StoreMetrics(tempDirStr, metricRegistry);
    log = new Log(tempDirStr, CuratedLogIndexState.LOG_CAPACITY, segmentCapacity, metrics);
    setupTestState(isLogSegmented, segmentCapacity, metrics);
  }

  /**
   * Destroys state and cleans up as required.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  void destroy() throws InterruptedException, IOException, StoreException {
    index.close();
    log.close();
    scheduler.shutdown();
    assertTrue(scheduler.awaitTermination(1, TimeUnit.SECONDS));
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
  FileSpan addPutEntries(int count, long size, long expiresAtMs) throws IOException, StoreException {
    if (count <= 0) {
      throw new IllegalArgumentException("Number of put entries to add cannot be <= 0");
    }
    ArrayList<IndexEntry> indexEntries = new ArrayList<>(count);
    Offset expectedJournalLastOffset = null;
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    for (int i = 0; i < count; i++) {
      byte[] dataWritten = appendToLog(size);
      FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, size);
      IndexValue value = new IndexValue(size, fileSpan.getStartOffset(), expiresAtMs);
      MockId id = getUniqueId();
      logOrder.put(fileSpan.getStartOffset(), new Pair<>(id, new LogEntry(dataWritten, value)));
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
  FileSpan addDeleteEntry(MockId idToDelete) throws IOException, StoreException {
    byte[] dataWritten = appendToLog(CuratedLogIndexState.DELETE_RECORD_SIZE);
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, CuratedLogIndexState.DELETE_RECORD_SIZE);
    index.markAsDeleted(idToDelete, fileSpan);

    IndexValue value = getExpectedValue(idToDelete, true);
    IndexValue newValue = new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs());
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(fileSpan.getStartOffset());
    newValue.setNewSize(CuratedLogIndexState.DELETE_RECORD_SIZE);

    logOrder.put(fileSpan.getStartOffset(), new Pair<>(idToDelete, new LogEntry(dataWritten, newValue)));
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
   * Advances time by {@code ms} and adjusts {@link #liveKeys} if any of the keys in it expire.
   * @param ms the amount in ms to advance.
   * @throws InterruptedException
   */
  void advanceTime(long ms) throws InterruptedException {
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
   * Appends random data of size {@code size} to the {@link #log}.
   * @param size the size of data that needs to be appeneded.
   * @return the data that was appended.
   * @throws IOException
   */
  byte[] appendToLog(long size) throws IOException {
    byte[] bytes = TestUtils.getRandomBytes((int) size);
    if (size > CuratedLogIndexState.HARD_DELETE_START_OFFSET) {
      // ensure atleast one byte is set to 1 for hard delete verification purposes
      int randomByte = (int) (CuratedLogIndexState.HARD_DELETE_START_OFFSET + TestUtils.RANDOM.nextInt(
          (int) (size - CuratedLogIndexState.HARD_DELETE_START_OFFSET
              - CuratedLogIndexState.HARD_DELETE_LAST_PART_SIZE)));
      bytes[randomByte] = 1;
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    ReadableByteChannel channel = Channels.newChannel(new ByteBufferInputStream(buffer));
    log.appendFrom(channel, buffer.capacity());
    return bytes;
  }

  /**
   * @return a {@link MockId} that is unique and has not been generated before in this run.
   */
  MockId getUniqueId() {
    MockId id;
    do {
      id = new MockId(UtilsTest.getRandomString(10));
    } while (generatedKeys.contains(id));
    generatedKeys.add(id);
    return id;
  }

  /**
   * Gets the value that is expected to obtained from the {@link PersistentIndex}.
   * @param id the {@link MockId} whose value is required.
   * @param wantPut {@code true} if the {@link IndexValue} of the PUT entry is required.
   * @return the value that is expected to obtained from the {@link PersistentIndex}
   */
  IndexValue getExpectedValue(MockId id, boolean wantPut) {
    Pair<IndexValue, IndexValue> indexValues = allKeys.get(id);
    return wantPut ? indexValues.getFirst() : indexValues.getSecond();
  }

  /**
   * Gets the data that is expected to obtained from the {@link Log}.
   * @param id the {@link MockId} whose value is required.
   * @param wantPut {@code true} if the {@link IndexValue} of the PUT entry is required.
   * @return the value that is expected to obtained from the {@link PersistentIndex}
   */
  byte[] getExpectedData(MockId id, boolean wantPut) {
    Pair<IndexValue, IndexValue> indexValues = allKeys.get(id);
    Offset offset = wantPut ? indexValues.getFirst().getOffset() : indexValues.getSecond().getOffset();
    return logOrder.get(offset).getSecond().buffer;
  }

  /**
   * Returns all ids that have records in the provided {@code segment}.
   * @param segment the {@link LogSegment} to get the ids of all records for.
   * @return all ids that have records in the provided {@code segment}.
   */
  Set<MockId> getIdsInLogSegment(LogSegment segment) {
    Set<MockId> idsInSegment = new HashSet<>();
    Offset indexSegmentStartOffset = new Offset(segment.getName(), segment.getStartOffset());
    while (indexSegmentStartOffset != null && indexSegmentStartOffset.getName().equals(segment.getName())) {
      idsInSegment.addAll(referenceIndex.get(indexSegmentStartOffset).keySet());
      indexSegmentStartOffset = referenceIndex.higherKey(indexSegmentStartOffset);
    }
    return idsInSegment;
  }

  /**
   * Gets an ID to delete from the index segment with start offset {@code indexSegmentStartOffset}. The returned ID will
   * have been removed from {@link #liveKeys} and added to {@link #deletedKeys}.
   * @param indexSegmentStartOffset the start offset of the index segment from which an ID is required.
   * @return an ID to delete from the index segment with start offset {@code indexSegmentStartOffset}.
   */
  MockId getIdToDeleteFromIndexSegment(Offset indexSegmentStartOffset) {
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
  MockId getIdToDeleteFromLogSegment(LogSegment segment) {
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
   * Verify that deleted entries are hard deleted
   * @param deletedIds the set of {@link MockId}s that are deleted
   * @throws IOException
   */
  void verifyEntriesForHardDeletes(Set<MockId> deletedIds) throws IOException {
    for (MockId id : deletedIds) {
      IndexValue putValue = allKeys.get(id).getFirst();
      Offset offset = putValue.getOffset();
      LogSegment segment = log.getSegment(offset.getName());
      long size = putValue.getSize() - CuratedLogIndexState.HARD_DELETE_START_OFFSET
          - CuratedLogIndexState.HARD_DELETE_LAST_PART_SIZE;
      ByteBuffer readBuf = ByteBuffer.allocate((int) size);
      segment.readInto(readBuf, offset.getOffset() + CuratedLogIndexState.HARD_DELETE_START_OFFSET);
      readBuf.flip();
      while (readBuf.hasRemaining()) {
        assertEquals("Hard delete has not zeroed out the data", (byte) 0, readBuf.get());
      }
    }
  }

  /**
   * Reloads the index. Uses the class variables as parameters. For e.g, if a particular implementation of
   * {@link MessageStoreRecovery} is desired, it can be set to {@link #recovery} and this function called. The newly
   * created index will use that implementation of {@link MessageStoreRecovery}.
   * @param closeBeforeReload {@code true} if index should be closed before reload. {@code false} otherwise.
   * @param deleteCleanShutdownFile {@code true} if the clean shutdown file should be deleted to mimic unclean shutdown.
   *                                            Ignored if {@code closeBeforeReload} is {@code false}.
   * @throws StoreException
   */
  void reloadIndex(boolean closeBeforeReload, boolean deleteCleanShutdownFile) throws StoreException {
    if (closeBeforeReload) {
      index.close();
      if (deleteCleanShutdownFile) {
        assertTrue("The clean shutdown file could not be deleted",
            new File(tempDir, PersistentIndex.CLEAN_SHUTDOWN_FILENAME).delete());
      }
    }
    metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(tempDirStr, metricRegistry);
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    sessionId = UUID.randomUUID();
    index = new PersistentIndex(tempDirStr, scheduler, log, config, CuratedLogIndexState.STORE_KEY_FACTORY, recovery,
        hardDelete, metrics, time, sessionId, incarnationId);
  }

  /**
   * Closes the index and clears all the index files.
   * @throws StoreException
   */
  void closeAndClearIndex() throws StoreException {
    index.close();
    // delete all index files
    File[] indexSegmentFiles = tempDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX) || name.endsWith(
            IndexSegment.BLOOM_FILE_NAME_SUFFIX);
      }
    });
    assertNotNull("Could not load index segment files", indexSegmentFiles);
    for (File indexSegmentFile : indexSegmentFiles) {
      assertTrue("Could not deleted index segment file", indexSegmentFile.delete());
    }
  }

  /**
   * Sets up some state in order to make sure all cases are represented and the tests don't need to do any setup
   * individually. For understanding the created index, please read the source code which is annotated with comments.
   * <p/>
   * Also tests critical functionality of {@link PersistentIndex} and behaviour of the {@link Journal} in the index.
   * Also verifies that the state in {@link #referenceIndex} matches the state in the real index.
   * @param isLogSegmented {@code true} if segmented. {@code false} otherwise.
   * @param segmentCapacity the intended capacity of each segment
   * @param metrics the {@link StoreMetrics} instance to use.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  private void setupTestState(boolean isLogSegmented, long segmentCapacity, StoreMetrics metrics)
      throws InterruptedException, IOException, StoreException {
    metricRegistry = new MetricRegistry();
    properties.put("store.index.max.number.of.inmem.elements",
        Integer.toString(CuratedLogIndexState.MAX_IN_MEM_ELEMENTS));
    properties.put("store.data.flush.delay.seconds", Integer.toString(0));
    properties.put("store.enable.hard.delete", "false");
    // the segment capacity property is never used, so it is not set.
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    index = new PersistentIndex(tempDirStr, scheduler, log, config, CuratedLogIndexState.STORE_KEY_FACTORY, recovery,
        hardDelete, metrics, time, sessionId, incarnationId);
    Offset expectedStartOffset = new Offset(log.getFirstSegment().getName(), log.getFirstSegment().getStartOffset());
    assertEquals("Start Offset of index not as expected", expectedStartOffset, index.getStartOffset());
    assertEquals("End Offset of index not as expected", log.getEndOffset(), index.getCurrentEndOffset());

    // advance time by a millisecond in order to be able to add expired keys and to avoid keys that are expired from
    // being picked for delete.
    time.sleep(1);
    assertEquals("Incorrect log segment count", 0, index.getLogSegmentCount());
    long expectedUsedCapacity;
    if (!isLogSegmented) {
      // log is filled about ~50%.
      addCuratedIndexEntriesToLogSegment(segmentCapacity / 2, 1);
      expectedUsedCapacity = segmentCapacity / 2;
      assertEquals("Used capacity reported not as expected", expectedUsedCapacity, index.getLogUsedCapacity());
    } else {
      // first log segment is filled to capacity.
      addCuratedIndexEntriesToLogSegment(segmentCapacity, 1);
      assertEquals("Used capacity reported not as expected", segmentCapacity, index.getLogUsedCapacity());

      // second log segment is filled but has some space at the end (free space has to be less than the lesser of the
      // standard delete and put record sizes so that the next write causes a roll over of log segments).
      addCuratedIndexEntriesToLogSegment(segmentCapacity - (CuratedLogIndexState.DELETE_RECORD_SIZE - 1), 2);
      assertEquals("Used capacity reported not as expected",
          2 * segmentCapacity - (CuratedLogIndexState.DELETE_RECORD_SIZE - 1), index.getLogUsedCapacity());

      // third log segment is partially filled and is left as the "active" segment
      // First Index Segment
      // 1 PUT entry
      addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      // DELETE for a key in the first log segment
      LogSegment segment = log.getFirstSegment();
      MockId idToDelete = getIdToDeleteFromLogSegment(segment);
      addDeleteEntry(idToDelete);
      assertEquals("Incorrect log segment count", 3, index.getLogSegmentCount());
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

      expectedUsedCapacity = 2 * segmentCapacity + segmentCapacity / 3;
      assertEquals("Used capacity reported not as expected", expectedUsedCapacity, index.getLogUsedCapacity());

      // fourth and fifth log segment are free.
    }
    // make sure all indexes are written to disk and mapped as required (forcing IndexPersistor to run).
    log.flush();
    reloadIndex(true, false);
    verifyState(isLogSegmented);
    assertEquals("Start Offset of index not as expected", expectedStartOffset, index.getStartOffset());
    assertEquals("End Offset of index not as expected", log.getEndOffset(), index.getCurrentEndOffset());
    assertEquals("Used capacity reported not as expected", expectedUsedCapacity, index.getLogUsedCapacity());
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
    Offset firstJournalEntryAddedNow =
        addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).getStartOffset();
    assertEquals("Incorrect log segment count", expectedLogSegmentCount, index.getLogSegmentCount());
    // 2 more PUT
    addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 2 PUT EXPIRED
    addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, 0);
    // 5 entries were added - firstJournalEntryAddedNow should still be a part of the journal
    List<JournalEntry> entries = index.journal.getEntriesSince(firstJournalEntryAddedNow, true);
    assertEquals("There should have been exactly 5 entries returned from the journal", 5, entries.size());

    // Second Index Segment
    // 4 PUT
    addPutEntries(4, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
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
      addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      // 1 PUT for an expired blob
      addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, 0);
      // 1 DELETE for the expired PUT
      MockId expiredId = logOrder.lastEntry().getValue().getFirst();
      addDeleteEntry(expiredId);
      deletedKeys.add(expiredId);
      expiredKeys.remove(expiredId);
      // 1 PUT
      addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    }

    Offset fourthIndexSegmentStartOffset = referenceIndex.lastKey();
    Offset thirdIndexSegmentStartOffset = referenceIndex.lowerKey(fourthIndexSegmentStartOffset);
    // Fifth Index Segment
    // 1 PUT entry
    addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
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
    assertEquals("Incorrect log segment count", expectedLogSegmentCount, index.getLogSegmentCount());
  }

  /**
   * Verifies that the state in {@link PersistentIndex} is the same as the one in {@link #referenceIndex}.
   * @param isLogSegmented {@code true} if segmented. {@code false} otherwise.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyState(boolean isLogSegmented) throws IOException, StoreException {
    assertEquals("Incorrect log segment count", isLogSegmented ? 3 : 1, index.getLogSegmentCount());
    NavigableMap<Offset, IndexSegment> realIndex = index.getIndexSegments();
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
    Map.Entry<Offset, Pair<MockId, LogEntry>> logEntry = logOrder.floorEntry(referenceIndex.lastKey());
    List<JournalEntry> entries = index.journal.getEntriesSince(referenceIndex.lastKey(), true);
    for (JournalEntry entry : entries) {
      assertNotNull("There are no more entries in the reference log but there are entries in the journal", logEntry);
      assertEquals("Offset in journal not as expected", logEntry.getKey(), entry.getOffset());
      assertEquals("Key in journal not as expected", logEntry.getValue().getFirst(), entry.getKey());
      logEntry = logOrder.higherEntry(logEntry.getKey());
    }
    assertNull("There should be no more entries in the reference log", logEntry);
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
        || lastEntry.getValue().size() == CuratedLogIndexState.MAX_IN_MEM_ELEMENTS) {
      indexSegmentStartOffset = recordOffset;
    }
    return indexSegmentStartOffset;
  }

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
          long size = readSet.sizeInBytes(count - 1) - CuratedLogIndexState.HARD_DELETE_START_OFFSET
              - CuratedLogIndexState.HARD_DELETE_LAST_PART_SIZE;
          ByteBuffer buf = ByteBuffer.allocate((int) size);
          Arrays.fill(buf.array(), (byte) 0);
          ByteBufferInputStream stream = new ByteBufferInputStream(buf);
          ReadableByteChannel channel = Channels.newChannel(stream);
          return new HardDeleteInfo(channel, buf.capacity(), CuratedLogIndexState.HARD_DELETE_START_OFFSET,
              CuratedLogIndexState.RECOVERY_INFO);
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
      Pair<MockId, LogEntry> idAndValue = logOrder.get(new Offset(segmentName, offset));
      IndexValue value = idAndValue.getSecond().indexValue;
      return new MessageInfo(idAndValue.getFirst(), value.getSize(), value.getExpiresAtMs());
    }
  }

  /**
   * Represents a reference to an entry in the {@link Log}.
   */
  static class LogEntry {
    final byte[] buffer;
    final IndexValue indexValue;

    /**
     * @param buffer the data in the {@link Log}.
     * @param indexValue the {@link IndexValue} that was generated.
     */
    LogEntry(byte[] buffer, IndexValue indexValue) {
      this.buffer = buffer;
      this.indexValue = indexValue;
    }
  }
}
