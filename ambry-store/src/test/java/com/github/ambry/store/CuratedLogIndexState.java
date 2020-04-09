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
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.ambry.store.StoreTestUtils.*;
import static com.github.ambry.utils.Utils.*;
import static org.junit.Assert.*;


/**
 * Represents a carefully built state that includes {@link Log} and {@link PersistentIndex} instances along with
 * data structures that represent the source of truth for data those instances must contain. Allows some manipulation
 * of state and some useful functionality.
 */
class CuratedLogIndexState {
  private static final byte[] RECOVERY_INFO = new byte[100];
  // setupTestState() is coupled to these numbers. Changing them *will* cause setting test state or tests to fail.
  private static final long LOG_CAPACITY = 40000;
  private static final long SEGMENT_CAPACITY = 4000;
  private static final long HARD_DELETE_START_OFFSET = 11;
  private static final long HARD_DELETE_LAST_PART_SIZE = 13;

  static final int DEFAULT_MAX_IN_MEM_ELEMENTS = 5;
  static final DiskIOScheduler DISK_IO_SCHEDULER = new DiskIOScheduler(null);
  static final long DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS = 10 * Time.MsPerSec;
  static final StoreKeyFactory STORE_KEY_FACTORY;
  // deliberately do not divide the capacities perfectly.
  static final long PUT_RECORD_SIZE = 53;
  static final long DELETE_RECORD_SIZE = 29;
  static final long TTL_UPDATE_RECORD_SIZE = 37;
  static final long UNDELETE_RECORD_SIZE = 29;

  static final int deleteRetentionDay = 1;

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
  final TreeMap<Offset, TreeMap<MockId, TreeSet<IndexValue>>> referenceIndex = new TreeMap<>();
  // A map of all the keys. The key is the MockId and the value is a list of IndexValues as seen in the referenceIndex
  // with earliest one first and latest one last
  final Map<MockId, TreeSet<IndexValue>> allKeys = new HashMap<>();
  // map of all index segments to their last modified times
  final Map<Offset, Long> lastModifiedTimesInSecs = new HashMap<>();
  // Set of all TTL updated keys
  final Set<MockId> ttlUpdatedKeys = new HashSet<>();
  // Set of all deleted keys
  final Set<MockId> deletedKeys = new HashSet<>();
  // Set of all undeleted keys
  final Set<MockId> undeletedKeys = new HashSet<>();
  // Set of all expired keys
  final Set<MockId> expiredKeys = new HashSet<>();
  // Set of all keys that are not deleted/expired
  final Set<MockId> liveKeys = new HashSet<>();
  // The keys in offset order as they appear in the log.
  final TreeMap<Offset, Pair<MockId, LogEntry>> logOrder = new TreeMap<>();
  // the properties that will used to generate a StoreConfig. Clear before use if required.
  final Properties properties = new Properties();
  // the time instance that will be used in the index
  final MockTime time = new MockTime();
  final boolean isLogSegmented;

  // the scheduler used in the index
  ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
  // The MessageStoreRecovery that is used with the index
  MessageStoreRecovery recovery = new DummyMessageStoreRecovery();
  // The MessageStoreHardDelete that is used with the index
  MessageStoreHardDelete hardDelete = new MockMessageStoreHardDelete();
  // The Log which has the data
  Log log;
  // The index of the log
  PersistentIndex index;
  // The session ID associated with the index
  UUID sessionId = UUID.randomUUID();
  // the incarnationId associated with the store
  UUID incarnationId = UUID.randomUUID();
  // The MetricRegistry that is used with the index
  MetricRegistry metricRegistry = new MetricRegistry();
  // The deleted key with associated PUT record in the same log segment
  MockId deletedKeyWithPutInSameSegment = null;

  // Variables that represent the folder where the data resides
  private final File tempDir;
  private final String tempDirStr;
  // used by getUniqueId() to make sure keys are never regenerated in a single test run.
  private final Set<MockId> generatedKeys = new HashSet<>();

  private StoreMetrics metrics;

  /**
   * Creates state in order to make sure all cases are represented and log-index tests don't need to do any setup
   * individually. For understanding the created index, please read the source code which is annotated with comments.
   * <p/>
   * Also verifies that the state in {@link #referenceIndex} matches the state in the real index (which implicitly
   * verifies some functionality of {@link PersistentIndex} and behaviour of the {@link Journal} in the index).
   * @param isLogSegmented {@code true} if segmented. {@code false} otherwise.
   * @param tempDir the directory where the log and index files should be created.
   * @param addTtlUpdates if {@code true}, adds entries that update TTL.
   * @param addUndeletes if {@code true}, adds undelete entries.
   * @throws IOException
   * @throws StoreException
   */
  CuratedLogIndexState(boolean isLogSegmented, File tempDir, boolean addTtlUpdates, boolean addUndeletes)
      throws IOException, StoreException {
    this(isLogSegmented, tempDir, false, true, addTtlUpdates, addUndeletes);
  }

  /**
   * Creates state in order to make sure all cases are represented and log-index tests don't need to do any setup
   * individually. For understanding the created index, please read the source code which is annotated with comments.
   * <p/>
   * Also verifies that the state in {@link #referenceIndex} matches the state in the real index (which implicitly
   * verifies some functionality of {@link PersistentIndex} and behaviour of the {@link Journal} in the index).
   * @param isLogSegmented {@code true} if segmented. {@code false} otherwise.
   * @param tempDir the directory where the log and index files should be created.
   * @param hardDeleteEnabled if {@code true}, hard delete is enabled.
   * @param initState sets up a diverse set of entries if {@code true}. Leaves the log and index empty if {@code false}.
   * @param addTtlUpdates if {@code true}, adds entries that update TTL.
   * @param addUndeletes if {@code true}, adds undelete entries.
   * @throws IOException
   * @throws StoreException
   */
  CuratedLogIndexState(boolean isLogSegmented, File tempDir, boolean hardDeleteEnabled, boolean initState,
      boolean addTtlUpdates, boolean addUndeletes) throws IOException, StoreException {
    this.isLogSegmented = isLogSegmented;
    // advance time here so when we set delete's operation time to 0, it will fall within retention day.
    advanceTime(TimeUnit.DAYS.toMillis(CuratedLogIndexState.deleteRetentionDay));
    this.tempDir = tempDir;
    tempDirStr = tempDir.getAbsolutePath();
    long segmentCapacity = isLogSegmented ? CuratedLogIndexState.SEGMENT_CAPACITY : CuratedLogIndexState.LOG_CAPACITY;
    metrics = new StoreMetrics(metricRegistry);
    log = new Log(tempDirStr, CuratedLogIndexState.LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(segmentCapacity, true), metrics);
    properties.put("store.index.max.number.of.inmem.elements",
        Integer.toString(CuratedLogIndexState.DEFAULT_MAX_IN_MEM_ELEMENTS));
    properties.put("store.enable.hard.delete", Boolean.toString(hardDeleteEnabled));
    // not used but set anyway since this is a package private variable.
    properties.put("store.segment.size.in.bytes", Long.toString(segmentCapacity));
    // set the delete retention day
    properties.put("store.deleted.message.retention.days", Integer.toString(CuratedLogIndexState.deleteRetentionDay));
    // switch off time movement for the hard delete thread. Otherwise blobs expire too quickly
    time.suspend(Collections.singleton(HardDeleter.getThreadName(tempDirStr)));
    initIndex(null);
    assertTrue("Expected empty index", index.isEmpty());
    if (initState) {
      setupTestState(isLogSegmented, segmentCapacity, addTtlUpdates, addUndeletes);
    }
  }

  /**
   * Destroys state and cleans up as required.
   * @throws IOException
   * @throws StoreException
   */
  void destroy() throws IOException, StoreException {
    shutDownExecutorService(scheduler, 30, TimeUnit.SECONDS);
    index.close(false);
    log.close(false);
    assertTrue(tempDir + " could not be cleaned", StoreTestUtils.cleanDirectory(tempDir, false));
  }

  /**
   * Adds {@code count} number of put entries each of size {@code size} and that expire at {@code expiresAtMs} to the
   * index (both real and reference).
   * @param count the number of PUT entries to add.
   * @param size the size of each PUT entry.
   * @param expiresAtMs the time at which each of the PUT entries expires.
   * @return the list of the added entries.
   * @throws StoreException
   */
  List<IndexEntry> addPutEntries(int count, long size, long expiresAtMs) throws StoreException {
    if (count <= 0) {
      throw new IllegalArgumentException("Number of put entries to add cannot be <= 0");
    }
    List<IndexEntry> indexEntries = new ArrayList<>(count);
    Offset expectedJournalLastOffset = null;
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    for (int i = 0; i < count; i++) {
      byte[] dataWritten = appendToLog(size);
      FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, size);
      Offset indexSegmentStartOffset = generateReferenceIndexSegmentStartOffset(fileSpan.getStartOffset());
      if (!referenceIndex.containsKey(indexSegmentStartOffset)) {
        // rollover will occur
        advanceTime(DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS);
        referenceIndex.put(indexSegmentStartOffset, new TreeMap<>());
      }
      MockId id = getUniqueId();
      IndexValue value =
          new IndexValue(size, fileSpan.getStartOffset(), expiresAtMs, time.milliseconds(), id.getAccountId(),
              id.getContainerId());
      IndexEntry entry = new IndexEntry(id, value);
      indexEntries.add(entry);
      logOrder.put(fileSpan.getStartOffset(), new Pair<>(id, new LogEntry(dataWritten, value)));
      allKeys.computeIfAbsent(id, k -> new TreeSet<>()).add(value);
      referenceIndex.get(indexSegmentStartOffset).computeIfAbsent(id, k -> new TreeSet<>()).add(value);
      if (expiresAtMs != Utils.Infinite_Time && expiresAtMs < time.milliseconds()) {
        expiredKeys.add(id);
      } else {
        liveKeys.add(id);
      }
      index.addToIndex(Collections.singletonList(entry), fileSpan);
      lastModifiedTimesInSecs.put(indexSegmentStartOffset, value.getOperationTimeInMs() / Time.MsPerSec);
      expectedJournalLastOffset = fileSpan.getStartOffset();
      endOffsetOfPrevMsg = fileSpan.getEndOffset();
    }
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", expectedJournalLastOffset, index.journal.getLastOffset());
    return indexEntries;
  }

  /**
   * Makes the TTL of {@code id} infinite.
   * @param id the {@link MockId} whose TTL needs to be made infinite.
   * @param forcePut if {@code true}, forces a ttl update record to be created even if a PUT isn't present. Does NOT
   *                 force if a ttl update or delete is present.
   * @return the {@link FileSpan} at which the update record was added
   * @throws StoreException
   */
  FileSpan makePermanent(MockId id, boolean forcePut) throws StoreException {
    return makePermanent(id, forcePut, MessageInfo.LIFE_VERSION_FROM_FRONTEND);
  }

  /**
   * Makes the TTL of {@code id} infinite.
   * @param id the {@link MockId} whose TTL needs to be made infinite.
   * @param forcePut if {@code true}, forces a ttl update record to be created even if a PUT isn't present. Does NOT
   *                 force if a ttl update or delete is present.
   * @param lifeVersion the life version of the ttl update
   * @return the {@link FileSpan} at which the update record was added
   * @throws StoreException
   */
  FileSpan makePermanent(MockId id, boolean forcePut, short lifeVersion) throws StoreException {
    IndexValue value = getExpectedValue(id, false);
    boolean hasLifeVersion = IndexValue.hasLifeVersion(lifeVersion);
    if (!forcePut && value == null) {
      throw new IllegalArgumentException(id + " does not exist in the index");
    } else if (expiredKeys.contains(id) || (value != null && value.isDelete())) {
      throw new IllegalArgumentException(id + " is deleted or expired");
    } else if (value != null && value.isTtlUpdate()) {
      throw new IllegalArgumentException(id + " ttl has already been updated");
    }

    byte[] dataWritten = appendToLog(CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE);
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE);
    Offset startOffset = fileSpan.getStartOffset();
    Offset indexSegmentStartOffset = generateReferenceIndexSegmentStartOffset(startOffset);
    if (!referenceIndex.containsKey(indexSegmentStartOffset)) {
      // rollover will occur
      advanceTime(DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS);
      referenceIndex.put(indexSegmentStartOffset, new TreeMap<>());
    }
    IndexValue newValue;
    if (value != null) {
      short ttlUpdateLifeVersion = hasLifeVersion ? lifeVersion : value.getLifeVersion();
      newValue =
          new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), Utils.Infinite_Time, time.milliseconds(),
              value.getAccountId(), value.getContainerId(), ttlUpdateLifeVersion);
      newValue.setNewOffset(startOffset);
      newValue.setNewSize(CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE);
    } else {
      short ttlUpdateLifeVersion = hasLifeVersion ? lifeVersion : 0;
      newValue =
          new IndexValue(CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, startOffset, IndexValue.FLAGS_DEFAULT_VALUE,
              Utils.Infinite_Time, time.milliseconds(), id.getAccountId(), id.getContainerId(), ttlUpdateLifeVersion);
      newValue.clearOriginalMessageOffset();
    }
    newValue.clearFlag(IndexValue.Flags.Undelete_Index);
    newValue.setFlag(IndexValue.Flags.Ttl_Update_Index);
    if (forcePut) {
      index.addToIndex(new IndexEntry(id, newValue), fileSpan);
    } else {
      index.markAsPermanent(id, fileSpan, newValue.getOperationTimeInMs());
    }
    ttlUpdatedKeys.add(id);
    logOrder.put(startOffset, new Pair<>(id, new LogEntry(dataWritten, newValue)));
    allKeys.computeIfAbsent(id, k -> new TreeSet<>()).add(newValue);
    referenceIndex.get(indexSegmentStartOffset).computeIfAbsent(id, k -> new TreeSet<>()).add(newValue);
    lastModifiedTimesInSecs.put(indexSegmentStartOffset,
        TimeUnit.MILLISECONDS.toSeconds(newValue.getOperationTimeInMs()));
    endOffsetOfPrevMsg = fileSpan.getEndOffset();
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", startOffset, index.journal.getLastOffset());
    return fileSpan;
  }

  /**
   * Adds a delete entry in the index (real and reference) for {@code idToDelete}.
   * @param idToDelete the id to be deleted.
   * @return the {@link FileSpan} of the added entries.
   * @throws StoreException
   */
  FileSpan addDeleteEntry(MockId idToDelete) throws StoreException {
    return addDeleteEntry(idToDelete, null);
  }

  FileSpan addDeleteEntry(MockId idToDelete, MessageInfo info) throws StoreException {
    return addDeleteEntry(idToDelete, info, MessageInfo.LIFE_VERSION_FROM_FRONTEND);
  }

  /**
   * Adds a delete entry in the index (real and reference) for {@code idToDelete}.
   * @param idToDelete the id to be deleted.
   * @param info the {@link MessageInfo} to use in case of recovery to fetch accountId, containerId and operationTime.
   * @return the {@link FileSpan} of the added entries.
   * @throws StoreException
   */
  FileSpan addDeleteEntry(MockId idToDelete, MessageInfo info, short lifeVersion) throws StoreException {
    IndexValue value = getExpectedValue(idToDelete, false);
    boolean hasLifeVersion = IndexValue.hasLifeVersion(lifeVersion);
    //check if we should reject this delete entry
    if (value == null && !allKeys.containsKey(idToDelete) && info == null) {
      throw new IllegalArgumentException(idToDelete + " does not exist in the index");
    } else if (value != null && value.isDelete()) {
      if (lifeVersion == MessageInfo.LIFE_VERSION_FROM_FRONTEND) {
        throw new IllegalArgumentException(idToDelete + " is already deleted");
      } else {
        if (value.getLifeVersion() >= lifeVersion) {
          throw new IllegalArgumentException(idToDelete + " lifeVersion Conflict");
        }
      }
    }

    //add delete to the log
    byte[] dataWritten = appendToLog(CuratedLogIndexState.DELETE_RECORD_SIZE);
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, CuratedLogIndexState.DELETE_RECORD_SIZE);
    Offset startOffset = fileSpan.getStartOffset();
    Offset indexSegmentStartOffset = generateReferenceIndexSegmentStartOffset(startOffset);
    if (!referenceIndex.containsKey(indexSegmentStartOffset)) {
      // rollover will occur
      advanceTime(DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS);
      referenceIndex.put(indexSegmentStartOffset, new TreeMap<>());
    }

    //add delete to the index
    boolean forceDeleteEntry = false;
    IndexValue newValue;
    if (value != null) {
      short delLifeVersion = hasLifeVersion ? lifeVersion : value.getLifeVersion();
      newValue = new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(),
          info == null ? time.milliseconds() : info.getOperationTimeMs(), value.getAccountId(), value.getContainerId(),
          delLifeVersion);
      newValue.setNewOffset(startOffset);
      newValue.setNewSize(CuratedLogIndexState.DELETE_RECORD_SIZE);
      if (!value.isPut()) {
        newValue.clearOriginalMessageOffset();
      }
    } else if (allKeys.containsKey(idToDelete)) {
      // this is because a ttl update record was forcibly added
      value = allKeys.get(idToDelete).last();
      short delLifeVersion = hasLifeVersion ? lifeVersion : value.getLifeVersion();
      newValue =
          new IndexValue(CuratedLogIndexState.DELETE_RECORD_SIZE, startOffset, value.getFlags(), value.getExpiresAtMs(),
              time.milliseconds(), value.getAccountId(), value.getContainerId(), delLifeVersion);
      newValue.clearOriginalMessageOffset();
      forceDeleteEntry = true;
    } else {
      // Force to add the delete entry to the log to mimic the situation when compaction happens to remove the put.
      newValue = new IndexValue(CuratedLogIndexState.DELETE_RECORD_SIZE, startOffset, IndexValue.FLAGS_DEFAULT_VALUE,
          info.getExpirationTimeInMs(), info.getOperationTimeMs(), info.getAccountId(), info.getContainerId(),
          info.getLifeVersion());
      newValue.clearOriginalMessageOffset();
      if (info.isTtlUpdated()) {
        newValue.setFlag(IndexValue.Flags.Ttl_Update_Index);
      }
      forceDeleteEntry = true;
    }
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.clearFlag(IndexValue.Flags.Undelete_Index);
    if (forceDeleteEntry) {
      index.addToIndex(new IndexEntry(idToDelete, newValue), fileSpan);
    } else {
      index.markAsDeleted(idToDelete, fileSpan, null, newValue.getOperationTimeInMs(), newValue.getLifeVersion());
    }
    markAsDeleted(idToDelete);
    logOrder.put(startOffset, new Pair<>(idToDelete, new LogEntry(dataWritten, newValue)));
    allKeys.computeIfAbsent(idToDelete, k -> new TreeSet<>()).add(newValue);
    referenceIndex.get(indexSegmentStartOffset).computeIfAbsent(idToDelete, k -> new TreeSet<>()).add(newValue);
    lastModifiedTimesInSecs.put(indexSegmentStartOffset, newValue.getOperationTimeInMs() / Time.MsPerSec);
    endOffsetOfPrevMsg = fileSpan.getEndOffset();
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", startOffset, index.journal.getLastOffset());
    return fileSpan;
  }

  /**
   * Adds a delete entry in the index (real and reference) for {@code idToDelete}.
   * @param idToDelete the id to be deleted.
   * @return the {@link FileSpan} of the added entries.
   * @throws IOException
   * @throws StoreException
   */
  FileSpan addUndeleteEntry(MockId idToDelete) throws StoreException {
    return addUndeleteEntry(idToDelete, MessageInfo.LIFE_VERSION_FROM_FRONTEND);
  }

  /**
   * Adds an undelete entry in the index (real and reference) for {@code idToDelete}.
   * @param idToUndelete the id to be deleted.
   * @return the {@link FileSpan} of the added entries.
   * @throws StoreException
   */
  FileSpan addUndeleteEntry(MockId idToUndelete, short lifeVersion) throws StoreException {
    IndexValue value = getExpectedValue(idToUndelete, false);
    boolean hasLifeVersion = IndexValue.hasLifeVersion(lifeVersion);
    if (value == null) {
      throw new IllegalArgumentException(idToUndelete + " does not exist in the index");
    }

    // add undelete to log
    byte[] dataWritten = appendToLog(CuratedLogIndexState.UNDELETE_RECORD_SIZE);
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, CuratedLogIndexState.UNDELETE_RECORD_SIZE);

    Offset startOffset = fileSpan.getStartOffset();
    Offset indexSegmentStartOffset = generateReferenceIndexSegmentStartOffset(startOffset);
    if (!referenceIndex.containsKey(indexSegmentStartOffset)) {
      // rollover will occur
      advanceTime(DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS);
      referenceIndex.put(indexSegmentStartOffset, new TreeMap<>());
    }

    // After compaction, undelete record would not be kept without Put and Delete record, so don't bother to forcely
    // add the undelete record to log.
    short undelLifeVersion = hasLifeVersion ? lifeVersion : (short) (value.getLifeVersion() + 1);
    IndexValue newValue = new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(),
        time.milliseconds(), value.getAccountId(), value.getContainerId(), undelLifeVersion);
    newValue.setNewOffset(startOffset);
    newValue.setNewSize(CuratedLogIndexState.UNDELETE_RECORD_SIZE);
    newValue.clearOriginalMessageOffset();
    newValue.setFlag(IndexValue.Flags.Undelete_Index);
    newValue.clearFlag(IndexValue.Flags.Delete_Index);
    if (hasLifeVersion) {
      index.markAsUndeleted(idToUndelete, fileSpan, newValue.getOperationTimeInMs(), lifeVersion);
    } else {
      index.markAsUndeleted(idToUndelete, fileSpan, newValue.getOperationTimeInMs());
    }
    markAsUndeleted(idToUndelete);
    logOrder.put(startOffset, new Pair<>(idToUndelete, new LogEntry(dataWritten, newValue)));
    allKeys.computeIfAbsent(idToUndelete, k -> new TreeSet<>()).add(newValue);
    referenceIndex.get(indexSegmentStartOffset).computeIfAbsent(idToUndelete, k -> new TreeSet<>()).add(newValue);
    lastModifiedTimesInSecs.put(indexSegmentStartOffset, newValue.getOperationTimeInMs() / Time.MsPerSec);
    endOffsetOfPrevMsg = fileSpan.getEndOffset();
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", startOffset, index.journal.getLastOffset());
    return fileSpan;
  }

  /**
   * Advances time by {@code ms} and adjusts {@link #liveKeys} if any of the keys in it expire.
   * @param ms the amount in ms to advance.
   */
  void advanceTime(long ms) {
    time.sleep(ms);
    Iterator<MockId> liveKeysIterator = liveKeys.iterator();
    while (liveKeysIterator.hasNext()) {
      MockId id = liveKeysIterator.next();
      IndexValue value = getExpectedValue(id, false);
      if (value.getExpiresAtMs() != Utils.Infinite_Time && value.getExpiresAtMs() < time.milliseconds()) {
        expiredKeys.add(id);
        liveKeysIterator.remove();
      }
    }
  }

  /**
   * Appends random data of size {@code size} to the {@link #log}.
   * @param size the size of data that needs to be appended.
   * @return the data that was appended.
   * @throws StoreException
   */
  byte[] appendToLog(long size) throws StoreException {
    byte[] bytes = TestUtils.getRandomBytes((int) size);
    if (size > CuratedLogIndexState.HARD_DELETE_START_OFFSET) {
      // ensure at least one byte is set to 1 for hard delete verification purposes
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
    return getUniqueId(10);
  }

  /**
   * @param length the length of the string to use to create the {@link MockId}.
   * @return a {@link MockId} that is unique and has not been generated before in this run.
   */
  MockId getUniqueId(int length) {
    MockId id;
    do {
      id = new MockId(TestUtils.getRandomString(length));
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
    return getExpectedValue(id, wantPut ? EnumSet.of(PersistentIndex.IndexEntryType.PUT)
        : EnumSet.of(PersistentIndex.IndexEntryType.PUT, PersistentIndex.IndexEntryType.DELETE,
            PersistentIndex.IndexEntryType.UNDELETE), null);
  }

  /**
   * Gets the value that is expected to obtained from the {@link PersistentIndex}.
   * @param id the {@link MockId} whose value is required.
   * @param types the types of {@link IndexEntry} that are ok to return
   * @param fileSpan the {@link FileSpan} to search in. Can be {@code null}
   * @return the value that is expected to obtained from the {@link PersistentIndex}
   */
  IndexValue getExpectedValue(MockId id, EnumSet<PersistentIndex.IndexEntryType> types, FileSpan fileSpan) {
    if (!allKeys.containsKey(id)) {
      return null;
    }
    TreeSet<IndexValue> indexValues = allKeys.get(id);
    if (fileSpan != null) {
      Offset modifiedStart = referenceIndex.floorKey(fileSpan.getStartOffset());
      Offset modifiedEnd = new Offset(fileSpan.getEndOffset().getName(), fileSpan.getEndOffset().getOffset() + 1);
      modifiedEnd = referenceIndex.ceilingKey(modifiedEnd);
      if (modifiedEnd == null) {
        modifiedEnd = index.getCurrentEndOffset();
      }
      fileSpan = new FileSpan(modifiedStart, modifiedEnd);
    }
    List<IndexValue> toConsider = new ArrayList<>();
    for (IndexValue value : indexValues) {
      if (isWithinFileSpan(value.getOffset(), fileSpan)) {
        toConsider.add(value);
      }
    }

    IndexValue retCandidate = null;
    ListIterator<IndexValue> iterator = toConsider.listIterator(toConsider.size());
    while (iterator.hasPrevious()) {
      IndexValue value = iterator.previous();
      if (types.contains(PersistentIndex.IndexEntryType.DELETE) && value.isDelete()) {
        retCandidate = value;
        break;
      } else if (types.contains(PersistentIndex.IndexEntryType.UNDELETE) && value.isUndelete()) {
        retCandidate = value;
        break;
      } else if (types.contains(PersistentIndex.IndexEntryType.TTL_UPDATE) && !value.isDelete() && !value.isUndelete()
          && value.isTtlUpdate()) {
        retCandidate = value;
        break;
      } else if (types.contains(PersistentIndex.IndexEntryType.PUT) && value.isPut()) {
        retCandidate = value;
        break;
      }
    }

    if (retCandidate != null) {
      IndexValue latest = toConsider.get(toConsider.size() - 1);
      if (latest.getExpiresAtMs() != retCandidate.getExpiresAtMs() || (!retCandidate.isTtlUpdate()
          && latest.isTtlUpdate())) {
        retCandidate = new IndexValue(retCandidate.getOffset().getName(), retCandidate.getBytes(),
            retCandidate.getFormatVersion());
        retCandidate.setFlag(IndexValue.Flags.Ttl_Update_Index);
        retCandidate.setExpiresAtMs(latest.getExpiresAtMs());
      }
    }
    return retCandidate;
  }

  /**
   * @param offset the {@link Offset} to check
   * @param fileSpan the {@link FileSpan} to use for checking
   * @return {@code true} if {@code offset} is within the {@code fileSpan}.
   */
  private boolean isWithinFileSpan(Offset offset, FileSpan fileSpan) {
    return fileSpan == null
        || offset.compareTo(fileSpan.getStartOffset()) >= 0 && offset.compareTo(fileSpan.getEndOffset()) < 0;
  }

  /**
   * Gets the data that is expected to obtained from the {@link Log}.
   * @param id the {@link MockId} whose value is required.
   * @param wantPut {@code true} if the {@link IndexValue} of the PUT entry is required.
   * @return the value that is expected to obtained from the {@link PersistentIndex}
   */
  byte[] getExpectedData(MockId id, boolean wantPut) {
    Offset offset = getExpectedValue(id, wantPut).getOffset();
    return logOrder.get(offset).getSecond().buffer;
  }

  /**
   * Returns all ids that have PUT records in the provided {@code segment}.
   * @param segment the {@link LogSegment} to get the ids of all records for.
   * @return all ids that have PUT records in the provided {@code segment}.
   */
  Set<MockId> getIdsWithPutsInLogSegment(LogSegment segment) {
    Set<MockId> idsInSegment = new HashSet<>();
    Offset indexSegmentStartOffset = new Offset(segment.getName(), segment.getStartOffset());
    while (indexSegmentStartOffset != null && indexSegmentStartOffset.getName().equals(segment.getName())) {
      referenceIndex.get(indexSegmentStartOffset).forEach((key, values) -> values.forEach(value -> {
        if (value.getFlags() == IndexValue.FLAGS_DEFAULT_VALUE) {
          idsInSegment.add(key);
        }
      }));
      indexSegmentStartOffset = referenceIndex.higherKey(indexSegmentStartOffset);
    }
    return idsInSegment;
  }

  /**
   * Gets an ID to ttl update from the index segment with start offset {@code indexSegmentStartOffset}. The returned ID
   * will have been added to {@link #ttlUpdatedKeys}. A call to {@link #makePermanent(MockId, boolean)} (MockId)} is expected.
   * @param indexSegmentStartOffset the start offset of the index segment from which an ID is required.
   * @return an ID to ttl update from the index segment with start offset {@code indexSegmentStartOffset}. {@code null}
   * if there is no such candidate.
   */
  MockId getIdToTtlUpdateFromIndexSegment(Offset indexSegmentStartOffset) {
    MockId updateCandidate = null;
    for (MockId id : referenceIndex.get(indexSegmentStartOffset).keySet()) {
      if (liveKeys.contains(id) && !ttlUpdatedKeys.contains(id)
          && getExpectedValue(id, true).getExpiresAtMs() != Utils.Infinite_Time) {
        updateCandidate = id;
        break;
      }
    }
    if (updateCandidate != null) {
      // adding here because recovery tests don't immediately add to the index and may call this function multiple
      // times
      ttlUpdatedKeys.add(updateCandidate);
    }
    return updateCandidate;
  }

  /**
   * Gets an ID to delete from the index segment with start offset {@code indexSegmentStartOffset}. The returned ID will
   * have been removed from {@link #liveKeys} and added to {@link #deletedKeys}. A call to
   * {@link #addDeleteEntry(MockId)} is expected.
   * @param indexSegmentStartOffset the start offset of the index segment from which an ID is required.
   * @param updated if {@code true}, returns an ID that has been updated. Otherwise returns one that has not been
   *                updated
   * @return an ID to delete from the index segment with start offset {@code indexSegmentStartOffset}. {@code null} if
   * there is no such candidate.
   */
  MockId getIdToDeleteFromIndexSegment(Offset indexSegmentStartOffset, boolean updated) {
    MockId deleteCandidate = null;
    TreeMap<MockId, TreeSet<IndexValue>> indexSegment = referenceIndex.get(indexSegmentStartOffset);
    for (MockId id : indexSegment.keySet()) {
      if (liveKeys.contains(id) && getExpectedValue(id, true).getExpiresAtMs() == Utils.Infinite_Time
          && updated == ttlUpdatedKeys.contains(id)) {
        deleteCandidate = id;
        break;
      }
    }
    if (deleteCandidate != null) {
      // adding here because recovery tests don't immediately add to the index and may call this function multiple
      // times
      markAsDeleted(deleteCandidate);
    }
    return deleteCandidate;
  }

  /**
   * Gets an ID to ttl update from the given log segment. The returned ID will have been added to
   * {@link #ttlUpdatedKeys}. A call to {@link #makePermanent(MockId, boolean)} (MockId)} is expected.
   * @param segment the {@link LogSegment} from which an ID is required. {@code null} if there is no such candidate.
   * @return the ID to ttl update.
   */
  MockId getIdToTtlUpdateFromLogSegment(LogSegment segment) {
    MockId updateCandidate;
    Offset indexSegmentStartOffset = new Offset(segment.getName(), segment.getStartOffset());
    do {
      updateCandidate = getIdToTtlUpdateFromIndexSegment(indexSegmentStartOffset);
      indexSegmentStartOffset = referenceIndex.higherKey(indexSegmentStartOffset);
      if (indexSegmentStartOffset == null || !indexSegmentStartOffset.getName().equals(segment.getName())) {
        break;
      }
    } while (updateCandidate == null);
    return updateCandidate;
  }

  /**
   * Gets an ID to delete from the given log segment. The returned ID will have been removed from {@link #liveKeys} and
   * added to {@link #deletedKeys}.
   * @param segment the {@link LogSegment} from which an ID is required. {@code null} if there is no such candidate.
   * @param updated if {@code true}, returns an ID that has been updated. Otherwise returns one that has not been
   *                updated
   * @return the ID to delete.
   */
  MockId getIdToDeleteFromLogSegment(LogSegment segment, boolean updated) {
    MockId deleteCandidate;
    Offset indexSegmentStartOffset = new Offset(segment.getName(), segment.getStartOffset());
    do {
      deleteCandidate = getIdToDeleteFromIndexSegment(indexSegmentStartOffset, updated);
      indexSegmentStartOffset = referenceIndex.higherKey(indexSegmentStartOffset);
      if (indexSegmentStartOffset == null || !indexSegmentStartOffset.getName().equals(segment.getName())) {
        break;
      }
    } while (deleteCandidate == null);
    return deleteCandidate;
  }

  /**
   * Gets the expected size of the valid data at {@code deleteReferenceTimeMs} in {@code segment}.
   * @param segment the {@link LogSegment} whose valid size is required.
   * @param deleteReferenceTimeMs the reference time in ms until which deletes are relevant.
   * @param expiryReferenceTimeMs the reference time in ms until which expirations are relevant
   * @param fileSpanUnderCompaction only useful for compaction tests to help determine whether update records will be
   *                                cleaned up. All other use cases should use {@code null}.
   * @return the expected size of the valid data at {@code deleteReferenceTimeMs} in {@code segment}.
   */
  long getValidDataSizeForLogSegment(LogSegment segment, long deleteReferenceTimeMs, long expiryReferenceTimeMs,
      FileSpan fileSpanUnderCompaction) {
    List<IndexEntry> validEntries =
        getValidIndexEntriesForLogSegment(segment, deleteReferenceTimeMs, expiryReferenceTimeMs,
            fileSpanUnderCompaction);
    long size = 0;
    for (IndexEntry indexEntry : validEntries) {
      size += indexEntry.getValue().getSize();
    }
    return size;
  }

  /**
   * Gets all the valid index entries (taking into account different reference times) in the {@code segment}.
   * @param segment the {@link LogSegment} from which valid index entries are required.
   * @param deleteReferenceTimeMs the reference time in ms until which deletes are relevant.
   * @param expiryReferenceTimeMs the reference time in ms until which expirations are relevant
   * @param fileSpanUnderCompaction only useful for compaction tests to help determine whether update records will be
   *                                cleaned up. All other use cases should use {@code null}.
   * @return all the valid index entries in the {@code segment}.
   */
  List<IndexEntry> getValidIndexEntriesForLogSegment(LogSegment segment, long deleteReferenceTimeMs,
      long expiryReferenceTimeMs, FileSpan fileSpanUnderCompaction) {
    List<IndexEntry> validEntries = new ArrayList<>();
    Offset indexSegmentStartOffset = new Offset(segment.getName(), segment.getStartOffset());
    while (indexSegmentStartOffset != null && indexSegmentStartOffset.getName().equals(segment.getName())) {
      validEntries.addAll(
          getValidIndexEntriesForIndexSegment(indexSegmentStartOffset, deleteReferenceTimeMs, expiryReferenceTimeMs,
              fileSpanUnderCompaction));
      indexSegmentStartOffset = referenceIndex.higherKey(indexSegmentStartOffset);
    }
    return validEntries;
  }

  /**
   * Verify that deleted entries are hard deleted
   * @param deletedIds the set of {@link MockId}s that are deleted
   * @throws IOException
   */
  void verifyEntriesForHardDeletes(Set<MockId> deletedIds) throws IOException {
    for (MockId id : deletedIds) {
      IndexValue putValue = getExpectedValue(id, true);
      if (putValue != null) {
        Offset offset = putValue.getOffset();
        LogSegment segment = log.getSegment(offset.getName());
        long size = putValue.getSize() - CuratedLogIndexState.HARD_DELETE_START_OFFSET
            - CuratedLogIndexState.HARD_DELETE_LAST_PART_SIZE;
        ByteBuffer readBuf = ByteBuffer.allocate((int) size);
        segment.readInto(readBuf, offset.getOffset() + CuratedLogIndexState.HARD_DELETE_START_OFFSET);
        readBuf.flip();
        while (readBuf.hasRemaining()) {
          assertEquals("Hard delete has not zeroed out the data for id " + id, (byte) 0, readBuf.get());
        }
      }
    }
  }

  /**
   * Returns {@code true} if the {@code id} is considered deleted at {@code referenceTimeMs}.
   * @param id the {@link MockId} to check.
   * @param referenceTimeMs the reference time in ms at which the check needs to be performed.
   * @return {@code true} if the {@code id} is considered deleted at {@code referenceTimeMs}. {@code false} otherwise.
   */
  boolean isDeletedAt(MockId id, long referenceTimeMs) {
    IndexValue value = getExpectedValue(id, false);
    Offset deleteIndexSegmentStartOffset = value.isDelete() ? referenceIndex.floorKey(value.getOffset()) : null;
    return deleteIndexSegmentStartOffset != null
        && lastModifiedTimesInSecs.get(deleteIndexSegmentStartOffset) * Time.MsPerSec < referenceTimeMs;
  }

  /**
   * Returns {@code true} if the {@code id} is considered expired at {@code referenceTimeMs}.
   * @param id the {@link MockId} to check.
   * @param referenceTimeMs the reference time in ms at which the check needs to be performed.
   * @return {@code true} if the {@code id} is considered expired at {@code referenceTimeMs}. {@code false} otherwise.
   */
  boolean isExpiredAt(MockId id, long referenceTimeMs) {
    long expiresAtMs = getExpectedValue(id, false).getExpiresAtMs();
    return expiresAtMs != Utils.Infinite_Time && expiresAtMs < referenceTimeMs;
  }

  /**
   * Ensures that the {@link PersistentIndex} is sane and correct by checking that
   * 1. It contains no duplicate entries.
   * 2. The ordering of PUT and DELETE entries is correct.
   * 3. There are no offsets in the log not accounted for in the index.
   * @throws StoreException
   */
  void verifyRealIndexSanity() throws StoreException {
    IndexSegment prevIndexSegment = null;
    Map<MockId, Boolean> keyToDeleteSeenMap = new HashMap<>();
    Map<MockId, Boolean> keyToTtlUpdateSeenMap = new HashMap<>();
    Map<MockId, Short> keyToLifeVersionMap = new HashMap<>();
    for (IndexSegment indexSegment : index.getIndexSegments().values()) {
      Offset indexSegmentStartOffset = indexSegment.getStartOffset();
      if (prevIndexSegment == null) {
        assertEquals("There are offsets in the log not accounted for in index", log.getFirstSegment().getStartOffset(),
            indexSegmentStartOffset.getOffset());
      } else if (prevIndexSegment.getLogSegmentName().equals(indexSegment.getLogSegmentName())) {
        assertEquals("There are offsets in the log not accounted for in index", prevIndexSegment.getEndOffset(),
            indexSegmentStartOffset);
      } else {
        LogSegment segment = log.getSegment(prevIndexSegment.getLogSegmentName());
        assertEquals("There are offsets in the log not accounted for in index", segment.getEndOffset(),
            prevIndexSegment.getEndOffset().getOffset());
        segment = log.getNextSegment(segment);
        assertEquals("There are offsets in the log not accounted for in index", segment.getStartOffset(),
            indexSegmentStartOffset.getOffset());
      }

      List<IndexEntry> indexEntries = new ArrayList<>();
      indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), indexEntries, new AtomicLong(0),
          false);
      for (IndexEntry entry : indexEntries) {
        MockId id = (MockId) entry.getKey();
        IndexValue value = entry.getValue();
        Boolean deleteSeen = keyToDeleteSeenMap.get(id);
        Boolean ttlUpdateSeen = keyToTtlUpdateSeenMap.get(id);
        if (value.isDelete()) {
          // should not be repeated
          assertTrue("Duplicated DELETE record for " + id, deleteSeen == null || !deleteSeen);
          keyToDeleteSeenMap.put(id, true);
        } else if (value.isUndelete()) {
          short prevLifeVersion = keyToLifeVersionMap.put(id, value.getLifeVersion());
          short currentLifeVersion = value.getLifeVersion();
          if (PersistentIndex.CURRENT_VERSION == PersistentIndex.VERSION_3) {
            assertTrue("Undelete's lifeVersion should be greater than previous one, Undelete: " + currentLifeVersion
                + " Previous: " + prevLifeVersion, prevLifeVersion < currentLifeVersion);
          }
          keyToDeleteSeenMap.put(id, false);
        } else if (value.isTtlUpdate()) {
          // should not be repeated
          assertTrue("Duplicated TTL update record for " + id, ttlUpdateSeen == null || !ttlUpdateSeen);
          // should not after a delete record
          assertTrue("TTL update record after delete record for " + id, deleteSeen == null || !deleteSeen);
          keyToTtlUpdateSeenMap.put(id, true);
          keyToDeleteSeenMap.putIfAbsent(id, false);
        } else {
          if (deleteSeen != null) {
            if (deleteSeen) {
              fail("PUT record after delete record for " + id);
            } else {
              fail("Duplicated PUT record for " + id);
            }
          }
          assertNull("Put record encountered after TTL update record", ttlUpdateSeen);
          keyToTtlUpdateSeenMap.put(id, false);
          keyToDeleteSeenMap.put(id, false);
        }
        keyToLifeVersionMap.put(id, value.getLifeVersion());
      }
      long expectedOffset = indexSegmentStartOffset.getOffset();
      NavigableSet<IndexEntry> indexEntriesByOffset = new TreeSet<>(PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
      indexEntriesByOffset.addAll(indexEntries);
      for (IndexEntry entry : indexEntriesByOffset) {
        IndexValue value = entry.getValue();
        assertEquals("There are offsets in the log not accounted for in index", expectedOffset,
            value.getOffset().getOffset());
        expectedOffset += value.getSize();
      }
      if (prevIndexSegment != null) {
        assertTrue("Last modified time of an older index segment (" + prevIndexSegment.getLastModifiedTimeMs()
                + ") > newer index segment (" + indexSegment.getLastModifiedTimeMs() + ")",
            prevIndexSegment.getLastModifiedTimeMs() <= indexSegment.getLastModifiedTimeMs());
      }
      prevIndexSegment = indexSegment;
    }
  }

  /**
   * Creates the index instance with the provided {@code metricRegistry}.
   * @param newScheduler the {@link ScheduledExecutorService} to use in testing. If null, a default scheduler will be created to use.
   * @throws StoreException
   */
  void initIndex(ScheduledExecutorService newScheduler) throws StoreException {
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    sessionId = UUID.randomUUID();
    metricRegistry = new MetricRegistry();
    metrics = new StoreMetrics(metricRegistry);
    shutDownExecutorService(scheduler, 1, TimeUnit.SECONDS);
    scheduler = newScheduler == null ? Utils.newScheduler(1, false) : newScheduler;
    index = new PersistentIndex(tempDirStr, tempDirStr, scheduler, log, config, CuratedLogIndexState.STORE_KEY_FACTORY,
        recovery, hardDelete, DISK_IO_SCHEDULER, metrics, time, sessionId, incarnationId);
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
      index.close(false);
      if (deleteCleanShutdownFile) {
        assertTrue("The clean shutdown file could not be deleted",
            new File(tempDir, PersistentIndex.CLEAN_SHUTDOWN_FILENAME).delete());
      }
    }
    initIndex(null);
  }

  void reloadIndexAndVerifyState(boolean closeBeforeReload, boolean deleteCleanShutdownFile) throws StoreException {
    reloadIndex(closeBeforeReload, deleteCleanShutdownFile);
    verifyState(isLogSegmented);
  }

  /**
   * Reloads the log and index by closing and recreating the class variables.
   * @param initIndex creates the index instance if {@code true}, if not, sets {@link #index} to {@code null} and it
   *                  has to be initialized with a call to {@link #initIndex(ScheduledExecutorService)}.
   * @throws IOException
   * @throws StoreException
   */
  void reloadLog(boolean initIndex) throws IOException, StoreException {
    long segmentCapacity = log.getSegmentCapacity();
    index.close(false);
    log.close(false);
    log = new Log(tempDirStr, LOG_CAPACITY, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
        createStoreConfig(segmentCapacity, true), metrics);
    index = null;
    if (initIndex) {
      initIndex(null);
    }
  }

  /**
   * Closes the index and clears all the index files.
   * @throws StoreException
   */
  void closeAndClearIndex() throws StoreException {
    index.close(false);
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
    logOrder.clear();
    referenceIndex.clear();
    allKeys.clear();
    liveKeys.clear();
    expiredKeys.clear();
  }

  /**
   * Sets up some state in order to make sure all cases are represented and the tests don't need to do any setup
   * individually. For understanding the created index, please read the source code which is annotated with comments.
   * <p/>
   * Also tests critical functionality of {@link PersistentIndex} and behaviour of the {@link Journal} in the index.
   * Also verifies that the state in {@link #referenceIndex} matches the state in the real index.
   * @param isLogSegmented {@code true} if segmented. {@code false} otherwise.
   * @param segmentCapacity the intended capacity of each segment
   * @param addTtlUpdates if {@code true}, adds entries that update TTL.
   * @param addUndeletes if {@code true}, adds undelete entries.
   * @throws IOException
   * @throws StoreException
   */
  private void setupTestState(boolean isLogSegmented, long segmentCapacity, boolean addTtlUpdates, boolean addUndeletes)
      throws IOException, StoreException {
    Offset expectedStartOffset = new Offset(log.getFirstSegment().getName(), log.getFirstSegment().getStartOffset());
    assertEquals("Start Offset of index not as expected", expectedStartOffset, index.getStartOffset());
    assertEquals("End Offset of index not as expected", log.getEndOffset(), index.getCurrentEndOffset());
    // advance time by a second in order to be able to add expired keys and to avoid keys that are expired from
    // being picked for delete.
    advanceTime(Time.MsPerSec);
    assertEquals("Incorrect log segment count", 0, index.getLogSegmentCount());
    long expectedUsedCapacity;
    if (!isLogSegmented) {
      // log is filled about ~50%.
      addCuratedIndexEntriesToLogSegment(segmentCapacity / 2, 1, addTtlUpdates);
      expectedUsedCapacity = segmentCapacity / 2;
      assertEquals("Used capacity reported not as expected", expectedUsedCapacity, index.getLogUsedCapacity());
    } else {
      // first log segment is filled to capacity.
      addCuratedIndexEntriesToLogSegment(segmentCapacity, 1, addTtlUpdates);
      assertEquals("Used capacity reported not as expected", segmentCapacity, index.getLogUsedCapacity());

      // second log segment is filled but has some space at the end (free space has to be less than the lesser of the
      // standard delete and put record sizes so that the next write causes a roll over of log segments).
      addCuratedIndexEntriesToLogSegment(segmentCapacity - (CuratedLogIndexState.DELETE_RECORD_SIZE - 1), 2,
          addTtlUpdates);
      assertEquals("Used capacity reported not as expected",
          2 * segmentCapacity - (CuratedLogIndexState.DELETE_RECORD_SIZE - 1), index.getLogUsedCapacity());

      // third log segment is partially filled and is left as the "active" segment
      // First Index Segment
      // 1 PUT entry
      addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      assertEquals("Incorrect log segment count", 3, index.getLogSegmentCount());
      // DELETE for a key in the first log segment
      LogSegment firstSegment = log.getFirstSegment();
      MockId idToDelete = getIdToDeleteFromLogSegment(firstSegment, false);
      addDeleteEntry(idToDelete);
      // DELETE for a key in the second segment
      LogSegment secondSegment = log.getNextSegment(firstSegment);
      idToDelete = getIdToDeleteFromLogSegment(secondSegment, false);
      addDeleteEntry(idToDelete);
      // 1 DELETE for the PUT in the same segment
      idToDelete = getIdToDeleteFromIndexSegment(referenceIndex.lastKey(), false);
      addDeleteEntry(idToDelete);
      if (addTtlUpdates) {
        // 1 TTL update for a key in first log segment
        MockId idToUpdate = getIdToTtlUpdateFromLogSegment(firstSegment);
        makePermanent(idToUpdate, false);

        // Second Index Segment
        // 1 TTL update for a key in the second log segment
        idToUpdate = getIdToTtlUpdateFromLogSegment(secondSegment);
        makePermanent(idToUpdate, false);
        // 1 TTL update for a key in first log segment
        idToUpdate = getIdToTtlUpdateFromLogSegment(firstSegment);
        makePermanent(idToUpdate, false);
        // 1 DELETE for the key above
        addDeleteEntry(idToUpdate);
        // 1 TTL update for a key in second log segment
        idToUpdate = getIdToTtlUpdateFromLogSegment(secondSegment);
        makePermanent(idToUpdate, false);
        // 1 DELETE for the key above
        addDeleteEntry(idToUpdate);

        // Third Index Segment
        // 1 DELETE for a key that's already ttl updated in the first log segment
        idToDelete = getIdToDeleteFromLogSegment(firstSegment, true);
        addDeleteEntry(idToDelete);
        // 1 DELETE for a key that's already ttl updated in the first second segment
        idToDelete = getIdToDeleteFromLogSegment(secondSegment, true);
        addDeleteEntry(idToDelete);
      }
      if (addUndeletes) {
        addCuratedUndeleteToLogSegment();
      }
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
    assertFalse("Expected nonempty index", index.isEmpty());
  }

  /**
   * Add several undeleted blobs to cover some possible undeleted scenarios.
   * @throws StoreException
   */
  private void addCuratedUndeleteToLogSegment() throws StoreException {
    // Make sure we have these records
    // 1. P, D -> U
    // 2. P, T, D -> U
    // 3. P, D, U, D -> U
    // 4. P, D, U, T, D -> U

    List<IndexEntry> entries = addPutEntries(4, CuratedLogIndexState.DELETE_RECORD_SIZE, Infinite_Time);
    IndexEntry pd = entries.get(0);
    IndexEntry ptd = entries.get(1);
    IndexEntry pdud = entries.get(2);
    IndexEntry pdutd = entries.get(3);

    MockId id = null;
    // finish P, D
    id = (MockId) pd.getKey();
    addDeleteEntry(id);
    // finish P, T, D
    id = (MockId) ptd.getKey();
    makePermanent(id, false);
    addDeleteEntry(id);
    // finish P, D, U, D
    id = (MockId) pdud.getKey();
    addDeleteEntry(id);
    addUndeleteEntry(id);
    addDeleteEntry(id);
    // finish P, D, U, T, D
    id = (MockId) pdutd.getKey();
    addDeleteEntry(id);
    addUndeleteEntry(id);
    makePermanent(id, false);
    addDeleteEntry(id);

    // add undelete to all of them
    for (IndexEntry ent : entries) {
      addUndeleteEntry((MockId) ent.getKey());
    }

    // Add records from replication, make sure we have these records
    // 1. P0 -> U3
    // 2. P0, T0 -> U3
    // 3. P0, U1 -> U3
    entries = addPutEntries(3, CuratedLogIndexState.DELETE_RECORD_SIZE, Infinite_Time);
    IndexEntry ptu = entries.get(1);
    IndexEntry puu = entries.get(2);

    // finish P, T
    id = (MockId) ptu.getKey();
    makePermanent(id, false);

    // finish P, U
    id = (MockId) puu.getKey();
    addUndeleteEntry(id, (short) 1);

    // add undelete to all of them
    for (IndexEntry ent : entries) {
      addUndeleteEntry((MockId) ent.getKey(), (short) 3);
    }
  }

  /**
   * Adds some curated entries into the index in order to ensure a good mix for testing. For understanding the created
   * index, please read the source code which is annotated with comments.
   * @param sizeToMakeIndexEntriesFor the size to make index entries for.
   * @param expectedLogSegmentCount the number of log segments that are expected to assist after the addition of the
   *                                first entry and at the end of the addition of all entries.
   * @param addTtlUpdates if {@code true}, adds entries that update TTL.
   * @throws StoreException
   */
  private void addCuratedIndexEntriesToLogSegment(long sizeToMakeIndexEntriesFor, int expectedLogSegmentCount,
      boolean addTtlUpdates) throws StoreException {
    // First Index Segment
    // 1 PUT
    Offset firstJournalEntryAddedNow =
        addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0).getValue().getOffset();
    assertEquals("Incorrect log segment count", expectedLogSegmentCount, index.getLogSegmentCount());
    assertFalse("Expected nonempty index", index.isEmpty());

    // 2 more PUT
    addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 2 PUT EXPIRED
    addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, 0);
    // 5 entries were added - firstJournalEntryAddedNow should still be a part of the journal
    List<JournalEntry> entries = index.journal.getEntriesSince(firstJournalEntryAddedNow, true);
    assertEquals("There should have been exactly 5 entries returned from the journal", 5, entries.size());

    // Second Index Segment
    // 3 PUT
    addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 1 DELETE for a PUT in the same index segment
    MockId idToDelete = getIdToDeleteFromIndexSegment(referenceIndex.lastKey(), false);
    addDeleteEntry(idToDelete);
    // Maintain one deleted key whose PUT record is in the same log segment. This is used for getBlobReadInfoTest() in IndexTest.
    // The intention is to ensure that hardDelete invokes getMessageInfo() in getDeletedBlobReadOptions() method and successfully
    // throw I/O exception with curated error message. Thus, we can verify disk I/O error can be correctly captured.
    deletedKeyWithPutInSameSegment =
        deletedKeyWithPutInSameSegment == null ? idToDelete : deletedKeyWithPutInSameSegment;
    // 1 DELETE for a PUT in the first index segment
    Offset firstIndexSegmentStartOffset = referenceIndex.lowerKey(referenceIndex.lastKey());
    idToDelete = getIdToDeleteFromIndexSegment(firstIndexSegmentStartOffset, false);
    addDeleteEntry(idToDelete);
    // 5 more entries (for a total of 10) were added - firstJournalEntryAddedNow should still be a part of the journal
    entries = index.journal.getEntriesSince(firstJournalEntryAddedNow, true);
    assertEquals("There should have been exactly 10 entries returned from the journal", 10, entries.size());

    // Third and Fourth Index Segment
    for (int seg = 0; seg < 2; seg++) {
      // 1 PUT
      addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      // > 10 entries were added - firstJournalEntryAddedNow should no longer be a part of the journal
      assertNull("There should no entries returned from the journal",
          index.journal.getEntriesSince(firstJournalEntryAddedNow, true));
      // 2 PUT
      addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      // 1 PUT for an expired blob
      addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, 0);
      // 1 DELETE for the expired PUT
      MockId expiredId = logOrder.lastEntry().getValue().getFirst();
      addDeleteEntry(expiredId);
      deletedKeys.add(expiredId);
      expiredKeys.remove(expiredId);
    }

    Offset fourthIndexSegmentStartOffset = referenceIndex.lastKey();
    Offset thirdIndexSegmentStartOffset = referenceIndex.lowerKey(fourthIndexSegmentStartOffset);
    // Fifth Index Segment
    // 1 PUT entry
    addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 1 DELETE for a PUT in each of the third and fourth segments
    idToDelete = getIdToDeleteFromIndexSegment(thirdIndexSegmentStartOffset, false);
    addDeleteEntry(idToDelete);
    idToDelete = getIdToDeleteFromIndexSegment(fourthIndexSegmentStartOffset, false);
    addDeleteEntry(idToDelete);
    // 1 DELETE for a PUT entry that does not exist
    MockId uniqueId = getUniqueId();
    addDeleteEntry(uniqueId, new MessageInfo(uniqueId, Integer.MAX_VALUE, Utils.Infinite_Time, uniqueId.getAccountId(),
        uniqueId.getContainerId(), time.milliseconds()));
    // Temporary code while other classes are upgraded to understand TTL updates. Once other classes do, the
    // variable "addTtlUpdates" will be dropped
    if (addTtlUpdates) {
      long expiresAtMs = time.milliseconds() + TimeUnit.HOURS.toMillis(1);
      // 1 PUT with non zero expire time
      addPutEntries(1, PUT_RECORD_SIZE, expiresAtMs);
      List<MockId> idsToDelete = new ArrayList<>();

      // sixth index segment
      // 3 PUT with non zero expire time
      addPutEntries(3, PUT_RECORD_SIZE, expiresAtMs);
      // 2 TTL updates for a PUTs in the same segment
      MockId idToUpdate = getIdToTtlUpdateFromIndexSegment(referenceIndex.lastKey());
      makePermanent(idToUpdate, false);
      idsToDelete.add(idToUpdate);
      makePermanent(getIdToTtlUpdateFromIndexSegment(referenceIndex.lastKey()), false);

      Offset prevIndSegStartOff = referenceIndex.lastKey();
      // seventh index segment
      // 1 TTL update for a PUT in the previous segment
      idToUpdate = getIdToTtlUpdateFromIndexSegment(prevIndSegStartOff);
      makePermanent(idToUpdate, false);
      idsToDelete.add(idToUpdate);
      // 4 more PUTs with non zero expire time
      addPutEntries(4, PUT_RECORD_SIZE, expiresAtMs);

      prevIndSegStartOff = referenceIndex.lastKey();
      // eighth index segment
      // 1 PUT, TTL update, DELETE in the same segment
      addPutEntries(1, PUT_RECORD_SIZE, expiresAtMs);
      idToUpdate = getIdToTtlUpdateFromIndexSegment(referenceIndex.lastKey());
      makePermanent(idToUpdate, false);
      addDeleteEntry(idToUpdate);
      // 1 TTL update for a PUT in the prev segment
      makePermanent(getIdToTtlUpdateFromIndexSegment(prevIndSegStartOff), false);
      // 1 more PUT with non zero expire time
      addPutEntries(1, PUT_RECORD_SIZE, expiresAtMs);

      prevIndSegStartOff = referenceIndex.lastKey();
      // ninth index segment
      // 1 TTL update for a PUT in prev segment
      idToUpdate = getIdToTtlUpdateFromIndexSegment(prevIndSegStartOff);
      makePermanent(idToUpdate, false);
      idsToDelete.add(idToUpdate);
      // 3 DELETES from idsToDelete
      assertEquals("Number of IDs to delete has changed", 3, idsToDelete.size());
      for (MockId id : idsToDelete) {
        addDeleteEntry(id);
      }
      // 1 DELETE that has the TTL update flag set but has no corresponding TTL update or PUT entries
      uniqueId = getUniqueId();
      addDeleteEntry(uniqueId,
          new MessageInfo(uniqueId, Integer.MAX_VALUE, true, true, Utils.Infinite_Time, uniqueId.getAccountId(),
              uniqueId.getContainerId(), time.milliseconds()));

      // tenth index segment
      // 1 orphaned TTL update entry (with delete in the same index segment)
      // note that if a TTL update record exists for a key, then a PUT or DELETE record MUST exist
      idToDelete = getUniqueId();
      makePermanent(idToDelete, true);
      // 1 DELETE for the entry above
      addDeleteEntry(idToDelete);
      // 1 orphaned TTL update entry (with delete in another index segment)
      idToDelete = getUniqueId();
      makePermanent(idToDelete, true);
      // 2 PUT
      addPutEntries(2, PUT_RECORD_SIZE, expiresAtMs);

      // eleventh index segment
      // 1 DELETE for an orphaned TTL update entry above
      addDeleteEntry(idToDelete);
      // 2 PUTs with infinite TTL
      addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);
      // 2 PUTs with finite TTL
      addPutEntries(2, PUT_RECORD_SIZE, expiresAtMs);

      // twelfth index segment (setting up for cross log segment)
      // 3 PUTs
      addPutEntries(3, PUT_RECORD_SIZE, expiresAtMs);
      // 1 TTL update for a key in this segment
      makePermanent(getIdToTtlUpdateFromIndexSegment(referenceIndex.lastKey()), false);
    }
    // 1 PUT entry that spans the rest of the data in the segment
    long size = sizeToMakeIndexEntriesFor - index.getCurrentEndOffset().getOffset();
    addPutEntries(1, size, Utils.Infinite_Time);
    assertEquals("Incorrect log segment count", expectedLogSegmentCount, index.getLogSegmentCount());
  }

  /**
   * Verifies that the state in {@link PersistentIndex} is the same as the one in {@link #referenceIndex}.
   * @param isLogSegmented {@code true} if segmented. {@code false} otherwise.
   * @throws StoreException
   */
  private void verifyState(boolean isLogSegmented) throws StoreException {
    verifyRealIndexSanity();
    assertEquals("Incorrect log segment count", isLogSegmented ? 3 : 1, index.getLogSegmentCount());
    NavigableMap<Offset, IndexSegment> realIndex = index.getIndexSegments();
    assertEquals("Number of index segments does not match expected", referenceIndex.size(), realIndex.size());
    Map.Entry<Offset, IndexSegment> realIndexEntry = realIndex.firstEntry();
    for (Map.Entry<Offset, TreeMap<MockId, TreeSet<IndexValue>>> referenceIndexEntry : referenceIndex.entrySet()) {
      assertEquals("Offset of index segment does not match expected", referenceIndexEntry.getKey(),
          realIndexEntry.getKey());
      TreeMap<MockId, TreeSet<IndexValue>> referenceIndexSegment = referenceIndexEntry.getValue();
      IndexSegment realIndexSegment = realIndexEntry.getValue();
      for (Map.Entry<MockId, TreeSet<IndexValue>> referenceIndexSegmentEntry : referenceIndexSegment.entrySet()) {
        MockId id = referenceIndexSegmentEntry.getKey();
        NavigableSet<IndexValue> referenceValues = referenceIndexSegmentEntry.getValue();
        NavigableSet<IndexValue> values = realIndexSegment.find(id);
        assertNotNull("No values returned from real index segment for " + id, values);
        assertTrue("No values returned from real index segment for " + id, values.size() > 0);
        IndexValue value = values.first();
        for (IndexValue referenceValue : referenceValues) {
          assertEquals("Offset does not match", referenceValue.getOffset(), value.getOffset());
          assertEquals("ExpiresAtMs does not match", referenceValue.getExpiresAtMs(), value.getExpiresAtMs());
          assertEquals("Size does not match", referenceValue.getSize(), value.getSize());
          assertEquals("Account ID does not match", referenceValue.getAccountId(), value.getAccountId());
          assertEquals("Container ID does not match", referenceValue.getContainerId(), value.getContainerId());
          assertEquals(
              "Original message offset does not match " + id.toString() + " " + referenceValue + " " + value.toString(),
              referenceValue.getOriginalMessageOffset(), value.getOriginalMessageOffset());
          assertEquals("Flags do not match " + id.toString() + " " + referenceValue.toString() + " " + value.toString(),
              referenceValue.getFlags(), value.getFlags());
          if (index.hardDeleter.enabled.get() && !deletedKeys.contains(referenceIndexSegmentEntry.getKey())) {
            assertEquals("Operation time does not match", referenceValue.getOperationTimeInMs(),
                value.getOperationTimeInMs());
            assertEquals("Value from IndexSegment does not match expected", referenceValue.getBytes(),
                value.getBytes());
          }
          value = values.higher(value);
        }
        assertNull("There are more values in the real index", value);
      }
      realIndexEntry = realIndex.higherEntry(realIndexEntry.getKey());
    }
    assertNull("There should no more index segments left", realIndexEntry);
    // all the elements in the last segment should be in the journal
    assertNotNull("There is no offset in the log that corresponds to the last index segment start offset",
        logOrder.get(referenceIndex.lastKey()));
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
   * Marks {@code id} as deleted.
   * @param id the {@link MockId} to mark as deleted.
   */
  private void markAsDeleted(MockId id) {
    deletedKeys.add(id);
    undeletedKeys.remove(id);
    liveKeys.remove(id);
  }

  /**
   * Marks {@code id} as undeleted.
   * @param id the {@link MockId} to mark as undeleted.
   */
  private void markAsUndeleted(MockId id) {
    undeletedKeys.add(id);
    liveKeys.add(id);
    deletedKeys.remove(id);
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

    Map.Entry<Offset, TreeMap<MockId, TreeSet<IndexValue>>> lastEntry = referenceIndex.lastEntry();
    Offset indexSegmentStartOffset = lastEntry.getKey();
    if (!indexSegmentStartOffset.getName().equals(recordOffset.getName())
        || lastEntry.getValue().size() == getMaxInMemElements()) {
      indexSegmentStartOffset = recordOffset;
    } else if (lastEntry.getValue().entrySet().iterator().next().getValue().iterator().next().getFormatVersion()
        != PersistentIndex.CURRENT_VERSION) {
      // the persistent index version changed.
      indexSegmentStartOffset = recordOffset;
    } else {
      final AtomicInteger numElements = new AtomicInteger(0);
      lastEntry.getValue().values().forEach(values -> numElements.addAndGet(values.size()));
      if (numElements.get() == getMaxInMemElements()) {
        indexSegmentStartOffset = recordOffset;
      }
    }
    return indexSegmentStartOffset;
  }

  /**
   * Gets all the valid index entries (taking into account different reference times) in the index segment with start
   * offset {@code indexSegmentStartOffset}.
   * @param indexSegmentStartOffset the start offset of the {@link IndexSegment} from which valid index entries are
   *                                required.
   * @param deleteReferenceTimeMs the reference time in ms until which deletes are relevant.
   * @param expiryReferenceTimeMs the reference time in ms until which expirations are relevant
   * @param fileSpanUnderCompaction only useful for compaction tests to help determine whether update records will
   *                                cleaned up. All other use cases should use {@code null}.
   * @return all the valid index entries valid in the index segment with start offset {@code indexSegmentStartOffset}.
   */
  List<IndexEntry> getValidIndexEntriesForIndexSegment(Offset indexSegmentStartOffset, long deleteReferenceTimeMs,
      long expiryReferenceTimeMs, FileSpan fileSpanUnderCompaction) {
    List<IndexEntry> validEntries = new ArrayList<>();
    if (referenceIndex.containsKey(indexSegmentStartOffset)) {
      for (Map.Entry<MockId, TreeSet<IndexValue>> indexSegmentEntry : referenceIndex.get(indexSegmentStartOffset)
          .entrySet()) {
        MockId key = indexSegmentEntry.getKey();
        NavigableSet<IndexValue> values = indexSegmentEntry.getValue();
        IndexValue latestValue = getExpectedValue(key, false);
        Iterator<IndexValue> iter = values.iterator();
        while (iter.hasNext()) {
          IndexValue currentValue = iter.next();
          IndexEntry currentEntry = new IndexEntry(key, currentValue);
          if (currentValue.isUndelete()) {
            if (latestValue.isUndelete() && latestValue.getLifeVersion() == currentValue.getLifeVersion()) {
              validEntries.add(currentEntry);
            }
          } else if (currentValue.isDelete()) {
            if (latestValue.isDelete() && latestValue.getLifeVersion() == currentValue.getLifeVersion()) {
              validEntries.add(currentEntry);
            }
          } else if (currentValue.isTtlUpdate()) {
            IndexValue putValue = getExpectedValue(key, EnumSet.of(PersistentIndex.IndexEntryType.PUT), null);
            if (putValue == null) {
              continue;
            }
            if (latestValue.isDelete()) {
              if (latestValue.getOperationTimeInMs() >= deleteReferenceTimeMs
                  || isTtlUpdateEntryValidWhenFinalStateIsDeleteAndRetention(key, currentValue,
                  fileSpanUnderCompaction)) {
                validEntries.add(currentEntry);
              }
            } else {
              validEntries.add(currentEntry);
            }
          } else {
            if (!isExpiredAt(key, expiryReferenceTimeMs)) {
              if (latestValue.isPut()) {
                validEntries.add(new IndexEntry(key, currentValue));
              } else if (latestValue.isDelete()) {
                if (latestValue.getOperationTimeInMs() >= deleteReferenceTimeMs) {
                  validEntries.add(currentEntry);
                }
              } else {
                validEntries.add(currentEntry);
              }
            }
          }
        }
      }
    }
    return validEntries;
  }

  /**
   * Determines whether a TTL update entry is valid
   * @param key the {@link MockId} being examined
   * @param ttlUpdateValue the {@link IndexValue} associated with the TTL update entry of {@code key}.
   * @param fileSpanUnderCompaction only useful for compaction tests to help determine whether update records will be
   *                                cleaned up. All other use cases should use {@code null}.
   * @return {@code true} if the TTL update entry is valid
   */
  private boolean isTtlUpdateEntryValidWhenFinalStateIsDeleteAndRetention(MockId key, IndexValue ttlUpdateValue,
      FileSpan fileSpanUnderCompaction) {
    // valid if the original put is valid
    IndexValue putValue = getExpectedValue(key, EnumSet.of(PersistentIndex.IndexEntryType.PUT), null);
    if (putValue != null) {
      if (fileSpanUnderCompaction != null) {
        return !fileSpanUnderCompaction.inSpan(putValue.getOffset());
      } else {
        return !putValue.getOffset().getName().equals(ttlUpdateValue.getOffset().getName());
      }
    }
    return false;
  }

  /**
   * @return the configured max elements per index segment
   */
  int getMaxInMemElements() {
    return new StoreConfig(new VerifiableProperties(properties)).storeIndexMaxNumberOfInmemElements;
  }

  /**
   * @param values the values whose size needs to be summed
   * @return the sum of the size of all {@code values}
   */
  static long getSizeOfAllValues(NavigableSet<IndexValue> values) {
    final AtomicLong size = new AtomicLong(0);
    values.forEach(value -> size.addAndGet(value.getSize()));
    return size.get();
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
