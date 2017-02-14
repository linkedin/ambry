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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
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
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

  // setupTestState() is coupled to these numbers. Changing them *will* cause setting test state or tests to fail.
  private static final long LOG_CAPACITY = 10000;
  private static final long SEGMENT_CAPACITY = 2000;
  private static final int MAX_IN_MEM_ELEMENTS = 5;
  // deliberately do not divide the capacities perfectly.
  private static final long PUT_RECORD_SIZE = 53;
  private static final long DELETE_RECORD_SIZE = 29;

  private static final int BUCKET_COUNT = 100;
  private static final long BUCKET_TIME_SPAN = 1000;
  private static final long SEGMENT_SCAN_OFFSET = 100000;

  // used by getUniqueId() to make sure keys are never regenerated in a single test run.
  private final Set<MockId> generatedKeys = new HashSet<>();
  private final TreeMap<StoreKey, IDInfo> allKeys = new TreeMap<>();
  private final TreeMap<StoreKey, IDInfo> expiredIDMap = new TreeMap<>();
  private final TreeMap<Long, HashMap<String, Long>> validDataSizePerSegment = new TreeMap<>();
  private final HashMap<String, Long> validDataSizePerContainer = new HashMap<>();

  // Indicates whether the log is segmented
  private final boolean isLogSegmented;
  // Variables that represent the folder where the data resides
  private final File tempDir;
  private final String tempDirStr;
  // the time instance that will be used in the index
  private final Time time = new MockTime();
  private final Object notifyObject = new Object();

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
  // The StatsEngine that builds the data structure used by BlobStoreStats
  private StatsEngine statsEngine;
  // The BlobStoreStats that exposes statistics about the blobstore
  private BlobStoreStats blobStoreStats;
  // the incarnationId associated with the store
  private UUID incarnationId = UUID.randomUUID();

  class IDInfo {
    StoreKey id;
    long size;
    long expirationDate;
    Offset indexSegmentStartOffset;
    String containerKey;

    IDInfo(StoreKey id, long size, long expirationDate, Offset indexSegmentStartOffset, String containerKey) {
      this.id = id;
      this.size = size;
      this.expirationDate = expirationDate;
      this.indexSegmentStartOffset = indexSegmentStartOffset;
      this.containerKey = containerKey;
    }

    public String toString() {
      return id + ", size:" + size + ", indexStartOffset:" + indexSegmentStartOffset;
    }
  }

  /**
   * A mock implementation of {@link MessageStoreHardDelete} that can be set to return {@link MessageInfo} for a
   * particular {@link MockId}.
   */
  static class MockMessageStoreHardDelete implements MessageStoreHardDelete {
    MessageInfo messageInfo = null;

    @Override
    public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory,
        List<byte[]> recoveryInfoList) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory factory) throws IOException {
      return messageInfo;
    }

    void setMessageInfo(MessageInfo messageInfo) {
      this.messageInfo = messageInfo;
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
  public BlobStoreStatsTest(boolean isLogSegmented) throws InterruptedException, IOException, StoreException {
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

  @Test
  public void testTotalCapacity() throws IOException, StoreException, InterruptedException {
    // verify total capacity
    verifyTotalCapacity(blobStoreStats.getTotalCapacity());
    // total capacity should be unaffected by any new writes to the store
    addPutEntries(10, PUT_RECORD_SIZE, Utils.Infinite_Time);
    verifyTotalCapacity(blobStoreStats.getTotalCapacity());
  }

  @Test
  public void testUsedCapacity() throws IOException, StoreException, InterruptedException {
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment());

    // 1 DELETE for a PUT in the 1th index segment
    Offset firstIndexSegmentStartOffset = index.indexes.firstKey();
    addDeleteEntry(getStoreKeyToDeleteFromIndexSegment(firstIndexSegmentStartOffset, time.milliseconds()));
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment());

    // 2 DELETEs for a PUT in last index segment
    addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);
    verifyUsedCapacity(blobStoreStats.getUsedCapacity());
    verifyUsedCapacityBySegment(blobStoreStats.getUsedCapacityBySegment());

    addDeleteEntry(getStoreKeyToDeleteFromIndexSegment(index.indexes.lastKey(), time.milliseconds()));
    addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);
    addDeleteEntry(getStoreKeyToDeleteFromIndexSegment(index.indexes.lastKey(), time.milliseconds()));
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
    statsEngine.close();
    Iterator<Map.Entry<Offset, IndexSegment>> indexesIterator = index.indexes.entrySet().iterator();
    while (indexesIterator.hasNext()) {
      Map.Entry<Offset, IndexSegment> indexEntry = indexesIterator.next();
      TimeRange timeRange = new TimeRange(indexEntry.getValue().getLastModifiedTime() + 1, 0);
      verifyValidDataSizeOfStore(timeRange);
      verifyValidDataSizeBySegment(timeRange);
    }

    verifyValidDataSizeByContainer();

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
   * Gets an StoreKey to delete from the index segment with start offset {@code indexSegmentStartOffset}.
   * @param indexSegmentStartOffset the start offset of the index segment from which an ID is required.
   * @param referenceTimeInMs the reference time in Ms to use to determine the validity of the data
   * @return a StoreKey to delete from the index segment with start offset {@code indexSegmentStartOffset}.
   */
  private StoreKey getStoreKeyToDeleteFromIndexSegment(Offset indexSegmentStartOffset, long referenceTimeInMs)
      throws IOException, StoreException {
    StoreKey deleteCandidate = null;
    IndexSegment indexSegment = index.indexes.get(indexSegmentStartOffset);
    List<MessageInfo> messageInfos = new ArrayList<>();
    indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
    for (MessageInfo messageInfo : messageInfos) {
      if (!messageInfo.isDeleted() && messageInfo.getExpirationTimeInMs() == Utils.Infinite_Time &&
          !(index.findKey(messageInfo.getStoreKey(), null).isFlagSet(IndexValue.Flags.Delete_Index))) {
        deleteCandidate = messageInfo.getStoreKey();
        break;
      }
    }
    return deleteCandidate;
  }

  /**
   * Gets a StoreKey to delete from the given log segment.
   * @param segment the {@link LogSegment} from which a non-deleted StoreKey is required.
   * @return the StoreKey to delete.
   */
  private StoreKey getStoreKeyToDeleteFromLogSegment(LogSegment segment) throws IOException, StoreException {
    StoreKey deleteCandidate;
    Offset indexSegmentStartOffset = new Offset(segment.getName(), segment.getStartOffset());
    do {
      deleteCandidate = getStoreKeyToDeleteFromIndexSegment(indexSegmentStartOffset, time.milliseconds());
      indexSegmentStartOffset = index.indexes.higherKey(indexSegmentStartOffset);
      if (indexSegmentStartOffset == null || !indexSegmentStartOffset.getName().equals(segment.getName())) {
        break;
      }
    } while (deleteCandidate == null);
    return deleteCandidate;
  }

  /**
   * Adds {@code count} number of put entries each of size {@code size} and that expire at {@code expiresAtMs} to the
   * {@link Log} and the {@link PersistentIndex}
   * @param count the number of PUT entries to add.
   * @param size the size of each PUT entry.
   * @param expiresAtMs the time at which each of the PUT entries expires.
   * @return a {@link Pair<FileSpan, List<MockId>>} in which the first entry refers to the {@link FileSpan} spanning all the
   * newly added entries and the second entry refers to the newly added entries themselves
   * @throws IOException
   * @throws StoreException
   */
  private Pair<FileSpan, ArrayList<MockId>> addPutEntries(int count, long size, long expiresAtMs)
      throws IOException, StoreException, InterruptedException {
    if (count <= 0) {
      throw new IllegalArgumentException("Number of put entries to add cannot be <= 0");
    }
    ArrayList<IndexEntry> indexEntries = new ArrayList<>(count);
    ArrayList<IndexEntry> newEntries = new ArrayList<>();
    ArrayList<MockId> newMockIds = new ArrayList<>();
    Offset expectedJournalLastOffset = null;
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    for (int i = 0; i < count; i++) {
      appendToLog(size);
      FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, size);
      IndexValue value = new IndexValue(size, fileSpan.getStartOffset(), expiresAtMs);
      MockId id = getUniqueId();
      newMockIds.add(id);
      indexEntries.clear();
      indexEntries.add(new IndexEntry(id, value));
      newEntries.add(new IndexEntry(id, value));
      // save end offset of prev msg
      endOffsetOfPrevMsg = index.getCurrentEndOffset();
      index.addToIndex(indexEntries, fileSpan);
      // find end offset of cur msg
      Offset endOffsetOfCurMsg = index.getCurrentEndOffset();
      Offset curIndexStartOffset = index.indexes.lowerKey(endOffsetOfCurMsg);
      Offset lastIndexStartOffset = index.indexes.lowerKey(endOffsetOfPrevMsg);
      if (lastIndexStartOffset != null && curIndexStartOffset.compareTo(lastIndexStartOffset) != 0) {
        // index segment rolled over
        Map.Entry<Offset, IndexSegment> prevtoLastSegmentEntry = index.indexes.lowerEntry(lastIndexStartOffset);
        populateValidDataSize(index.indexes.get(lastIndexStartOffset),
            prevtoLastSegmentEntry != null ? prevtoLastSegmentEntry.getValue() : null);
        assertEquals("Entries in valid data str mismatch ", index.indexes.size() - 1, validDataSizePerSegment.size());
        // sleep for 10 sec to ensure every index segment will have diff last modified time
        time.sleep(10000);
      } else {
        assertEquals("Entries in valid data str mismatch ", index.indexes.size() - 1, validDataSizePerSegment.size());
      }
      expectedJournalLastOffset = fileSpan.getStartOffset();
      endOffsetOfPrevMsg = fileSpan.getEndOffset();
    }
    FileSpan fileSpan = new FileSpan(newEntries.get(0).getValue().getOffset(), endOffsetOfPrevMsg);
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", expectedJournalLastOffset, index.journal.getLastOffset());
    return new Pair<>(fileSpan, newMockIds);
  }

  private void populateValidDataSize(IndexSegment indexSegment, IndexSegment prevIndexSegment) throws IOException {
    ConcurrentSkipListMap<Offset, Long> prevTimeRangeTreeMap = null;
    HashMap<String, Long> prevHashMap = null;
    long currentIndexLastModifiedTimeInSecs = indexSegment.getLastModifiedTime();
    String currentLogSegmentName = indexSegment.getLogSegmentName();

    if (!validDataSizePerSegment.isEmpty()) {
      prevHashMap = validDataSizePerSegment.lastEntry().getValue();
    }
    validDataSizePerSegment.put(currentIndexLastModifiedTimeInSecs, new HashMap<>());

    if (validDataSizePerSegment.size() > 1) {
      validDataSizePerSegment.get(currentIndexLastModifiedTimeInSecs).putAll(prevHashMap);
    }

    if (!validDataSizePerSegment.get(currentIndexLastModifiedTimeInSecs).containsKey(currentLogSegmentName)) {
      validDataSizePerSegment.get(currentIndexLastModifiedTimeInSecs).put(currentLogSegmentName, 0L);
    }

    long validDataSize = 0;
    ArrayList<MessageInfo> immediatelyExpiredPuts = new ArrayList<>();
    ArrayList<MessageInfo> messageInfos = new ArrayList<>();
    indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
    for (MessageInfo messageInfo : messageInfos) {
      String containerKey = messageInfo.getServiceId().concat("-").concat(messageInfo.getContainerId());
      if (!messageInfo.isDeleted()) {
        IDInfo idInfo = new IDInfo(messageInfo.getStoreKey(), messageInfo.getSize(),
            messageInfo.getExpirationTimeInMs(), indexSegment.getStartOffset(), containerKey);
        allKeys.put(messageInfo.getStoreKey(), idInfo);
        // put record
        if (!isExpired(messageInfo.getExpirationTimeInMs(), currentIndexLastModifiedTimeInSecs * Time.MsPerSec)) {
          validDataSize += messageInfo.getSize();
          if (messageInfo.getExpirationTimeInMs() != Utils.Infinite_Time) {
            // expiry set for a put record
            expiredIDMap.put(messageInfo.getStoreKey(), idInfo);
          }
          long containerValidDataSize = validDataSizePerContainer.containsKey(containerKey) ?
              validDataSizePerContainer.get(containerKey) : 0L;
          validDataSizePerContainer.put(containerKey, containerValidDataSize + messageInfo.getSize());
        } else {
          immediatelyExpiredPuts.add(messageInfo);
        }
      } else {
        // delete record
        validDataSize += messageInfo.getSize();
        // invalidate the put record size in the corresponding index segment
        IDInfo putRecordInfo = allKeys.get(messageInfo.getStoreKey());
        if (putRecordInfo != null) {
          String putRecordSegmentName = putRecordInfo.indexSegmentStartOffset.getName();
          long putRecordSegmentValidDataSize =
              validDataSizePerSegment.get(currentIndexLastModifiedTimeInSecs).get(putRecordSegmentName);
          validDataSizePerSegment.get(currentIndexLastModifiedTimeInSecs).put(putRecordSegmentName,
              putRecordSegmentValidDataSize - putRecordInfo.size);
          long putRecordContainerValidDataSize = validDataSizePerContainer.get(putRecordInfo.containerKey);
          validDataSizePerContainer.put(putRecordInfo.containerKey, putRecordContainerValidDataSize - putRecordInfo.size);
          if (expiredIDMap.containsKey(putRecordInfo.id)) {
            expiredIDMap.remove(putRecordInfo.id);
          }
        }
      }
    }

    for (Map.Entry<Long, HashMap<String, Long>> validDataSizePerSegmentEntry : validDataSizePerSegment.entrySet()) {
      long pastValidDataSize = validDataSizePerSegmentEntry.getValue().get(currentLogSegmentName) == null ?
          0L : validDataSizePerSegmentEntry.getValue().get(currentLogSegmentName);
      // Aggregate immediately expired puts within current index segment to previous time points
      for (MessageInfo messageInfo : immediatelyExpiredPuts) {
        if (!isExpired(messageInfo.getExpirationTimeInMs(), validDataSizePerSegmentEntry.getKey() * Time.MsPerSec)) {
          pastValidDataSize += messageInfo.getSize();
        }
      }
      validDataSizePerSegmentEntry.getValue().put(currentLogSegmentName, pastValidDataSize + validDataSize);
    }
    // for all the entries that is expiring between last segment's modified time and current segment last modified time,
    // update valid data size
    ArrayList<StoreKey> expiredIds = new ArrayList<>();
    for (Map.Entry<StoreKey, IDInfo> expiredIdInfo : expiredIDMap.entrySet()) {
      if (expiredIdInfo.getValue().expirationDate <= currentIndexLastModifiedTimeInSecs * Time.MsPerSec) {
        expiredIds.add(expiredIdInfo.getKey());
        String segmentName = allKeys.get(expiredIdInfo.getKey()).indexSegmentStartOffset.getName();
        long oldSegmentValidDataSize = validDataSizePerSegment.get(currentIndexLastModifiedTimeInSecs).get(segmentName);
        validDataSizePerSegment.get(currentIndexLastModifiedTimeInSecs).put(segmentName,
            oldSegmentValidDataSize - expiredIdInfo.getValue().size);
        long oldContainerValidDataSize = validDataSizePerContainer.get(expiredIdInfo.getValue().containerKey);
        validDataSizePerContainer.put(expiredIdInfo.getValue().containerKey,
            oldContainerValidDataSize - expiredIdInfo.getValue().size);
      }
    }

    for (StoreKey expiredID : expiredIds) {
      expiredIDMap.remove(expiredID);
    }
  }

  /**
   * Adds a delete entry in the index (real and reference) for {@code idToDelete}.
   * @param idToDelete the id to be deleted.
   * @return the {@link FileSpan} of the added entries.
   * @throws IOException
   * @throws StoreException
   */
  private FileSpan addDeleteEntry(StoreKey idToDelete) throws IOException, StoreException, InterruptedException {
    appendToLog(DELETE_RECORD_SIZE);
    Offset endOffsetOfPrevMsg = index.getCurrentEndOffset();
    FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfPrevMsg, DELETE_RECORD_SIZE);
    index.markAsDeleted(idToDelete, fileSpan);
    Offset endOffsetOfCurMsg = index.getCurrentEndOffset();
    Offset curIndexStartOffset = index.indexes.lowerKey(endOffsetOfCurMsg);
    Offset prevIndexStartOffset = index.indexes.lowerKey(endOffsetOfPrevMsg);
    if (prevIndexStartOffset != null && curIndexStartOffset.compareTo(prevIndexStartOffset) != 0) {
      // index segment rolled over
      Map.Entry<Offset, IndexSegment> prevtoPrevSegmentEntry = index.indexes.lowerEntry(prevIndexStartOffset);
      populateValidDataSize(index.indexes.get(prevIndexStartOffset),
          prevtoPrevSegmentEntry != null ? prevtoPrevSegmentEntry.getValue() : null);
      assertEquals("entries in valid data str mismatch ", index.indexes.size() - 1, validDataSizePerSegment.size());
      // sleep for 10 sec to ensure every index segment will have diff last modified time
      time.sleep(10000);
    }
    endOffsetOfPrevMsg = fileSpan.getEndOffset();
    assertEquals("End Offset of index not as expected", endOffsetOfPrevMsg, index.getCurrentEndOffset());
    assertEquals("Journal's last offset not as expected", fileSpan.getStartOffset(), index.journal.getLastOffset());
    return fileSpan;
  }

  // verifiers

  // blob Store stats test helpers

  /**
   * Verifies used capacity of the store
   * @param actualUsedCapacity actual used capacity of the store
   */
  private void verifyUsedCapacity(long actualUsedCapacity) {
    assertEquals("Used capacity mismatch ", log.getUsedCapacity(), actualUsedCapacity);
  }

  /**
   * Verifies Used capacity of the store by segments
   * @param actualUsedCapacity actual used capacity by segment of the store
   */
  private void verifyUsedCapacityBySegment(SortedMap<String, Long> actualUsedCapacity) {
    Iterator<Map.Entry<String, Long>> usedCapacityIterator = actualUsedCapacity.entrySet().iterator();
    LogSegment expectedLogSegment = log.getFirstSegment();
    while (usedCapacityIterator.hasNext()) {
      Map.Entry<String, Long> segmentNameCapacityPair = usedCapacityIterator.next();
      assertEquals("Log segment name mismatch while fetching Used capacity", expectedLogSegment.getName(),
          segmentNameCapacityPair.getKey());
      assertEquals("Used capacity mismatch for  " + expectedLogSegment.getName(), expectedLogSegment.getEndOffset(),
          segmentNameCapacityPair.getValue().longValue());
      expectedLogSegment = log.getNextSegment(expectedLogSegment);
    }
    assertNull("No of segments mismatch ", expectedLogSegment);
  }

  /**
   * Verifies the valid data size of store by segments
   * @param timeRange the {@link TimeRange} that needs to be used while fetching valid data size
   * @throws IOException
   * @throws StoreException
   */
  private void verifyValidDataSizeOfStore(TimeRange timeRange) throws IOException, StoreException {

    if (validDataSizePerSegment.size() < index.indexes.size()) {
      populateValidDataSize(index.indexes.lastEntry().getValue(),
          index.indexes.lowerEntry(index.indexes.lastKey()).getValue());
    }

    HashMap<String, Long> expectedSegmentValidDataSizeMap = validDataSizePerSegment.get(timeRange.getStart());
    if (expectedSegmentValidDataSizeMap == null) {
      expectedSegmentValidDataSizeMap = validDataSizePerSegment.lowerEntry(timeRange.getStart()).getValue();
    }

    long expectedSegmentValidDataSize = 0;

    for (Map.Entry<String, Long> validDataSizePerSegment : expectedSegmentValidDataSizeMap.entrySet()) {
      expectedSegmentValidDataSize += validDataSizePerSegment.getValue();
    }

    Pair<Long, Long> actualValidDataSizeOfStore = blobStoreStats.getValidDataSize(timeRange);
    assertTrue("Valid data size collection time should be greater than start time in the range",
        timeRange.getStart() <= actualValidDataSizeOfStore.getFirst());
    assertTrue("Valid data size collection time should be lesser than end time in the range",
        actualValidDataSizeOfStore.getFirst() <= timeRange.getEnd());
    //assertEquals("Valid data size of the store mismatch ", expectedValidDataSize,
    //    actualValidDataSizeOfStore.getSecond().longValue());
    assertEquals("Valid data size of the store mismatch", expectedSegmentValidDataSize,
        actualValidDataSizeOfStore.getSecond().longValue());
  }

  /**
   * Verifies the valid data size of store by segments
   * @param timeRange the {@link TimeRange} that needs to be used while fetching valid data size
   * @throws IOException
   * @throws StoreException
   */
  private void verifyValidDataSizeBySegment(TimeRange timeRange) throws IOException, StoreException {

    if (validDataSizePerSegment.size() < index.indexes.size()) {
      populateValidDataSize(index.indexes.lastEntry().getValue(),
          index.indexes.lowerEntry(index.indexes.lastKey()).getValue());
    }

    HashMap<String, Long> expectedSegmentValidDataSizeMap = validDataSizePerSegment.get(timeRange.getStart());
    if (expectedSegmentValidDataSizeMap == null) {
      expectedSegmentValidDataSizeMap = validDataSizePerSegment.lowerEntry(timeRange.getStart()).getValue();
    }

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

  private void verifyValidDataSizeByContainer() {
    ArrayList<String> containerList = new ArrayList<>();
    HashMap<String, ArrayList<String>> serviceIdContainerIdMap = new HashMap<>();
    for (String containerKey : validDataSizePerContainer.keySet()) {
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
      for (Map.Entry<String, Long> containerValidDataSize : actualValidDataSizePerContainer.entrySet()) {
        String containerKey = idMapEntry.getKey().concat("-").concat(containerValidDataSize.getKey());
        assertTrue("Container not found in validDataSizePerContainer" + containerKey,
            validDataSizePerContainer.containsKey(containerKey));
        Long expectedContainerValue =
            validDataSizePerContainer.get(containerKey);
        for (Map.Entry<StoreKey, IDInfo> expiredIdInfo : expiredIDMap.entrySet()) {
          if (expiredIdInfo.getValue().containerKey.equals(containerKey) &&
              expiredIdInfo.getValue().expirationDate <= time.milliseconds()) {
            expectedContainerValue -= expiredIdInfo.getValue().size;
          }
        }
        assertEquals("Container's valid data size mismatch", expectedContainerValue, containerValidDataSize.getValue());
      }
      assertEquals("Mismatch in number of containers returned by getValidDataSizeByContainer",
          idMapEntry.getValue().size(), actualValidDataSizePerContainer.size());
    }
    SortedMap<String, Long> actualValidDataSizePerContainer = blobStoreStats.getValidDataSizeByContainer(
        "temp", containerList);
    for (Map.Entry<String, Long> validContainerSize : actualValidDataSizePerContainer.entrySet()) {
      Long expectedValue = validDataSizePerContainer.get("temp-01");
      for (Map.Entry<StoreKey, IDInfo> expiredIdInfo : expiredIDMap.entrySet()) {
        if (expiredIdInfo.getValue().containerKey.equals("temp-01") &&
            expiredIdInfo.getValue().expirationDate <= time.milliseconds()) {
          expectedValue -= expiredIdInfo.getValue().size;
        }
      }
      assertEquals("Container's valid data size mismatch", expectedValue, validContainerSize.getValue());
    }
  }

  /**
   * Derives valid data size for the store
   * @param referenceTimeInMs the reference time in Ms that is of interest at which valid data size is required
   * @param validDataSizeBySegments the {@link SortedMap} that needs to be updated with valid data size value
   *                                for every log segment. Could be {@code null} if interested only in total valid data size
   * @return a {@link Pair} of reference time and total valid data size of the store
   * @throws IOException
   * @throws StoreException
   */
  private Pair<Long, Long> getValidDataSizeOfAllSegments(long referenceTimeInMs,
      SortedMap<String, Long> validDataSizeBySegments) throws IOException, StoreException {
    long validDataSize = 0;
    long totalValidDataSize = 0;
    Offset lastEligibleStartOffset = getLastEligibleStartOffsetForDelete(referenceTimeInMs);
    IndexSegment indexSegment = null;
    for (Map.Entry<Offset, IndexSegment> indexSegmentEntry : index.indexes.entrySet()) {
      if (indexSegmentEntry.getValue().getLastModifiedTime() < referenceTimeInMs) {
        // index segment is considered valid wrt referenceTime
        IndexSegment nextIndexSegment = indexSegmentEntry.getValue();
        if (indexSegment == null || nextIndexSegment.getLogSegmentName().equals(indexSegment.getLogSegmentName())) {
          // index segment is null or refers to same log segment as previous index segment
          validDataSize += getValidDataSizePerIndexSegment(nextIndexSegment,
              new FileSpan(nextIndexSegment.getStartOffset(), lastEligibleStartOffset), referenceTimeInMs);
        } else {
          // index segment refers to a new log segment compared to previous index segment
          String logSegmentName = indexSegment.getLogSegmentName();
          totalValidDataSize += validDataSize;
          if (validDataSizeBySegments != null) {
            validDataSizeBySegments.put(logSegmentName, validDataSize);
          }
          validDataSize = getValidDataSizePerIndexSegment(nextIndexSegment,
              new FileSpan(nextIndexSegment.getStartOffset(), lastEligibleStartOffset), referenceTimeInMs);
        }
        indexSegment = nextIndexSegment;
      }
    }
    if (indexSegment != null) {
      String logSegmentName = indexSegment.getLogSegmentName();
      totalValidDataSize += validDataSize;
      if (validDataSizeBySegments != null) {
        validDataSizeBySegments.put(logSegmentName, validDataSize);
      }
    }
    return new Pair<>(referenceTimeInMs, totalValidDataSize);
  }

  /**
   * Finds the last eligible index start offset whose last modified time surpases {@code referenceTimeInMs}
   * @param referenceTimeInMs the reference time against which the last eligible index start offset needs to be found
   * @return the last eligible index start offset whose last modified time surpases {@code referenceTimeInMs}
   */
  private Offset getLastEligibleStartOffsetForDelete(long referenceTimeInMs) {
    Offset latestEligibleOffset = index.indexes.lastKey();
    for (IndexSegment indexSegment : index.indexes.values()) {
      if (indexSegment.getLastModifiedTime() > referenceTimeInMs) {
        latestEligibleOffset = indexSegment.getStartOffset();
        break;
      }
    }
    return latestEligibleOffset;
  }

  /**
   * Get valid data size of an index segment
   * @param indexSegment the {@link IndexSegment} for which valid data size has to be determined
   * @param forwardSearchSpan the {@link FileSpan} against which search has to be done to determine validity of data
   * @param referenceTime time in ms used as reference to check for expiration
   * @return the valid data size of the given index segment
   * @throws IOException
   * @throws StoreException
   */
  private long getValidDataSizePerIndexSegment(IndexSegment indexSegment, FileSpan forwardSearchSpan,
      long referenceTime) throws IOException, StoreException {
    long validSize = 0;
    List<MessageInfo> messageInfos = new ArrayList<>();
    indexSegment.getEntriesSince(null, new FindEntriesCondition(Integer.MAX_VALUE), messageInfos, new AtomicLong(0));
    for (MessageInfo messageInfo : messageInfos) {
      IndexValue value = index.findKey(messageInfo.getStoreKey(), forwardSearchSpan);
      if (messageInfo.getExpirationTimeInMs() == Utils.Infinite_Time && !value.isFlagSet(
          IndexValue.Flags.Delete_Index)) {
        // put record w/o any expiration and not deleted
        validSize += messageInfo.getSize();
      } else if (!messageInfo.isDeleted() && (isExpired(messageInfo.getExpirationTimeInMs(), referenceTime)
          || value.isFlagSet(IndexValue.Flags.Delete_Index))) {
        // a put record either expired or deleted(in future index segment) within referenceTimeInMs
        validSize += messageInfo.getSize();
      } else if (messageInfo.isDeleted()) {
        // delete record
        validSize += messageInfo.getSize();
      }
    }
    return validSize;
  }

  /**
   * Check if {@code expirationTimeInMs} has expired compared to {@code referenceTimeInMs}
   * @param expirationTimeInMs time in ms to be checked for expiration
   * @param referenceTimeInMs the epoch time to use to check for expiration
   * @return {@code true} if {@code expirationTimeInMs} expired wrt {@code referenceTimeInMs}, {@code false} otherwise
   */
  private static boolean isExpired(long expirationTimeInMs, long referenceTimeInMs) {
    return expirationTimeInMs != Utils.Infinite_Time && referenceTimeInMs > expirationTimeInMs;
  }

  /**
   * Verifies total capacity of the {@link BlobStore}
   * @param totalCapacity the total capacity of the {@link BlobStore}
   */
  private void verifyTotalCapacity(long totalCapacity) {
    assertEquals("Total capacity mismatch ", LOG_CAPACITY, totalCapacity);
  }

  /**
   * Closes the index and clears all the index files essentially creating a new index.
   * @throws StoreException
   */
  /*private void closeAndClearIndex() throws StoreException {
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
  }*/

  // test setup

  /**
   * Sets up some state in order to make sure all cases are represented and the tests don't need to do any setup
   * individually. For understanding the created index, please read the source code which is annotated with comments.
   * <p/>
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
        new PersistentIndex(tempDirStr, scheduler, log, config, STORE_KEY_FACTORY, recovery, hardDelete, metrics, time,
            incarnationId);
    statsEngine = new StatsEngine(log, index, LOG_CAPACITY, BUCKET_COUNT,
        BUCKET_TIME_SPAN, SEGMENT_SCAN_OFFSET, time, notifyObject);
    blobStoreStats = statsEngine.getBlobStoreStats();
    assertEquals("End Offset of index not as expected", log.getStartOffset(), index.getCurrentEndOffset());

    // advance time by a millisecond in order to be able to add expired keys and to avoid keys that are expired from
    // being picked for delete.
    time.sleep(10000);
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
      StoreKey idToDelete = getStoreKeyToDeleteFromLogSegment(segment);
      addDeleteEntry(idToDelete);
      verifyLogSegmentCount(3);
      // DELETE for a key in the second segment
      segment = log.getNextSegment(segment);
      idToDelete = getStoreKeyToDeleteFromLogSegment(segment);
      addDeleteEntry(idToDelete);
      // 1 DELETE for the PUT in the same segment
      idToDelete = getStoreKeyToDeleteFromIndexSegment(index.indexes.lastKey(), time.milliseconds());
      addDeleteEntry(idToDelete);
      // 1 PUT entry that spans the rest of the data in the third segment and expire in the distant future
      long size = segmentCapacity / 3 - index.getCurrentEndOffset().getOffset();
      addPutEntries(1, size, time.milliseconds() + 60 * Time.MsPerSec);

      // fourth and fifth log segment are free.
    }
    // populate valid data size for last segment
    // make sure all indexes are written to disk and mapped as required (forcing IndexPersistor to run).
    log.flush();
    //reloadIndex(false);
    //statsEngine.start();
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
      throws IOException, StoreException, InterruptedException {
    // First Index Segment
    // 1 PUT
    Offset firstJournalEntryAddedNow =
        addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time).getFirst().getStartOffset();
    verifyLogSegmentCount(expectedLogSegmentCount);
    // 1 PUT EXPIRED
    addPutEntries(1, PUT_RECORD_SIZE, time.milliseconds() - 100);
    // 3 more PUT
    addPutEntries(3, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // 5 entries were added - firstJournalEntryAddedNow should still be a part of the journal
    List<JournalEntry> entries = index.journal.getEntriesSince(firstJournalEntryAddedNow, true);
    assertEquals("There should have been exactly 5 entries returned from the journal", 5, entries.size());

    // Second Index Segment
    // 1 PUT that will expire in the future;
    addPutEntries(1, PUT_RECORD_SIZE, time.milliseconds() + 60 * Time.MsPerSec);
    // 2 PUT
    addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);
    // 1 PUT that will expire in the next index segment
    addPutEntries(1, PUT_RECORD_SIZE, time.milliseconds() + 2 * Time.MsPerSec);
    // 1 DELETE for a PUT in the same index segment
    StoreKey idToDelete = getStoreKeyToDeleteFromIndexSegment(index.indexes.lastKey(), time.milliseconds());
    addDeleteEntry(idToDelete);
    // 5 more entries (for a total of 10) were added - firstJournalEntryAddedNow should still be a part of the journal
    entries = index.journal.getEntriesSince(firstJournalEntryAddedNow, true);
    assertEquals("There should have been exactly 10 entries returned from the journal", 10, entries.size());
    // 1 DELETE for a PUT in the first index segment
    Offset firstIndexSegmentStartOffset = index.indexes.firstKey();
    idToDelete = getStoreKeyToDeleteFromIndexSegment(firstIndexSegmentStartOffset, time.milliseconds());
    addDeleteEntry(idToDelete);

    // 1 more entry (for a total of 11) was added - firstJournalEntryAddedNow should no longer be a part of the journal
    assertNull("There should no entries returned from the journal",
        index.journal.getEntriesSince(firstJournalEntryAddedNow, true));

    // Third and Fourth Index Segment
    for (int seg = 0; seg < 2; seg++) {
      // 2 PUT
      addPutEntries(2, PUT_RECORD_SIZE, Utils.Infinite_Time);
      // 1 PUT that will expire in the next index segment
      addPutEntries(1, PUT_RECORD_SIZE, time.milliseconds() + 2 * Time.MsPerSec);
      // 1 PUT for an expired blob
      List<MockId> newEntries = addPutEntries(1, PUT_RECORD_SIZE, time.milliseconds() - 100).getSecond();
      // 1 DELETE for the expired PUT
      MockId expiredId = newEntries.get(0);
      addDeleteEntry(expiredId);
      // 1 PUT that will expire in the distant future;
      addPutEntries(1, PUT_RECORD_SIZE, 60 * time.milliseconds());
    }

    Offset fourthIndexSegmentStartOffset = index.indexes.lastKey();
    Offset thirdIndexSegmentStartOffset = index.indexes.lowerKey(fourthIndexSegmentStartOffset);
    // Fifth Index Segment
    // 1 PUT entry
    addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);

    // 1 DELETE for a PUT in each of the third and fourth segments
    idToDelete = getStoreKeyToDeleteFromIndexSegment(thirdIndexSegmentStartOffset, time.milliseconds());
    addDeleteEntry(idToDelete);
    idToDelete = getStoreKeyToDeleteFromIndexSegment(fourthIndexSegmentStartOffset, time.milliseconds());
    addDeleteEntry(idToDelete);
    // 1 DELETE for the PUT in the same segment
    idToDelete = getStoreKeyToDeleteFromIndexSegment(index.indexes.lastKey(), time.milliseconds());
    addDeleteEntry(idToDelete);
    // 1 PUT entry that spans the rest of the data in the segment
    long size = sizeToMakeIndexEntriesFor - index.getCurrentEndOffset().getOffset();
    addPutEntries(1, size, Utils.Infinite_Time);
    verifyLogSegmentCount(expectedLogSegmentCount);
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
        new PersistentIndex(tempDirStr, scheduler, log, config, STORE_KEY_FACTORY, recovery, hardDelete, metrics, time,
            incarnationId);
    statsEngine = new StatsEngine(log, index, LOG_CAPACITY, BUCKET_COUNT,
        BUCKET_TIME_SPAN, SEGMENT_SCAN_OFFSET, time, notifyObject);
    blobStoreStats = statsEngine.getBlobStoreStats();
  }
}