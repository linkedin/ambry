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
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


/**
 * BlobStore test utils which assists in testing a blob store. It has all the supporting cast to populate
 * data for the {@link BlobStore} which can be used for verification purposes
 */
class BlobStoreTestUtils {
  static final StoreKeyFactory STORE_KEY_FACTORY;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  // setupTestState() is coupled to these numbers. Changing them *will* cause setting test state or tests to fail.
  static final long LOG_CAPACITY = 10000;
  static final long SEGMENT_CAPACITY = 2000;
  static final int MAX_IN_MEM_ELEMENTS = 5;
  // deliberately do not divide the capacities perfectly.
  static final long PUT_RECORD_SIZE = 53;
  static final long DELETE_RECORD_SIZE = 29;

  /**
   * A mock implementation of {@link MessageWriteSet} to help write to the {@link BlobStore}
   */
  static class MockMessageWriteSet implements MessageWriteSet {
    final List<ByteBuffer> buffers;
    final List<MessageInfo> infos;

    MockMessageWriteSet(List<MessageInfo> infos, List<ByteBuffer> buffers) {
      this.infos = infos;
      this.buffers = buffers;
    }

    @Override
    public long writeTo(Write writeChannel) throws IOException {
      long sizeWritten = 0;
      for (ByteBuffer buffer : buffers) {
        sizeWritten += buffer.remaining();
        writeChannel.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), buffer.remaining());
      }
      return sizeWritten;
    }

    @Override
    public List<MessageInfo> getMessageSetInfo() {
      return infos;
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

  // used by getUniqueId() to make sure keys are never regenerated in a single test run.
  final Set<MockId> generatedKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());
  // A map of all the keys. The key is the MockId and the value is a Pair that contains the metadata and data of the
  // message.
  final Map<MockId, MockIdInfo> allKeys = new ConcurrentHashMap<>();
  // A list of keys grouped by the log segment that they belong to
  final List<Set<MockId>> idsByLogSegment = new ArrayList<>();
  // Set of all deleted keys
  final Set<MockId> deletedKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());
  // Set of all expired keys
  final Set<MockId> expiredKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());
  // Set of all keys that are not deleted/expired
  final Set<MockId> liveKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());

  // Indicates whether the log is segmented
  final boolean isLogSegmented;
  // Variables that represent the folder where the data resides
  final File tempDir;
  final String tempDirStr;
  // the time instance that will be used in the index
  final Time time = new MockTime();

  final String storeId = UtilsTest.getRandomString(10);
  final DiskIOScheduler diskIOScheduler = new DiskIOScheduler(null);
  final ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
  final Properties properties = new Properties();

  // The BlobStore instance
  BlobStore store;
  // The MessageStoreRecovery that is used with the BlobStore
  MessageStoreRecovery recovery = new DummyMessageStoreRecovery();
  // The MessageStoreHardDelete that is used with the BlobStore
  MessageStoreHardDelete hardDelete = new MockMessageStoreHardDelete();
  // The MetricRegistry that is used with the index
  MetricRegistry metricRegistry;

  /**
   * Creates a temporary directory and sets up some test state.
   * @throws IOException
   */
  BlobStoreTestUtils(boolean isLogSegmented) throws InterruptedException, IOException, StoreException {
    this.isLogSegmented = isLogSegmented;
    tempDir = StoreTestUtils.createTempDirectory("storeDir-" + UtilsTest.getRandomString(10));
    tempDirStr = tempDir.getAbsolutePath();
    setupTestState();
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  public void cleanup() throws InterruptedException, IOException, StoreException {
    if (store.isStarted()) {
      store.shutdown();
    }
    scheduler.shutdown();
    assertTrue(scheduler.awaitTermination(1, TimeUnit.SECONDS));
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  // helpers
  // general

  class MockIdInfo {
    MessageInfo msgInfo;
    ByteBuffer byteBuffer;
    int logSegmentIndexForPut;
    int logSegmentIndexforDelete;

    MockIdInfo(MessageInfo msgInfo, ByteBuffer byteBuffer, int logSegmentIndexForPut) {
      this.msgInfo = msgInfo;
      this.byteBuffer = byteBuffer;
      this.logSegmentIndexForPut = logSegmentIndexForPut;
    }
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
   * Puts some blobs into the {@link BlobStore}. Assumes new puts go into last segment as per {@link #idsByLogSegment}
   * @param count the number of blobs to PUT.
   * @param size the size of each blob.
   * @param expiresAtMs the expiry time (in ms) of each blob.
   * @return the {@link MockId}s of the blobs created.
   * @throws StoreException
   */
  List<MockId> put(int count, long size, long expiresAtMs) throws StoreException {
    if (count <= 0) {
      throw new IllegalArgumentException("Number of put entries to add cannot be <= 0");
    }
    List<MockId> ids = new ArrayList<>(count);
    List<MessageInfo> infos = new ArrayList<>(count);
    List<ByteBuffer> buffers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      MockId id = getUniqueId();
      MessageInfo info = new MessageInfo(id, size, expiresAtMs);
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) size));
      ids.add(id);
      infos.add(info);
      buffers.add(buffer);
      allKeys.put(id, new MockIdInfo(info, buffer, idsByLogSegment.size() - 1));
      if (expiresAtMs != Utils.Infinite_Time && expiresAtMs < time.milliseconds()) {
        expiredKeys.add(id);
      } else {
        liveKeys.add(id);
      }
    }
    store.put(new MockMessageWriteSet(infos, buffers));
    return ids;
  }

  /**
   * Deletes a blob
   * @param idToDelete the {@link MockId} of the blob to DELETE.
   * @return the {@link MessageInfo} associated with the DELETE.
   * @throws StoreException
   */
  MessageInfo delete(MockId idToDelete) throws StoreException {
    MessageInfo info = new MessageInfo(idToDelete, DELETE_RECORD_SIZE);
    ByteBuffer buffer = ByteBuffer.allocate((int) DELETE_RECORD_SIZE);
    store.delete(new MockMessageWriteSet(Collections.singletonList(info), Collections.singletonList(buffer)));
    deletedKeys.add(idToDelete);
    allKeys.get(idToDelete).logSegmentIndexforDelete = idsByLogSegment.size() - 1;
    return info;
  }

  // test setup helpers

  /**
   * Sets up some state in order to make sure all cases are represented and the tests don't need to do any setup
   * individually. For understanding the created store, please read the source code which is annotated with comments.
   * @throws InterruptedException
   * @throws StoreException
   */
  void setupTestState() throws InterruptedException, StoreException {
    long segmentCapacity = isLogSegmented ? SEGMENT_CAPACITY : LOG_CAPACITY;
    metricRegistry = new MetricRegistry();
    StorageManagerMetrics metrics = new StorageManagerMetrics(metricRegistry);
    properties.put("store.index.max.number.of.inmem.elements", Integer.toString(MAX_IN_MEM_ELEMENTS));
    properties.put("store.segment.size.in.bytes", Long.toString(segmentCapacity));
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    store =
        new BlobStore(storeId, config, scheduler, diskIOScheduler, metrics, tempDirStr, LOG_CAPACITY, STORE_KEY_FACTORY,
            recovery, hardDelete, time);
    store.start();
    // advance time by a millisecond in order to be able to add expired keys and to avoid keys that are expired from
    // being picked for delete.
    time.sleep(1);
    long expectedStoreSize;
    if (!isLogSegmented) {
      // log is filled about ~50%.
      expectedStoreSize = segmentCapacity / 2;
      addCuratedData(expectedStoreSize, 0);
    } else {
      expectedStoreSize = segmentCapacity;
      // first log segment is filled to capacity.
      addCuratedData(segmentCapacity, 0);
      assertEquals("Store size not as expected", expectedStoreSize, store.getSizeInBytes());

      // second log segment is filled but has some space at the end (free space has to be less than the lesser of the
      // standard delete and put record sizes so that the next write causes a roll over of log segments).
      long sizeToWrite = segmentCapacity - (DELETE_RECORD_SIZE - 1);
      expectedStoreSize += sizeToWrite;
      addCuratedData(sizeToWrite, 1);
      assertEquals("Store size not as expected", expectedStoreSize, store.getSizeInBytes());

      // third log segment is partially filled and is left as the "active" segment
      sizeToWrite = segmentCapacity / 3;
      // First Index Segment
      // 1 PUT entry
      idsByLogSegment.add(new HashSet<MockId>());
      MockId addedId = put(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
      idsByLogSegment.get(2).add(addedId);
      // 1 DELETE for a key in the first log segment
      MockId idToDelete = getIdToDelete(idsByLogSegment.get(0));
      delete(idToDelete);
      idsByLogSegment.get(2).add(idToDelete);
      // 1 DELETE for a key in the second log segment
      idToDelete = getIdToDelete(idsByLogSegment.get(1));
      delete(idToDelete);
      idsByLogSegment.get(2).add(idToDelete);
      // 1 DELETE for the PUT in the same log segment
      deletedKeys.add(addedId);
      liveKeys.remove(addedId);
      delete(addedId);
      // 1 PUT entry that spans the rest of the data in the segment (upto a third of the segment size)
      long size = sizeToWrite - (LogSegment.HEADER_SIZE + PUT_RECORD_SIZE + 3 * DELETE_RECORD_SIZE);
      addedId = put(1, size, Utils.Infinite_Time).get(0);
      idsByLogSegment.get(2).add(addedId);
      // the store counts the wasted space at the end of the second segment as "used capacity".
      expectedStoreSize = 2 * segmentCapacity + sizeToWrite;
      assertEquals("Store size not as expected", expectedStoreSize, store.getSizeInBytes());
      // fourth and fifth log segment are free.
    }
    // make sure all indexes are written to disk and mapped as required (forcing IndexPersistor to run).
    reloadStore();
    assertEquals("Store size not as expected", expectedStoreSize, store.getSizeInBytes());
  }

  /**
   * Adds some curated data into the store in order to ensure a good mix for testing. For understanding the created
   * store, please read the source code which is annotated with comments.
   * @param sizeToWrite the size to add for.
   * @param logSegmentIndex the index of the log segment being written to (0 being the first one).
   * @throws StoreException
   */
  void addCuratedData(long sizeToWrite, int logSegmentIndex) throws StoreException {
    Set<MockId> idsInLogSegment = new HashSet<>();
    idsByLogSegment.add(idsInLogSegment);
    List<Set<MockId>> idsGroupedByIndexSegment = new ArrayList<>();
    int deletedKeyCount = 0;

    Set<MockId> idsInIndexSegment = new HashSet<>();
    // First Index Segment
    // 1 PUT
    idsInIndexSegment.addAll(put(1, PUT_RECORD_SIZE, Utils.Infinite_Time));
    // 2 more PUT
    idsInIndexSegment.addAll(put(2, PUT_RECORD_SIZE, Utils.Infinite_Time));
    // 2 PUT EXPIRED
    idsInIndexSegment.addAll(put(2, PUT_RECORD_SIZE, 0));
    idsGroupedByIndexSegment.add(idsInIndexSegment);
    idsInLogSegment.addAll(idsInIndexSegment);

    idsInIndexSegment = new HashSet<>();
    // Second Index Segment
    // 4 PUT
    idsInIndexSegment.addAll(put(4, PUT_RECORD_SIZE, Utils.Infinite_Time));
    // 1 DELETE for a PUT in the same index segment
    delete(getIdToDelete(idsInIndexSegment));
    deletedKeyCount++;
    // 1 DELETE for a PUT in the first index segment
    delete(getIdToDelete(idsGroupedByIndexSegment.get(0)));
    deletedKeyCount++;
    idsGroupedByIndexSegment.add(idsInIndexSegment);
    idsInLogSegment.addAll(idsInIndexSegment);

    // Third and Fourth Index Segment
    for (int seg = 0; seg < 2; seg++) {
      idsInIndexSegment = new HashSet<>();
      // 3 PUT
      idsInIndexSegment.addAll(put(3, PUT_RECORD_SIZE, Utils.Infinite_Time));
      // 1 PUT for an expired blob
      MockId expiredId = put(1, PUT_RECORD_SIZE, 0).get(0);
      idsInIndexSegment.add(expiredId);
      // 1 DELETE for the expired PUT
      delete(expiredId);
      deletedKeys.add(expiredId);
      expiredKeys.remove(expiredId);
      deletedKeyCount++;
      // 1 PUT
      idsInIndexSegment.addAll(put(1, PUT_RECORD_SIZE, Utils.Infinite_Time));
      idsGroupedByIndexSegment.add(idsInIndexSegment);
      idsInLogSegment.addAll(idsInIndexSegment);
    }

    idsInIndexSegment = new HashSet<>();
    // Fifth Index Segment
    // 1 PUT entry
    idsInIndexSegment.addAll(put(1, PUT_RECORD_SIZE, Utils.Infinite_Time));
    // 1 DELETE for a PUT in each of the third and fourth segments
    delete(getIdToDelete(idsGroupedByIndexSegment.get(2)));
    deletedKeyCount++;
    delete(getIdToDelete(idsGroupedByIndexSegment.get(3)));
    deletedKeyCount++;
    // 1 DELETE for the PUT in the same segment
    delete(getIdToDelete(idsInIndexSegment));
    deletedKeyCount++;
    // 1 PUT entry that spans the rest of the data in the segment
    idsInLogSegment.addAll(idsInIndexSegment);

    long sizeWritten = isLogSegmented ? LogSegment.HEADER_SIZE : 0;
    sizeWritten += idsInLogSegment.size() * PUT_RECORD_SIZE + deletedKeyCount * DELETE_RECORD_SIZE;
    MockId id = put(1, sizeToWrite - sizeWritten, Utils.Infinite_Time).get(0);
    idsInIndexSegment.add(id);
    idsGroupedByIndexSegment.add(idsInIndexSegment);
    idsInLogSegment.add(id);
  }

  /**
   * Gets an id to delete from {@code ids} by picking the first live key encountered.
   * @param ids the {@link MockId}s to choose from
   * @return an id to delete from {@code ids}
   */
  MockId getIdToDelete(Set<MockId> ids) {
    MockId deleteCandidate = null;
    for (MockId id : ids) {
      if (liveKeys.contains(id)) {
        deleteCandidate = id;
        break;
      }
    }
    if (deleteCandidate == null) {
      throw new IllegalStateException("Could not find a key to delete in set: " + ids);
    }
    deletedKeys.add(deleteCandidate);
    liveKeys.remove(deleteCandidate);
    return deleteCandidate;
  }

  /**
   * Shuts down and restarts the store. All further tests will implcitly test persistence.
   * @throws StoreException
   */
  void reloadStore() throws StoreException {
    if (store.isStarted()) {
      store.shutdown();
    }
    assertFalse("Store should be shutdown", store.isStarted());
    metricRegistry = new MetricRegistry();
    StorageManagerMetrics metrics = new StorageManagerMetrics(metricRegistry);
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    store =
        new BlobStore(storeId, config, scheduler, diskIOScheduler, metrics, tempDirStr, LOG_CAPACITY, STORE_KEY_FACTORY,
            recovery, hardDelete, time);
    assertFalse("Store should not be started", store.isStarted());
    store.start();
    assertTrue("Store should be started", store.isStarted());
  }

  // putErrorCasesTest() helpers

  /**
   * Verifies that PUT fails.
   * @param idToPut the {@link MockId} to PUT.
   * @param expectedErrorCode the expected {@link StoreErrorCodes} for the failure.
   */
  void verifyPutFailure(MockId idToPut, StoreErrorCodes expectedErrorCode) {
    MessageInfo info = new MessageInfo(idToPut, PUT_RECORD_SIZE);
    MessageWriteSet writeSet = new BlobStoreTestUtils.MockMessageWriteSet(Collections.singletonList(info),
        Collections.singletonList(ByteBuffer.allocate(1)));
    try {
      store.put(writeSet);
      fail("Store PUT should have failed");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
    }
  }

  // deleteErrorCasesTest() helpers

  /**
   * Verifies that DELETE fails.
   * @param idToDelete the {@link MockId} to DELETE.
   * @param expectedErrorCode the expected {@link StoreErrorCodes} for the failure.
   */
  void verifyDeleteFailure(MockId idToDelete, StoreErrorCodes expectedErrorCode) {
    MessageInfo info = new MessageInfo(idToDelete, DELETE_RECORD_SIZE);
    MessageWriteSet writeSet =
        new MockMessageWriteSet(Collections.singletonList(info), Collections.singletonList(ByteBuffer.allocate(1)));
    try {
      store.delete(writeSet);
      fail("Store DELETE should have failed");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
    }
  }
}
