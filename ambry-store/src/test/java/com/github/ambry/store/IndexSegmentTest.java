/*
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
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.store.IndexSegment.*;
import static com.github.ambry.utils.Utils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link IndexSegment}s
 */
@RunWith(Parameterized.class)
public class IndexSegmentTest {
  private static final int CUSTOM_ID_SIZE = 10;
  private static final int KEY_SIZE = new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE)).sizeInBytes();
  private static final int SMALLER_KEY_SIZE = new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE / 2)).sizeInBytes();
  private static final int LARGER_KEY_SIZE =
      new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE + CUSTOM_ID_SIZE / 2)).sizeInBytes();
  private static final MockTime time = new MockTime();
  private static final long DELETE_FILE_SPAN_SIZE = 10;
  private static final long TTL_UPDATE_FILE_SPAN_SIZE = 7;
  private static final StoreKeyFactory STORE_KEY_FACTORY;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private final File tempDir;
  private final StoreMetrics metrics;
  private final short formatVersion;
  private StoreConfig config;
  private final Properties properties = new Properties();

  /**
   * Running for all versions and {@link IndexMemState} values.
   * @return an array with the list of parameters for different iterations.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    short[] versions = new short[]{PersistentIndex.VERSION_0, PersistentIndex.VERSION_1, PersistentIndex.VERSION_2,
        PersistentIndex.VERSION_3};
    List<Object[]> parametersList = new ArrayList<>();
    for (short version : versions) {
      for (IndexMemState state : IndexMemState.values()) {
        parametersList.add(new Object[]{version, state});
      }
    }
    return parametersList;
  }

  /**
   * Creates a temporary directory and sets up metrics.
   * @param formatVersion the format version of the index
   * @param indexMemState the value for {@link StoreConfig#storeIndexMemStateName}
   * @throws IOException
   */
  public IndexSegmentTest(short formatVersion, IndexMemState indexMemState) throws IOException {
    tempDir = StoreTestUtils.createTempDirectory("indexSegmentDir-" + TestUtils.getRandomString(10));
    MetricRegistry metricRegistry = new MetricRegistry();
    metrics = new StoreMetrics(metricRegistry);
    this.formatVersion = formatVersion;
    PersistentIndex.CURRENT_VERSION = formatVersion;
    setIndexMemState(indexMemState);
  }

  /**
   * Deletes the temporary directory.
   * @throws IOException
   */
  @After
  public void cleanup() throws IOException {
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Comprehensive tests for {@link IndexSegment}
   */
  @Test
  public void comprehensiveTest() throws IOException, StoreException {
    if (formatVersion >= PersistentIndex.VERSION_2) {
      for (boolean includeSmall : new boolean[]{false, true}) {
        for (boolean includeLarge : new boolean[]{false, true}) {
          doComprehensiveTest(formatVersion, includeSmall, includeLarge);
        }
      }
    } else {
      doComprehensiveTest(formatVersion, false, false);
    }
  }

  /**
   * Tests the case when {@link IndexSegment#writeIndexSegmentToFile(Offset)} is provided with different offsets <=
   * {@link IndexSegment#getEndOffset()} and makes sure that only the relevant parts of the segment are written to disk.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void partialWriteTest() throws IOException, StoreException {
    String prevLogSegmentName = LogSegmentNameHelper.getName(0, 0);
    String logSegmentName = LogSegmentNameHelper.getNextPositionName(prevLogSegmentName);
    Offset startOffset = new Offset(logSegmentName, 0);
    MockId id1 = new MockId("0" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    MockId id2 = new MockId("1" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    MockId id3 = new MockId("2" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    short updateVersion = 0;
    IndexValue value1 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 0), Utils.Infinite_Time, time.milliseconds(),
            accountId, containerId, updateVersion, formatVersion);
    IndexValue value2 = IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 1000), time.milliseconds() + 1,
        time.milliseconds(), accountId, containerId, updateVersion, formatVersion);
    IndexValue value3 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 2000), Utils.Infinite_Time, time.milliseconds(),
            accountId, containerId, updateVersion, formatVersion);
    time.sleep(TimeUnit.SECONDS.toMillis(1));
    // generate a TTL Update
    IndexValue ttlUpValue2 =
        IndexValueTest.getIndexValue(value2.getSize(), value2.getOffset(), Utils.Infinite_Time, time.milliseconds(),
            value2.getAccountId(), value2.getContainerId(), updateVersion, formatVersion);
    ttlUpValue2.setNewOffset(new Offset(logSegmentName, 3000));
    ttlUpValue2.setNewSize(50);
    ttlUpValue2.setFlag(IndexValue.Flags.Ttl_Update_Index);
    time.sleep(TimeUnit.SECONDS.toMillis(1));
    // generate a DELETE
    IndexValue delValue2 = IndexValueTest.getIndexValue(value2.getSize(), value2.getOffset(), ttlUpValue2.getFlags(),
        value2.getExpiresAtMs(), value2.getOffset().getOffset(), time.milliseconds(), value2.getAccountId(),
        value2.getContainerId(), updateVersion, formatVersion);
    delValue2.setNewOffset(new Offset(logSegmentName, 3050));
    delValue2.setNewSize(100);
    delValue2.setFlag(IndexValue.Flags.Delete_Index);
    IndexSegment indexSegment = generateIndexSegment(startOffset, STORE_KEY_FACTORY);
    // inserting in the opposite order by design to ensure that writes are based on offset ordering and not key ordering
    indexSegment.addEntry(new IndexEntry(id3, value1), new Offset(logSegmentName, 1000));
    indexSegment.addEntry(new IndexEntry(id2, value2), new Offset(logSegmentName, 2000));
    indexSegment.addEntry(new IndexEntry(id1, value3), new Offset(logSegmentName, 3000));
    indexSegment.addEntry(new IndexEntry(id2, ttlUpValue2), new Offset(logSegmentName, 3050));
    indexSegment.addEntry(new IndexEntry(id2, delValue2), new Offset(logSegmentName, 3150));

    // provide end offsets such that nothing is written
    checkNonCreationOfIndexSegmentFile(indexSegment, new Offset(prevLogSegmentName, 0));
    checkNonCreationOfIndexSegmentFile(indexSegment,
        new Offset(prevLogSegmentName, indexSegment.getStartOffset().getOffset()));
    checkNonCreationOfIndexSegmentFile(indexSegment, new Offset(logSegmentName, 0));
    List<MockId> shouldBeFound = new ArrayList<>();
    List<MockId> shouldNotBeFound = new ArrayList<>(Arrays.asList(id3, id2, id1));
    for (int safeEndPoint = 1000; safeEndPoint <= 3000; safeEndPoint += 1000) {
      shouldBeFound.add(shouldNotBeFound.remove(0));
      // repeat twice
      for (int i = 0; i < 2; i++) {
        indexSegment.writeIndexSegmentToFile(new Offset(logSegmentName, safeEndPoint));
        Journal journal = new Journal(tempDir.getAbsolutePath(), 3, 3);
        IndexSegment fromDisk =
            new IndexSegment(indexSegment.getFile(), false, STORE_KEY_FACTORY, config, metrics, journal, time);
        assertEquals("End offset not as expected", new Offset(logSegmentName, safeEndPoint), fromDisk.getEndOffset());
        assertEquals("Number of items incorrect", shouldBeFound.size(), fromDisk.getNumberOfItems());
        for (MockId id : shouldBeFound) {
          verifyValues(fromDisk, id, 1, EnumSet.noneOf(IndexValue.Flags.class));
        }
        for (MockId id : shouldNotBeFound) {
          assertNull("Values for key should not have been found", fromDisk.find(id));
        }
      }
    }
    // now persist the ttl update only
    indexSegment.writeIndexSegmentToFile(new Offset(logSegmentName, 3050));
    Journal journal = new Journal(tempDir.getAbsolutePath(), 3, 3);
    IndexSegment fromDisk =
        new IndexSegment(indexSegment.getFile(), false, STORE_KEY_FACTORY, config, metrics, journal, time);
    assertEquals("Number of items incorrect", 4, fromDisk.getNumberOfItems());
    for (MockId id : new MockId[]{id1, id2, id3}) {
      int valueCount = id.equals(id2) ? 2 : 1;
      EnumSet<IndexValue.Flags> flags =
          id.equals(id2) ? EnumSet.of(IndexValue.Flags.Ttl_Update_Index) : EnumSet.noneOf(IndexValue.Flags.class);
      verifyValues(fromDisk, id, valueCount, flags);
    }
    // now persist the delete too
    indexSegment.writeIndexSegmentToFile(new Offset(logSegmentName, 3150));
    journal = new Journal(tempDir.getAbsolutePath(), 3, 3);
    fromDisk = new IndexSegment(indexSegment.getFile(), false, STORE_KEY_FACTORY, config, metrics, journal, time);
    assertEquals("Number of items incorrect", 5, fromDisk.getNumberOfItems());
    for (MockId id : new MockId[]{id1, id2, id3}) {
      int valueCount = id.equals(id2) ? 3 : 1;
      EnumSet<IndexValue.Flags> flags =
          id.equals(id2) ? EnumSet.of(IndexValue.Flags.Delete_Index, IndexValue.Flags.Ttl_Update_Index)
              : EnumSet.noneOf(IndexValue.Flags.class);
      verifyValues(fromDisk, id, valueCount, flags);
    }
  }

  /**
   * Tests some corner cases with
   * {@link IndexSegment#getIndexEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong, boolean)}
   * - tests that all values of a key are returned even if the find entries condition max size expires when the first
   * value is loaded
   * @throws StoreException
   */
  @Test
  public void getIndexEntriesCornerCasesTest() throws IOException, StoreException {
    String logSegmentName = LogSegmentNameHelper.getName(0, 0);
    MockId id1 = new MockId("0" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    MockId id2 = new MockId("1" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    MockId id3 = new MockId("2" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    short updateVersion = (short) 0;
    IndexValue value1 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 0), Utils.Infinite_Time, time.milliseconds(),
            accountId, containerId, updateVersion, formatVersion);
    IndexValue value2 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 1000), Utils.Infinite_Time, time.milliseconds(),
            accountId, containerId, updateVersion, formatVersion);
    IndexValue value3 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 2000), Utils.Infinite_Time, time.milliseconds(),
            accountId, containerId, updateVersion, formatVersion);
    time.sleep(TimeUnit.SECONDS.toMillis(1));
    // generate a TTL Update
    IndexValue ttlUpValue2 =
        IndexValueTest.getIndexValue(value2.getSize(), value2.getOffset(), Utils.Infinite_Time, time.milliseconds(),
            value2.getAccountId(), value2.getContainerId(), updateVersion, formatVersion);
    ttlUpValue2.setNewOffset(new Offset(logSegmentName, 3000));
    ttlUpValue2.setNewSize(50);
    ttlUpValue2.setFlag(IndexValue.Flags.Ttl_Update_Index);
    time.sleep(TimeUnit.SECONDS.toMillis(1));
    // generate a DELETE
    IndexValue delValue2 = IndexValueTest.getIndexValue(value2.getSize(), value2.getOffset(), ttlUpValue2.getFlags(),
        value2.getExpiresAtMs(), value2.getOffset().getOffset(), time.milliseconds(), value2.getAccountId(),
        value2.getContainerId(), updateVersion, formatVersion);
    delValue2.setNewOffset(new Offset(logSegmentName, 3050));
    delValue2.setNewSize(100);
    delValue2.setFlag(IndexValue.Flags.Delete_Index);
    IndexSegment indexSegment = generateIndexSegment(new Offset(logSegmentName, 0), STORE_KEY_FACTORY);
    // inserting in the opposite order by design to ensure that writes are based on offset ordering and not key ordering
    indexSegment.addEntry(new IndexEntry(id3, value1), new Offset(logSegmentName, 1000));
    indexSegment.addEntry(new IndexEntry(id2, value2), new Offset(logSegmentName, 2000));
    indexSegment.addEntry(new IndexEntry(id1, value3), new Offset(logSegmentName, 3000));
    indexSegment.addEntry(new IndexEntry(id2, ttlUpValue2), new Offset(logSegmentName, 3050));
    indexSegment.addEntry(new IndexEntry(id2, delValue2), new Offset(logSegmentName, 3150));

    indexSegment.writeIndexSegmentToFile(new Offset(logSegmentName, 3150));
    indexSegment.seal();
    List<IndexEntry> entries = new ArrayList<>();
    for (boolean sealed : new boolean[]{false, true}) {
      Journal journal = new Journal(tempDir.getAbsolutePath(), 3, 3);
      IndexSegment fromDisk =
          new IndexSegment(indexSegment.getFile(), sealed, STORE_KEY_FACTORY, config, metrics, journal, time);
      // getIndexEntriesSince with maxSize = 0 should not return anything
      FindEntriesCondition condition = new FindEntriesCondition(0);
      assertFalse("getIndexEntriesSince() should not return anything",
          fromDisk.getIndexEntriesSince(null, condition, entries, new AtomicLong(0), false));
      assertEquals("There should be no entries returned", 0, entries.size());
      // getIndexEntriesSince with maxSize <= 1000 should return only the first key (id1)
      condition = new FindEntriesCondition(1000);
      assertTrue("getIndexEntriesSince() should return one entry",
          fromDisk.getIndexEntriesSince(null, condition, entries, new AtomicLong(0), false));
      assertEquals("There should be one entry returned", 1, entries.size());
      assertEquals("Key in entry is incorrect", id1, entries.get(0).getKey());
      assertEquals("Value in entry is incorrect", value3.getBytes(), entries.get(0).getValue().getBytes());
      entries.clear();
      // getIndexEntriesSince with maxSize > 1000 and <= 2150 should return four entries
      for (int maxSize : new int[]{1001, 2050, 2150}) {
        condition = new FindEntriesCondition(maxSize);
        assertTrue("getIndexEntriesSince() should return entries",
            fromDisk.getIndexEntriesSince(null, condition, entries, new AtomicLong(0), false));
        assertEquals("There should be four entries returned", 4, entries.size());
        assertEquals("Key in entry is incorrect", id1, entries.get(0).getKey());
        assertEquals("Value in entry is incorrect", value3.getBytes(), entries.get(0).getValue().getBytes());
        assertEquals("Key in entry is incorrect", id2, entries.get(1).getKey());
        assertEquals("Value in entry is incorrect", value2.getBytes(), entries.get(1).getValue().getBytes());
        assertEquals("Key in entry is incorrect", id2, entries.get(2).getKey());
        assertEquals("Value in entry is incorrect", ttlUpValue2.getBytes(), entries.get(2).getValue().getBytes());
        assertEquals("Key in entry is incorrect", id2, entries.get(3).getKey());
        assertEquals("Value in entry is incorrect", delValue2.getBytes(), entries.get(3).getValue().getBytes());
        entries.clear();
      }
      // getIndexEntriesSince with maxSize > 2150 should return five entries
      condition = new FindEntriesCondition(2151);
      assertTrue("getIndexEntriesSince() should return entries",
          fromDisk.getIndexEntriesSince(null, condition, entries, new AtomicLong(0), false));
      assertEquals("There should be five entries returned", 5, entries.size());
      assertEquals("Key in entry is incorrect", id1, entries.get(0).getKey());
      assertEquals("Value in entry is incorrect", value3.getBytes(), entries.get(0).getValue().getBytes());
      assertEquals("Key in entry is incorrect", id2, entries.get(1).getKey());
      assertEquals("Value in entry is incorrect", value2.getBytes(), entries.get(1).getValue().getBytes());
      assertEquals("Key in entry is incorrect", id2, entries.get(2).getKey());
      assertEquals("Value in entry is incorrect", ttlUpValue2.getBytes(), entries.get(2).getValue().getBytes());
      assertEquals("Key in entry is incorrect", id2, entries.get(3).getKey());
      assertEquals("Value in entry is incorrect", delValue2.getBytes(), entries.get(3).getValue().getBytes());
      assertEquals("Key in entry is incorrect", id3, entries.get(4).getKey());
      assertEquals("Value in entry is incorrect", value1.getBytes(), entries.get(4).getValue().getBytes());
      entries.clear();
      // getIndexEntriesSince with maxSize > 0 and <= 1150 starting from id2 should return two entries
      for (int maxSize : new int[]{1, 1050, 1150}) {
        condition = new FindEntriesCondition(maxSize);
        assertTrue("getIndexEntriesSince() should return entries",
            fromDisk.getIndexEntriesSince(id1, condition, entries, new AtomicLong(0), false));
        assertEquals("There should be three entries returned", 3, entries.size());
        assertEquals("Key in entry is incorrect", id2, entries.get(0).getKey());
        assertEquals("Value in entry is incorrect", value2.getBytes(), entries.get(0).getValue().getBytes());
        assertEquals("Key in entry is incorrect", id2, entries.get(1).getKey());
        assertEquals("Value in entry is incorrect", ttlUpValue2.getBytes(), entries.get(1).getValue().getBytes());
        assertEquals("Key in entry is incorrect", id2, entries.get(2).getKey());
        assertEquals("Value in entry is incorrect", delValue2.getBytes(), entries.get(2).getValue().getBytes());
        entries.clear();
      }
    }
  }

  /**
   * Test cases where exceptions occurred while performing memory mapping of index segment.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void memoryMapFailureTest() throws IOException, StoreException {
    assumeTrue(formatVersion == PersistentIndex.VERSION_1
        && config.storeIndexMemState == IndexMemState.MMAP_WITHOUT_FORCE_LOAD);
    String logSegmentName = LogSegmentNameHelper.getName(0, 0);
    StoreKeyFactory mockStoreKeyFactory = Mockito.spy(STORE_KEY_FACTORY);
    IndexSegment indexSegment = generateIndexSegment(new Offset(logSegmentName, 0), mockStoreKeyFactory);
    // verify that StoreErrorCodes.File_Not_Found can be captured when performing memory map
    try {
      indexSegment.seal();
      fail("should fail");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.File_Not_Found, e.getErrorCode());
    }
    // verify that StoreErrorCodes.IOError can be captured when performing memory map
    indexSegment = generateIndexSegment(new Offset(logSegmentName, 0), mockStoreKeyFactory);
    MockId id1 = new MockId("0" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    IndexValue value1 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 0), Utils.Infinite_Time, time.milliseconds(),
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), (short) 0, formatVersion);
    indexSegment.addEntry(new IndexEntry(id1, value1), new Offset(logSegmentName, 1000));
    indexSegment.writeIndexSegmentToFile(new Offset(logSegmentName, 1000));
    doThrow(new IOException(StoreException.IO_ERROR_STR)).when(mockStoreKeyFactory)
        .getStoreKey(any(DataInputStream.class));
    try {
      indexSegment.seal();
      fail("should fail");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.IOError, e.getErrorCode());
    }
    // test that when IOException's error message is null, the error code should be Unknown_Error
    doThrow(new IOException()).when(mockStoreKeyFactory).getStoreKey(any(DataInputStream.class));
    try {
      indexSegment.seal();
      fail("should fail");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.Unknown_Error, e.getErrorCode());
    }
  }

  /**
   * Test cases where exceptions occurred while reading from file.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void readFromFileFailureTest() throws StoreException, IOException {
    String logSegmentName = LogSegmentNameHelper.getName(0, 0);
    StoreKeyFactory mockStoreKeyFactory = Mockito.spy(STORE_KEY_FACTORY);
    Journal journal = new Journal(tempDir.getAbsolutePath(), 3, 3);
    IndexSegment indexSegment = generateIndexSegment(new Offset(logSegmentName, 0), mockStoreKeyFactory);
    MockId id1 = new MockId("0" + TestUtils.getRandomString(CUSTOM_ID_SIZE - 1));
    IndexValue value1 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName, 0), Utils.Infinite_Time, time.milliseconds(),
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), (short) 0, formatVersion);
    indexSegment.addEntry(new IndexEntry(id1, value1), new Offset(logSegmentName, 1000));
    indexSegment.writeIndexSegmentToFile(new Offset(logSegmentName, 1000));
    // test IOException is captured and StoreException.IOError is thrown
    doThrow(new IOException(StoreException.IO_ERROR_STR)).when(mockStoreKeyFactory)
        .getStoreKey(any(DataInputStream.class));
    try {
      new IndexSegment(indexSegment.getFile(), false, mockStoreKeyFactory, config, metrics, journal, time);
      fail("should fail");
    } catch (StoreException e) {
      assertEquals("Mismatch in error code", StoreErrorCodes.Index_Creation_Failure, e.getErrorCode());
    }
  }

  /**
   * Test populating bloom filter with whole blob id and with UUID respectively.
   * @throws Exception
   */
  @Test
  public void populateBloomFilterWithUuidTest() throws Exception {
    assumeTrue(formatVersion > PersistentIndex.VERSION_1);
    // with default config, bloom filter will be populated by whole blob id bytes array
    String logSegmentName1 = LogSegmentNameHelper.getName(0, 0);
    int indexValueSize =
        PersistentIndex.CURRENT_VERSION == PersistentIndex.VERSION_3 ? IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3
            : IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2;
    IndexSegment indexSegment1 =
        new IndexSegment(tempDir.getAbsolutePath(), new Offset(logSegmentName1, 0), STORE_KEY_FACTORY,
            KEY_SIZE + indexValueSize, indexValueSize, config,
            metrics, time);
    Random random = new Random();
    short accountId1 = getRandomShort(random);
    short containerId1 = getRandomShort(random);
    short accountId2, containerId2;
    do {
      accountId2 = getRandomShort(random);
    } while (accountId2 == accountId1);
    do {
      containerId2 = getRandomShort(random);
    } while (containerId2 == containerId1);
    String idStr = TestUtils.getRandomString(CUSTOM_ID_SIZE);
    // generate two ids with same id string but different account/container.
    MockId id1 = new MockId(idStr, accountId1, containerId1);
    MockId id2 = new MockId(idStr, accountId2, containerId2);
    IndexValue value1 =
        IndexValueTest.getIndexValue(1000, new Offset(logSegmentName1, 0), Utils.Infinite_Time, time.milliseconds(),
            accountId1, containerId1, (short) 0, formatVersion);
    indexSegment1.addEntry(new IndexEntry(id1, value1), new Offset(logSegmentName1, 1000));
    // deliberately add one more entry for later bloom rebuilding test
    indexSegment1.addEntry(new IndexEntry(id2, value1), new Offset(logSegmentName1, 2000));
    indexSegment1.writeIndexSegmentToFile(new Offset(logSegmentName1, 2000));
    indexSegment1.seal();
    // test that id1 can be found in index segment but id2 should be non-existent because bloom filter considers it not present
    Set<IndexValue> findResult = indexSegment1.find(id1);
    assertNotNull("Should have found the added key", findResult);
    assertEquals(accountId1, findResult.iterator().next().getAccountId());
    assertNull("Should have failed to find non existent key", indexSegment1.find(id2));

    // create second index segment whose bloom filter is populated with UUID only.
    // We add id1 into indexSegment2 and attempt to get id2 which should succeed.
    Properties properties = new Properties();
    properties.setProperty("store.uuid.based.bloom.filter.enabled", Boolean.toString(true));
    StoreConfig storeConfig = new StoreConfig(new VerifiableProperties(properties));
    String logSegmentName2 = LogSegmentNameHelper.getName(1, 0);
    IndexSegment indexSegment2 =
        new IndexSegment(tempDir.getAbsolutePath(), new Offset(logSegmentName2, 0), STORE_KEY_FACTORY,
            KEY_SIZE + indexValueSize, indexValueSize,
            storeConfig, metrics, time);
    indexSegment2.addEntry(new IndexEntry(id1, value1), new Offset(logSegmentName2, 1000));
    indexSegment2.writeIndexSegmentToFile(new Offset(logSegmentName2, 1000));
    indexSegment2.seal();
    findResult = indexSegment2.find(id1);
    assertNotNull("Should have found the id1", findResult);
    // test that we are able to get id2 from indexSegment2
    findResult = indexSegment2.find(id2);
    assertNotNull("Should have found the id2", findResult);
    // verify that the found entry actually has account/container associated with id1
    IndexValue indexValue = findResult.iterator().next();
    assertTrue("Account or container is not expected",
        indexValue.getAccountId() == accountId1 && indexValue.getContainerId() == containerId1);
    File bloomFile1 =
        new File(tempDir, generateIndexSegmentFilenamePrefix(new Offset(logSegmentName1, 0)) + BLOOM_FILE_NAME_SUFFIX);
    assertTrue("The bloom file should exist", bloomFile1.exists());
    long lengthBeforeRebuild = bloomFile1.length();

    // rebuild bloom filter in indexSegment1
    properties.setProperty("store.index.rebuild.bloom.filter.enabled", Boolean.toString(true));
    storeConfig = new StoreConfig(new VerifiableProperties(properties));
    IndexSegment indexSegmentFromFile =
        new IndexSegment(indexSegment1.getFile(), true, STORE_KEY_FACTORY, storeConfig, metrics, null, time);
    // test that id2 is found in reconstructed index segment (with bloom filter rebuilt based on UUID only)
    findResult = indexSegmentFromFile.find(id2);
    assertNotNull("Should have found the id2", findResult);
    indexValue = findResult.iterator().next();
    assertTrue("Account or container is not expected",
        indexValue.getAccountId() == accountId1 && indexValue.getContainerId() == containerId1);
    // verify that bloom file length didn't change
    assertEquals("Bloom file length has changed", lengthBeforeRebuild, bloomFile1.length());

    // now, change value of storeIndexMaxNumberOfInmemElements to 1
    properties.setProperty("store.index.max.number.of.inmem.elements", Integer.toString(1));
    storeConfig = new StoreConfig(new VerifiableProperties(properties));
    // rebuild bloomFile1 again and file size should be changed
    new IndexSegment(indexSegment1.getFile(), true, STORE_KEY_FACTORY, storeConfig, metrics, null, time);
    assertTrue("Bloom file length has changed", lengthBeforeRebuild > bloomFile1.length());

    // additional test: exception occurs when deleting previous bloom file
    assertTrue("Could not make unwritable", tempDir.setWritable(false));
    try {
      new IndexSegment(indexSegment1.getFile(), true, STORE_KEY_FACTORY, storeConfig, metrics, null, time);
      fail("Deletion on unwritable file should fail");
    } catch (StoreException e) {
      assertEquals("Error code is not expected.", StoreErrorCodes.Index_Creation_Failure, e.getErrorCode());
    } finally {
      assertTrue("Could not make writable", tempDir.setWritable(true));
    }
  }

  // helpers

  /**
   * @param state the value for {@link StoreConfig#storeIndexMemStateName}
   */
  private void setIndexMemState(IndexMemState state) {
    properties.setProperty(StoreConfig.storeIndexMemStateName, state.name());
    properties.setProperty("store.set.file.permission.enabled", Boolean.toString(true));
    config = new StoreConfig(new VerifiableProperties(properties));
  }

  // comprehensiveTest() helpers

  /**
   * Comprehensive tests for {@link IndexSegment}.
   * 1. Creates a segment and checks the getters to make sure they return the right values
   * 2. Adds some put entries with random sizes, checks getters again and exercises {@link IndexSegment#find(StoreKey)}
   * and {@link IndexSegment#getEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong)}.
   * 3. Adds some delete entries (deletes some existing put entries and creates deletes for puts not in this segment)
   * and does the same checks as #2.
   * 4. Writes index to a file and loads it sealed and not sealed and does all the checks in #2 once again along with
   * checking that journal entries are populated correctly.
   * @throws IOException
   * @throws StoreException
   */
  private void doComprehensiveTest(short version, boolean includeSmallKeys, boolean includeLargeKeys)
      throws IOException, StoreException {
    String[] logSegmentNames = {LogSegmentNameHelper.generateFirstSegmentName(false), generateRandomLogSegmentName()};
    int valueSize;
    switch (version) {
      case PersistentIndex.VERSION_0:
        valueSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V0;
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        valueSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2;
        break;
      case PersistentIndex.VERSION_3:
        valueSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3;
        break;
      default:
        fail("Unknown PersistentIndex formatVersion");
        valueSize = -1;
    }
    for (String logSegmentName : logSegmentNames) {
      long writeStartOffset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      Offset startOffset = new Offset(logSegmentName, writeStartOffset);
      NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex = new TreeMap<>();
      // advance time so that last modified time for VERSION_1 has different last modified times for different index
      // segments
      time.sleep(10 * Time.MsPerSec);
      PersistentIndex.cleanupIndexSegmentFilesForLogSegment(tempDir.getAbsolutePath(), logSegmentName);
      IndexSegment indexSegment = generateIndexSegment(startOffset, STORE_KEY_FACTORY);
      Pair<StoreKey, PersistentIndex.IndexEntryType> resetKey = null;
      verifyIndexSegmentDetails(indexSegment, startOffset, 0, 0, false, startOffset.getOffset(), time.milliseconds(),
          null);
      int numItems = 10;
      int numSmallKeys = 0;
      int numLargeKeys = 0;
      List<Long> offsets = new ArrayList<>();
      offsets.add(writeStartOffset);
      for (int i = 0; i < numItems - 1; i++) {
        // size has to be > 0 (no record is 0 sized)
        long size = Utils.getRandomLong(TestUtils.RANDOM, 1000) + 1;
        offsets.add(writeStartOffset + size);
        writeStartOffset += size;
        if (i % 3 == 1 && includeSmallKeys) {
          numSmallKeys++;
        }
        if (i % 3 == 2 && includeLargeKeys) {
          numLargeKeys++;
        }
      }
      long lastEntrySize = Utils.getRandomLong(TestUtils.RANDOM, 1000) + 1;
      long endOffset = offsets.get(offsets.size() - 1) + lastEntrySize;
      List<IndexEntry> newEntries =
          addPutEntries(offsets, lastEntrySize, indexSegment, referenceIndex, includeSmallKeys, includeLargeKeys);
      if (version != PersistentIndex.VERSION_0) {
        resetKey = new Pair<>(newEntries.get(0).getKey(), PersistentIndex.IndexEntryType.PUT);
      }
      int expectedSizeWritten =
          SMALLER_KEY_SIZE * numSmallKeys + LARGER_KEY_SIZE * numLargeKeys + KEY_SIZE * (numItems - numSmallKeys
              - numLargeKeys) + numItems * valueSize;
      verifyAllForIndexSegmentFromFile(referenceIndex, indexSegment, startOffset, numItems, expectedSizeWritten, false,
          endOffset, time.milliseconds(), resetKey);

      int extraIdsToTtlUpdate = 5;
      Set<MockId> idsToTtlUpdate = getIdsToTtlUpdate(referenceIndex, extraIdsToTtlUpdate);
      addTtlUpdateEntries(idsToTtlUpdate, indexSegment, referenceIndex);
      endOffset += idsToTtlUpdate.size() * TTL_UPDATE_FILE_SPAN_SIZE;
      numItems += idsToTtlUpdate.size();
      for (MockId id : idsToTtlUpdate) {
        expectedSizeWritten += valueSize + id.sizeInBytes();
      }
      verifyAllForIndexSegmentFromFile(referenceIndex, indexSegment, startOffset, numItems, expectedSizeWritten, false,
          endOffset, time.milliseconds(), resetKey);
      indexSegment.writeIndexSegmentToFile(indexSegment.getEndOffset());
      verifyReadFromFile(referenceIndex, indexSegment.getFile(), startOffset, numItems, expectedSizeWritten, endOffset,
          time.milliseconds(), resetKey);

      int extraIdsToDelete = 5;
      Set<MockId> idsToDelete = getIdsToDelete(referenceIndex, extraIdsToDelete);
      addDeleteEntries(idsToDelete, indexSegment, referenceIndex);
      endOffset += idsToDelete.size() * DELETE_FILE_SPAN_SIZE;
      numItems += idsToDelete.size();
      for (MockId id : idsToDelete) {
        expectedSizeWritten += valueSize + id.sizeInBytes();
      }
      verifyAllForIndexSegmentFromFile(referenceIndex, indexSegment, startOffset, numItems, expectedSizeWritten, false,
          endOffset, time.milliseconds(), resetKey);
      indexSegment.writeIndexSegmentToFile(indexSegment.getEndOffset());
      verifyReadFromFile(referenceIndex, indexSegment.getFile(), startOffset, numItems, expectedSizeWritten, endOffset,
          time.milliseconds(), resetKey);

      // all combinations
      offsets = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        offsets.add(endOffset);
        endOffset += Utils.getRandomLong(TestUtils.RANDOM, 1000) + 1;
        numItems++;
        expectedSizeWritten += valueSize + KEY_SIZE;
      }
      List<IndexEntry> puts =
          addPutEntries(offsets, endOffset - offsets.get(offsets.size() - 1), indexSegment, referenceIndex, false,
              false);
      // the second entry has only a TTL update
      addTtlUpdateEntries(Collections.singleton((MockId) puts.get(1).getKey()), indexSegment, referenceIndex);
      // the third entry has a TTL update and a delete
      addTtlUpdateEntries(Collections.singleton((MockId) puts.get(2).getKey()), indexSegment, referenceIndex);
      addDeleteEntries(Collections.singleton((MockId) puts.get(2).getKey()), indexSegment, referenceIndex);
      // an ID that is only TTL updated
      addTtlUpdateEntries(Collections.singleton(generateIds(referenceIndex, 1).get(0)), indexSegment, referenceIndex);
      // an ID that is TTL updated and deleted
      MockId idToTtlUpdateAndDelete = generateIds(referenceIndex, 1).get(0);
      addTtlUpdateEntries(Collections.singleton(idToTtlUpdateAndDelete), indexSegment, referenceIndex);
      addDeleteEntries(Collections.singleton(idToTtlUpdateAndDelete), indexSegment, referenceIndex);
      // an ID that is only deleted
      addDeleteEntries(Collections.singleton(generateIds(referenceIndex, 1).get(0)), indexSegment, referenceIndex);
      endOffset += 4 * TTL_UPDATE_FILE_SPAN_SIZE + 3 * DELETE_FILE_SPAN_SIZE;
      numItems += 7;
      expectedSizeWritten += 7 * (KEY_SIZE + valueSize);
      verifyAllForIndexSegmentFromFile(referenceIndex, indexSegment, startOffset, numItems, expectedSizeWritten, false,
          endOffset, time.milliseconds(), resetKey);
      indexSegment.writeIndexSegmentToFile(indexSegment.getEndOffset());
      verifyReadFromFile(referenceIndex, indexSegment.getFile(), startOffset, numItems, expectedSizeWritten, endOffset,
          time.milliseconds(), resetKey);

      // verify that flipping StoreConfig.storeIndexMemStateName does not break anything
      IndexMemState saved = config.storeIndexMemState;
      for (IndexMemState state : IndexMemState.values()) {
        if (!state.equals(saved)) {
          setIndexMemState(state);
          verifyReadFromFile(referenceIndex, indexSegment.getFile(), startOffset, numItems, expectedSizeWritten,
              endOffset, time.milliseconds(), resetKey);
        }
      }
      setIndexMemState(saved);
    }
  }

  /**
   * @return a random log segment name.
   */
  private String generateRandomLogSegmentName() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    return LogSegmentNameHelper.getName(pos, gen);
  }

  /**
   * Generates an {@link IndexSegment} for entries from {@code startOffset}.
   * @param startOffset the start {@link Offset} of the {@link IndexSegment}.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use when generating index segment
   * @return an {@link IndexSegment} for entries from {@code startOffset}.
   */
  private IndexSegment generateIndexSegment(Offset startOffset, StoreKeyFactory storeKeyFactory) {
    IndexSegment indexSegment;
    if (formatVersion == PersistentIndex.VERSION_0) {
      indexSegment = new MockIndexSegment(tempDir.getAbsolutePath(), startOffset, storeKeyFactory,
          KEY_SIZE + IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V0, IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V0, config, metrics,
          time, PersistentIndex.VERSION_0);
    } else if (formatVersion == PersistentIndex.VERSION_1) {
      indexSegment = new MockIndexSegment(tempDir.getAbsolutePath(), startOffset, storeKeyFactory,
          KEY_SIZE + IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2, IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2, config,
          metrics, time, formatVersion);
    } else if (formatVersion == PersistentIndex.VERSION_2) {
      indexSegment = new IndexSegment(tempDir.getAbsolutePath(), startOffset, storeKeyFactory,
          KEY_SIZE + IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2, IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2, config,
          metrics, time);
    } else {
      indexSegment = new MockIndexSegment(tempDir.getAbsolutePath(), startOffset, storeKeyFactory,
          KEY_SIZE + IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3, IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3, config, metrics,
          time, formatVersion);
    }
    return indexSegment;
  }

  /**
   * Creates an {@link IndexSegment} from the given {@code file}.
   * @param file the {@link File} to use to build the {@link IndexSegment}.
   * @param sealed {@code true} if the segment needs to be sealed
   * @param journal the {@link Journal} to use.
   * @return an {@link IndexSegment} from the given {@code file}.
   * @throws StoreException
   */
  private IndexSegment createIndexSegmentFromFile(File file, boolean sealed, Journal journal) throws StoreException {
    return new IndexSegment(file, sealed, STORE_KEY_FACTORY, config, metrics, journal, time);
  }

  /**
   * Adds put entries at offsets {@code offsets} into {@code segment}. The {@code offsets} are assumed to be contiguous
   * with no breaks.
   * @param offsets the offsets to add the entries at. Difference b/w two entries will be used to compute size.
   * @param lastEntrySize the size of the last entry in {@code offsets}.
   * @param segment the {@link IndexSegment} to add the entries to.
   * @param referenceIndex the {@link NavigableMap} to add all the entries to. This represents the source of truth for
   *                       all checks.
   * @param includeSmallKeys if true, entries that are added will include small keys.
   * @param includeLargeKeys if true, entries that are added will include large keys.
   * @return {@link List} of {@link IndexEntry}s that were added to the {@link IndexSegment}
   * @throws StoreException
   */
  private List<IndexEntry> addPutEntries(List<Long> offsets, long lastEntrySize, IndexSegment segment,
      NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex, boolean includeSmallKeys, boolean includeLargeKeys)
      throws StoreException {
    List<IndexEntry> addedEntries = new ArrayList<>();
    for (int i = 0; i < offsets.size(); i++) {
      MockId id;
      do {
        if (includeSmallKeys && i % 3 == 1) {
          id = new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE / 2));
        } else if (includeLargeKeys && i % 3 == 2) {
          id = new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE + CUSTOM_ID_SIZE / 2));
        } else {
          id = new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE));
        }
      } while (referenceIndex.containsKey(id));
      long offset = offsets.get(i);
      long size = i == offsets.size() - 1 ? lastEntrySize : offsets.get(i + 1) - offset;
      IndexValue value = IndexValueTest.getIndexValue(size, new Offset(segment.getLogSegmentName(), offset),
          time.milliseconds() + TimeUnit.HOURS.toMillis(1), time.milliseconds(), Utils.getRandomShort(TestUtils.RANDOM),
          Utils.getRandomShort(TestUtils.RANDOM), (short) 0, formatVersion);
      IndexEntry entry = new IndexEntry(id, value);
      segment.addEntry(entry, new Offset(segment.getLogSegmentName(), offset + size));
      addedEntries.add(entry);
      referenceIndex.computeIfAbsent(id, k -> new TreeSet<>()).add(value);
    }
    return addedEntries;
  }

  /**
   * Verifies the getters of the {@link IndexSegment} and makes sure the values returned by them match the reference
   * values.
   * @param indexSegment the {@link IndexSegment} to test.
   * @param startOffset the expected start {@link Offset} of the {@code indexSegment}
   * @param numItems the expected number of items the {@code indexSegment}
   * @param sizeWritten the expected number of bytes written to the {@code indexSegment}
   * @param sealed the expected sealed state of the {@code indexSegment}
   * @param endOffset the expected end offset of the {@code indexSegment}
   * @param lastModifiedTimeInMs the last modified time in ms
   * @param resetKey the reset key for the index segment
   */
  private void verifyIndexSegmentDetails(IndexSegment indexSegment, Offset startOffset, int numItems, int sizeWritten,
      boolean sealed, long endOffset, long lastModifiedTimeInMs,
      Pair<StoreKey, PersistentIndex.IndexEntryType> resetKey) {
    String logSegmentName = startOffset.getName();
    int valueSize;
    switch (formatVersion) {
      case PersistentIndex.VERSION_0:
        valueSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V0;
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        valueSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2;
        break;
      case PersistentIndex.VERSION_3:
        valueSize = IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3;
        break;
      default:
        fail("Unknown PersistentIndex formatVersion");
        valueSize = -1;
    }
    long indexPersistedEntrySize = indexSegment.getPersistedEntrySize();
    assertEquals("LogSegment name not as expected", logSegmentName, indexSegment.getLogSegmentName());
    assertEquals("Start offset not as expected", startOffset, indexSegment.getStartOffset());
    assertEquals("End offset not as expected", new Offset(logSegmentName, endOffset), indexSegment.getEndOffset());
    assertEquals("Sealed state is incorrect", sealed, indexSegment.isSealed());
    assertEquals("Entry size is incorrect: " + formatVersion, indexPersistedEntrySize,
        indexSegment.getPersistedEntrySize());
    assertEquals("Value size is incorrect", valueSize, indexSegment.getValueSize());
    assertEquals("Reset key mismatch ", resetKey, indexSegment.getResetKey());
    if (formatVersion != PersistentIndex.VERSION_0) {
      assertEquals("Last modified time is incorrect", lastModifiedTimeInMs, indexSegment.getLastModifiedTimeMs());
    }
    // incase of formatVersion 0, last modified time is calculated based on SystemTime and hence cannot verify for equivalency
    if (!sealed) {
      assertEquals("Size written not as expected", sizeWritten, indexSegment.getSizeWritten());
      assertEquals("Number of items not as expected", numItems, indexSegment.getNumberOfItems());
    }

    String expectedFilename =
        startOffset.getOffset() + BlobStore.SEPARATOR + IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX;
    if (!logSegmentName.isEmpty()) {
      expectedFilename = logSegmentName + BlobStore.SEPARATOR + expectedFilename;
    }
    String path = tempDir.getAbsolutePath() + File.separator + expectedFilename;
    assertEquals("File path not as expected", path, indexSegment.getFile().getAbsolutePath());
    assertEquals("Segment start offset from filename not as expected", startOffset,
        IndexSegment.getIndexSegmentStartOffset(indexSegment.getFile().getName()));
  }

  /**
   * Verifies {@link IndexSegment#find(StoreKey)} to make sure that it returns/does not return values.
   * @param referenceIndex the index entries to be used as reference.
   * @param segment the {@link IndexSegment} to test
   * @throws StoreException
   */
  private void verifyFind(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex, IndexSegment segment)
      throws StoreException {
    for (Map.Entry<MockId, NavigableSet<IndexValue>> entry : referenceIndex.entrySet()) {
      NavigableSet<IndexValue> referenceValues = entry.getValue();
      NavigableSet<IndexValue> values = segment.find(entry.getKey());
      assertNotNull("Values obtained from segment is null", values);
      IndexValue valueFromSegment = values.first();
      for (IndexValue referenceValue : referenceValues) {
        assertEquals("Offset is not equal", referenceValue.getOffset(), valueFromSegment.getOffset());
        assertEquals("Value is not equal", referenceValue.getBytes(), valueFromSegment.getBytes());
        valueFromSegment = values.higher(valueFromSegment);
      }
      assertNull("There should be no more values in the segment", valueFromSegment);
    }
    // try to find a key that does not exist.
    MockId id = generateIds(referenceIndex, 1).get(0);
    assertNull("Should have failed to find non existent key", segment.find(id));
  }

  /**
   * Verifies {@link IndexSegment#getEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong)} to make sure that
   * it returns the right values for all keys in {@code referenceIndex} and for all conditions.
   * @param referenceIndex the index entries to be used as reference.
   * @param segment the {@link IndexSegment} to test
   * @throws StoreException
   */
  private void verifyGetEntriesSince(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex,
      IndexSegment segment) throws StoreException {
    // index segment is "too" recent
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE, segment.getLastModifiedTimeSecs() - 1);
    List<MessageInfo> entries = new ArrayList<>();
    assertFalse("Should not have fetched entries since segment is too recent",
        segment.getEntriesSince(null, condition, entries, new AtomicLong(0)));
    assertEquals("Should not have fetched entries since segment is too recent", 0, entries.size());

    long sizeLeftInSegment = segment.getEndOffset().getOffset() - segment.getStartOffset().getOffset();
    getEntriesSinceTest(referenceIndex, segment, null, sizeLeftInSegment);
    for (Map.Entry<MockId, NavigableSet<IndexValue>> entry : referenceIndex.entrySet()) {
      for (IndexValue value : entry.getValue()) {
        sizeLeftInSegment -= value.getSize();
      }
      getEntriesSinceTest(referenceIndex, segment, entry.getKey(), sizeLeftInSegment);
    }
    // try to getEntriesSince a key that does not exist.
    MockId id = new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE));
    doGetEntriesSinceTest(referenceIndex, segment, id, Long.MAX_VALUE, 0, null);
  }

  /**
   * Verifies {@link IndexSegment#getEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong)} to make sure that
   * it returns the right values for {@code idToCheck} for all size conditions.
   * @param referenceIndex the index entries to be used as reference.
   * @param segment the {@link IndexSegment} to test
   * @param idToCheck the {@link MockId} to use as input for getEntriesSince().
   * @param sizeLeftInSegment the total size of values in the segment beyond {@code idToCheck}.
   * @throws StoreException
   */
  private void getEntriesSinceTest(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex, IndexSegment segment,
      MockId idToCheck, long sizeLeftInSegment) throws StoreException {
    long maxSize = 0;
    MockId idHigherThanIdToCheck = idToCheck == null ? referenceIndex.firstKey() : referenceIndex.higherKey(idToCheck);
    MockId highestIdIncluded = null;
    while (maxSize <= sizeLeftInSegment) {
      MockId nextHighestIdIncluded =
          highestIdIncluded == null ? idHigherThanIdToCheck : referenceIndex.higherKey(highestIdIncluded);
      long existingSize = 0;
      while (existingSize < maxSize) {
        doGetEntriesSinceTest(referenceIndex, segment, idToCheck, maxSize, existingSize, highestIdIncluded);
        for (IndexValue value : referenceIndex.get(highestIdIncluded)) {
          existingSize += value.getSize();
        }
        highestIdIncluded = referenceIndex.lowerKey(highestIdIncluded);
      }
      doGetEntriesSinceTest(referenceIndex, segment, idToCheck, maxSize, maxSize, null);
      if (nextHighestIdIncluded != null) {
        highestIdIncluded = nextHighestIdIncluded;
        for (IndexValue value : referenceIndex.get(highestIdIncluded)) {
          maxSize += value.getSize();
        }
      } else {
        break;
      }
    }
    highestIdIncluded = referenceIndex.lastKey().equals(idToCheck) ? null : referenceIndex.lastKey();
    // check the case where maxSize is more than the number of entries in the segment
    doGetEntriesSinceTest(referenceIndex, segment, idToCheck, maxSize + 1, 0, highestIdIncluded);
  }

  /**
   * Does the {@link IndexSegment#getEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong)} and checks that
   * all the returned entries are as expected and that all the entries expected have been returned.
   * @param referenceIndex the index entries to be used as reference.
   * @param segment the {@link IndexSegment} to test
   * @param idToCheck the {@link MockId} to check.
   * @param maxSize the parameter for {@link FindEntriesCondition#FindEntriesCondition(long)}.
   * @param existingSize the third parameter for getEntriesSince().
   * @param highestExpectedId the highest expected Id in the returned entries.
   * @throws StoreException
   */
  private void doGetEntriesSinceTest(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex,
      IndexSegment segment, MockId idToCheck, long maxSize, long existingSize, MockId highestExpectedId)
      throws StoreException {
    // test getEntriesSince
    FindEntriesCondition condition = new FindEntriesCondition(maxSize);

    List<MessageInfo> entries = new ArrayList<>();
    assertEquals("Unexpected return value from getEntriesSince()", highestExpectedId != null,
        segment.getEntriesSince(idToCheck, condition, entries, new AtomicLong(existingSize)));
    if (highestExpectedId != null) {
      assertEquals("Highest ID not as expected", highestExpectedId, entries.get(entries.size() - 1).getStoreKey());
      MockId nextExpectedId = idToCheck == null ? referenceIndex.firstKey() : referenceIndex.higherKey(idToCheck);
      for (MessageInfo info : entries) {
        assertEquals("Entry is unexpected", nextExpectedId, info.getStoreKey());
        nextExpectedId = referenceIndex.higherKey(nextExpectedId);
      }
    } else {
      assertEquals("Entries list is not empty", 0, entries.size());
    }

    // test getIndexEntriesSince()
    for (boolean oneEntryPerKey : new boolean[]{true, false}) {
      List<IndexEntry> indexEntries = new ArrayList<>();
      assertEquals("Unexpected return value from getIndexEntriesSince()", highestExpectedId != null,
          segment.getIndexEntriesSince(idToCheck, condition, indexEntries, new AtomicLong(existingSize),
              oneEntryPerKey));
      if (highestExpectedId != null) {
        assertEquals("Highest ID not as expected", highestExpectedId,
            indexEntries.get(indexEntries.size() - 1).getKey());
        MockId nextExpectedId = idToCheck == null ? referenceIndex.firstKey() : referenceIndex.higherKey(idToCheck);
        // gather all index entries that should be there
        final List<IndexEntry> expectedEntries = new ArrayList<>();
        while (nextExpectedId != null) {
          NavigableSet<IndexValue> values = referenceIndex.get(nextExpectedId);
          if (oneEntryPerKey) {
            IndexValue value = values.last();
            if (values.size() > 1 && !value.isDelete() && value.isTtlUpdate()) {
              value = values.first();
            }
            expectedEntries.add(new IndexEntry(nextExpectedId, value));
          } else {
            for (IndexValue value : values) {
              expectedEntries.add(new IndexEntry(nextExpectedId, value));
            }
          }
          if (nextExpectedId.equals(highestExpectedId)) {
            break;
          }
          nextExpectedId = referenceIndex.higherKey(nextExpectedId);
        }
        assertEquals("Number of entries not as expected", expectedEntries.size(), indexEntries.size());
        Iterator<IndexEntry> it = indexEntries.iterator();
        for (IndexEntry expected : expectedEntries) {
          assertTrue("There should be more entries", it.hasNext());
          IndexEntry actual = it.next();
          assertEquals("Key not as expected", expected.getKey(), actual.getKey());
          assertEquals("Value not as expected", expected.getValue().getBytes(), actual.getValue().getBytes());
        }
      } else {
        assertEquals("Entries list is not empty", 0, indexEntries.size());
      }
    }
  }

  /**
   * Gets some IDs for ttl update. Picks the first half of ids from {@code referenceIndex} and also randomly generates
   * {@code outOfSegmentIdCount} ids.
   * @param referenceIndex the index entries to pick for ttl update from.
   * @param outOfSegmentIdCount the number of ids to be generated that are not in {@code referenceIndex}.
   * @return a {@link Set} of IDs to create ttl update entries for.
   */
  private Set<MockId> getIdsToTtlUpdate(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex,
      int outOfSegmentIdCount) {
    Set<MockId> idsToTtlUpdate = new HashSet<>();
    // return the first half of ids in the map
    int needed = referenceIndex.size() / 2;
    int current = 0;
    for (MockId id : referenceIndex.keySet()) {
      if (current >= needed) {
        break;
      }
      idsToTtlUpdate.add(id);
      current++;
    }
    // generate some ids for ttl update
    idsToTtlUpdate.addAll(generateIds(referenceIndex, outOfSegmentIdCount));
    return idsToTtlUpdate;
  }

  /**
   * Gets some IDs for deleting. Picks alternate IDs from {@code referenceIndex} and randomly generates
   * {@code outOfSegmentIdCount} ids.
   * @param referenceIndex the index entries to pick for delete from.
   * @param outOfSegmentIdCount the number of ids to be generated that are not in {@code referenceIndex}.
   * @return a {@link Set} of IDs to create delete entries for.
   */
  private Set<MockId> getIdsToDelete(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex,
      int outOfSegmentIdCount) {
    Set<MockId> idsToDelete = new HashSet<>();
    // return every alternate id in the map
    boolean include = true;
    for (MockId id : referenceIndex.keySet()) {
      if (include) {
        idsToDelete.add(id);
      }
      include = !include;
    }
    // generate some ids for delete
    idsToDelete.addAll(generateIds(referenceIndex, outOfSegmentIdCount));
    return idsToDelete;
  }

  /**
   * Generates {@code count} unique ids that arent in {@code referenceIndex}.
   * @param referenceIndex the current reference index
   * @param count the number of unique ids to generate
   * @return a set with {@code count} unique ids
   */
  private List<MockId> generateIds(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex, int count) {
    List<MockId> generatedIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      MockId id;
      do {
        id = new MockId(TestUtils.getRandomString(CUSTOM_ID_SIZE));
      } while (referenceIndex.containsKey(id) || generatedIds.contains(id));
      generatedIds.add(id);
    }
    return generatedIds;
  }

  /**
   * Adds ttl update entries to {@code segment}.
   * @param idsToTtlUpdate the {@link Set} of IDs to create ttl update entries for.
   * @param segment the {@link IndexSegment} to add the entries to.
   * @param referenceIndex the {@link NavigableMap} to add all the entries to. This represents the source of truth for
   *                       all checks.
   * @throws StoreException
   */
  private void addTtlUpdateEntries(Set<MockId> idsToTtlUpdate, IndexSegment segment,
      NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex) throws StoreException {
    for (MockId id : idsToTtlUpdate) {
      Offset offset = segment.getEndOffset();
      NavigableSet<IndexValue> values = segment.find(id);
      IndexValue value;
      if (values == null) {
        // create an index value with a random log segment name
        value = IndexValueTest.getIndexValue(1, new Offset(TestUtils.getRandomString(1), 0), Utils.Infinite_Time,
            time.milliseconds(), id.getAccountId(), id.getContainerId(), (short) 1, formatVersion);
      } else if (values.last().isDelete()) {
        throw new IllegalArgumentException(id + " is deleted");
      } else if (values.last().isTtlUpdate()) {
        throw new IllegalArgumentException("TTL of " + id + " is already updated");
      } else {
        value = values.last();
      }
      IndexValue newValue = IndexValueTest.getIndexValue(value, formatVersion);
      newValue.setFlag(IndexValue.Flags.Ttl_Update_Index);
      newValue.setExpiresAtMs(Utils.Infinite_Time);
      newValue.setNewOffset(offset);
      newValue.setNewSize(TTL_UPDATE_FILE_SPAN_SIZE);
      segment.addEntry(new IndexEntry(id, newValue),
          new Offset(offset.getName(), offset.getOffset() + TTL_UPDATE_FILE_SPAN_SIZE));
      referenceIndex.computeIfAbsent(id, k -> new TreeSet<>()).add(newValue);
    }
  }

  /**
   * Adds delete entries to {@code segment.}
   * @param idsToDelete the {@link Set} of IDs to create delete entries for.
   * @param segment the {@link IndexSegment} to add the entries to.
   * @param referenceIndex the {@link NavigableMap} to add all the entries to. This repreents the source of truth for
   *                       all checks.
   * @throws StoreException
   */
  private void addDeleteEntries(Set<MockId> idsToDelete, IndexSegment segment,
      NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex) throws StoreException {
    for (MockId id : idsToDelete) {
      Offset offset = segment.getEndOffset();
      NavigableSet<IndexValue> values = segment.find(id);
      IndexValue value;
      if (values == null) {
        // create an index value with a random log segment name
        value = IndexValueTest.getIndexValue(1, new Offset(TestUtils.getRandomString(1), 0), Utils.Infinite_Time,
            time.milliseconds(), Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM),
            (short) 0, formatVersion);
      } else if (values.last().isDelete()) {
        throw new IllegalArgumentException(id + " is already deleted");
      } else {
        value = values.first();
        // update the expiration time if required
        if (values.size() > 1 && value.getExpiresAtMs() != values.last().getExpiresAtMs()) {
          value.setFlag(IndexValue.Flags.Ttl_Update_Index);
          value.setExpiresAtMs(values.last().getExpiresAtMs());
        }
      }
      IndexValue newValue = IndexValueTest.getIndexValue(value, formatVersion);
      newValue.setFlag(IndexValue.Flags.Delete_Index);
      newValue.setNewOffset(offset);
      newValue.setNewSize(DELETE_FILE_SPAN_SIZE);
      segment.addEntry(new IndexEntry(id, newValue),
          new Offset(offset.getName(), offset.getOffset() + DELETE_FILE_SPAN_SIZE));
      referenceIndex.computeIfAbsent(id, k -> new TreeSet<>()).add(newValue);
    }
  }

  /**
   * Creates an {@link IndexSegment} from the given {@code file} and checks for both sanity and find operations.
   * @param referenceIndex the index entries to be used as reference.
   * @param file the {@link File} to be used to load the index.
   * @param startOffset the expected start {@link Offset} of the {@link IndexSegment}
   * @param numItems the expected number of items the {@code indexSegment}
   * @param expectedSizeWritten the expected number of bytes written to the {@code indexSegment}
   * @param endOffset the expected end offset of the {@code indexSegment}
   * @param lastModifiedTimeInMs the last modified time of the index segment in ms
   * @param resetKey the resetKey of the index segment
   * @throws StoreException
   */
  private void verifyReadFromFile(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex, File file,
      Offset startOffset, int numItems, int expectedSizeWritten, long endOffset, long lastModifiedTimeInMs,
      Pair<StoreKey, PersistentIndex.IndexEntryType> resetKey) throws StoreException {
    // read from file (not sealed) and verify that everything is ok
    Journal journal = new Journal(tempDir.getAbsolutePath(), Integer.MAX_VALUE, Integer.MAX_VALUE);
    IndexSegment fromDisk = createIndexSegmentFromFile(file, false, journal);
    verifyAllForIndexSegmentFromFile(referenceIndex, fromDisk, startOffset, numItems, expectedSizeWritten, false,
        endOffset, lastModifiedTimeInMs, resetKey);
    // journal should contain all the entries
    verifyJournal(referenceIndex, journal);
    File bloomFile = new File(file.getParent(),
        IndexSegment.generateIndexSegmentFilenamePrefix(startOffset) + BLOOM_FILE_NAME_SUFFIX);
    fromDisk.seal();
    assertTrue("Bloom file does not exist", bloomFile.exists());
    verifyAllForIndexSegmentFromFile(referenceIndex, fromDisk, startOffset, numItems, expectedSizeWritten, true,
        endOffset, lastModifiedTimeInMs, resetKey);

    // read from file (sealed) and verify that everything is ok
    journal = new Journal(tempDir.getAbsolutePath(), Integer.MAX_VALUE, Integer.MAX_VALUE);
    fromDisk = createIndexSegmentFromFile(file, true, journal);
    verifyAllForIndexSegmentFromFile(referenceIndex, fromDisk, startOffset, numItems, expectedSizeWritten, true,
        endOffset, lastModifiedTimeInMs, resetKey);
    // journal should not contain any entries
    assertNull("Journal should not have any entries", journal.getFirstOffset());

    // test bloom file recreation
    // delete the bloom file
    assertTrue("File could not be deleted", bloomFile.delete());

    // read from file (sealed) again and verify that everything is ok
    journal = new Journal(tempDir.getAbsolutePath(), Integer.MAX_VALUE, Integer.MAX_VALUE);
    fromDisk = createIndexSegmentFromFile(file, true, journal);
    assertTrue("Bloom file does not exist", bloomFile.exists());
    verifyAllForIndexSegmentFromFile(referenceIndex, fromDisk, startOffset, numItems, expectedSizeWritten, true,
        endOffset, lastModifiedTimeInMs, resetKey);
    // journal should not contain any entries
    assertNull("Journal should not have any entries", journal.getFirstOffset());
  }

  /**
   * Verify all for an {@link IndexSegment} created from the given {@code file} in terms of both sanity and find operations.
   * @param referenceIndex the index entries to be used as reference.
   * @param fromDisk the {@link IndexSegment} created from file.
   * @param startOffset the expected start {@link Offset} of the {@link IndexSegment}
   * @param numItems the expected number of items the {@code indexSegment}
   * @param expectedSizeWritten the expected number of bytes written to the {@code indexSegment}
   * @param sealed the expected sealed state of the {@code indexSegment}
   * @param endOffset the expected end offset of the {@code indexSegment}
   * @param lastModifiedTimeInMs the last modified time of the index segment in ms
   * @param resetKey the resetKey of the index segment
   * @throws StoreException
   */
  private void verifyAllForIndexSegmentFromFile(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex,
      IndexSegment fromDisk, Offset startOffset, int numItems, int expectedSizeWritten, boolean sealed, long endOffset,
      long lastModifiedTimeInMs, Pair<StoreKey, PersistentIndex.IndexEntryType> resetKey) throws StoreException {
    verifyIndexSegmentDetails(fromDisk, startOffset, numItems, expectedSizeWritten, sealed, endOffset,
        lastModifiedTimeInMs, resetKey);
    verifyFind(referenceIndex, fromDisk);
    verifyGetEntriesSince(referenceIndex, fromDisk);
  }

  /**
   * Verfies that the journal has all and only expected entries.
   * @param referenceIndex the index entries to be used as reference.
   * @param journal the {@link Journal} to check.
   */
  private void verifyJournal(NavigableMap<MockId, NavigableSet<IndexValue>> referenceIndex, Journal journal) {
    // order all available IndexValue by offset
    final TreeMap<Offset, MockId> allIdsByOffset = new TreeMap<>();
    referenceIndex.forEach(
        (key, value) -> value.forEach(indexValue -> allIdsByOffset.put(indexValue.getOffset(), key)));
    List<JournalEntry> entries = journal.getEntriesSince(allIdsByOffset.firstKey(), true);
    assertEquals("Size of entries returned from journal not as expected", allIdsByOffset.size(), entries.size());
    for (JournalEntry entry : entries) {
      StoreKey key = entry.getKey();
      Offset offset = entry.getOffset();
      assertEquals("Key not as expected", key, allIdsByOffset.get(offset));
    }
  }

  // partialWriteTest() helpers

  /**
   * Checks that an index segment file is not created
   * @param indexSegment the index segment to write to file
   * @param safeEndPoint the safe end point to use to the call to {@link IndexSegment#writeIndexSegmentToFile(Offset)}.
   * @throws IOException
   * @throws StoreException
   */
  private void checkNonCreationOfIndexSegmentFile(IndexSegment indexSegment, Offset safeEndPoint)
      throws IOException, StoreException {
    indexSegment.writeIndexSegmentToFile(safeEndPoint);
    assertFalse("Index file should not have been created", indexSegment.getFile().exists());
  }

  /**
   * Verifies that the values obtained for {@code id} from {@code segment} satisfy the count and deleted state as
   * provided
   * @param segment the {@link IndexSegment} to check
   * @param id the {@link MockId} to find values for
   * @param valueCount the number of values expected to be returned
   * @param flags the expected flags for the latest value
   * @throws StoreException
   */
  private void verifyValues(IndexSegment segment, MockId id, int valueCount, EnumSet<IndexValue.Flags> flags)
      throws StoreException {
    NavigableSet<IndexValue> values = segment.find(id);
    assertNotNull("Values should have been found for " + id, values);
    assertEquals("Unexpected number of values for " + id, valueCount, values.size());
    for (IndexValue.Flags flagToCheck : IndexValue.Flags.values()) {
      assertEquals("Flag " + flagToCheck + " status not as expected", flags.contains(flagToCheck),
          values.last().isFlagSet(flagToCheck));
    }
  }
}

/**
 * Mock {@link IndexSegment} that uses non-{@link PersistentIndex#CURRENT_VERSION}
 */
class MockIndexSegment extends IndexSegment {
  private final int persistedEntrySizeIfV0V1;
  private short persistentIndexVersion;

  MockIndexSegment(String dataDir, Offset startOffset, StoreKeyFactory factory, int entrySize, int valueSize,
      StoreConfig config, StoreMetrics metrics, Time time, short persistentIndexVersion) {
    super(dataDir, startOffset, factory, entrySize, valueSize, config, metrics, time);
    this.persistentIndexVersion = persistentIndexVersion;
    persistedEntrySizeIfV0V1 = entrySize;
  }

  @Override
  short getVersion() {
    return persistentIndexVersion;
  }

  @Override
  void setVersion(short version) {
    persistentIndexVersion = version;
  }

  @Override
  int getPersistedEntrySize() {
    if (getVersion() <= PersistentIndex.VERSION_1) {
      return persistedEntrySizeIfV0V1;
    } else {
      return super.getPersistedEntrySize();
    }
  }

  @Override
  Pair<StoreKey, PersistentIndex.IndexEntryType> getResetKey() {
    if (getVersion() == PersistentIndex.VERSION_0) {
      return null;
    } else {
      return super.getResetKey();
    }
  }
}

