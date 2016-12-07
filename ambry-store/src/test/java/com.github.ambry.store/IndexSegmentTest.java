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
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link IndexSegment}.s
 */
public class IndexSegmentTest {
  private static final int CUSTOM_ID_SIZE = 10;
  private static final int KEY_SIZE = new MockId(UtilsTest.getRandomString(CUSTOM_ID_SIZE)).sizeInBytes();
  private static final int VALUE_SIZE = new IndexValue(0, new Offset("", 0)).getBytes().capacity();
  private static final StoreConfig STORE_CONFIG = new StoreConfig(new VerifiableProperties(new Properties()));
  private static final long DELETE_FILE_SPAN_SIZE = 10;
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

  /**
   * Creates a temporary directory and sets up metrics.
   * @throws IOException
   */
  public IndexSegmentTest() throws IOException {
    tempDir = StoreTestUtils.createTempDirectory("indexSegmentDir-" + UtilsTest.getRandomString(10));
    metrics = new StoreMetrics(tempDir.getAbsolutePath(), new MetricRegistry());
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
   * Comprehensive tests for {@link IndexSegment}.
   * 1. Creates a segment and checks the getters to make sure they return the right values
   * 2. Adds some put entries with random sizes, checks getters again and exercises {@link IndexSegment#find(StoreKey)}
   * and {@link IndexSegment#getEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong)}.
   * 3. Adds some delete entries (deletes some existing put entries and creates deletes for puts not in this segment)
   * and does the same checks as #2.
   * 4. Writes index to a file and loads it mapped and unmapped and does all the checks in #2 once again along with
   * checking that journal entries are populated correctly.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void comprehensiveTest() throws IOException, StoreException {
    String[] logSegmentNames = {LogSegmentNameHelper.generateFirstSegmentName(1), generateRandomLogSegmentName()};
    for (String logSegmentName : logSegmentNames) {
      long writeStartOffset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      Offset startOffset = new Offset(logSegmentName, writeStartOffset);
      NavigableMap<MockId, IndexValue> referenceIndex = new TreeMap<>();
      IndexSegment indexSegment = generateIndexSegment(startOffset);
      verifyIndexSegmentDetails(indexSegment, startOffset, 0, false, startOffset.getOffset());
      int numItems = 10;
      List<Long> offsets = new ArrayList<>();
      offsets.add(writeStartOffset);
      for (int i = 0; i < numItems - 1; i++) {
        // size has to be > 0 (no record is 0 sized)
        long size = Utils.getRandomLong(TestUtils.RANDOM, 1000) + 1;
        offsets.add(writeStartOffset + size);
        writeStartOffset += size;
      }
      long lastEntrySize = Utils.getRandomLong(TestUtils.RANDOM, 1000) + 1;
      long endOffset = offsets.get(offsets.size() - 1) + lastEntrySize;
      addPutEntries(offsets, lastEntrySize, indexSegment, referenceIndex);
      verifyIndexSegmentDetails(indexSegment, startOffset, offsets.size(), false, endOffset);
      verifyFind(referenceIndex, indexSegment);
      verifyGetEntriesSince(referenceIndex, indexSegment);

      int extraIdsToDelete = 10;
      Set<MockId> idsToDelete = getIdsToDelete(referenceIndex, extraIdsToDelete);
      Map<Offset, MockId> extraOffsetsToCheck = addDeleteEntries(idsToDelete, indexSegment, referenceIndex);
      endOffset += idsToDelete.size() * DELETE_FILE_SPAN_SIZE;
      numItems += extraIdsToDelete;
      verifyIndexSegmentDetails(indexSegment, startOffset, numItems, false, endOffset);
      verifyFind(referenceIndex, indexSegment);
      verifyGetEntriesSince(referenceIndex, indexSegment);

      // write to file
      indexSegment.writeIndexSegmentToFile(indexSegment.getEndOffset());
      verifyReadFromFile(referenceIndex, indexSegment.getFile(), startOffset, numItems, endOffset, extraOffsetsToCheck);
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
    MockId id1 = new MockId("id1");
    MockId id2 = new MockId("id2");
    MockId id3 = new MockId("id3");
    IndexValue value1 = new IndexValue(1000, new Offset(logSegmentName, 0), (byte) 0, Utils.Infinite_Time);
    IndexValue value2 = new IndexValue(1000, new Offset(logSegmentName, 1000), (byte) 0, Utils.Infinite_Time);
    IndexValue value3 = new IndexValue(1000, new Offset(logSegmentName, 2000), (byte) 0, Utils.Infinite_Time);
    IndexSegment indexSegment = generateIndexSegment(startOffset);
    // inserting in the opposite order by design to ensure that writes are based on offset ordering and not key ordering
    indexSegment.addEntry(new IndexEntry(id3, value1), new Offset(logSegmentName, 1000));
    indexSegment.addEntry(new IndexEntry(id2, value2), new Offset(logSegmentName, 2000));
    indexSegment.addEntry(new IndexEntry(id1, value3), new Offset(logSegmentName, 3000));

    // provide end offset such that nothing is written
    indexSegment.writeIndexSegmentToFile(new Offset(prevLogSegmentName, 0));
    assertFalse("Index file should not have been created", indexSegment.getFile().exists());
    List<MockId> shouldBeFound = new ArrayList<>();
    List<MockId> shouldNotBeFound = new ArrayList<>(Arrays.asList(id3, id2, id1));
    for (int safeEndPoint = 1000; safeEndPoint <= 3000; safeEndPoint += 1000) {
      shouldBeFound.add(shouldNotBeFound.remove(0));
      // repeat twice
      for (int i = 0; i < 2; i++) {
        indexSegment.writeIndexSegmentToFile(new Offset(logSegmentName, safeEndPoint));
        Journal journal = new Journal(tempDir.getAbsolutePath(), 3, 3);
        IndexSegment fromDisk =
            new IndexSegment(indexSegment.getFile(), false, STORE_KEY_FACTORY, STORE_CONFIG, metrics, journal);
        for (MockId id : shouldBeFound) {
          assertNotNull("Value for key should have been found", fromDisk.find(id));
        }
        for (MockId id : shouldNotBeFound) {
          assertNull("Value for key should not have been found", fromDisk.find(id));
        }
      }
    }
  }

  // helpers
  // comprehensiveTest() helpers

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
   * @returnn an {@link IndexSegment} for entries from {@code startOffset}.
   */
  private IndexSegment generateIndexSegment(Offset startOffset) {
    return new IndexSegment(tempDir.getAbsolutePath(), startOffset, STORE_KEY_FACTORY, KEY_SIZE, VALUE_SIZE,
        STORE_CONFIG, metrics);
  }

  /**
   * Creates an {@link IndexSegment} from the given {@code file}.
   * @param file the {@link File} to use to build the {@link IndexSegment}.
   * @param isMapped {@code true} if the segment needs to be mmapped, {@code false} for holding entries in memory.
   * @param journal the {@link Journal} to use.
   * @return an {@link IndexSegment} from the given {@code file}.
   * @throws StoreException
   */
  private IndexSegment createIndexSegmentFromFile(File file, boolean isMapped, Journal journal) throws StoreException {
    return new IndexSegment(file, isMapped, STORE_KEY_FACTORY, STORE_CONFIG, metrics, journal);
  }

  /**
   * Adds put entries at offsets {@code offsets} into {@code segment}. The {@code offsets} are assumed to be contiguous
   * with no breaks.
   * @param offsets the offsets to add the entries at. Difference b/w two entries will be used to compute size.
   * @param lastEntrySize the size of the last entry in {@code offsets}.
   * @param segment the {@link IndexSegment} to add the entries to.
   * @param referenceIndex the {@link NavigableMap} to add all the entries to. This repreents the source of truth for
   *                       all checks.
   * @throws StoreException
   */
  private void addPutEntries(List<Long> offsets, long lastEntrySize, IndexSegment segment,
      NavigableMap<MockId, IndexValue> referenceIndex) throws StoreException {
    for (int i = 0; i < offsets.size(); i++) {
      MockId id;
      do {
        id = new MockId(UtilsTest.getRandomString(CUSTOM_ID_SIZE));
      } while (referenceIndex.containsKey(id));
      long offset = offsets.get(i);
      long size = i == offsets.size() - 1 ? lastEntrySize : offsets.get(i + 1) - offset;
      IndexValue value =
          new IndexValue(size, new Offset(segment.getLogSegmentName(), offset), (byte) 0, Utils.Infinite_Time);
      segment.addEntry(new IndexEntry(id, value), new Offset(segment.getLogSegmentName(), offset + size));
      referenceIndex.put(id, value);
    }
  }

  /**
   * Verifies the getters of the {@link IndexSegment} and makes sure the values returned by them match the reference
   * values.
   * @param indexSegment the {@link IndexSegment} to test.
   * @param startOffset the expected start {@link Offset} of the {@code indexSegment}
   * @param numItems the expected numner of items the {@code indexSegment}
   * @param isMapped the expected mapped state of the {@code indexSegment}
   * @param endOffset the expected end offset of the {@code indexSegment}
   */
  private void verifyIndexSegmentDetails(IndexSegment indexSegment, Offset startOffset, int numItems, boolean isMapped,
      long endOffset) {
    String logSegmentName = startOffset.getName();
    long indexEntrySize = KEY_SIZE + VALUE_SIZE;
    assertEquals("LogSegment name not as expected", logSegmentName, indexSegment.getLogSegmentName());
    assertEquals("Start offset not as expected", startOffset, indexSegment.getStartOffset());
    assertEquals("End offset not as expected", new Offset(logSegmentName, endOffset), indexSegment.getEndOffset());
    assertEquals("Mapped state is incorrect", isMapped, indexSegment.isMapped());
    assertEquals("Key size is incorrect", KEY_SIZE, indexSegment.getKeySize());
    assertEquals("Value size is incorrect", VALUE_SIZE, indexSegment.getValueSize());
    if (!isMapped) {
      assertEquals("Size written not as expected", indexEntrySize * numItems, indexSegment.getSizeWritten());
      assertEquals("Number of items not as expected", numItems, indexSegment.getNumberOfItems());
    }

    String expectedFilename =
        startOffset.getOffset() + BlobStore.SEPARATOR + PersistentIndex.INDEX_SEGMENT_FILE_NAME_SUFFIX;
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
  private void verifyFind(NavigableMap<MockId, IndexValue> referenceIndex, IndexSegment segment) throws StoreException {
    for (Map.Entry<MockId, IndexValue> entry : referenceIndex.entrySet()) {
      IndexValue referenceValue = entry.getValue();
      IndexValue valueFromSegment = segment.find(entry.getKey());
      assertNotNull("Value obtained from segment is null", valueFromSegment);
      assertEquals("Offset is not equal", referenceValue.getOffset(), valueFromSegment.getOffset());
      assertEquals("Value is not equal", referenceValue.getBytes(), valueFromSegment.getBytes());
    }
    // try to find a key that does not exist.
    MockId id = new MockId(UtilsTest.getRandomString(CUSTOM_ID_SIZE));
    assertNull("Should have failed to find non existent key", segment.find(id));
  }

  /**
   * Verifies {@link IndexSegment#getEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong)} to make sure that
   * it returns the right values for all keys in {@code referenceIndex} and for all conditions.
   * @param referenceIndex the index entries to be used as reference.
   * @param segment the {@link IndexSegment} to test
   * @throws IOException
   * @throws StoreException
   */
  private void verifyGetEntriesSince(NavigableMap<MockId, IndexValue> referenceIndex, IndexSegment segment)
      throws IOException, StoreException {
    // index segment is "too" recent
    FindEntriesCondition condition = new FindEntriesCondition(Long.MAX_VALUE, segment.getLastModifiedTime() - 1);
    List<MessageInfo> entries = new ArrayList<>();
    assertFalse("Should not have fetched entries since segment is too recent",
        segment.getEntriesSince(null, condition, entries, new AtomicLong(0)));
    assertEquals("Should not have fetched entries since segment is too recent", 0, entries.size());

    long sizeLeftInSegment = segment.getEndOffset().getOffset() - segment.getStartOffset().getOffset();
    getEntriesSinceTest(referenceIndex, segment, null, sizeLeftInSegment);
    for (Map.Entry<MockId, IndexValue> entry : referenceIndex.entrySet()) {
      sizeLeftInSegment -= entry.getValue().getSize();
      getEntriesSinceTest(referenceIndex, segment, entry.getKey(), sizeLeftInSegment);
    }
  }

  /**
   * Verifies {@link IndexSegment#getEntriesSince(StoreKey, FindEntriesCondition, List, AtomicLong)} to make sure that
   * it returns the right values for {@code idToCheck} for all size conditions.
   * @param referenceIndex the index entries to be used as reference.
   * @param segment the {@link IndexSegment} to test
   * @param idToCheck the {@link MockId} to use as input for getEntriesSince().
   * @param sizeLeftInSegment the total size of values in the segment beyond {@code idToCheck}.
   * @throws IOException
   */
  private void getEntriesSinceTest(NavigableMap<MockId, IndexValue> referenceIndex, IndexSegment segment,
      MockId idToCheck, long sizeLeftInSegment) throws IOException {
    long maxSize = 0;
    MockId idHigherThanIdToCheck = idToCheck == null ? referenceIndex.firstKey() : referenceIndex.higherKey(idToCheck);
    MockId highestIdIncluded = null;
    while (maxSize <= sizeLeftInSegment) {
      MockId nextHighestIdIncluded =
          highestIdIncluded == null ? idHigherThanIdToCheck : referenceIndex.higherKey(highestIdIncluded);
      long existingSize = 0;
      while (existingSize < maxSize) {
        doGetEntriesSinceTest(referenceIndex, segment, idToCheck, maxSize, existingSize, highestIdIncluded);
        existingSize += referenceIndex.get(highestIdIncluded).getSize();
        highestIdIncluded = referenceIndex.lowerKey(highestIdIncluded);
      }
      doGetEntriesSinceTest(referenceIndex, segment, idToCheck, maxSize, maxSize, null);
      if (nextHighestIdIncluded != null) {
        highestIdIncluded = nextHighestIdIncluded;
        maxSize += referenceIndex.get(highestIdIncluded).getSize();
      } else {
        break;
      }
    }
    highestIdIncluded = referenceIndex.lastKey().equals(idToCheck) ? null : referenceIndex.lastKey();
    // check the case where maxSize is more than the number of entries in the segment
    doGetEntriesSinceTest(referenceIndex, segment, idToCheck, maxSize + 1, 0, highestIdIncluded);
    // try to getEntriesSince a key that does not exist.
    MockId id = new MockId(UtilsTest.getRandomString(CUSTOM_ID_SIZE));
    doGetEntriesSinceTest(referenceIndex, segment, id, Long.MAX_VALUE, 0, null);
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
   * @throws IOException
   */
  private void doGetEntriesSinceTest(NavigableMap<MockId, IndexValue> referenceIndex, IndexSegment segment,
      MockId idToCheck, long maxSize, long existingSize, MockId highestExpectedId) throws IOException {
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
  }

  /**
   * Gets some IDs for deleting. Picks alternate IDs from {@code referenceIndex} and randomly generates
   * {@code outOfSegmentIdCount} ids.
   * @param referenceIndex the index entries to pick for delete from.
   * @param outOfSegmentIdCount the number of ids to be generated that are not in {@code referenceIndex}.
   * @return a {@link Set} of IDs to create delete entries for.
   */
  private Set<MockId> getIdsToDelete(NavigableMap<MockId, IndexValue> referenceIndex, int outOfSegmentIdCount) {
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
    for (int i = 0; i < outOfSegmentIdCount; i++) {
      MockId id;
      do {
        id = new MockId(UtilsTest.getRandomString(CUSTOM_ID_SIZE));
      } while (idsToDelete.contains(id));
      idsToDelete.add(id);
    }
    return idsToDelete;
  }

  /**
   * Adds delete entries to {@code segment.}
   * @param idsToDelete the {@link Set} of IDs to create delete entries for.
   * @param segment the {@link IndexSegment} to add the entries to.
   * @param referenceIndex the {@link NavigableMap} to add all the entries to. This repreents the source of truth for
   *                       all checks.
   * @return a {@link Map} that defines the put record offsets of keys whose index entries have been replaced by delete
   * entries owing to the fact that the put and delete both occurred in the same index segment.
   * @throws StoreException
   */
  private Map<Offset, MockId> addDeleteEntries(Set<MockId> idsToDelete, IndexSegment segment,
      NavigableMap<MockId, IndexValue> referenceIndex) throws StoreException {
    Map<Offset, MockId> putRecordOffsets = new HashMap<>();
    for (MockId id : idsToDelete) {
      Offset offset = segment.getEndOffset();
      IndexValue value = segment.find(id);
      if (value == null) {
        // create an index value with a random log segment name
        value = new IndexValue(1, new Offset(UtilsTest.getRandomString(1), 0), (byte) 0, Utils.Infinite_Time);
      } else {
        // if in this segment, add to putRecordOffsets so that journal can verify these later
        putRecordOffsets.put(value.getOffset(), id);
      }
      IndexValue newValue =
          new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs());
      newValue.setFlag(IndexValue.Flags.Delete_Index);
      newValue.setNewOffset(offset);
      newValue.setNewSize(DELETE_FILE_SPAN_SIZE);
      segment.addEntry(new IndexEntry(id, newValue),
          new Offset(offset.getName(), offset.getOffset() + DELETE_FILE_SPAN_SIZE));
      referenceIndex.put(id, newValue);
    }
    return putRecordOffsets;
  }

  /**
   * Creates an {@link IndexSegment} from the given {@code file} and checks for both sanity and find operations.
   * @param referenceIndex the index entries to be used as reference.
   * @param file the {@link File} to be used to load the index.
   * @param startOffset the expected start {@link Offset} of the {@link IndexSegment}
   * @param numItems the expected numner of items the {@code indexSegment}
   * @param endOffset the expected end offset of the {@code indexSegment}
   * @param extraOffsetsToCheck a {@link Map} that defines the put record offsets of keys whose presence needs to be
   *                            verified in the {@link Journal}.
   * @throws IOException
   * @throws StoreException
   */
  private void verifyReadFromFile(NavigableMap<MockId, IndexValue> referenceIndex, File file, Offset startOffset,
      int numItems, long endOffset, Map<Offset, MockId> extraOffsetsToCheck) throws IOException, StoreException {
    // read from file (unmapped) and verify that everything is ok
    Journal journal = new Journal(tempDir.getAbsolutePath(), Integer.MAX_VALUE, Integer.MAX_VALUE);
    IndexSegment fromDisk = createIndexSegmentFromFile(file, false, journal);
    verifyIndexSegmentDetails(fromDisk, startOffset, numItems, false, endOffset);
    verifyFind(referenceIndex, fromDisk);
    verifyGetEntriesSince(referenceIndex, fromDisk);
    // journal should contain all the entries
    verifyJournal(referenceIndex, startOffset, journal, extraOffsetsToCheck);
    fromDisk.map(true);

    // read from file (mapped) and verify that everything is ok
    journal = new Journal(tempDir.getAbsolutePath(), Integer.MAX_VALUE, Integer.MAX_VALUE);
    fromDisk = createIndexSegmentFromFile(file, true, journal);
    verifyIndexSegmentDetails(fromDisk, startOffset, numItems, true, endOffset);
    verifyFind(referenceIndex, fromDisk);
    verifyGetEntriesSince(referenceIndex, fromDisk);
    // journal should not contain any entries
    assertNull("Journal should not have any entries", journal.getFirstOffset());
  }

  /**
   * Verfies that the journal has all and only expected entries.
   * @param referenceIndex the index entries to be used as reference.
   * @param indexSegmentStartOffset the start offset of the {@link IndexSegment} that filled the journal.
   * @param journal the {@link Journal} to check.
   * @param extraOffsetsToCheck a {@link Map} that defines the put record offsets of keys whose presence needs to be
   *                            verified in the {@link Journal}.
   */
  private void verifyJournal(NavigableMap<MockId, IndexValue> referenceIndex, Offset indexSegmentStartOffset,
      Journal journal, Map<Offset, MockId> extraOffsetsToCheck) {
    Set<StoreKey> seenKeys = new TreeSet<>();
    Map<Offset, Boolean> extraEntriesCheckState = null;
    if (extraOffsetsToCheck != null) {
      extraEntriesCheckState = new HashMap<>();
      for (Map.Entry<Offset, MockId> extra : extraOffsetsToCheck.entrySet()) {
        extraEntriesCheckState.put(extra.getKey(), false);
      }
    }
    List<JournalEntry> entries = journal.getEntriesSince(indexSegmentStartOffset, true);
    for (JournalEntry entry : entries) {
      StoreKey key = entry.getKey();
      Offset offset = entry.getOffset();
      seenKeys.add(key);
      if (extraOffsetsToCheck != null && extraOffsetsToCheck.containsKey(offset) && extraOffsetsToCheck.get(offset)
          .equals(key)) {
        extraEntriesCheckState.put(offset, true);
      }
    }
    assertEquals("Keys seen does not match keys in reference index", seenKeys.size(), referenceIndex.size());
    seenKeys.containsAll(referenceIndex.keySet());
    if (extraEntriesCheckState != null) {
      assertFalse("One of the extraOffsetsToCheck was not found", extraEntriesCheckState.values().contains(false));
    }
  }
}
