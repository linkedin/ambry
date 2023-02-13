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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.CuratedLogIndexState.*;
import static com.github.ambry.store.StoreFindToken.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link PersistentIndex}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class IndexTest {

  private final boolean isLogSegmented;
  private final File tempDir;
  private final File tokenTestDir;
  private CuratedLogIndexState state;
  private final CuratedLogIndexState stateForTokenTest;
  private final short persistentIndexVersion;
  private final boolean addUndeletes;
  private static final String COMPACT_POLICY_INFO_FILE_NAME_V2 = "compactionPolicyInfoV2.json";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(IndexTest.class);

  // TODO: test that verifies that files with "_index" are not picked up if the corresponding log segment is not in log

  /**
   * Running for both segmented and non-segmented log.
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{false, PersistentIndex.VERSION_2, false}, {true, PersistentIndex.VERSION_2, false},
            {true, PersistentIndex.VERSION_3, true}, {true, PersistentIndex.VERSION_3, false},
            {true, PersistentIndex.VERSION_4, true}, {true, PersistentIndex.VERSION_4, false}});
  }

  /**
   * Creates a temporary directory and sets up some test state.
   * @throws IOException
   * @throws StoreException
   */
  public IndexTest(boolean isLogSegmented, short persistentIndexVersion, boolean addUndeletes)
      throws IOException, StoreException {
    this.isLogSegmented = isLogSegmented;
    tempDir = StoreTestUtils.createTempDirectory("indexDir-" + TestUtils.getRandomString(10));
    tokenTestDir = StoreTestUtils.createTempDirectory("tokenTestDir-" + TestUtils.getRandomString(10));
    this.persistentIndexVersion = persistentIndexVersion;
    PersistentIndex.CURRENT_VERSION = persistentIndexVersion;
    this.addUndeletes = addUndeletes;
    if (addUndeletes) {
      Assert.assertTrue("Undelete records require persistent index version to be 3 and above",
          PersistentIndex.CURRENT_VERSION >= PersistentIndex.VERSION_3);
    }
    state = new CuratedLogIndexState(isLogSegmented, tempDir, true, addUndeletes);
    stateForTokenTest = new CuratedLogIndexState(true, tokenTestDir, true, addUndeletes, true);
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws Exception
   */
  @After
  public void cleanup() throws Exception {
    if (state != null) {
      state.destroy();
    }
    if (stateForTokenTest != null) {
      stateForTokenTest.destroy();
    }
    assertTrue(tempDir + " could not be cleaned", StoreTestUtils.cleanDirectory(tempDir, true));
    assertTrue(tokenTestDir + " could not be cleaned", StoreTestUtils.cleanDirectory(tokenTestDir, true));
    PersistentIndex.CURRENT_VERSION = this.persistentIndexVersion;
  }

  /**
   * Test forward and backward compatibility when the version is 2 and then we bump the version to 3 and add some index entries.
   * make sure those entries can still be read at version 2.
   */
  @Test
  public void compatibilityTest() throws StoreException {
    assumeTrue(PersistentIndex.CURRENT_VERSION == PersistentIndex.VERSION_2
        || PersistentIndex.CURRENT_VERSION == PersistentIndex.VERSION_3);
    // change the version to newer version and reload the index.
    PersistentIndex.CURRENT_VERSION = (short) (persistentIndexVersion + 1);
    // reloading the index at new version to test backward compatibility
    state.reloadIndexAndVerifyState(true, false);

    // get some values out to make sure we can read from the old version
    IndexValue value = state.index.findKey(state.liveKeys.iterator().next());
    assertNotNull("New version should be able to read old version live key", value);
    assertEquals("Version doesn't match", persistentIndexVersion, value.getFormatVersion());
    assertFalse("Not deleted", value.isDelete());

    value = state.index.findKey(state.expiredKeys.iterator().next());
    assertNotNull("New version should be able to old version expired key", value);
    assertEquals("Version doesn't match", persistentIndexVersion, value.getFormatVersion());
    assertFalse("Not deleted", value.isDelete());
    assertFalse("Not ttlUpdated", value.isTtlUpdate());

    value = state.index.findKey(state.deletedKeys.iterator().next());
    assertNotNull("New version should be able to old version deleted key", value);
    assertEquals("Version doesn't match", persistentIndexVersion, value.getFormatVersion());
    assertTrue("Not deleted", value.isDelete());

    // If we are here, then at new version we can read all the index entries in old version.
    // Now add some index entries at new version.
    int indexSegmentCount = state.index.getIndexSegments().size();
    IndexEntry entry =
        state.addPutEntries(1, PUT_RECORD_SIZE, state.time.milliseconds() + TimeUnit.DAYS.toMillis(2)).get(0);
    MockId key = (MockId) entry.getKey();
    state.makePermanent(key, false);
    state.addDeleteEntry(key);
    assertEquals("New version entry should roll over to new index segment", indexSegmentCount + 1,
        state.index.getIndexSegments().size());

    value = state.index.findKey(key);
    assertNotNull("New version should be able to new version expired key", value);
    assertEquals("Version doesn't match", PersistentIndex.CURRENT_VERSION, value.getFormatVersion());
    assertTrue("Not deleted", value.isDelete());

    // change the version back to old version, so we can test forward compatibility
    PersistentIndex.CURRENT_VERSION = persistentIndexVersion;
    state.reloadIndexAndVerifyState(true, false);

    // get records written in new version make sure we can read it out.
    value = state.index.findKey(key);
    assertNotNull("Old version should be able to new version expired key", value);
    assertEquals("Version doesn't match", (short) (persistentIndexVersion + 1), value.getFormatVersion());
    assertTrue("Not deleted", value.isDelete());
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
   * Tests for {@link PersistentIndex#findKey(StoreKey, FileSpan, EnumSet)}.
   * Cases:
   * 1. FileSpan exactly that of the message
   * 2. FileSpan before and after message including it on the boundary
   * 3. FileSpan that includes the message.
   * 4. FileSpan before and after message not including it
   * @throws StoreException
   */
  @Test
  public void findKeyWithFileSpanTest() throws StoreException {
    for (MockId id : state.allKeys.keySet()) {
      doFindKeyWithFileSpanTest(id,
          EnumSet.of(PersistentIndex.IndexEntryType.PUT, PersistentIndex.IndexEntryType.DELETE));
      doFindKeyWithFileSpanTest(id, EnumSet.allOf(PersistentIndex.IndexEntryType.class));
      for (PersistentIndex.IndexEntryType type : PersistentIndex.IndexEntryType.values()) {
        doFindKeyWithFileSpanTest(id, EnumSet.of(type));
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
    state.reloadIndex(true, false);
    List<EnumSet<StoreGetOptions>> allCombos = new ArrayList<>();
    allCombos.add(EnumSet.noneOf(StoreGetOptions.class));
    allCombos.add(EnumSet.of(StoreGetOptions.Store_Include_Expired));
    allCombos.add(EnumSet.of(StoreGetOptions.Store_Include_Deleted));
    allCombos.add(EnumSet.allOf(StoreGetOptions.class));
    for (MockId id : state.allKeys.keySet()) {
      for (EnumSet<StoreGetOptions> getOptions : allCombos) {
        if (state.liveKeys.contains(id)) {
          verifyBlobReadOptions(id, getOptions, null);
        } else if (state.expiredKeys.contains(id)) {
          StoreErrorCodes expectedErrorCode =
              getOptions.contains(StoreGetOptions.Store_Include_Expired) ? null : StoreErrorCodes.TTL_Expired;
          verifyBlobReadOptions(id, getOptions, expectedErrorCode);
        } else if (state.deletedKeys.contains(id)) {
          StoreErrorCodes expectedErrorCode =
              state.getExpectedValue(id, true) != null && getOptions.contains(StoreGetOptions.Store_Include_Deleted)
                  ? null : StoreErrorCodes.ID_Deleted;
          verifyBlobReadOptions(id, getOptions, expectedErrorCode);
        }
      }
    }
    // try to get BlobReadOption for a non existent key
    MockId nonExistentId = state.getUniqueId();
    verifyBlobReadOptions(nonExistentId, EnumSet.allOf(StoreGetOptions.class), StoreErrorCodes.ID_Not_Found);
  }

  /**
   * Test to change index segments when getting blob read info. Changing index segments is simulating compaction.
   * @throws Exception
   */
  @Test
  @Ignore
  public void getBlobReadInfoTestWithChangingIndexSegments() throws Exception {
    Assume.assumeTrue(isLogSegmented);
    Assume.assumeTrue(persistentIndexVersion >= PersistentIndex.VERSION_3);
    // First get key that are in second log segment
    LogSegment firstLogSegment = state.log.getFirstSegment();
    LogSegment secondLogSegment = state.log.getNextSegment(firstLogSegment);

    MockId mockId = null;
    Iterator<MockId> iter = state.liveKeys.iterator();
    while (iter.hasNext()) {
      MockId potentialId = iter.next();
      // Make sure this is a key that point to a put value in second log segment
      IndexValue value = state.getExpectedValue(potentialId, false);
      if (value != null && value.isPut() && value.getOffset().getName().equals(secondLogSegment.getName())) {
        mockId = potentialId;
        break;
      }
    }
    if (mockId == null) {
      Assert.fail("Failed to get a put value's mock id in second log segment");
    }

    // Now we know we this id's value is point to second log segment, we will drop all the index segments for second
    // log segment.
    Set<Offset> segmentsToDrop = new HashSet<>();
    for (Offset indexOffset : state.index.getIndexSegments().keySet()) {
      if (indexOffset.getName().equals(secondLogSegment.getName())) {
        segmentsToDrop.add(indexOffset);
      }
    }

    ExecutorService service = Utils.newScheduler(2, false);
    final MockId finalMockId = mockId;
    final CountDownLatch latch = new CountDownLatch(1);
    // Add a callback method to block getBlobReadInfo method so the changeIndexSegments can be invoked.
    state.index.setGetBlobReadInfoTestCallback(() -> {
      latch.countDown();
      try {
        // sleep for 3 second to simulate gc pause
        Thread.sleep(3000);
      } catch (Exception e) {
      }
    });
    Future<BlobReadOptions> future = service.submit(() -> {
      BlobReadOptions options = state.index.getBlobReadInfo(finalMockId, EnumSet.noneOf(StoreGetOptions.class));
      Assert.assertEquals(1, secondLogSegment.refCount());
      options.close();
      Assert.assertEquals(0, secondLogSegment.refCount());
      return options;
    });
    service.submit((Callable) () -> {
      latch.await();
      state.index.changeIndexSegments(Collections.emptyList(), segmentsToDrop);
      state.log.dropSegment(secondLogSegment.getName(), true);
      return null;
    });

    try {
      // We are not expecting any exception
      future.get();
    } catch (Exception e) {
      Assert.fail("We are not expecting any exception" + e.toString());
    } finally {
      service.shutdown();
    }
    state.reloadIndex(true, false);
  }

  /**
   * Tests {@link PersistentIndex#findMissingKeys(List)}.
   * @throws StoreException
   */
  @Test
  public void findMissingKeysTest() throws StoreException {
    List<StoreKey> idsToProvide = new ArrayList<>(state.allKeys.keySet());
    Set<StoreKey> nonExistentIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      nonExistentIds.add(state.getUniqueId());
    }
    idsToProvide.addAll(nonExistentIds);
    Collections.shuffle(idsToProvide);
    Set<StoreKey> missingKeys = state.index.findMissingKeys(idsToProvide);
    assertEquals("Set of missing keys not as expected", nonExistentIds, missingKeys);
  }

  @Test
  public void findMissingKeysInBatchTest() throws Exception {
    Assume.assumeTrue(isLogSegmented);
    Assume.assumeFalse(addUndeletes);
    Assume.assumeTrue(persistentIndexVersion >= VERSION_3);
    if (state != null) {
      state.destroy();
    }
    state = new CuratedLogIndexState(true, tempDir, false, false, true, false, false, false);

    final String dataNode = "remotedatanode1";
    final int maxEntriesInOneIndexSegment = DEFAULT_MAX_IN_MEM_ELEMENTS;
    final int numberOfIndexSegments = 10;
    // Let's deal with DELETEs for now
    List<StoreKey> deletedStoreKeys = new ArrayList<>(maxEntriesInOneIndexSegment * numberOfIndexSegments);
    for (int i = 0; i < numberOfIndexSegments * maxEntriesInOneIndexSegment; i++) {
      MockId p = state.getUniqueId();
      state.addDeleteEntry(p, new MessageInfo(p, DELETE_RECORD_SIZE, true, false, p.getAccountId(), p.getContainerId(),
          state.time.milliseconds()), (short) 0);
      deletedStoreKeys.add(p);
    }

    // Make sure we persist the index and seal the index segments.
    state.index.persistIndex();
    List<Offset> indexSegmentOffsets = new ArrayList<>();
    // referenceIndex is a tree map, it returns keys in order
    state.referenceIndex.keySet().forEach(offset -> indexSegmentOffsets.add(offset));

    // FindMissingKeysInBatch should only call findKey for the first key in first index segment
    LinkedList<Offset> cachedKeysOffsets = new LinkedList<>();
    Offset indexSegmentOffset = state.referenceIndex.firstKey();
    boolean isFirst = true;
    for (int i = 0; i < state.config.storeCacheSizeForFindMissingKeysInBatchMode; i++) {
      cachedKeysOffsets.addFirst(indexSegmentOffset);
      findMissingKeysInBatchForIndexSegmentAndVerify(indexSegmentOffset, isFirst ? 1L : 0, cachedKeysOffsets, dataNode);
      indexSegmentOffset = state.referenceIndex.higherKey(indexSegmentOffset);
      isFirst = false;
    }

    // Back to first index segment, it shouldn't call find key here
    findMissingKeysInBatchForIndexSegmentAndVerify(state.referenceIndex.firstKey(), 0, cachedKeysOffsets, dataNode);
    // Move on to fourth index segment, the first index segment would be removed from the cache
    cachedKeysOffsets.removeLast();
    cachedKeysOffsets.addFirst(indexSegmentOffset);
    findMissingKeysInBatchForIndexSegmentAndVerify(indexSegmentOffset, 0, cachedKeysOffsets, dataNode);

    Offset lastIndexSegmentOffset = state.referenceIndex.lastKey();
    // Last index segment is not sealed, find missing keys in the last index segment would clear the cache.
    StoreKey cacheClearKey = state.referenceIndex.get(lastIndexSegmentOffset).keySet().iterator().next();
    Assert.assertEquals(0,
        state.index.findMissingKeysInBatch(Collections.singletonList(cacheClearKey), dataNode).size());
    Assert.assertEquals(0, state.index.getCachedKeys().get(dataNode).size());

    // First key is in second index segment, and second key is in first index segment
    List<StoreKey> keys = getRandomKeyFromIndexSegment(1, 0);
    Assert.assertEquals(0, state.index.findMissingKeysInBatch(keys, dataNode).size());
    LinkedList<Pair<Offset, Set<StoreKey>>> cachedKeys = state.index.getCachedKeys().get(dataNode);
    Assert.assertEquals(2, cachedKeys.size());
    Assert.assertEquals(indexSegmentOffsets.get(0), cachedKeys.get(0).getFirst());
    Assert.assertEquals(indexSegmentOffsets.get(1), cachedKeys.get(1).getFirst());

    // missing one key won't clear the cache
    keys = Collections.singletonList(state.getUniqueId());
    Assert.assertEquals(1, state.index.findMissingKeysInBatch(keys, dataNode).size());
    Assert.assertEquals(2, state.index.getCachedKeys().get(dataNode).size());

    // Trying a different datanode, it should not interfere with the existing datanode's cache
    final String anotherDataNode = "anotherDataNode";
    findMissingKeysInBatchForIndexSegmentAndVerify(indexSegmentOffsets.get(0), 1,
        Collections.singletonList(indexSegmentOffsets.get(0)), anotherDataNode);
    cachedKeys = state.index.getCachedKeys().get(dataNode);
    Assert.assertEquals(2, cachedKeys.size());
    Assert.assertEquals(indexSegmentOffsets.get(0), cachedKeys.get(0).getFirst());
    Assert.assertEquals(indexSegmentOffsets.get(1), cachedKeys.get(1).getFirst());

    // Back to first dataNode, missing several keys would clear the cache
    int numKeys = DEFAULT_NUMBER_CACHE_MISS_IN_FIND_MISSING_KEY_IN_BATCH_MODE;
    keys = new ArrayList<>(numKeys);
    for (int i = 0; i < numKeys + 1; i++) {
      keys.add(state.getUniqueId());
    }
    keys.addAll(getRandomKeyFromIndexSegment(0, 1));
    long findKeyCallCountBefore = state.metrics.findTime.getCount();
    Assert.assertEquals(numKeys + 1, state.index.findMissingKeysInBatch(keys, dataNode).size());
    long findKeyCallCountAfter = state.metrics.findTime.getCount();
    Assert.assertEquals(keys.size(), findKeyCallCountAfter - findKeyCallCountBefore);
    Assert.assertEquals(0, state.index.getCachedKeys().get(dataNode).size());
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
            state.time.milliseconds(), Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM),
            (short) 0);
    try {
      state.index.addToIndex(new IndexEntry(state.getUniqueId(), value), fileSpan);
      fail("Should have failed because filespan provided < currentIndexEndOffset");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    if (isLogSegmented) {
      // FileSpan spans across segments
      Offset startOffset = state.index.getCurrentEndOffset();
      LogSegmentName nextLogSegmentName = startOffset.getName().getNextPositionName();
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
   * Tests error cases for {@link PersistentIndex#markAsDeleted(StoreKey, FileSpan, long)}.
   * Cases
   * 1. FileSpan end offset < currentIndexEndOffset
   * 2. FileSpan is across segments
   * 3. ID does not exist
   * 4. ID already deleted
   * @throws StoreException
   */
  @Test
  public void markAsDeletedBadInputTest() throws StoreException {
    // FileSpan end offset < currentIndexEndOffset
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getStartOffset(), 1);
    try {
      state.index.markAsDeleted(state.liveKeys.iterator().next(), fileSpan, state.time.milliseconds());
      fail("Should have failed because filespan provided < currentIndexEndOffset");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    if (isLogSegmented) {
      // FileSpan spans across segments
      Offset startOffset = state.index.getCurrentEndOffset();
      LogSegmentName nextLogSegmentName = startOffset.getName().getNextPositionName();
      Offset endOffset = new Offset(nextLogSegmentName, 0);
      fileSpan = new FileSpan(startOffset, endOffset);
      try {
        state.index.markAsDeleted(state.liveKeys.iterator().next(), fileSpan, state.time.milliseconds());
        fail("Should have failed because fileSpan provided spanned across segments");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }

    state.appendToLog(5);
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), 5);
    // ID does not exist
    try {
      state.index.markAsDeleted(state.getUniqueId(), fileSpan, state.time.milliseconds());
      fail("Should have failed because ID provided for delete does not exist");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Not_Found, e.getErrorCode());
    }

    // ID already deleted
    try {
      state.index.markAsDeleted(state.deletedKeys.iterator().next(), fileSpan, state.time.milliseconds());
      fail("Should have failed because ID provided for delete is already deleted");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Deleted, e.getErrorCode());
    }
  }

  /**
   * Tests Undelete basic operation when it's invoked from frontend.
   * @throws StoreException
   */
  @Test
  public void undeleteBasicTest() throws StoreException {
    assumeTrue(isLogSegmented && addUndeletes);
    // Get deleted key that hasn't been TTLUpdated
    StoreKey targetKey = null;
    for (StoreKey key : state.deletedKeys) {
      IndexValue value = state.index.findKey(key);
      if (!value.isTtlUpdate() && !state.index.isExpired(value) && state.getExpectedValue((MockId) key, true) != null) {
        targetKey = key;
        break;
      }
    }
    if (targetKey == null) {
      throw new IllegalStateException("Could not find deleted key without TTL Update");
    }
    //Undelete deleted key
    short expectedLifeVersion = 1;
    undeleteKeyAndVerify(targetKey, expectedLifeVersion, false);
    //Delete Key
    deleteKeyAndVerify(targetKey, expectedLifeVersion);
    //Undelete key again
    expectedLifeVersion++;
    undeleteKeyAndVerify(targetKey, expectedLifeVersion, false);
    //TTL Update Key
    ttlUpdateKeyAndVerify(targetKey, expectedLifeVersion);
    //Delete Key again
    deleteKeyAndVerify(targetKey, expectedLifeVersion);
    //Undelete Key again
    expectedLifeVersion++;
    undeleteKeyAndVerify(targetKey, expectedLifeVersion, true);
  }

  /**
   * Test {@link PersistentIndex#markAsUndeleted(StoreKey, FileSpan, MessageInfo, long, short)} when the lifeVersion passed
   * into the method is not {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND}.
   * @throws Exception
   */
  @Test
  public void markAsUndeleteLifeVersion() throws Exception {
    assumeTrue(isLogSegmented && addUndeletes);
    short lifeVersion = 2;
    IndexEntry entry = state.addPutEntries(1, PUT_RECORD_SIZE, 0, lifeVersion).get(0);
    MockId id = (MockId) entry.getKey();
    // ID already expired, undelete it with higher life version, should succeed
    state.appendToLog(UNDELETE_RECORD_SIZE);
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    long operationTime = state.time.milliseconds();
    state.index.markAsUndeleted(id, fileSpan, null, operationTime, (short) (lifeVersion + 1));
    IndexValue value = state.index.findKey(id);
    checkUndelete(value, false, (short) (lifeVersion + 1), id.getAccountId(), id.getContainerId(), UNDELETE_RECORD_SIZE,
        fileSpan.getStartOffset(), entry.getValue().getExpiresAtMs(), operationTime);

    entry = state.addPutEntries(1, PUT_RECORD_SIZE, 0, lifeVersion).get(0);
    id = (MockId) entry.getKey();
    state.appendToLog(UNDELETE_RECORD_SIZE);
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    // Undelete with -1 as lifeVersion, since its's not deleted, should fail
    try {
      state.index.markAsUndeleted(id, fileSpan, null, state.time.milliseconds(),
          MessageInfo.LIFE_VERSION_FROM_FRONTEND);
      fail("should not succeed");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.ID_Not_Deleted, e.getErrorCode());
    }

    // Undelete with smaller lifeVersion, should fail
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    try {
      state.index.markAsUndeleted(id, fileSpan, null, state.time.milliseconds(), (short) (lifeVersion - 1));
      fail("should not succeed");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Life_Version_Conflict, e.getErrorCode());
    }

    // Undelete with same lifeVersion, should fail
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    try {
      state.index.markAsUndeleted(id, fileSpan, null, state.time.milliseconds(), lifeVersion);
      fail("should not succeed");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Life_Version_Conflict, e.getErrorCode());
    }

    // Undelete with larger lifeVersion, should succeed
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    operationTime = state.time.milliseconds();
    state.index.markAsUndeleted(id, fileSpan, null, operationTime, (short) (lifeVersion + 1));
    value = state.index.findKey(id);
    checkUndelete(value, false, (short) (lifeVersion + 1), id.getAccountId(), id.getContainerId(), UNDELETE_RECORD_SIZE,
        fileSpan.getStartOffset(), entry.getValue().getExpiresAtMs(), operationTime);

    state.appendToLog(UNDELETE_RECORD_SIZE);
    // Undelete with same lifeVersion, should fail.
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    try {
      state.index.markAsUndeleted(id, fileSpan, null, operationTime, (short) (lifeVersion + 1));
      fail("should not succeed");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.ID_Undeleted, e.getErrorCode());
    }

    // Undelete with even larger lifeVersion, should succeed.
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    operationTime = state.time.milliseconds();
    state.index.markAsUndeleted(id, fileSpan, null, operationTime, (short) (lifeVersion + 4));
    value = state.index.findKey(id);
    checkUndelete(value, false, (short) (lifeVersion + 4), id.getAccountId(), id.getContainerId(), UNDELETE_RECORD_SIZE,
        fileSpan.getStartOffset(), entry.getValue().getExpiresAtMs(), operationTime);
  }

  /**
   * Tests error cases for {@link PersistentIndex#markAsUndeleted(StoreKey, FileSpan, long)}.
   * Cases
   * 1. FileSpan end offset < currentIndexEndOffset
   * 2. FileSpan is across segments
   * 3. ID does not exist
   * 4. Live id
   * 5. Undeleted id
   * 6. Permanently deleted id
   * @throws StoreException
   */
  @Test
  public void markAsUndeletedBadInputTest() throws StoreException {
    assumeTrue(isLogSegmented && addUndeletes);
    // FileSpan end offset < currentIndexEndOffset
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getStartOffset(), 1);
    try {
      state.index.markAsUndeleted(state.deletedKeys.iterator().next(), fileSpan, state.time.milliseconds());
      fail("Should have failed because filespan provided < currentIndexEndOffset");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // FileSpan spans across segments
    Offset startOffset = state.index.getCurrentEndOffset();
    LogSegmentName nextLogSegmentName = startOffset.getName().getNextPositionName();
    Offset endOffset = new Offset(nextLogSegmentName, 0);
    fileSpan = new FileSpan(startOffset, endOffset);
    try {
      state.index.markAsUndeleted(state.deletedKeys.iterator().next(), fileSpan, state.time.milliseconds());
      fail("Should have failed because fileSpan provided spanned across segments");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    state.appendToLog(5);
    fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), 5);
    // ID does not exist
    try {
      state.index.markAsUndeleted(state.getUniqueId(), fileSpan, state.time.milliseconds());
      fail("Should have failed because ID provided for undelete does not exist");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Not_Found, e.getErrorCode());
    }

    try {
      // live keys might contain undelete keys, filter them out
      MockId id = null;
      Iterator<MockId> iter = state.liveKeys.iterator();
      while (iter.hasNext()) {
        id = iter.next();
        IndexValue value = state.getExpectedValue(id, false);
        if (value != null && !value.isUndelete()) {
          break;
        }
      }
      assertNotNull(id);
      state.index.markAsUndeleted(id, fileSpan, state.time.milliseconds());
      fail("Should have failed because ID provided for undelete hasn't be deleted yet");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Not_Deleted, e.getErrorCode());
    }

    try {
      state.index.markAsUndeleted(state.undeletedKeys.iterator().next(), fileSpan, state.time.milliseconds());
      fail("Should have failed because ID provided for udneleted is already undeleted.");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Undeleted, e.getErrorCode());
    }
  }

  /**
   * Test the sanity check of the before and after index segment offsets.
   */
  @Test
  public void testBeforeAndAfterOffsetSanityCheck() throws Exception {
    assumeTrue(isLogSegmented && persistentIndexVersion >= PersistentIndex.VERSION_3);
    // Adding valid before and after offsets for finished compaction
    Map<Offset, Offset> beforeAndAfter = new HashMap<>();
    Offset after = state.index.getIndexSegments().firstKey();
    Offset before = new Offset(after.getName().getNextGenerationName(), LogSegment.HEADER_SIZE);
    beforeAndAfter.put(before, after); // This is one redirection.
    Offset beforeBefore = new Offset(before.getName().getNextGenerationName(), LogSegment.HEADER_SIZE);
    beforeAndAfter.put(beforeBefore, before); // This is two redirections.
    state.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(state.time.milliseconds(), beforeAndAfter, false);
    state.index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();
    assertFalse(state.index.isSanityCheckFailed());

    // Adding invalid before and after offsets for inprogress compaction
    try {
      state.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(state.time.milliseconds(), beforeAndAfter, true);
      fail("Before and after offsets should be invalid for inprogress compaction");
    } catch (Exception e) {
    }

    // Adding invalid before and after offset for finished compaction.
    beforeAndAfter.clear();
    Offset secondIndexSegmentOffset = state.index.getIndexSegments().higherKey(after);
    // Before offset exist in the current index
    beforeAndAfter.put(secondIndexSegmentOffset, after);
    state.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(state.time.milliseconds(), beforeAndAfter, false);
    state.index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();

    assertTrue(state.index.isSanityCheckFailed());

    state.reloadIndex(true, false);
    assertFalse(state.index.isSanityCheckFailed());
    beforeAndAfter.clear();
    // Create two non-existing offsets
    Offset nonExistingBefore =
        new Offset(secondIndexSegmentOffset.getName().getNextGenerationName(), LogSegment.HEADER_SIZE);
    Offset nonExistingAfter = new Offset(nonExistingBefore.getName().getNextGenerationName(), LogSegment.HEADER_SIZE);
    beforeAndAfter.put(nonExistingBefore, nonExistingAfter);
    state.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(state.time.milliseconds(), beforeAndAfter, false);
    state.index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();
    assertTrue(state.index.isSanityCheckFailed());

    state.reloadIndex(true, false);
    assertFalse(state.index.isSanityCheckFailed());
    beforeAndAfter.clear();
    // Create a loop
    beforeAndAfter.put(nonExistingBefore, nonExistingAfter);
    beforeAndAfter.put(nonExistingAfter, nonExistingBefore);
    state.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(state.time.milliseconds(), beforeAndAfter, false);
    state.index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();
    assertTrue(state.index.isSanityCheckFailed());
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
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void hardDeleteTest() throws IOException, StoreException {
    state.properties.put("store.deleted.message.retention.hours", Integer.toString(1));
    state.properties.put("store.hard.delete.operations.bytes.per.sec", Integer.toString(Integer.MAX_VALUE / 10));
    state.reloadIndex(true, false);
    state.index.hardDeleter.enabled.set(true);
    assertFalse("Hard delete did work even though no message is past retention time",
        state.index.hardDeleter.hardDelete());
    // IndexSegment still uses real time so advance time so that it goes 2 days past the real time.
    state.advanceTime(SystemTime.getInstance().milliseconds() + TimeUnit.DAYS.toMillis(2));
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
   * @throws StoreException
   */
  @Test
  public void expirationTest() throws StoreException {
    // add a PUT entry that will expire if time advances
    // advance time so that time moves to whole second with no residual milliseconds
    state.time.sleep(Time.MsPerSec - state.time.milliseconds());
    long expiresAtMs = state.time.milliseconds() + CuratedLogIndexState.DELAY_BETWEEN_LAST_MODIFIED_TIMES_MS + 1000;
    state.addPutEntries(1, 1, expiresAtMs);
    MockId id = state.logOrder.lastEntry().getValue().getFirst();
    verifyBlobReadOptions(id, EnumSet.noneOf(StoreGetOptions.class), null);
    state.advanceTime(expiresAtMs - state.time.milliseconds() + TimeUnit.SECONDS.toMillis(1));
    verifyBlobReadOptions(id, EnumSet.noneOf(StoreGetOptions.class), StoreErrorCodes.TTL_Expired);
  }

  /**
   * Tests that end offsets are set correctly in log segments when the index is created.
   * Cases
   * 1. Current end offsets have been set correctly
   * 2. Add data to log but not index, restart and check that end offsets have been reset.
   * 3. Add data to log such that a new log segment is created but not to index, restart and check that the new log
   * segment is gone.
   * @throws StoreException
   */
  @Test
  public void setEndOffsetsTest() throws StoreException {
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
   * @throws StoreException
   */
  @Test
  public void changeIndexSegmentsTest() throws StoreException {
    ConcurrentSkipListMap<Offset, IndexSegment> indexes = state.index.getIndexSegments();
    Set<Offset> saved = indexes.clone().keySet();

    Map<Offset, IndexSegment> toRemoveOne = new HashMap<>();
    Set<Offset> toRetainOne = new HashSet<>();

    partitionIndexSegments(toRemoveOne, toRetainOne);

    // remove the first batch without adding anything
    state.index.changeIndexSegments(Collections.emptyList(), toRemoveOne.keySet());
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
    state.index.changeIndexSegments(filesToAdd, Collections.emptySet());
    assertEquals("Offsets in index do not match expected", saved, state.index.getIndexSegments().keySet());
    // make sure persist does not throw errors
    state.index.persistIndex();

    // error case
    // try to remove the last segment (its offset is in the journal)
    try {
      state.index.changeIndexSegments(Collections.emptyList(), Collections.singleton(state.referenceIndex.lastKey()));
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
      state.index.changeIndexSegments(Collections.singletonList(firstSegment.getFile()), Collections.emptySet());
      fail("Should have failed to add index segment because its end offset is past the first offset in the journal");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Test getting absolute end position of last PUT under different circumstances (i.e. last PUT is out of Journal)
   * @throws Exception
   */
  @Test
  public void getAbsoluteEndPositionOfLastPutTest() throws Exception {
    File testDir = StoreTestUtils.createTempDirectory("indexDirTest-" + TestUtils.getRandomString(10));
    CuratedLogIndexState indexState =
        new CuratedLogIndexState(isLogSegmented, testDir, false, false, true, true, false, false);
    assertEquals("There should be no PUT record since index is empty", -1,
        indexState.index.getAbsoluteEndPositionOfLastPut());
    long expiresAtMs = indexState.time.milliseconds() + TimeUnit.HOURS.toMillis(1);
    // DEFAULT_MAX_IN_MEM_ELEMENTS = 5, here we put 5 entries into the log
    List<IndexEntry> indexEntries = indexState.addPutEntries(5, PUT_RECORD_SIZE, expiresAtMs);
    assertEquals("Number of index segments should be 1", 1, indexState.index.getIndexSegments().size());
    // update ttl for above 5 entries and the ttl entries fall in second index segment.
    for (IndexEntry entry : indexEntries) {
      indexState.makePermanent((MockId) entry.getKey(), false);
    }
    assertEquals("Number of index segments (after ttl update) should be 2", 2,
        indexState.index.getIndexSegments().size());
    // default journal size should be 2 * DEFAULT_MAX_IN_MEM_ELEMENTS = 10
    assertEquals("Journal size not expected", 2 * DEFAULT_MAX_IN_MEM_ELEMENTS,
        indexState.index.journal.getAllEntries().size());
    // delete above 5 entries, these delete entries should fall in third index segment
    for (IndexEntry entry : indexEntries) {
      indexState.addDeleteEntry((MockId) entry.getKey());
    }
    assertEquals("Number of index segments (after deletion) should be 3", 3,
        indexState.index.getIndexSegments().size());
    // although there are now 15 entries in total, journal size is still 10 (old entries are removed from journal)
    assertEquals("Journal size (after deletion) shouldn't change", 2 * DEFAULT_MAX_IN_MEM_ELEMENTS,
        indexState.index.journal.getAllEntries().size());
    // after deletion, the last PUT record falls in first segment (out of journal)
    // for segmented log, there is a header size = 18
    assertEquals("Absolute end position of last PUT record not expected",
        5 * PUT_RECORD_SIZE + (isLogSegmented ? 18 : 0), indexState.index.getAbsoluteEndPositionOfLastPut());
    // close the index to seal all index segments
    indexState.index.close(false);
    // get end position of last PUT again, it should return same result (this is to test getting last PUT in sealed index segment)
    assertEquals("Absolute end position of last PUT record not expected",
        5 * PUT_RECORD_SIZE + (isLogSegmented ? 18 : 0), indexState.index.getAbsoluteEndPositionOfLastPut());

    // calculate current end position in log segment (note that there are 5 PUT, 5 TTL update and 5 DELETE entries)
    long currentEndPosition =
        5 * PUT_RECORD_SIZE + 5 * TTL_UPDATE_RECORD_SIZE + 5 * DELETE_RECORD_SIZE + (isLogSegmented ? 18 : 0);
    // add one more PUT entry and delete it afterwards
    indexEntries = indexState.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    indexState.addDeleteEntry((MockId) indexEntries.get(0).getKey());
    assertEquals("Number of index segments after new PUT and delete should be 4", 4,
        indexState.index.getIndexSegments().size());
    // now, the latest PUT entry should be in the journal
    assertEquals("Absolute end position of last PUT record not expected", currentEndPosition + PUT_RECORD_SIZE,
        indexState.index.getAbsoluteEndPositionOfLastPut());
    indexState.destroy();
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
          IndexValue value =
              new IndexValue(1, fileSpan.getStartOffset(), Utils.Infinite_Time, state.time.milliseconds(),
                  Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM));
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
          state.index.changeIndexSegments(Collections.emptyList(),
              Collections.singleton(segmentToUse.getStartOffset()));
          // ensure that the relevant index segment is gone.
          if (state.index.getIndexSegments().containsKey(segmentToUse.getStartOffset())) {
            throw new IllegalStateException(
                "Segment with offset " + segmentToUse.getStartOffset() + " should have been" + " removed.");
          }
          // add it back
          state.index.changeIndexSegments(Collections.singletonList(segmentToUse.getFile()), Collections.emptySet());
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
   * @throws StoreException
   */
  @Test
  public void recoverySuccessTest() throws StoreException {
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
    MessageInfo info = new MessageInfo(state.liveKeys.iterator().next(), CuratedLogIndexState.PUT_RECORD_SIZE,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), Utils.Infinite_Time);
    doRecoveryFailureTest(info, StoreErrorCodes.Initialization_Error);
    // recovery info contains a PUT for a key that has been deleted
    info = new MessageInfo(state.deletedKeys.iterator().next(), CuratedLogIndexState.PUT_RECORD_SIZE,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), Utils.Infinite_Time);
    doRecoveryFailureTest(info, StoreErrorCodes.Initialization_Error);
    // recovery info contains a Ttl Update for a key that does not exist and there is no delete info that follows
    MockId nonExistentId = state.getUniqueId();
    info = new MessageInfo(nonExistentId, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true,
        nonExistentId.getAccountId(), nonExistentId.getContainerId(), state.time.milliseconds());
    doRecoveryFailureTest(info, StoreErrorCodes.Initialization_Error);
    // recovery info contains a Ttl Update for a key that is already Ttl updated
    MockId updatedId = null;
    for (MockId id : state.ttlUpdatedKeys) {
      if (state.liveKeys.contains(id)) {
        updatedId = id;
        break;
      }
    }
    assertNotNull("There is no key that is ttl updated but not deleted", updatedId);
    info =
        new MessageInfo(updatedId, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true, updatedId.getAccountId(),
            updatedId.getContainerId(), state.time.milliseconds());
    doRecoveryFailureTest(info, StoreErrorCodes.Already_Updated);
    // recovery info contains a Ttl Update for a key that is already deleted
    MockId deletedId = state.deletedKeys.iterator().next();
    info =
        new MessageInfo(deletedId, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true, deletedId.getAccountId(),
            deletedId.getContainerId(), state.time.milliseconds());
    doRecoveryFailureTest(info, StoreErrorCodes.ID_Deleted);
    // recovery info contains a DELETE for a key that has been deleted, with the same lifeVersion
    info = new MessageInfo(state.deletedKeys.iterator().next(), CuratedLogIndexState.DELETE_RECORD_SIZE, true, false,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), state.time.milliseconds());
    doRecoveryFailureTest(info, StoreErrorCodes.Life_Version_Conflict);
    // recovery info that contains a PUT beyond the end offset of the log segment
    info = new MessageInfo(state.getUniqueId(), CuratedLogIndexState.PUT_RECORD_SIZE,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), Utils.Infinite_Time);
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
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceTest() throws StoreException {
    // add some more entries so that the journal gets entries across segments and doesn't start at the beginning
    // of an index segment.
    state.addPutEntries(7, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));

    // token with log end offset should not return anything
    StoreFindToken token =
        new StoreFindToken(state.log.getEndOffset(), state.sessionId, state.incarnationId, false, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
    token.setBytesRead(state.index.getLogUsedCapacity());
    doFindEntriesSinceTest(token, Long.MAX_VALUE, Collections.emptySet(), token);

    findEntriesSinceToIndexBasedTest();
    findEntriesSinceToJournalBasedTest();
    findEntriesSinceOneByOneTest();
    findEntriesSinceIndexBasedTokenForOffsetInJournalTest();

    // error case - can never have provided an index based token that is contains the offset of the last segment
    token = new StoreFindToken(state.referenceIndex.lastEntry().getValue().firstKey(), state.referenceIndex.lastKey(),
        state.sessionId, state.incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION);
    doFindEntriesSinceFailureTest(token, StoreErrorCodes.Unknown_Error);

    findEntriesSinceInEmptyIndexTest(false);
    findEntriesSinceTtlUpdateCornerCaseTest();
    findEntriesSinceTtlUpdateAndPutInJournalTest();
  }

  /**
   * Tests {@link PersistentIndex#findEntriesSince(FindToken, long)} when last log segment gets auto closed and token
   * points to the last index segment.
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceAfterAutoCloseLastLogSegmentTest() throws StoreException {
    state.addPutEntries(7, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.addDeleteEntry(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    if (isLogSegmented) {
      CompactionPolicySwitchInfo compactionPolicySwitchInfo =
          new CompactionPolicySwitchInfo(System.currentTimeMillis(), true);
      backUpCompactionPolicyInfo(tempDir.toString(), compactionPolicySwitchInfo);
      if (state.log.autoCloseLastLogSegmentIfQualified()) {
        //refresh journal.
        state.index.journal.cleanUpJournal();
      }

      //1. index based token points to last entry of second last index segment + 1 -> index based token points to first
      //entry of last Index segment
      Offset lastIndexSegmentStartOffset = state.referenceIndex.lastKey();
      Offset secondLastSegmentStartOffset = state.referenceIndex.lowerKey(lastIndexSegmentStartOffset);
      TreeMap<MockId, TreeSet<IndexValue>> lastIndexSegment = state.referenceIndex.get(lastIndexSegmentStartOffset);
      Map.Entry<MockId, TreeSet<IndexValue>> lastIndexSegmentFirstEntry = lastIndexSegment.firstEntry();
      long maxTotalSizeOfEntries =
          getSizeOfAllValues(state.referenceIndex.get(lastIndexSegmentStartOffset).firstEntry().getValue());

      MockId lastId = state.referenceIndex.get(secondLastSegmentStartOffset).lastKey();
      StoreFindToken startToken =
          new StoreFindToken(lastId, secondLastSegmentStartOffset, state.sessionId, state.incarnationId, null, null,
              UNINITIALIZED_RESET_KEY_VERSION);
      IndexSegment segmentOfToken = state.index.getIndexSegments().get(lastIndexSegmentStartOffset);
      StoreFindToken expectedEndToken =
          new StoreFindToken(lastIndexSegmentFirstEntry.getKey(), lastIndexSegmentStartOffset, state.sessionId,
              state.incarnationId, segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(),
              segmentOfToken.getResetKeyLifeVersion());
      try {
        expectedEndToken.setBytesRead(state.index.getAbsoluteReadBytesFromIndexBasedToken(expectedEndToken));
      } catch (StoreException e) {
        expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(lastIndexSegmentStartOffset));
      }
      Set<MockId> expectedKeys = new HashSet<>();
      expectedKeys.add(lastIndexSegmentFirstEntry.getKey());
      doFindEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

      //2. index based token points to last entry of last index segment -> index based token points to last entry of last
      //Index segment
      lastId = state.referenceIndex.get(lastIndexSegmentStartOffset).lastKey();
      startToken =
          new StoreFindToken(lastId, lastIndexSegmentStartOffset, state.sessionId, state.incarnationId, null, null,
              UNINITIALIZED_RESET_KEY_VERSION);
      Map.Entry<MockId, TreeSet<IndexValue>> lastIndexSegmentLastEntry = lastIndexSegment.lastEntry();
      expectedEndToken =
          new StoreFindToken(lastIndexSegmentLastEntry.getKey(), lastIndexSegmentStartOffset, state.sessionId,
              state.incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION);
      try {
        expectedEndToken.setBytesRead(state.index.getAbsoluteReadBytesFromIndexBasedToken(expectedEndToken));
      } catch (StoreException e) {
        expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(lastIndexSegmentStartOffset));
      }
      expectedKeys = new HashSet<>();
      maxTotalSizeOfEntries =
          getSizeOfAllValues(state.referenceIndex.get(lastIndexSegmentStartOffset).lastEntry().getValue());
      doFindEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);
    }
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
        new StoreFindToken(secondRecordFileSpan.getStartOffset(), new UUID(1, 1), state.incarnationId, false, null,
            null, UNINITIALIZED_RESET_KEY_VERSION);
    doFindEntriesSinceFailureTest(startToken, StoreErrorCodes.Unknown_Error);

    UUID oldSessionId = state.sessionId;
    final MockId newId = state.getUniqueId();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    long operationTimeMs = state.time.milliseconds();
    // add to allKeys() so that doFindEntriesSinceTest() works correctly.
    IndexValue putValue =
        new IndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, firstRecordFileSpan.getStartOffset(), Utils.Infinite_Time,
            operationTimeMs, accountId, containerId);
    state.allKeys.computeIfAbsent(newId, k -> new TreeSet<>()).add(putValue);
    state.recovery = (read, startOffset, endOffset, factory) -> Collections.singletonList(
        new MessageInfo(newId, CuratedLogIndexState.PUT_RECORD_SIZE, accountId, containerId, operationTimeMs));
    state.reloadIndex(true, true);

    // If there is no incarnationId in the incoming token, for backwards compatibility purposes we consider it as valid
    // and proceed with session id validation and so on.
    UUID[] incarnationIds = new UUID[]{state.incarnationId, null};
    for (UUID incarnationIdToTest : incarnationIds) {
      long bytesRead = state.index.getAbsolutePositionInLogForOffset(firstRecordFileSpan.getEndOffset());
      // create a token that will be past the index end offset on startup after recovery.
      if (incarnationIdToTest == null) {
        startToken = getTokenWithNullIncarnationId(
            new StoreFindToken(secondRecordFileSpan.getEndOffset(), oldSessionId, state.incarnationId, false, null,
                null, UNINITIALIZED_RESET_KEY_VERSION));
        assertNull("IncarnationId is expected to be null ", startToken.getIncarnationId());
      } else {
        startToken =
            new StoreFindToken(secondRecordFileSpan.getEndOffset(), oldSessionId, incarnationIdToTest, false, null,
                null, UNINITIALIZED_RESET_KEY_VERSION);
      }
      // token should get reset internally, no keys should be returned and the returned token should be correct (offset
      // in it will be the current log end offset = firstRecordFileSpan.getEndOffset()). The returned token should have correct reset key info.
      IndexSegment segmentOfToken =
          state.index.getIndexSegments().floorEntry(firstRecordFileSpan.getEndOffset()).getValue();
      StoreFindToken expectedEndToken =
          new StoreFindToken(firstRecordFileSpan.getEndOffset(), state.sessionId, state.incarnationId, true,
              segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
      expectedEndToken.setBytesRead(bytesRead);
      doFindEntriesSinceTest(startToken, Long.MAX_VALUE, Collections.emptySet(), expectedEndToken);

      // create a token that is not past the index end offset on startup after recovery. Should work as expected
      if (incarnationIdToTest == null) {
        startToken = getTokenWithNullIncarnationId(
            new StoreFindToken(lastRecordOffset, oldSessionId, state.incarnationId, false, null, null,
                UNINITIALIZED_RESET_KEY_VERSION));
        assertNull("IncarnationId is expected to be null ", startToken.getIncarnationId());
      } else {
        startToken = new StoreFindToken(lastRecordOffset, oldSessionId, incarnationIdToTest, false, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
      }
      segmentOfToken = state.index.getIndexSegments().floorEntry(firstRecordFileSpan.getStartOffset()).getValue();
      expectedEndToken =
          new StoreFindToken(firstRecordFileSpan.getStartOffset(), state.sessionId, state.incarnationId, false,
              segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
      expectedEndToken.setBytesRead(bytesRead);
      doFindEntriesSinceTest(startToken, Long.MAX_VALUE, Collections.singleton(newId), expectedEndToken);
    }
  }

  /**
   * Tests {@link PersistentIndex#getLogSegmentsNotInJournal()}
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void getLogSegmentsNotInJournalTest() throws IOException, StoreException {
    if (isLogSegmented) {
      testGetLogSegmentsNotInJournal(2, 2 * state.getMaxInMemElements());
      testGetLogSegmentsNotInJournal(3, 2 * state.getMaxInMemElements());
    } else {
      assertNull("LogSegments mismatch for non segmented log ", state.index.getLogSegmentsNotInJournal());
    }
    state.closeAndClearIndex();
    state.reloadIndex(false, false);
    assertNull("There should be no offsets in the journal", state.index.journal.getFirstOffset());
    assertNull("There should be no log segments returned", state.index.getLogSegmentsNotInJournal());
  }

  /**
   * Tests the cases where tokens have to be revalidated on a call to
   * {@link PersistentIndex#findEntriesSince(FindToken, long)}.
   * @throws StoreException
   */
  @Test
  public void tokenRevalidationTest() throws StoreException {
    // this test is valid only when the log has > 1 log segments i.e. when it can undergo compaction.
    if (isLogSegmented) {
      IndexSegment segmentOfToken = state.index.getIndexSegments().floorEntry(state.logOrder.lastKey()).getValue();
      StoreFindToken absoluteEndToken =
          new StoreFindToken(state.logOrder.lastKey(), state.sessionId, state.incarnationId, false,
              segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
      absoluteEndToken.setBytesRead(state.index.getLogUsedCapacity());

      Offset firstIndexSegmentStartOffset = state.referenceIndex.firstKey();
      assertTrue("The first index segment must have an offset > 0", firstIndexSegmentStartOffset.getOffset() > 0);

      // generate an offset that does not exist.
      LogSegmentName newName = firstIndexSegmentStartOffset.getName().getNextGenerationName();
      long newOffset = firstIndexSegmentStartOffset.getOffset() + 1;

      // Generate an offset that is below the index start offset
      long offsetBeforeStart = firstIndexSegmentStartOffset.getOffset() - 1;

      Offset[] invalidOffsets =
          {new Offset(newName, newOffset), new Offset(firstIndexSegmentStartOffset.getName(), offsetBeforeStart)};
      MockId firstIdInFirstIndexSegment = state.referenceIndex.firstEntry().getValue().firstKey();

      for (Offset invalidOffset : invalidOffsets) {
        // Generate an index based token from invalidOffset
        StoreFindToken startToken =
            new StoreFindToken(firstIdInFirstIndexSegment, invalidOffset, state.sessionId, state.incarnationId, null,
                null, UNINITIALIZED_RESET_KEY_VERSION);
        doFindEntriesSinceTest(startToken, Long.MAX_VALUE, state.allKeys.keySet(), absoluteEndToken);

        // Generate a journal based token from invalidOffset (not in journal and is invalid)
        startToken = new StoreFindToken(invalidOffset, state.sessionId, state.incarnationId, false, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
        doFindEntriesSinceTest(startToken, Long.MAX_VALUE, state.allKeys.keySet(), absoluteEndToken);
      }
    }
  }

  /**
   * Tests generating a journal based token mapping to first indexEntry in last index segment. it will return the next
   * journal based token in journal.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenWhenInputJournalBasedTokenTestCase1() throws StoreException {
    assumeTrue(isLogSegmented);
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    Offset validOffset =
        new Offset(lastIndexSegment.getLogSegmentName(), lastIndexSegment.getStartOffset().getOffset());
    Offset nextOffset = stateForTokenTest.index.journal.getEntriesSince(validOffset, false).get(0).getOffset();
    StoreFindToken startToken =
        new StoreFindToken(validOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
            lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
            lastIndexSegment.getResetKeyLifeVersion());
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    StoreFindToken expectedToken =
        new StoreFindToken(nextOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
            lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
            lastIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);
  }

  /**
   * Test generating the last journal based token, it will return the original token.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenWhenInputJournalBasedTokenTestCase2() throws StoreException {
    assumeTrue(isLogSegmented);
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    Offset validOffset = stateForTokenTest.index.journal.getLastOffset();
    StoreFindToken startToken =
        new StoreFindToken(validOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
            lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
            lastIndexSegment.getResetKeyLifeVersion());
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    StoreFindToken expectedToken = startToken;
    compareTokens(expectedToken, token);
  }

  /**
   * Test generating a index based token mapping to last indexEntry not in journal. it will return the first journal
   * based token in journal.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenWhenInputIndexBasedTokenTest() throws StoreException {
    assumeTrue(isLogSegmented);
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    IndexSegment secondLastIndexSegment =
        stateForTokenTest.index.getIndexSegments().lowerEntry(lastIndexSegment.getStartOffset()).getValue();
    StoreKey lastStoreKeyInLastIndexSegment =
        secondLastIndexSegment.listIterator(DEFAULT_MAX_IN_MEM_ELEMENTS - 1).next().getKey();
    Offset firstOffsetInJournal = stateForTokenTest.index.journal.getFirstOffset();
    StoreFindToken startToken =
        new StoreFindToken(lastStoreKeyInLastIndexSegment, secondLastIndexSegment.getStartOffset(),
            stateForTokenTest.sessionId, stateForTokenTest.incarnationId, secondLastIndexSegment.getResetKey(),
            secondLastIndexSegment.getResetKeyType(), secondLastIndexSegment.getResetKeyLifeVersion());
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    StoreFindToken expectedToken;
    expectedToken =
        new StoreFindToken(firstOffsetInJournal, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
            lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
            lastIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);
  }

  /**
   * Test generating a journal based token and close last log segment, should return the index based token point to first
   * index entry of last index segment.
   * Then input the output index based token and check if we can move the token forward until it reached to the end of
   * last index segment.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenWhenLastSegmentAutoClosedTestCase1() throws StoreException {
    assumeTrue(isLogSegmented);
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    Offset validOffset = lastIndexSegment.getStartOffset();
    StoreFindToken startToken =
        new StoreFindToken(validOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
            lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
            lastIndexSegment.getResetKeyLifeVersion());
    autoCloseLastLogSegmentAndCleanUpJournal();
    StoreFindToken token = startToken;
    StoreFindToken expectedToken;
    int totalIndexEntryInLastIndexSegment = lastIndexSegment.size();
    for (int i = 0; i <= totalIndexEntryInLastIndexSegment; i++) {
      FindInfo findInfo = stateForTokenTest.index.findEntriesSince(token, 1);
      token = (StoreFindToken) findInfo.getFindToken();
      if (i < totalIndexEntryInLastIndexSegment) {
        //the index based token is moving forward after call findEntriesSince until it reached to the end of last index segment.
        expectedToken = new StoreFindToken(lastIndexSegment.listIterator(i + 1).previous().getKey(),
            lastIndexSegment.getStartOffset(), stateForTokenTest.sessionId, stateForTokenTest.incarnationId,
            lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
            lastIndexSegment.getResetKeyLifeVersion());
      } else {
        //after index based token point to the last entry of index segment, should return directly return the original index based token.
        expectedToken =
            new StoreFindToken(lastIndexSegment.listIterator(totalIndexEntryInLastIndexSegment).previous().getKey(),
                lastIndexSegment.getStartOffset(), stateForTokenTest.sessionId, stateForTokenTest.incarnationId,
                lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
                lastIndexSegment.getResetKeyLifeVersion());
      }
      compareTokens(expectedToken, token);
    }
  }

  /**
   * Test generating an index based token for second last index segment and close last log segment, should return index based
   * token point to the beginning of last Index Segment.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenWhenLastSegmentAutoClosedTestCase2() throws StoreException {
    assumeTrue(isLogSegmented);
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    IndexSegment secondLastIndexSegment =
        stateForTokenTest.index.getIndexSegments().lowerEntry(lastIndexSegment.getStartOffset()).getValue();
    StoreKey secondLastStoreKeyInSecondLastIndexSegment =
        secondLastIndexSegment.listIterator(secondLastIndexSegment.size()).previous().getKey();
    StoreFindToken startToken =
        new StoreFindToken(secondLastStoreKeyInSecondLastIndexSegment, secondLastIndexSegment.getStartOffset(),
            stateForTokenTest.sessionId, stateForTokenTest.incarnationId, secondLastIndexSegment.getResetKey(),
            secondLastIndexSegment.getResetKeyType(), secondLastIndexSegment.getResetKeyLifeVersion());
    autoCloseLastLogSegmentAndCleanUpJournal();
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    StoreFindToken expectedToken =
        new StoreFindToken(lastIndexSegment.listIterator(1).previous().getKey(), lastIndexSegment.getStartOffset(),
            stateForTokenTest.sessionId, stateForTokenTest.incarnationId, lastIndexSegment.getResetKey(),
            lastIndexSegment.getResetKeyType(), lastIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);
  }

  /**
   * Test closing last log segment and generating an index based token pointing to last entry of last index segment,
   * it should return the same index token.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenWhenLastSegmentAutoClosedTestCase3() throws StoreException {
    assumeTrue(isLogSegmented);
    autoCloseLastLogSegmentAndCleanUpJournal();
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    Offset validOffset =
        new Offset(lastIndexSegment.getLogSegmentName(), lastIndexSegment.getStartOffset().getOffset());
    StoreFindToken startToken =
        new StoreFindToken(lastIndexSegment.listIterator(lastIndexSegment.size()).previous().getKey(), validOffset,
            stateForTokenTest.sessionId, stateForTokenTest.incarnationId, lastIndexSegment.getResetKey(),
            lastIndexSegment.getResetKeyType(), lastIndexSegment.getResetKeyLifeVersion());
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    StoreFindToken expectedToken = startToken;
    compareTokens(expectedToken, token);
  }

  /**
   * Test generate a journal based token point to last entry of index segment, and close the last log segment and clean up journal.
   * It should first return the index based token point to the last IndexSegment startOffset and first key.
   * Test after two more entries added in current last log segment, when put the index based token point to last entry of last index segment,
   * should return the journal based token point to first entry of journal.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenWhenLastSegmentAutoClosedTestCase4() throws StoreException {
    assumeTrue(isLogSegmented);
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    Offset validOffset = stateForTokenTest.index.journal.getLastOffset();
    StoreFindToken startToken =
        new StoreFindToken(validOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
            lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
            lastIndexSegment.getResetKeyLifeVersion());
    autoCloseLastLogSegmentAndCleanUpJournal();
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    StoreFindToken expectedToken =
        new StoreFindToken(lastIndexSegment.iterator().next().getKey(), lastIndexSegment.getStartOffset(),
            stateForTokenTest.sessionId, stateForTokenTest.incarnationId, lastIndexSegment.getResetKey(),
            lastIndexSegment.getResetKeyType(), lastIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);

    stateForTokenTest.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);

    //put the index based token point to last entry of last index segment, should return the journal based token point to first entry of journal.
    lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    IndexSegment secondLastIndexSegment =
        stateForTokenTest.index.getIndexSegments().lowerEntry(lastIndexSegment.getStartOffset()).getValue();
    StoreKey lastStoreKeyInSecondLastIndexSegment =
        secondLastIndexSegment.listIterator(secondLastIndexSegment.size()).previous().getKey();
    startToken = new StoreFindToken(lastStoreKeyInSecondLastIndexSegment, secondLastIndexSegment.getStartOffset(),
        stateForTokenTest.sessionId, stateForTokenTest.incarnationId, secondLastIndexSegment.getResetKey(),
        secondLastIndexSegment.getResetKeyType(), secondLastIndexSegment.getResetKeyLifeVersion());
    findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    token = (StoreFindToken) findInfo.getFindToken();
    expectedToken = new StoreFindToken(stateForTokenTest.index.journal.getFirstOffset(), stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, false, lastIndexSegment.getResetKey(), lastIndexSegment.getResetKeyType(),
        lastIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);
  }

  /**
   * Tests cases where find token is rebuilt based on reset key.
   * @throws StoreException
   */
  @Test
  public void rebuildTokenBasedOnResetKeyTest() throws StoreException {
    assumeTrue(isLogSegmented);
    IndexSegment firstIndexSegment = stateForTokenTest.index.getIndexSegments().firstEntry().getValue();
    IndexSegment secondIndexSegment =
        stateForTokenTest.index.getIndexSegments().higherEntry(firstIndexSegment.getStartOffset()).getValue();
    StoreKey firstKey = firstIndexSegment.iterator().next().getKey();
    StoreKey keyFromFirstSegment = firstIndexSegment.listIterator(DEFAULT_MAX_IN_MEM_ELEMENTS - 2).next().getKey();
    // 1. generate an invalid index-based token with reset key and the key can be found in current index
    Offset invalidOffset =
        new Offset(firstIndexSegment.getLogSegmentName(), firstIndexSegment.getStartOffset().getOffset() - 1);
    StoreFindToken startToken = new StoreFindToken(keyFromFirstSegment, invalidOffset, stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, firstIndexSegment.getResetKey(), firstIndexSegment.getResetKeyType(),
        firstIndexSegment.getResetKeyLifeVersion());
    //  the invalid token should be revalidated by looking up reset key in index
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    // the key in the token should be first key in first index segment, as we only get one entry from new start point,
    // which is first key in first index segment (inclusive)
    StoreFindToken expectedToken =
        new StoreFindToken(firstKey, firstIndexSegment.getStartOffset(), stateForTokenTest.sessionId,
            stateForTokenTest.incarnationId, firstIndexSegment.getResetKey(), firstIndexSegment.getResetKeyType(),
            firstIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);

    // 2. generate an invalid journal based token with reset key pointing to 2nd index segment (this is to mock that the
    // reset key fell out of journal and was copied to 2nd index segment by compaction)
    IndexSegment lastIndexSegment = stateForTokenTest.index.getIndexSegments().lastEntry().getValue();
    invalidOffset = new Offset(lastIndexSegment.getLogSegmentName().getNextGenerationName(),
        lastIndexSegment.getStartOffset().getOffset() + 1);
    startToken = new StoreFindToken(invalidOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
        secondIndexSegment.getResetKey(), secondIndexSegment.getResetKeyType(),
        secondIndexSegment.getResetKeyLifeVersion());
    findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    token = (StoreFindToken) findInfo.getFindToken();
    // expected token should be the first key in 2nd index segment (index-based)
    StoreKey firstKeyFromSecondSegment = secondIndexSegment.iterator().next().getKey();
    expectedToken =
        new StoreFindToken(firstKeyFromSecondSegment, secondIndexSegment.getStartOffset(), stateForTokenTest.sessionId,
            stateForTokenTest.incarnationId, secondIndexSegment.getResetKey(), secondIndexSegment.getResetKeyType(),
            secondIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);

    // 3. generate an invalid token with reset key but the reset key is not found in current index
    StoreKey keyNotFound = stateForTokenTest.getUniqueId();
    startToken = new StoreFindToken(invalidOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false,
        keyNotFound, PersistentIndex.IndexEntryType.PUT, (short) 0);
    findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    token = (StoreFindToken) findInfo.getFindToken();
    // token should be reset to the very beginning which is the first index segment
    expectedToken = new StoreFindToken(firstKey, firstIndexSegment.getStartOffset(), stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, firstIndexSegment.getResetKey(), firstIndexSegment.getResetKeyType(),
        firstIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);
  }

  @Test
  public void rebuildTokenBasedOnCompactionHistoryTest() throws StoreException {
    assumeTrue(isLogSegmented);
    stateForTokenTest.index.enableRebuildTokenBasedOnCompactionHistory();

    IndexSegment firstIndexSegment = stateForTokenTest.index.getIndexSegments().firstEntry().getValue();
    IndexSegment secondIndexSegment =
        stateForTokenTest.index.getIndexSegments().higherEntry(firstIndexSegment.getStartOffset()).getValue();
    long now = stateForTokenTest.time.milliseconds();
    Map<Offset, Offset> indexSegmentOffsets = new HashMap<>();
    Offset secondIndexSegmentOffset = secondIndexSegment.getStartOffset();
    Offset before =
        new Offset(secondIndexSegmentOffset.getName().getNextGenerationName(), secondIndexSegmentOffset.getOffset());
    indexSegmentOffsets.put(before, secondIndexSegmentOffset);
    stateForTokenTest.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(now, indexSegmentOffsets, false);
    stateForTokenTest.index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();

    // 1. Generate an invalid index-based token with invalid offset, but it can be found in the before and after compaction map
    Offset invalidOffset = before;
    StoreKey keyFromFirstIndexSegment = firstIndexSegment.iterator().next().getKey();
    StoreKey keyFromSecondIndexSegment = secondIndexSegment.iterator().next().getKey();
    StoreFindToken startToken = new StoreFindToken(keyFromFirstIndexSegment, invalidOffset, stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, null, null, (short) -1);
    // The invalid token should be revalidated by looking up the before and after compaction index segment map
    FindInfo findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    StoreFindToken token = (StoreFindToken) findInfo.getFindToken();
    StoreFindToken expectedToken =
        new StoreFindToken(keyFromSecondIndexSegment, secondIndexSegmentOffset, stateForTokenTest.sessionId,
            stateForTokenTest.incarnationId, secondIndexSegment.getResetKey(), secondIndexSegment.getResetKeyType(),
            secondIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);

    // 2. Generate an invalid index-based token with invalid offset, but it can't be found in the before and after compaction map
    invalidOffset = new Offset(before.getName().getNextGenerationName(), before.getOffset());
    startToken = new StoreFindToken(keyFromSecondIndexSegment, invalidOffset, stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, null, null, (short) -1);
    // The invalid token will be revalidated to the beginning of the log
    findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    token = (StoreFindToken) findInfo.getFindToken();
    expectedToken =
        new StoreFindToken(keyFromFirstIndexSegment, firstIndexSegment.getStartOffset(), stateForTokenTest.sessionId,
            stateForTokenTest.incarnationId, firstIndexSegment.getResetKey(), firstIndexSegment.getResetKeyType(),
            firstIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);

    // 3. Generate an invalid index-based token with invalid offset, but it can be found by multiple redirections.
    Offset beforeBefore = new Offset(before.getName().getNextGenerationName(), before.getOffset() + 100);
    indexSegmentOffsets.put(beforeBefore, before);
    stateForTokenTest.index.updateBeforeAndAfterCompactionIndexSegmentOffsets(now, indexSegmentOffsets, false);
    stateForTokenTest.index.sanityCheckBeforeAndAfterCompactionIndexSegmentOffsets();
    invalidOffset = beforeBefore;
    startToken = new StoreFindToken(keyFromFirstIndexSegment, invalidOffset, stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, null, null, (short) -1);
    // The invalid token should be revalidated by looking up the before and after compaction index segment map
    findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    token = (StoreFindToken) findInfo.getFindToken();
    expectedToken = new StoreFindToken(keyFromSecondIndexSegment, secondIndexSegmentOffset, stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, secondIndexSegment.getResetKey(), secondIndexSegment.getResetKeyType(),
        secondIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);

    // Generate an invalid journal-based token with invalid offset, but it can be found in the before and after compaction map
    invalidOffset = new Offset(before.getName(), before.getOffset() + 100);
    startToken =
        new StoreFindToken(invalidOffset, stateForTokenTest.sessionId, stateForTokenTest.incarnationId, false, null,
            null, (short) -1);
    // The invalid token should be revalidated by looking up the before and after compaction index segment map
    findInfo = stateForTokenTest.index.findEntriesSince(startToken, 1);
    token = (StoreFindToken) findInfo.getFindToken();
    expectedToken = new StoreFindToken(keyFromSecondIndexSegment, secondIndexSegmentOffset, stateForTokenTest.sessionId,
        stateForTokenTest.incarnationId, secondIndexSegment.getResetKey(), secondIndexSegment.getResetKeyType(),
        secondIndexSegment.getResetKeyLifeVersion());
    compareTokens(expectedToken, token);
  }

  /**
   * Auto close last log segment and clean up journal.
   * @throws StoreException
   */
  private void autoCloseLastLogSegmentAndCleanUpJournal() throws StoreException {
    CompactionPolicySwitchInfo compactionPolicySwitchInfo =
        new CompactionPolicySwitchInfo(System.currentTimeMillis(), true);
    backUpCompactionPolicyInfo(tokenTestDir.toString(), compactionPolicySwitchInfo);
    stateForTokenTest.index.log.autoCloseLastLogSegmentIfQualified();
    stateForTokenTest.index.journal.cleanUpJournal();
  }

  /**
   * Back up compactionPolicySwitchInfo to file in tempDir.
   * @param tempDir the dir which stores the compactionPolicySwitchInfo file
   * @param compactionPolicySwitchInfo the {@link CompactionPolicySwitchInfo} which contains the info for compaction policy switching.
   */
  private void backUpCompactionPolicyInfo(String tempDir, CompactionPolicySwitchInfo compactionPolicySwitchInfo) {
    if (tempDir != null) {
      File tempFile = new File(tempDir, COMPACT_POLICY_INFO_FILE_NAME_V2 + ".temp");
      try {
        tempFile.createNewFile();
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempFile, compactionPolicySwitchInfo);
        tempFile.renameTo(new File(tempDir, COMPACT_POLICY_INFO_FILE_NAME_V2));
      } catch (IOException e) {
        logger.error("Exception while store compaction policy info for local report. Output file path - {}",
            tempFile.getAbsolutePath(), e);
      }
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
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceOnCrashRestartTest() throws StoreException {
    UUID oldSessionId = state.sessionId;
    state.addPutEntries(3, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    state.reloadIndex(true, true);
    // create a token that will be past the log end offset on start up after restart.
    StoreFindToken startToken = new StoreFindToken(new Offset(state.log.getEndOffset().getName(),
        (state.log.getEndOffset().getOffset() + (2 * CuratedLogIndexState.PUT_RECORD_SIZE))), oldSessionId,
        state.incarnationId, false, null, null, UNINITIALIZED_RESET_KEY_VERSION);
    // end token should point to log end offset on startup
    long bytesRead = state.index.getAbsolutePositionInLogForOffset(state.log.getEndOffset());
    IndexSegment segmentOfToken = state.index.getIndexSegments().floorEntry(state.log.getEndOffset()).getValue();
    StoreFindToken expectedEndToken =
        new StoreFindToken(state.log.getEndOffset(), state.sessionId, state.incarnationId, true,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
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
    segmentOfToken = state.index.getIndexSegments().floorEntry(state.index.journal.getLastOffset()).getValue();
    expectedEndToken =
        new StoreFindToken(state.index.journal.getLastOffset(), state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
    expectedEndToken.setBytesRead(bytesRead);
    doFindEntriesSinceTest((StoreFindToken) findInfo.getFindToken(), Long.MAX_VALUE, expectedEntries, expectedEndToken);
  }

  /**
   * Tests behaviour of {@link PersistentIndex#findEntriesSince(FindToken, long)} relating to incarnationId
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceIncarnationIdTest() throws StoreException {
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
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    long operationTimeMs = state.time.milliseconds();

    // add to allKeys() so that doFindEntriesSinceTest() works correctly.
    IndexValue putValue =
        new IndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, firstRecordFileSpan.getStartOffset(), Utils.Infinite_Time,
            operationTimeMs, accountId, containerId);
    state.allKeys.computeIfAbsent(newId, k -> new TreeSet<>()).add(putValue);
    state.recovery = (read, startOffset, endOffset, factory) -> Collections.singletonList(
        new MessageInfo(newId, CuratedLogIndexState.PUT_RECORD_SIZE, accountId, containerId, operationTimeMs));
    // change in incarnationId
    state.incarnationId = UUID.randomUUID();
    state.reloadIndex(true, true);

    long bytesRead = state.index.getAbsolutePositionInLogForOffset(firstRecordFileSpan.getEndOffset());
    // create a token that will be past the index end offset on startup after recovery with old incarnationId
    StoreFindToken startToken =
        new StoreFindToken(secondRecordFileSpan.getEndOffset(), oldSessionId, oldIncarnationId, false, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
    // token should get reset internally, all keys should be returned and the returned token should be pointing to
    // start offset of firstRecordFileSpan.
    IndexSegment segmentOfToken =
        state.index.getIndexSegments().floorEntry(firstRecordFileSpan.getStartOffset()).getValue();
    StoreFindToken expectedEndToken =
        new StoreFindToken(firstRecordFileSpan.getStartOffset(), state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
    expectedEndToken.setBytesRead(bytesRead);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, state.allKeys.keySet(), expectedEndToken);

    // create a token that is not past the index end offset on startup after recovery with old incarnationId.
    // token should get reset internally, all keys should be returned and the returned token should be be pointing to
    // start offset of firstRecordFileSpan.
    startToken = new StoreFindToken(lastRecordOffset, oldSessionId, oldIncarnationId, false, null, null,
        UNINITIALIZED_RESET_KEY_VERSION);
    expectedEndToken =
        new StoreFindToken(firstRecordFileSpan.getStartOffset(), state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
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
   * @throws StoreException
   */
  @Test
  public void findDeletedEntriesSinceTest() throws StoreException {
    // add some more entries so that the journal gets entries across segments and doesn't start at the beginning
    // of an index segment.
    state.addPutEntries(7, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    MockId idToDelete = state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false);
    state.addDeleteEntry(idToDelete);

    // token with log end offset should not return anything
    StoreFindToken token =
        new StoreFindToken(state.log.getEndOffset(), state.sessionId, state.incarnationId, false, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
    doFindDeletedEntriesSinceTest(token, Long.MAX_VALUE, Collections.emptySet(), token);

    findDeletedEntriesSinceToIndexBasedTest();
    findDeletedEntriesSinceToJournalBasedTest();
    findDeletedEntriesSinceOneByOneTest();
    findDeletedEntriesSinceIndexBasedTokenForOffsetInJournalTest();

    findEntriesSinceInEmptyIndexTest(true);
  }

  /**
   * Tests the index segment roll over when there is a change in IndexValue size. With introduction of
   * {@link PersistentIndex#VERSION_1} there is a change in IndexValue.
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testIndexSegmentRollOverNewIndexSegmentVersion() throws StoreException, IOException {
    indexSegmentRollOverTest(PersistentIndex.VERSION_0, true);
    indexSegmentRollOverTest(PersistentIndex.VERSION_0, false);
    indexSegmentRollOverTest(PersistentIndex.VERSION_1, true);
    indexSegmentRollOverTest(PersistentIndex.VERSION_1, false);
  }

  /**
   * Tests index segment rollover behavior with respect to key size changes. For the current version
   * ({@link PersistentIndex#VERSION_2}), a change in key size should cause a rollover only if the persistedEntrySize of
   * the active segment is smaller than the entry size of the incoming entry.
   * Also test index segment rollover with respect to a value size change.
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testIndexSegmentRollOverKeySizeAndValueSizeChange() throws StoreException, IOException {
    state.closeAndClearIndex();
    int persistedEntryMinBytes = 100;
    state.properties.put("store.index.persisted.entry.min.bytes", Long.toString(persistedEntryMinBytes));
    state.properties.put("store.index.max.number.of.inmem.elements", Integer.toString(10));
    StoreConfig config = new StoreConfig(new VerifiableProperties(state.properties));
    state.reloadIndex(false, false);

    List<IndexEntry> indexEntries = new ArrayList<>();
    int indexCount = state.index.getIndexSegments().size();
    int serOverheadBytes = state.getUniqueId(10).sizeInBytes() - 10;

    int latestSegmentExpectedEntrySize = config.storeIndexPersistedEntryMinBytes;
    // add first entry with size under storeIndexPersistedEntryMinBytes.
    int indexValueSize =
        PersistentIndex.CURRENT_VERSION >= PersistentIndex.VERSION_3 ? IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3_V4
            : IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2;
    int keySize = config.storeIndexPersistedEntryMinBytes / 2 - indexValueSize - serOverheadBytes;
    addEntriesAndAssert(indexEntries, keySize, 1, ++indexCount, latestSegmentExpectedEntrySize, false);

    // Now, the active segment consists of one element. Add 2nd element of a smaller key size; and entry size still under
    // storeIndexPersistedEntryMinBytes.
    keySize = keySize / 2;
    addEntriesAndAssert(indexEntries, keySize, 1, indexCount, latestSegmentExpectedEntrySize, false);

    // 3rd element with key size greater than the first entry, but still under storeIndexPersistedEntryMinBytes.
    keySize = keySize * 3;
    addEntriesAndAssert(indexEntries, keySize, 1, indexCount, latestSegmentExpectedEntrySize, false);

    // 4th element with key size increase, and entry size at exactly storeIndexPersistedEntryMinBytes.
    // This should also not cause a rollover.
    keySize = config.storeIndexPersistedEntryMinBytes - indexValueSize - serOverheadBytes;
    addEntriesAndAssert(indexEntries, keySize, 1, indexCount, latestSegmentExpectedEntrySize, false);

    // 5th element key size increase, and above storeIndexPersistedEntryMinBytes. This continues to be supported via
    // a rollover.
    keySize = config.storeIndexPersistedEntryMinBytes - indexValueSize - serOverheadBytes + 1;
    addEntriesAndAssert(indexEntries, keySize, 1, ++indexCount, ++latestSegmentExpectedEntrySize, false);

    // 2nd and 3rd element in the next segment of original size. This should be accommodated in the same segment.
    keySize = config.storeIndexPersistedEntryMinBytes - indexValueSize - serOverheadBytes;
    addEntriesAndAssert(indexEntries, keySize, 2, indexCount, latestSegmentExpectedEntrySize, false);

    // verify index values
    verifyIndexValues(indexEntries);

    // Verify that a decrease in the config value for persistedEntryMinBytes does not affect the loaded segment.
    // Now close and reload index with a change in the minPersistedBytes.
    state.properties.put("store.index.persisted.entry.min.bytes", Long.toString(persistedEntryMinBytes / 2));
    state.reloadIndex(true, false);
    keySize = latestSegmentExpectedEntrySize - indexValueSize - serOverheadBytes;
    addEntriesAndAssert(indexEntries, keySize, 7, indexCount, latestSegmentExpectedEntrySize, false);

    // At this point, index will rollover due to max number of entries being reached. Verify that the new segment that is
    // created honors the new config value.
    config = new StoreConfig(new VerifiableProperties(state.properties));
    latestSegmentExpectedEntrySize = config.storeIndexPersistedEntryMinBytes;
    keySize = config.storeIndexPersistedEntryMinBytes - indexValueSize - serOverheadBytes;
    addEntriesAndAssert(indexEntries, keySize, 1, ++indexCount, latestSegmentExpectedEntrySize, false);

    // verify index values
    verifyIndexValues(indexEntries);

    // Verify that an increase in the config value for persistedEntryMinBytes does not affect the loaded segment.
    // Now close and reload index with a change in the minPersistedBytes.
    state.properties.put("store.index.persisted.entry.min.bytes", Long.toString(persistedEntryMinBytes * 2));
    state.reloadIndex(true, false);
    // Make sure we add entries that can fit in the latest segment.
    keySize = latestSegmentExpectedEntrySize - indexValueSize - serOverheadBytes;
    addEntriesAndAssert(indexEntries, keySize, 9, indexCount, latestSegmentExpectedEntrySize, false);

    // At this point, index will rollover due to max number of entries being reached. Verify that the new segment that is
    // created has the new entry size.
    config = new StoreConfig(new VerifiableProperties(state.properties));
    keySize = latestSegmentExpectedEntrySize - indexValueSize - serOverheadBytes;
    latestSegmentExpectedEntrySize = config.storeIndexPersistedEntryMinBytes;
    addEntriesAndAssert(indexEntries, keySize, 1, ++indexCount, latestSegmentExpectedEntrySize, false);

    // verify index values
    verifyIndexValues(indexEntries);

    // Verify that a value size change will cause a rollover
    addEntriesAndAssert(indexEntries, keySize, 1, ++indexCount, latestSegmentExpectedEntrySize, true);
    verifyIndexValues(indexEntries);
  }

  /**
   * Tests the Index persistor for all cases
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void indexPersistorTest() throws IOException, StoreException {
    // make sure persistor is persisting all segments
    int numIndexSegments = state.index.getIndexSegments().size();
    state.index.persistIndex();
    List<File> indexFiles = Arrays.asList(tempDir.listFiles(PersistentIndex.INDEX_SEGMENT_FILE_FILTER));
    indexFiles.sort(PersistentIndex.INDEX_SEGMENT_FILE_COMPARATOR);
    assertEquals("Not as many files written as there were index segments", numIndexSegments, indexFiles.size());
    long prevFilePersistTimeMs = Long.MIN_VALUE;
    for (File indexFile : indexFiles) {
      long thisFilePersistTimeMs = indexFile.lastModified();
      assertTrue("A later index segment was persisted earlier", prevFilePersistTimeMs <= thisFilePersistTimeMs);
      prevFilePersistTimeMs = thisFilePersistTimeMs;
    }

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
        new IndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, fileSpan.getStartOffset(), Utils.Infinite_Time,
            state.time.milliseconds(), Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM));
    state.index.addToIndex(new IndexEntry(state.getUniqueId(), value), fileSpan);
    try {
      state.index.persistIndex();
      fail("Should have thrown exception since index has entries which log does not have");
    } catch (StoreException e) {
      assertEquals("StoreException error code mismatch ", StoreErrorCodes.Illegal_Index_State, e.getErrorCode());
    }
    // append to log so that log and index are in sync with each other
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
    // test persistor.write() failure due to I/O error
    Log mockLog = Mockito.spy(state.log);
    state.log = mockLog;
    state.reloadIndex(true, false);
    try {
      doThrow(new IOException(StoreException.IO_ERROR_STR)).when(mockLog).flush();
      state.index.close(false);
      fail("Should have thrown exception due to I/O error");
    } catch (StoreException e) {
      assertEquals("StoreException error code mismatch ", StoreErrorCodes.IOError, e.getErrorCode());
    }
    // test that when IOException's error message is null, the error code should be Unknown_Error
    try {
      doThrow(new IOException()).when(mockLog).flush();
      state.index.close(false);
      fail("Should have thrown exception due to I/O error");
    } catch (StoreException e) {
      assertEquals("StoreException error code mismatch ", StoreErrorCodes.Unknown_Error, e.getErrorCode());
    }
    Mockito.reset(mockLog);
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
    Offset badSegmentOffset =
        new Offset(LogSegmentName.fromPositionAndGeneration(state.index.getLogSegmentCount() + 1, 0), 0);
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
   * @throws StoreException
   */
  @Test
  public void hardDeleteNullTest() throws StoreException {
    state.hardDelete = null;
    state.reloadIndex(true, false);
    state.addPutEntries(1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    MockId idToCheck = state.logOrder.lastEntry().getValue().getFirst();
    state.reloadIndex(true, false);
    verifyValue(idToCheck, state.index.findKey(idToCheck));
  }

  /**
   * Tests correctness of {@link PersistentIndex#getIndexSegmentFilesForLogSegment(String, LogSegmentName)} and makes sure
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
   * Tests {@link PersistentIndex#close(boolean)} can correctly cancel the scheduled persistor task and makes sure no persistor
   * is running background after index closed.
   * @throws StoreException
   * @throws InterruptedException
   */
  @Test
  public void closeIndexToCancelPersistorTest() throws StoreException, InterruptedException {
    long SCHEDULER_PERIOD_MS = 10;
    state.index.close(false);
    // re-initialize index by using mock scheduler (the intention is to speed up testing by using shorter period)
    ScheduledThreadPoolExecutor scheduler = (ScheduledThreadPoolExecutor) Utils.newScheduler(1, false);
    ScheduledThreadPoolExecutor mockScheduler = Mockito.spy(scheduler);
    AtomicReference<ScheduledFuture> persistorTask = new AtomicReference<>();
    class MockPersistor implements Runnable {
      private CountDownLatch invokeCountDown;

      @Override
      public void run() {
        invokeCountDown.countDown();
      }
    }
    MockPersistor mockPersistor = new MockPersistor();
    mockPersistor.invokeCountDown = new CountDownLatch(1);
    doAnswer(invocation -> {
      persistorTask.set(
          mockScheduler.scheduleAtFixedRate(mockPersistor, 0, SCHEDULER_PERIOD_MS, TimeUnit.MILLISECONDS));
      return persistorTask.get();
    }).when(mockScheduler)
        .scheduleAtFixedRate(any(PersistentIndex.IndexPersistor.class), anyLong(), anyLong(),
            any(TimeUnit.SECONDS.getClass()));
    state.initIndex(mockScheduler);
    // verify that the persistor task is successfully scheduled
    assertTrue("The persistor task wasn't invoked within the expected time",
        mockPersistor.invokeCountDown.await(2 * SCHEDULER_PERIOD_MS, TimeUnit.MILLISECONDS));
    state.index.close(false);
    mockPersistor.invokeCountDown = new CountDownLatch(1);
    // verify that the persisitor task is canceled after index closed and is never invoked again.
    assertTrue("The persistor task should be canceled after index closed", persistorTask.get().isCancelled());
    assertFalse("The persistor task should not be invoked again",
        mockPersistor.invokeCountDown.await(2 * SCHEDULER_PERIOD_MS, TimeUnit.MILLISECONDS));
  }

  /**
   * Tests {@link PersistentIndex#cleanupIndexSegmentFilesForLogSegment(String, LogSegmentName)} and makes sure it deletes all
   * the relevant files and no more.
   * @throws StoreException
   */
  @Test
  public void cleanupIndexSegmentFilesForLogSegmentTest() throws StoreException {
    state.index.close(false);
    LogSegment logSegment = state.log.getFirstSegment();
    while (logSegment != null) {
      LogSegment nextSegment = state.log.getNextSegment(logSegment);
      final LogSegmentName logSegmentName = logSegment.getName();
      int totalFilesInDir = tempDir.listFiles().length;
      File[] filesToCheck = tempDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(logSegmentName.toString()) && (
              name.endsWith(IndexSegment.INDEX_SEGMENT_FILE_NAME_SUFFIX) || name.endsWith(
                  IndexSegment.BLOOM_FILE_NAME_SUFFIX));
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
      // test list index segment files failure (use incorrect path to simulate I/O errors)
      try {
        PersistentIndex.cleanupIndexSegmentFilesForLogSegment("Non-Directory", logSegmentName);
        fail("should fail when listing index segment files");
      } catch (StoreException e) {
        assertEquals("Mismatch in error code", StoreErrorCodes.IOError, e.getErrorCode());
      }
      PersistentIndex.cleanupIndexSegmentFilesForLogSegment(tempDir.getAbsolutePath(), logSegmentName);
      for (File fileToCheck : filesToCheck) {
        assertFalse(fileToCheck + " should have been deleted", fileToCheck.exists());
      }
      assertEquals("More than the expected number of files were deleted", totalFilesInDir - filesToCheck.length,
          tempDir.listFiles().length);
      logSegment = nextSegment;
    }
  }

  /**
   * Tests the behavior of {@link Journal} bootstrap.
   */
  @Test
  public void journalBootstrapTest() throws StoreException {
    if (state.getMaxInMemElements() <= 1) {
      fail("This test can work only if the max in mem elements config > 1");
    }
    if (state.index.getIndexSegments().lastEntry().getValue().getNumberOfItems() <= 1) {
      state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    }
    assertTrue("There should be more than one element in the last index segment for this test to work",
        state.index.getIndexSegments().lastEntry().getValue().getNumberOfItems() > 1);
    // change the config to lower max journal size
    state.properties.put("store.index.max.number.of.inmem.elements", "1");
    state.reloadIndex(true, false);
    int entriesInJournal = state.index.journal.getCurrentNumberOfEntries();
    assertEquals("Number of entries in the journal should be equal to the number of elements in the last index segment",
        state.index.getIndexSegments().lastEntry().getValue().getNumberOfItems(), entriesInJournal);
    // add some entries to trigger rollover.
    state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
    assertEquals("Number of entries in the journal should not have changed", entriesInJournal,
        state.index.journal.getCurrentNumberOfEntries());
    // Reload the index and the journal size should now reflect the config change
    state.reloadIndex(true, false);
    assertEquals("Number of entries in the journal should be exactly 1", 1,
        state.index.journal.getCurrentNumberOfEntries());
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
      assertEquals("StoreKey not as expected", id, options.getMessageInfo().getStoreKey());
      assertEquals("Log Segment Name not as expected", putEntryValue.getOffset().getName(),
          options.getLogSegmentName());
      assertEquals("Offset not as expected for entry " + putEntryValue.toString(),
          putEntryValue.getOffset().getOffset(), options.getOffset());
      assertEquals("Size not as expected", putEntryValue.getSize(), options.getMessageInfo().getSize());
      assertEquals("ExpiresAtMs not as expected", putEntryValue.getExpiresAtMs(),
          options.getMessageInfo().getExpirationTimeInMs());
      if (storeGetOptions.contains(StoreGetOptions.Store_Include_Deleted) && state.deletedKeys.contains(id)) {
        assertTrue("MessageInfo for deleted messages should have deleted flag true",
            options.getMessageInfo().isDeleted());
      }
    } catch (StoreException e) {
      if (expectedErrorCode == null) {
        throw e;
      } else {
        assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
      }
    }
  }

  /**
   * Tests and verifies an undelete with an expected life version
   * @param targetKey the {@link StoreKey} to look up
   * @param expectedLifeVersion the expected lifeVersion
   * @param expectTtlUpdateSet true when expecting ttl update flag is set from returned {@link IndexValue}
   * @throws StoreException
   */
  private void undeleteKeyAndVerify(StoreKey targetKey, short expectedLifeVersion, boolean expectTtlUpdateSet)
      throws StoreException {
    assertTrue("targetKey is not deleted", state.index.findKey(targetKey).isDelete());
    assertFalse("targetKey is undeleted early", state.index.findKey(targetKey).isUndelete());
    IndexValue prevValue = state.index.findKey(targetKey);
    assertEquals("Life version isn't " + (expectedLifeVersion - 1) + " but " + prevValue.getLifeVersion(),
        expectedLifeVersion - 1, prevValue.getLifeVersion());
    state.appendToLog(UNDELETE_RECORD_SIZE);
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), UNDELETE_RECORD_SIZE);
    long operationTime = state.time.milliseconds();
    state.index.markAsUndeleted(targetKey, fileSpan, operationTime);
    IndexValue value = state.index.findKey(targetKey);
    checkUndelete(value, expectTtlUpdateSet, expectedLifeVersion, ((MockId) targetKey).getAccountId(),
        ((MockId) targetKey).getContainerId(), UNDELETE_RECORD_SIZE, fileSpan.getStartOffset(),
        prevValue.getExpiresAtMs(), operationTime);
  }

  private void checkUndelete(IndexValue value, boolean expectedTtlUpdate, short expectedLifeVersion,
      short expectedAccountId, short expectedContainerId, long expectedSize, Offset expectedOffset,
      long expectedExpiresAtMs, long expectedOperationTime) {
    assertNotNull(value);
    assertTrue(value.isUndelete());
    assertFalse(value.isDelete());
    assertEquals(expectedTtlUpdate, value.isTtlUpdate());
    assertEquals(expectedLifeVersion, value.getLifeVersion());
    assertEquals(expectedAccountId, value.getAccountId());
    assertEquals(expectedContainerId, value.getContainerId());
    assertEquals(expectedSize, value.getSize());
    assertEquals(expectedOffset, value.getOffset());
    assertEquals(expectedExpiresAtMs, value.getExpiresAtMs());
    assertEquals(expectedOperationTime, value.getOperationTimeInMs());
  }

  /**
   * Tests and verifies a delete with an expected life version
   * @param key the {@link StoreKey} to look up
   * @param expectedLifeVersion the expected lifeVersion
   * @throws StoreException
   */
  private void deleteKeyAndVerify(StoreKey key, short expectedLifeVersion) throws StoreException {
    assertFalse("targetKey is already deleted", state.index.findKey(key).isDelete());
    short actualLifeVersion = state.index.findKey(key).getLifeVersion();
    assertEquals("Life version isn't " + expectedLifeVersion + " but " + actualLifeVersion, expectedLifeVersion,
        actualLifeVersion);
    state.appendToLog(DELETE_RECORD_SIZE);
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), DELETE_RECORD_SIZE);
    state.index.markAsDeleted(key, fileSpan, System.currentTimeMillis());
    assertFalse("targetKey is undeleted", state.index.findKey(key).isUndelete());
    assertTrue("targetKey is not deleted", state.index.findKey(key).isDelete());
    actualLifeVersion = state.index.findKey(key).getLifeVersion();
    assertEquals("Life version isn't " + expectedLifeVersion + " but " + actualLifeVersion, expectedLifeVersion,
        actualLifeVersion);
  }

  /**
   * Tests and verifies a ttlUpdate with an expected life version
   * @param key the {@link StoreKey} to look up
   * @param expectedLifeVersion the expected lifeVersion
   * @throws StoreException
   */
  private void ttlUpdateKeyAndVerify(StoreKey key, short expectedLifeVersion) throws StoreException {
    assertFalse("targetKey is already ttlUpdated", state.index.findKey(key).isTtlUpdate());
    short actualLifeVersion = state.index.findKey(key).getLifeVersion();
    assertEquals("Life version isn't " + expectedLifeVersion + " but " + actualLifeVersion, expectedLifeVersion,
        actualLifeVersion);
    state.appendToLog(TTL_UPDATE_RECORD_SIZE);
    FileSpan fileSpan = state.log.getFileSpanForMessage(state.index.getCurrentEndOffset(), TTL_UPDATE_RECORD_SIZE);
    state.index.markAsPermanent(key, fileSpan, System.currentTimeMillis());
    assertTrue("targetKey is not ttlUpdated", state.index.findKey(key).isTtlUpdate());
    actualLifeVersion = state.index.findKey(key).getLifeVersion();
    assertEquals("Life version isn't " + expectedLifeVersion + " but " + actualLifeVersion, expectedLifeVersion,
        actualLifeVersion);
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
    verifyValue(id, valueFromFind, null,
        EnumSet.of(PersistentIndex.IndexEntryType.PUT, PersistentIndex.IndexEntryType.DELETE,
            PersistentIndex.IndexEntryType.UNDELETE));
  }

  /**
   * Verifies that {@code valueFromFind} matches the expected value from {@link CuratedLogIndexState#referenceIndex}.
   * @param id the {@link MockId} whose value is required.
   * @param valueFromFind the {@link IndexValue} that needs to be verified.
   * @param fileSpan the {@link FileSpan} to use to get expected value. Can be {@code null}
   * @param types the types of {@link PersistentIndex.IndexEntryType} to look for. The latest entry matching one of the
   *              types will be returned
   */
  private void verifyValue(MockId id, IndexValue valueFromFind, FileSpan fileSpan,
      EnumSet<PersistentIndex.IndexEntryType> types) {
    if (state.allKeys.containsKey(id)) {
      IndexValue expectedValue = state.getExpectedValue(id, types, fileSpan);
      if (expectedValue != null) {
        assertNotNull("Value should be successfully fetched", valueFromFind);
        assertEquals("Bytes from value from index not as expected for " + id + " ex: " + expectedValue + ", actual: "
            + valueFromFind, expectedValue.getBytes(), valueFromFind.getBytes());
      } else {
        assertNull("There should be no value for " + id, valueFromFind);
      }
    } else {
      assertNull("There should have been no value returned", valueFromFind);
    }
  }

  // findKeyWithFileSpanTest() helpers

  /**
   * Does the test for {@link PersistentIndex#findKey(StoreKey, FileSpan, EnumSet)} with the given {@code id}
   * @param id the {@link MockId} to do the test for
   * @param types the types of {@link PersistentIndex.IndexEntryType} to look for. The latest entry matching one of the
   *              types will be returned
   * @throws StoreException
   */
  private void doFindKeyWithFileSpanTest(MockId id, EnumSet<PersistentIndex.IndexEntryType> types)
      throws StoreException {
    IndexValue expectedValue = state.getExpectedValue(id, types, null);
    verifyValue(id, state.index.findKey(id, null, types), null, types);
    if (expectedValue == null) {
      // there is no filespan testing required if the given type of entry does not exist
      return;
    }
    FileSpan fileSpanForMessage = state.log.getFileSpanForMessage(expectedValue.getOffset(), expectedValue.getSize());

    // FileSpan that is exactly that of the message
    verifyValue(id, state.index.findKey(id, fileSpanForMessage, types), fileSpanForMessage, types);

    Offset recordOffset = expectedValue.getOffset();
    Offset indexSegmentStartOffset = state.referenceIndex.floorKey(recordOffset);
    Offset lowerSegmentStartOffset = state.referenceIndex.lowerKey(indexSegmentStartOffset);
    Offset higherSegmentStartOffset = state.referenceIndex.higherKey(indexSegmentStartOffset);

    // FileSpan from start offset of message to index end offset
    FileSpan fileSpan = new FileSpan(fileSpanForMessage.getStartOffset(), state.index.getCurrentEndOffset());
    verifyValue(id, state.index.findKey(id, fileSpan, types), fileSpan, types);

    // FileSpan from start offset of index to end offset of message
    fileSpan = new FileSpan(state.index.getStartOffset(), fileSpanForMessage.getEndOffset());
    verifyValue(id, state.index.findKey(id, fileSpan, types), fileSpan, types);

    // FileSpan that includes the message
    Offset startOffset = lowerSegmentStartOffset == null ? indexSegmentStartOffset : lowerSegmentStartOffset;
    Offset endOffset = higherSegmentStartOffset == null ? state.index.getCurrentEndOffset() : higherSegmentStartOffset;
    fileSpan = new FileSpan(startOffset, endOffset);
    verifyValue(id, state.index.findKey(id, fileSpan, types), fileSpan, types);

    if (higherSegmentStartOffset != null) {
      // FileSpan higher than the entry (does not include entry)
      fileSpan = new FileSpan(higherSegmentStartOffset, state.log.getEndOffset());
      assertNull("There should have been no value returned", state.index.findKey(id, fileSpan, types));
    }

    if (lowerSegmentStartOffset != null) {
      // FileSpan lower than the entry (does not include entry)
      fileSpan = new FileSpan(state.index.getStartOffset(), lowerSegmentStartOffset);
      IndexValue valueFromIndex = state.index.findKey(id, fileSpan, types);
      if (expectedValue.getFlags() != IndexValue.FLAGS_DEFAULT_VALUE) {
        assertTrue("The update/delete entry should not be returned",
            valueFromIndex == null || !valueFromIndex.getOffset().equals(expectedValue.getOffset()));
      } else {
        assertNull("There should have been no value returned", valueFromIndex);
      }
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
    state.properties.put("store.deleted.message.retention.hours", Integer.toString(1));
    state.properties.put("store.hard.delete.operations.bytes.per.sec", Integer.toString(Integer.MAX_VALUE / 10));
    state.properties.setProperty("store.data.flush.interval.seconds", "3600");
    // enable daemon thread to run hard deletes
    state.properties.put("store.enable.hard.delete", "true");
    state.reloadIndex(true, false);
    assertTrue("Hard delete is not enabled", state.index.hardDeleter.isRunning());
    // IndexSegment still uses real time so advance time so that it goes 2 days past the real time.
    state.advanceTime(TimeUnit.DAYS.toMillis(2));
    long expectedProgress = state.index.getAbsolutePositionInLogForOffset(state.logOrder.lastKey());
    // give it some time so that hard delete completes one cycle
    waitUntilExpectedProgress(expectedProgress, 5000);
    state.verifyEntriesForHardDeletes(state.deletedKeys);
    Set<MockId> idsDeleted = new HashSet<>();
    // delete two entries
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    Set<MockId> idsToDelete = new HashSet<>();
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));
    for (MockId id : idsToDelete) {
      state.addDeleteEntry(id);
    }
    idsDeleted.addAll(idsToDelete);
    // pause hard delete
    state.index.hardDeleter.pause();
    assertTrue("Hard deletes should have been paused ", state.index.hardDeleter.isPaused());
    assertTrue("Hard delete thread did not go into waiting state",
        TestUtils.waitUntilExpectedState(state.index.hardDeleteThread, Thread.State.WAITING,
            HardDeleter.HARD_DELETE_SLEEP_TIME_ON_CAUGHT_UP_MS + 1));

    // delete two entries
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    idsToDelete.clear();
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));
    state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
    idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));
    for (MockId id : idsToDelete) {
      state.addDeleteEntry(id);
    }
    idsDeleted.addAll(idsToDelete);

    if (reloadIndex) {
      state.reloadIndex(true, true);
      assertTrue("Hard delete thread did not go into waiting state",
          TestUtils.waitUntilExpectedState(state.index.hardDeleteThread, Thread.State.WAITING,
              HardDeleter.HARD_DELETE_SLEEP_TIME_ON_CAUGHT_UP_MS + 1));
      idsToDelete.clear();
      state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));
      state.addPutEntries(2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time);
      idsToDelete.add(state.getIdToDeleteFromIndexSegment(state.referenceIndex.lastKey(), false));
      for (MockId id : idsToDelete) {
        state.addDeleteEntry(id);
      }
      idsDeleted.addAll(idsToDelete);
    }

    // advance time so that deleted entries becomes eligible to be hard deleted
    state.advanceTime(TimeUnit.DAYS.toMillis(3));
    // resume and verify new entries have been hard deleted
    state.index.hardDeleter.resume();
    assertFalse("Hard deletes should have been resumed ", state.index.hardDeleter.isPaused());
    expectedProgress = state.index.getAbsolutePositionInLogForOffset(state.logOrder.lastKey());
    // after resuming. hard deletes should progress. Give it some time to hard delete next range
    waitUntilExpectedProgress(expectedProgress, 5000);
    state.verifyEntriesForHardDeletes(idsDeleted);
  }

  // getLogSegmentsNotInJournal helpers

  /**
   * Tests {@link PersistentIndex#getLogSegmentsNotInJournal()}
   * @param maxLogSegmentsToBeIgnored number of log segments not to be returned via {@link PersistentIndex#getLogSegmentsNotInJournal()}
   * @param maxEntriesInJournal max number of entries in {@link Journal}
   * @throws StoreException
   */
  private void testGetLogSegmentsNotInJournal(int maxLogSegmentsToBeIgnored, int maxEntriesInJournal)
      throws StoreException {
    // fill current log segment to its capacity
    fillLastLogSegmentToCapacity(1, false);

    // test every log segment except the last one is returned
    fillLastLogSegmentToCapacity(maxEntriesInJournal, true);
    assertEquals("LogSegments mismatch ", getExpectedLogSegmentNames(1), state.index.getLogSegmentsNotInJournal());

    // create maxLogSegmentsToBeIgnored
    int logSegmentCount = maxLogSegmentsToBeIgnored;
    if (maxEntriesInJournal % maxLogSegmentsToBeIgnored > 0) {
      logSegmentCount--;
    }
    for (int i = 0; i < logSegmentCount; i++) {
      fillLastLogSegmentToCapacity(maxEntriesInJournal / maxLogSegmentsToBeIgnored, true);
    }
    if (maxEntriesInJournal % maxLogSegmentsToBeIgnored > 0) {
      fillLastLogSegmentToCapacity(
          maxEntriesInJournal - (logSegmentCount * (maxEntriesInJournal / maxLogSegmentsToBeIgnored)), true);
    }
    // every log segment except the last "maxLogSegmentsToBeIgnored" number of log segments should be returned
    assertEquals("LogSegments mismatch ", getExpectedLogSegmentNames(maxLogSegmentsToBeIgnored),
        state.index.getLogSegmentsNotInJournal());

    // reload and verify every log segment except the last one should be returned
    state.reloadIndex(true, false);
    assertEquals("LogSegments mismatch ", getExpectedLogSegmentNames(1), state.index.getLogSegmentsNotInJournal());
  }

  /**
   * Fetch expected log segment names ignoring the last {@code segmentsToIgnoreFromLast} from the list
   * of actual log segments in the log
   * @param segmentsToIgnoreFromLast number of log segments to be ignored from the last
   * @return a {@link List} of log segments
   */
  private List<LogSegmentName> getExpectedLogSegmentNames(int segmentsToIgnoreFromLast) {
    List<LogSegmentName> expectedLogSegments = new ArrayList<>();
    LogSegment logSegment = state.log.getFirstSegment();
    while (logSegment != null) {
      expectedLogSegments.add(logSegment.getName());
      logSegment = state.log.getNextSegment(logSegment);
    }
    return expectedLogSegments.subList(0, expectedLogSegments.size() - segmentsToIgnoreFromLast);
  }

  /**
   * Fill the current log segment to its capacity
   * @param numberOfEntries number of entries to be added to fill the log segment to its capacity
   * @param newLogSegment {@code true} if this is a new log segment. {@code false} otherwise
   * @throws StoreException
   */
  private void fillLastLogSegmentToCapacity(int numberOfEntries, boolean newLogSegment) throws StoreException {
    if (newLogSegment) {
      // to ensure a new log segment is created. If not, find the remaining size below will fail
      state.addPutEntries(1, PUT_RECORD_SIZE, Utils.Infinite_Time);
      numberOfEntries--;
    }
    // add PUT entries that spans the rest of the data in the last segment
    long remainingSize = state.log.getSegmentCapacity() - state.index.getCurrentEndOffset().getOffset();
    long sizePerPutRecord = remainingSize / numberOfEntries;
    for (int i = 0; i < numberOfEntries; i++) {
      state.addPutEntries(1, sizePerPutRecord, Utils.Infinite_Time);
    }
  }

  // recoverySuccessTest() helpers

  /**
   * Test recovery of a single segment.
   * @throws StoreException
   */
  private void singleSegmentRecoveryTest() throws StoreException {
    Offset indexEndOffsetBeforeRecovery = state.index.getCurrentEndOffset();
    // recover a few messages in a single segment
    final List<MessageInfo> infos = getCuratedSingleSegmentRecoveryInfos();
    final AtomicInteger returnTracker = new AtomicInteger(0);
    state.recovery = (read, startOffset, endOffset, factory) -> {
      switch (returnTracker.getAndIncrement()) {
        case 0:
          return infos;
        default:
          throw new IllegalStateException("This function should not have been called more than once");
      }
    };
    // This test relies on log segment not spilling over. If that happens, this test will fail.
    LogSegment activeSegment = state.log.getSegment(indexEndOffsetBeforeRecovery.getName());
    long expectedSegmentEndOffset = activeSegment.getEndOffset();
    // write a little "extra" data
    state.appendToLog(2 * CuratedLogIndexState.PUT_RECORD_SIZE);

    state.reloadIndex(true, false);
    assertEquals("Journal should only include entry for last log segment",
        state.index.journal.getFirstOffset().getName(), state.index.log.getLastSegment().getName());
    assertEquals("End offset not as expected", expectedSegmentEndOffset, activeSegment.getEndOffset());
    infos.forEach(this::checkRecoveryInfoEquivalence);
  }

  /**
   * Tests recovery of more than one segment.
   * @throws StoreException
   */
  private void multipleSegmentRecoveryTest() throws StoreException {
    Offset indexEndOffsetBeforeRecovery = state.index.getCurrentEndOffset();
    LogSegment activeSegment = state.log.getSegment(indexEndOffsetBeforeRecovery.getName());
    // recover a few messages across segments
    final List<MessageInfo> activeSegmentInfos = new ArrayList<>();
    // 1 PUT record that will be deleted in the next log segment
    MockId idToCreateAndDeleteAcrossSegments = state.getUniqueId();
    short pdAccountId = idToCreateAndDeleteAcrossSegments.getAccountId();
    short pdContainerId = idToCreateAndDeleteAcrossSegments.getContainerId();
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
    activeSegmentInfos.add(
        new MessageInfo(idToCreateAndDeleteAcrossSegments, CuratedLogIndexState.PUT_RECORD_SIZE, pdAccountId,
            pdContainerId, state.time.milliseconds()));
    // 1 PUT record that will be ttl updated in the next log segment
    MockId idToCreateAndUpdateAcrossSegments = state.getUniqueId();
    short puAccountId = idToCreateAndUpdateAcrossSegments.getAccountId();
    short puContainerId = idToCreateAndUpdateAcrossSegments.getContainerId();
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
    activeSegmentInfos.add(
        new MessageInfo(idToCreateAndUpdateAcrossSegments, CuratedLogIndexState.PUT_RECORD_SIZE, puAccountId,
            puContainerId, state.time.milliseconds()));
    // 1 TTL update record that will be deleted in the next log segment
    MockId idToUpdateAndDeleteAcrossSegments = state.getIdToDeleteFromLogSegment(state.log.getFirstSegment(), false);
    short udAccountId = idToUpdateAndDeleteAcrossSegments.getAccountId();
    short udContainerId = idToUpdateAndDeleteAcrossSegments.getContainerId();
    state.appendToLog(CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE);
    activeSegmentInfos.add(
        new MessageInfo(idToUpdateAndDeleteAcrossSegments, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true,
            udAccountId, udContainerId, state.time.milliseconds()));
    state.ttlUpdatedKeys.add(idToUpdateAndDeleteAcrossSegments);
    // 1 PUT record that will remain and covers almost the rest of the active segment.
    long size =
        activeSegment.getCapacityInBytes() - activeSegment.getEndOffset() - (CuratedLogIndexState.DELETE_RECORD_SIZE
            - 1);
    state.appendToLog(size);
    activeSegmentInfos.add(new MessageInfo(state.getUniqueId(), size, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), Utils.Infinite_Time));
    final List<MessageInfo> nextSegmentInfos = getCuratedSingleSegmentRecoveryInfos();
    // 1 DELETE record for the PUT in the previous segment
    state.appendToLog(CuratedLogIndexState.DELETE_RECORD_SIZE);
    nextSegmentInfos.add(
        new MessageInfo(idToCreateAndDeleteAcrossSegments, CuratedLogIndexState.DELETE_RECORD_SIZE, true, false, false,
            Utils.Infinite_Time, null, pdAccountId, pdContainerId, state.time.milliseconds(), (short) 5));
    // 1 TTL update for the PUT in the previous segment
    state.appendToLog(CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE);
    nextSegmentInfos.add(
        new MessageInfo(idToCreateAndUpdateAcrossSegments, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true,
            false, Utils.Infinite_Time, null, puAccountId, puContainerId, state.time.milliseconds(), (short) 6));
    state.ttlUpdatedKeys.add(idToUpdateAndDeleteAcrossSegments);
    // 1 DELETE for the TTL update in the previous segment
    state.appendToLog(CuratedLogIndexState.DELETE_RECORD_SIZE);
    nextSegmentInfos.add(
        new MessageInfo(idToUpdateAndDeleteAcrossSegments, CuratedLogIndexState.DELETE_RECORD_SIZE, true, false, false,
            Utils.Infinite_Time, null, udAccountId, udContainerId, state.time.milliseconds(), (short) 7));
    final AtomicInteger returnTracker = new AtomicInteger(0);
    state.recovery = (read, startOffset, endOffset, factory) -> {
      switch (returnTracker.getAndIncrement()) {
        case 0:
          return activeSegmentInfos;
        case 1:
          return nextSegmentInfos;
        default:
          throw new IllegalStateException("This function should not have been called more than two times");
      }
    };
    long activeSegmentExpectedEndOffset = activeSegment.getEndOffset();
    long nextSegmentExpectedEndOffset = state.log.getNextSegment(activeSegment).getEndOffset();
    // write a little "extra" data
    state.appendToLog(2 * CuratedLogIndexState.PUT_RECORD_SIZE);
    state.reloadIndex(true, false);
    assertEquals("End offset of former active segment not as expected", activeSegmentExpectedEndOffset,
        activeSegment.getEndOffset());
    assertEquals("Journal should only include entry for last log segment",
        state.index.journal.getFirstOffset().getName(), state.index.log.getLastSegment().getName());
    activeSegment = state.log.getNextSegment(activeSegment);
    assertNotNull("A new segment has not been created", activeSegment);
    assertEquals("End offset active segment not as expected", nextSegmentExpectedEndOffset,
        activeSegment.getEndOffset());
    List<MessageInfo> infos = new ArrayList<>(activeSegmentInfos);
    infos.addAll(nextSegmentInfos);
    infos.forEach(this::checkRecoveryInfoEquivalence);
  }

  /**
   * Creates a few curated recovery entries. For understanding the created entries, please read the source code which is
   * annotated with comments.
   * @return curated recovery entries.
   * @throws StoreException
   */
  private List<MessageInfo> getCuratedSingleSegmentRecoveryInfos() throws StoreException {
    List<MessageInfo> infos = new ArrayList<>();
    state.appendToLog(5 * CuratedLogIndexState.DELETE_RECORD_SIZE + 4 * CuratedLogIndexState.PUT_RECORD_SIZE
        + 5 * TTL_UPDATE_RECORD_SIZE);
    // 1 TTL update for a PUT not in the infos (won't be deleted)
    MockId idToUpdate = state.getIdToTtlUpdateFromLogSegment(state.log.getFirstSegment());
    infos.add(
        new MessageInfo(idToUpdate, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true, idToUpdate.getAccountId(),
            idToUpdate.getContainerId(), state.time.milliseconds()));
    state.ttlUpdatedKeys.add(idToUpdate);
    // 1 TTL update for a PUT not in the infos (will be deleted)
    MockId idToUpdateAndDelete = state.getIdToDeleteFromLogSegment(state.log.getFirstSegment(), false);
    infos.add(new MessageInfo(idToUpdateAndDelete, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true,
        idToUpdateAndDelete.getAccountId(), idToUpdateAndDelete.getContainerId(), state.time.milliseconds()));
    state.ttlUpdatedKeys.add(idToUpdateAndDelete);
    // 1 DELETE for a PUT not in the infos
    // ttl updated is false because when the delete record is read, the MessageInfo constructed will not know if there
    // has been a ttl update
    infos.add(new MessageInfo(idToUpdateAndDelete, CuratedLogIndexState.DELETE_RECORD_SIZE, true, false,
        idToUpdateAndDelete.getAccountId(), idToUpdateAndDelete.getContainerId(), state.time.milliseconds()));
    // 1 DELETE for a PUT + ttl update not in the infos
    MockId idToDelete = state.getIdToDeleteFromLogSegment(state.log.getFirstSegment(), true);
    // ttl updated is false because when the delete record is read, the MessageInfo constructed will not know if there
    // has been a ttl update
    infos.add(
        new MessageInfo(idToDelete, CuratedLogIndexState.DELETE_RECORD_SIZE, true, false, idToDelete.getAccountId(),
            idToDelete.getContainerId(), state.time.milliseconds()));
    // 3 PUT
    MockId idToCreateUpdateAndDelete = state.getUniqueId();
    short accountId = idToCreateUpdateAndDelete.getAccountId();
    short containerId = idToCreateUpdateAndDelete.getContainerId();
    infos.add(new MessageInfo(idToCreateUpdateAndDelete, CuratedLogIndexState.PUT_RECORD_SIZE, accountId, containerId,
        Utils.Infinite_Time));
    MockId idToCreateAndUpdate = state.getUniqueId();
    infos.add(
        new MessageInfo(idToCreateAndUpdate, CuratedLogIndexState.PUT_RECORD_SIZE, idToCreateAndUpdate.getAccountId(),
            idToCreateAndUpdate.getContainerId(), state.time.milliseconds()));
    MockId id = state.getUniqueId();
    infos.add(new MessageInfo(id, CuratedLogIndexState.PUT_RECORD_SIZE, id.getAccountId(), id.getContainerId(),
        Utils.Infinite_Time));
    // 1 TTL update for a PUT in the infos (will get deleted)
    infos.add(
        new MessageInfo(idToCreateUpdateAndDelete, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true, accountId,
            containerId, state.time.milliseconds()));
    state.ttlUpdatedKeys.add(idToCreateUpdateAndDelete);
    // 1 DELETE for a PUT + ttl update in the infos
    infos.add(
        new MessageInfo(idToCreateUpdateAndDelete, CuratedLogIndexState.DELETE_RECORD_SIZE, true, false, accountId,
            containerId, state.time.milliseconds()));
    // 1 TTL update for a PUT in the infos (won't get deleted)
    infos.add(new MessageInfo(idToCreateAndUpdate, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true,
        idToCreateAndUpdate.getAccountId(), idToCreateAndUpdate.getContainerId(), state.time.milliseconds()));
    state.ttlUpdatedKeys.add(idToCreateAndUpdate);
    // 1 expired PUT
    id = state.getUniqueId();
    infos.add(new MessageInfo(id, CuratedLogIndexState.PUT_RECORD_SIZE, 0, id.getAccountId(), id.getContainerId(),
        Utils.Infinite_Time));
    // 1 delete for PUT that does not exist in the index
    id = state.getUniqueId();
    infos.add(new MessageInfo(id, CuratedLogIndexState.DELETE_RECORD_SIZE, true, false, id.getAccountId(),
        id.getContainerId(), state.time.milliseconds()));
    // 1 ttl update for a PUT that does not exist in the index (because compaction has cleaned the PUT)
    id = state.getUniqueId();
    infos.add(new MessageInfo(id, CuratedLogIndexState.TTL_UPDATE_RECORD_SIZE, false, true, id.getAccountId(),
        id.getContainerId(), state.time.milliseconds()));
    // a delete for the TTL update above (compaction can only clean if there was a delete - so there will never be JUST
    // a TTL update)
    // ttl updated is false because when the delete record is read, the MessageInfo constructed will not know if there
    // has been a ttl update
    infos.add(new MessageInfo(id, CuratedLogIndexState.DELETE_RECORD_SIZE, true, false, id.getAccountId(),
        id.getContainerId(), state.time.milliseconds()));

    if (addUndeletes) {
      // There are 5 UNDELETE  2 DELETE  2 TTL_UPDATE  1 PUT
      state.appendToLog(
          5 * UNDELETE_RECORD_SIZE + 2 * DELETE_RECORD_SIZE + 2 * TTL_UPDATE_RECORD_SIZE + PUT_RECORD_SIZE);
      // 1 UNDELETE for a compacted PUT and DELETE
      id = state.getUniqueId();
      infos.add(
          new MessageInfo(id, UNDELETE_RECORD_SIZE, false, false, true, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), (short) 1));
      // 1 DELETE for the undelete above
      infos.add(
          new MessageInfo(id, DELETE_RECORD_SIZE, true, false, false, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), (short) 1));

      // 1 PUT (will be expired when recovering)
      id = state.getUniqueId();
      infos.add(
          new MessageInfo(id, PUT_RECORD_SIZE, 0, id.getAccountId(), id.getContainerId(), state.time.milliseconds()));
      // 1 DELETE and UNDELETE for this id
      infos.add(new MessageInfo(id, DELETE_RECORD_SIZE, true, false, id.getAccountId(), id.getContainerId(),
          state.time.milliseconds()));
      infos.add(
          new MessageInfo(id, UNDELETE_RECORD_SIZE, false, false, true, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), (short) 1));
      // 1 TTL update for this id
      infos.add(
          new MessageInfo(id, TTL_UPDATE_RECORD_SIZE, false, true, false, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), (short) 1));
      state.ttlUpdatedKeys.add(id);

      // 1 UNDELETE for DELETE (not ttlupdate) not in infos
      id = null;
      for (MockId mockId : state.deletedKeys) {
        if (!state.ttlUpdatedKeys.contains(mockId) && !state.getExpectedValue(mockId, false).isTtlUpdate()) {
          id = mockId;
          break;
        }
      }
      assertNotNull(id);
      infos.add(
          new MessageInfo(id, UNDELETE_RECORD_SIZE, false, false, true, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), (short) 1));
      // 1 TTL update
      infos.add(
          new MessageInfo(id, TTL_UPDATE_RECORD_SIZE, false, true, false, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), (short) 1));
      state.ttlUpdatedKeys.add(id);

      // 1 UNDELETE for DELETE (ttlupdated) not in infos
      id = null;
      for (MockId mockId : state.deletedKeys) {
        if (state.ttlUpdatedKeys.contains(mockId)) {
          id = mockId;
          break;
        }
      }
      assertNotNull(id);
      infos.add(
          new MessageInfo(id, UNDELETE_RECORD_SIZE, false, false, true, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), (short) 1));

      // 1 UNDELETE for UNDELETE not in infos
      id = state.undeletedKeys.iterator().next();
      short lifeVersion = (short) (state.getExpectedValue(id, false).getLifeVersion() + 1);
      infos.add(
          new MessageInfo(id, UNDELETE_RECORD_SIZE, false, false, true, Utils.Infinite_Time, null, id.getAccountId(),
              id.getContainerId(), state.time.milliseconds(), lifeVersion));
    }
    return infos;
  }

  /**
   * Checks that the details in {@code info} are equivalent to the value returned from the index.
   * @param info the {@link MessageInfo} to check.
   */
  private void checkRecoveryInfoEquivalence(MessageInfo info) {
    PersistentIndex.IndexEntryType toSearchFor = PersistentIndex.IndexEntryType.PUT;
    if (info.isDeleted()) {
      toSearchFor = PersistentIndex.IndexEntryType.DELETE;
    } else if (info.isUndeleted()) {
      toSearchFor = PersistentIndex.IndexEntryType.UNDELETE;
    } else if (info.isTtlUpdated()) {
      toSearchFor = PersistentIndex.IndexEntryType.TTL_UPDATE;
    }
    IndexValue value;
    try {
      value = state.index.findKey(info.getStoreKey(), null, EnumSet.of(toSearchFor));
    } catch (StoreException e) {
      throw new IllegalStateException(e);
    }
    assertEquals("Inconsistent size", info.getSize(), value.getSize());
    assertEquals("Inconsistent delete state ", info.isDeleted(), value.isDelete());
    // if the info says ttl update is true, then the value must reflect that. vice versa need not be true because put
    // infos won't have it set but the value returned from the index will if a ttl update was applied later. Same
    // applies if the info is for a delete record in which case it won't have the ttl update set to true because it is
    // not known at the time of the info generation from the log that the id was previously updated
    if (info.isTtlUpdated()) {
      assertTrue("Inconsistent ttl update state ", value.isTtlUpdate());
      assertEquals("Inconsistent expiresAtMs", info.getExpirationTimeInMs(), value.getExpiresAtMs());
    }
    assertEquals("Incorrect accountId", info.getAccountId(), value.getAccountId());
    assertEquals("Incorrect containerId", info.getContainerId(), value.getContainerId());
    assertEquals("Incorrect operationTimeMs", Utils.getTimeInMsToTheNearestSec(info.getOperationTimeMs()),
        value.getOperationTimeInMs());
    assertEquals("Inconsistent lifeVersion for info " + info, info.getLifeVersion(), value.getLifeVersion());
    assertEquals(info.isUndeleted(), value.isUndelete());
    assertEquals(info.isDeleted(), value.isDelete());
  }

  /**
   * Tests the case where the index is lost completely and needs to be recovered from scratch.
   * @throws StoreException
   */
  private void totalIndexLossRecoveryTest() throws StoreException {
    state.closeAndClearIndex();
    final AtomicInteger returnTracker = new AtomicInteger(0);
    state.recovery = (read, startOffset, endOffset, factory) -> {
      switch (returnTracker.getAndIncrement()) {
        case 0:
          return Collections.singletonList(new MessageInfo(state.getUniqueId(), CuratedLogIndexState.PUT_RECORD_SIZE,
              Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), Utils.Infinite_Time));
        default:
          return Collections.emptyList();
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
    state.recovery = (read, startOffset, endOffset, factory) -> Collections.singletonList(info);
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
    // ------------------
    // 1. Index -> Index
    Offset firstIndexSegmentStartOffset = state.referenceIndex.firstKey();
    Offset secondIndexSegmentStartOffset = state.referenceIndex.higherKey(firstIndexSegmentStartOffset);
    MockId firstId = state.referenceIndex.get(firstIndexSegmentStartOffset).firstKey();
    // All elements from first index segment and two from the second to be returned (because of size restrictions)
    Set<MockId> expectedKeys = new HashSet<>();
    long maxTotalSizeOfEntries = 0;
    for (Map.Entry<MockId, TreeSet<IndexValue>> segmentEntry : state.referenceIndex.get(firstIndexSegmentStartOffset)
        .entrySet()) {
      if (!segmentEntry.getKey().equals(firstId)) {
        expectedKeys.add(segmentEntry.getKey());
        maxTotalSizeOfEntries += getSizeOfAllValues(segmentEntry.getValue());
      }
    }
    TreeMap<MockId, TreeSet<IndexValue>> secondIndexSegment = state.referenceIndex.get(secondIndexSegmentStartOffset);
    Map.Entry<MockId, TreeSet<IndexValue>> secondIndexSegmentEntry = secondIndexSegment.firstEntry();
    expectedKeys.add(secondIndexSegmentEntry.getKey());
    maxTotalSizeOfEntries += getSizeOfAllValues(secondIndexSegmentEntry.getValue());
    secondIndexSegmentEntry = secondIndexSegment.higherEntry(secondIndexSegmentEntry.getKey());
    expectedKeys.add(secondIndexSegmentEntry.getKey());
    maxTotalSizeOfEntries += getSizeOfAllValues(secondIndexSegmentEntry.getValue());

    StoreFindToken startToken =
        new StoreFindToken(firstId, firstIndexSegmentStartOffset, state.sessionId, state.incarnationId, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
    IndexSegment segmentOfToken = state.index.getIndexSegments().get(secondIndexSegmentStartOffset);
    StoreFindToken expectedEndToken =
        new StoreFindToken(secondIndexSegmentEntry.getKey(), secondIndexSegmentStartOffset, state.sessionId,
            state.incarnationId, segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(),
            segmentOfToken.getResetKeyLifeVersion());
    expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(secondIndexSegmentStartOffset));
    doFindEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 2. Uninitialized -> Index
    // add firstStoreKey and its size
    expectedKeys.add(firstId);
    maxTotalSizeOfEntries +=
        getSizeOfAllValues(state.referenceIndex.get(firstIndexSegmentStartOffset).firstEntry().getValue());
    doFindEntriesSinceTest(new StoreFindToken(), maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 3. Journal -> Index
    // create a journal based token for an offset that isn't in the journal
    startToken = new StoreFindToken(state.logOrder.firstKey(), state.sessionId, state.incarnationId, false, null, null,
        UNINITIALIZED_RESET_KEY_VERSION);
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
    IndexSegment segmentOfToken = state.index.getIndexSegments().lastEntry().getValue();
    StoreFindToken absoluteEndToken =
        new StoreFindToken(state.logOrder.lastKey(), state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
    absoluteEndToken.setBytesRead(state.index.getLogUsedCapacity());

    // ------------------
    // 1. Uninitialized -> Journal
    doFindEntriesSinceTest(new StoreFindToken(), Long.MAX_VALUE, state.allKeys.keySet(), absoluteEndToken);

    // ------------------
    // 2. Index -> Journal
    Offset firstIndexSegmentStartOffset = state.referenceIndex.firstKey();
    StoreKey firstStoreKey = state.referenceIndex.get(firstIndexSegmentStartOffset).firstKey();
    StoreFindToken startToken =
        new StoreFindToken(firstStoreKey, firstIndexSegmentStartOffset, state.sessionId, state.incarnationId, null,
            null, UNINITIALIZED_RESET_KEY_VERSION);
    Set<MockId> expectedKeys = new HashSet<>(state.allKeys.keySet());
    if (!state.deletedKeys.contains(firstStoreKey)) {
      // if firstStoreKey has not been deleted, it will not show up in findEntries since its PUT record is ignored
      expectedKeys.remove(firstStoreKey);
    }
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 3. Journal -> Journal
    // a. Token no longer in journal
    startToken = new StoreFindToken(state.logOrder.firstKey(), state.sessionId, state.incarnationId, false, null, null,
        UNINITIALIZED_RESET_KEY_VERSION);
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, state.allKeys.keySet(), absoluteEndToken);

    // b. Token still in journal
    startToken =
        new StoreFindToken(state.index.journal.getFirstOffset(), state.sessionId, state.incarnationId, false, null,
            null, UNINITIALIZED_RESET_KEY_VERSION);
    expectedKeys = new HashSet<>();
    for (Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> entry : state.logOrder.tailMap(
        startToken.getOffset(), false).entrySet()) {
      expectedKeys.add(entry.getValue().getFirst());
    }
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // c. Token still in journal with inclusiveness set to true
    startToken =
        new StoreFindToken(state.index.journal.getFirstOffset(), state.sessionId, state.incarnationId, true, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
    expectedKeys.add(state.logOrder.tailMap(startToken.getOffset(), true).firstEntry().getValue().getFirst());
    doFindEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 4. Journal no change
    doFindEntriesSinceTest(absoluteEndToken, Long.MAX_VALUE, Collections.emptySet(), absoluteEndToken);
  }

  /**
   * Uses {@link PersistentIndex#findEntriesSince(FindToken, long)} to get entries one by one.
   * @throws StoreException
   */
  private void findEntriesSinceOneByOneTest() throws StoreException {
    Offset journalStartOffset = state.index.journal.getFirstOffset();
    StoreFindToken startToken = new StoreFindToken();
    Offset stoppedAt = null;
    for (Map.Entry<Offset, TreeMap<MockId, TreeSet<IndexValue>>> indexEntry : state.referenceIndex.entrySet()) {
      Offset indexSegmentStartOffset = indexEntry.getKey();
      // We get index based tokens as long as
      // 1. The original token is index based
      // 2. The size of entries being obtained is <= the size of records in the current index segment
      if (indexSegmentStartOffset.compareTo(journalStartOffset) >= 0) {
        stoppedAt = indexSegmentStartOffset;
        break;
      }
      IndexSegment segmentOfToken = state.index.getIndexSegments().get(indexSegmentStartOffset);
      for (Map.Entry<MockId, TreeSet<IndexValue>> indexSegmentEntry : indexEntry.getValue().entrySet()) {
        MockId id = indexSegmentEntry.getKey();
        StoreFindToken expectedEndToken =
            new StoreFindToken(id, indexSegmentStartOffset, state.sessionId, state.incarnationId,
                segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(),
                segmentOfToken.getResetKeyLifeVersion());
        expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(indexSegmentStartOffset));
        doFindEntriesSinceTest(startToken, getSizeOfAllValues(indexSegmentEntry.getValue()), Collections.singleton(id),
            expectedEndToken);
        startToken = expectedEndToken;
      }
    }

    Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> logEntry = state.logOrder.floorEntry(stoppedAt);
    while (logEntry != null) {
      Offset startOffset = logEntry.getKey();
      MockId id = logEntry.getValue().getFirst();
      // size returned is the size of the most recent record
      long size = state.getExpectedValue(id, EnumSet.allOf(PersistentIndex.IndexEntryType.class), null).getSize();
      IndexSegment segmentOfToken = state.index.getIndexSegments().floorEntry(startOffset).getValue();
      StoreFindToken expectedEndToken =
          new StoreFindToken(startOffset, state.sessionId, state.incarnationId, false, segmentOfToken.getResetKey(),
              segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
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
    Map.Entry<Offset, TreeMap<MockId, TreeSet<IndexValue>>> indexEntry =
        state.referenceIndex.floorEntry(state.index.journal.getFirstOffset());
    Offset nextIndexSegmentStartOffset = state.referenceIndex.higherKey(indexEntry.getKey());
    MockId firstIdInSegment = indexEntry.getValue().firstKey();
    StoreFindToken startToken =
        new StoreFindToken(firstIdInSegment, indexEntry.getKey(), state.sessionId, state.incarnationId, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
    long maxSize = 0;
    Set<MockId> expectedKeys = new HashSet<>();
    for (Map.Entry<MockId, TreeSet<IndexValue>> indexSegmentEntry : indexEntry.getValue().entrySet()) {
      if (!firstIdInSegment.equals(indexSegmentEntry.getKey())) {
        expectedKeys.add(indexSegmentEntry.getKey());
        maxSize += getSizeOfAllValues(indexSegmentEntry.getValue());
      }
    }
    MockId logId = state.logOrder.get(nextIndexSegmentStartOffset).getFirst();
    expectedKeys.add(logId);
    long size = state.deletedKeys.contains(logId) ? CuratedLogIndexState.DELETE_RECORD_SIZE
        : state.logOrder.get(nextIndexSegmentStartOffset).getSecond().indexValue.getSize();
    maxSize += size;

    Offset endOffset = state.log.getFileSpanForMessage(nextIndexSegmentStartOffset, size).getEndOffset();
    IndexSegment segmentOfToken = state.index.getIndexSegments().get(nextIndexSegmentStartOffset);
    StoreFindToken expectedEndToken =
        new StoreFindToken(nextIndexSegmentStartOffset, state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
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
      MockId id = (MockId) info.getStoreKey();
      IndexValue putValue = state.getExpectedValue(id, EnumSet.of(PersistentIndex.IndexEntryType.PUT), null);
      IndexValue ttlUpdateValue =
          state.getExpectedValue(id, EnumSet.of(PersistentIndex.IndexEntryType.TTL_UPDATE), null);
      IndexValue deleteValue = state.getExpectedValue(id, EnumSet.of(PersistentIndex.IndexEntryType.DELETE), null);
      IndexValue undeleteValue = state.getExpectedValue(id, EnumSet.of(PersistentIndex.IndexEntryType.UNDELETE), null);
      // size returned is hard to predict if the key has been updated - it depends on the locations of the PUT and
      // update entries and whether all or some of them are present in the return list. It is not useful to recompute
      // the situations here and check
      long expiresAtMs = undeleteValue != null ? undeleteValue.getExpiresAtMs()
          : deleteValue != null ? deleteValue.getExpiresAtMs()
              : ttlUpdateValue != null ? ttlUpdateValue.getExpiresAtMs() : putValue.getExpiresAtMs();
      short accountId = undeleteValue != null ? undeleteValue.getAccountId()
          : deleteValue != null ? deleteValue.getAccountId()
              : ttlUpdateValue != null ? ttlUpdateValue.getAccountId() : putValue.getAccountId();
      short containerId = undeleteValue != null ? undeleteValue.getContainerId()
          : deleteValue != null ? deleteValue.getContainerId()
              : ttlUpdateValue != null ? ttlUpdateValue.getContainerId() : putValue.getContainerId();
      long operationTimeMs = undeleteValue != null ? undeleteValue.getOperationTimeInMs()
          : deleteValue != null ? deleteValue.getOperationTimeInMs()
              : ttlUpdateValue != null ? ttlUpdateValue.getOperationTimeInMs() : putValue.getOperationTimeInMs();
      boolean isTtlUpdated = undeleteValue != null ? undeleteValue.isTtlUpdate()
          : deleteValue != null ? deleteValue.isTtlUpdate() : ttlUpdateValue != null;
      // if a key is updated, it doesn't matter if we reached the update record or not, the updated state will be
      // the one that is returned.
      if (undeleteValue != null || deleteValue != null) {
        // there is either delete or undelete
        IndexValue lastValue = state.getExpectedValue(id,
            EnumSet.of(PersistentIndex.IndexEntryType.DELETE, PersistentIndex.IndexEntryType.UNDELETE), null);
        assertEquals("Inconsistent delete state for key " + id, lastValue.isDelete(), info.isDeleted());
        assertEquals("Inconsistent undelete state for key " + id, lastValue.isUndelete(), info.isUndeleted());
      }
      assertEquals("Inconsistent TTL update state", isTtlUpdated, info.isTtlUpdated());
      assertEquals("Inconsistent expiresAtMs", expiresAtMs, info.getExpirationTimeInMs());
      assertEquals("Inconsistent accountId", accountId, info.getAccountId());
      assertEquals("Inconsistent containerId", containerId, info.getContainerId());
      assertEquals("Inconsistent operationTimeMs", operationTimeMs, info.getOperationTimeMs());
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
      doFindDeletedEntriesSinceTest(token, Long.MAX_VALUE, Collections.emptySet(), token);
    } else {
      doFindEntriesSinceTest(token, Long.MAX_VALUE, Collections.emptySet(), token);
    }
  }

  /**
   * Tests the case where the PUT and TTL update entries are in different index segments and journal has the TTL update
   * entry (immaterial whether it has the PUT or not).
   * @throws StoreException
   */
  private void findEntriesSinceTtlUpdateCornerCaseTest() throws StoreException {
    state.closeAndClearIndex();
    state.properties.setProperty("store.index.max.number.of.inmem.elements", "1");
    state.reloadIndex(false, false);
    long expiresAtMs = state.time.milliseconds() + TimeUnit.HOURS.toMillis(1);
    MockId id = (MockId) state.addPutEntries(1, PUT_RECORD_SIZE, expiresAtMs).get(0).getKey();
    state.makePermanent(id, false);
    IndexSegment segmentOfToken = state.index.getIndexSegments().floorEntry(state.logOrder.lastKey()).getValue();
    StoreFindToken expectedEndToken =
        new StoreFindToken(state.logOrder.lastKey(), state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
    expectedEndToken.setBytesRead(state.index.getLogUsedCapacity());
    doFindEntriesSinceTest(new StoreFindToken(), Long.MAX_VALUE, Collections.singleton(id), expectedEndToken);
    findEntriesSinceOneByOneTest();
  }

  /**
   * Test the refinements to the size limit logic for cases where a journal query returns both a put entry and a TTL
   * update entry. In these cases, the put entry size will be counted against the size limit since the index assumes
   * that the replicator probably wants to fetch the original blob as well as the TTL update.
   * @throws StoreException
   */
  private void findEntriesSinceTtlUpdateAndPutInJournalTest() throws StoreException {
    state.closeAndClearIndex();
    state.properties.setProperty("store.index.max.number.of.inmem.elements", "10");
    state.reloadIndex(false, false);
    long expiresAtMs = state.time.milliseconds() + TimeUnit.HOURS.toMillis(1);
    List<IndexEntry> putEntries = state.addPutEntries(4, PUT_RECORD_SIZE, expiresAtMs);
    state.makePermanent((MockId) putEntries.get(0).getKey(), false);
    state.makePermanent((MockId) putEntries.get(1).getKey(), false);

    StoreFindToken startToken = new StoreFindToken();
    Offset tokenOffset = putEntries.get(0).getValue().getOffset();
    StoreFindToken expectedEndToken = getExpectedJournalEndToken(tokenOffset, putEntries.get(0).getKey());
    Set<MockId> expectedKeys = Collections.singleton((MockId) putEntries.get(0).getKey());
    // only one ID should be returned since the put size should be counted against the size limit
    doFindEntriesSinceTest(startToken, TTL_UPDATE_RECORD_SIZE * 2, expectedKeys, expectedEndToken);

    startToken = expectedEndToken;
    tokenOffset = putEntries.get(1).getValue().getOffset();
    expectedEndToken = getExpectedJournalEndToken(tokenOffset, putEntries.get(1).getKey());
    expectedKeys = Collections.singleton((MockId) putEntries.get(1).getKey());
    // only one ID should be returned since the put size should be counted against the size limit
    doFindEntriesSinceTest(startToken, TTL_UPDATE_RECORD_SIZE * 2, expectedKeys, expectedEndToken);

    startToken = expectedEndToken;
    // the last key is the TTL update for the second blob
    expectedEndToken = getExpectedJournalEndToken(state.logOrder.lastKey(), putEntries.get(1).getKey());
    expectedKeys = putEntries.stream().map(entry -> (MockId) entry.getKey()).collect(Collectors.toSet());
    // expect the last two put entries and the two TTL update journal entries to be processed
    doFindEntriesSinceTest(startToken, 2 * PUT_RECORD_SIZE + 2 * TTL_UPDATE_RECORD_SIZE, expectedKeys,
        expectedEndToken);
  }

  /**
   * Create a journal-based find token that can be compared against a token returned by
   * {@link PersistentIndex#findEntriesSince}.
   * @param tokenOffset the offset to include in the token
   * @param lastKey the {@link StoreKey} of the last message that will be returned in the query.
   * @return a {@link StoreFindToken} with bytes read set.
   */
  private StoreFindToken getExpectedJournalEndToken(Offset tokenOffset, StoreKey lastKey) throws StoreException {
    IndexSegment segmentOfToken = state.index.getIndexSegments().floorEntry(tokenOffset).getValue();
    StoreFindToken expectedEndToken =
        new StoreFindToken(tokenOffset, state.sessionId, state.incarnationId, false, segmentOfToken.getResetKey(),
            segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
    // the last message's size will be the size of the most recent log entry for its key, not necessarily the entry at
    // tokenOffset. This size is used to calculate bytes read. even if it is not strictly the entry at the log offset
    // of the last journal entry processed.
    long lastMessageSize =
        state.getExpectedValue((MockId) lastKey, EnumSet.allOf(PersistentIndex.IndexEntryType.class), null).getSize();
    Offset endOffset = state.log.getFileSpanForMessage(tokenOffset, lastMessageSize).getEndOffset();
    expectedEndToken.setBytesRead(state.index.getAbsolutePositionInLogForOffset(endOffset));
    return expectedEndToken;
  }

  // findDeletedEntriesSinceTest() helpers

  /**
   * Tests all cases of {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} that result in an index
   * based {@link StoreFindToken} being returned.
   * 1. Uninitialized -> Index
   * 2. Index -> Index
   * 3. Journal -> Index
   * @throws StoreException
   */
  private void findDeletedEntriesSinceToIndexBasedTest() throws StoreException {
    // ------------------
    // 1. Index -> Index
    Offset secondIndexSegmentStartOffset = state.referenceIndex.higherKey(state.referenceIndex.firstKey());
    Map.Entry<MockId, TreeSet<IndexValue>> firstSegmentEntry =
        state.referenceIndex.get(secondIndexSegmentStartOffset).firstEntry();
    // Most elements from the second to be returned (because of size restrictions)
    Set<MockId> expectedKeys = new HashSet<>();
    long maxTotalSizeOfEntries = 0;
    MockId lastKey = null;
    for (Map.Entry<MockId, TreeSet<IndexValue>> segmentEntry : state.referenceIndex.get(secondIndexSegmentStartOffset)
        .entrySet()) {
      if (!segmentEntry.equals(firstSegmentEntry)) {
        if (segmentEntry.getValue().last().isDelete()
            && state.getExpectedValue(segmentEntry.getKey(), EnumSet.of(PersistentIndex.IndexEntryType.UNDELETE), null)
            == null) {
          expectedKeys.add(segmentEntry.getKey());
        }
        maxTotalSizeOfEntries += getSizeOfAllValues(segmentEntry.getValue());
      }
      lastKey = segmentEntry.getKey();
    }
    StoreFindToken startToken =
        new StoreFindToken(firstSegmentEntry.getKey(), secondIndexSegmentStartOffset, state.sessionId,
            state.incarnationId, null, null, UNINITIALIZED_RESET_KEY_VERSION);
    IndexSegment segmentOfToken = state.index.getIndexSegments().floorEntry(secondIndexSegmentStartOffset).getValue();
    StoreFindToken expectedEndToken =
        new StoreFindToken(lastKey, secondIndexSegmentStartOffset, state.sessionId, state.incarnationId,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
    doFindDeletedEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 2. Uninitialized -> Index
    // add size of values and any keys that are supposed to be returned from the first index segment
    for (Map.Entry<MockId, TreeSet<IndexValue>> segmentEntry : state.referenceIndex.firstEntry()
        .getValue()
        .entrySet()) {
      if (segmentEntry.getValue().last().isDelete()
          && state.getExpectedValue(segmentEntry.getKey(), EnumSet.of(PersistentIndex.IndexEntryType.UNDELETE), null)
          == null) {
        expectedKeys.add(segmentEntry.getKey());
      }
      maxTotalSizeOfEntries += getSizeOfAllValues(segmentEntry.getValue());
    }
    // add size of value of firstIdInSegment
    maxTotalSizeOfEntries += getSizeOfAllValues(firstSegmentEntry.getValue());
    if (firstSegmentEntry.getValue().last().isDelete()
        && state.getExpectedValue(firstSegmentEntry.getKey(), EnumSet.of(PersistentIndex.IndexEntryType.UNDELETE), null)
        == null) {
      expectedKeys.add(firstSegmentEntry.getKey());
    }
    doFindDeletedEntriesSinceTest(new StoreFindToken(), maxTotalSizeOfEntries, expectedKeys, expectedEndToken);

    // ------------------
    // 3. Journal -> Index
    // create a journal based token for an offset that isn't in the journal
    startToken = new StoreFindToken(state.logOrder.firstKey(), state.sessionId, state.incarnationId, false, null, null,
        UNINITIALIZED_RESET_KEY_VERSION);
    doFindDeletedEntriesSinceTest(startToken, maxTotalSizeOfEntries, expectedKeys, expectedEndToken);
  }

  /**
   * Tests all cases of {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} that result in an journal
   * based {@link StoreFindToken} being returned.
   * 1. Uninitialized -> Journal
   * 2. Index -> Journal
   * 3. Journal -> Journal
   * 4. No movement.
   * @throws StoreException
   */
  private void findDeletedEntriesSinceToJournalBasedTest() throws StoreException {
    IndexSegment segmentOfToken = state.index.getIndexSegments().lastEntry().getValue();
    StoreFindToken absoluteEndToken =
        new StoreFindToken(state.logOrder.lastKey(), state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());

    // ------------------
    // 1. Uninitialized -> Journal
    doFindDeletedEntriesSinceTest(new StoreFindToken(), Long.MAX_VALUE, state.deletedKeys, absoluteEndToken);

    // ------------------
    // 2. Index -> Journal
    Offset secondIndexSegmentStartOffset = state.referenceIndex.higherKey(state.referenceIndex.firstKey());
    // second index segment contains the first delete entry
    StoreKey firstDeletedKey = getDeletedKeyFromIndexSegment(secondIndexSegmentStartOffset);
    StoreFindToken startToken =
        new StoreFindToken(firstDeletedKey, secondIndexSegmentStartOffset, state.sessionId, state.incarnationId, null,
            null, UNINITIALIZED_RESET_KEY_VERSION);
    Set<MockId> expectedKeys = new HashSet<>(state.deletedKeys);
    expectedKeys.remove(firstDeletedKey);
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 3. Journal -> Journal
    // a. Token no longer in journal
    startToken =
        new StoreFindToken(state.getExpectedValue((MockId) firstDeletedKey, false).getOffset(), state.sessionId,
            state.incarnationId, false, null, null, UNINITIALIZED_RESET_KEY_VERSION);
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, state.deletedKeys, absoluteEndToken);

    // b. Token still in journal
    startToken =
        new StoreFindToken(state.index.journal.getFirstOffset(), state.sessionId, state.incarnationId, false, null,
            null, UNINITIALIZED_RESET_KEY_VERSION);
    expectedKeys.clear();
    for (Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> entry : state.logOrder.tailMap(
        startToken.getOffset(), false).entrySet()) {
      if (entry.getValue().getSecond().indexValue.isDelete() &&
          state.getExpectedValue(entry.getValue().getFirst(), EnumSet.of(PersistentIndex.IndexEntryType.UNDELETE), null)
              == null) {
        expectedKeys.add(entry.getValue().getFirst());
      }
    }
    doFindDeletedEntriesSinceTest(startToken, Long.MAX_VALUE, expectedKeys, absoluteEndToken);

    // ------------------
    // 4. Journal no change
    doFindDeletedEntriesSinceTest(absoluteEndToken, Long.MAX_VALUE, Collections.emptySet(), absoluteEndToken);
  }

  /**
   * Uses {@link PersistentIndex#findDeletedEntriesSince(FindToken, long, long)} to get entries one by one.
   * @throws StoreException
   */
  private void findDeletedEntriesSinceOneByOneTest() throws StoreException {
    Offset journalStartOffset = state.index.journal.getFirstOffset();
    StoreFindToken startToken = new StoreFindToken();
    Offset stoppedAt = null;
    for (Map.Entry<Offset, TreeMap<MockId, TreeSet<IndexValue>>> indexEntry : state.referenceIndex.entrySet()) {
      Offset indexSegmentStartOffset = indexEntry.getKey();
      // We get index based tokens as long as
      // 1. The original token is index based
      // 2. The size of entries being obtained is <= the size of records in the current index segment
      if (indexSegmentStartOffset.compareTo(journalStartOffset) >= 0) {
        stoppedAt = indexSegmentStartOffset;
        break;
      }
      IndexSegment segmentOfToken = state.index.getIndexSegments().get(indexSegmentStartOffset);
      for (Map.Entry<MockId, TreeSet<IndexValue>> indexSegmentEntry : indexEntry.getValue().entrySet()) {
        MockId id = indexSegmentEntry.getKey();
        boolean isDeleted = indexSegmentEntry.getValue().last().isDelete() &&
            state.getExpectedValue(indexSegmentEntry.getKey(), EnumSet.of(PersistentIndex.IndexEntryType.UNDELETE),
                null) == null;
        StoreFindToken expectedEndToken =
            new StoreFindToken(id, indexSegmentStartOffset, state.sessionId, state.incarnationId,
                segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(),
                segmentOfToken.getResetKeyLifeVersion());
        long size = getSizeOfAllValues(indexSegmentEntry.getValue());
        doFindDeletedEntriesSinceTest(startToken, size, isDeleted ? Collections.singleton(id) : Collections.emptySet(),
            expectedEndToken);
        startToken = expectedEndToken;
      }
    }

    Map.Entry<Offset, Pair<MockId, CuratedLogIndexState.LogEntry>> logEntry = state.logOrder.floorEntry(stoppedAt);
    while (logEntry != null) {
      Offset startOffset = logEntry.getKey();
      MockId id = logEntry.getValue().getFirst();
      IndexValue value = state.getExpectedValue(id, false);
      boolean isDeleted = value.isDelete()
          && state.getExpectedValue(id, EnumSet.of(PersistentIndex.IndexEntryType.UNDELETE), null) == null;
      // size returned is the size of the delete if the key has been deleted.
      long size = value.getSize();
      IndexSegment segmentOfToken = state.index.getIndexSegments().floorEntry(startOffset).getValue();
      StoreFindToken expectedEndToken =
          new StoreFindToken(startOffset, state.sessionId, state.incarnationId, false, segmentOfToken.getResetKey(),
              segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
      doFindDeletedEntriesSinceTest(startToken, size, isDeleted ? Collections.singleton(id) : Collections.emptySet(),
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
    Map.Entry<Offset, TreeMap<MockId, TreeSet<IndexValue>>> indexEntry =
        state.referenceIndex.floorEntry(state.index.journal.getFirstOffset());
    Offset nextIndexSegmentStartOffset = state.referenceIndex.higherKey(indexEntry.getKey());
    MockId firstIdInSegment = indexEntry.getValue().firstKey();
    StoreFindToken startToken =
        new StoreFindToken(firstIdInSegment, indexEntry.getKey(), state.sessionId, state.incarnationId, null, null,
            UNINITIALIZED_RESET_KEY_VERSION);
    long maxSize = 0;
    Set<MockId> expectedKeys = new HashSet<>();
    for (Map.Entry<MockId, TreeSet<IndexValue>> indexSegmentEntry : indexEntry.getValue().entrySet()) {
      if (!firstIdInSegment.equals(indexSegmentEntry.getKey())) {
        if (indexSegmentEntry.getValue().last().isDelete()) {
          expectedKeys.add(indexSegmentEntry.getKey());
        }
        maxSize += getSizeOfAllValues(indexSegmentEntry.getValue());
      }
    }
    MockId logId = state.logOrder.get(nextIndexSegmentStartOffset).getFirst();
    long size = state.logOrder.get(nextIndexSegmentStartOffset).getSecond().indexValue.getSize();
    if (state.deletedKeys.contains(logId)) {
      expectedKeys.add(logId);
      size = CuratedLogIndexState.DELETE_RECORD_SIZE;
    }
    maxSize += size;
    IndexSegment segmentOfToken = state.index.getIndexSegments().get(nextIndexSegmentStartOffset);
    StoreFindToken expectedEndToken =
        new StoreFindToken(nextIndexSegmentStartOffset, state.sessionId, state.incarnationId, false,
            segmentOfToken.getResetKey(), segmentOfToken.getResetKeyType(), segmentOfToken.getResetKeyLifeVersion());
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
      IndexValue value = state.getExpectedValue((MockId) info.getStoreKey(), false);
      assertEquals("Inconsistent size", value.getSize(), info.getSize());
      assertTrue("Not deleted", info.isDeleted());
      assertEquals("Inconsistent expiresAtMs", value.getExpiresAtMs(), info.getExpirationTimeInMs());
      keysExamined.add(info.getStoreKey());
    }
    assertEquals("All keys should be present " + expectedKeys + "\n" + keysExamined, expectedKeys, keysExamined);
  }

  /**
   * Gets a deleted key from the index segment with start offset {@code indexSegmentStartOffset}.
   * @param indexSegmentStartOffset the start {@link Offset} of the index segment from which a deleted key is required
   * @return a deleted key from the index segment with start offset {@code indexSegmentStartOffset}.
   */
  private MockId getDeletedKeyFromIndexSegment(Offset indexSegmentStartOffset) {
    MockId deletedId = null;
    for (Map.Entry<MockId, TreeSet<IndexValue>> indexSegmentEntry : state.referenceIndex.get(indexSegmentStartOffset)
        .entrySet()) {
      if (indexSegmentEntry.getValue().last().isDelete()) {
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
   * @param indexVersion the version of the index segment that will get rolled over.
   * @param rollOverWithPutRecord {@code true} if the entry that causes rollover should be a put record,
   *                              {@code false} if the entry that causes rollover should be a delete record
   * @throws StoreException
   * @throws IOException
   */
  private void indexSegmentRollOverTest(short indexVersion, boolean rollOverWithPutRecord)
      throws StoreException, IOException {
    state.closeAndClearIndex();
    Offset currentEndOffset = state.index.getCurrentEndOffset();

    List<IndexEntry> indexEntries = new ArrayList<>();
    // create an index entry in older version.
    IndexEntry entry = new IndexEntry(state.getUniqueId(),
        IndexValueTest.getIndexValue(CuratedLogIndexState.PUT_RECORD_SIZE, currentEndOffset, Utils.Infinite_Time,
            state.time.milliseconds(), Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, (short) 0,
            indexVersion));
    Offset startOffset = entry.getValue().getOffset();
    int entrySize = entry.getKey().sizeInBytes() + entry.getValue().getBytes().capacity();
    int valueSize = entry.getValue().getBytes().capacity();
    IndexSegment indexSegment = generateIndexSegment(startOffset, entrySize, valueSize, indexVersion);
    state.appendToLog(CuratedLogIndexState.PUT_RECORD_SIZE);
    FileSpan fileSpan = state.log.getFileSpanForMessage(currentEndOffset, CuratedLogIndexState.PUT_RECORD_SIZE);
    indexSegment.addEntry(entry, fileSpan.getEndOffset());
    indexEntries.add(entry);
    // add more entries to the segment
    indexEntries.addAll(addPutEntries(fileSpan.getEndOffset(), indexSegment, 2, CuratedLogIndexState.PUT_RECORD_SIZE,
        Utils.Infinite_Time, 10, false));
    // persist the index segment of older version.
    indexSegment.writeIndexSegmentToFile(indexSegment.getEndOffset());

    state.reloadIndex(false, false);
    assertEquals("Reloaded index segments should have the same version they were created in", indexVersion,
        state.index.getIndexSegments().lastEntry().getValue().getVersion());
    int indexCount = state.index.getIndexSegments().size();
    // add an entry and verify if roll over happened
    currentEndOffset = state.index.getCurrentEndOffset();
    if (rollOverWithPutRecord) {
      indexEntries.addAll(
          addPutEntries(currentEndOffset, null, 1, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time, 10,
              false));
    } else {
      IndexEntry entryToDelete = indexEntries.get(TestUtils.RANDOM.nextInt(indexEntries.size()));
      state.appendToLog(state.DELETE_RECORD_SIZE);
      fileSpan = state.log.getFileSpanForMessage(currentEndOffset, CuratedLogIndexState.DELETE_RECORD_SIZE);
      state.index.markAsDeleted(entryToDelete.getKey(), fileSpan, state.time.milliseconds());
      // remove entryToDelete from indexEntries as it will be part of latest index segment
      indexEntries.remove(entryToDelete);
    }
    assertEquals("Index roll over should have happened ", indexCount + 1, state.index.getIndexSegments().size());
    currentEndOffset = state.index.getCurrentEndOffset();
    indexEntries.addAll(
        addPutEntries(currentEndOffset, null, 2, CuratedLogIndexState.PUT_RECORD_SIZE, Utils.Infinite_Time, 10, false));
    assertEquals("Index roll over should not have happened ", indexCount + 1, state.index.getIndexSegments().size());
    // verify index values
    verifyIndexValues(indexEntries);
  }

  /**
   * Adds entries to the index and asserts that the number of index segments and the persistedEntrySizes of the latest
   * segment are as expected.
   * @param indexEntries the list of {@link IndexEntry} to fill with the entries that are added to the index
   *                     (used for verification by the caller)
   * @param keySize the length of the ids used within the keys that are to be created for the entries.
   * @param numEntriesToAdd the number of entries to be added.
   * @param expectedSegmentCount the expected number of index segments after the entries are added.
   * @param latestSegmentExpectedEntrySize the expected persistedEntrySize of the latest segment after the entries are
   *                                       added.
   * @param createV0IndexValue if true, create {@link IndexValue} in V0.
   * @throws IOException
   * @throws StoreException
   */
  private void addEntriesAndAssert(List<IndexEntry> indexEntries, int keySize, int numEntriesToAdd,
      int expectedSegmentCount, int latestSegmentExpectedEntrySize, boolean createV0IndexValue)
      throws IOException, StoreException {
    Offset currentEndOffset = state.index.getCurrentEndOffset();
    indexEntries.addAll(addPutEntries(currentEndOffset, null, numEntriesToAdd, CuratedLogIndexState.PUT_RECORD_SIZE,
        Utils.Infinite_Time, keySize, createV0IndexValue));
    assertEquals("Unexpected index segment count", expectedSegmentCount, state.index.getIndexSegments().size());
    assertEquals(latestSegmentExpectedEntrySize,
        state.index.getIndexSegments().lastEntry().getValue().getPersistedEntrySize());
  }

  /**
   * Generate {@link IndexSegment} of version {@link PersistentIndex#VERSION_0}
   * @param startOffset the start offset of the {@link IndexSegment}
   * @param entrySize The entry size that this segment supports
   * @param valueSize The value size that this segment supports
   * @param persistentIndexVersion
   * @return the {@link IndexSegment} created of version {@link PersistentIndex#VERSION_0}
   */
  private IndexSegment generateIndexSegment(Offset startOffset, int entrySize, int valueSize,
      short persistentIndexVersion) {
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    StoreConfig config = new StoreConfig(new VerifiableProperties(state.properties));
    return new MockIndexSegment(tempDir.getAbsolutePath(), startOffset, CuratedLogIndexState.STORE_KEY_FACTORY,
        entrySize, valueSize, config, metrics, state.time, persistentIndexVersion);
  }

  /**
   * Adds {@link IndexEntry}s to the given {@link IndexSegment} for {@link IndexValue}s of version
   * {@link PersistentIndex#VERSION_0} or {@link PersistentIndex#VERSION_1} or to the actual {@link PersistentIndex} for
   * {@link IndexValue}s of version {@link PersistentIndex#CURRENT_VERSION}
   * @param prevEntryEndOffset end offset of last {@link IndexEntry}
   * @param indexSegment the {@link IndexSegment} to which put entries need to be added. If {@code null},
   *                     {@link IndexEntry}s will be added to the {@link PersistentIndex}
   * @param count total count of put entries to be added
   * @param size size of put entries
   * @param expiresAtMs expiration value at ms for the put entries
   * @param createV0IndexValue if true, create {@link IndexValue} in V0.
   * @return a list {@link IndexEntry}s added to the {@link PersistentIndex}
   * @throws StoreException
   */
  private List<IndexEntry> addPutEntries(Offset prevEntryEndOffset, IndexSegment indexSegment, int count, long size,
      long expiresAtMs, int idLength, boolean createV0IndexValue) throws StoreException {
    List<IndexEntry> indexEntries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      state.appendToLog(size);
      FileSpan fileSpan = state.log.getFileSpanForMessage(prevEntryEndOffset, size);
      IndexEntry entry;
      MockId putId = state.getUniqueId(idLength);
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      if (indexSegment != null) {
        entry = new IndexEntry(putId,
            IndexValueTest.getIndexValue(size, prevEntryEndOffset, expiresAtMs, state.time.milliseconds(), accountId,
                containerId, (short) 0, indexSegment.getVersion()));
        indexSegment.addEntry(entry, fileSpan.getEndOffset());
      } else {
        entry = new IndexEntry(putId,
            IndexValueTest.getIndexValue(size, prevEntryEndOffset, expiresAtMs, state.time.milliseconds(), accountId,
                containerId, (short) 0,
                createV0IndexValue ? PersistentIndex.VERSION_0 : PersistentIndex.CURRENT_VERSION));
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
      IndexValue value = state.index.findKey(entry.getKey(), null,
          EnumSet.of(PersistentIndex.IndexEntryType.PUT, PersistentIndex.IndexEntryType.DELETE));
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
      assertEquals("AccountId mismatch for " + entry.getKey(), expectedValue.getAccountId(), value.getAccountId());
      assertEquals("ContainerId mismatch for " + entry.getKey(), expectedValue.getContainerId(),
          value.getContainerId());
    }
  }

  /**
   * Call {@link PersistentIndex#findMissingKeysInBatch} on all the keys from {@link IndexSegment} referenced by the given
   * {@code offset}.
   * @param offset The given offset referencing the IndexSegment.
   * @param expectedFindKeyCallCount Expected findKey call count.
   * @param expectedCachedKeysOffsets Expected cached keys offsets.
   * @throws StoreException
   */
  private void findMissingKeysInBatchForIndexSegmentAndVerify(Offset offset, long expectedFindKeyCallCount,
      List<Offset> expectedCachedKeysOffsets, String dataNode) throws StoreException {
    List<StoreKey> indexSegmentKeys = new LinkedList<>(state.referenceIndex.get(offset).keySet());
    long findKeyCallCountBefore = state.metrics.findTime.getCount();
    Assert.assertEquals(0, state.index.findMissingKeysInBatch(indexSegmentKeys, dataNode).size());
    long findKeyCallCountAfter = state.metrics.findTime.getCount();
    Assert.assertEquals(expectedFindKeyCallCount, findKeyCallCountAfter - findKeyCallCountBefore);
    if (expectedCachedKeysOffsets != null) {
      LinkedList<Pair<Offset, Set<StoreKey>>> cachedKeys = state.index.getCachedKeys().get(dataNode);
      Assert.assertEquals(expectedCachedKeysOffsets.size(), cachedKeys.size());
      for (int j = 0; j < expectedCachedKeysOffsets.size(); j++) {
        Assert.assertEquals(expectedCachedKeysOffsets.get(j), cachedKeys.get(j).getFirst());
      }
    }
  }

  /**
   * Return random keys from the referenceIndex in curated log index state. the given indexes are the indexes of index
   * segments, it starts with 0.
   * @param indexes the list of indexes.
   * @return A list of store keys.
   */
  private List<StoreKey> getRandomKeyFromIndexSegment(int... indexes) {
    List<Offset> offsets = new ArrayList<>();
    state.referenceIndex.keySet().forEach(offset -> offsets.add(offset));

    List<StoreKey> results = new ArrayList<>(indexes.length);
    for (int index : indexes) {
      Offset offset = offsets.get(index);
      results.add(state.referenceIndex.get(offset).keySet().iterator().next());
    }
    return results;
  }
}
