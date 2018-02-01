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
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.WriteStatusDelegate;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link BlobStore}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class BlobStoreTest {
  private static final StoreKeyFactory STORE_KEY_FACTORY;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  // setupTestState() is coupled to these numbers. Changing them *will* cause setting test state or tests to fail.
  private static final long LOG_CAPACITY = 10000;
  private static final long SEGMENT_CAPACITY = 2000;
  private static final int MAX_IN_MEM_ELEMENTS = 5;
  // deliberately do not divide the capacities perfectly.
  private static final int PUT_RECORD_SIZE = 53;
  private static final int DELETE_RECORD_SIZE = 29;

  private final Random random = new Random();

  /**
   * A mock implementation of {@link MessageWriteSet} to help write to the {@link BlobStore}
   */
  private static class MockMessageWriteSet implements MessageWriteSet {
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
  private static class MockMessageStoreHardDelete implements MessageStoreHardDelete {
    private MessageInfo messageInfo = null;

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
   * An abstraction to unify and contains the results from the {@link Putter}, {@link Getter} and {@link Deleter}
   */
  private static class CallableResult {
    // Will be non-null only for PUT.
    final MockId id;
    // Will be non-null only for GET.
    final StoreInfo storeInfo;

    CallableResult(MockId id, StoreInfo storeInfo) {
      this.id = id;
      this.storeInfo = storeInfo;
    }
  }

  // a static instance to return for Deleter::call().
  private static final CallableResult EMPTY_RESULT = new CallableResult(null, null);

  /**
   * Puts a blob and returns the {@link MockId} associated with it.
   */
  private class Putter implements Callable<CallableResult> {

    @Override
    public CallableResult call() throws Exception {
      return new CallableResult(put(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0), null);
    }
  }

  /**
   * Gets a blob by returning the {@link StoreInfo} associated with it.
   */
  private class Getter implements Callable<CallableResult> {
    final MockId id;
    final EnumSet<StoreGetOptions> storeGetOptions;

    /**
     * @param id the {@link MockId} of the blob to GET
     * @param storeGetOptions options for the GET.
     */
    Getter(MockId id, EnumSet<StoreGetOptions> storeGetOptions) {
      this.id = id;
      this.storeGetOptions = storeGetOptions;
    }

    @Override
    public CallableResult call() throws Exception {
      return new CallableResult(null, store.get(Collections.singletonList(id), storeGetOptions));
    }
  }

  /**
   * Deletes a blob.
   */
  private class Deleter implements Callable<CallableResult> {

    final MockId id;

    /**
     * @param id the {@link MockId} to delete.
     */
    Deleter(MockId id) {
      this.id = id;
    }

    @Override
    public CallableResult call() throws Exception {
      delete(id);
      return EMPTY_RESULT;
    }
  }

  // used by getUniqueId() to make sure keys are never regenerated in a single test run.
  private final Set<MockId> generatedKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());
  // A map of all the keys. The key is the MockId and the value is a Pair that contains the metadata and data of the
  // message.
  private final Map<MockId, Pair<MessageInfo, ByteBuffer>> allKeys = new ConcurrentHashMap<>();
  // A list of keys grouped by the log segment that they belong to
  private final List<Set<MockId>> idsByLogSegment = new ArrayList<>();
  // Set of all deleted keys
  private final Set<MockId> deletedKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());
  // Set of all expired keys
  private final Set<MockId> expiredKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());
  // Set of all keys that are not deleted/expired
  private final Set<MockId> liveKeys = Collections.newSetFromMap(new ConcurrentHashMap<MockId, Boolean>());

  // Indicates whether the log is segmented
  private final boolean isLogSegmented;
  // Variables that represent the folder where the data resides
  private final File tempDir;
  private final String tempDirStr;
  // the time instance that will be used in the index
  private final Time time = new MockTime();

  private final String storeId = UtilsTest.getRandomString(10);
  private final DiskIOScheduler diskIOScheduler = new DiskIOScheduler(null);
  private final DiskSpaceAllocator diskSpaceAllocator = StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR;
  private final ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
  private final ScheduledExecutorService storeStatsScheduler = Utils.newScheduler(1, false);
  private final Properties properties = new Properties();

  // The BlobStore instance
  private BlobStore store;
  // The MessageStoreRecovery that is used with the BlobStore
  private MessageStoreRecovery recovery = new DummyMessageStoreRecovery();
  // The MessageStoreHardDelete that is used with the BlobStore
  private MessageStoreHardDelete hardDelete = new MockMessageStoreHardDelete();

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
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  public BlobStoreTest(boolean isLogSegmented) throws InterruptedException, IOException, StoreException {
    this.isLogSegmented = isLogSegmented;
    tempDir = StoreTestUtils.createTempDirectory("storeDir-" + UtilsTest.getRandomString(10));
    tempDirStr = tempDir.getAbsolutePath();
    setupTestState();
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    if (store.isStarted()) {
      store.shutdown();
    }
    scheduler.shutdown();
    assertTrue(scheduler.awaitTermination(1, TimeUnit.SECONDS));
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Tests blob store use of {@link WriteStatusDelegate}
   * @throws StoreException
   */
  @Test
  public void testClusterManagerWriteStatusDelegateUse() throws StoreException, IOException, InterruptedException {
    //Setup threshold test properties, replicaId, mock write status delegate
    StoreConfig defaultConfig = changeThreshold(65, 5, true);
    StoreTestUtils.MockReplicaId replicaId = getMockReplicaId(tempDirStr);
    WriteStatusDelegate writeStatusDelegate = mock(WriteStatusDelegate.class);
    when(writeStatusDelegate.unseal(any())).thenReturn(true);
    when(writeStatusDelegate.seal(any())).thenReturn(true);

    //Restart store
    restartStore(defaultConfig, replicaId, writeStatusDelegate);

    //Check that after start, because ReplicaId defaults to non-sealed, delegate is not called
    verifyZeroInteractions(writeStatusDelegate);

    //Verify that putting in data that doesn't go over the threshold doesn't trigger the delegate
    put(1, 50, Utils.Infinite_Time);
    verifyNoMoreInteractions(writeStatusDelegate);

    //Verify that after putting in enough data, the store goes to read only
    List<MockId> addedIds = put(4, 900, Utils.Infinite_Time);
    verify(writeStatusDelegate, times(1)).seal(replicaId);

    //Assumes ClusterParticipant sets replicaId status to true
    replicaId.setSealedState(true);

    //Change config threshold but with delegate disabled, verify that nothing happens
    restartStore(changeThreshold(99, 1, false), replicaId, writeStatusDelegate);
    verifyNoMoreInteractions(writeStatusDelegate);

    //Change config threshold to higher, see that it gets changed to unsealed on reset
    restartStore(changeThreshold(99, 1, true), replicaId, writeStatusDelegate);
    verify(writeStatusDelegate, times(1)).unseal(replicaId);
    replicaId.setSealedState(false);

    //Reset thresholds, verify that it changed back
    restartStore(defaultConfig, replicaId, writeStatusDelegate);
    verify(writeStatusDelegate, times(2)).seal(replicaId);
    replicaId.setSealedState(true);

    //Remaining tests only relevant for segmented logs
    if (isLogSegmented) {
      //Delete added data
      for (MockId addedId : addedIds) {
        delete(addedId);
      }

      //Need to restart blob otherwise compaction will ignore segments in journal (which are all segments right now).
      //By restarting, only last segment will be in journal
      restartStore(defaultConfig, replicaId, writeStatusDelegate);
      verifyNoMoreInteractions(writeStatusDelegate);

      //Advance time by 8 days, call compaction to compact segments with deleted data, then verify
      //that the store is now read-write
      time.sleep(TimeUnit.DAYS.toMillis(8));
      store.compact(store.getCompactionDetails(new CompactAllPolicy(defaultConfig, time)));
      verify(writeStatusDelegate, times(2)).unseal(replicaId);

      //Test if replicaId is erroneously true that it updates the status upon startup
      replicaId.setSealedState(true);
      restartStore(defaultConfig, replicaId, writeStatusDelegate);
      verify(writeStatusDelegate, times(3)).unseal(replicaId);
    }
    store.shutdown();
  }

  /**
   * Tests {@link BlobStore#start()} for corner cases and error cases.
   * Corner cases
   * 1. Creating a directory on first startup
   * Error cases
   * 1. Start an already started store.
   * 2. Unable to create store directory on first startup.
   * 3. Starting two stores at the same path.
   * 4. Directory not readable.
   * 5. Path is not a directory.
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void storeStartupTests() throws IOException, StoreException {

    // attempt to start when store is already started fails
    verifyStartupFailure(store, StoreErrorCodes.Store_Already_Started);

    String nonExistentDir = new File(tempDir, UtilsTest.getRandomString(10)).getAbsolutePath();

    // fail if attempt to create directory fails
    String badPath = new File(nonExistentDir, UtilsTest.getRandomString(10)).getAbsolutePath();

    BlobStore blobStore = createBlobStore(getMockReplicaId(badPath));

    verifyStartupFailure(blobStore, StoreErrorCodes.Initialization_Error);

    ReplicaId replicaIdWithNonExistentDir = getMockReplicaId(nonExistentDir);

    // create directory if it does not exist
    blobStore = createBlobStore(replicaIdWithNonExistentDir);
    verifyStartupSuccess(blobStore);
    File createdDir = new File(nonExistentDir);
    assertTrue("Directory should now exist", createdDir.exists() && createdDir.isDirectory());

    // should not be able to start two stores at the same path
    blobStore = createBlobStore(replicaIdWithNonExistentDir);
    blobStore.start();
    BlobStore secondStore = createBlobStore(replicaIdWithNonExistentDir);
    verifyStartupFailure(secondStore, StoreErrorCodes.Initialization_Error);
    blobStore.shutdown();

    // fail if directory is not readable
    assertTrue("Could not set readable state to false", createdDir.setReadable(false));
    verifyStartupFailure(blobStore, StoreErrorCodes.Initialization_Error);

    assertTrue("Could not set readable state to true", createdDir.setReadable(true));
    assertTrue("Directory could not be deleted", StoreTestUtils.cleanDirectory(createdDir, true));

    // fail if provided path is not a directory
    File file = new File(tempDir, UtilsTest.getRandomString(10));
    assertTrue("Test file could not be created", file.createNewFile());
    file.deleteOnExit();
    blobStore = createBlobStore(getMockReplicaId(file.getAbsolutePath()));
    verifyStartupFailure(blobStore, StoreErrorCodes.Initialization_Error);
  }

  /**
   * Does a basic test by getting all the blobs that were put (and deleted) during the test setup. This implicitly tests
   * all three of PUT, GET and DELETE.
   * </p>
   * Also tests GET with different combinations of {@link StoreGetOptions}.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @Test
  public void basicTest() throws InterruptedException, IOException, StoreException {
    // PUT a key that is slated to expire when time advances by 1s
    MockId addedId = put(1, PUT_RECORD_SIZE, time.seconds() + 1).get(0);
    time.sleep(2 * Time.MsPerSec);
    liveKeys.remove(addedId);
    expiredKeys.add(addedId);

    // GET of all the keys implicitly tests the PUT and DELETE.
    // live keys
    StoreInfo storeInfo = store.get(new ArrayList<>(liveKeys), EnumSet.noneOf(StoreGetOptions.class));
    checkStoreInfo(storeInfo, liveKeys);

    MockMessageStoreHardDelete hd = (MockMessageStoreHardDelete) hardDelete;
    for (MockId id : deletedKeys) {
      hd.setMessageInfo(allKeys.get(id).getFirst());

      // cannot get without StoreGetOptions
      verifyGetFailure(id, StoreErrorCodes.ID_Deleted);

      // with StoreGetOptions.Store_Include_Deleted
      storeInfo = store.get(Collections.singletonList(id), EnumSet.of(StoreGetOptions.Store_Include_Deleted));
      checkStoreInfo(storeInfo, Collections.singleton(id));

      // with all StoreGetOptions
      storeInfo = store.get(Collections.singletonList(id), EnumSet.allOf(StoreGetOptions.class));
      checkStoreInfo(storeInfo, Collections.singleton(id));
    }

    for (MockId id : expiredKeys) {
      // cannot get without StoreGetOptions
      verifyGetFailure(id, StoreErrorCodes.TTL_Expired);

      // with StoreGetOptions.Store_Include_Expired
      storeInfo = store.get(Collections.singletonList(id), EnumSet.of(StoreGetOptions.Store_Include_Expired));
      checkStoreInfo(storeInfo, Collections.singleton(id));

      // with all StoreGetOptions
      storeInfo = store.get(Collections.singletonList(id), EnumSet.allOf(StoreGetOptions.class));
      checkStoreInfo(storeInfo, Collections.singleton(id));
    }

    // should be able to delete expired blobs
    delete(addedId);

    // non existent ID has to fail
    verifyGetFailure(getUniqueId(), StoreErrorCodes.ID_Not_Found);
  }

  /**
   * Tests the case where there are many concurrent PUTs.
   * @throws Exception
   */
  @Test
  public void concurrentPutTest() throws Exception {
    int blobCount = 4000 / PUT_RECORD_SIZE + 1;
    List<Putter> putters = new ArrayList<>(blobCount);
    for (int i = 0; i < blobCount; i++) {
      putters.add(new Putter());
    }
    ExecutorService executorService = Executors.newFixedThreadPool(putters.size());
    List<Future<CallableResult>> futures = executorService.invokeAll(putters);
    verifyPutFutures(putters, futures);
  }

  /**
   * Tests the case where there are many concurrent GETs.
   * @throws Exception
   */
  @Test
  public void concurrentGetTest() throws Exception {
    int extraBlobCount = 4000 / PUT_RECORD_SIZE + 1;
    put(extraBlobCount, PUT_RECORD_SIZE, Utils.Infinite_Time);
    List<Getter> getters = new ArrayList<>(allKeys.size());
    for (MockId id : allKeys.keySet()) {
      getters.add(new Getter(id, EnumSet.noneOf(StoreGetOptions.class)));
    }
    ExecutorService executorService = Executors.newFixedThreadPool(getters.size());
    List<Future<CallableResult>> futures = executorService.invokeAll(getters);
    verifyGetFutures(getters, futures);
  }

  /**
   * Tests the case where there are many concurrent DELETEs.
   * @throws Exception
   */
  @Test
  public void concurrentDeleteTest() throws Exception {
    int extraBlobCount = 2000 / PUT_RECORD_SIZE + 1;
    put(extraBlobCount, PUT_RECORD_SIZE, Utils.Infinite_Time);
    List<Deleter> deleters = new ArrayList<>(liveKeys.size());
    for (MockId id : liveKeys) {
      deleters.add(new Deleter(id));
    }
    ExecutorService executorService = Executors.newFixedThreadPool(deleters.size());
    List<Future<CallableResult>> futures = executorService.invokeAll(deleters);
    verifyDeleteFutures(deleters, futures);
  }

  /**
   * Tests the case where there are concurrent PUTs, GETs and DELETEs.
   * @throws Exception
   */
  @Test
  public void concurrentAllTest() throws Exception {
    int putBlobCount = 1500 / PUT_RECORD_SIZE + 1;
    List<Putter> putters = new ArrayList<>(putBlobCount);
    for (int i = 0; i < putBlobCount; i++) {
      putters.add(new Putter());
    }

    List<Getter> getters = new ArrayList<>(liveKeys.size());
    for (MockId id : liveKeys) {
      getters.add(new Getter(id, EnumSet.allOf(StoreGetOptions.class)));
    }

    int deleteBlobCount = 1500 / PUT_RECORD_SIZE;
    List<MockId> idsToDelete = put(deleteBlobCount, PUT_RECORD_SIZE, Utils.Infinite_Time);
    List<Deleter> deleters = new ArrayList<>(deleteBlobCount);
    for (MockId id : idsToDelete) {
      deleters.add(new Deleter(id));
    }

    List<Callable<CallableResult>> callables = new ArrayList<Callable<CallableResult>>(putters);
    callables.addAll(getters);
    callables.addAll(deleters);

    ExecutorService executorService = Executors.newFixedThreadPool(callables.size());
    List<Future<CallableResult>> futures = executorService.invokeAll(callables);
    verifyPutFutures(putters, futures.subList(0, putters.size()));
    verifyGetFutures(getters, futures.subList(putters.size(), putters.size() + getters.size()));
    verifyDeleteFutures(deleters, futures.subList(putters.size() + getters.size(), callables.size()));
  }

  /**
   * Tests error cases for {@link BlobStore#put(MessageWriteSet)}.
   */
  @Test
  public void putErrorCasesTest() {
    // ID that exists
    // live
    verifyPutFailure(liveKeys.iterator().next(), StoreErrorCodes.Already_Exist);
    // expired
    verifyPutFailure(expiredKeys.iterator().next(), StoreErrorCodes.Already_Exist);
    // deleted
    verifyPutFailure(deletedKeys.iterator().next(), StoreErrorCodes.Already_Exist);
  }

  /**
   * Tests error cases for {@link BlobStore#delete(MessageWriteSet)}.
   */
  @Test
  public void deleteErrorCasesTest() {
    // ID that is already deleted
    verifyDeleteFailure(deletedKeys.iterator().next(), StoreErrorCodes.ID_Deleted);
    // ID that does not exist
    verifyDeleteFailure(getUniqueId(), StoreErrorCodes.ID_Not_Found);
  }

  /**
   * Test DELETE authorization success for {@link BlobStore#delete(MessageWriteSet)}.
   * If either accountId or containerId is -1, validation is skipped.
   */
  @Test
  public void deleteAuthorizationSuccessTest() throws Exception {
    short[] accountIds = {-1, Utils.getRandomShort(TestUtils.RANDOM), -1};
    short[] containerIds = {-1, -1, Utils.getRandomShort(TestUtils.RANDOM)};
    for (int i = 0; i < accountIds.length; i++) {
      MockId mockId = put(1, PUT_RECORD_SIZE, Utils.Infinite_Time, accountIds[i], containerIds[i]).get(0);
      delete(
          new MockId(mockId.getID(), Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM)));
      verifyDeleteFailure(mockId, StoreErrorCodes.ID_Deleted);
    }
  }

  /**
   * Test DELETE authorization failure for {@link BlobStore#delete(MessageWriteSet)}.
   */
  @Test
  public void deleteAuthorizationFailureTest() throws Exception {
    MockId mockId = put(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    assertNotSame("accountId should not be -1 for added Blob.", -1, mockId.getAccountId());
    assertNotSame("containerId should not be -1 for added Blob.", -1, mockId.getContainerId());
    short[] accountIds =
        {-1, Utils.getRandomShort(TestUtils.RANDOM), -1, mockId.getAccountId(), Utils.getRandomShort(TestUtils.RANDOM)};
    short[] containerIds = {-1, -1, Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(
        TestUtils.RANDOM), mockId.getContainerId()};
    for (int i = 0; i < accountIds.length; i++) {
      verifyDeleteFailure(new MockId(mockId.getID(), accountIds[i], containerIds[i]),
          StoreErrorCodes.Authorization_Failure);
    }
  }

  /**
   * Test GET authorization success for {@link BlobStore#get(List, EnumSet)}.
   * If either accountId or containerId is -1, validation is skipped.
   */
  @Test
  public void getAuthorizationSuccessTest() throws Exception {
    short[] accountIds = {-1, Utils.getRandomShort(TestUtils.RANDOM), -1};
    short[] containerIds = {-1, -1, Utils.getRandomShort(TestUtils.RANDOM)};
    for (int i = 0; i < accountIds.length; i++) {
      MockId mockId = put(1, PUT_RECORD_SIZE, Utils.Infinite_Time, accountIds[i], containerIds[i]).get(0);
      StoreInfo storeinfo = store.get(Collections.singletonList(
          new MockId(mockId.getID(), Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM))),
          EnumSet.noneOf(StoreGetOptions.class));
      checkStoreInfo(storeinfo, Collections.singleton(mockId));
    }
  }

  /**
   * Test GET authorization failure for {@link BlobStore#get(List, EnumSet)}.
   */
  @Test
  public void getAuthorizationFailureTest() throws Exception {
    MockId mockId = put(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
    assertNotSame("accountId should not be -1 for added Blob.", -1, mockId.getAccountId());
    assertNotSame("containerId should not be -1 for added Blob.", -1, mockId.getContainerId());
    short[] accountIds =
        {-1, Utils.getRandomShort(TestUtils.RANDOM), -1, mockId.getAccountId(), Utils.getRandomShort(TestUtils.RANDOM)};
    short[] containerIds = {-1, -1, Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(
        TestUtils.RANDOM), mockId.getContainerId()};
    for (int i = 0; i < accountIds.length; i++) {
      verifyGetFailure(new MockId(mockId.getID(), accountIds[i], containerIds[i]), StoreErrorCodes.Authorization_Failure);
    }
  }

  /**
   * Test various duplicate and collision cases for {@link BlobStore#put(MessageWriteSet)}
   * @throws Exception
   */
  @Test
  public void idCollisionTest() throws Exception {
    // Populate global lists of keys and crcs.
    List<StoreKey> allMockIdList = new ArrayList<>();
    List<Long> allCrcList = new ArrayList<>();
    for (long i = 0; i < 4; i++) {
      allMockIdList.add(new MockId(Long.toString(i)));
      allCrcList.add(i);
    }

    // Put the initial two messages.
    List<StoreKey> mockIdList = Arrays.asList(allMockIdList.get(0), allMockIdList.get(1));
    List<Long> crcList = Arrays.asList(allCrcList.get(0), allCrcList.get(1));
    Set<StoreKey> missingKeysAfter = new HashSet<>(Arrays.asList(allMockIdList.get(2), allMockIdList.get(3)));
    putWithKeysAndCrcs(mockIdList, crcList);
    assertEquals(missingKeysAfter, store.findMissingKeys(allMockIdList));

    // 1. SOME_NOT_ALL_DUPLICATE - should fail.
    // first one duplicate, second one absent.
    mockIdList = Arrays.asList(allMockIdList.get(0), allMockIdList.get(2));
    crcList = Arrays.asList(allCrcList.get(0), allCrcList.get(2));
    try {
      putWithKeysAndCrcs(mockIdList, crcList);
      fail("Put should fail if some keys exist, but some do not");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Already_Exist, e.getErrorCode());
    }
    assertEquals(missingKeysAfter, store.findMissingKeys(allMockIdList));

    // first one absent, second one duplicate.
    mockIdList = Arrays.asList(allMockIdList.get(2), allMockIdList.get(0));
    crcList = Arrays.asList(allCrcList.get(2), allCrcList.get(0));
    try {
      putWithKeysAndCrcs(mockIdList, crcList);
      fail("Put should fail if some keys exist, but some do not");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Already_Exist, e.getErrorCode());
    }
    assertEquals(missingKeysAfter, store.findMissingKeys(allMockIdList));

    // 2. COLLIDING - should fail.
    // first one duplicate, second one colliding.
    mockIdList = Arrays.asList(allMockIdList.get(0), allMockIdList.get(1));
    crcList = Arrays.asList(allCrcList.get(0), allCrcList.get(2));
    try {
      putWithKeysAndCrcs(mockIdList, crcList);
      fail("Put should fail if some keys exist, but some do not");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Already_Exist, e.getErrorCode());
    }
    assertEquals(missingKeysAfter, store.findMissingKeys(allMockIdList));

    // first one absent, second one colliding.
    mockIdList = Arrays.asList(allMockIdList.get(3), allMockIdList.get(1));
    crcList = Arrays.asList(allCrcList.get(3), allCrcList.get(2));
    try {
      putWithKeysAndCrcs(mockIdList, crcList);
      fail("Put should fail if some keys exist, but some do not");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Already_Exist, e.getErrorCode());
    }
    assertEquals(missingKeysAfter, store.findMissingKeys(allMockIdList));

    // 3. ALL_DUPLICATE - should succeed.
    mockIdList = Arrays.asList(allMockIdList.get(0), allMockIdList.get(1));
    crcList = Arrays.asList(allCrcList.get(0), allCrcList.get(1));
    putWithKeysAndCrcs(mockIdList, crcList);
    assertEquals(missingKeysAfter, store.findMissingKeys(allMockIdList));

    // 4. ALL_ABSENT
    mockIdList = Arrays.asList(allMockIdList.get(2), allMockIdList.get(3));
    crcList = Arrays.asList(allCrcList.get(2), allCrcList.get(3));
    putWithKeysAndCrcs(mockIdList, crcList);
    // Ensure that all new entries were added.
    missingKeysAfter.clear();
    assertEquals(missingKeysAfter, store.findMissingKeys(allMockIdList));
  }

  /**
   * Tests {@link BlobStore#findEntriesSince(FindToken, long)}.
   * <p/>
   * This test is minimal for two reasons
   * 1. The BlobStore simply calls into the index for this function and the index has extensive tests for this.
   * 2. It is almost impossible to validate the StoreFindToken here.
   * If the first condition changes, then more tests will be required here
   * @throws StoreException
   */
  @Test
  public void findEntriesSinceTest() throws StoreException {
    FindInfo findInfo = store.findEntriesSince(new StoreFindToken(), Long.MAX_VALUE);
    Set<StoreKey> keysPresent = new HashSet<>();
    for (MessageInfo info : findInfo.getMessageEntries()) {
      keysPresent.add(info.getStoreKey());
    }
    assertEquals("All keys were not present in the return from findEntriesSince()", allKeys.keySet(), keysPresent);
  }

  /**
   * Tests {@link BlobStore#findMissingKeys(List)}.
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
    Set<StoreKey> missingKeys = store.findMissingKeys(idsToProvide);
    assertEquals("Set of missing keys not as expected", nonExistentIds, missingKeys);
  }

  /**
   * Tests {@link BlobStore#isKeyDeleted(StoreKey)} including error cases.
   * @throws StoreException
   */
  @Test
  public void isKeyDeletedTest() throws StoreException {
    for (MockId id : allKeys.keySet()) {
      assertEquals("Returned state is not as expected", deletedKeys.contains(id), store.isKeyDeleted(id));
    }
    // non existent id
    try {
      store.isKeyDeleted(getUniqueId());
      fail("Getting the deleted state of a non existent key should have failed");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Not_Found, e.getErrorCode());
    }
  }

  /**
   * Tests store shutdown and the ability to do operations when a store is shutdown or has not been started yet.
   * @throws StoreException
   */
  @Test
  public void shutdownTest() throws StoreException {
    store.shutdown();
    // no operations should be possible if store is not up or has been shutdown
    verifyOperationFailuresOnInactiveStore(store);
    store = createBlobStore(getMockReplicaId(tempDirStr));
    verifyOperationFailuresOnInactiveStore(store);
  }

  /**
   * Tests that {@link BlobStore#getDiskSpaceRequirements()} functions as expected.
   * @throws StoreException
   */
  @Test
  public void diskSpaceRequirementsTest() throws Exception {
    // expect three log segments to be already allocated (from setup process)
    int segmentsAllocated = 3;
    doDiskSpaceRequirementsTest(segmentsAllocated, 0);

    // try adding fake swap segment log segment.
    File tempFile = File.createTempFile("sample-swap",
        LogSegmentNameHelper.SUFFIX + BlobStoreCompactor.TEMP_LOG_SEGMENT_NAME_SUFFIX, tempDir);
    doDiskSpaceRequirementsTest(segmentsAllocated, 1);
    assertTrue("Could not delete temp file", tempFile.delete());

    addCuratedData(SEGMENT_CAPACITY);
    segmentsAllocated += 1;
    doDiskSpaceRequirementsTest(segmentsAllocated, 0);

    File.createTempFile("sample-swap", LogSegmentNameHelper.SUFFIX + BlobStoreCompactor.TEMP_LOG_SEGMENT_NAME_SUFFIX,
        tempDir).deleteOnExit();
    File.createTempFile("sample-swap", LogSegmentNameHelper.SUFFIX + BlobStoreCompactor.TEMP_LOG_SEGMENT_NAME_SUFFIX,
        tempDir).deleteOnExit();
    addCuratedData(SEGMENT_CAPACITY);
    segmentsAllocated += 1;
    doDiskSpaceRequirementsTest(segmentsAllocated, 2);
  }

  // helpers
  // general

  /**
   * @return a {@link MockId} that is unique and has not been generated before in this run.
   */
  private MockId getUniqueId() {
    return getUniqueId(Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM));
  }

  /**
   * Get a unique MockId with specified accountId and containerId.
   * @param accountId the accountId of the MockId.
   * @param containerId the containerId of the MockId.
   * @return a {@link MockId} that is unique and has not been generated before in this run.
   */
  private MockId getUniqueId(short accountId, short containerId) {
    MockId id;
    do {
      id = new MockId(UtilsTest.getRandomString(10), accountId, containerId);
    } while (generatedKeys.contains(id));
    generatedKeys.add(id);
    return id;
  }

  /**
   * Puts some blobs into the {@link BlobStore}.
   * @param count the number of blobs to PUT.
   * @param size the size of each blob.
   * @param expiresAtMs the expiry time (in ms) of each blob.
   * @return the {@link MockId}s of the blobs created.
   * @throws StoreException
   */
  private List<MockId> put(int count, long size, long expiresAtMs) throws StoreException {
    return put(count, size, expiresAtMs, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM));
  }

  /**
   * Puts some blobs into the {@link BlobStore}.
   * @param count the number of blobs to PUT.
   * @param size the size of each blob.
   * @param expiresAtMs the expiry time (in ms) of each blob.
   * @param accountId the accountId of each blob.
   * @param containerId the containerId of each blob.
   * @return the {@link MockId}s of the blobs created.
   * @throws StoreException
   */
  private List<MockId> put(int count, long size, long expiresAtMs, short accountId, short containerId)
      throws StoreException {
    if (count <= 0) {
      throw new IllegalArgumentException("Number of put entries to add cannot be <= 0");
    }
    List<MockId> ids = new ArrayList<>(count);
    List<MessageInfo> infos = new ArrayList<>(count);
    List<ByteBuffer> buffers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      MockId id = getUniqueId(accountId, containerId);
      long crc = random.nextLong();
      MessageInfo info = new MessageInfo(id, size, false, expiresAtMs, crc, id.getAccountId(), id.getContainerId(),
          Utils.Infinite_Time);
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) size));
      ids.add(id);
      infos.add(info);
      buffers.add(buffer);
      allKeys.put(id, new Pair<>(info, buffer));
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
  private MessageInfo delete(MockId idToDelete) throws StoreException {
    MessageInfo putMsgInfo = allKeys.get(idToDelete).getFirst();
    MessageInfo info =
        new MessageInfo(idToDelete, DELETE_RECORD_SIZE, putMsgInfo.getAccountId(), putMsgInfo.getContainerId(),
            time.milliseconds());
    ByteBuffer buffer = ByteBuffer.allocate(DELETE_RECORD_SIZE);
    store.delete(new MockMessageWriteSet(Collections.singletonList(info), Collections.singletonList(buffer)));
    deletedKeys.add(idToDelete);
    return info;
  }

  /**
   * Verifies the provided {@code storeInfo} for correctness of the {@link MessageInfo}. Also reads the blob as
   * described by the {@link MessageReadSet} inside {@code storeInfo} to verify that it matches the reference.
   * @param storeInfo the {@link StoreInfo} to verify.
   * @param expectedKeys all the {@link MockId}s that are expected to be found in {@code storeInfo}.
   * @throws IOException
   */
  private void checkStoreInfo(StoreInfo storeInfo, Set<MockId> expectedKeys) throws IOException {
    List<MessageInfo> messageInfos = storeInfo.getMessageReadSetInfo();
    MessageReadSet readSet = storeInfo.getMessageReadSet();
    assertEquals("ReadSet contains an unexpected number of messages", expectedKeys.size(), readSet.count());
    Set<MockId> examinedKeys = new HashSet<>();
    for (int i = 0; i < messageInfos.size(); i++) {
      MessageInfo messageInfo = messageInfos.get(i);
      MockId id = (MockId) messageInfo.getStoreKey();
      MessageInfo expectedInfo = allKeys.get(id).getFirst();
      assertEquals("Unexpected size in MessageInfo", expectedInfo.getSize(), messageInfo.getSize());
      assertEquals("AccountId mismatch", expectedInfo.getAccountId(), messageInfo.getAccountId());
      assertEquals("ContainerId mismatch", expectedInfo.getContainerId(), messageInfo.getContainerId());
      assertEquals("OperationTime mismatch", expectedInfo.getOperationTimeMs(), messageInfo.getOperationTimeMs());
      assertEquals("Unexpected expiresAtMs in MessageInfo",
          (expectedInfo.getExpirationTimeInMs() != Utils.Infinite_Time ?
              (expectedInfo.getExpirationTimeInMs() / Time.MsPerSec) * Time.MsPerSec : Utils.Infinite_Time),
          messageInfo.getExpirationTimeInMs());

      assertEquals("Unexpected key in readSet", id, readSet.getKeyAt(i));
      assertEquals("Unexpected size in ReadSet", expectedInfo.getSize(), readSet.sizeInBytes(i));
      ByteBuffer readBuf = ByteBuffer.allocate((int) expectedInfo.getSize());
      ByteBufferOutputStream stream = new ByteBufferOutputStream(readBuf);
      WritableByteChannel channel = Channels.newChannel(stream);
      readSet.writeTo(i, channel, 0, expectedInfo.getSize());
      ByteBuffer expectedData = allKeys.get(id).getSecond();
      assertArrayEquals("Data obtained from reset does not match original", expectedData.array(), readBuf.array());
      examinedKeys.add(id);
    }
    assertEquals("Expected and examined keys do not match", expectedKeys, examinedKeys);
  }

  /**
   * Verifies that a GET of {@code id} fails.
   * @param id the {@link MockId} to GET.
   * @param expectedErrorCode the {@link StoreErrorCodes} for the failure.
   */
  private void verifyGetFailure(MockId id, StoreErrorCodes expectedErrorCode) {
    try {
      store.get(Collections.singletonList(id), EnumSet.noneOf(StoreGetOptions.class));
      fail("Should not be able to GET " + id);
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
    }
  }

  // test setup helpers

  /**
   * Sets up some state in order to make sure all cases are represented and the tests don't need to do any setup
   * individually. For understanding the created store, please read the source code which is annotated with comments.
   * @throws InterruptedException
   * @throws StoreException
   */
  private void setupTestState() throws InterruptedException, StoreException {
    long segmentCapacity = isLogSegmented ? SEGMENT_CAPACITY : LOG_CAPACITY;
    properties.put("store.index.max.number.of.inmem.elements", Integer.toString(MAX_IN_MEM_ELEMENTS));
    properties.put("store.segment.size.in.bytes", Long.toString(segmentCapacity));
    properties.put("store.validate.authorization", "true");
    store = createBlobStore(getMockReplicaId(tempDirStr));
    store.start();
    // advance time by a second in order to be able to add expired keys and to avoid keys that are expired from
    // being picked for delete.
    time.sleep(Time.MsPerSec);
    long expectedStoreSize;
    if (!isLogSegmented) {
      // log is filled about ~50%.
      expectedStoreSize = segmentCapacity / 2;
      addCuratedData(expectedStoreSize);
    } else {
      expectedStoreSize = segmentCapacity;
      // first log segment is filled to capacity.
      addCuratedData(segmentCapacity);
      assertEquals("Store size not as expected", expectedStoreSize, store.getSizeInBytes());

      // second log segment is filled but has some space at the end (free space has to be less than the lesser of the
      // standard delete and put record sizes so that the next write causes a roll over of log segments).
      long sizeToWrite = segmentCapacity - (DELETE_RECORD_SIZE - 1);
      expectedStoreSize += sizeToWrite;
      addCuratedData(sizeToWrite);
      assertEquals("Store size not as expected", expectedStoreSize, store.getSizeInBytes());

      // third log segment is partially filled and is left as the "active" segment
      sizeToWrite = segmentCapacity / 3;
      // First Index Segment
      // 1 PUT entry
      MockId addedId = put(1, PUT_RECORD_SIZE, Utils.Infinite_Time).get(0);
      idsByLogSegment.add(new HashSet<MockId>());
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
   * @throws StoreException
   */
  private void addCuratedData(long sizeToWrite) throws StoreException {
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
  private MockId getIdToDelete(Set<MockId> ids) {
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
  private void reloadStore() throws StoreException {
    if (store.isStarted()) {
      store.shutdown();
    }
    assertFalse("Store should be shutdown", store.isStarted());
    store = createBlobStore(getMockReplicaId(tempDirStr));
    assertFalse("Store should not be started", store.isStarted());
    store.start();
    assertTrue("Store should be started", store.isStarted());
  }

  // storeStartupTests() helpers

  /**
   * Verifies that {@link BlobStore#start()} executes successfully and shuts down the store.
   * @param blobStore the {@link BlobStore} to start.
   * @throws StoreException
   */
  private void verifyStartupSuccess(BlobStore blobStore) throws StoreException {
    blobStore.start();
    assertTrue("Store has not been started", blobStore.isStarted());
    blobStore.shutdown();
  }

  /**
   * Verifies that {@link BlobStore#start()} fails.
   * @param blobStore the {@link BlobStore} to start.
   * @param expectedErrorCode the expected {@link StoreErrorCodes} for the failure.
   */
  private void verifyStartupFailure(BlobStore blobStore, StoreErrorCodes expectedErrorCode) {
    try {
      blobStore.start();
      fail("Store start should have failed");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
    }
  }

  // concurrent tests helpers

  /**
   * Verifies that the blobs have been successfully put and the GETs them to make sure the metadata and data match the
   * reference.
   * @param putters list of {@link Putter} instances.
   * @param futures list of {@link Future}s returned from submitting the {@code putters} to an {@link ExecutorService}
   * @throws Exception
   */
  private void verifyPutFutures(List<Putter> putters, List<Future<CallableResult>> futures) throws Exception {
    for (int i = 0; i < putters.size(); i++) {
      Future<CallableResult> future = futures.get(i);
      MockId id = future.get(1, TimeUnit.SECONDS).id;
      StoreInfo storeInfo = store.get(Collections.singletonList(id), EnumSet.noneOf(StoreGetOptions.class));
      checkStoreInfo(storeInfo, Collections.singleton(id));
    }
  }

  /**
   * Verifies that the GETs initiated by the {@code getters} are successful and consistent with the state expected i.e.
   * live, expired or deleted. If live, return value is also verified for correctness.
   * @param getters list of {@link Getter} instances.
   * @param futures  list of {@link Future}s returned from submitting the {@code getters} to an {@link ExecutorService}
   * @throws Exception
   */
  private void verifyGetFutures(List<Getter> getters, List<Future<CallableResult>> futures) throws Exception {
    for (int i = 0; i < getters.size(); i++) {
      MockId id = getters.get(i).id;
      Future<CallableResult> future = futures.get(i);
      if (liveKeys.contains(id)) {
        StoreInfo storeInfo = future.get(1, TimeUnit.SECONDS).storeInfo;
        checkStoreInfo(storeInfo, Collections.singleton(getters.get(i).id));
      } else {
        try {
          future.get(1, TimeUnit.SECONDS);
          fail("Operation should have failed");
        } catch (ExecutionException e) {
          StoreException storeException = (StoreException) e.getCause();
          StoreErrorCodes expectedCode = StoreErrorCodes.ID_Not_Found;
          if (deletedKeys.contains(id)) {
            expectedCode = StoreErrorCodes.ID_Deleted;
          } else if (expiredKeys.contains(id)) {
            expectedCode = StoreErrorCodes.TTL_Expired;
          }
          assertEquals("Unexpected StoreErrorCode", expectedCode, storeException.getErrorCode());
        }
      }
    }
  }

  /**
   * Verifies that the deletes are successful. Also ensures that a GET for the {@link MockId} fails with
   * {@link StoreErrorCodes#ID_Deleted}.
   * @param deleters list of {@link Deleter} instances.
   * @param futures list of {@link Future}s returned from submitting the {@code deleters} to an {@link ExecutorService}
   * @throws Exception
   */
  private void verifyDeleteFutures(List<Deleter> deleters, List<Future<CallableResult>> futures) throws Exception {
    for (int i = 0; i < deleters.size(); i++) {
      MockId id = deleters.get(i).id;
      Future<CallableResult> future = futures.get(i);
      future.get(1, TimeUnit.SECONDS);
      verifyGetFailure(id, StoreErrorCodes.ID_Deleted);
    }
  }

  // putErrorCasesTest() helpers

  /**
   * Verifies that PUT fails.
   * @param idToPut the {@link MockId} to PUT.
   * @param expectedErrorCode the expected {@link StoreErrorCodes} for the failure.
   */
  private void verifyPutFailure(MockId idToPut, StoreErrorCodes expectedErrorCode) {
    MessageInfo info = new MessageInfo(idToPut, PUT_RECORD_SIZE, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), Utils.Infinite_Time);
    MessageWriteSet writeSet =
        new MockMessageWriteSet(Collections.singletonList(info), Collections.singletonList(ByteBuffer.allocate(1)));
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
  private void verifyDeleteFailure(MockId idToDelete, StoreErrorCodes expectedErrorCode) {
    MessageInfo info =
        new MessageInfo(idToDelete, DELETE_RECORD_SIZE, idToDelete.getAccountId(), idToDelete.getContainerId(),
            System.currentTimeMillis());
    MessageWriteSet writeSet =
        new MockMessageWriteSet(Collections.singletonList(info), Collections.singletonList(ByteBuffer.allocate(1)));
    try {
      store.delete(writeSet);
      fail("Store DELETE should have failed");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
    }
  }

  // shutdownTest() helpers

  /**
   * Verifies that operations on {@link BlobStore} fail because it is inactive.
   * @param blobStore the inactive {@link BlobStore} on which the operations are performed.
   */
  private void verifyOperationFailuresOnInactiveStore(BlobStore blobStore) {
    try {
      blobStore.get(Collections.EMPTY_LIST, EnumSet.noneOf(StoreGetOptions.class));
      fail("Operation should have failed because store is inactive");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }

    try {
      blobStore.put(new MockMessageWriteSet(Collections.EMPTY_LIST, Collections.EMPTY_LIST));
      fail("Operation should have failed because store is inactive");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }

    try {
      blobStore.delete(new MockMessageWriteSet(Collections.EMPTY_LIST, Collections.EMPTY_LIST));
      fail("Operation should have failed because store is inactive");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }

    try {
      blobStore.findEntriesSince(new StoreFindToken(), Long.MAX_VALUE);
      fail("Operation should have failed because store is inactive");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }

    try {
      blobStore.findMissingKeys(Collections.EMPTY_LIST);
      fail("Operation should have failed because store is inactive");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }

    try {
      blobStore.isKeyDeleted(getUniqueId());
      fail("Operation should have failed because store is inactive");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }

    try {
      blobStore.shutdown();
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }
  }

  /**
   * Attempt to write a set of messages with keys and crcs from the given lists of keys and crcs.
   * @param mockIdList the list of keys of the messages.
   * @param crcList the list of crcs of the messages.
   * @throws StoreException
   */
  private void putWithKeysAndCrcs(List<StoreKey> mockIdList, List<Long> crcList) throws StoreException {
    List<ByteBuffer> bufferList = new ArrayList<>();
    List<MessageInfo> messageInfoList = new ArrayList<>();
    for (int i = 0; i < mockIdList.size(); i++) {
      bufferList.add(ByteBuffer.allocate(PUT_RECORD_SIZE));
      MockId mockId = (MockId) mockIdList.get(i);
      messageInfoList.add(new MessageInfo(mockId, PUT_RECORD_SIZE, false, Utils.Infinite_Time, crcList.get(i),
          Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), Utils.Infinite_Time));
    }
    MessageWriteSet writeSet = new MockMessageWriteSet(messageInfoList, bufferList);
    // Put the initial two messages.
    store.put(writeSet);
  }

  // diskSpaceRequirementsTest() helpers

  /**
   * Run the {@link BlobStore#getDiskSpaceRequirements()} test.
   * @param segmentsAllocated the number of segments currently used by the blob store.
   * @param numSwapSegments the number of swap segments currently used by the blob store.
   * @throws Exception
   */
  private void doDiskSpaceRequirementsTest(int segmentsAllocated, int numSwapSegments) throws Exception {
    DiskSpaceRequirements requirements = store.getDiskSpaceRequirements();
    if (!isLogSegmented) {
      assertNull("Expected null DiskSpaceRequirements for non segmented log", requirements);
    } else {
      assertEquals(SEGMENT_CAPACITY, requirements.getSegmentSizeInBytes());
      assertEquals((LOG_CAPACITY / SEGMENT_CAPACITY) - segmentsAllocated, requirements.getSegmentsNeeded());
      assertEquals(numSwapSegments, requirements.getSwapSegmentsInUse());
    }
  }

  private BlobStore createBlobStore(ReplicaId replicaId) {
    StoreConfig config = new StoreConfig(new VerifiableProperties(properties));
    return createBlobStore(replicaId, config, null);
  }

  private BlobStore createBlobStore(ReplicaId replicaId, StoreConfig config, WriteStatusDelegate writeStatusDelegate) {
    MetricRegistry registry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(registry);
    return new BlobStore(replicaId, config, scheduler, storeStatsScheduler, diskIOScheduler, diskSpaceAllocator,
        metrics, metrics, STORE_KEY_FACTORY, recovery, hardDelete, writeStatusDelegate, time);
  }

  private StoreTestUtils.MockReplicaId getMockReplicaId(String filePath) {
    return StoreTestUtils.createMockReplicaId(storeId, LOG_CAPACITY, filePath);
  }

  /**
   * Change threshold levels for dynamic replica sealing
   * @param readOnlyThreshold new storeReadOnlyEnableSizeThresholdPercentageName value
   * @param readWriteDeltaThreshold new storeReadWriteEnableSizeThresholdPercentageDeltaName value
   * @return StoreConfig object with new threshold values
   */
  private StoreConfig changeThreshold(int readOnlyThreshold, int readWriteDeltaThreshold, boolean delegateEnabled) {
    properties.setProperty(StoreConfig.storeWriteStatusDelegateEnableName, Boolean.toString(delegateEnabled));
    properties.setProperty(StoreConfig.storeReadOnlyEnableSizeThresholdPercentageName,
        Integer.toString(readOnlyThreshold));
    properties.setProperty(StoreConfig.storeReadWriteEnableSizeThresholdPercentageDeltaName,
        Integer.toString(readWriteDeltaThreshold));
    return new StoreConfig(new VerifiableProperties(properties));
  }

  private void restartStore(StoreConfig config, ReplicaId replicaId, WriteStatusDelegate delegate)
      throws StoreException {
    store.shutdown();
    store = createBlobStore(replicaId, config, delegate);
    store.start();
  }
}
