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
import com.github.ambry.utils.ByteBufferOutputStream;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests for {@link BlobStore}. Tests both segmented and non segmented log use cases.
 */
@RunWith(Parameterized.class)
public class BlobStoreTest {
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
      return new CallableResult(
          blobStoreTestUtils.put(1, blobStoreTestUtils.PUT_RECORD_SIZE, Utils.Infinite_Time).get(0), null);
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
      return new CallableResult(null, blobStoreTestUtils.store.get(Collections.singletonList(id), storeGetOptions));
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
      blobStoreTestUtils.delete(id);
      return EMPTY_RESULT;
    }
  }

  private final BlobStoreTestUtils blobStoreTestUtils;

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
    blobStoreTestUtils = new BlobStoreTestUtils(isLogSegmented);
  }

  /**
   * Releases all resources and deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   * @throws StoreException
   */
  @After
  public void cleanup() throws InterruptedException, IOException, StoreException {
    blobStoreTestUtils.cleanup();
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
    StoreConfig config = new StoreConfig(new VerifiableProperties(blobStoreTestUtils.properties));

    // attempt to start when store is already started fails
    verifyStartupFailure(blobStoreTestUtils.store, StoreErrorCodes.Store_Already_Started);

    String nonExistentDir = new File(blobStoreTestUtils.tempDir, UtilsTest.getRandomString(10)).getAbsolutePath();

    // fail if attempt to create directory fails
    String badPath = new File(nonExistentDir, UtilsTest.getRandomString(10)).getAbsolutePath();
    MetricRegistry registry = new MetricRegistry();
    StorageManagerMetrics metrics = new StorageManagerMetrics(registry);
    BlobStore blobStore = new BlobStore(blobStoreTestUtils.storeId, config, blobStoreTestUtils.scheduler,
        blobStoreTestUtils.diskIOScheduler, metrics, badPath, blobStoreTestUtils.LOG_CAPACITY,
        blobStoreTestUtils.STORE_KEY_FACTORY, blobStoreTestUtils.recovery, blobStoreTestUtils.hardDelete,
        blobStoreTestUtils.time);
    verifyStartupFailure(blobStore, StoreErrorCodes.Initialization_Error);

    // create directory if it does not exist
    registry = new MetricRegistry();
    metrics = new StorageManagerMetrics(registry);
    blobStore = new BlobStore(blobStoreTestUtils.storeId, config, blobStoreTestUtils.scheduler,
        blobStoreTestUtils.diskIOScheduler, metrics, nonExistentDir, blobStoreTestUtils.LOG_CAPACITY,
        blobStoreTestUtils.STORE_KEY_FACTORY, blobStoreTestUtils.recovery, blobStoreTestUtils.hardDelete,
        blobStoreTestUtils.time);
    verifyStartupSuccess(blobStore);
    File createdDir = new File(nonExistentDir);
    assertTrue("Directory should now exist", createdDir.exists() && createdDir.isDirectory());

    // should not be able to start two stores at the same path
    registry = new MetricRegistry();
    metrics = new StorageManagerMetrics(registry);
    blobStore = new BlobStore(blobStoreTestUtils.storeId, config, blobStoreTestUtils.scheduler,
        blobStoreTestUtils.diskIOScheduler, metrics, nonExistentDir, blobStoreTestUtils.LOG_CAPACITY,
        blobStoreTestUtils.STORE_KEY_FACTORY, blobStoreTestUtils.recovery, blobStoreTestUtils.hardDelete,
        blobStoreTestUtils.time);
    blobStore.start();
    registry = new MetricRegistry();
    metrics = new StorageManagerMetrics(registry);
    BlobStore secondStore = new BlobStore(blobStoreTestUtils.storeId, config, blobStoreTestUtils.scheduler,
        blobStoreTestUtils.diskIOScheduler, metrics, nonExistentDir, blobStoreTestUtils.LOG_CAPACITY,
        blobStoreTestUtils.STORE_KEY_FACTORY, blobStoreTestUtils.recovery, blobStoreTestUtils.hardDelete,
        blobStoreTestUtils.time);
    verifyStartupFailure(secondStore, StoreErrorCodes.Initialization_Error);
    blobStore.shutdown();

    // fail if directory is not readable
    assertTrue("Could not set readable state to false", createdDir.setReadable(false));
    verifyStartupFailure(blobStore, StoreErrorCodes.Initialization_Error);

    assertTrue("Could not set readable state to true", createdDir.setReadable(true));
    assertTrue("Directory could not be deleted", StoreTestUtils.cleanDirectory(createdDir, true));

    // fail if provided path is not a directory
    File file = new File(blobStoreTestUtils.tempDir, UtilsTest.getRandomString(10));
    assertTrue("Test file could not be created", file.createNewFile());
    file.deleteOnExit();
    registry = new MetricRegistry();
    metrics = new StorageManagerMetrics(registry);
    blobStore = new BlobStore(blobStoreTestUtils.storeId, config, blobStoreTestUtils.scheduler,
        blobStoreTestUtils.diskIOScheduler, metrics, file.getAbsolutePath(), blobStoreTestUtils.LOG_CAPACITY,
        blobStoreTestUtils.STORE_KEY_FACTORY, blobStoreTestUtils.recovery, blobStoreTestUtils.hardDelete,
        blobStoreTestUtils.time);
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
    // PUT a key that is slated to expire when time advances by 2ms
    MockId addedId =
        blobStoreTestUtils.put(1, blobStoreTestUtils.PUT_RECORD_SIZE, blobStoreTestUtils.time.milliseconds() + 1)
            .get(0);
    blobStoreTestUtils.time.sleep(2);
    blobStoreTestUtils.liveKeys.remove(addedId);
    blobStoreTestUtils.expiredKeys.add(addedId);

    // GET of all the keys implicitly tests the PUT and DELETE.
    // live keys
    StoreInfo storeInfo = blobStoreTestUtils.store.get(new ArrayList<>(blobStoreTestUtils.liveKeys),
        EnumSet.noneOf(StoreGetOptions.class));
    checkStoreInfo(storeInfo, blobStoreTestUtils.liveKeys);

    BlobStoreTestUtils.MockMessageStoreHardDelete hd =
        (BlobStoreTestUtils.MockMessageStoreHardDelete) blobStoreTestUtils.hardDelete;
    for (MockId id : blobStoreTestUtils.deletedKeys) {
      hd.setMessageInfo(blobStoreTestUtils.allKeys.get(id).msgInfo);

      // cannot get without StoreGetOptions
      verifyGetFailure(id, StoreErrorCodes.ID_Deleted);

      // with StoreGetOptions.Store_Include_Deleted
      storeInfo = blobStoreTestUtils.store.get(Collections.singletonList(id),
          EnumSet.of(StoreGetOptions.Store_Include_Deleted));
      checkStoreInfo(storeInfo, Collections.singleton(id));

      // with all StoreGetOptions
      storeInfo = blobStoreTestUtils.store.get(Collections.singletonList(id), EnumSet.allOf(StoreGetOptions.class));
      checkStoreInfo(storeInfo, Collections.singleton(id));
    }

    for (MockId id : blobStoreTestUtils.expiredKeys) {
      // cannot get without StoreGetOptions
      verifyGetFailure(id, StoreErrorCodes.TTL_Expired);

      // with StoreGetOptions.Store_Include_Expired
      storeInfo = blobStoreTestUtils.store.get(Collections.singletonList(id),
          EnumSet.of(StoreGetOptions.Store_Include_Expired));
      checkStoreInfo(storeInfo, Collections.singleton(id));

      // with all StoreGetOptions
      storeInfo = blobStoreTestUtils.store.get(Collections.singletonList(id), EnumSet.allOf(StoreGetOptions.class));
      checkStoreInfo(storeInfo, Collections.singleton(id));
    }

    // should be able to delete expired blobs
    blobStoreTestUtils.delete(addedId);

    // non existent ID has to fail
    verifyGetFailure(blobStoreTestUtils.getUniqueId(), StoreErrorCodes.ID_Not_Found);
  }

  /**
   * Tests the case where there are many concurrent PUTs.
   * @throws Exception
   */
  @Test
  public void concurrentPutTest() throws Exception {
    long blobCount = 4000 / blobStoreTestUtils.PUT_RECORD_SIZE + 1;
    List<Putter> putters = new ArrayList<>((int) blobCount);
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
    long extraBlobCount = 4000 / blobStoreTestUtils.PUT_RECORD_SIZE + 1;
    blobStoreTestUtils.put((int) extraBlobCount, blobStoreTestUtils.PUT_RECORD_SIZE, Utils.Infinite_Time);
    List<Getter> getters = new ArrayList<>(blobStoreTestUtils.allKeys.size());
    for (MockId id : blobStoreTestUtils.allKeys.keySet()) {
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
    long extraBlobCount = 2000 / blobStoreTestUtils.PUT_RECORD_SIZE + 1;
    blobStoreTestUtils.put((int) extraBlobCount, blobStoreTestUtils.PUT_RECORD_SIZE, Utils.Infinite_Time);
    List<Deleter> deleters = new ArrayList<>(blobStoreTestUtils.liveKeys.size());
    for (MockId id : blobStoreTestUtils.liveKeys) {
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
    long putBlobCount = 1500 / blobStoreTestUtils.PUT_RECORD_SIZE + 1;
    List<Putter> putters = new ArrayList<>((int) putBlobCount);
    for (int i = 0; i < putBlobCount; i++) {
      putters.add(new Putter());
    }

    List<Getter> getters = new ArrayList<>(blobStoreTestUtils.liveKeys.size());
    for (MockId id : blobStoreTestUtils.liveKeys) {
      getters.add(new Getter(id, EnumSet.allOf(StoreGetOptions.class)));
    }

    long deleteBlobCount = 1500 / blobStoreTestUtils.PUT_RECORD_SIZE;
    List<MockId> idsToDelete =
        blobStoreTestUtils.put((int) deleteBlobCount, blobStoreTestUtils.PUT_RECORD_SIZE, Utils.Infinite_Time);
    List<Deleter> deleters = new ArrayList<>((int) deleteBlobCount);
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
    blobStoreTestUtils.verifyPutFailure(blobStoreTestUtils.liveKeys.iterator().next(), StoreErrorCodes.Already_Exist);
    // expired
    blobStoreTestUtils.verifyPutFailure(blobStoreTestUtils.expiredKeys.iterator().next(),
        StoreErrorCodes.Already_Exist);
    // deleted
    blobStoreTestUtils.verifyPutFailure(blobStoreTestUtils.deletedKeys.iterator().next(),
        StoreErrorCodes.Already_Exist);
  }

  /**
   * Tests error cases for {@link BlobStore#delete(MessageWriteSet)}.
   */
  @Test
  public void deleteErrorCasesTest() {
    // ID that is already deleted
    blobStoreTestUtils.verifyDeleteFailure(blobStoreTestUtils.deletedKeys.iterator().next(),
        StoreErrorCodes.ID_Deleted);
    // ID that does not exist
    blobStoreTestUtils.verifyDeleteFailure(blobStoreTestUtils.getUniqueId(), StoreErrorCodes.ID_Not_Found);
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
    FindInfo findInfo = blobStoreTestUtils.store.findEntriesSince(new StoreFindToken(), Long.MAX_VALUE);
    Set<StoreKey> keysPresent = new HashSet<>();
    for (MessageInfo info : findInfo.getMessageEntries()) {
      keysPresent.add(info.getStoreKey());
    }
    assertEquals("All keys were not present in the return from findEntriesSince()", blobStoreTestUtils.allKeys.keySet(),
        keysPresent);
  }

  /**
   * Tests {@link BlobStore#findMissingKeys(List)}.
   * @throws StoreException
   */
  @Test
  public void findMissingKeysTest() throws StoreException {
    List<StoreKey> idsToProvide = new ArrayList<StoreKey>(blobStoreTestUtils.allKeys.keySet());
    Set<StoreKey> nonExistentIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      nonExistentIds.add(blobStoreTestUtils.getUniqueId());
    }
    idsToProvide.addAll(nonExistentIds);
    Collections.shuffle(idsToProvide);
    Set<StoreKey> missingKeys = blobStoreTestUtils.store.findMissingKeys(idsToProvide);
    assertEquals("Set of missing keys not as expected", nonExistentIds, missingKeys);
  }

  /**
   * Tests {@link BlobStore#isKeyDeleted(StoreKey)} including error cases.
   * @throws StoreException
   */
  @Test
  public void isKeyDeletedTest() throws StoreException {
    for (MockId id : blobStoreTestUtils.allKeys.keySet()) {
      assertEquals("Returned state is not as expected", blobStoreTestUtils.deletedKeys.contains(id),
          blobStoreTestUtils.store.isKeyDeleted(id));
    }
    // non existent id
    try {
      blobStoreTestUtils.store.isKeyDeleted(blobStoreTestUtils.getUniqueId());
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
    blobStoreTestUtils.store.shutdown();
    // no operations should be possible if store is not up or has been shutdown
    verifyOperationFailuresOnInactiveStore(blobStoreTestUtils.store);
    StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
    StoreConfig config = new StoreConfig(new VerifiableProperties(blobStoreTestUtils.properties));
    blobStoreTestUtils.store = new BlobStore(blobStoreTestUtils.storeId, config, blobStoreTestUtils.scheduler,
        blobStoreTestUtils.diskIOScheduler, metrics, blobStoreTestUtils.tempDirStr, blobStoreTestUtils.LOG_CAPACITY,
        blobStoreTestUtils.STORE_KEY_FACTORY, blobStoreTestUtils.recovery, blobStoreTestUtils.hardDelete,
        blobStoreTestUtils.time);
    verifyOperationFailuresOnInactiveStore(blobStoreTestUtils.store);
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
      MessageInfo expectedInfo = blobStoreTestUtils.allKeys.get(id).msgInfo;
      assertEquals("Unexpected size in MessageInfo", expectedInfo.getSize(), messageInfo.getSize());
      assertEquals("Unexpected expiresAtMs in MessageInfo", expectedInfo.getExpirationTimeInMs(),
          messageInfo.getExpirationTimeInMs());

      assertEquals("Unexpected key in readSet", id, readSet.getKeyAt(i));
      assertEquals("Unexpected size in ReadSet", expectedInfo.getSize(), readSet.sizeInBytes(i));
      ByteBuffer readBuf = ByteBuffer.allocate((int) expectedInfo.getSize());
      ByteBufferOutputStream stream = new ByteBufferOutputStream(readBuf);
      WritableByteChannel channel = Channels.newChannel(stream);
      readSet.writeTo(i, channel, 0, expectedInfo.getSize());
      ByteBuffer expectedData = blobStoreTestUtils.allKeys.get(id).byteBuffer;
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
      blobStoreTestUtils.store.get(Collections.singletonList(id), EnumSet.noneOf(StoreGetOptions.class));
      fail("Should not be able to GET " + id);
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", expectedErrorCode, e.getErrorCode());
    }
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
      StoreInfo storeInfo =
          blobStoreTestUtils.store.get(Collections.singletonList(id), EnumSet.noneOf(StoreGetOptions.class));
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
      if (blobStoreTestUtils.liveKeys.contains(id)) {
        StoreInfo storeInfo = future.get(1, TimeUnit.SECONDS).storeInfo;
        checkStoreInfo(storeInfo, Collections.singleton(getters.get(i).id));
      } else {
        try {
          future.get(1, TimeUnit.SECONDS);
          fail("Operation should have failed");
        } catch (ExecutionException e) {
          StoreException storeException = (StoreException) e.getCause();
          StoreErrorCodes expectedCode = StoreErrorCodes.ID_Not_Found;
          if (blobStoreTestUtils.deletedKeys.contains(id)) {
            expectedCode = StoreErrorCodes.ID_Deleted;
          } else if (blobStoreTestUtils.expiredKeys.contains(id)) {
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
      blobStore.put(new BlobStoreTestUtils.MockMessageWriteSet(Collections.EMPTY_LIST, Collections.EMPTY_LIST));
      fail("Operation should have failed because store is inactive");
    } catch (StoreException e) {
      assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }

    try {
      blobStore.delete(new BlobStoreTestUtils.MockMessageWriteSet(Collections.EMPTY_LIST, Collections.EMPTY_LIST));
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
      blobStore.isKeyDeleted(blobStoreTestUtils.getUniqueId());
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
}
