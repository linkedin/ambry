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
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.UtilsTest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests {@link CompactionManager}.
 */
public class CompactionManagerTest {

  private static final long CAPACITY_IN_BYTES = 10 * 1024 * 1024;
  private static final long DEFAULT_USED_CAPACITY_IN_BYTES = CAPACITY_IN_BYTES * 6 / 10;
  private static final String MOUNT_PATH = "/tmp/";
  // the properties that will used to generate a StoreConfig. Clear before use if required.
  private final Properties properties = new Properties();
  private final Time time = new MockTime();
  private StoreConfig config;
  private MockBlobStore blobStore;
  private CompactionManager compactionManager;
  private long messageRetentionTimeInMs;

  /**
   * Instantiates {@link CompactionManagerTest} with the required cast
   * @throws InterruptedException
   */
  public CompactionManagerTest() throws InterruptedException {
    config = new StoreConfig(new VerifiableProperties(properties));
    messageRetentionTimeInMs = config.storeDeletedMessageRetentionDays * Time.SecsPerDay * Time.MsPerSec;
    time.sleep(2 * messageRetentionTimeInMs);
    MetricRegistry metricRegistry = new MetricRegistry();
    StorageManagerMetrics metrics = new StorageManagerMetrics(metricRegistry);
    blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES, null);
    compactionManager =
        new CompactionManager(MOUNT_PATH, config, Collections.singleton((BlobStore) blobStore), metrics, time);
  }

  /**
   * Tests the enabling and disabling of the {@link CompactionManager} with and without compaction enabled.
   */
  @Test
  public void testEnableDisable() {
    // without compaction enabled.
    compactionManager.enable();
    compactionManager.disable();
    compactionManager.awaitTermination();

    // with compaction enabled.
    properties.setProperty("store.enable.compaction", Boolean.toString(true));
    config = new StoreConfig(new VerifiableProperties(properties));
    StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
    blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES, null);
    compactionManager =
        new CompactionManager(MOUNT_PATH, config, Collections.singleton((BlobStore) blobStore), metrics, time);
    compactionManager.enable();
    assertNotNull("Compaction thread should be created",
        TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    compactionManager.disable();
    compactionManager.awaitTermination();
    assertFalse("Compaction thread should not be running", compactionManager.isCompactionExecutorRunning());
  }

  /**
   * Tests {@link CompactionManager#disable()} without having called {@link CompactionManager#enable()} first.
   */
  @Test
  public void testDisableWithoutEnable() {
    // without compaction enabled.
    compactionManager.disable();
    compactionManager.awaitTermination();

    // with compaction enabled.
    properties.setProperty("store.enable.compaction", Boolean.toString(true));
    config = new StoreConfig(new VerifiableProperties(properties));
    StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
    blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES, null);
    compactionManager =
        new CompactionManager(MOUNT_PATH, config, Collections.singleton((BlobStore) blobStore), metrics, time);
    compactionManager.disable();
    compactionManager.awaitTermination();
  }

  /**
   * Basic tests for {@link CompactionManager#getCompactionDetails(BlobStore)} for different values of
   * log segment count
   * @throws StoreException
   * @throws InterruptedException
   */
  @Test
  public void testCompactionManager() throws StoreException {
    // null case
    CompactionDetails actual = compactionManager.getCompactionDetails(blobStore);
    assertNull("CompactionDetails expected to be null ", actual);

    // random CompactionDetails
    blobStore.details = generateRandomCompactionDetails(5);
    assertEquals("CompactionDetails mismatch", blobStore.details, compactionManager.getCompactionDetails(blobStore));
  }

  public void testCompactionManagerFailureCases() throws StoreException {
    blobStore.details = generateRandomCompactionDetails(5);
    // StoreException thrown
    blobStore.exceptionToThrow = new StoreException("Random StoreException to test", StoreErrorCodes.Unknown_Error);
    try {
      compactionManager.getCompactionDetails(blobStore);
    } catch (StoreException e) {
      assertEquals("StoreErrorCode mismatch ", StoreErrorCodes.Unknown_Error, e.getErrorCode());
    }
  }

  /**
   * Tests that compaction is triggered on all stores provided they do not misbehave. Also includes a store that is
   * not ready for compaction. Ensures that {@link BlobStore#maybeResumeCompaction()} is called before
   * {@link BlobStore#compact(CompactionDetails)} is called.
   * @throws Exception
   */
  @Test
  public void testCompactionExecutorHappyPath() throws Exception {
    int numStores = 3;
    List<BlobStore> stores = new ArrayList<>();
    // one store with nothing to compact isn't going to get compact calls.
    CountDownLatch compactCallsCountdown = new CountDownLatch(numStores - 1);
    properties.setProperty("store.enable.compaction", Boolean.toString(true));
    config = new StoreConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    StorageManagerMetrics metrics = new StorageManagerMetrics(metricRegistry);
    for (int i = 0; i < numStores; i++) {
      MockBlobStore store = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES,
          compactCallsCountdown, null);
      // one store should not have any segments to compact
      store.validLogSegments = i == 0 ? null : generateRandomStrings(i);
      stores.add(store);
    }
    compactionManager = new CompactionManager(MOUNT_PATH, config, stores, metrics, time);
    compactionManager.enable();
    assertNotNull("Compaction thread should be created",
        TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    assertTrue("Compaction calls did not come within the expected time",
        compactCallsCountdown.await(1, TimeUnit.SECONDS));
    for (int i = 0; i < numStores; i++) {
      MockBlobStore store = (MockBlobStore) stores.get(i);
      if (store.callOrderException != null) {
        throw store.callOrderException;
      }
      if (i > 0) {
        assertTrue("Compact was not called", store.compactCalled);
      } else {
        // should not call for i == 0 because there are no compaction details.
        assertFalse("Compact should not have been called", store.compactCalled);
      }
    }
    compactionManager.disable();
    compactionManager.awaitTermination();
    assertFalse("Compaction thread should not be running", compactionManager.isCompactionExecutorRunning());
  }

  /**
   * Tests that compaction proceeds on all non misbehaving stores even in the presence of some misbehaving stores
   * (not started/throwing exceptions).
   * @throws Exception
   */
  @Test
  public void testCompactionWithMisbehavingStores() throws Exception {
    int numStores = 5;
    List<BlobStore> stores = new ArrayList<>();
    // one store that isn't started isn't going to get compact calls.
    // another store that throws an exception on resumeCompaction() isn't going to get a compact call.
    CountDownLatch compactCallsCountdown = new CountDownLatch(numStores - 2);
    properties.setProperty("store.enable.compaction", Boolean.toString(true));
    config = new StoreConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    StorageManagerMetrics metrics = new StorageManagerMetrics(metricRegistry);
    for (int i = 0; i < numStores; i++) {
      MockBlobStore store = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES,
          compactCallsCountdown, null);
      store.validLogSegments = generateRandomStrings(2);
      if (i == 0) {
        // one store should not be started
        store.started = false;
      } else if (i == 1) {
        // one store should throw on resumeCompaction()
        store.exceptionToThrowOnResume = new RuntimeException("Misbehaving store");
      } else if (i == 2) {
        // one store should throw on compact()
        store.exceptionToThrowOnCompact = new RuntimeException("Misbehaving store");
      }
      stores.add(store);
    }
    compactionManager = new CompactionManager(MOUNT_PATH, config, stores, metrics, time);
    compactionManager.enable();
    assertNotNull("Compaction thread should be created",
        TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    assertTrue("Compaction calls did not come within the expected time",
        compactCallsCountdown.await(1, TimeUnit.SECONDS));
    for (int i = 0; i < numStores; i++) {
      MockBlobStore store = (MockBlobStore) stores.get(i);
      if (store.callOrderException != null) {
        throw store.callOrderException;
      }
      if (i > 1) {
        assertTrue("Compact was not called", store.compactCalled);
      } else {
        // should not call for i == 0 because store has not been started.
        // should not call for i == 1 because resumeCompaction() would have marked this as a misbehaving store.
        assertFalse("Compact should not have been called", store.compactCalled);
      }
    }
    compactionManager.disable();
    compactionManager.awaitTermination();
    assertFalse("Compaction thread should not be running", compactionManager.isCompactionExecutorRunning());
  }

  // helper methods

  /**
   * Generates random strings
   * @param count the total number of random strings that needs to be generated
   * @return a {@link List} of random strings of size {@code count}
   */
  private List<String> generateRandomStrings(int count) {
    List<String> randomStrings = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      randomStrings.add(UtilsTest.getRandomString(5));
    }
    return randomStrings;
  }

  /**
   * Generates random {@link CompactionDetails} with random log segment names
   * @param count number of log segments to be compacted as part of {@link CompactionDetails}
   * @return the randomly generated {@link CompactionDetails}
   */
  private CompactionDetails generateRandomCompactionDetails(int count) {
    List<String> logSegmentsNames = CompactionPolicyTest.generateRandomStrings(count);
    return new CompactionDetails(time.milliseconds(), logSegmentsNames);
  }


  // verification helper methods

  /**
   * Verifies {@link CompactionManager#getCompactionDetails(BlobStore)} returns expected values i.e.
   * {@code expectedCompactionDetails}
   * @param expectedCompactionDetails expected {@link CompactionDetails}
   * @throws StoreException
   */
  private void verifyCompactionDetails(CompactionDetails expectedCompactionDetails) throws StoreException {
    CompactionDetails compactionDetails = compactionManager.getCompactionDetails(blobStore);
    if (expectedCompactionDetails == null) {
      assertNull("CompactionDetails expected to be null ", compactionDetails);
    } else {
      assertEquals("Returned invalid time ", expectedCompactionDetails.getReferenceTimeMs(),
          compactionDetails.getReferenceTimeMs());
      assertEquals("Compaction range mismatch ", expectedCompactionDetails.getLogSegmentsUnderCompaction(),
          compactionDetails.getLogSegmentsUnderCompaction());
    }
  }

  /**
   * MockBlobStore to assist in testing {@link CompactionManager}
   */
  private class MockBlobStore extends BlobStore {
    private long usedCapacity;
    private long capacityInBytes;
    private final CountDownLatch compactCallsCountdown;
    List<String> validLogSegments = null;
    StoreException exceptionToThrow;
    CompactionDetails details;

    private boolean resumeCompactionCalled = false;
    boolean compactCalled = false;
    Exception callOrderException = null;

    RuntimeException exceptionToThrowOnResume = null;
    RuntimeException exceptionToThrowOnCompact = null;
    boolean started = true;

    MockBlobStore(StoreConfig config, StorageManagerMetrics metrics, Time time, long capacityInBytes,
        long usedCapacity, CompactionDetails details) {
      this(config, metrics, time, capacityInBytes, usedCapacity, new CountDownLatch(0), details);
    }

    MockBlobStore(StoreConfig config, StorageManagerMetrics metrics, Time time, long capacityInBytes, long usedCapacity,
        CountDownLatch compactCallsCountdown, CompactionDetails details) {
      super("", config, null, null, metrics, null, 0, null, null, null, time);
      this.capacityInBytes = capacityInBytes;
      this.usedCapacity = usedCapacity;
      this.compactCallsCountdown = compactCallsCountdown;
      this.details = details;
    }

    @Override
    public long getSizeInBytes() {
      return usedCapacity;
    }

    @Override
    CompactionDetails getCompactionDetails(CompactionPolicy compactionPolicy) throws StoreException {
      return details;
    }

    @Override
    void compact(CompactionDetails details) {
      compactCalled = true;
      compactCallsCountdown.countDown();
      if (details == null) {
        callOrderException = new Exception("Called compact() with null details");
      } else if (!resumeCompactionCalled) {
        callOrderException = new Exception("Called compact() before calling maybeResumeCompaction()");
      }
      if (exceptionToThrowOnCompact != null) {
        throw exceptionToThrowOnCompact;
      }
    }

    @Override
    void maybeResumeCompaction() {
      if (resumeCompactionCalled) {
        callOrderException = new Exception("maybeResumeCompaction() called more than once");
      }
      resumeCompactionCalled = true;
      if (exceptionToThrowOnResume != null) {
        throw exceptionToThrowOnResume;
      }
    }

    @Override
    public boolean isStarted() {
      return started;
    }
  }
}
