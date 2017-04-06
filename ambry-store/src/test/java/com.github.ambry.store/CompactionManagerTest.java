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
    blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES);
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
    blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES);
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
    blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES);
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
  public void testGetCompactionDetailsBasicTest() throws StoreException, InterruptedException {
    // null validLogSegments
    blobStore.validLogSegments = null;
    verifyCompactionDetails(null);

    // 1 valid LogSegment
    blobStore.validLogSegments = generateRandomStrings(1);
    verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.validLogSegments));

    // random no of valid logSegments
    for (int i = 0; i < 3; i++) {
      int logSegmentCount = TestUtils.RANDOM.nextInt(10) + 1;
      blobStore.validLogSegments = generateRandomStrings(logSegmentCount);
      verifyCompactionDetails(
          new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.validLogSegments));
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for {@link BlobStore}'s
   * used capacity
   * @throws StoreException
   */
  @Test
  public void testDifferentUsedCapacities() throws StoreException {
    blobStore.validLogSegments = generateRandomStrings(2);
    // if used capacity is <= 60%, compaction details will be null. If not, validLogSegments needs to be returned.
    Long[] usedCapacities = new Long[]{
        CAPACITY_IN_BYTES * 2 / 10, (CAPACITY_IN_BYTES * 4 / 10),
        CAPACITY_IN_BYTES * 5 / 10,
        CAPACITY_IN_BYTES * 51 / 100, (CAPACITY_IN_BYTES * 6 / 10),
        CAPACITY_IN_BYTES * 7 / 10, CAPACITY_IN_BYTES * 9 / 10};
    for (Long usedCapacity : usedCapacities) {
      blobStore.usedCapacity = usedCapacity;
      if (blobStore.usedCapacity < (config.storeMinUsedCapacityToTriggerCompactionInPercentage / 100.0
          * blobStore.capacityInBytes)) {
        verifyCompactionDetails(null);
      } else {
        verifyCompactionDetails(
            new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.validLogSegments));
      }
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for
   * {@link StoreConfig#storeMinUsedCapacityToTriggerCompactionInPercentage}
   */
  @Test
  public void testDifferentThresholdsForMinLogSizeToCompact() throws StoreException {
    int[] minLogSizeToTriggerCompactionInPercentages = new int[]{10, 20, 35, 40, 50, 59, 60, 61, 65, 70, 80, 95};
    // when used capacity(60%) is <= (minLogSize) % of total capacity, compactionDetails is expected to be null.
    // If not, validLogSegments needs to be returned
    for (int minLogSize : minLogSizeToTriggerCompactionInPercentages) {
      properties.setProperty("store.min.log.size.to.trigger.compaction.in.percent", String.valueOf(minLogSize));
      config = new StoreConfig(new VerifiableProperties(properties));
      MetricRegistry metricRegistry = new MetricRegistry();
      StorageManagerMetrics metrics = new StorageManagerMetrics(metricRegistry);
      blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES);
      compactionManager =
          new CompactionManager(MOUNT_PATH, config, Collections.singleton((BlobStore) blobStore), metrics, time);
      blobStore.validLogSegments = generateRandomStrings(2);
      if (blobStore.usedCapacity < (config.storeMinUsedCapacityToTriggerCompactionInPercentage / 100.0
          * blobStore.capacityInBytes)) {
        verifyCompactionDetails(null);
      } else {
        verifyCompactionDetails(
            new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.validLogSegments));
      }
    }
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)} for different values for
   * {@link StoreConfig#storeDeletedMessageRetentionDays}
   */
  @Test
  public void testDifferentMessageRetentionDays() throws StoreException, InterruptedException {
    int[] messageRetentionDayValues = new int[]{1, 2, 3, 6, 9};
    time.sleep(10 * messageRetentionTimeInMs);
    for (int messageRetentionDays : messageRetentionDayValues) {
      properties.setProperty("store.deleted.message.retention.days", String.valueOf(messageRetentionDays));
      config = new StoreConfig(new VerifiableProperties(properties));
      MetricRegistry metricRegistry = new MetricRegistry();
      StorageManagerMetrics metrics = new StorageManagerMetrics(metricRegistry);
      blobStore = new MockBlobStore(config, metrics, time, CAPACITY_IN_BYTES, DEFAULT_USED_CAPACITY_IN_BYTES);
      compactionManager =
          new CompactionManager(MOUNT_PATH, config, Collections.singleton((BlobStore) blobStore), metrics, time);
      blobStore.validLogSegments = generateRandomStrings(2);
      verifyCompactionDetails(
          new CompactionDetails(time.milliseconds() - messageRetentionDays * Time.SecsPerDay * Time.MsPerSec,
              blobStore.validLogSegments));
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
          compactCallsCountdown);
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
          compactCallsCountdown);
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

    private boolean resumeCompactionCalled = false;
    boolean compactCalled = false;
    Exception callOrderException = null;

    RuntimeException exceptionToThrowOnResume = null;
    RuntimeException exceptionToThrowOnCompact = null;
    boolean started = true;

    MockBlobStore(StoreConfig config, StorageManagerMetrics metrics, Time time, long capacityInBytes,
        long usedCapacity) {
      this(config, metrics, time, capacityInBytes, usedCapacity, new CountDownLatch(0));
    }

    MockBlobStore(StoreConfig config, StorageManagerMetrics metrics, Time time, long capacityInBytes, long usedCapacity,
        CountDownLatch compactCallsCountdown) {
      super("", config, null, null, metrics, null, 0, null, null, null, time);
      this.capacityInBytes = capacityInBytes;
      this.usedCapacity = usedCapacity;
      this.compactCallsCountdown = compactCallsCountdown;
    }

    @Override
    public long getSizeInBytes() {
      return usedCapacity;
    }

    /**
     * Return total capacity of the {@link BlobStore} in bytes
     * @return the total capacity of the {@link BlobStore} in bytes
     */
    @Override
    long getCapacityInBytes() {
      return capacityInBytes;
    }

    /**
     * Fetches a list of valid {@link LogSegment} names that are considered valid for the purpose of compaction
     * @return list of valid {@link LogSegment} names that are considered valid for the purpose of compaction
     */
    @Override
    List<String> getLogSegmentsNotInJournal() throws StoreException {
      return validLogSegments;
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
