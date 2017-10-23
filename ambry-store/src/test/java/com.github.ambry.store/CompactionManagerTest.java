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
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests {@link CompactionManager}.
 */
public class CompactionManagerTest {
  private static final String MOUNT_PATH = "/tmp/";
  private static final String ALL_COMPACTION_TRIGGERS = "Periodic,Admin";
  // the properties that will used to generate a StoreConfig. Clear before use if required.
  private final Properties properties = new Properties();
  private final Time time = new MockTime();
  private StoreConfig config;
  private MockBlobStore blobStore;
  private CompactionManager compactionManager;

  /**
   * Instantiates {@link CompactionManagerTest} with the required cast
   * @throws InterruptedException
   */
  public CompactionManagerTest() throws InterruptedException {
    config = new StoreConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    blobStore = new MockBlobStore(config, metrics, time, null);
    compactionManager = new CompactionManager(MOUNT_PATH, config, Collections.singleton(blobStore),
        new StorageManagerMetrics(metricRegistry), time);
  }

  /**
   * Tests the enabling and disabling of the {@link CompactionManager} with and without compaction enabled.
   */
  @Test
  public void testEnableDisable() {
    // without compaction enabled.
    compactionManager.enable();
    // functions should work ok
    assertNull("Compaction thread should not be created",
        TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    assertFalse("Compaction Executor should not be running", compactionManager.isCompactionExecutorRunning());
    assertFalse("Compactions should not be scheduled after termination",
        compactionManager.scheduleNextForCompaction(blobStore));
    compactionManager.disable();
    compactionManager.awaitTermination();

    // with compaction enabled.
    properties.setProperty("store.compaction.triggers", ALL_COMPACTION_TRIGGERS);
    config = new StoreConfig(new VerifiableProperties(properties));
    StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
    compactionManager = new CompactionManager(MOUNT_PATH, config, Collections.singleton(blobStore), metrics, time);
    compactionManager.enable();
    assertNotNull("Compaction thread should be created",
        TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    compactionManager.disable();
    compactionManager.awaitTermination();
    assertFalse("Compaction thread should not be running", compactionManager.isCompactionExecutorRunning());
    assertFalse("Compactions should not be scheduled after termination",
        compactionManager.scheduleNextForCompaction(blobStore));
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
    properties.setProperty("store.compaction.triggers", ALL_COMPACTION_TRIGGERS);
    config = new StoreConfig(new VerifiableProperties(properties));
    StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
    compactionManager = new CompactionManager(MOUNT_PATH, config, Collections.singleton(blobStore), metrics, time);
    compactionManager.disable();
    compactionManager.awaitTermination();
  }

  /**
   * Tests construction failure on bad input.
   */
  @Test
  public void testConstructorBadArgs() {
    properties.setProperty("store.compaction.triggers", "@@BAD_TRIGGER@@");
    config = new StoreConfig(new VerifiableProperties(properties));
    StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
    try {
      new CompactionManager(MOUNT_PATH, config, Collections.singleton(blobStore), metrics, time);
      fail("Construction should have failed because one of the trigger values is invalid");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
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
    int numStores = 5;
    List<BlobStore> stores = new ArrayList<>();
    // one store with nothing to compact isn't going to get compact calls.
    // since we are using mock time, wait for compact calls to arrive twice to ensure the time based scheduling works
    CountDownLatch compactCallsCountdown = new CountDownLatch(2 * (numStores - 1));
    properties.setProperty("store.compaction.triggers", ALL_COMPACTION_TRIGGERS);
    config = new StoreConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    MockBlobStore lastStore = null;
    for (int i = 0; i < numStores; i++) {
      MockBlobStore store = new MockBlobStore(config, metrics, time, compactCallsCountdown, null);
      // one store should not have any segments to compact
      store.details = i == 0 ? null : generateRandomCompactionDetails(i);
      stores.add(store);
      lastStore = store;
    }
    compactionManager =
        new CompactionManager(MOUNT_PATH, config, stores, new StorageManagerMetrics(metricRegistry), time);
    compactionManager.enable();
    assertNotNull("Compaction thread should be created",
        TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    assertTrue("Compaction calls did not come within the expected time",
        compactCallsCountdown.await(1, TimeUnit.SECONDS));
    assertTrue("Compaction Executor should be running", compactionManager.isCompactionExecutorRunning());
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
    // test scheduleNextForCompaction()
    lastStore.compactCallsCountdown = new CountDownLatch(1);
    lastStore.compactCalled = false;
    assertTrue("Should schedule compaction", compactionManager.scheduleNextForCompaction(lastStore));
    assertTrue("Compaction call did not come within the expected time",
        lastStore.compactCallsCountdown.await(1, TimeUnit.HOURS));
    if (lastStore.callOrderException != null) {
      throw lastStore.callOrderException;
    }
    assertTrue("compact() should have been called", lastStore.compactCalled);
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
    properties.setProperty("store.compaction.triggers", ALL_COMPACTION_TRIGGERS);
    properties.setProperty("store.compaction.check.frequency.in.hours", Integer.toString(100));
    config = new StoreConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(metricRegistry);
    for (int i = 0; i < numStores; i++) {
      MockBlobStore store = new MockBlobStore(config, metrics, time, compactCallsCountdown, null);
      store.details = generateRandomCompactionDetails(2);
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
    // using real time here so that compaction is not scheduled more than once for a store during the test unless
    // asked for.
    compactionManager = new CompactionManager(MOUNT_PATH, config, stores, new StorageManagerMetrics(metricRegistry),
        SystemTime.getInstance());
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

    // set all compact called to false
    for (BlobStore store : stores) {
      ((MockBlobStore) store).compactCalled = false;
    }

    // stores that are not started or failed on resumeCompaction() and compact() cannot be scheduled for compaction
    for (int i = 0; i < numStores; i++) {
      MockBlobStore store = (MockBlobStore) stores.get(i);
      if (i < 3) {
        assertFalse("Should not schedule compaction", compactionManager.scheduleNextForCompaction(store));
        assertFalse("compact() should not have been called", store.compactCalled);
      } else {
        store.compactCallsCountdown = new CountDownLatch(1);
        assertFalse("compactCalled should be reset", store.compactCalled);
        assertTrue("Should schedule compaction", compactionManager.scheduleNextForCompaction(store));
        assertTrue("Compaction call did not come within the expected time",
            store.compactCallsCountdown.await(1, TimeUnit.HOURS));
        if (store.callOrderException != null) {
          throw store.callOrderException;
        }
        assertTrue("compact() should have been called", store.compactCalled);
      }
    }
    compactionManager.disable();
    compactionManager.awaitTermination();
    assertFalse("Compaction thread should not be running", compactionManager.isCompactionExecutorRunning());
  }

  /**
   * Tests for cases where only certain triggers are enabled.
   * @throws Exception
   */
  @Test
  public void testDifferentTriggers() throws Exception {
    blobStore.details = generateRandomCompactionDetails(2);
    Runnable adminDisabledChecker =
        () -> assertFalse("Compaction should not be scheduled", compactionManager.scheduleNextForCompaction(blobStore));

    // 1. all enabled is already tested in testCompactionExecutorHappyPath()

    // 2. none enabled
    properties.setProperty("store.compaction.triggers", "");
    AtomicLong startTimeMs = new AtomicLong(time.milliseconds());
    // the fact that the time based trigger has not been enabled can only be tested indirectly by making sure
    // time has not moved forward
    Runnable periodicDisabledChecker =
        () -> assertEquals("Time should not have moved forward", startTimeMs.get(), time.milliseconds());
    doTestTrigger(Arrays.asList(adminDisabledChecker, periodicDisabledChecker));

    // 3. only periodic enabled
    properties.setProperty("store.compaction.triggers", "Periodic");
    Runnable periodicEnabledChecker = () -> {
      try {
        assertTrue("Compaction calls did not come within the expected time",
            blobStore.compactCallsCountdown.await(1, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      if (blobStore.callOrderException != null) {
        throw new IllegalStateException(blobStore.callOrderException);
      }
      assertTrue("compact() should have been called", blobStore.compactCalled);
    };
    doTestTrigger(Arrays.asList(adminDisabledChecker, periodicEnabledChecker));

    // 4. only admin enabled
    properties.setProperty("store.compaction.triggers", "Admin");
    startTimeMs.set(time.milliseconds());
    Runnable adminEnabledChecker = () -> {
      assertTrue("Compaction should be scheduled", compactionManager.scheduleNextForCompaction(blobStore));
      try {
        assertTrue("Compaction calls did not come within the expected time",
            blobStore.compactCallsCountdown.await(1, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      if (blobStore.callOrderException != null) {
        throw new IllegalStateException(blobStore.callOrderException);
      }
      assertTrue("compact() should have been called", blobStore.compactCalled);
    };
    periodicDisabledChecker =
        () -> assertEquals("Time should not have moved forward", startTimeMs.get(), time.milliseconds());
    doTestTrigger(Arrays.asList(adminEnabledChecker, periodicDisabledChecker));
  }

  // helper methods

  // general

  /**
   * Generates random {@link CompactionDetails} with random log segment names
   * @param count number of log segments to be compacted as part of {@link CompactionDetails}
   * @return the randomly generated {@link CompactionDetails}
   */
  private CompactionDetails generateRandomCompactionDetails(int count) {
    List<String> logSegmentsNames = CompactionPolicyTest.generateRandomStrings(count);
    return new CompactionDetails(time.milliseconds(), logSegmentsNames);
  }

  // testDifferentTriggers() helpers

  /**
   * Does the compaction triggers test by running each trigger in {@code triggerRunners} after resetting state.
   * @param triggerRunners the code that triggers compaction.
   * @throws Exception
   */
  private void doTestTrigger(List<Runnable> triggerRunners) throws Exception {
    blobStore.resumeCompactionCalled = false;
    StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
    config = new StoreConfig(new VerifiableProperties(properties));
    compactionManager = new CompactionManager(MOUNT_PATH, config, Collections.singleton(blobStore), metrics, time);
    compactionManager.enable();
    if (config.storeCompactionTriggers[0].isEmpty()) {
      assertNull("Compaction thread should not be created",
          TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    } else {
      assertNotNull("Compaction thread should be created",
          TestUtils.getThreadByThisName(CompactionManager.THREAD_NAME_PREFIX));
    }
    for (Runnable triggerRunner : triggerRunners) {
      blobStore.compactCallsCountdown = new CountDownLatch(1);
      blobStore.compactCalled = false;
      triggerRunner.run();
    }
    compactionManager.disable();
    compactionManager.awaitTermination();
  }

  /**
   * MockBlobStore to assist in testing {@link CompactionManager}
   */
  private class MockBlobStore extends BlobStore {
    CountDownLatch compactCallsCountdown;
    CompactionDetails details;

    boolean resumeCompactionCalled = false;
    boolean compactCalled = false;
    Exception callOrderException = null;

    StoreException exceptionToThrowOnGetCompactionDetails = null;
    RuntimeException exceptionToThrowOnResume = null;
    RuntimeException exceptionToThrowOnCompact = null;
    boolean started = true;

    MockBlobStore(StoreConfig config, StoreMetrics metrics, Time time, CompactionDetails details) {
      this(config, metrics, time, new CountDownLatch(0), details);
    }

    MockBlobStore(StoreConfig config, StoreMetrics metrics, Time time, CountDownLatch compactCallsCountdown,
        CompactionDetails details) {
      super(StoreTestUtils.createMockReplicaId("", 0, null), config, null, null, null, null, metrics, metrics, null,
          null, null, null, time);
      this.compactCallsCountdown = compactCallsCountdown;
      this.details = details;
    }

    @Override
    CompactionDetails getCompactionDetails(CompactionPolicy compactionPolicy) throws StoreException {
      if (exceptionToThrowOnGetCompactionDetails != null) {
        throw exceptionToThrowOnGetCompactionDetails;
      }
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
