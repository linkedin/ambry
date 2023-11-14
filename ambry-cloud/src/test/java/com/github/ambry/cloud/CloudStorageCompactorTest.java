/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class CloudStorageCompactorTest {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageCompactorTest.class);

  private final CloudDestination mockDest = mock(CloudDestination.class);
  private final CloudStorageCompactor compactor;
  private final Map<PartitionId, PartitionInfo> partitionMap = new HashMap<>();
  private final VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());
  private final int pageSize = 10;
  protected ScheduledExecutorService cloudCompactionScheduler;
  protected CloudConfig cloudConfig;
  public CloudStorageCompactorTest() {
    Properties properties = new Properties();
    properties.setProperty(CloudConfig.CLOUD_BLOB_COMPACTION_QUERY_LIMIT, String.valueOf(pageSize));
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_QUERY_BUCKET_DAYS, "7");
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_LOOKBACK_DAYS, "28");
    properties.setProperty(CloudConfig.CLOUD_BLOB_COMPACTION_SHUTDOWN_TIMEOUT_SECS, "1"); // 10-sec default too high
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_NUM_THREADS, "3");
    properties.setProperty(CloudConfig.CLOUD_BLOB_COMPACTION_INTERVAL_HOURS, "48"); // Set a long sleep between cycles
    properties.setProperty(CloudConfig.CLOUD_BLOB_COMPACTION_ENABLED, "true");
    properties.setProperty(CloudConfig.CLOUD_BLOB_COMPACTION_STARTUP_DELAY_SECS, "0"); // No delay for testing
    cloudConfig = new CloudConfig(new VerifiableProperties(properties));
    compactor = new CloudStorageCompactor(mockDest, cloudConfig, partitionMap.keySet(), vcrMetrics);
    cloudCompactionScheduler =
        Utils.newScheduler(1, "cloud-compaction-controller-", true);
  }

  /**
   * Test the compactPartitions method.
   */
  @Test
  public void testCompactPartitions() throws Exception {
    // start with empty map
    assertEquals(0, compactor.compactPartitions());

    int numPartitions = 40;
    // add partitions to map
    String defaultClass = MockClusterMap.DEFAULT_PARTITION_CLASS;
    for (int i = 0; i < numPartitions; i++) {
      partitionMap.put(new MockPartitionId(i, defaultClass), null);
      when(mockDest.compactPartition(eq(Integer.toString(i)))).thenReturn(pageSize);
    }

    assertEquals(pageSize * numPartitions, compactor.compactPartitions());
    assertEquals(0, vcrMetrics.compactionFailureCount.getCount());

    // remove a partition from map
    partitionMap.remove(new MockPartitionId(0, defaultClass));
    assertEquals(pageSize * (numPartitions - 1), compactor.compactPartitions());
    assertEquals(0, vcrMetrics.compactionFailureCount.getCount());

    // Make compaction fail for some partitions
    CloudStorageException csex = new CloudStorageException("failure", new RuntimeException("Don't hurt me!"));
    when(mockDest.compactPartition(eq("2"))).thenThrow(csex);
    when(mockDest.compactPartition(eq("20"))).thenThrow(csex);
    assertEquals(pageSize * (numPartitions - 3), compactor.compactPartitions());
    assertEquals(2, vcrMetrics.compactionFailureCount.getCount());

    // Test shutdown
    assertFalse("Should not be shutting down yet", compactor.isShutDown());
    compactor.shutdown();
    assertTrue("Should be shutting down now", compactor.isShutDown());
    // TODO: test shutting down with compaction still in progress (more involved)
  }

  protected void waitForThreadState(Thread thread, Thread.State state, boolean assertFinalState) throws InterruptedException {
    int waitAttempt = 0, maxWaitAttempt = 10;
    // 1-second loop
    while (thread.getState() != state && waitAttempt < maxWaitAttempt) {
      logger.info("Waiting for {} thread state to change from {} to {}...", thread.getName(), thread.getState(), state);
      Thread.sleep(100);
      waitAttempt++;
    }
    if (assertFinalState) {
      assertEquals(state, thread.getState());
    }
  }

  /**
   * Adds some partitions to compact
   * @param numPartitions Number of partitions to compact
   */
  protected void addPartitionsToCompact(int numPartitions) {
    String defaultClass = MockClusterMap.DEFAULT_PARTITION_CLASS;
    for (int i = 0; i < numPartitions; i++) {
      partitionMap.put(new MockPartitionId(i, defaultClass), null);
    }
  }

  /**
   * Waits for main compaction thread to start by checking the startLatch
   * @param compactor
   * @return
   * @throws InterruptedException
   */
  protected Thread waitOrFailForMainCompactionThreadToStart(CloudStorageCompactor compactor) throws InterruptedException {
    // Timed wait is better than an indefinite one
    int wait = 30;
    compactor.getStartLatchRef().get().await(wait, TimeUnit.SECONDS);
    Thread compactionController = compactor.getMainCompactorThreadRef().get();
    if (compactionController == null) {
      fail(String.format("Main compaction thread did not start in %s seconds", wait));
    }
    return compactionController;
  }

  /**
   * Waits for main compaction thread to start by checking the doneLatch
   * @param compactor
   * @throws InterruptedException
   */
  protected void waitOrFailForMainCompactionThreadToEnd(CloudStorageCompactor compactor) throws InterruptedException {
    // Timed wait is better than an indefinite one
    int wait = 30;
    compactor.getDoneLatchRef().get().await(wait, TimeUnit.SECONDS);
    if (compactor.getDoneLatchRef().get().getCount() != 0) {
      fail(String.format("Main compaction thread did not finish in %s seconds", wait));
    }
  }

  /**
   * Shuts down compaction workers and asserts a shutdown
   * @param compactor
   */
  protected void shutdownCompactionWorkers(CloudStorageCompactor compactor) {
    compactor.shutdown();
    assertTrue(compactor.isShutDown());
  }

  /**
   * Tests compaction shutdown
   * @throws CloudStorageException
   * @throws InterruptedException
   */
  @Test
  public void testShutdownCompaction() throws CloudStorageException, InterruptedException {
    addPartitionsToCompact(17);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    Mockito.lenient().when(mockDest.compactPartition(any())).thenReturn(1479);
    cloudCompactionScheduler.scheduleWithFixedDelay(compactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours), TimeUnit.SECONDS);
    Thread compactionController =
        waitOrFailForMainCompactionThreadToStart(compactor);
    waitOrFailForMainCompactionThreadToEnd(compactor);
    shutdownCompactionWorkers(compactor);
    cloudCompactionScheduler.shutdownNow();
    waitForThreadState(compactionController, Thread.State.TERMINATED, true);
  }

  /**
   * Tests compaction shutdown for slow or blocked workers
   * @throws CloudStorageException
   * @throws InterruptedException
   */
  @Test
  public void testShutdownCompactionSlowWorkers() throws CloudStorageException, InterruptedException {
    addPartitionsToCompact(23);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    Mockito.lenient().when(mockDest.compactPartition(any())).thenAnswer((Answer<Integer>) invocation -> {
      try {
        // Emulate slow worker
        logger.info("Thread {} is sleeping for 24 hours", Thread.currentThread().getName());
        Thread.sleep(TimeUnit.HOURS.toMillis(24));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return 1358;
    });
    cloudCompactionScheduler.scheduleWithFixedDelay(compactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours), TimeUnit.SECONDS);
    Thread compactionController =
        waitOrFailForMainCompactionThreadToStart(compactor);
    // Controller thread is waiting for slow workers
    waitForThreadState(compactionController, Thread.State.WAITING, true);
    shutdownCompactionWorkers(compactor);
    cloudCompactionScheduler.shutdownNow();
    waitOrFailForMainCompactionThreadToEnd(compactor);
    waitForThreadState(compactionController, Thread.State.TERMINATED, true);
  }

  /**
   * Tests compaction shutdown before submitting compaction tasks
   */
  @Test
  public void testShutdownCompactionBeforeSchedulingWorkers() {
    addPartitionsToCompact(29);
    shutdownCompactionWorkers(compactor);
    assertEquals(0, compactor.compactPartitions());
    // Just one invocation for shutdown
    assertEquals(1, Mockito.mockingDetails(mockDest).getInvocations().size());
  }

  /**
   * Tests compaction for disowned partitions
   */
  @Test
  public void testCompactionDisownedPartition() {
    addPartitionsToCompact(19);
    CloudStorageCompactor spyCompactor = spy(compactor);
    when(spyCompactor.isPartitionOwned(any())).thenReturn(false);
    assertEquals(0, compactor.compactPartitions());
  }
}
