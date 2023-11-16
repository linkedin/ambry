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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.*;
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

  @Before
  public void beforeTest() {
    partitionMap.clear();
  }

  @After
  public void afterTest() {
    compactor.shutdown();
    Utils.shutDownExecutorService(cloudCompactionScheduler, cloudConfig.cloudBlobCompactionShutdownTimeoutSecs,
        TimeUnit.SECONDS);
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

  /**
   * Adds some partitions to compact
   * @param numPartitions Number of partitions to compact
   */
  protected void addPartitionsToCompact(int numPartitions) {
    IntStream.rangeClosed(1, numPartitions)
        .forEach(i -> partitionMap.put(new MockPartitionId(i, MockClusterMap.DEFAULT_PARTITION_CLASS), null));
  }

  /**
   * Waits for main compaction thread to end and return num of blob erased
   * @param compactor
   * @throws InterruptedException
   */
  protected int getNumBlobsErased(CloudStorageCompactor compactor) throws InterruptedException {
    // Timed wait is better than an indefinite one
    int wait = 30;
    compactor.getDoneLatch().await(wait, TimeUnit.SECONDS);
    if (compactor.getDoneLatch().getCount() != 0) {
      fail(String.format("Main compaction thread did not finish in %s seconds", wait));
    }
    return compactor.getNumBlobsErased();
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
   * Emulate slow worker
   * @return
   */
  protected int slowWorker(String partition, int numSlowWorkers, CountDownLatch fastWorkerLatch,
      CountDownLatch slowWorkerLatch) {
    // Partition indexing starts from 1, hence <= and not <.
    if (Integer.valueOf(partition) <= numSlowWorkers) {
      try {
        slowWorkerLatch.countDown();
        int sleep = 24;
        logger.info("Thread {} is sleeping for {} hours on partition-{}", Thread.currentThread().getName(),
            sleep, partition);
        sleep(TimeUnit.HOURS.toMillis(sleep));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    fastWorkerLatch.countDown();
    return 1; // Just erase one blob for easy debug
  }

  /**
   * Emulate error worker
   * @return
   */
  protected int errorWorker(String partition, int errPct) {
    // Every n-th partition throws an error
    if (Math.floorMod(Integer.valueOf(partition), errPct) == 0) {
      String err = String.format("Thread {} throws error for partition-%s",
          Thread.currentThread().getName(), partition);
      throw new RuntimeException(err);
    }
    return 1; // Just erase one blob for easy debug
  }

  /**
   * Mock partition ownership
   * @return
   */
  protected boolean mockIsPartitionOwned(PartitionId partitionId, int errPct) {
    // Skip every n-th partition
    return Math.floorMod(partitionId.getId(), errPct) != 0;
  }

  /**
   * Tests compaction shutdown for slow workers
   * @throws InterruptedException
   */
  @Test
  public void testCompactionFastSlowWorkers() throws InterruptedException {
    int numPartitions = 5000;
    int numSlowWorkers = cloudConfig.cloudCompactionNumThreads - 1; // n-1 slow worker
    addPartitionsToCompact(numPartitions);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    // Emulate slow worker for some partitions
    CountDownLatch fastWorkerLatch = new CountDownLatch(numPartitions - numSlowWorkers);
    CountDownLatch slowWorkerLatch = new CountDownLatch(numSlowWorkers);
    try {
      Mockito.lenient().when(mockDest.compactPartition(any()))
          .thenAnswer((Answer<Integer>) invocation ->
              slowWorker(invocation.getArgument(0), numSlowWorkers, fastWorkerLatch, slowWorkerLatch));
    } catch (CloudStorageException e) {
      throw new RuntimeException(e);
    }
    cloudCompactionScheduler.scheduleWithFixedDelay(compactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        cloudConfig.cloudBlobCompactionIntervalHours, TimeUnit.HOURS);
    fastWorkerLatch.await(); // Not sure why, but removing this fails the test
    slowWorkerLatch.await();
    shutdownCompactionWorkers(compactor);
    assertEquals(numPartitions-numSlowWorkers, getNumBlobsErased(compactor));
  }

  /**
   * Tests compaction for disowned partitions
   */
  @Test
  public void testCompactionDisownedPartition() throws InterruptedException, CloudStorageException {
    int numPartitions = 5000, errPct = 10; // 10% disowned
    // The mock isn't effective when there are 128+ mocks.
    // The current method is scalable and effective. Set a common mock, instead of 128 and let it decide.
    int numDisownedPartitions = Math.floorDiv(numPartitions, errPct);
    addPartitionsToCompact(numPartitions);
    CloudStorageCompactor spyCompactor = spy(compactor);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    // Emulate disowned partition
    Mockito.lenient()
        .when(spyCompactor.isPartitionOwned(any()))
        .thenAnswer((Answer<Boolean>) invocation -> mockIsPartitionOwned(invocation.getArgument(0), errPct));
    Mockito.lenient().when(mockDest.compactPartition(any())).thenReturn(1);
    cloudCompactionScheduler.scheduleWithFixedDelay(spyCompactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        cloudConfig.cloudBlobCompactionIntervalHours, TimeUnit.HOURS);
    assertEquals(numPartitions-numDisownedPartitions, getNumBlobsErased(spyCompactor));
  }

  /**
   * Tests compaction for error workers
   * @throws InterruptedException
   */
  @Test
  public void testCompactionErrorWorkers() throws InterruptedException {
    int numPartitions = 5000, errPct = 10; // 10% errors
    int numErrorWorkers = Math.floorDiv(numPartitions, errPct);
    addPartitionsToCompact(numPartitions);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    // Emulate error worker for some partitions
    try {
      Mockito.lenient().when(mockDest.compactPartition(any()))
          .thenAnswer((Answer<Integer>) invocation -> errorWorker(invocation.getArgument(0), errPct));
    } catch (CloudStorageException e) {
      throw new RuntimeException(e);
    }
    cloudCompactionScheduler.scheduleWithFixedDelay(compactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        cloudConfig.cloudBlobCompactionIntervalHours, TimeUnit.HOURS);
    assertEquals(numPartitions-numErrorWorkers, getNumBlobsErased(compactor));
  }

  /**
   * Tests compaction when during shutdown
   * @throws InterruptedException
   */
  @Test
  public void testCompactionDuringShutdown() throws InterruptedException {
    int numPartitions = 5000;
    addPartitionsToCompact(numPartitions);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    // Emulate shutdown worker for some partitions
    Mockito.lenient().when(mockDest.isCompactionStopped()).thenReturn(true);
    cloudCompactionScheduler.scheduleWithFixedDelay(compactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        cloudConfig.cloudBlobCompactionIntervalHours, TimeUnit.HOURS);
    assertEquals(0, getNumBlobsErased(compactor));
  }
}
