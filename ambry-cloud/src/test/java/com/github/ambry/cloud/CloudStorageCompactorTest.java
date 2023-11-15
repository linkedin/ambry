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
    String defaultClass = MockClusterMap.DEFAULT_PARTITION_CLASS;
    IntStream.rangeClosed(1, numPartitions).forEach(i -> partitionMap.put(new MockPartitionId(i, defaultClass), null));
  }

  /**
   * Waits for main compaction thread to start by checking the doneLatch
   * @param compactor
   * @throws InterruptedException
   */
  protected void waitOrFailForMainCompactionThreadToEnd(CloudStorageCompactor compactor) throws InterruptedException {
    // Timed wait is better than an indefinite one
    int wait = 30;
    compactor.getDoneLatch().await(wait, TimeUnit.SECONDS);
    if (compactor.getDoneLatch().getCount() != 0) {
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
   * Emulate slow worker
   * @return
   */
  protected int slowWorker(int partition) {
    try {
      logger.info("Thread {} is sleeping for 24 hours on partition-{}", Thread.currentThread().getName(), partition);
      sleep(TimeUnit.HOURS.toMillis(24));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return 1;
  }

  /**
   * Emulate slow worker
   * @return
   */
  protected int errorWorker(int partition) {
    String err = String.format("Thread {} throws error for partition-{}", Thread.currentThread().getName(), partition);
    throw new RuntimeException(err);
  }

  /**
   * Emulate fast worker
   * @return
   */
  protected int fastWorker(int partition, CountDownLatch fastWorkerLatch) {
    logger.info("Thread {} is compacting partition-{}", Thread.currentThread().getName(), partition);
    fastWorkerLatch.countDown();
    return 1;
  }

  /**
   * Tests compaction shutdown for slow or blocked workers
   * @throws InterruptedException
   */
  @Test
  public void testShutdownCompactionFastSlowWorkers() throws InterruptedException {
    int numPartitions = 3000, numBlobsErased = 1;
    int numSlowWorkers = cloudConfig.cloudCompactionNumThreads - 1; // n-1 slow worker
    addPartitionsToCompact(numPartitions);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    // Emulate slow worker for some partitions
    IntStream.rangeClosed(1, numSlowWorkers).forEach(i -> {
      try {
        Mockito.lenient().when(mockDest.compactPartition(eq(String.valueOf(i))))
            .thenAnswer((Answer<Integer>) invocation -> slowWorker(i));
      } catch (CloudStorageException e) {
        throw new RuntimeException(e);
      }
    });
    CountDownLatch fastWorkerLatch = new CountDownLatch(numPartitions - numSlowWorkers);
    // For the remaining partitions, return a valid answer
    IntStream.rangeClosed(numSlowWorkers+1, numPartitions).forEach(i -> {
      try {
        Mockito.lenient().when(mockDest.compactPartition(eq(String.valueOf(i))))
            .thenAnswer((Answer<Integer>) invocation -> fastWorker(i, fastWorkerLatch));
      } catch (CloudStorageException e) {
        throw new RuntimeException(e);
      }
    });

    cloudCompactionScheduler.scheduleWithFixedDelay(compactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours), TimeUnit.SECONDS);
    fastWorkerLatch.await();
    shutdownCompactionWorkers(compactor);
    waitOrFailForMainCompactionThreadToEnd(compactor);
    assertEquals((numPartitions-numSlowWorkers)*numBlobsErased, compactor.getNumBlobsErased());
  }


  /**
   * Tests compaction for disowned partitions
   */
  @Test
  public void testCompactionDisownedPartition() {
    int numPartitions = 5000, numBlobsErased = 1; // Erase 1 blob for debugging
    int numDisownedPartitions = Math.floorDiv(numPartitions, 10); // 10% disowned partitions
    addPartitionsToCompact(numPartitions);
    CloudStorageCompactor spyCompactor = spy(compactor);
    // Emulate disowned partition
    IntStream.rangeClosed(1, numDisownedPartitions).forEach(i ->
        Mockito.lenient()
            .when(spyCompactor.isPartitionOwned(eq(new MockPartitionId(i, MockClusterMap.DEFAULT_PARTITION_CLASS))))
            .thenReturn(false));
    // Return valid answer for other partitions
    IntStream.rangeClosed(numDisownedPartitions+1, numPartitions).forEach(i -> {
      try {
        // Use eq(), not any(). It messes up mocking for this test. Be specific in mock-args.
        Mockito.lenient().when(mockDest.compactPartition(eq(String.valueOf(i)))).thenReturn(numBlobsErased);
      } catch (CloudStorageException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals((numPartitions-numDisownedPartitions)*numBlobsErased, spyCompactor.compactPartitions());
  }

  /**
   * Tests compaction shutdown for error workers
   * @throws InterruptedException
   */
  @Test
  public void testShutdownCompactionErrorWorkers() throws InterruptedException {
    int numPartitions = 3000, numBlobsErased = 1;
    int numSlowWorkers = cloudConfig.cloudCompactionNumThreads - 1; // n-1 slow worker
    addPartitionsToCompact(numPartitions);
    // mockDest is used inside compactor but Mockito cannot infer this.
    // Use lenient() to avoid UnnecessaryStubbingException.
    // Emulate error worker for some partitions
    IntStream.rangeClosed(1, numSlowWorkers).forEach(i -> {
      try {
        Mockito.lenient().when(mockDest.compactPartition(eq(String.valueOf(i))))
            .thenAnswer((Answer<Integer>) invocation -> errorWorker(i));
      } catch (CloudStorageException e) {
        throw new RuntimeException(e);
      }
    });
    CountDownLatch fastWorkerLatch = new CountDownLatch(numPartitions - numSlowWorkers);
    // For the remaining partitions, return a valid answer
    IntStream.rangeClosed(numSlowWorkers+1, numPartitions).forEach(i -> {
      try {
        Mockito.lenient().when(mockDest.compactPartition(eq(String.valueOf(i))))
            .thenAnswer((Answer<Integer>) invocation -> fastWorker(i, fastWorkerLatch));
      } catch (CloudStorageException e) {
        throw new RuntimeException(e);
      }
    });

    cloudCompactionScheduler.scheduleWithFixedDelay(compactor, cloudConfig.cloudBlobCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours), TimeUnit.SECONDS);
    fastWorkerLatch.await();
    shutdownCompactionWorkers(compactor);
    waitOrFailForMainCompactionThreadToEnd(compactor);
    assertEquals((numPartitions-numSlowWorkers)*numBlobsErased, compactor.getNumBlobsErased());
  }
}
