/*
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class StorageManagerTest {
  private static final Random RANDOM = new Random();
  private MockClusterMap clusterMap;
  private MetricRegistry metricRegistry;

  /**
   * Startup the {@link MockClusterMap} for a test.
   * @throws IOException
   */
  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, 1, 3, 3);
    this.metricRegistry = clusterMap.getMetricRegistry();
  }

  /**
   * Cleanup the {@link MockClusterMap} after a test.
   * @throws IOException
   */
  @After
  public void cleanupCluster() throws IOException {
    clusterMap.cleanup();
  }

  /**
   * Test that stores on a disk without a valid mount path are not started.
   * @throws Exception
   */
  @Test
  public void mountPathNotFoundTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<String> mountPaths = dataNode.getMountPaths();
    String mountPathToDelete = mountPaths.get(RANDOM.nextInt(mountPaths.size()));
    int downReplicaCount = 0;
    for (ReplicaId replica : replicas) {
      if (replica.getMountPath().equals(mountPathToDelete)) {
        downReplicaCount++;
      }
    }
    deleteDirectory(new File(mountPathToDelete));
    StorageManager storageManager = createAndStartStoreManager(replicas, metricRegistry);
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(downReplicaCount, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals(1, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    for (ReplicaId replica : replicas) {
      PartitionId id = replica.getPartitionId();
      if (replica.getMountPath().equals(mountPathToDelete)) {
        assertNull("This store should not be accessible.", storageManager.getStore(id));
        assertFalse("Compaction should not be scheduled", storageManager.scheduleNextForCompaction(id));
      } else {
        Store store = storageManager.getStore(id);
        assertTrue("Store should be started", ((BlobStore) store).isStarted());
        assertTrue("Compaction should be scheduled", storageManager.scheduleNextForCompaction(id));
      }
    }

    assertEquals("Compaction thread count is incorrect", mountPaths.size() - 1,
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, mountPaths.size() - 1);
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
    assertEquals(downReplicaCount,
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
  }

  /**
   * Tests that {@link StorageManager} can start even when certain stores cannot be started. Checks that these stores
   * are not accessible. We can make the replica path non-readable to induce a store starting failure.
   * @throws Exception
   */
  @Test
  public void storeStartFailureTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    Set<Integer> badReplicaIndexes = new HashSet<>(Arrays.asList(2, 7));
    for (Integer badReplicaIndex : badReplicaIndexes) {
      new File(replicas.get(badReplicaIndex).getReplicaPath()).setReadable(false);
    }
    StorageManager storageManager = createAndStartStoreManager(replicas, metricRegistry);
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(badReplicaIndexes.size(),
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    for (int i = 0; i < replicas.size(); i++) {
      ReplicaId replica = replicas.get(i);
      PartitionId id = replica.getPartitionId();
      if (badReplicaIndexes.contains(i)) {
        assertNull("This store should not be accessible.", storageManager.getStore(id));
        assertFalse("Compaction should not be scheduled", storageManager.scheduleNextForCompaction(id));
      } else {
        Store store = storageManager.getStore(id);
        assertTrue("Store should be started", ((BlobStore) store).isStarted());
        assertTrue("Compaction should be scheduled", storageManager.scheduleNextForCompaction(id));
      }
    }
    assertEquals("Compaction thread count is incorrect", dataNode.getMountPaths().size(),
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, dataNode.getMountPaths().size());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
    assertEquals(badReplicaIndexes.size(),
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
  }

  /**
   * Tests that {@link StorageManager} can start when all of the stores on one disk fail to start. Checks that these
   * stores are not accessible. We can make the replica path non-readable to induce a store starting failure.
   * @throws Exception
   */
  @Test
  public void storeStartFailureOnOneDiskTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<String> mountPaths = dataNode.getMountPaths();
    String badDiskMountPath = mountPaths.get(RANDOM.nextInt(mountPaths.size()));
    int downReplicaCount = 0;
    for (ReplicaId replica : replicas) {
      if (replica.getMountPath().equals(badDiskMountPath)) {
        new File(replica.getReplicaPath()).setReadable(false);
        downReplicaCount++;
      }
    }
    StorageManager storageManager = createAndStartStoreManager(replicas, metricRegistry);
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(downReplicaCount, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    for (ReplicaId replica : replicas) {
      PartitionId id = replica.getPartitionId();
      if (replica.getMountPath().equals(badDiskMountPath)) {
        assertNull("This store should not be accessible.", storageManager.getStore(id));
        assertFalse("Compaction should not be scheduled", storageManager.scheduleNextForCompaction(id));
      } else {
        Store store = storageManager.getStore(id);
        assertTrue("Store should be started", ((BlobStore) store).isStarted());
        assertTrue("Compaction should be scheduled", storageManager.scheduleNextForCompaction(id));
      }
    }
    assertEquals("Compaction thread count is incorrect", mountPaths.size(),
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, mountPaths.size());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
    assertEquals(downReplicaCount,
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
  }

  /**
   * Test that stores for all partitions on a node have been started and partitions not present on this node are
   * inaccessible. Also tests all stores are shutdown on shutting down the storage manager
   * @throws Exception
   */
  @Test
  public void successfulStartupShutdownTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    StorageManager storageManager = createAndStartStoreManager(replicas, metricRegistry);
    for (ReplicaId replica : replicas) {
      Store store = storageManager.getStore(replica.getPartitionId());
      assertTrue("Store should be started", ((BlobStore) store).isStarted());
      assertTrue("Compaction should be scheduled", storageManager.scheduleNextForCompaction(replica.getPartitionId()));
    }
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    MockPartitionId invalidPartition = new MockPartitionId(Long.MAX_VALUE, Collections.<MockDataNodeId>emptyList(), 0);
    assertNull("Should not have found a store for an invalid partition.", storageManager.getStore(invalidPartition));
    assertEquals("Compaction thread count is incorrect", dataNode.getMountPaths().size(),
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, dataNode.getMountPaths().size());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
  }

  /**
   * Create a {@link StorageManager} and start stores for the passed in set of replicas.
   * @param replicas the list of replicas for the {@link StorageManager} to use.
   * @param metricRegistry the {@link MetricRegistry} instance to use to instantiate {@link StorageManager}
   * @return a started {@link StorageManager}
   * @throws StoreException
   */
  private StorageManager createAndStartStoreManager(List<ReplicaId> replicas, MetricRegistry metricRegistry)
      throws StoreException, InterruptedException {
    Properties properties = new Properties();
    properties.put("store.compaction.triggers", "Periodic,Admin");
    StorageManager storageManager =
        new StorageManager(new StoreConfig(new VerifiableProperties(properties)), Utils.newScheduler(1, false),
            metricRegistry, replicas, new MockIdFactory(), new DummyMessageStoreRecovery(),
            new DummyMessageStoreHardDelete(), null, SystemTime.getInstance());
    storageManager.start();
    return storageManager;
  }

  /**
   * Shutdown a {@link StorageManager} and assert that the stores cannot be accessed for the provided replicas.
   * @param storageManager the {@link StorageManager} to shutdown.
   * @param replicas the {@link ReplicaId}s to check for store inaccessibility.
   * @throws StoreException
   * @throws InterruptedException
   */
  private static void shutdownAndAssertStoresInaccessible(StorageManager storageManager, List<ReplicaId> replicas)
      throws StoreException, InterruptedException {
    storageManager.shutdown();
    for (ReplicaId replica : replicas) {
      assertNull(storageManager.getStore(replica.getPartitionId()));
    }
  }

  /**
   * Get the counter value for the metric in {@link StorageManagerMetrics} for the given class and suffix.
   * @param counters Map of counter metrics to use
   * @param className the class to which the metric belongs to
   * @param suffix the suffix of the metric that distinguishes it from other metrics in the class.
   * @return the value of the counter.
   */
  private long getCounterValue(Map<String, Counter> counters, String className, String suffix) {
    return counters.get(className + "." + suffix).getCount();
  }

  /**
   * Delete a directory recursively.
   * @param file the directory to delete.
   */
  private static void deleteDirectory(File file) {
    File[] contents = file.listFiles();
    if (contents != null) {
      for (File f : contents) {
        deleteDirectory(f);
      }
    }
    file.delete();
  }

  /**
   * Verifies that return value {@link StorageManager#getCompactionThreadCount()} of the given {@code storageManager}
   * is equal to {@code expectedCount}
   * @param storageManager the {@link StorageManager} instance to use.
   * @param expectedCount the number of compaction threads expected.
   * @throws InterruptedException
   */
  private static void verifyCompactionThreadCount(StorageManager storageManager, int expectedCount)
      throws InterruptedException {
    // there is no option but to sleep here since we have to wait for the CompactionManager to start the threads up
    // we cannot mock these components since they are internally constructed within the StorageManager and DiskManager.
    int totalWaitTimeMs = 1000;
    int alreadyWaitedMs = 0;
    int singleWaitTimeMs = 10;
    while (storageManager.getCompactionThreadCount() != expectedCount && alreadyWaitedMs < totalWaitTimeMs) {
      Thread.sleep(singleWaitTimeMs);
      alreadyWaitedMs += singleWaitTimeMs;
    }
    assertEquals("Compaction thread count report not as expected", expectedCount,
        storageManager.getCompactionThreadCount());
  }
}

