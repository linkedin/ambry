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
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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


/**
 * Test {@link StorageManager} and {@link DiskManager}
 */
public class StorageManagerTest {
  private static final Random RANDOM = new Random();

  private DiskManagerConfig diskManagerConfig;
  private StoreConfig storeConfig;
  private MockClusterMap clusterMap;
  private MetricRegistry metricRegistry;

  /**
   * Startup the {@link MockClusterMap} for a test.
   * @throws IOException
   */
  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, 1, 3, 3);
    metricRegistry = clusterMap.getMetricRegistry();
    generateConfigs(false);
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
    Utils.deleteFileOrDirectory(new File(mountPathToDelete));
    StorageManager storageManager = createStorageManager(replicas, metricRegistry);
    storageManager.start();
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals("DiskSpaceAllocator should not have failed to start.", 0,
        getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
    assertEquals("Unexpected number of store start failures", downReplicaCount,
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals("Expected 1 disk mount path failure", 1,
        getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    checkStoreAccessibility(replicas, mountPathToDelete, storageManager);

    assertEquals("Compaction thread count is incorrect", mountPaths.size() - 1,
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, mountPaths.size() - 1);
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
    assertEquals(downReplicaCount,
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
  }

  /**
   * Tests that schedule compaction and disable compaction in StorageManager
   * @throws Exception
   */
  @Test
  public void testScheduleAndDisableCompaction() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<MockDataNodeId> dataNodes = new ArrayList<>();
    dataNodes.add(dataNode);
    MockPartitionId invalidPartition = new MockPartitionId(Long.MAX_VALUE, dataNodes, 0);
    List<? extends ReplicaId> invalidPartitionReplicas = invalidPartition.getReplicaIds();
    StorageManager storageManager = createStorageManager(replicas, metricRegistry);
    storageManager.start();
    // add invalid replica id
    replicas.add(invalidPartitionReplicas.get(0));
    for (int i = 0; i < replicas.size() - 1; i++) {
      ReplicaId replica = replicas.get(i);
      PartitionId id = replica.getPartitionId();
      assertTrue("Schedule compaction should succeed", storageManager.scheduleNextForCompaction(id));
      if (i == replicas.size() - 1) {
        assertFalse("Schedule compaction should fail", storageManager.scheduleNextForCompaction(id));
        assertFalse("Disable compaction should fail", storageManager.disableCompactionForBlobStore(id));
      }
    }
    ReplicaId replica = replicas.get(0);
    PartitionId id = replica.getPartitionId();
    assertTrue("Disable compaction should succeed", storageManager.disableCompactionForBlobStore(id));
    assertFalse("Schedule compaction should fail", storageManager.scheduleNextForCompaction(id));
    replica = replicas.get(1);
    id = replica.getPartitionId();
    assertTrue("Schedule compaction should succeed", storageManager.scheduleNextForCompaction(id));
    replicas.remove(replicas.size() - 1);
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry);
    storageManager.start();
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(0,
        getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry);
    storageManager.start();
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(0,
        getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
    assertEquals(downReplicaCount, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    checkStoreAccessibility(replicas, badDiskMountPath, storageManager);
    assertEquals("Compaction thread count is incorrect", mountPaths.size(),
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, mountPaths.size());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
    assertEquals(downReplicaCount,
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
  }

  /**
   * Test that stores on a disk are inaccessible if the {@link DiskSpaceAllocator} fails to start.
   * @throws Exception
   */
  @Test
  public void diskSpaceAllocatorTest() throws Exception {
    generateConfigs(true);
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<String> mountPaths = dataNode.getMountPaths();
    // There should be 1 unallocated segment per replica on a mount path (each replica can have 2 segments) and the
    // swap segments.
    int expectedSegmentsInPool =
        (replicas.size() / mountPaths.size()) + diskManagerConfig.diskManagerRequiredSwapSegmentsPerSize;
    // Test that StorageManager starts correctly when segments are created in the reserve pool.
    // Startup/shutdown one more time to verify the restart scenario.
    for (int i = 0; i < 2; i++) {
      metricRegistry = new MetricRegistry();
      StorageManager storageManager = createStorageManager(replicas, metricRegistry);
      storageManager.start();
      checkStoreAccessibility(replicas, null, storageManager);
      Map<String, Counter> counters = metricRegistry.getCounters();
      assertEquals(0,
          getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
      assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
      assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
      for (String mountPath : dataNode.getMountPaths()) {
        DiskSpaceAllocatorTest.verifyPoolState(new File(mountPath, diskManagerConfig.diskManagerReserveFileDirName),
            new DiskSpaceAllocatorTest.ExpectedState().add(storeConfig.storeSegmentSizeInBytes,
                expectedSegmentsInPool));
      }
      shutdownAndAssertStoresInaccessible(storageManager, replicas);
      assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
    }

    // Induce a initializePool failure by:
    // 1. deleting a file size directory
    // 2. instantiating the DiskManagers (this will not fail b/c the directory just won't be inventory)
    // 3. creating a regular file with the same name as the file size directory
    // 4. start the DiskManagers (this should cause the DiskSpaceAllocator to fail to initialize when it sees the
    //    file where the directory should be created.
    metricRegistry = new MetricRegistry();
    String diskToFail = mountPaths.get(RANDOM.nextInt(mountPaths.size()));
    File reservePoolDir = new File(diskToFail, diskManagerConfig.diskManagerReserveFileDirName);
    File fileSizeDir =
        new File(reservePoolDir, DiskSpaceAllocator.generateFileSizeDirName(storeConfig.storeSegmentSizeInBytes));
    Utils.deleteFileOrDirectory(fileSizeDir);
    StorageManager storageManager = createStorageManager(replicas, metricRegistry);
    assertTrue("File creation should have succeeded", fileSizeDir.createNewFile());
    storageManager.start();
    checkStoreAccessibility(replicas, diskToFail, storageManager);
    Map<String, Counter> counters = metricRegistry.getCounters();
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry);
    storageManager.start();
    checkStoreAccessibility(replicas, null, storageManager);
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(0,
        getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
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
   * Construct a {@link StorageManager} for the passed in set of replicas.
   * @param replicas the list of replicas for the {@link StorageManager} to use.
   * @param metricRegistry the {@link MetricRegistry} instance to use to instantiate {@link StorageManager}
   * @return a started {@link StorageManager}
   * @throws StoreException
   */
  private StorageManager createStorageManager(List<ReplicaId> replicas, MetricRegistry metricRegistry)
      throws StoreException, InterruptedException {
    StorageManager storageManager =
        new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry, replicas,
            new MockIdFactory(), new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(), null,
            SystemTime.getInstance());
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

  /**
   * Check that stores on a bad disk are not accessible and that all other stores are accessible.
   * @param replicas a list of all {@link ReplicaId}s on the node.
   * @param badDiskMountPath the disk mount path that should have caused failures or {@code null} if all disks are fine.
   * @param storageManager the {@link StorageManager} to test.
   */
  private void checkStoreAccessibility(List<ReplicaId> replicas, String badDiskMountPath,
      StorageManager storageManager) {
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
  }

  /**
   * Generates {@link StoreConfig} and {@link DiskManagerConfig} for use in tests.
   * @param segmentedLog {@code true} to set a segment capacity lower than total store capacity
   */
  private void generateConfigs(boolean segmentedLog) {
    Properties properties = new Properties();
    properties.put("disk.manager.enable.segment.pooling", "true");
    properties.put("store.compaction.triggers", "Periodic,Admin");
    if (segmentedLog) {
      long replicaCapacity = clusterMap.getAllPartitionIds().get(0).getReplicaIds().get(0).getCapacityInBytes();
      properties.put("store.segment.size.in.bytes", Long.toString(replicaCapacity / 2L));
    }
    VerifiableProperties vProps = new VerifiableProperties(properties);
    diskManagerConfig = new DiskManagerConfig(vProps);
    storeConfig = new StoreConfig(vProps);
  }
}

