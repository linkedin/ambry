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
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


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
    clusterMap = new MockClusterMap(false, 1, 3, 3, false);
    metricRegistry = clusterMap.getMetricRegistry();
    generateConfigs(false);
  }

  /**
   * Cleanup the {@link MockClusterMap} after a test.
   * @throws IOException
   */
  @After
  public void cleanupCluster() throws IOException {
    if (clusterMap != null) {
      clusterMap.cleanup();
    }
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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals("DiskSpaceAllocator should not have failed to start.", 0,
        getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
    assertEquals("Unexpected number of store start failures", downReplicaCount,
        getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals("Expected 1 disk mount path failure", 1,
        getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    checkStoreAccessibility(replicas, mountPathToDelete, storageManager);

    assertEquals("Compaction thread count is incorrect", mountPaths.size() - 1,
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, mountPaths.size() - 1);
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
  }

  /**
   * Tests that schedule compaction and control compaction in StorageManager
   * @throws Exception
   */
  @Test
  public void scheduleAndControlCompactionTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<MockDataNodeId> dataNodes = new ArrayList<>();
    dataNodes.add(dataNode);
    MockPartitionId invalidPartition =
        new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS, dataNodes, 0);
    List<? extends ReplicaId> invalidPartitionReplicas = invalidPartition.getReplicaIds();
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be 1 unexpected partition reported", 1, getNumUnrecognizedPartitionsReported());
    // add invalid replica id
    replicas.add(invalidPartitionReplicas.get(0));
    for (int i = 0; i < replicas.size(); i++) {
      ReplicaId replica = replicas.get(i);
      PartitionId id = replica.getPartitionId();
      if (i == replicas.size() - 1) {
        assertFalse("Schedule compaction should fail", storageManager.scheduleNextForCompaction(id));
        assertFalse("Disable compaction should fail", storageManager.controlCompactionForBlobStore(id, false));
        assertFalse("Enable compaction should fail", storageManager.controlCompactionForBlobStore(id, true));
      } else {
        assertTrue("Enable compaction should succeed", storageManager.controlCompactionForBlobStore(id, true));
        assertTrue("Schedule compaction should succeed", storageManager.scheduleNextForCompaction(id));
      }
    }
    ReplicaId replica = replicas.get(0);
    PartitionId id = replica.getPartitionId();
    assertTrue("Disable compaction should succeed", storageManager.controlCompactionForBlobStore(id, false));
    assertFalse("Schedule compaction should fail", storageManager.scheduleNextForCompaction(id));
    assertTrue("Enable compaction should succeed", storageManager.controlCompactionForBlobStore(id, true));
    assertTrue("Schedule compaction should succeed", storageManager.scheduleNextForCompaction(id));

    replica = replicas.get(1);
    id = replica.getPartitionId();
    assertTrue("Schedule compaction should succeed", storageManager.scheduleNextForCompaction(id));
    replicas.remove(replicas.size() - 1);
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test start BlobStore with given {@link PartitionId}.
   */
  @Test
  public void startBlobStoreTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<MockDataNodeId> dataNodes = new ArrayList<>();
    dataNodes.add(dataNode);
    MockPartitionId invalidPartition =
        new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS, dataNodes, 0);
    List<? extends ReplicaId> invalidPartitionReplicas = invalidPartition.getReplicaIds();
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    PartitionId id = null;
    storageManager.start();
    assertEquals("There should be 1 unexpected partition reported", 1, getNumUnrecognizedPartitionsReported());
    // shutdown all the replicas first
    for (ReplicaId replica : replicas) {
      id = replica.getPartitionId();
      assertTrue("Shutdown should succeed on given store", storageManager.shutdownBlobStore(id));
    }
    ReplicaId replica = replicas.get(0);
    id = replica.getPartitionId();
    // test start a store successfully
    assertTrue("Start should succeed on given store", storageManager.startBlobStore(id));
    // test start the store which is already started
    assertTrue("Start should succeed on the store which is already started", storageManager.startBlobStore(id));
    // test invalid partition
    replica = invalidPartitionReplicas.get(0);
    id = replica.getPartitionId();
    assertFalse("Start should fail on given invalid replica", storageManager.startBlobStore(id));
    // test start the store whose DiskManager is not running
    replica = replicas.get(replicas.size() - 1);
    id = replica.getPartitionId();
    storageManager.getDiskManager(id).shutdown();
    assertFalse("Start should fail on given store whose DiskManager is not running", storageManager.startBlobStore(id));
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test get DiskManager with given {@link PartitionId}.
   */
  @Test
  public void getDiskManagerTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<MockDataNodeId> dataNodes = new ArrayList<>();
    dataNodes.add(dataNode);
    MockPartitionId invalidPartition =
        new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS, dataNodes, 0);
    List<? extends ReplicaId> invalidPartitionReplicas = invalidPartition.getReplicaIds();
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    PartitionId id = null;
    storageManager.start();
    assertEquals("There should be 1 unexpected partition reported", 1, getNumUnrecognizedPartitionsReported());
    for (ReplicaId replica : replicas) {
      id = replica.getPartitionId();
      assertNotNull("DiskManager should not be null for valid partition", storageManager.getDiskManager(id));
    }
    // test invalid partition
    ReplicaId replica = invalidPartitionReplicas.get(0);
    id = replica.getPartitionId();
    assertNull("DiskManager should be null for invalid partition", storageManager.getDiskManager(id));
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test shutdown blobstore with given {@link PartitionId}.
   */
  @Test
  public void shutdownBlobStoreTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<MockDataNodeId> dataNodes = new ArrayList<>();
    dataNodes.add(dataNode);
    MockPartitionId invalidPartition =
        new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS, dataNodes, 0);
    List<? extends ReplicaId> invalidPartitionReplicas = invalidPartition.getReplicaIds();
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be 1 unexpected partition reported", 1, getNumUnrecognizedPartitionsReported());
    for (int i = 1; i < replicas.size() - 1; i++) {
      ReplicaId replica = replicas.get(i);
      PartitionId id = replica.getPartitionId();
      assertTrue("Shutdown should succeed on given store", storageManager.shutdownBlobStore(id));
    }
    // test shutdown the store which is not started
    ReplicaId replica = replicas.get(replicas.size() - 1);
    PartitionId id = replica.getPartitionId();
    Store store = storageManager.getStore(id);
    store.shutdown();
    assertTrue("Shutdown should succeed on the store which is not started", storageManager.shutdownBlobStore(id));
    // test shutdown the store whose DiskManager is not running
    replica = replicas.get(0);
    id = replica.getPartitionId();
    storageManager.getDiskManager(id).shutdown();
    assertFalse("Shutdown should fail on given store whose DiskManager is not running",
        storageManager.shutdownBlobStore(id));
    // test invalid partition
    replica = invalidPartitionReplicas.get(0);
    id = replica.getPartitionId();
    assertFalse("Shutdown should fail on given invalid replica", storageManager.shutdownBlobStore(id));
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test set stopped state of blobstore with given list of {@link PartitionId} in failure cases.
   */
  @Test
  public void setBlobStoreStoppedStateFailureTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<MockDataNodeId> dataNodes = new ArrayList<>();
    dataNodes.add(dataNode);
    MockPartitionId invalidPartition =
        new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS, dataNodes, 0);
    List<? extends ReplicaId> invalidPartitionReplicas = invalidPartition.getReplicaIds();
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be 1 unexpected partition reported", 1, getNumUnrecognizedPartitionsReported());
    // test set the state of store whose replicaStatusDelegate is null
    ReplicaId replica = replicas.get(0);
    PartitionId id = replica.getPartitionId();
    storageManager.getDiskManager(id).shutdown();
    List<PartitionId> failToUpdateList = storageManager.setBlobStoreStoppedState(Arrays.asList(id), true);
    assertEquals("Set store stopped state should fail on given store whose replicaStatusDelegate is null", id,
        failToUpdateList.get(0));
    // test invalid partition case (where diskManager == null)
    replica = invalidPartitionReplicas.get(0);
    id = replica.getPartitionId();
    failToUpdateList = storageManager.setBlobStoreStoppedState(Arrays.asList(id), true);
    assertEquals("Set store stopped state should fail on given invalid replica", id, failToUpdateList.get(0));
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test successfully set stopped state of blobstore with given list of {@link PartitionId}.
   */
  @Test
  public void setBlobStoreStoppedStateSuccessTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<PartitionId> partitionIds = new ArrayList<>();
    Map<DiskId, List<ReplicaId>> diskToReplicas = new HashMap<>();
    // test set the state of store with instantiated replicaStatusDelegate
    ReplicaStatusDelegate replicaStatusDelegate = new MockReplicaStatusDelegate();
    ReplicaStatusDelegate replicaStatusDelegateSpy = Mockito.spy(replicaStatusDelegate);
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, replicaStatusDelegateSpy);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    for (ReplicaId replica : replicas) {
      partitionIds.add(replica.getPartitionId());
      diskToReplicas.computeIfAbsent(replica.getDiskId(), disk -> new ArrayList<>()).add(replica);
    }
    List<PartitionId> failToUpdateList;

    // add a list of stores to STOPPED list. Note that the stores are residing on 3 disks.
    failToUpdateList = storageManager.setBlobStoreStoppedState(partitionIds, true);
    // make sure the update operation succeeds
    assertTrue("Add stores to stopped list should succeed, failToUpdateList should be empty",
        failToUpdateList.isEmpty());
    // make sure the stopped list contains all the added stores
    Set<String> stoppedReplicasCopy = new HashSet<>(replicaStatusDelegateSpy.getStoppedReplicas());
    for (ReplicaId replica : replicas) {
      assertTrue("The stopped list should contain the replica: " + replica.getPartitionId().toPathString(),
          stoppedReplicasCopy.contains(replica.getPartitionId().toPathString()));
    }
    // make sure replicaStatusDelegate is invoked 3 times and each time the input replica list conforms with stores on particular disk
    for (List<ReplicaId> replicasPerDisk : diskToReplicas.values()) {
      verify(replicaStatusDelegateSpy, times(1)).markStopped(replicasPerDisk);
    }

    // remove a list of stores from STOPPED list. Note that the stores are residing on 3 disks.
    storageManager.setBlobStoreStoppedState(partitionIds, false);
    // make sure the update operation succeeds
    assertTrue("Remove stores from stopped list should succeed, failToUpdateList should be empty",
        failToUpdateList.isEmpty());
    // make sure the stopped list is empty because all the stores are successfully removed.
    assertTrue("The stopped list should be empty after removing all stores",
        replicaStatusDelegateSpy.getStoppedReplicas().isEmpty());
    // make sure replicaStatusDelegate is invoked 3 times and each time the input replica list conforms with stores on particular disk
    for (List<ReplicaId> replicasPerDisk : diskToReplicas.values()) {
      verify(replicaStatusDelegateSpy, times(1)).unmarkStopped(replicasPerDisk);
    }
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Tests that{@link StorageManager} can correctly determine if disk is unavailable based on states of all stores.
   */
  @Test
  public void isDiskAvailableTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    Map<DiskId, List<ReplicaId>> diskToReplicas = new HashMap<>();
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    for (ReplicaId replica : replicas) {
      diskToReplicas.computeIfAbsent(replica.getDiskId(), disk -> new ArrayList<>()).add(replica);
    }
    // for each disk, shutdown all the stores except for the last one
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      for (int i = 0; i < replicasOnDisk.size() - 1; ++i) {
        storageManager.getStore(replicasOnDisk.get(i).getPartitionId()).shutdown();
      }
    }
    // verify all disks are still available because at least one store on them is up
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      assertTrue("Disk should be available", storageManager.isDiskAvailable(replicasOnDisk.get(0).getDiskId()));
      assertEquals("Disk state be available", HardwareState.AVAILABLE, replicasOnDisk.get(0).getDiskId().getState());
    }

    // now, shutdown the last store on each disk
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      storageManager.getStore(replicasOnDisk.get(replicasOnDisk.size() - 1).getPartitionId()).shutdown();
    }
    // verify all disks are unavailable because all stores are down
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      assertFalse("Disk should be unavailable", storageManager.isDiskAvailable(replicasOnDisk.get(0).getDiskId()));
    }

    // then, start the one store on each disk to test if disk is up again
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      storageManager.startBlobStore(replicasOnDisk.get(0).getPartitionId());
    }
    // verify all disks are available again because one store is started
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      assertTrue("Disk should be available", storageManager.isDiskAvailable(replicasOnDisk.get(0).getDiskId()));
      assertEquals("Disk state be available", HardwareState.AVAILABLE, replicasOnDisk.get(0).getDiskId().getState());
    }

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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
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
      StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
      storageManager.start();
      assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    assertTrue("File creation should have succeeded", fileSizeDir.createNewFile());
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
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
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    checkStoreAccessibility(replicas, null, storageManager);
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(0,
        getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    MockPartitionId invalidPartition = new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.<MockDataNodeId>emptyList(), 0);
    assertNull("Should not have found a store for an invalid partition.", storageManager.getStore(invalidPartition));
    assertEquals("Compaction thread count is incorrect", dataNode.getMountPaths().size(),
        TestUtils.numThreadsByThisName(CompactionManager.THREAD_NAME_PREFIX));
    verifyCompactionThreadCount(storageManager, dataNode.getMountPaths().size());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
    assertEquals("Compaction thread count is incorrect", 0, storageManager.getCompactionThreadCount());
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreShutdownFailures"));
  }

  /**
   * Test the stopped stores are correctly skipped and not started during StorageManager's startup.
   */
  @Test
  public void skipStoppedStoresTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    MockReplicaStatusDelegate replicaStatusDelegate = new MockReplicaStatusDelegate();
    replicaStatusDelegate.stoppedReplicas.add(replicas.get(0).getPartitionId().toPathString());
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, replicaStatusDelegate);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    for (int i = 0; i < replicas.size(); ++i) {
      PartitionId id = replicas.get(i).getPartitionId();
      if (i == 0) {
        assertNull("Store should be null because stopped stores will be skipped and will not be started",
            storageManager.getStore(id));
        assertFalse("Compaction should not be scheduled", storageManager.scheduleNextForCompaction(id));
      } else {
        Store store = storageManager.getStore(id);
        assertTrue("Store should be started", ((BlobStore) store).isStarted());
        assertTrue("Compaction should be scheduled", storageManager.scheduleNextForCompaction(id));
      }
    }
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Tests that unrecognized directories are reported correctly
   * @throws Exception
   */
  @Test
  public void unrecognizedDirsOnDiskTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    int extraDirsCount = 0;
    Set<String> createdMountPaths = new HashSet<>();
    for (ReplicaId replicaId : replicas) {
      if (createdMountPaths.add(replicaId.getMountPath())) {
        int count = TestUtils.RANDOM.nextInt(6) + 5;
        createFilesAndDirsAtPath(new File(replicaId.getDiskId().getMountPath()), count - 1, count);
        //  the extra files should not get reported
        extraDirsCount += count;
      }
    }
    StorageManager storageManager = createStorageManager(replicas, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be some unexpected partitions reported", extraDirsCount,
        getNumUnrecognizedPartitionsReported());
    checkStoreAccessibility(replicas, null, storageManager);
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  // helpers

  /**
   * Construct a {@link StorageManager} for the passed in set of replicas.
   * @param replicas the list of replicas for the {@link StorageManager} to use.
   * @param metricRegistry the {@link MetricRegistry} instance to use to instantiate {@link StorageManager}
   * @return a started {@link StorageManager}
   * @throws StoreException
   */
  private StorageManager createStorageManager(List<ReplicaId> replicas, MetricRegistry metricRegistry,
      ReplicaStatusDelegate replicaStatusDelegate) throws StoreException {
    return new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry, replicas,
        new MockIdFactory(), new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(), replicaStatusDelegate,
        SystemTime.getInstance());
  }

  /**
   * Shutdown a {@link StorageManager} and assert that the stores cannot be accessed for the provided replicas.
   * @param storageManager the {@link StorageManager} to shutdown.
   * @param replicas the {@link ReplicaId}s to check for store inaccessibility.
   * @throws InterruptedException
   */
  private static void shutdownAndAssertStoresInaccessible(StorageManager storageManager, List<ReplicaId> replicas)
      throws InterruptedException {
    storageManager.shutdown();
    for (ReplicaId replica : replicas) {
      assertNull(storageManager.getStore(replica.getPartitionId()));
    }
  }

  /**
   * @return the value of {@link StorageManagerMetrics#unexpectedDirsOnDisk}.
   */
  private long getNumUnrecognizedPartitionsReported() {
    return getCounterValue(metricRegistry.getCounters(), DiskManager.class.getName(), "UnexpectedDirsOnDisk");
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
    properties.put("store.replica.status.delegate.enable", "true");
    if (segmentedLog) {
      long replicaCapacity = clusterMap.getAllPartitionIds(null).get(0).getReplicaIds().get(0).getCapacityInBytes();
      properties.put("store.segment.size.in.bytes", Long.toString(replicaCapacity / 2L));
    }
    VerifiableProperties vProps = new VerifiableProperties(properties);
    diskManagerConfig = new DiskManagerConfig(vProps);
    storeConfig = new StoreConfig(vProps);
  }

  // unrecognizedDirsOnDiskTest() helpers

  /**
   * Creates {@code fileCount} files and {@code dirCount} directories at {@code dir}.
   * @param dir the directory to create the files and dirs at
   * @param fileCount the number of files to be created
   * @param dirCount the number of directories to be created
   * @return the list of files,dirs created as a pair.
   * @throws IOException
   */
  private Pair<List<File>, List<File>> createFilesAndDirsAtPath(File dir, int fileCount, int dirCount)
      throws IOException {
    List<File> createdFiles = new ArrayList<>();
    for (int i = 0; i < fileCount; i++) {
      File createdFile = new File(dir, "created-file-" + i);
      if (!createdFile.exists()) {
        assertTrue("Could not create " + createdFile, createdFile.createNewFile());
      }
      createdFile.deleteOnExit();
      createdFiles.add(createdFile);
    }
    List<File> createdDirs = new ArrayList<>();
    for (int i = 0; i < dirCount; i++) {
      File createdDir = new File(dir, "created-dir-" + i);
      assertTrue("Could not create " + createdDir + " now", createdDir.mkdir());
      createdDir.deleteOnExit();
      createdDirs.add(createdDir);
    }
    return new Pair<>(createdFiles, createdDirs);
  }

  /**
   * An extension of {@link ReplicaStatusDelegate} to help with tests.
   */
  private static class MockReplicaStatusDelegate extends ReplicaStatusDelegate {
    Set<String> stoppedReplicas = new HashSet<>();

    MockReplicaStatusDelegate() {
      super(mock(ClusterParticipant.class));
    }

    @Override
    public boolean markStopped(List<ReplicaId> replicaIds) {
      replicaIds.forEach(replicaId -> stoppedReplicas.add(replicaId.getPartitionId().toPathString()));
      return true;
    }

    @Override
    public boolean unmarkStopped(List<ReplicaId> replicaIds) {
      replicaIds.forEach(replicaId -> stoppedReplicas.remove(replicaId.getPartitionId().toPathString()));
      return true;
    }

    @Override
    public List<String> getStoppedReplicas() {
      return new ArrayList<>(stoppedReplicas);
    }
  }
}

