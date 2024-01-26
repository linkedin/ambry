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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeConfigSourceType;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.HelixAdminFactory;
import com.github.ambry.clustermap.HelixBootstrapUpgradeUtil;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.HelixFactory;
import com.github.ambry.clustermap.HelixParticipant;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockDiskId;
import com.github.ambry.clustermap.MockHelixManagerFactory;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSealStatus;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.AmbryStatsReport;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.HelixBootstrapUpgradeUtil.HelixAdminOperation.*;
import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static com.github.ambry.store.BlobStoreTest.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test {@link StorageManager} and {@link DiskManager}
 */
public class StorageManagerTest {
  private static final Random RANDOM = new Random();
  private static final String CLUSTER_NAME = "AmbryTestCluster";

  private DiskManagerConfig diskManagerConfig;
  private ClusterMapConfig clusterMapConfig;
  private StoreConfig storeConfig;
  private MockClusterMap clusterMap;
  private MetricRegistry metricRegistry;

  /**
   * Startup the {@link MockClusterMap} for a test.
   * @throws IOException
   */
  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, true, 1, 3, 3, false, false, null);
    metricRegistry = clusterMap.getMetricRegistry();
    generateConfigs(false, false);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
   * Test the case where disk failures surpass threshold to fail initialization
   * @throws Exception
   */
  @Test
  public void initializationErrorDueToDiskHealth() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<String> mountPaths = dataNode.getMountPaths();
    for (String mountPathToDelete : mountPaths) {
      Utils.deleteFileOrDirectory(new File(mountPathToDelete));
    }
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    try {
      storageManager.start();
      fail("Should fail due to disk health");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Initialization_Error, e.getErrorCode());
    }
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
   * Test add new BlobStore with given {@link ReplicaId}.
   */
  @Test
  public void addBlobStoreTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    int newMountPathIndex = 3;
    // add new MountPath to local node
    File f = File.createTempFile("ambry", ".tmp");
    File mountFile =
        new File(f.getParent(), "mountpathfile" + MockClusterMap.PLAIN_TEXT_PORT_START_NUMBER + newMountPathIndex);
    MockClusterMap.deleteFileOrDirectory(mountFile);
    assertTrue("Couldn't create mount path directory", mountFile.mkdir());
    localNode.addMountPaths(Collections.singletonList(mountFile.getAbsolutePath()));
    PartitionId newPartition1 =
        new MockPartitionId(10L, MockClusterMap.DEFAULT_PARTITION_CLASS, clusterMap.getDataNodes(), newMountPathIndex);
    StorageManager storageManager = createStorageManager(localNode, metricRegistry, null);
    storageManager.start();
    // test add store that already exists, which should fail
    assertFalse("Add store which is already existing should fail", storageManager.addBlobStore(localReplicas.get(0)));
    // test add store onto a new disk, which should succeed
    assertTrue("Add new store should succeed", storageManager.addBlobStore(newPartition1.getReplicaIds().get(0)));
    assertNotNull("The store shouldn't be null because new store is successfully added",
        storageManager.getStore(newPartition1, false));
    // test add store whose diskManager is not running, which should fail
    PartitionId newPartition2 =
        new MockPartitionId(11L, MockClusterMap.DEFAULT_PARTITION_CLASS, clusterMap.getDataNodes(), 0);
    storageManager.getDiskManager(localReplicas.get(0).getPartitionId()).shutdown();
    assertFalse("Add store onto the DiskManager which is not running should fail",
        storageManager.addBlobStore(newPartition2.getReplicaIds().get(0)));
    // Create storage manager again to create disk managers again
    storageManager.shutdown();
    storageManager = createStorageManager(localNode, metricRegistry, null);
    storageManager.start();
    // test replica addition can correctly handle existing dir (should delete it and create a new one)
    // To verify the directory has been recreated, we purposely put a test file in previous dir.
    PartitionId newPartition3 =
        new MockPartitionId(12L, MockClusterMap.DEFAULT_PARTITION_CLASS, clusterMap.getDataNodes(), 0);
    ReplicaId replicaToAdd = newPartition3.getReplicaIds().get(0);
    File previousDir = new File(replicaToAdd.getReplicaPath());
    File testFile = new File(previousDir, "testFile");
    MockClusterMap.deleteFileOrDirectory(previousDir);
    assertTrue("Cannot create dir for " + replicaToAdd.getReplicaPath(), previousDir.mkdir());
    assertTrue("Cannot create test file within previous dir", testFile.createNewFile());
    assertTrue("Adding new store should succeed", storageManager.addBlobStore(replicaToAdd));
    assertFalse("Test file should not exist", testFile.exists());
    assertNotNull("Store associated new added replica should not be null",
        storageManager.getStore(newPartition3, false));
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
    // test add store but fail to add segment requirements to DiskSpaceAllocator. (This is simulated by inducing
    // addRequiredSegments failure to make store inaccessible)
    List<String> mountPaths = localNode.getMountPaths();
    String diskToFail = mountPaths.get(0);
    File reservePoolDir = new File(diskToFail, diskManagerConfig.diskManagerReserveFileDirName);
    File storeReserveDir = new File(reservePoolDir, DiskSpaceAllocator.STORE_DIR_PREFIX + newPartition2.toPathString());
    StorageManager storageManager2 = createStorageManager(localNode, new MetricRegistry(), null);
    storageManager2.start();
    Utils.deleteFileOrDirectory(storeReserveDir);
    assertTrue("File creation should succeed", storeReserveDir.createNewFile());

    assertFalse("Add store should fail if store couldn't start due to initializePool failure",
        storageManager2.addBlobStore(newPartition2.getReplicaIds().get(0)));
    assertNull("New store shouldn't be in in-memory data structure", storageManager2.getStore(newPartition2, false));
    shutdownAndAssertStoresInaccessible(storageManager2, localReplicas);
  }

  /**
   * test that both success and failure in storage manager when replica becomes BOOTSTRAP from OFFLINE (update
   * InstanceConfig in Helix is turned off in this test)
   * @throws Exception
   */
  @Test
  public void replicaFromOfflineToBootstrapFailureTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    // 1. get listeners from Helix participant and verify there is a storageManager listener.
    Map<StateModelListenerType, PartitionStateChangeListener> listeners =
        mockHelixParticipant.getPartitionStateChangeListeners();
    assertTrue("Should contain storage manager listener",
        listeners.containsKey(StateModelListenerType.StorageManagerListener));
    // 2. if new bootstrap replica is not found, there should be an exception
    try {
      mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(String.valueOf(partitionIds.size() + 1));
      fail("should fail due to bootstrap replica not found");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaNotFound, e.getErrorCode());
    }

    // 3. test regular store didn't start up (which triggers StoreNotStarted exception)
    ReplicaId replicaId = localReplicas.get(0);
    Store localStore = storageManager.getStore(replicaId.getPartitionId(), true);
    localStore.shutdown();
    try {
      mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(replicaId.getPartitionId().toPathString());
      fail("should fail due to store not started");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StoreNotStarted, e.getErrorCode());
    }
    localStore.start();

    // 4. test both failure and success cases regarding new replica addition
    PartitionId newPartition = clusterMap.createNewPartition(Collections.singletonList(localNode));
    assertNull("There should not be any store associated with new partition",
        storageManager.getStore(newPartition, true));
    // find an existing replica that shares disk with new replica
    ReplicaId newReplica = newPartition.getReplicaIds().get(0);
    ReplicaId replicaOnSameDisk =
        localReplicas.stream().filter(r -> r.getDiskId().equals(newReplica.getDiskId())).findFirst().get();
    // test add new store failure by shutting down target diskManager
    storageManager.getDiskManager(replicaOnSameDisk.getPartitionId()).shutdown();
    try {
      mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
      fail("should fail due to disk is down");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaOperationFailure, e.getErrorCode());
    }

    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  @Test
  public void replicaFromOfflineToBootstrapSuccessTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();

    // 0. get listeners from Helix participant and verify there is a storageManager listener.
    Map<StateModelListenerType, PartitionStateChangeListener> listeners =
        mockHelixParticipant.getPartitionStateChangeListeners();
    assertTrue("Should contain storage manager listener",
        listeners.containsKey(StateModelListenerType.StorageManagerListener));

    // 1. Test case where new replica(store) is successfully added into StorageManager
    PartitionId newPartition = clusterMap.createNewPartition(Collections.singletonList(localNode));
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
    BlobStore newAddedStore = (BlobStore) storageManager.getStore(newPartition);
    assertNotNull("There should be a started store associated with new partition", newAddedStore);
    // verify that new added store has bootstrap file
    assertTrue("There should be a bootstrap file indicating store is in BOOTSTRAP state",
        newAddedStore.isBootstrapInProgress());
    assertEquals("The store's current state should be BOOTSTRAP", ReplicaState.BOOTSTRAP,
        newAddedStore.getCurrentState());

    // 2. test that state transition should succeed for existing non-empty replicas (we write some data into store beforehand)
    MockId id = new MockId(TestUtils.getRandomString(MOCK_ID_STRING_LENGTH), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM));
    MessageInfo info =
        new MessageInfo(id, PUT_RECORD_SIZE, id.getAccountId(), id.getContainerId(), Utils.Infinite_Time);
    MessageWriteSet writeSet = new MockMessageWriteSet(Collections.singletonList(info),
        Collections.singletonList(ByteBuffer.allocate(PUT_RECORD_SIZE)));
    Store storeToWrite = storageManager.getStore(localReplicas.get(1).getPartitionId());
    storeToWrite.put(writeSet);
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(localReplicas.get(1).getPartitionId().toPathString());
    assertFalse("There should not be any bootstrap file for existing non-empty store",
        storeToWrite.isBootstrapInProgress());
    assertEquals("The store's current state should be BOOTSTRAP", ReplicaState.BOOTSTRAP,
        storeToWrite.getCurrentState());

    // 3. test that for new created (empty) store, state transition puts it into BOOTSTRAP state
    ReplicaId replicaId = localReplicas.get(0);
    Store localStore = storageManager.getStore(replicaId.getPartitionId(), true);
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(localReplicas.get(0).getPartitionId().toPathString());
    assertTrue("There should be a bootstrap file because store is empty and probably recreated",
        localStore.isBootstrapInProgress());
    assertEquals("The store's current state should be BOOTSTRAP", ReplicaState.BOOTSTRAP, localStore.getCurrentState());

    // 4. test that when an existing store is already leader or standby, state transition is not going to change it
    // back to BOOTSTRAP
    storageManager.getStore(localReplicas.get(0).getPartitionId()).setCurrentState(ReplicaState.STANDBY);
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(localReplicas.get(0).getPartitionId().toPathString());
    assertEquals("The store's current state should be STANDBY", ReplicaState.STANDBY, localStore.getCurrentState());
    storageManager.getStore(localReplicas.get(0).getPartitionId()).setCurrentState(ReplicaState.LEADER);
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(localReplicas.get(0).getPartitionId().toPathString());
    assertEquals("The store's current state should be LEADER", ReplicaState.LEADER, localStore.getCurrentState());

    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * test both success and failure cases during STANDBY -> INACTIVE transition
   */
  @Test
  public void replicaFromStandbyToInactiveTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    // 1. get listeners from Helix participant and verify there is a storageManager listener.
    Map<StateModelListenerType, PartitionStateChangeListener> listeners =
        mockHelixParticipant.getPartitionStateChangeListeners();
    assertTrue("Should contain storage manager listener",
        listeners.containsKey(StateModelListenerType.StorageManagerListener));
    // 2. not found replica should encounter exception
    try {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby("-1");
      fail("should fail because replica is not found");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaNotFound, e.getErrorCode());
    }
    // 3. not found store should throw exception (induced by removing the store)
    ReplicaId replicaToRemove = localReplicas.get(localReplicas.size() - 1);
    storageManager.controlCompactionForBlobStore(replicaToRemove.getPartitionId(), false);
    storageManager.shutdownBlobStore(replicaToRemove.getPartitionId());
    storageManager.getDiskManager(replicaToRemove.getPartitionId()).removeBlobStore(replicaToRemove.getPartitionId());
    try {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby(replicaToRemove.getPartitionId().toPathString());
      fail("should fail because store is not found");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaNotFound, e.getErrorCode());
    }
    // 4. store not started exception
    ReplicaId localReplica = localReplicas.get(0);
    storageManager.shutdownBlobStore(localReplica.getPartitionId());
    try {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby(localReplica.getPartitionId().toPathString());
      fail("should fail because store is not started");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StoreNotStarted, e.getErrorCode());
    }
    storageManager.startBlobStore(localReplica.getPartitionId());
    // 5. store is disabled due to disk I/O error
    BlobStore localStore = (BlobStore) storageManager.getStore(localReplica.getPartitionId());
    localStore.setDisableState(true);
    try {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby(localReplica.getPartitionId().toPathString());
      fail("should fail because store is disabled");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaOperationFailure, e.getErrorCode());
    }
    localStore.setDisableState(false);
    // 6. success case (verify both replica's state and decommission file)
    mockHelixParticipant.onPartitionBecomeInactiveFromStandby(localReplica.getPartitionId().toPathString());
    assertEquals("local store state should be set to INACTIVE", ReplicaState.INACTIVE,
        storageManager.getStore(localReplica.getPartitionId()).getCurrentState());
    File decommissionFile = new File(localReplica.getReplicaPath(), BlobStore.DECOMMISSION_FILE_NAME);
    assertTrue("Decommission file is not found in local replica's dir", decommissionFile.exists());
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);

    // 7. mock disable compaction failure
    mockHelixParticipant = new MockClusterParticipant();
    MockStorageManager mockStorageManager =
        new MockStorageManager(localNode, Collections.singletonList(mockHelixParticipant));
    mockStorageManager.start();
    try {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby(localReplica.getPartitionId().toPathString());
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaNotFound, e.getErrorCode());
    } finally {
      shutdownAndAssertStoresInaccessible(mockStorageManager, localReplicas);
    }
  }

  /**
   * Test Offline-To-Dropped transition.
   * @throws Exception
   */
  @Test
  public void replicaFromOfflineToDroppedTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    ReplicaId testReplica = localReplicas.get(0);
    MockClusterParticipant mockHelixParticipant = Mockito.spy(new MockClusterParticipant());
    doNothing().when(mockHelixParticipant).setPartitionDisabledState(anyString(), anyBoolean());
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    storageManager.controlCompactionForBlobStore(testReplica.getPartitionId(), false);
    CountDownLatch participantLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      mockHelixParticipant.onPartitionBecomeDroppedFromOffline(testReplica.getPartitionId().toPathString());
      participantLatch.countDown();
    }, false).start();
    assertTrue("Helix participant transition didn't get invoked within 1 sec",
        participantLatch.await(1, TimeUnit.SECONDS));
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * Test disk free space is decreased after replica is added.
   */
  @Test
  public void updateDiskSpaceOnReplicaAdditionTest() throws InterruptedException, StoreException {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    PartitionId newPartition = clusterMap.createNewPartition(Collections.singletonList(localNode));
    // Get the replica that has been created for this new partition
    ReplicaId newReplica = newPartition.getReplicaIds()
        .stream()
        .filter(replicaId -> replicaId.getDataNodeId().equals(localNode))
        .findFirst()
        .get();
    // 1. Get disk space before adding the replica
    long diskSpaceBefore = newReplica.getDiskId().getAvailableSpaceInBytes();
    // 2. Induce state transition to add a replica
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
    // 3. Verify disk space is reduced after adding the replica
    assertEquals("Disk space should be reduced after replica addition",
        diskSpaceBefore - newReplica.getCapacityInBytes(), newReplica.getDiskId().getAvailableSpaceInBytes());
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * Test disk free space is increased after replica is removed.
   */
  @Test
  public void updateDiskSpaceOnReplicaRemovalTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    ReplicaId oldReplica = localReplicas.get(0);
    MockClusterParticipant mockHelixParticipant = Mockito.spy(new MockClusterParticipant());
    doNothing().when(mockHelixParticipant).setPartitionDisabledState(anyString(), anyBoolean());
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    // Get disk space before dropping the replica
    MockDiskId diskId = (MockDiskId) oldReplica.getDiskId();
    long originalSpace = diskId.getAvailableSpaceInBytes();
    storageManager.controlCompactionForBlobStore(oldReplica.getPartitionId(), false);
    storageManager.shutdownBlobStore(oldReplica.getPartitionId());
    mockHelixParticipant.onPartitionBecomeDroppedFromOffline(oldReplica.getPartitionId().toPathString());
    // Verify disk space is increased on removing replica
    assertEquals("Disk free space should be increased after removing replica",
        originalSpace + oldReplica.getCapacityInBytes(), oldReplica.getDiskId().getAvailableSpaceInBytes());
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * Test failure cases when updating InstanceConfig in Helix for both Offline-To-Bootstrap and Offline-To-Dropped.
   */
  @Test
  public void updateInstanceConfigFailureTest() throws Exception {
    generateConfigs(true, true);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    // create a new partition and get its replica on local node
    PartitionId newPartition = clusterMap.createNewPartition(Collections.singletonList(localNode));
    // override return value of updateDataNodeInfoInCluster() to mock update InstanceConfig failure
    mockHelixParticipant.updateNodeInfoReturnVal = false;
    try {
      mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
      fail("should fail because updating InstanceConfig didn't succeed during Offline-To-Bootstrap");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StateTransitionException.TransitionErrorCode.HelixUpdateFailure,
          e.getErrorCode());
    }
    try {
      mockHelixParticipant.onPartitionBecomeDroppedFromOffline(localReplicas.get(0).getPartitionId().toPathString());
      fail("should fail because updating InstanceConfig didn't succeed during Offline-To-Dropped");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StateTransitionException.TransitionErrorCode.HelixUpdateFailure,
          e.getErrorCode());
    }
    mockHelixParticipant.updateNodeInfoReturnVal = null;
    // mock InstanceConfig not found error (note that MockHelixAdmin is empty by default, so no InstanceConfig is present)
    newPartition = clusterMap.createNewPartition(Collections.singletonList(localNode));
    try {
      mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
      fail("should fail because InstanceConfig is not found during Offline-To-Bootstrap");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StateTransitionException.TransitionErrorCode.HelixUpdateFailure,
          e.getErrorCode());
    }
    try {
      mockHelixParticipant.onPartitionBecomeDroppedFromOffline(localReplicas.get(1).getPartitionId().toPathString());
      fail("should fail because InstanceConfig is not found during Offline-To-Dropped");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StateTransitionException.TransitionErrorCode.HelixUpdateFailure,
          e.getErrorCode());
    }
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * Test success case when updating InstanceConfig in Helix after new replica is added in storage manager.
   */
  @Test
  public void updateInstanceConfigSuccessTest() throws Exception {
    generateConfigs(true, true);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    // create a new partition and get its replica on local node
    PartitionId newPartition = clusterMap.createNewPartition(Collections.singletonList(localNode));
    ReplicaId newReplica = newPartition.getReplicaIds().get(0);
    // for updating instanceConfig test, we first add an empty InstanceConfig of current node
    String instanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(localNode.getHostname());
    instanceConfig.setPort(Integer.toString(localNode.getPort()));
    // for current test, we initial InstanceConfig empty, non-empty case will be tested in HelixParticipantTest
    Map<String, Map<String, String>> diskInfos = new HashMap<>();
    instanceConfig.getRecord().setMapFields(diskInfos);
    HelixAdmin helixAdmin = mockHelixParticipant.getHelixAdmin();
    helixAdmin.addCluster(CLUSTER_NAME);
    helixAdmin.addInstance(CLUSTER_NAME, instanceConfig);
    // test success case
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
    instanceConfig = helixAdmin.getInstanceConfig(CLUSTER_NAME, instanceName);
    // verify that new replica info is present in InstanceConfig
    Map<String, Map<String, String>> mountPathToDiskInfos = instanceConfig.getRecord().getMapFields();
    Map<String, String> diskInfo = mountPathToDiskInfos.get(newReplica.getMountPath());
    String replicasStr = diskInfo.get("Replicas");
    Set<String> partitionStrs = new HashSet<>();
    for (String replicaInfo : replicasStr.split(",")) {
      String[] infos = replicaInfo.split(":");
      partitionStrs.add(infos[0]);
    }
    assertTrue("New replica info is not found in InstanceConfig", partitionStrs.contains(newPartition.toPathString()));
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
    Store store = storageManager.getStore(id, false);
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
   * Test remove blob store with given {@link PartitionId}
   * @throws Exception
   */
  @Test
  public void removeBlobStoreTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<MockDataNodeId> dataNodes = new ArrayList<>();
    dataNodes.add(dataNode);
    MockPartitionId invalidPartition =
        new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS, dataNodes, 0);
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    // shut down replica[1] ~ replica[size - 2]. The replica[0] will be used to test removing store that disk is not running
    // Replica[1] will be used to test removing a started store. Replica[2] will be used to test a store with compaction enabled
    for (int i = 3; i < replicas.size(); i++) {
      ReplicaId replica = replicas.get(i);
      PartitionId id = replica.getPartitionId();
      assertTrue("Disable compaction should succeed", storageManager.controlCompactionForBlobStore(id, false));
      assertTrue("Shutdown should succeed on given store", storageManager.shutdownBlobStore(id));
      assertTrue("Removing store should succeed", storageManager.removeBlobStore(id));
      assertNull("The store should not exist", storageManager.getStore(id, false));
    }
    // test remove store that compaction is still enabled on it, even though it is shutdown
    PartitionId id = replicas.get(2).getPartitionId();
    assertTrue("Shutdown should succeed on given store", storageManager.shutdownBlobStore(id));
    assertFalse("Removing store should fail because compaction is enabled on this store",
        storageManager.removeBlobStore(id));
    // test remove store that is still started
    id = replicas.get(1).getPartitionId();
    assertFalse("Removing store should fail because store is still started", storageManager.removeBlobStore(id));
    // test remove store that the disk manager is not running
    id = replicas.get(0).getPartitionId();
    storageManager.getDiskManager(id).shutdown();
    assertFalse("Removing store should fail because disk manager is not running", storageManager.removeBlobStore(id));
    // test a store that doesn't exist
    assertFalse("Removing not-found store should return false", storageManager.removeBlobStore(invalidPartition));
    shutdownAndAssertStoresInaccessible(storageManager, replicas);

    // test that remove store when compaction executor is not instantiated
    // by default, storeCompactionTriggers = "" which makes compaction executor = null during initialization
    VerifiableProperties vProps = new VerifiableProperties(new Properties());
    storageManager =
        new StorageManager(new StoreConfig(vProps), diskManagerConfig, Utils.newScheduler(1, false), metricRegistry,
            new MockIdFactory(), clusterMap, dataNode, new DummyMessageStoreHardDelete(), null,
            SystemTime.getInstance(), new DummyMessageStoreRecovery(), new InMemAccountService(false, false));
    storageManager.start();
    for (ReplicaId replica : replicas) {
      id = replica.getPartitionId();
      assertTrue("Disable compaction should succeed", storageManager.controlCompactionForBlobStore(id, false));
      assertTrue("Shutdown should succeed on given store", storageManager.shutdownBlobStore(id));
      assertTrue("Removing store should succeed", storageManager.removeBlobStore(id));
      assertNull("The store should not exist", storageManager.getStore(id, false));
    }
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test setting blob stop state in two clusters (if server participates into two Helix clusters)
   * @throws Exception
   */
  @Test
  public void setBlobStoreStoppedStateWithMultiDelegatesTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    MockClusterParticipant mockClusterParticipant1 = new MockClusterParticipant();
    MockClusterParticipant mockClusterParticipant2 = new MockClusterParticipant(null, false);
    List<ClusterParticipant> participants = Arrays.asList(mockClusterParticipant1, mockClusterParticipant2);
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, participants);
    storageManager.start();
    PartitionId id = replicas.get(0).getPartitionId();
    // test that any delegate fails to update stop state, then the whole operation fails
    List<PartitionId> failToUpdateList = storageManager.setBlobStoreStoppedState(Collections.singletonList(id), true);
    assertEquals("Set store stopped state should fail because one of delegates returns false", id,
        failToUpdateList.get(0));
    // test the success case, both delegates succeed in updating stop state of replica
    mockClusterParticipant2.setStopStateReturnVal = null;
    failToUpdateList = storageManager.setBlobStoreStoppedState(Collections.singletonList(id), true);
    assertTrue("Set store stopped state should succeed", failToUpdateList.isEmpty());
    // verify both delegates have the correct stopped replica list.
    List<String> expectedStoppedReplicas = Collections.singletonList(id.toPathString());
    assertEquals("Stopped replica list from participant 1 is not expected", expectedStoppedReplicas,
        mockClusterParticipant1.getStoppedReplicas());
    assertEquals("Stopped replica list from participant 2 is not expected", expectedStoppedReplicas,
        mockClusterParticipant2.getStoppedReplicas());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test that, if store is not started, all participants on this node are able to mark it in ERROR state during
   * OFFLINE -> BOOTSTRAP transition.
   * @throws Exception
   */
  @Test
  public void multiParticipantsMarkStoreInErrorStateTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<ClusterParticipant> participants = Arrays.asList(new MockClusterParticipant(), new MockClusterParticipant());
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, participants);
    storageManager.start();
    // stop one of the stores to induce transition failure
    PartitionId id = replicas.get(0).getPartitionId();
    storageManager.shutdownBlobStore(id);
    // verify that both participants throw exception during OFFLINE -> BOOTSTRAP transition
    for (ClusterParticipant participant : participants) {
      try {
        ((MockClusterParticipant) participant).onPartitionBecomeBootstrapFromOffline(id.toPathString());
        fail("should fail because store is not started");
      } catch (StateTransitionException e) {
        assertEquals("Error code doesn't match", StoreNotStarted, e.getErrorCode());
      }
    }
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
    // test setting the state of store via instantiated MockClusterParticipant
    ClusterParticipant participant = new MockClusterParticipant();
    ClusterParticipant participantSpy = Mockito.spy(participant);
    StorageManager storageManager =
        createStorageManager(dataNode, metricRegistry, Collections.singletonList(participantSpy));
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
    Set<String> stoppedReplicasCopy = new HashSet<>(participantSpy.getStoppedReplicas());
    for (ReplicaId replica : replicas) {
      assertTrue("The stopped list should contain the replica: " + replica.getPartitionId().toPathString(),
          stoppedReplicasCopy.contains(replica.getPartitionId().toPathString()));
    }
    // make sure replicaStatusDelegate is invoked 3 times and each time the input replica list conforms with stores on particular disk
    for (List<ReplicaId> replicasPerDisk : diskToReplicas.values()) {
      verify(participantSpy, times(1)).setReplicaStoppedState(replicasPerDisk, true);
    }

    // remove a list of stores from STOPPED list. Note that the stores are residing on 3 disks.
    storageManager.setBlobStoreStoppedState(partitionIds, false);
    // make sure the update operation succeeds
    assertTrue("Remove stores from stopped list should succeed, failToUpdateList should be empty",
        failToUpdateList.isEmpty());
    // make sure the stopped list is empty because all the stores are successfully removed.
    assertTrue("The stopped list should be empty after removing all stores",
        participantSpy.getStoppedReplicas().isEmpty());
    // make sure replicaStatusDelegate is invoked 3 times and each time the input replica list conforms with stores on particular disk
    for (List<ReplicaId> replicasPerDisk : diskToReplicas.values()) {
      verify(participantSpy, times(1)).setReplicaStoppedState(replicasPerDisk, false);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    for (ReplicaId replica : replicas) {
      diskToReplicas.computeIfAbsent(replica.getDiskId(), disk -> new ArrayList<>()).add(replica);
    }
    // for each disk, shutdown all the stores except for the last one
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      for (int i = 0; i < replicasOnDisk.size() - 1; ++i) {
        storageManager.getStore(replicasOnDisk.get(i).getPartitionId(), false).shutdown();
      }
    }
    // verify all disks are still available because at least one store on them is up
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      assertTrue("Disk should be available", storageManager.isDiskAvailable(replicasOnDisk.get(0).getDiskId()));
      assertEquals("Disk state be available", HardwareState.AVAILABLE, replicasOnDisk.get(0).getDiskId().getState());
    }

    // now, shutdown the last store on each disk
    for (List<ReplicaId> replicasOnDisk : diskToReplicas.values()) {
      storageManager.getStore(replicasOnDisk.get(replicasOnDisk.size() - 1).getPartitionId(), false).shutdown();
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
        assertNull("This store should not be accessible.", storageManager.getStore(id, false));
        assertFalse("Compaction should not be scheduled", storageManager.scheduleNextForCompaction(id));
      } else {
        Store store = storageManager.getStore(id, false);
        assertTrue("Store should be started", store.isStarted());
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
   * Test the case where the blob store can't get started and we need to remove the directory and then restart the blobstore.
   * @throws Exception
   */
  @Test
  public void storeRemoveDirectoryAndRestartTest() throws Exception {
    // Make sure that we use segmented log files
    generateConfigs(true, false);
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    ReplicaId replica = clusterMap.getReplicaIds(dataNode).get(0);
    ReplicaId goodReplica = clusterMap.getReplicaIds(dataNode).get(1);
    // Just start the storage manager and this will make sure all the replicas' directories are created
    // and the first log segment file is also created for each replica.
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    // Both replicas should have blobstore returned
    assertNotNull(storageManager.getStore(replica.getPartitionId(), false));
    assertNotNull(storageManager.getStore(goodReplica.getPartitionId(), false));

    File file = getFileLogSegment(replica);
    assertTrue("First log segment file should exist " + file.getAbsolutePath(), file.exists());

    // Now shutdown the storage manager and modify the first log segment by truncating the size to half.
    // This will break the log segment file header
    storageManager.shutdown();
    corruptFirstLogSegment(replica);

    // Now restart the storage manager
    storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    assertEquals(1,
        getCounterValue(metricRegistry.getCounters(), DiskManager.class.getName(), "TotalStoreStartFailures"));
    // bad replica is not started, but the good replica is
    assertNull(storageManager.getStore(replica.getPartitionId(), false));
    assertNotNull(storageManager.getStore(goodReplica.getPartitionId(), false));

    storageManager.shutdown();

    // Now enable the feature to remove directory and restart blob store
    generateConfigs(true, false, false, 2, true);
    storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();

    // Getting blob stores for both replicas should both result in not null
    assertNotNull(storageManager.getStore(replica.getPartitionId(), false));
    assertNotNull(storageManager.getStore(goodReplica.getPartitionId(), false));
    storageManager.shutdown();
  }

  @Test
  public void storeRemoveDirectoryAndRestartTestWithDiskFailure() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<String> mountPaths = dataNode.getMountPaths();
    String badDiskMountPath = mountPaths.get(RANDOM.nextInt(mountPaths.size()));
    List<ReplicaId> badReplicas =
        replicas.stream().filter(r -> r.getMountPath().equals(badDiskMountPath)).collect(Collectors.toList());

    generateConfigs(true, false, false, 2, true);
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    storageManager.shutdown();

    // Now corrupt all the replicas on the disk except for the last one
    // So we will remove the bad directories and restart blob stores
    for (ReplicaId replica : badReplicas.subList(0, badReplicas.size() - 1)) {
      corruptFirstLogSegment(replica);
    }
    storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    // All replicas should have its blob store
    for (ReplicaId replica : badReplicas) {
      assertNotNull(storageManager.getStore(replica.getPartitionId(), false));
    }
    storageManager.shutdown();

    // Now corrupt all the replicas on the disk
    for (ReplicaId replica : badReplicas) {
      corruptFirstLogSegment(replica);
    }
    storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    // All replicas should not have its blob store
    for (ReplicaId replica : badReplicas) {
      assertNull(storageManager.getStore(replica.getPartitionId(), false));
    }
    storageManager.shutdown();
  }

  @Test
  public void storeRemoveDirectoryAndRestartTestWithStoppedReplica() throws Exception {
    generateConfigs(true, false, false, 2, true);
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    ReplicaId replica = clusterMap.getReplicaIds(dataNode).get(0);

    // Start a storage manager so the replica's first log segment would be created
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    storageManager.shutdown();

    ClusterParticipant mockParticipant = new MockClusterParticipant();
    mockParticipant.setReplicaStoppedState(Collections.singletonList(replica), true);
    // The first log segment is corrupted, but this replica is also in stopped replicas list, it will not be restarted
    corruptFirstLogSegment(replica);
    storageManager = createStorageManager(dataNode, metricRegistry, Collections.singletonList(mockParticipant));
    storageManager.start();
    assertNull(storageManager.getStore(replica.getPartitionId(), false));
    storageManager.shutdown();
  }

  @Test
  public void storeRemoveDirectoryAndRestartTestWithOtherError() throws Exception {
    generateConfigs(true, false, false, 2, true);
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    ReplicaId replica = clusterMap.getReplicaIds(dataNode).get(0);

    // Start a storage manager so the replica's first log segment would be created
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    storageManager.shutdown();

    // Now find the lock file in the blob store and lock this file
    FileLock fileLock = new FileLock(new File(replica.getReplicaPath(), BlobStore.LockFile));
    Assert.assertTrue(fileLock.tryLock());

    // This file is locked already, replica won't be started
    storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    assertNull(storageManager.getStore(replica.getPartitionId(), false));
    storageManager.shutdown();
    fileLock.destroy();
  }

  /**
   * Test that stores on a disk are inaccessible if the {@link DiskSpaceAllocator} fails to start.
   * @throws Exception
   */
  @Test
  public void diskSpaceAllocatorTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<String> mountPaths = dataNode.getMountPaths();
    Map<String, List<ReplicaId>> replicasByMountPath = new HashMap<>();
    for (ReplicaId replica : replicas) {
      replicasByMountPath.computeIfAbsent(replica.getMountPath(), key -> new ArrayList<>()).add(replica);
    }

    // Test that StorageManager starts correctly when segments are created in the reserve pool.
    // Startup/shutdown one more time to verify the restart scenario.
    for (int i = 0; i < 2; i++) {
      metricRegistry = new MetricRegistry();
      StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
      storageManager.start();
      assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
      checkStoreAccessibility(replicas, null, storageManager);
      Map<String, Counter> counters = metricRegistry.getCounters();
      assertEquals(0,
          getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
      assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
      assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
      for (String mountPath : dataNode.getMountPaths()) {
        List<ReplicaId> replicasOnDisk = replicasByMountPath.get(mountPath);
        DiskSpaceAllocatorTest.ExpectedState expectedState = new DiskSpaceAllocatorTest.ExpectedState();
        // There should be 1 unallocated segment per replica on a mount path (each replica can have 2 segments) and the
        // swap segments.
        expectedState.addSwapSeg(storeConfig.storeSegmentSizeInBytes, 1);
        for (ReplicaId replica : replicasOnDisk) {
          expectedState.addStoreSeg(replica.getPartitionId().toPathString(), storeConfig.storeSegmentSizeInBytes, 1);
        }
        DiskSpaceAllocatorTest.verifyPoolState(new File(mountPath, diskManagerConfig.diskManagerReserveFileDirName),
            expectedState);
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
    File storeReserveDir = new File(reservePoolDir,
        DiskSpaceAllocator.STORE_DIR_PREFIX + replicasByMountPath.get(diskToFail)
            .get(0)
            .getPartitionId()
            .toPathString());
    File fileSizeDir =
        new File(storeReserveDir, DiskSpaceAllocator.generateFileSizeDirName(storeConfig.storeSegmentSizeInBytes));
    Utils.deleteFileOrDirectory(fileSizeDir);
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    checkStoreAccessibility(replicas, null, storageManager);
    Map<String, Counter> counters = metricRegistry.getCounters();
    assertEquals(0,
        getCounterValue(counters, DiskSpaceAllocator.class.getName(), "DiskSpaceAllocatorInitFailureCount"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "TotalStoreStartFailures"));
    assertEquals(0, getCounterValue(counters, DiskManager.class.getName(), "DiskMountPathFailures"));
    MockPartitionId invalidPartition =
        new MockPartitionId(Long.MAX_VALUE, MockClusterMap.DEFAULT_PARTITION_CLASS, Collections.emptyList(), 0);
    assertNull("Should not have found a store for an invalid partition.",
        storageManager.getStore(invalidPartition, false));
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
    ClusterParticipant mockParticipant = new MockClusterParticipant();
    mockParticipant.setReplicaStoppedState(Collections.singletonList(replicas.get(0)), true);
    StorageManager storageManager =
        createStorageManager(dataNode, metricRegistry, Collections.singletonList(mockParticipant));
    storageManager.start();
    assertEquals("There should be no unexpected partitions reported", 0, getNumUnrecognizedPartitionsReported());
    for (int i = 0; i < replicas.size(); ++i) {
      PartitionId id = replicas.get(i).getPartitionId();
      if (i == 0) {
        assertNull("Store should be null because stopped stores will be skipped and will not be started",
            storageManager.getStore(id, false));
        assertFalse("Compaction should not be scheduled", storageManager.scheduleNextForCompaction(id));
      } else {
        Store store = storageManager.getStore(id, false);
        assertTrue("Store should be started", store.isStarted());
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
    Set<String> createdMountPaths = new HashSet<>();
    Set<File> extraDirs = new HashSet<>();
    for (ReplicaId replicaId : replicas) {
      if (createdMountPaths.add(replicaId.getMountPath())) {
        int count = TestUtils.RANDOM.nextInt(6) + 5;
        Pair<List<File>, List<File>> filesAndDirectories =
            createFilesAndDirsAtPath(new File(replicaId.getDiskId().getMountPath()), count - 1, count);
        //  the extra files should not get reported
        extraDirs.addAll(filesAndDirectories.getSecond());
      }
    }
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be some unexpected partitions reported", extraDirs.size(),
        getNumUnrecognizedPartitionsReported());
    checkStoreAccessibility(replicas, null, storageManager);
    for (File extraDir : extraDirs) {
      Assert.assertTrue("Directory" + extraDir.getAbsolutePath() + " shouldn't be deleted", extraDir.exists());
    }
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * @throws Exception
   */
  @Test
  public void unrecognizedDirsRemovalTest() throws Exception {
    generateConfigs(true, false, true, 2, false);
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    try {
      Set<File> extraDirs = new HashSet<>();
      Set<File> extraReservedDirs = new HashSet<>();
      List<String> mountPaths = dataNode.getMountPaths();
      // Use a huge partition id, so it doesn't overlap with existing partition ids
      int extraPartitionId = 10000;
      for (String mountPath : mountPaths) {
        // Create two extra partitions
        for (int i = 0; i < 2; i++) {
          String partitionName = String.valueOf(extraPartitionId);

          File partitionDir = new File(mountPath, partitionName);
          assertTrue("Could not create " + partitionDir + " now", partitionDir.mkdir());
          partitionDir.deleteOnExit();
          extraDirs.add(partitionDir);

          File reservedDir = Paths.get(mountPath, diskManagerConfig.diskManagerReserveFileDirName,
              DiskSpaceAllocator.STORE_DIR_PREFIX + partitionName).toFile();
          assertTrue("Could not create " + reservedDir + " now", reservedDir.mkdirs());
          System.out.println("Created reversed directory " + reservedDir.getAbsolutePath());
          reservedDir.deleteOnExit();
          extraReservedDirs.add(reservedDir);

          extraPartitionId++;
        }
      }
      clusterMap.shouldDataNodeBeInFullAuto(false);
      // First make sure we don't delete directories when the current node is not in FULL AUTO
      StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
      storageManager.start();
      assertEquals("There should be some unexpected partitions reported", extraDirs.size(),
          getNumUnrecognizedPartitionsReported());
      checkStoreAccessibility(replicas, null, storageManager);
      for (File extraDir : extraDirs) {
        Assert.assertTrue("Directory" + extraDir.getAbsolutePath() + " shouldn't be deleted", extraDir.exists());
      }
      // reserved directories would be removed anyway
      for (File extraReservedDir : extraReservedDirs) {
        Assert.assertFalse("Directory" + extraReservedDir.getAbsolutePath() + " should be deleted",
            extraReservedDir.exists());
      }
      storageManager.shutdown();

      // Second, make sure we delete directories when the current node is in FULL AUTO
      clusterMap.shouldDataNodeBeInFullAuto(true);
      storageManager = createStorageManager(dataNode, metricRegistry, null);
      storageManager.start();
      assertEquals("There should be some unexpected partitions reported", extraDirs.size(),
          getNumUnrecognizedPartitionsReported());
      checkStoreAccessibility(replicas, null, storageManager);
      for (File extraDir : extraDirs) {
        Assert.assertFalse("Directory" + extraDir.getAbsolutePath() + " should be deleted", extraDir.exists());
      }
      for (File extraReservedDir : extraReservedDirs) {
        Assert.assertFalse("Directory" + extraReservedDir.getAbsolutePath() + " should be deleted",
            extraReservedDir.exists());
      }
      shutdownAndAssertStoresInaccessible(storageManager, replicas);
    } finally {
      clusterMap.shouldDataNodeBeInFullAuto(false);
    }
  }

  /**
   * Test that residual directory associated with removed replica is deleted correctly during OFFLINE -> DROPPED transition.
   * @throws Exception
   */
  @Test
  public void residualDirDeletionTest() throws Exception {
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = Mockito.spy(new MockClusterParticipant());
    doNothing().when(mockHelixParticipant).setPartitionDisabledState(anyString(), anyBoolean());
    // create an extra store dir at one of the mount paths
    String mountPath = replicas.get(0).getMountPath();
    String extraPartitionName = "1000";
    File extraStoreDir = new File(mountPath, extraPartitionName);
    assertTrue("Can't create an extra store dir", extraStoreDir.mkdir());
    StorageManager storageManager =
        createStorageManager(localNode, metricRegistry, Collections.singletonList(mockHelixParticipant));
    storageManager.start();
    // failure case: IOException when deleting store dir
    File invalidDir = new File(extraStoreDir.getAbsolutePath(), "invalidDir");
    invalidDir.deleteOnExit();
    assertTrue("Couldn't create dir within store dir", invalidDir.mkdir());
    assertTrue("Could not make unreadable", invalidDir.setReadable(false));
    try {
      mockHelixParticipant.onPartitionBecomeDroppedFromOffline(extraPartitionName);
      fail("should fail because there is IOException when deleting store dir");
    } catch (StateTransitionException e) {
      assertEquals("Error code is not expected", ReplicaOperationFailure, e.getErrorCode());
    }
    assertTrue("Could not make readable", invalidDir.setReadable(true));
    // trigger OFFLINE -> DROPPED transition on extra partition. Storage manager should delete residual store dir.
    mockHelixParticipant.onPartitionBecomeDroppedFromOffline(extraPartitionName);
    verify(mockHelixParticipant).setPartitionDisabledState(extraPartitionName, false);
    assertFalse("Extra store dir should not exist", extraStoreDir.exists());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test bootstrap retries in Full auto mode
   * @throws Exception
   */
  @Test
  public void replicaFromOfflineToBootstrapFailureRetryTest() throws Exception {
    generateConfigs(true, false);
    MockClusterMap spyClusterMap = spy(clusterMap);
    MockDataNodeId localNode = spyClusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = spyClusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry,
            new MockIdFactory(), spyClusterMap, localNode, new DummyMessageStoreHardDelete(),
            Collections.singletonList(mockHelixParticipant), SystemTime.getInstance(), new DummyMessageStoreRecovery(),
            new InMemAccountService(false, false));
    storageManager.start();

    // 0. Mock the cluster to be in Full auto
    doReturn(true).when(spyClusterMap).isDataNodeInFullAutoMode(any());

    // 1. Create "newReplica1" and shutdown its disk
    PartitionId newPartition1 = spyClusterMap.createNewPartition(Collections.singletonList(localNode), 0);
    ReplicaId newReplica1 = newPartition1.getReplicaIds().get(0);
    ReplicaId replicaOnSameDisk =
        localReplicas.stream().filter(r -> r.getDiskId().equals(newReplica1.getDiskId())).findFirst().get();
    storageManager.getDiskManager(replicaOnSameDisk.getPartitionId()).shutdown();

    // 2. Create "newReplica2" which has disk running
    PartitionId newPartition2 = spyClusterMap.createNewPartition(Collections.singletonList(localNode), 1);
    ReplicaId newReplica2 = newPartition2.getReplicaIds().get(0);

    // 3. Return "newReplica1" on 1st attempt and "newReplica2" on 2nd attempt
    doReturn(newReplica1, newReplica2).when(spyClusterMap).getBootstrapReplica(any(), any());

    // 4. Invoke bootstrap ST. It should pass on 2nd attempt.
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition1.toPathString());

    // 5. Verify getBootstrap replica is called 2 times
    verify(spyClusterMap, times(2)).getBootstrapReplica(anyString(), any());

    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * Test when a bootstrap replica can't be added to a disk, storage manager would clean up all the temporary files
   * @throws Exception
   */
  @Test
  public void replicaFromOfflineToBootstrapFailureRetryWithDiskSpaceRequirementTest() throws Exception {
    generateConfigs(true, false, false, 4, false);
    MockClusterMap spyClusterMap = spy(clusterMap);
    MockDataNodeId localNode = spyClusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = spyClusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager =
        new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry,
            new MockIdFactory(), spyClusterMap, localNode, new DummyMessageStoreHardDelete(),
            Collections.singletonList(mockHelixParticipant), SystemTime.getInstance(), new DummyMessageStoreRecovery(),
            new InMemAccountService(false, false));
    storageManager.start();

    // 0. Mock the cluster to be in Full auto
    doReturn(true).when(spyClusterMap).isDataNodeInFullAutoMode(any());

    // 1. Create "newReplica1" and shutdown its disk
    PartitionId newPartition1 = spyClusterMap.createNewPartition(Collections.singletonList(localNode), 0);
    ReplicaId newReplica1 = newPartition1.getReplicaIds().get(0);
    ReplicaId replicaOnSameDisk =
        localReplicas.stream().filter(r -> r.getDiskId().equals(newReplica1.getDiskId())).findFirst().get();
    DiskManager diskManager = storageManager.getDiskManager(replicaOnSameDisk.getPartitionId());
    Field field = DiskManager.class.getDeclaredField("diskSpaceAllocator");
    field.setAccessible(true);
    DiskSpaceAllocator diskSpaceAllocator = (DiskSpaceAllocator) field.get(diskManager);
    DiskSpaceAllocator spyDiskSpaceAllocator = spy(diskSpaceAllocator);
    field.set(diskManager, spyDiskSpaceAllocator);

    // 2. Create "newReplica2" which has disk running
    PartitionId newPartition2 = spyClusterMap.createNewPartition(Collections.singletonList(localNode), 1);
    ReplicaId newReplica2 = newPartition2.getReplicaIds().get(0);

    // 3. Return "newReplica1" on 1st attempt and "newReplica2" on 2nd attempt
    doReturn(newReplica1, newReplica2).when(spyClusterMap).getBootstrapReplica(any(), any());

    // 4. It should fail when disk space allocator is allocating multiple reserved files. We will let the first reserved
    // file be created, but fail the second one
    AtomicBoolean isFirstReservedFile = new AtomicBoolean(true);
    doAnswer(invocation -> {
      long fileSize = invocation.getArgument(0);
      File dir = invocation.getArgument(1, File.class);
      if (isFirstReservedFile.get()) {
        isFirstReservedFile.set(false);
        return diskSpaceAllocator.createReserveFile(fileSize, dir);
      } else {
        throw new IOException("Fail creation of file in test");
      }
    }).when(spyDiskSpaceAllocator).createReserveFile(anyLong(), any());

    // 5. Invoke bootstrap ST. It should pass on 2nd attempt.
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition1.toPathString());

    // 6. Verify getBootstrap replica is called 2 times
    verify(spyClusterMap, times(2)).getBootstrapReplica(anyString(), any());

    // 7. the directories should be cleaned up
    assertFalse("File " + newReplica1.getReplicaPath() + " shouldn't exist",
        new File(newReplica1.getReplicaPath()).exists());

    File reservedDir =
        Paths.get(newReplica1.getDiskId().getMountPath(), diskManagerConfig.diskManagerReserveFileDirName,
            DiskSpaceAllocator.STORE_DIR_PREFIX + newReplica1.getPartitionId().toPathString()).toFile();
    Assert.assertFalse("Directory" + reservedDir.getAbsolutePath() + " should be deleted", reservedDir.exists());

    assertTrue("File " + newReplica2.getReplicaPath() + " should exist",
        new File(newReplica2.getReplicaPath()).exists());

    reservedDir = Paths.get(newReplica2.getDiskId().getMountPath(), diskManagerConfig.diskManagerReserveFileDirName,
        DiskSpaceAllocator.STORE_DIR_PREFIX + newReplica2.getPartitionId().toPathString()).toFile();
    Assert.assertTrue("Directory" + reservedDir.getAbsolutePath() + " should exist", reservedDir.exists());

    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * Test disk failure handler with real helix clustermap and helix participant.
   */
  @Test
  public void testDiskFailureHandler() throws Exception {
    String tempDirPath = getTempDir("StorageManagerTest-");
    List<ZkInfo> zkInfoList = new ArrayList<>();
    String clusterName = "StorageManagerTestCluster";
    String dcName = "DC0";
    zkInfoList.add(new ZkInfo(tempDirPath, dcName, (byte) 0, 2199, true));
    String hardwareLayoutPath = tempDirPath + "/hardwareLayoutTest.json";
    String partitionLayoutPath = tempDirPath + "/partitionLayoutTest.json";
    String zkLayoutPath = tempDirPath + "/zkLayoutPath.json";
    String oldBaseMountPath = TestHardwareLayout.baseMountPath;
    long oldMinCapacity = MIN_REPLICA_CAPACITY_IN_BYTES;
    MIN_REPLICA_CAPACITY_IN_BYTES = 1024;
    TestHardwareLayout.baseMountPath = tempDirPath + "/mnt";
    TestHardwareLayout testHardwareLayout =
        new TestHardwareLayout(clusterName, 6, 100L * 1024 * 1024 * 1024, 6, 1, 18088, 20, false);
    TestPartitionLayout testPartitionLayout =
        new TestPartitionLayout(testHardwareLayout, 100, PartitionState.READ_WRITE, 1024, 3, null, 0);
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);

    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    DataNodeId dataNodeId = testHardwareLayout.getRandomDataNodeFromDc(dcName);
    props.setProperty("clustermap.port", String.valueOf(dataNodeId.getPort()));
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dcName);
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.data.node.config.source.type", DataNodeConfigSourceType.PROPERTY_STORE.name());
    props.setProperty("clustermap.enable.state.model.listener", "true");
    props.setProperty("clustermap.update.datanode.info", "true");
    props.setProperty(ClusterMapConfig.DISTRIBUTED_LOCK_LEASE_TIMEOUT_IN_MS, "10000");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    storeConfig = new StoreConfig(new VerifiableProperties(props));
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "", dcName, 100,
        false, false, new HelixAdminFactory(), false, ClusterMapConfig.AMBRY_STATE_MODEL_DEF, BootstrapCluster,
        DataNodeConfigSourceType.PROPERTY_STORE, false, 1000);

    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings).get(
                clusterMapConfig.clusterMapDatacenterName).getZkConnectStrs().get(0), true);
    // Mock a state change listener to throw an exception
    PartitionStateChangeListener listener = mock(PartitionStateChangeListener.class);
    doThrow(new StateTransitionException("error", StateTransitionException.TransitionErrorCode.BootstrapFailure)).when(
        listener).onPartitionBecomeBootstrapFromOffline(anyString());
    helixParticipant.registerPartitionStateChangeListener(StateModelListenerType.StatsManagerListener, listener);
    helixParticipant.participate(Collections.emptyList(), null, null);

    HelixAdmin helixAdmin = helixParticipant.getHelixAdmin();
    HelixClusterManager clusterMap =
        new HelixClusterManager(clusterMapConfig, instanceName, new HelixFactory(), metricRegistry);

    try {
      DataNodeId localNode = clusterMap.getDataNodeId("localhost", clusterMapConfig.clusterMapPort);
      List<? extends ReplicaId> replicas = clusterMap.getReplicaIds(localNode);
      Map<DiskId, List<ReplicaId>> diskToReplicas = new HashMap<>();
      for (ReplicaId replica : replicas) {
        diskToReplicas.computeIfAbsent(replica.getDiskId(), disk -> new ArrayList<>()).add(replica);
      }
      // Create all mount paths
      for (DiskId diskId : diskToReplicas.keySet()) {
        File file = new File(diskId.getMountPath());
        if (!file.exists()) {
          assertTrue(file.mkdirs());
          file.deleteOnExit();
        }
      }
      StorageManager storageManager =
          new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry,
              new MockIdFactory(), clusterMap, localNode, new DummyMessageStoreHardDelete(),
              Collections.singletonList(helixParticipant), SystemTime.getInstance(), new DummyMessageStoreRecovery(),
              new InMemAccountService(false, false));
      storageManager.start();
      // starting the storage manager won't start Disk failure handler right away, since there is a 10 minutes
      // delay to run the handler in a scheduler
      StorageManager.DiskFailureHandler handler = storageManager.new DiskFailureHandler();

      assertEquals(new ArrayList<>(storageManager.getDiskToDiskManager().keySet()), handler.getAllDisks());
      assertEquals("There shouldn't be any failed disk", 0, handler.getFailedDisks().size());
      // Turn FULL_AUTO on this host
      String resourceName = helixAdmin.getResourcesInCluster(clusterName).get(0);
      IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
      final String instanceGroupTag = "TAG_1000000";
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setInstanceGroupTag(instanceGroupTag);
      instanceConfig.addTag(instanceGroupTag);
      helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
      helixAdmin.setResourceIdealState(clusterName, resourceName, idealState);
      Thread.sleep(500); // wait for clustermap to catch up
      assertTrue(clusterMap.isDataNodeInFullAutoMode(localNode));
      // Set every replicas to error state
      sendStateTransitionMessages(helixParticipant.getHelixManager(), "10000", replicas, "OFFLINE", "BOOTSTRAP");
      Thread.sleep(1000);

      long failureCountBefore = storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount();
      // The case where there is no failed disk, running handler doesn't change anything.
      handler.run();
      assertEquals(failureCountBefore, storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount());

      DiskId diskToFail = diskToReplicas.entrySet().iterator().next().getKey();
      List<ReplicaId> replicasOnFailedDisk = diskToReplicas.get(diskToFail);

      // The case where all but one replicas on the given disk are shutdown. Since the disk is not considered as failed,
      // running the handler doesn't change anything.
      for (int i = 0; i < replicasOnFailedDisk.size() - 1; i++) {
        storageManager.getStore(replicasOnFailedDisk.get(i).getPartitionId(), false).shutdown();
      }
      handler.run();
      assertEquals(failureCountBefore, storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount());

      // The case where all replicas on this disk are down
      storageManager.getStore(replicasOnFailedDisk.get(replicasOnFailedDisk.size() - 1).getPartitionId(), false)
          .shutdown();
      verifyDiskFailureSuccess(storageManager, handler, helixAdmin, clusterMap, clusterName, localNode, diskToReplicas,
          diskToFail);

      // The case to run this again, since there is no new failed disk, running the handler again won't change anything.
      failureCountBefore = storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount();
      long successCount = storageManager.getStoreMainMetrics().handleDiskFailureSuccessCount.getCount();
      handler.run();
      assertEquals(failureCountBefore, storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount());
      assertEquals(successCount, storageManager.getStoreMainMetrics().handleDiskFailureSuccessCount.getCount());

      // The case to fail another disk
      List<DiskId> healthyDisks = new ArrayList<>(diskToReplicas.keySet());
      healthyDisks.remove(diskToFail);
      diskToFail = healthyDisks.iterator().next();
      replicasOnFailedDisk = diskToReplicas.get(diskToFail);
      for (ReplicaId replica : replicasOnFailedDisk) {
        storageManager.getStore(replica.getPartitionId(), false).shutdown();
      }
      verifyDiskFailureSuccess(storageManager, handler, helixAdmin, clusterMap, clusterName, localNode, diskToReplicas,
          diskToFail);

      // The case where a disk is empty
      // first set the disk back to available
      helixParticipant.setDisksState(Collections.singletonList(diskToFail), HardwareState.AVAILABLE);
      Thread.sleep(500);// clustermap will be updated
      // Add this disk to the disk manager in storage manager, this disk should have no replicas anymore
      storageManager.addDisk(diskToFail);
      // Create a new handler so the failed disk in this new handler would be empty
      handler = storageManager.new DiskFailureHandler();
      failureCountBefore = storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount();
      handler.run();
      assertEquals(failureCountBefore, storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount());

      storageManager.shutdown();
    } finally {
      TestHardwareLayout.baseMountPath = oldBaseMountPath;
      MIN_REPLICA_CAPACITY_IN_BYTES = oldMinCapacity;
      try {
        Utils.deleteFileOrDirectory(new File(tempDirPath));
        clusterMap.close();
        helixParticipant.close();
        helixAdmin.dropCluster(clusterName);
        for (ZkInfo zkInfo : zkInfoList) {
          zkInfo.shutdown();
        }
      } catch (Exception e) {
        System.out.println("Fail to clean up all the components:" + e.getMessage());
      }
    }
  }

  @Test
  public void testDiskFailureErrorCases() throws Exception {
    String tempDirPath = getTempDir("StorageManagerTest-");
    List<ZkInfo> zkInfoList = new ArrayList<>();
    String clusterName = "StorageManagerTestCluster";
    String dcName = "DC0";
    zkInfoList.add(new ZkInfo(tempDirPath, dcName, (byte) 0, 2199, true));
    String hardwareLayoutPath = tempDirPath + "/hardwareLayoutTest.json";
    String partitionLayoutPath = tempDirPath + "/partitionLayoutTest.json";
    String zkLayoutPath = tempDirPath + "/zkLayoutPath.json";
    String oldBaseMountPath = TestHardwareLayout.baseMountPath;
    long oldMinCapacity = MIN_REPLICA_CAPACITY_IN_BYTES;
    MIN_REPLICA_CAPACITY_IN_BYTES = 1024;
    TestHardwareLayout.baseMountPath = tempDirPath + "/mnt";
    TestHardwareLayout testHardwareLayout =
        new TestHardwareLayout(clusterName, 6, 100L * 1024 * 1024 * 1024, 6, 1, 18088, 20, false);
    TestPartitionLayout testPartitionLayout =
        new TestPartitionLayout(testHardwareLayout, 100, PartitionState.READ_WRITE, 1024, 3, null, 0);
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);

    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    DataNodeId dataNodeId = testHardwareLayout.getRandomDataNodeFromDc(dcName);
    props.setProperty("clustermap.port", String.valueOf(dataNodeId.getPort()));
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dcName);
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.data.node.config.source.type", DataNodeConfigSourceType.PROPERTY_STORE.name());
    props.setProperty("clustermap.enable.state.model.listener", "true");
    props.setProperty("clustermap.update.datanode.info", "true");
    props.setProperty(ClusterMapConfig.DISTRIBUTED_LOCK_LEASE_TIMEOUT_IN_MS, "10000");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    storeConfig = new StoreConfig(new VerifiableProperties(props));
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "", dcName, 100,
        false, false, new HelixAdminFactory(), false, ClusterMapConfig.AMBRY_STATE_MODEL_DEF, BootstrapCluster,
        DataNodeConfigSourceType.PROPERTY_STORE, false, 1000);

    String instanceName = ClusterMapUtils.getInstanceName("localhost", clusterMapConfig.clusterMapPort);
    HelixParticipant helixParticipant =
        new HelixParticipant(mock(HelixClusterManager.class), clusterMapConfig, new HelixFactory(), metricRegistry,
            parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings).get(
                clusterMapConfig.clusterMapDatacenterName).getZkConnectStrs().get(0), true);

    // Mock a state change listener to throw an exception
    PartitionStateChangeListener listener = mock(PartitionStateChangeListener.class);
    doThrow(new StateTransitionException("error", StateTransitionException.TransitionErrorCode.BootstrapFailure)).when(
        listener).onPartitionBecomeBootstrapFromOffline(anyString());
    helixParticipant.registerPartitionStateChangeListener(StateModelListenerType.StatsManagerListener, listener);
    helixParticipant.participate(Collections.emptyList(), null, null);

    HelixAdmin helixAdmin = helixParticipant.getHelixAdmin();
    HelixClusterManager clusterMap =
        new HelixClusterManager(clusterMapConfig, instanceName, new HelixFactory(), metricRegistry);

    try {
      DataNodeId localNode = clusterMap.getDataNodeId("localhost", clusterMapConfig.clusterMapPort);
      List<? extends ReplicaId> replicas = clusterMap.getReplicaIds(localNode);
      Map<DiskId, List<ReplicaId>> diskToReplicas = new HashMap<>();
      for (ReplicaId replica : replicas) {
        diskToReplicas.computeIfAbsent(replica.getDiskId(), disk -> new ArrayList<>()).add(replica);
      }
      // Create all mount paths
      for (DiskId diskId : diskToReplicas.keySet()) {
        File file = new File(diskId.getMountPath());
        if (!file.exists()) {
          assertTrue(file.mkdirs());
          file.deleteOnExit();
        }
      }
      StorageManager storageManager =
          new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry,
              new MockIdFactory(), clusterMap, localNode, new DummyMessageStoreHardDelete(),
              Collections.singletonList(helixParticipant), SystemTime.getInstance(), new DummyMessageStoreRecovery(),
              new InMemAccountService(false, false));
      storageManager.start();
      // starting the storage manager won't start Disk failure handler right away, since there is a 10 minutes
      // delay to run the handler in a scheduler
      StorageManager.DiskFailureHandler handler = storageManager.new DiskFailureHandler();

      assertEquals(new ArrayList<>(storageManager.getDiskToDiskManager().keySet()), handler.getAllDisks());
      assertEquals("There shouldn't be any failed disk", 0, handler.getFailedDisks().size());
      // Turn FULL_AUTO on this host
      String resourceName = helixAdmin.getResourcesInCluster(clusterName).get(0);
      IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
      final String instanceGroupTag = "TAG_1000000";
      idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      idealState.setInstanceGroupTag(instanceGroupTag);
      instanceConfig.addTag(instanceGroupTag);
      helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
      helixAdmin.setResourceIdealState(clusterName, resourceName, idealState);
      Thread.sleep(500); // wait for clustermap to catch up
      assertTrue(clusterMap.isDataNodeInFullAutoMode(localNode));

      // Above are the same initialization code from the previous test method
      long failureCountBefore = storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount();
      Assert.assertEquals(0, failureCountBefore);

      // First shutdown all the replicas on the disk, so we know there will be a failure to be detected
      DiskId diskToFail = diskToReplicas.entrySet().iterator().next().getKey();
      List<ReplicaId> replicasOnFailedDisk = diskToReplicas.get(diskToFail);
      for (int i = 0; i < replicasOnFailedDisk.size(); i++) {
        storageManager.getStore(replicasOnFailedDisk.get(i).getPartitionId(), false).shutdown();
      }
      // Test case 1. Failed to enter maintenance mode
      long diskFailureErrorCountBefore = storageManager.getStoreMainMetrics().handleDiskFailureErrorCount.getCount();
      long diskFailureSuccessCountBefore =
          storageManager.getStoreMainMetrics().handleDiskFailureSuccessCount.getCount();
      helixAdmin.manuallyEnableMaintenanceMode(clusterName, true, "Block test", null);
      handler.run();
      // there should be one disk failure
      assertEquals(failureCountBefore + 1, storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount());
      // but it should fail due to the failure to enter maintenance mode
      assertEquals(diskFailureErrorCountBefore + 1,
          storageManager.getStoreMainMetrics().handleDiskFailureErrorCount.getCount());
      assertEquals(diskFailureSuccessCountBefore,
          storageManager.getStoreMainMetrics().handleDiskFailureSuccessCount.getCount());
      // The diskToFail should be removed from the failedDisk list
      Assert.assertFalse(handler.getFailedDisks().contains(diskToFail));
      failureCountBefore++;
      diskFailureErrorCountBefore++;
      // Get out of maintenance mode
      helixAdmin.manuallyEnableMaintenanceMode(clusterName, false, "Block Test", null);

      // Test case 2. Not all replicas are in ERROR state
      List<ReplicaId> replicasToTransitionToError = replicasOnFailedDisk.subList(0, replicasOnFailedDisk.size() - 1);
      sendStateTransitionMessages(helixParticipant.getHelixManager(), "10000", replicasToTransitionToError, "OFFLINE",
          "BOOTSTRAP");
      Thread.sleep(1000);
      handler.run();
      assertEquals(failureCountBefore + 1, storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount());
      assertEquals(diskFailureErrorCountBefore + 1,
          storageManager.getStoreMainMetrics().handleDiskFailureErrorCount.getCount());
      assertEquals(diskFailureSuccessCountBefore,
          storageManager.getStoreMainMetrics().handleDiskFailureSuccessCount.getCount());
      // The diskToFail should be removed from the failedDisk list
      Assert.assertFalse(handler.getFailedDisks().contains(diskToFail));
      failureCountBefore++;
      diskFailureErrorCountBefore++;

      // Test case 3. A success case
      // Set every replicas to error state
      sendStateTransitionMessages(helixParticipant.getHelixManager(), "10000",
          replicasOnFailedDisk.subList(replicasOnFailedDisk.size() - 1, replicasOnFailedDisk.size()), "OFFLINE",
          "BOOTSTRAP");
      Thread.sleep(1000);
      verifyDiskFailureSuccess(storageManager, handler, helixAdmin, clusterMap, clusterName, localNode, diskToReplicas,
          diskToFail);
      storageManager.shutdown();
    } finally {
      TestHardwareLayout.baseMountPath = oldBaseMountPath;
      MIN_REPLICA_CAPACITY_IN_BYTES = oldMinCapacity;
      try {
        Utils.deleteFileOrDirectory(new File(tempDirPath));
        clusterMap.close();
        helixParticipant.close();
        helixAdmin.dropCluster(clusterName);
        for (ZkInfo zkInfo : zkInfoList) {
          zkInfo.shutdown();
        }
      } catch (Exception e) {
        System.out.println("Fail to clean up all the components:" + e.getMessage());
      }
    }
  }

  // helpers

  /**
   * Verify the state after handling a disk failure.
   * @param storageManager The {@link StorageManager}.
   * @param handler The {@link com.github.ambry.store.StorageManager.DiskFailureHandler}.
   * @param helixAdmin The {@link HelixAdmin}.
   * @param clusterMap The {@link ClusterMap}.
   * @param clusterName The cluster name
   * @param localNode The local node
   * @param diskToReplicas The map from disk id to a list of replicas
   * @param diskToFail The disk to fail
   * @throws Exception
   */
  private void verifyDiskFailureSuccess(StorageManager storageManager, StorageManager.DiskFailureHandler handler,
      HelixAdmin helixAdmin, ClusterMap clusterMap, String clusterName, DataNodeId localNode,
      Map<DiskId, List<ReplicaId>> diskToReplicas, DiskId diskToFail) throws Exception {
    long failureCountBefore = storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount();
    long successCountBefore = storageManager.getStoreMainMetrics().handleDiskFailureSuccessCount.getCount();
    List<? extends ReplicaId> allReplicaIds = clusterMap.getReplicaIds(localNode);
    int numDisksInMemory = storageManager.getDiskToDiskManager().size();
    String instanceName = ClusterMapUtils.getInstanceName(localNode.getHostname(), localNode.getPort());
    int failedDisksBefore = handler.getFailedDisks().size();
    int offlineReplicasBefore = getNumberOfReplicaInStateFromMetric("offline", metricRegistry);

    handler.run();
    assertEquals(failureCountBefore + 1, storageManager.getStoreMainMetrics().handleDiskFailureCount.getCount());
    assertEquals(successCountBefore + 1, storageManager.getStoreMainMetrics().handleDiskFailureSuccessCount.getCount());

    assertEquals("1 more disk failed", failedDisksBefore + 1, handler.getFailedDisks().size());
    assertFalse("Cluster should not in maintenance mode", helixAdmin.isInMaintenanceMode(clusterName));
    Thread.sleep(500); // wait for clustermap to sync with zookeeper
    // replicas should be removed from helix clustermap
    List<? extends ReplicaId> healthyReplicaIds = clusterMap.getReplicaIds(localNode);
    Set<? extends ReplicaId> removedReplicas = new HashSet<>(allReplicaIds);
    removedReplicas.removeAll(healthyReplicaIds);
    List<ReplicaId> replicasOnFailedDisk = diskToReplicas.get(diskToFail);
    assertEquals("Replicas should be removed from helix clustermap", new HashSet<>(replicasOnFailedDisk),
        removedReplicas);
    assertEquals("Disk should be unavailable now", HardwareState.UNAVAILABLE, diskToFail.getState());
    assertEquals(
        "Replicas should be reset to offline, number of replica on failed disk is " + replicasOnFailedDisk.size()
            + " offline replica before is " + offlineReplicasBefore,
        offlineReplicasBefore + replicasOnFailedDisk.size(),
        getNumberOfReplicaInStateFromMetric("offline", metricRegistry));
    diskToReplicas.remove(diskToFail);
    int healthyDiskCapacity =
        (int) (diskToReplicas.keySet().stream().mapToLong(DiskId::getRawCapacityInBytes).sum() / 1024 / 1024 / 1024
            * 0.95);
    assertEquals("Disk capacity should be updated", healthyDiskCapacity,
        helixAdmin.getInstanceConfig(clusterName, instanceName)
            .getInstanceCapacityMap()
            .get(HelixParticipant.DISK_KEY)
            .intValue());
    for (ReplicaId replicaId : replicasOnFailedDisk) {
      assertNull(storageManager.getStore(replicaId.getPartitionId(), false));
      assertNull(storageManager.getReplica(replicaId.getPartitionId().toPathString()));
      assertNull(storageManager.getDiskManager(replicaId.getPartitionId()));
    }
    assertEquals(numDisksInMemory - 1, storageManager.getDiskToDiskManager().size());
  }

  /**
   * Construct a {@link StorageManager} for the passed in set of replicas.
   * @param currentNode the list of replicas for the {@link StorageManager} to use.
   * @param metricRegistry the {@link MetricRegistry} instance to use to instantiate {@link StorageManager}
   * @param clusterParticipants a list of {@link ClusterParticipant}(s) to use in storage manager
   * @return a started {@link StorageManager}
   * @throws StoreException
   */
  private StorageManager createStorageManager(DataNodeId currentNode, MetricRegistry metricRegistry,
      List<ClusterParticipant> clusterParticipants) throws StoreException {
    return new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry,
        new MockIdFactory(), clusterMap, currentNode, new DummyMessageStoreHardDelete(), clusterParticipants,
        SystemTime.getInstance(), new DummyMessageStoreRecovery(), new InMemAccountService(false, false));
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
      assertNull(storageManager.getStore(replica.getPartitionId(), false));
    }
  }

  /**
   * @return the value of unexpected directory.
   */
  private int getNumUnrecognizedPartitionsReported() {
    return getGaugeValue(metricRegistry.getGauges(), DiskManager.class.getName(), "UnexpectedDirsOnDisk");
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
   * Get the gauge value for the metric in {@link StorageManagerMetrics} for the given class and suffix.
   * @param gauges Map of gauge metrics to use
   * @param className the class to which the metric belongs to
   * @param suffix the suffix of the metric that distinguishes it from other metrics in the class.
   * @return the value of the counter.
   */
  private int getGaugeValue(Map<String, Gauge> gauges, String className, String suffix) {
    return (int) gauges.get(className + "." + suffix).getValue();
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
        assertNull("This store should not be accessible.", storageManager.getStore(id, false));
        assertFalse("Compaction should not be scheduled", storageManager.scheduleNextForCompaction(id));
      } else {
        Store store = storageManager.getStore(id, false);
        assertTrue("Store should be started", store.isStarted());
        assertTrue("Compaction should be scheduled", storageManager.scheduleNextForCompaction(id));
      }
    }
  }

  /**
   * Generates {@link StoreConfig} and {@link DiskManagerConfig} for use in tests.
   * @param segmentedLog {@code true} to set a segment capacity lower than total store capacity
   * @param updateInstanceConfig whether to update InstanceConfig in Helix
   * @param removeUnexpectedDirs {@code true} to remove unexpectedd directories when current node is in FULL AUTO
   * @param numSegment the number of log segment files to create
   * @parem removeDirectoryAndRestart {@code true} to remove directory of a blob store and restart it when it failes to start
   */
  private void generateConfigs(boolean segmentedLog, boolean updateInstanceConfig, boolean removeUnexpectedDirs,
      int numSegment, boolean removeDirectoryAndRestart) {
    List<com.github.ambry.utils.TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new com.github.ambry.utils.TestUtils.ZkInfo(null, "DC0", (byte) 0, 2199, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    Properties properties = new Properties();
    properties.put("disk.manager.enable.segment.pooling", "true");
    properties.put("store.compaction.triggers", "Periodic,Admin");
    properties.put("store.replica.status.delegate.enable", "true");
    properties.put("store.set.local.partition.state.enabled", "true");
    properties.setProperty(StoreConfig.storeRemoveUnexpectedDirsInFullAutoName, String.valueOf(removeUnexpectedDirs));
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.port", "2200");
    properties.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    properties.setProperty("clustermap.datacenter.name", "DC0");
    properties.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    properties.setProperty("clustermap.update.datanode.info", Boolean.toString(updateInstanceConfig));
    // By default, there are 3 disks(mount paths) created for each data node. In order to surpass 0.9 threshold, all
    // disks have to fail.
    properties.setProperty(StoreConfig.storeThresholdOfDiskFailuresToTerminateName, "0.9");
    if (segmentedLog) {
      long replicaCapacity = clusterMap.getAllPartitionIds(null).get(0).getReplicaIds().get(0).getCapacityInBytes();
      properties.put("store.segment.size.in.bytes", Long.toString(replicaCapacity / numSegment));
    }
    properties.setProperty(StoreConfig.storeRemoveDirectoryAndRestartBlobStoreName,
        String.valueOf(removeDirectoryAndRestart));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    diskManagerConfig = new DiskManagerConfig(vProps);
    storeConfig = new StoreConfig(vProps);
    clusterMapConfig = new ClusterMapConfig(vProps);
  }

  /**
   * Generates {@link StoreConfig} and {@link DiskManagerConfig} for use in tests.
   * @param segmentedLog {@code true} to set a segment capacity lower than total store capacity
   * @param updateInstanceConfig whether to update InstanceConfig in Helix
   */
  private void generateConfigs(boolean segmentedLog, boolean updateInstanceConfig) {
    generateConfigs(segmentedLog, updateInstanceConfig, false, 2, false);
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

  private File getFileLogSegment(ReplicaId replica) {
    return Paths.get(replica.getReplicaPath(), LogSegmentName.fromPositionAndGeneration(0, 0).toFilename()).toFile();
  }

  private void corruptFirstLogSegment(ReplicaId replica) throws Exception {
    File file = getFileLogSegment(replica);
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    fileChannel.truncate(fileChannel.size() / 2);
    fileChannel.close();
  }

  /**
   * An extension of {@link HelixParticipant} to help with tests.
   */
  class MockClusterParticipant extends HelixParticipant {
    Boolean updateNodeInfoReturnVal = null;
    Set<ReplicaId> sealedReplicas = new HashSet<>();
    Set<ReplicaId> partiallySealedReplicas = new HashSet<>();
    Set<ReplicaId> stoppedReplicas = new HashSet<>();
    Set<ReplicaId> disabledReplicas = new HashSet<>();
    private Boolean setSealStateReturnVal;
    private Boolean setStopStateReturnVal;

    MockClusterParticipant() {
      this(null, null);
      markDisablePartitionComplete();
    }

    /**
     * Ctor for MockClusterParticipant with arguments to override return value of some methods.
     * @param setSealStateReturnVal if not null, use this value to override result of setReplicaSealedState(). If null,
     *                              go through standard workflow in the method.
     * @param setStopStateReturnVal if not null, use this value to override result of setReplicaStoppedState(). If null,
     *                              go through standard workflow in the method.
     */
    MockClusterParticipant(Boolean setSealStateReturnVal, Boolean setStopStateReturnVal) {
      super(mock(HelixClusterManager.class), clusterMapConfig, new MockHelixManagerFactory(), new MetricRegistry(),
          parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings).get(
              clusterMapConfig.clusterMapDatacenterName).getZkConnectStrs().get(0), true);
      this.setSealStateReturnVal = setSealStateReturnVal;
      this.setStopStateReturnVal = setStopStateReturnVal;
      markDisablePartitionComplete();
    }

    @Override
    public void participate(List<AmbryStatsReport> ambryStatsReports, AccountStatsStore accountStatsStore,
        Callback<AggregatedAccountStorageStats> callback) throws IOException {
      // no op
    }

    @Override
    public boolean setReplicaSealedState(ReplicaId replicaId, ReplicaSealStatus replicaSealStatus) {
      if (setSealStateReturnVal != null) {
        return setSealStateReturnVal;
      }
      switch (replicaSealStatus) {
        case SEALED:
          sealedReplicas.add(replicaId);
          partiallySealedReplicas.remove(replicaId);
          break;
        case PARTIALLY_SEALED:
          partiallySealedReplicas.add(replicaId);
          sealedReplicas.remove(replicaId);
          break;
        case NOT_SEALED:
          partiallySealedReplicas.remove(replicaId);
          sealedReplicas.remove(replicaId);
          break;
      }
      return true;
    }

    @Override
    public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
      if (setStopStateReturnVal != null) {
        return setStopStateReturnVal;
      }
      if (markStop) {
        stoppedReplicas.addAll(replicaIds);
      } else {
        stoppedReplicas.removeAll(replicaIds);
      }
      return true;
    }

    @Override
    public void setReplicaDisabledState(ReplicaId replicaId, boolean disable) {
      if (disable) {
        disabledReplicas.add(replicaId);
      } else {
        disabledReplicas.remove(replicaId);
      }
    }

    @Override
    public List<String> getSealedReplicas() {
      return sealedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
    }

    @Override
    public List<String> getPartiallySealedReplicas() {
      return partiallySealedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
    }

    @Override
    public List<String> getStoppedReplicas() {
      return stoppedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
    }

    @Override
    public boolean updateDataNodeInfoInCluster(ReplicaId replicaId, boolean shouldExist) {
      return updateNodeInfoReturnVal == null ? super.updateDataNodeInfoInCluster(replicaId, shouldExist)
          : updateNodeInfoReturnVal;
    }

    @Override
    public void close() {
      // no op
    }

    @Override
    public void setPartitionDisabledState(String partitionName, boolean disable) {
      super.setPartitionDisabledState(partitionName, disable);
    }
  }

  /**
   * An extension of {@link StorageManager} to help mock failure case
   */
  private class MockStorageManager extends StorageManager {
    boolean controlCompactionReturnVal = false;

    MockStorageManager(DataNodeId currentNode, List<ClusterParticipant> clusterParticipants) throws Exception {
      super(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry, new MockIdFactory(),
          clusterMap, currentNode, new DummyMessageStoreHardDelete(), clusterParticipants, SystemTime.getInstance(),
          new DummyMessageStoreRecovery(), new InMemAccountService(false, false));
    }

    @Override
    public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
      return controlCompactionReturnVal;
    }
  }
}

