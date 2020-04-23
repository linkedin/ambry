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
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.HelixParticipant;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockHelixManagerFactory;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.AmbryHealthReport;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;
import static com.github.ambry.clustermap.TestUtils.*;
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
    clusterMap = new MockClusterMap(false, 1, 3, 3, false, false);
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
    storageManager.getDiskManager(localReplicas.get(0).getPartitionId()).start();
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
    File storeReserveDir = new File(reservePoolDir, DiskSpaceAllocator.STORE_DIR_PREFIX + newPartition2.toString());
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
  public void replicaFromOfflineToBootstrapTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager = createStorageManager(localNode, metricRegistry, mockHelixParticipant);
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
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaOperationFailure, e.getErrorCode());
    }
    // restart disk manager to test case where new replica(store) is successfully added into StorageManager
    storageManager.getDiskManager(replicaOnSameDisk.getPartitionId()).start();
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
    BlobStore newAddedStore = (BlobStore) storageManager.getStore(newPartition);
    assertNotNull("There should be a started store associated with new partition", newAddedStore);

    // 5. verify that new added store has bootstrap file
    assertTrue("There should be a bootstrap file indicating store is in BOOTSTRAP state",
        newAddedStore.isBootstrapInProgress());

    // 6. test that state transition should succeed for existing replicas
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(localReplicas.get(0).getPartitionId().toPathString());
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
    StorageManager storageManager = createStorageManager(localNode, metricRegistry, mockHelixParticipant);
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
    // 3. store not started exception
    ReplicaId localReplica = localReplicas.get(0);
    storageManager.shutdownBlobStore(localReplica.getPartitionId());
    try {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby(localReplica.getPartitionId().toPathString());
      fail("should fail because store is not started");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StoreNotStarted, e.getErrorCode());
    }
    storageManager.startBlobStore(localReplica.getPartitionId());
    // 4. success case (verify both replica's state and decommission file)
    mockHelixParticipant.onPartitionBecomeInactiveFromStandby(localReplica.getPartitionId().toPathString());
    assertEquals("local store state should be set to INACTIVE", ReplicaState.INACTIVE,
        storageManager.getStore(localReplica.getPartitionId()).getCurrentState());
    File decommissionFile = new File(localReplica.getReplicaPath(), BlobStore.DECOMMISSION_FILE_NAME);
    assertTrue("Decommission file is not found in local replica's dir", decommissionFile.exists());
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);

    // 5. mock disable compaction failure
    mockHelixParticipant = new MockClusterParticipant();
    MockStorageManager mockStorageManager = new MockStorageManager(localNode, mockHelixParticipant);
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
   * Test shutting down blob store failure during Inactive-To-Offline transition.
   * @throws Exception
   */
  @Test
  public void replicaFromInactiveToOfflineTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    ReplicaId testReplica = localReplicas.get(0);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager = createStorageManager(localNode, metricRegistry, mockHelixParticipant);
    storageManager.start();
    // test shutdown store failure (this is induced by shutting down disk manager)
    storageManager.getDiskManager(testReplica.getPartitionId()).shutdown();
    mockHelixParticipant.getReplicaSyncUpManager().initiateDisconnection(testReplica);
    CountDownLatch participantLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      try {
        mockHelixParticipant.onPartitionBecomeOfflineFromInactive(testReplica.getPartitionId().toPathString());
        fail("should fail because of shutting down store failure");
      } catch (StateTransitionException e) {
        assertEquals("Error code doesn't match", ReplicaOperationFailure, e.getErrorCode());
        participantLatch.countDown();
      }
    }, false).start();
    // make sync-up complete to let code proceed and encounter exception in storage manager.
    mockHelixParticipant.getReplicaSyncUpManager().onDisconnectionComplete(testReplica);
    assertTrue("Helix participant transition didn't get invoked within 1 sec",
        participantLatch.await(1, TimeUnit.SECONDS));
    shutdownAndAssertStoresInaccessible(storageManager, localReplicas);
  }

  /**
   * Test that initializing participant metrics fails because the initial offline partition count is not zero.
   * @throws Exception
   */
  @Test
  public void initParticipantMetricsFailureTest() throws Exception {
    generateConfigs(true, false);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    // create first storage manager and start
    StorageManager storageManager1 = createStorageManager(localNode, new MetricRegistry(), mockHelixParticipant);
    storageManager1.start();
    shutdownAndAssertStoresInaccessible(storageManager1, localReplicas);
    // create second storage manager with same mock helix participant
    StorageManager storageManager2 = createStorageManager(localNode, new MetricRegistry(), mockHelixParticipant);
    try {
      storageManager2.start();
      fail("should fail because offline partition count is non-zero before initialization");
    } catch (IllegalStateException e) {
      // expected
    } finally {
      shutdownAndAssertStoresInaccessible(storageManager2, localReplicas);
    }
  }

  /**
   * Test failure cases when updating InstanceConfig in Helix for both Offline-To-Bootstrap and Inactive-To-Offline.
   */
  @Test
  public void updateInstanceConfigFailureTest() throws Exception {
    generateConfigs(true, true);
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> localReplicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    StorageManager storageManager = createStorageManager(localNode, metricRegistry, mockHelixParticipant);
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
      mockHelixParticipant.onPartitionBecomeOfflineFromInactive(localReplicas.get(0).getPartitionId().toPathString());
      fail("should fail because updating InstanceConfig didn't succeed during Inactive-To-Offline");
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
      mockHelixParticipant.onPartitionBecomeOfflineFromInactive(localReplicas.get(1).getPartitionId().toPathString());
      fail("should fail because InstanceConfig is not found during Inactive-To-Offline");
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
    StorageManager storageManager = createStorageManager(localNode, metricRegistry, mockHelixParticipant);
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
            SystemTime.getInstance(), new DummyMessageStoreRecovery());
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, participantSpy);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, mockParticipant);
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
    StorageManager storageManager = createStorageManager(dataNode, metricRegistry, null);
    storageManager.start();
    assertEquals("There should be some unexpected partitions reported", extraDirsCount,
        getNumUnrecognizedPartitionsReported());
    checkStoreAccessibility(replicas, null, storageManager);
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test that residual directory associated with removed replica is deleted correctly during OFFLINE -> DROPPED transition.
   * @throws Exception
   */
  @Test
  public void residualDirDeletionTest() throws Exception {
    MockDataNodeId localNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(localNode);
    MockClusterParticipant mockHelixParticipant = new MockClusterParticipant();
    // create an extra store dir at one of the mount paths
    String mountPath = replicas.get(0).getMountPath();
    String extraPartitionName = "1000";
    File extraStoreDir = new File(mountPath, extraPartitionName);
    assertTrue("Can't create an extra store dir", extraStoreDir.mkdir());
    StorageManager storageManager = createStorageManager(localNode, metricRegistry, mockHelixParticipant);
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
    assertFalse("Extra store dir should not exist", extraStoreDir.exists());
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  // helpers

  /**
   * Construct a {@link StorageManager} for the passed in set of replicas.
   * @param currentNode the list of replicas for the {@link StorageManager} to use.
   * @param metricRegistry the {@link MetricRegistry} instance to use to instantiate {@link StorageManager}
   * @param clusterParticipant the {@link ClusterParticipant} to use in storage manager
   * @return a started {@link StorageManager}
   * @throws StoreException
   */
  private StorageManager createStorageManager(DataNodeId currentNode, MetricRegistry metricRegistry,
      ClusterParticipant clusterParticipant) throws StoreException {
    return new StorageManager(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry,
        new MockIdFactory(), clusterMap, currentNode, new DummyMessageStoreHardDelete(), clusterParticipant,
        SystemTime.getInstance(), new DummyMessageStoreRecovery());
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
   */
  private void generateConfigs(boolean segmentedLog, boolean updateInstanceConfig) throws IOException {
    List<com.github.ambry.utils.TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new com.github.ambry.utils.TestUtils.ZkInfo(null, "DC0", (byte) 0, 2199, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    Properties properties = new Properties();
    properties.put("disk.manager.enable.segment.pooling", "true");
    properties.put("store.compaction.triggers", "Periodic,Admin");
    properties.put("store.replica.status.delegate.enable", "true");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.port", "2200");
    properties.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    properties.setProperty("clustermap.datacenter.name", "DC0");
    properties.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    properties.setProperty("clustermap.update.datanode.info", Boolean.toString(updateInstanceConfig));
    if (segmentedLog) {
      long replicaCapacity = clusterMap.getAllPartitionIds(null).get(0).getReplicaIds().get(0).getCapacityInBytes();
      properties.put("store.segment.size.in.bytes", Long.toString(replicaCapacity / 2L));
    }
    VerifiableProperties vProps = new VerifiableProperties(properties);
    diskManagerConfig = new DiskManagerConfig(vProps);
    storeConfig = new StoreConfig(vProps);
    clusterMapConfig = new ClusterMapConfig(vProps);
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
   * An extension of {@link HelixParticipant} to help with tests.
   */
  private class MockClusterParticipant extends HelixParticipant {
    Boolean updateNodeInfoReturnVal = null;
    Set<ReplicaId> sealedReplicas = new HashSet<>();
    Set<ReplicaId> stoppedReplicas = new HashSet<>();

    MockClusterParticipant() throws IOException {
      super(clusterMapConfig, new MockHelixManagerFactory(), new MetricRegistry(),
          parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings).get(
              clusterMapConfig.clusterMapDatacenterName).getZkConnectStrs().get(0), true);
    }

    @Override
    public void participate(List<AmbryHealthReport> ambryHealthReports) throws IOException {
      // no op
    }

    @Override
    public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
      if (isSealed) {
        sealedReplicas.add(replicaId);
      } else {
        sealedReplicas.remove(replicaId);
      }
      return true;
    }

    @Override
    public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
      if (markStop) {
        stoppedReplicas.addAll(replicaIds);
      } else {
        stoppedReplicas.removeAll(replicaIds);
      }
      return true;
    }

    @Override
    public List<String> getSealedReplicas() {
      return sealedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
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
  }

  /**
   * An extension of {@link StorageManager} to help mock failure case
   */
  private class MockStorageManager extends StorageManager {
    boolean controlCompactionReturnVal = false;

    MockStorageManager(DataNodeId currentNode, ClusterParticipant clusterParticipant) throws Exception {
      super(storeConfig, diskManagerConfig, Utils.newScheduler(1, false), metricRegistry, new MockIdFactory(),
          clusterMap, currentNode, new DummyMessageStoreHardDelete(), clusterParticipant, SystemTime.getInstance(),
          new DummyMessageStoreRecovery());
    }

    @Override
    public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
      return controlCompactionReturnVal;
    }
  }
}

