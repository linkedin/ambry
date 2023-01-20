/**
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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.AmbryReplicaSyncUpManager;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockHelixParticipant;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.ValidatingTransformer;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.ReplicaMetadataResponseInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockId;
import com.github.ambry.store.MockMessageWriteSet;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.MockClusterMap.*;
import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;


/**
 * Tests for ReplicaThread for both pairs of compatible ReplicaMetadataRequest and ReplicaMetadataResponse
 * {@code ReplicaMetadataRequest#Replica_Metadata_Request_Version_V1}, {@code ReplicaMetadataResponse#REPLICA_METADATA_RESPONSE_VERSION_V_5}
 * {@code ReplicaMetadataRequest#Replica_Metadata_Request_Version_V2}, {@code ReplicaMetadataResponse#REPLICA_METADATA_RESPONSE_VERSION_V_6}
 */
@RunWith(Parameterized.class)
public class ReplicationTest extends ReplicationTestHelper {
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  /**
   * Constructor to set the configs
   */
  public ReplicationTest(short requestVersion, short responseVersion, boolean shouldUseNetworkClient) {
    super(requestVersion, responseVersion, shouldUseNetworkClient);
  }

  /**
   * Running for the two sets of compatible ReplicaMetadataRequest and ReplicaMetadataResponse,
   * viz {{@code ReplicaMetadataRequest#Replica_Metadata_Request_Version_V1}, {@code ReplicaMetadataResponse#REPLICA_METADATA_RESPONSE_VERSION_V_5}}
   * & {{@code ReplicaMetadataRequest#Replica_Metadata_Request_Version_V2}, {@code ReplicaMetadataResponse#REPLICA_METADATA_RESPONSE_VERSION_V_6}}
   * @return an array with both pairs of compatible request and response.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    //@formatter:off
    return Arrays.asList(new Object[][]{
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5, false},
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6, false},
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6, true}
    });
    //@formatter:on
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Tests add/remove replicaInfo to {@link ReplicaThread}
   * @throws Exception
   */
  @Test
  public void remoteReplicaInfoAddRemoveTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    MockStoreKeyConverterFactory mockStoreKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    mockStoreKeyConverterFactory.setReturnInputIfAbsent(true);
    mockStoreKeyConverterFactory.setConversionMap(new HashMap<>());
    StoreKeyConverter storeKeyConverter = mockStoreKeyConverterFactory.getStoreKeyConverter();
    Transformer transformer = new ValidatingTransformer(storeKeyFactory, storeKeyConverter);

    ReplicationMetrics replicationMetrics =
        new ReplicationMetrics(new MetricRegistry(), clusterMap.getReplicaIds(localHost.dataNodeId));
    replicationMetrics.populateSingleColoMetrics(remoteHost.dataNodeId.getDatacenterName());
    List<RemoteReplicaInfo> remoteReplicaInfoList = localHost.getRemoteReplicaInfos(remoteHost, null);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    hosts.put(remoteHost.dataNodeId, remoteHost);
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 4);
    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", new MockFindTokenHelper(storeKeyFactory, replicationConfig), clusterMap,
            new AtomicInteger(0), localHost.dataNodeId, connectionPool, null, replicationConfig, replicationMetrics,
            null, mockStoreKeyConverterFactory.getStoreKeyConverter(), transformer, clusterMap.getMetricRegistry(),
            false, localHost.dataNodeId.getDatacenterName(), new ResponseHandler(clusterMap), time, null, null, null);
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfoList) {
      replicaThread.addRemoteReplicaInfo(remoteReplicaInfo);
    }
    List<RemoteReplicaInfo> actualRemoteReplicaInfoList =
        replicaThread.getRemoteReplicaInfos().get(remoteHost.dataNodeId);
    Comparator<RemoteReplicaInfo> remoteReplicaInfoComparator =
        Comparator.comparing(info -> info.getReplicaId().getPartitionId().toPathString());
    Collections.sort(remoteReplicaInfoList, remoteReplicaInfoComparator);
    Collections.sort(actualRemoteReplicaInfoList, remoteReplicaInfoComparator);
    assertEquals("getRemoteReplicaInfos not correct", remoteReplicaInfoList, actualRemoteReplicaInfoList);

    // Test remove remoteReplicaInfo.
    replicaThread.removeRemoteReplicaInfo(remoteReplicaInfoList.get(remoteReplicaInfoList.size() - 1));
    actualRemoteReplicaInfoList = replicaThread.getRemoteReplicaInfos().get(remoteHost.dataNodeId);
    Collections.sort(actualRemoteReplicaInfoList, remoteReplicaInfoComparator);
    remoteReplicaInfoList.remove(remoteReplicaInfoList.size() - 1);
    assertEquals("getRemoteReplicaInfos not correct", remoteReplicaInfoList, actualRemoteReplicaInfoList);
  }

  /**
   * Test dynamically add/remove replica in {@link ReplicationManager}
   * @throws Exception
   */
  @Test
  public void addAndRemoveReplicaTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    StoreConfig storeConfig = new StoreConfig(verifiableProperties);
    DataNodeId dataNodeId = clusterMap.getDataNodeIds().get(0);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    StorageManager storageManager =
        new StorageManager(storeConfig, new DiskManagerConfig(verifiableProperties), Utils.newScheduler(1, true),
            new MetricRegistry(), null, clusterMap, dataNodeId, null, null, new MockTime(), null,
            new InMemAccountService(false, false));
    storageManager.start();
    MockReplicationManager replicationManager =
        new MockReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, clusterMap,
            dataNodeId, storeKeyConverterFactory, null);
    ReplicaId replicaToTest = clusterMap.getReplicaIds(dataNodeId).get(0);
    // Attempting to add replica that already exists should fail
    assertFalse("Adding an existing replica should fail", replicationManager.addReplica(replicaToTest));
    // Create a brand new replica that sits on one of the disk of datanode, add it into replication manager
    PartitionId newPartition = clusterMap.createNewPartition(clusterMap.getDataNodes());
    for (ReplicaId replicaId : newPartition.getReplicaIds()) {
      if (replicaId.getDataNodeId() == dataNodeId) {
        replicaToTest = replicaId;
        break;
      }
    }
    // Before adding replica, partitionToPartitionInfo and mountPathToPartitionInfos should not contain new partition
    assertFalse("partitionToPartitionInfo should not contain new partition",
        replicationManager.getPartitionToPartitionInfoMap().containsKey(newPartition));
    for (PartitionInfo partitionInfo : replicationManager.getMountPathToPartitionInfosMap()
        .get(replicaToTest.getMountPath())) {
      assertNotSame("mountPathToPartitionInfos should not contain new partition", partitionInfo.getPartitionId(),
          newPartition);
    }
    // Add new replica to replication manager
    assertTrue("Adding new replica to replication manager should succeed",
        replicationManager.addReplica(replicaToTest));
    // After adding replica, partitionToPartitionInfo and mountPathToPartitionInfos should contain new partition
    assertTrue("partitionToPartitionInfo should contain new partition",
        replicationManager.getPartitionToPartitionInfoMap().containsKey(newPartition));
    Optional<PartitionInfo> newPartitionInfo = replicationManager.getMountPathToPartitionInfosMap()
        .get(replicaToTest.getMountPath())
        .stream()
        .filter(partitionInfo -> partitionInfo.getPartitionId() == newPartition)
        .findAny();
    assertTrue("mountPathToPartitionInfos should contain new partition info", newPartitionInfo.isPresent());
    // Verify that all remoteReplicaInfos of new added replica have assigned thread
    for (RemoteReplicaInfo remoteReplicaInfo : newPartitionInfo.get().getRemoteReplicaInfos()) {
      assertNotNull("The remote replica should be assigned to one replica thread",
          remoteReplicaInfo.getReplicaThread());
    }

    // Remove replica
    assertTrue("Remove replica from replication manager should succeed",
        replicationManager.removeReplica(replicaToTest));
    // Verify replica is removed, so partitionToPartitionInfo and mountPathToPartitionInfos should not contain new partition
    assertFalse("partitionToPartitionInfo should not contain new partition",
        replicationManager.getPartitionToPartitionInfoMap().containsKey(newPartition));
    for (PartitionInfo partitionInfo : replicationManager.getMountPathToPartitionInfosMap()
        .get(replicaToTest.getMountPath())) {
      assertNotSame("mountPathToPartitionInfos should not contain new partition", partitionInfo.getPartitionId(),
          newPartition);
    }
    // Verify that none of remoteReplicaInfo should have assigned thread
    for (RemoteReplicaInfo remoteReplicaInfo : newPartitionInfo.get().getRemoteReplicaInfos()) {
      assertNull("The remote replica should be assigned to one replica thread", remoteReplicaInfo.getReplicaThread());
    }
    // Remove the same replica that doesn't exist should be no-op
    ReplicationManager mockManager = Mockito.spy(replicationManager);
    assertFalse("Remove non-existent replica should return false", replicationManager.removeReplica(replicaToTest));
    verify(mockManager, never()).removeRemoteReplicaInfoFromReplicaThread(anyList());
    storageManager.shutdown();
  }

  /**
   * Test cluster map change callback in {@link ReplicationManager} when any remote replicas are added or removed.
   * Test setup: attempt to add 3 replicas and remove 3 replicas respectively. The three replicas are picked as follows:
   *   (1) 1st replica on current node (should skip)
   *   (2) 2nd replica on remote node sharing partition with current one (should be added or removed)
   *   (3) 3rd replica on remote node but doesn't share partition with current one (should skip)
   * @throws Exception
   */
  @Test
  public void onReplicaAddedOrRemovedCallbackTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    StoreConfig storeConfig = new StoreConfig(verifiableProperties);
    // pick a node with no special partition as current node
    Set<DataNodeId> specialPartitionNodes = clusterMap.getSpecialPartition()
        .getReplicaIds()
        .stream()
        .map(ReplicaId::getDataNodeId)
        .collect(Collectors.toSet());
    DataNodeId currentNode =
        clusterMap.getDataNodes().stream().filter(d -> !specialPartitionNodes.contains(d)).findFirst().get();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    StorageManager storageManager =
        new StorageManager(storeConfig, new DiskManagerConfig(verifiableProperties), Utils.newScheduler(1, true),
            new MetricRegistry(), null, clusterMap, currentNode, null, null, new MockTime(), null,
            new InMemAccountService(false, false));
    storageManager.start();
    MockReplicationManager replicationManager =
        new MockReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, clusterMap,
            currentNode, storeKeyConverterFactory, null);
    ClusterMapChangeListener clusterMapChangeListener = clusterMap.getClusterMapChangeListener();
    // find the special partition (not on current node) and get an irrelevant replica from it
    PartitionId absentPartition = clusterMap.getSpecialPartition();
    ReplicaId irrelevantReplica = absentPartition.getReplicaIds().get(0);
    // find an existing replica on current node and one of its peer replicas on remote node
    ReplicaId existingReplica = clusterMap.getReplicaIds(currentNode).get(0);
    ReplicaId peerReplicaToRemove =
        existingReplica.getPartitionId().getReplicaIds().stream().filter(r -> r != existingReplica).findFirst().get();
    // create a new node and place a peer of existing replica on it.
    MockDataNodeId remoteNode = createDataNode(
        getListOfPorts(PLAIN_TEXT_PORT_START_NUMBER + 10, SSL_PORT_START_NUMBER + 10, HTTP2_PORT_START_NUMBER + 10),
        clusterMap.getDatacenterName((byte) 0), 3);
    ReplicaId addedReplica =
        new MockReplicaId(remoteNode.getPort(), (MockPartitionId) existingReplica.getPartitionId(), remoteNode, 0);
    // populate added replica and removed replica lists
    List<ReplicaId> replicasToAdd = new ArrayList<>(Arrays.asList(existingReplica, addedReplica, irrelevantReplica));
    List<ReplicaId> replicasToRemove =
        new ArrayList<>(Arrays.asList(existingReplica, peerReplicaToRemove, irrelevantReplica));
    PartitionInfo partitionInfo =
        replicationManager.getPartitionToPartitionInfoMap().get(existingReplica.getPartitionId());
    assertNotNull("PartitionInfo is not found", partitionInfo);
    RemoteReplicaInfo peerReplicaInfo = partitionInfo.getRemoteReplicaInfos()
        .stream()
        .filter(info -> info.getReplicaId() == peerReplicaToRemove)
        .findFirst()
        .get();
    // get the replica-thread for this peer replica
    ReplicaThread peerReplicaThread = peerReplicaInfo.getReplicaThread();

    // Test Case 1: replication manager encountered exception during startup (remote replica addition/removal will be skipped)
    replicationManager.startWithException();
    clusterMapChangeListener.onReplicaAddedOrRemoved(replicasToAdd, replicasToRemove);
    // verify that PartitionInfo stays unchanged
    verifyRemoteReplicaInfo(partitionInfo, addedReplica, false);
    verifyRemoteReplicaInfo(partitionInfo, peerReplicaToRemove, true);

    // Test Case 2: startup latch is interrupted
    CountDownLatch initialLatch = replicationManager.startupLatch;
    CountDownLatch mockLatch = Mockito.mock(CountDownLatch.class);
    doThrow(new InterruptedException()).when(mockLatch).await();
    replicationManager.startupLatch = mockLatch;
    try {
      clusterMapChangeListener.onReplicaAddedOrRemoved(replicasToAdd, replicasToRemove);
      fail("should fail because startup latch is interrupted");
    } catch (IllegalStateException e) {
      // expected
    }
    replicationManager.startupLatch = initialLatch;

    // Test Case 3: replication manager is successfully started
    replicationManager.start();
    clusterMapChangeListener.onReplicaAddedOrRemoved(replicasToAdd, replicasToRemove);
    // verify that PartitionInfo has latest remote replica infos
    verifyRemoteReplicaInfo(partitionInfo, addedReplica, true);
    verifyRemoteReplicaInfo(partitionInfo, peerReplicaToRemove, false);
    verifyRemoteReplicaInfo(partitionInfo, irrelevantReplica, false);
    // verify new added replica is assigned to a certain thread
    ReplicaThread replicaThread =
        replicationManager.getDataNodeIdToReplicaThreadMap().get(addedReplica.getDataNodeId());
    assertNotNull("There is no ReplicaThread assocated with new replica", replicaThread);
    Optional<RemoteReplicaInfo> findResult = replicaThread.getRemoteReplicaInfos()
        .get(remoteNode)
        .stream()
        .filter(info -> info.getReplicaId() == addedReplica)
        .findAny();
    assertTrue("New added remote replica info should exist in corresponding thread", findResult.isPresent());

    // verify the removed replica info's thread is null
    assertNull("Thread in removed replica info should be null", peerReplicaInfo.getReplicaThread());
    findResult = peerReplicaThread.getRemoteReplicaInfos()
        .get(peerReplicaToRemove.getDataNodeId())
        .stream()
        .filter(info -> info.getReplicaId() == peerReplicaToRemove)
        .findAny();
    assertFalse("Previous replica thread should not contain RemoteReplicaInfo that is already removed",
        findResult.isPresent());
    storageManager.shutdown();
  }

  /**
   * Test that state transition in replication manager from OFFLINE to BOOTSTRAP
   * @throws Exception
   */
  @Test
  public void replicaFromOfflineToBootstrapTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);
    DataNodeId currentNode = clusterMap.getDataNodeIds().get(0);
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();
    assertTrue("State change listener in cluster participant should contain replication manager listener",
        mockHelixParticipant.getPartitionStateChangeListeners()
            .containsKey(StateModelListenerType.ReplicationManagerListener));
    // 1. test partition not found case (should throw exception)
    try {
      mockHelixParticipant.onPartitionBecomeBootstrapFromOffline("-1");
      fail("should fail because replica is not found");
    } catch (StateTransitionException e) {
      assertEquals("Transition error doesn't match", ReplicaNotFound, e.getErrorCode());
    }
    // 2. create a new partition and test replica addition success case
    ReplicaId newReplicaToAdd = getNewReplicaToAdd(clusterMap);
    PartitionId newPartition = newReplicaToAdd.getPartitionId();
    assertTrue("Adding new replica to Storage Manager should succeed", storageManager.addBlobStore(newReplicaToAdd));
    assertFalse("partitionToPartitionInfo should not contain new partition",
        replicationManager.partitionToPartitionInfo.containsKey(newPartition));
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
    assertTrue("partitionToPartitionInfo should contain new partition",
        replicationManager.partitionToPartitionInfo.containsKey(newPartition));
    // 3. test replica addition failure case
    replicationManager.partitionToPartitionInfo.remove(newPartition);
    replicationManager.addReplicaReturnVal = false;
    try {
      mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(newPartition.toPathString());
      fail("should fail due to replica addition failure");
    } catch (StateTransitionException e) {
      assertEquals("Transition error doesn't match", ReplicaOperationFailure, e.getErrorCode());
    }
    replicationManager.addReplicaReturnVal = null;
    // 4. test OFFLINE -> BOOTSTRAP on existing replica (should be no-op)
    ReplicaId existingReplica = clusterMap.getReplicaIds(currentNode).get(0);
    assertTrue("partitionToPartitionInfo should contain existing partition",
        replicationManager.partitionToPartitionInfo.containsKey(existingReplica.getPartitionId()));
    mockHelixParticipant.onPartitionBecomeBootstrapFromOffline(existingReplica.getPartitionId().toPathString());
    storageManager.shutdown();
  }

  /**
   * Test BOOTSTRAP -> STANDBY transition on both existing and new replicas. For new replica, we test both failure and
   * success cases.
   * @throws Exception
   */
  @Test
  public void replicaFromBootstrapToStandbyTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();
    // 1. test existing partition trough Bootstrap-To-Standby transition, should be no op.
    PartitionId existingPartition = replicationManager.partitionToPartitionInfo.keySet().iterator().next();
    mockHelixParticipant.onPartitionBecomeStandbyFromBootstrap(existingPartition.toPathString());
    assertEquals("Store state doesn't match", ReplicaState.STANDBY,
        storageManager.getStore(existingPartition).getCurrentState());
    // 2. test transition failure due to store not started
    storageManager.shutdownBlobStore(existingPartition);
    try {
      mockHelixParticipant.onPartitionBecomeStandbyFromBootstrap(existingPartition.toPathString());
      fail("should fail because store is not started");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StoreNotStarted, e.getErrorCode());
    }

    // 3. create new replica and add it into storage manager, test replica that needs to initiate bootstrap
    ReplicaId newReplicaToAdd = getNewReplicaToAdd(clusterMap);
    assertTrue("Adding new replica to Storage Manager should succeed", storageManager.addBlobStore(newReplicaToAdd));
    // override partition state change listener in ReplicationManager to help thread manipulation
    mockHelixParticipant.registerPartitionStateChangeListener(StateModelListenerType.ReplicationManagerListener,
        replicationManager.replicationListener);
    CountDownLatch participantLatch = new CountDownLatch(1);
    replicationManager.listenerExecutionLatch = new CountDownLatch(1);
    // create a new thread and trigger BOOTSTRAP -> STANDBY transition
    Utils.newThread(() -> {
      mockHelixParticipant.onPartitionBecomeStandbyFromBootstrap(newReplicaToAdd.getPartitionId().toPathString());
      participantLatch.countDown();
    }, false).start();
    assertTrue("Partition state change listener in ReplicationManager didn't get called within 1 sec",
        replicationManager.listenerExecutionLatch.await(1, TimeUnit.SECONDS));
    assertEquals("Replica should be in BOOTSTRAP state before bootstrap is complete", ReplicaState.BOOTSTRAP,
        storageManager.getStore(newReplicaToAdd.getPartitionId()).getCurrentState());
    // make bootstrap succeed
    mockHelixParticipant.getReplicaSyncUpManager().onBootstrapComplete(newReplicaToAdd);
    assertTrue("Bootstrap-To-Standby transition didn't complete within 1 sec",
        participantLatch.await(1, TimeUnit.SECONDS));
    storageManager.shutdown();
  }

  /**
   * Test STANDBY -> INACTIVE transition on existing replica (both success and failure cases)
   */
  @Test
  public void replicaFromStandbyToInactiveTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();
    // get an existing partition to test both success and failure cases
    PartitionId existingPartition = replicationManager.partitionToPartitionInfo.keySet().iterator().next();
    storageManager.shutdownBlobStore(existingPartition);
    try {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby(existingPartition.toPathString());
      fail("should fail because store is not started");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StoreNotStarted, e.getErrorCode());
    }
    // restart the store and trigger Standby-To-Inactive transition again
    storageManager.startBlobStore(existingPartition);

    // write a blob with size = 100 into local store (end offset of last PUT = 100 + 18 = 118)
    Store localStore = storageManager.getStore(existingPartition);
    MockId id = new MockId(TestUtils.getRandomString(10), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM));
    long crc = (new Random()).nextLong();
    long blobSize = 100;
    MessageInfo info =
        new MessageInfo(id, blobSize, false, false, Utils.Infinite_Time, crc, id.getAccountId(), id.getContainerId(),
            Utils.Infinite_Time);
    List<MessageInfo> infos = new ArrayList<>();
    List<ByteBuffer> buffers = new ArrayList<>();
    ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) blobSize));
    infos.add(info);
    buffers.add(buffer);
    localStore.put(new MockMessageWriteSet(infos, buffers));
    ReplicaId localReplica = storageManager.getReplica(existingPartition.toPathString());

    // override partition state change listener in ReplicationManager to help thread manipulation
    mockHelixParticipant.registerPartitionStateChangeListener(StateModelListenerType.ReplicationManagerListener,
        replicationManager.replicationListener);
    CountDownLatch participantLatch = new CountDownLatch(1);
    replicationManager.listenerExecutionLatch = new CountDownLatch(1);
    // create a new thread and trigger STANDBY -> INACTIVE transition
    Utils.newThread(() -> {
      mockHelixParticipant.onPartitionBecomeInactiveFromStandby(existingPartition.toPathString());
      participantLatch.countDown();
    }, false).start();
    assertTrue("Partition state change listener didn't get called within 1 sec",
        replicationManager.listenerExecutionLatch.await(1, TimeUnit.SECONDS));
    assertEquals("Local store state should be INACTIVE", ReplicaState.INACTIVE,
        storageManager.getStore(existingPartition).getCurrentState());

    List<RemoteReplicaInfo> remoteReplicaInfos =
        replicationManager.partitionToPartitionInfo.get(existingPartition).getRemoteReplicaInfos();
    ReplicaId peerReplica1 = remoteReplicaInfos.get(0).getReplicaId();

    assertFalse("Sync up should not complete because not enough replicas have caught up",
        mockHelixParticipant.getReplicaSyncUpManager()
            .updateReplicaLagAndCheckSyncStatus(localReplica, peerReplica1, 10L, ReplicaState.INACTIVE));
    // pick another remote replica to update the replication lag
    ReplicaId peerReplica2 = remoteReplicaInfos.get(1).getReplicaId();
    replicationManager.updateTotalBytesReadByRemoteReplica(existingPartition,
        peerReplica1.getDataNodeId().getHostname(), peerReplica1.getReplicaPath(), 118);
    assertFalse("Sync up shouldn't complete because only one replica has caught up with local replica",
        mockHelixParticipant.getReplicaSyncUpManager().isSyncUpComplete(localReplica));
    // make second peer replica catch up with last PUT in local store
    replicationManager.updateTotalBytesReadByRemoteReplica(existingPartition,
        peerReplica2.getDataNodeId().getHostname(), peerReplica2.getReplicaPath(), 118);

    assertTrue("Standby-To-Inactive transition didn't complete within 1 sec",
        participantLatch.await(1, TimeUnit.SECONDS));

    // we purposely update lag against local replica to verify local replica is no longer in ReplicaSyncUpManager because
    // deactivation is complete and local replica should be removed from "replicaToLagInfos" map.
    assertFalse("Sync up should complete (2 replicas have caught up), hence updated should be false",
        mockHelixParticipant.getReplicaSyncUpManager()
            .updateReplicaLagAndCheckSyncStatus(localReplica, peerReplica2, 0L, ReplicaState.INACTIVE));
    storageManager.shutdown();
  }

  /**
   * Test state transition in replication manager from STANDBY to LEADER
   * Test setup: When creating partitions, make sure that there is exactly one replica in LEADER STATE on each data center
   * Test condition: When a partition on current node moves from standby to leader, verify that in-memory map storing
   * partition to peer leader replicas is updated correctly
   * @throws Exception
   */
  @Test
  public void replicaFromStandbyToLeaderTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);

    ReplicationConfig initialReplicationConfig = replicationConfig;
    properties.setProperty("replication.model.across.datacenters", "LEADER_BASED");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));

    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();

    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      if (mockReplicaId.getReplicaState() == ReplicaState.LEADER) {
        PartitionId existingPartition = mockReplicaId.getPartitionId();
        mockHelixParticipant.onPartitionBecomeLeaderFromStandby(existingPartition.toPathString());
        Set<ReplicaId> peerLeaderReplicasInReplicationManager =
            replicationManager.leaderBasedReplicationAdmin.getLeaderPartitionToPeerLeaderReplicas()
                .get(existingPartition.toPathString());
        Set<ReplicaId> peerLeaderReplicasInClusterMap =
            new HashSet<>(existingPartition.getReplicaIdsByState(ReplicaState.LEADER, null));
        peerLeaderReplicasInClusterMap.remove(mockReplicaId);
        assertThat(
            "Mismatch in list of leader peer replicas stored by partition in replication manager and cluster map",
            peerLeaderReplicasInReplicationManager, is(peerLeaderReplicasInClusterMap));
      }
    }
    storageManager.shutdown();

    replicationConfig = initialReplicationConfig;
  }

  /**
   * Test state transition in replication manager from LEADER to STANDBY
   * Test setup: When creating partitions, make sure that there is exactly one replica in LEADER STATE on each data center
   * Test condition: When a partition on the current node moves from leader to standby, verify that in-memory map storing
   * partition to peer leader replicas is updated correctly
   * @throws Exception
   */
  @Test
  public void replicaFromLeaderToStandbyTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);

    ReplicationConfig initialReplicationConfig = replicationConfig;
    properties.setProperty("replication.model.across.datacenters", "LEADER_BASED");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));

    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();
    PartitionId existingPartition = replicationManager.partitionToPartitionInfo.keySet().iterator().next();
    mockHelixParticipant.onPartitionBecomeLeaderFromStandby(existingPartition.toPathString());
    Map<String, Set<ReplicaId>> peerLeaderReplicasByPartition =
        replicationManager.leaderBasedReplicationAdmin.getLeaderPartitionToPeerLeaderReplicas();
    assertTrue(
        "Partition is not present in the map of partition to peer leader replicas after it moved from standby to leader",
        peerLeaderReplicasByPartition.containsKey(existingPartition.toPathString()));
    mockHelixParticipant.onPartitionBecomeStandbyFromLeader(existingPartition.toPathString());
    assertFalse(
        "Partition is still present in the map of partition to peer leader replicas after it moved from leader to standby",
        peerLeaderReplicasByPartition.containsKey(existingPartition.toPathString()));
    storageManager.shutdown();

    replicationConfig = initialReplicationConfig;
  }

  /**
   * Test INACTIVE -> OFFLINE transition on existing replica (both success and failure cases)
   */
  @Test
  public void replicaFromInactiveToOfflineTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();
    // 1. test replica not found case
    try {
      mockHelixParticipant.onPartitionBecomeOfflineFromInactive("-1");
      fail("should fail because of invalid partition");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", ReplicaNotFound, e.getErrorCode());
    }
    // 2. test store not started case
    PartitionId existingPartition = replicationManager.partitionToPartitionInfo.keySet().iterator().next();
    storageManager.shutdownBlobStore(existingPartition);
    try {
      mockHelixParticipant.onPartitionBecomeOfflineFromInactive(existingPartition.toPathString());
      fail("should fail because store is not started");
    } catch (StateTransitionException e) {
      assertEquals("Error code doesn't match", StoreNotStarted, e.getErrorCode());
    }
    storageManager.startBlobStore(existingPartition);
    // before testing success case, let's write a blob (size = 100) into local store and add a delete record for new blob
    Store localStore = storageManager.getStore(existingPartition);
    MockId id = new MockId(TestUtils.getRandomString(10), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM));
    long crc = (new Random()).nextLong();
    long blobSize = 100;
    MessageInfo info =
        new MessageInfo(id, blobSize, false, false, Utils.Infinite_Time, crc, id.getAccountId(), id.getContainerId(),
            Utils.Infinite_Time);
    List<MessageInfo> infos = new ArrayList<>();
    List<ByteBuffer> buffers = new ArrayList<>();
    ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) blobSize));
    infos.add(info);
    buffers.add(buffer);
    localStore.put(new MockMessageWriteSet(infos, buffers));
    // delete the blob
    int deleteRecordSize = (int) (new DeleteMessageFormatInputStream(id, (short) 0, (short) 0, 0).getSize());
    MessageInfo deleteInfo =
        new MessageInfo(id, deleteRecordSize, id.getAccountId(), id.getContainerId(), time.milliseconds());
    localStore.delete(Collections.singletonList(deleteInfo));
    int sizeOfPutAndHeader = 100 + 18;
    int sizeOfWhole = sizeOfPutAndHeader + deleteRecordSize;
    // note that end offset of last PUT = 100 + 18 = 118, end offset of the store is sizeOfWhole
    // 3. test success case (create a new thread and trigger INACTIVE -> OFFLINE transition)
    ReplicaId localReplica = storageManager.getReplica(existingPartition.toPathString());
    // put a decommission-in-progress file into local store dir
    File decommissionFile = new File(localReplica.getReplicaPath(), "decommission_in_progress");
    assertTrue("Couldn't create decommission file in local store", decommissionFile.createNewFile());
    decommissionFile.deleteOnExit();
    assertNotSame("Before disconnection, the local store state shouldn't be OFFLINE", ReplicaState.OFFLINE,
        localStore.getCurrentState());
    mockHelixParticipant.registerPartitionStateChangeListener(StateModelListenerType.ReplicationManagerListener,
        replicationManager.replicationListener);
    CountDownLatch participantLatch = new CountDownLatch(1);
    replicationManager.listenerExecutionLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      mockHelixParticipant.onPartitionBecomeOfflineFromInactive(existingPartition.toPathString());
      participantLatch.countDown();
    }, false).start();
    assertTrue("Partition state change listener in ReplicationManager didn't get called within 1 sec",
        replicationManager.listenerExecutionLatch.await(1, TimeUnit.SECONDS));
    // the state of local store should be updated to OFFLINE
    assertEquals("Local store state is not expected", ReplicaState.OFFLINE, localStore.getCurrentState());
    // update replication lag between local and peer replicas
    List<RemoteReplicaInfo> remoteReplicaInfos =
        replicationManager.partitionToPartitionInfo.get(existingPartition).getRemoteReplicaInfos();
    ReplicaId peerReplica1 = remoteReplicaInfos.get(0).getReplicaId();
    ReplicaId peerReplica2 = remoteReplicaInfos.get(1).getReplicaId();
    // peer1 catches up with last PUT, peer2 catches up with end offset of local store. In this case, SyncUp is not complete
    replicationManager.updateTotalBytesReadByRemoteReplica(existingPartition,
        peerReplica1.getDataNodeId().getHostname(), peerReplica1.getReplicaPath(), sizeOfPutAndHeader);
    replicationManager.updateTotalBytesReadByRemoteReplica(existingPartition,
        peerReplica2.getDataNodeId().getHostname(), peerReplica2.getReplicaPath(), sizeOfWhole);
    assertFalse("Only one peer replica has fully caught up with end offset so sync-up should not complete",
        mockHelixParticipant.getReplicaSyncUpManager().isSyncUpComplete(localReplica));
    // make peer1 catch up with end offset
    replicationManager.updateTotalBytesReadByRemoteReplica(existingPartition,
        peerReplica1.getDataNodeId().getHostname(), peerReplica1.getReplicaPath(), sizeOfWhole);
    // Now, sync-up should complete and transition should be able to proceed.
    assertTrue("Inactive-To-Offline transition didn't complete within 1 sec",
        participantLatch.await(1, TimeUnit.SECONDS));
    assertFalse("Local store should be stopped after transition", localStore.isStarted());
    storageManager.shutdown();
  }

  /**
   * Test that resuming decommission on certain replica behaves correctly.
   * @throws Exception
   */
  @Test
  public void replicaResumeDecommissionTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = Mockito.spy(new MockHelixParticipant(clusterMapConfig));
    doNothing().when(mockHelixParticipant).setPartitionDisabledState(anyString(), anyBoolean());
    // choose a replica on local node and put decommission file into its dir
    ReplicaId localReplica = clusterMap.getReplicaIds(clusterMap.getDataNodeIds().get(0)).get(0);
    String partitionName = localReplica.getPartitionId().toPathString();
    File decommissionFile = new File(localReplica.getReplicaPath(), "decommission_in_progress");
    assertTrue("Can't create decommission file", decommissionFile.createNewFile());
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    // failure case 1: store is not started when resuming decommission
    storageManager.shutdownBlobStore(localReplica.getPartitionId());
    try {
      mockHelixParticipant.onPartitionBecomeDroppedFromOffline(partitionName);
      fail("should fail");
    } catch (StateTransitionException e) {
      assertEquals("Mismatch in error code", ReplicaOperationFailure, e.getErrorCode());
    }
    storageManager.startBlobStore(localReplica.getPartitionId());

    // failure case 2: fail to remove replica from InstanceConfig in Helix
    AmbryReplicaSyncUpManager replicaSyncUpManager =
        (AmbryReplicaSyncUpManager) mockHelixParticipant.getReplicaSyncUpManager();
    mockHelixParticipant.updateNodeInfoReturnVal = false;
    CountDownLatch executionLatch = new CountDownLatch(1);
    AtomicBoolean exceptionOccurred = new AtomicBoolean(false);
    Utils.newThread(() -> {
      try {
        mockHelixParticipant.onPartitionBecomeDroppedFromOffline(partitionName);
        fail("should fail because updating node info returns false");
      } catch (StateTransitionException e) {
        exceptionOccurred.getAndSet(true);
        assertEquals("Mismatch in error code", ReplicaOperationFailure, e.getErrorCode());
      } finally {
        executionLatch.countDown();
      }
    }, false).start();
    while (!replicaSyncUpManager.getPartitionToDeactivationLatch().containsKey(partitionName)) {
      Thread.sleep(100);
    }
    replicaSyncUpManager.onDeactivationComplete(localReplica);
    while (!replicaSyncUpManager.getPartitionToDisconnectionLatch().containsKey(partitionName)) {
      Thread.sleep(100);
    }
    replicaSyncUpManager.onDisconnectionComplete(localReplica);
    assertTrue("Offline-To-Dropped transition didn't complete within 1 sec", executionLatch.await(1, TimeUnit.SECONDS));
    assertTrue("State transition exception should be thrown", exceptionOccurred.get());
    mockHelixParticipant.updateNodeInfoReturnVal = null;
    storageManager.startBlobStore(localReplica.getPartitionId());

    // success case
    mockHelixParticipant.mockStatsManagerListener = Mockito.mock(PartitionStateChangeListener.class);
    doNothing().when(mockHelixParticipant.mockStatsManagerListener).onPartitionBecomeDroppedFromOffline(anyString());
    mockHelixParticipant.registerPartitionStateChangeListener(StateModelListenerType.StatsManagerListener,
        mockHelixParticipant.mockStatsManagerListener);
    CountDownLatch participantLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      mockHelixParticipant.onPartitionBecomeDroppedFromOffline(partitionName);
      participantLatch.countDown();
    }, false).start();
    while (!replicaSyncUpManager.getPartitionToDeactivationLatch().containsKey(partitionName)) {
      Thread.sleep(100);
    }
    replicaSyncUpManager.onDeactivationComplete(localReplica);
    while (!replicaSyncUpManager.getPartitionToDisconnectionLatch().containsKey(partitionName)) {
      Thread.sleep(100);
    }
    replicaSyncUpManager.onDisconnectionComplete(localReplica);
    assertTrue("Offline-To-Dropped transition didn't complete within 1 sec",
        participantLatch.await(1, TimeUnit.SECONDS));
    // verify stats manager listener is called
    verify(mockHelixParticipant.mockStatsManagerListener).onPartitionBecomeDroppedFromOffline(anyString());
    // verify setPartitionDisabledState method is called
    verify(mockHelixParticipant).setPartitionDisabledState(partitionName, false);
    File storeDir = new File(localReplica.getReplicaPath());
    assertFalse("Store dir should not exist", storeDir.exists());
    storageManager.shutdown();
  }

  /**
   * Tests pausing all partitions and makes sure that the replica thread pauses. Also tests that it resumes when one
   * eligible partition is re-enabled and that replication completes successfully.
   * @throws Exception
   */
  @Test
  public void replicationAllPauseTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();

    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add 10 messages into each partition and place it on remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 10);
    }

    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    MockStoreKeyConverterFactory mockStoreKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    mockStoreKeyConverterFactory.setReturnInputIfAbsent(true);
    mockStoreKeyConverterFactory.setConversionMap(new HashMap<>());

    int batchSize = 4;
    StoreKeyConverter storeKeyConverter = mockStoreKeyConverterFactory.getStoreKeyConverter();
    Transformer transformer = new ValidatingTransformer(storeKeyFactory, storeKeyConverter);
    CountDownLatch readyToPause = new CountDownLatch(1);
    CountDownLatch readyToProceed = new CountDownLatch(1);
    AtomicReference<CountDownLatch> reachedLimitLatch = new AtomicReference<>(new CountDownLatch(1));
    AtomicReference<Exception> exception = new AtomicReference<>();
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer,
            (store, messageInfos) -> {
              try {
                readyToPause.countDown();
                readyToProceed.await();
                if (store.messageInfos.size() == remoteHost.infosByPartition.get(store.id).size()) {
                  reachedLimitLatch.get().countDown();
                }
              } catch (Exception e) {
                exception.set(e);
              }
            }, null, remoteHost);
    ReplicaThread replicaThread = replicasAndThread.getSecond();
    Thread thread = Utils.newThread(replicaThread, false);
    thread.start();

    assertEquals("There should be no disabled partitions", 0, replicaThread.getReplicationDisabledPartitions().size());
    // wait to pause replication
    readyToPause.await(10, TimeUnit.SECONDS);
    replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), false);
    Set<PartitionId> expectedPaused = new HashSet<>(clusterMap.getAllPartitionIds(null));
    assertEquals("Disabled partitions sets do not match", expectedPaused,
        replicaThread.getReplicationDisabledPartitions());
    // signal the replica thread to move forward
    readyToProceed.countDown();
    // wait for the thread to go into waiting state
    assertTrue("Replica thread did not go into waiting state",
        TestUtils.waitUntilExpectedState(thread, Thread.State.WAITING, 10000));
    // unpause one partition
    replicaThread.controlReplicationForPartitions(Collections.singletonList(partitionIds.get(0)), true);
    expectedPaused.remove(partitionIds.get(0));
    assertEquals("Disabled partitions sets do not match", expectedPaused,
        replicaThread.getReplicationDisabledPartitions());
    // wait for it to catch up
    reachedLimitLatch.get().await(10, TimeUnit.SECONDS);
    // reset limit
    reachedLimitLatch.set(new CountDownLatch(partitionIds.size() - 1));
    // unpause all partitions
    replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), true);
    assertEquals("There should be no disabled partitions", 0, replicaThread.getReplicationDisabledPartitions().size());
    // wait until all catch up
    reachedLimitLatch.get().await(10, TimeUnit.SECONDS);
    // shutdown
    replicaThread.shutdown();
    if (exception.get() != null) {
      throw exception.get();
    }
    Map<PartitionId, List<MessageInfo>> missingInfos = remoteHost.getMissingInfos(localHost.infosByPartition);
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : missingInfos.entrySet()) {
      assertEquals("No infos should be missing", 0, entry.getValue().size());
    }
    Map<PartitionId, List<ByteBuffer>> missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      assertEquals("No buffers should be missing", 0, entry.getValue().size());
    }
  }

  /**
   * Test that max partition count per request is honored in {@link ReplicaThread} if there are too many partitions to
   * replicate from the remote node.
   * @throws Exception
   */
  @Test
  public void limitMaxPartitionCountPerRequestTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();

    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add 5 messages into each partition and place it on remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 5);
    }
    StoreKeyFactory storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    MockStoreKeyConverterFactory mockStoreKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    mockStoreKeyConverterFactory.setReturnInputIfAbsent(true);
    mockStoreKeyConverterFactory.setConversionMap(new HashMap<>());
    // we set batchSize to 10 in order to get all messages from one partition within single replication cycle
    int batchSize = 10;
    StoreKeyConverter storeKeyConverter = mockStoreKeyConverterFactory.getStoreKeyConverter();
    Transformer transformer = new ValidatingTransformer(storeKeyFactory, storeKeyConverter);
    // we set max partition count per request to 5, which forces thread to replicate replicas in two cycles. (Note that
    // number of partition to replicate is 10, it's 9 default partition class and one special class. They will be
    // replicated in two batches)
    ReplicationConfig initialReplicationConfig = replicationConfig;
    properties.setProperty("replication.max.partition.count.per.request", String.valueOf(5));
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));
    CountDownLatch replicationCompleted = new CountDownLatch(partitionIds.size());
    AtomicReference<Exception> exception = new AtomicReference<>();
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer,
            (store, messageInfos) -> {
              try {
                replicationCompleted.countDown();
                // for each partition, replication should complete within single cycle (fetch once should suffice), so
                // we shut down local store once blobs are written. This can avoid unnecessary metadata requests sent to
                // remote host.
                store.shutdown();
              } catch (Exception e) {
                exception.set(e);
              }
            }, null, remoteHost);
    ReplicaThread replicaThread = replicasAndThread.getSecond();
    Thread thread = Utils.newThread(replicaThread, false);
    thread.start();
    assertTrue("Replication didn't complete within 10 secs", replicationCompleted.await(10, TimeUnit.SECONDS));
    // verify the # of replicas per metadata request is limited to 5 (note that there are 10 replicas to replicate, they
    // are split into to 2 small batches and get replicated in separate requests)
    assertEquals("There should be 2 metadata requests and each has 5 replicas to replicate", Arrays.asList(5, 5),
        remoteHost.replicaCountPerRequestTracker);
    // shutdown
    replicaThread.shutdown();
    if (exception.get() != null) {
      throw exception.get();
    }
    replicationConfig = initialReplicationConfig;
  }

  /**
   * Tests pausing replication for all and individual partitions. Also tests replication will pause on store that is not
   * started and resume when store restarted.
   * @throws Exception
   */
  @Test
  public void replicationPauseTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();

    List<PartitionId> partitionIds = clusterMap.getAllPartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add  10 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 10);
    }

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    StoreKeyConverter storeKeyConverter = storeKeyConverterFactory.getStoreKeyConverter();
    Transformer transformer = new ValidatingTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    Map<PartitionId, Integer> progressTracker = new HashMap<>();
    PartitionId partitionToResumeFirst = clusterMap.getAllPartitionIds(null).get(0);
    PartitionId partitionToShutdownLocally = clusterMap.getAllPartitionIds(null).get(1);
    boolean allStopped = false;
    boolean onlyOneResumed = false;
    boolean allReenabled = false;
    boolean shutdownStoreRestarted = false;
    Set<PartitionId> expectedPaused = new HashSet<>();
    assertEquals("There should be no disabled partitions", expectedPaused,
        replicaThread.getReplicationDisabledPartitions());
    while (true) {
      replicaThread.replicate();
      boolean replicationDone = true;
      for (RemoteReplicaInfo replicaInfo : replicasToReplicate.get(remoteHost.dataNodeId)) {
        PartitionId id = replicaInfo.getReplicaId().getPartitionId();
        MockFindToken token = (MockFindToken) replicaInfo.getToken();
        int lastProgress = progressTracker.computeIfAbsent(id, id1 -> 0);
        int currentProgress = token.getIndex();
        boolean partDone = currentProgress + 1 == remoteHost.infosByPartition.get(id).size();
        if (allStopped || (onlyOneResumed && !id.equals(partitionToResumeFirst)) || (allReenabled
            && !shutdownStoreRestarted && id.equals(partitionToShutdownLocally))) {
          assertEquals("There should have been no progress", lastProgress, currentProgress);
        } else if (!partDone) {
          assertTrue("There has been no progress", currentProgress > lastProgress);
          progressTracker.put(id, currentProgress);
        }
        replicationDone = replicationDone && partDone;
      }
      if (!allStopped && !onlyOneResumed && !allReenabled && !shutdownStoreRestarted) {
        replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), false);
        expectedPaused.addAll(clusterMap.getAllPartitionIds(null));
        assertEquals("Disabled partitions sets do not match", expectedPaused,
            replicaThread.getReplicationDisabledPartitions());
        allStopped = true;
      } else if (!onlyOneResumed && !allReenabled && !shutdownStoreRestarted) {
        // resume replication for first partition
        replicaThread.controlReplicationForPartitions(Collections.singletonList(partitionIds.get(0)), true);
        expectedPaused.remove(partitionIds.get(0));
        assertEquals("Disabled partitions sets do not match", expectedPaused,
            replicaThread.getReplicationDisabledPartitions());
        allStopped = false;
        onlyOneResumed = true;
      } else if (!allReenabled && !shutdownStoreRestarted) {
        // not removing the first partition
        replicaThread.controlReplicationForPartitions(clusterMap.getAllPartitionIds(null), true);
        // shutdown one local store to pause replication against that store
        localHost.storesByPartition.get(partitionToShutdownLocally).shutdown();
        onlyOneResumed = false;
        allReenabled = true;
        expectedPaused.clear();
        assertEquals("Disabled partitions sets do not match", expectedPaused,
            replicaThread.getReplicationDisabledPartitions());
      } else if (!shutdownStoreRestarted) {
        localHost.storesByPartition.get(partitionToShutdownLocally).start();
        shutdownStoreRestarted = true;
      }
      if (replicationDone) {
        break;
      }
    }

    Map<PartitionId, List<MessageInfo>> missingInfos = remoteHost.getMissingInfos(localHost.infosByPartition);
    for (Map.Entry<PartitionId, List<MessageInfo>> entry : missingInfos.entrySet()) {
      assertEquals("No infos should be missing", 0, entry.getValue().size());
    }
    Map<PartitionId, List<ByteBuffer>> missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      assertEquals("No buffers should be missing", 0, entry.getValue().size());
    }
  }

  /**
   * Tests that replication between a local and remote server who have different
   * blob IDs for the same blobs (via StoreKeyConverter)
   * @throws Exception
   */
  @Test
  public void replicaThreadTestConverter() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockHost expectedLocalHost = new MockHost(localHost.dataNodeId, clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition = new HashMap<>();

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();
    /*
        STORE KEY CONVERTER MAPPING
        Key     Value
        B0      B0'
        B1      B1'
        B2      null

        BEFORE
        Local   Remote
        B0'     B0
                B1
                B2

        AFTER
        Local   Remote
        B0'     B0
        B1'     B1
                B2
        B0 is B0' for local,
        B1 is B1' for local,
        B2 is null for local,
        so it already has B0/B0'
        B1 is transferred to B1'
        and B2 is invalid for L
        so it does not count as missing
        Missing Keys: 1
    */
    Map<PartitionId, List<BlobId>> partitionIdToDeleteBlobId = new HashMap<>();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    Map<PartitionId, BlobId> expectedPartitionIdToDeleteBlobId = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      List<BlobId> deleteBlobIds = new ArrayList<>();
      partitionIdToDeleteBlobId.put(partitionId, deleteBlobIds);
      BlobId b0 = generateRandomBlobId(partitionId);
      deleteBlobIds.add(b0);
      BlobId b0p = generateRandomBlobId(partitionId);
      expectedPartitionIdToDeleteBlobId.put(partitionId, b0p);
      BlobId b1 = generateRandomBlobId(partitionId);
      BlobId b1p = generateRandomBlobId(partitionId);
      BlobId b2 = generateRandomBlobId(partitionId);
      deleteBlobIds.add(b2);
      conversionMap.put(b0, b0p);
      conversionMap.put(b1, b1p);
      conversionMap.put(b2, null);
      //Convert current conversion map so that BlobIdTransformer can
      //create b1p in expectedLocalHost
      storeKeyConverter.setConversionMap(conversionMap);
      storeKeyConverter.convert(conversionMap.keySet());
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0p), Arrays.asList(localHost));
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0, b1, b2), Arrays.asList(remoteHost));
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0p, b1), Arrays.asList(null, transformer),
          Arrays.asList(expectedLocalHost));

      //Check that expected local host contains the correct blob ids
      Set<BlobId> expectedLocalHostBlobIds = new HashSet<>();
      expectedLocalHostBlobIds.add(b0p);
      expectedLocalHostBlobIds.add(b1p);
      for (MessageInfo messageInfo : expectedLocalHost.infosByPartition.get(partitionId)) {
        assertTrue("Remove should never fail", expectedLocalHostBlobIds.remove(messageInfo.getStoreKey()));
      }
      assertTrue("expectedLocalHostBlobIds should now be empty", expectedLocalHostBlobIds.isEmpty());
    }
    storeKeyConverter.setConversionMap(conversionMap);

    int expectedIndex = assertMissingKeysAndFixMissingStoreKeys(0, 2, 1, replicaThread, replicasToReplicate);

    //Check that there are no missing buffers between expectedLocalHost and LocalHost
    Map<PartitionId, List<ByteBuffer>> missingBuffers =
        expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());
    missingBuffers = localHost.getMissingBuffers(expectedLocalHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());
    /*
        BEFORE
        Local   Remote
        B0'     B0
        B1'     B1
                B2
                dB0 (delete B0)
                dB2

        AFTER
        Local   Remote
        B0'     B0
        B1'     B1
        dB0'    B2
                dB0
                dB2
        delete B0 gets converted
        to delete B0' in Local
        Missing Keys: 0
     */
    //delete blob
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      List<BlobId> deleteBlobIds = partitionIdToDeleteBlobId.get(partitionId);
      for (BlobId deleteBlobId : deleteBlobIds) {
        addDeleteMessagesToReplicasOfPartition(partitionId, deleteBlobId, Arrays.asList(remoteHost));
      }
      addDeleteMessagesToReplicasOfPartition(partitionId, expectedPartitionIdToDeleteBlobId.get(partitionId),
          Arrays.asList(expectedLocalHost));
    }

    expectedIndex = assertMissingKeysAndFixMissingStoreKeys(expectedIndex, 2, 0, replicaThread, replicasToReplicate);

    //Check that there are no missing buffers between expectedLocalHost and LocalHost
    missingBuffers = expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());
    missingBuffers = localHost.getMissingBuffers(expectedLocalHost.buffersByPartition);
    assertTrue(missingBuffers.isEmpty());

    // Sleep for some time so no replicas are in backoff mode.
    time.sleep(1000);
    // 3 unconverted + 2 unconverted deleted expected missing buffers
    replicaThread.replicate();
    verifyNoMoreMissingKeysAndExpectedMissingBufferCount(remoteHost, localHost, replicaThread, replicasToReplicate,
        idsToBeIgnoredByPartition, storeKeyConverter, expectedIndex, expectedIndex, 5);
  }

  /**
   * Tests if deprecated containers have been blocked during replication.
   */
  @Test
  public void blockDeprecatedContainerReplicationTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    Properties properties = new Properties();
    properties.setProperty("replication.container.deletion.enabled", "true");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    storeKeyConverter.setConversionMap(conversionMap);
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);

    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      BlobId b0 = generateRandomBlobId(partitionId);
      conversionMap.put(b0, b0);
      BlobId b1 = generateRandomBlobId(partitionId);
      conversionMap.put(b1, b1);
      // add 2 messages to both hosts.
      storeKeyConverter.setConversionMap(conversionMap);
      storeKeyConverter.convert(conversionMap.keySet());
      //addPutMessagesToReplicasOfPartition(Arrays.asList(b0), Arrays.asList(localHost, remoteHost));
      // add 3 messages to the remote host only
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0, b1), Collections.singletonList(remoteHost));
    }

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    ReplicationMetrics replicationMetrics =
        new ReplicationMetrics(new MetricRegistry(), clusterMap.getReplicaIds(localHost.dataNodeId));
    replicationMetrics.populateSingleColoMetrics(remoteHost.dataNodeId.getDatacenterName());

    List<RemoteReplicaInfo> remoteReplicaInfoList = localHost.getRemoteReplicaInfos(remoteHost, null);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate =
        Collections.singletonMap(remoteHost.dataNodeId, remoteReplicaInfoList);
    storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    hosts.put(remoteHost.dataNodeId, remoteHost);
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);
    Predicate<MessageInfo> skipPredicate = new ReplicationSkipPredicate(accountService, replicationConfig);
    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", new MockFindTokenHelper(storeKeyFactory, replicationConfig), clusterMap,
            new AtomicInteger(0), localHost.dataNodeId, connectionPool, replicationConfig, replicationMetrics, null,
            storeKeyConverter, transformer, clusterMap.getMetricRegistry(), false,
            localHost.dataNodeId.getDatacenterName(), new ResponseHandler(clusterMap), time, null, skipPredicate);
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfoList) {
      replicaThread.addRemoteReplicaInfo(remoteReplicaInfo);
    }
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasToReplicate.get(remoteHost.dataNodeId);

    DataNodeId remoteNode = remoteReplicaInfos.get(0).getReplicaId().getDataNodeId();
    ReplicaMetadataResponse response = replicaThread.getReplicaMetadataResponse(remoteReplicaInfos,
        new MockConnectionPool.MockConnection(remoteHost, batchSize), remoteNode);
    //case1 DELETE_IN_PROGRESS container with retention time qualified.
    for (int i = 0; i < 2; i++) {
      RemoteReplicaInfo remoteReplicaInfo = remoteReplicaInfos.get(i);
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo = response.getReplicaMetadataResponseInfoList().get(i);
      new ResponseHandler(clusterMap).onEvent(remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
      for (int j = 0; j < replicaMetadataResponseInfo.getMessageInfoList().size(); j++) {
        short accountId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getAccountId();
        short containerId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getContainerId();
        Container container = Mockito.mock(Container.class);
        Account account = Mockito.mock(Account.class);
        Mockito.when(account.getContainerById(containerId)).thenReturn(container);
        Mockito.when(accountService.getAccountById(accountId)).thenReturn(account);
        Mockito.when(container.getDeleteTriggerTime())
            .thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(
                replicationConfig.replicationContainerDeletionRetentionDays + 1));
        Mockito.when(container.getStatus()).thenReturn(Container.ContainerStatus.DELETE_IN_PROGRESS);
      }
      Set<MessageInfo> remoteMissingStoreKeys =
          replicaThread.getMissingStoreMessages(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
      assertEquals("All DELETE_IN_PROGRESS blobs qualified with retention time should be skipped during replication", 0,
          remoteMissingStoreKeys.size());
      Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap = replicaThread.batchConvertReplicaMetadataResponseKeys(response);
      replicaThread.processReplicaMetadataResponse(remoteMissingStoreKeys, replicaMetadataResponseInfo,
          remoteReplicaInfo, remoteNode, remoteKeyToLocalKeyMap);
    }
    //case2 DELETE_IN_PROGRESS container with retention time not qualified.
    for (int i = 2; i < 4; i++) {
      RemoteReplicaInfo remoteReplicaInfo = remoteReplicaInfos.get(i);
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo = response.getReplicaMetadataResponseInfoList().get(i);
      new ResponseHandler(clusterMap).onEvent(remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
      for (int j = 0; j < replicaMetadataResponseInfo.getMessageInfoList().size(); j++) {
        short accountId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getAccountId();
        short containerId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getContainerId();
        Container container = Mockito.mock(Container.class);
        Account account = Mockito.mock(Account.class);
        Mockito.when(account.getContainerById(containerId)).thenReturn(container);
        Mockito.when(accountService.getAccountById(accountId)).thenReturn(account);
        Mockito.when(container.getStatus()).thenReturn(Container.ContainerStatus.DELETE_IN_PROGRESS);
        Mockito.when(container.getDeleteTriggerTime()).thenReturn(System.currentTimeMillis());
      }
      Set<MessageInfo> remoteMissingStoreKeys =
          replicaThread.getMissingStoreMessages(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
      assertEquals(
          "All DELETE_IN_PROGRESS blobs not qualified with retention time should not be skipped during replication", 2,
          remoteMissingStoreKeys.size());
      Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap = replicaThread.batchConvertReplicaMetadataResponseKeys(response);
      replicaThread.processReplicaMetadataResponse(remoteMissingStoreKeys, replicaMetadataResponseInfo,
          remoteReplicaInfo, remoteNode, remoteKeyToLocalKeyMap);
    }
    //case3 INACTIVE container
    for (int i = 4; i < 6; i++) {
      RemoteReplicaInfo remoteReplicaInfo = remoteReplicaInfos.get(i);
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo = response.getReplicaMetadataResponseInfoList().get(i);
      new ResponseHandler(clusterMap).onEvent(remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
      for (int j = 0; j < replicaMetadataResponseInfo.getMessageInfoList().size(); j++) {
        short accountId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getAccountId();
        short containerId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getContainerId();
        Container container = Mockito.mock(Container.class);
        Account account = Mockito.mock(Account.class);
        Mockito.when(account.getContainerById(containerId)).thenReturn(container);
        Mockito.when(accountService.getAccountById(accountId)).thenReturn(account);
        Mockito.when(container.getStatus()).thenReturn(Container.ContainerStatus.INACTIVE);
      }
      Set<MessageInfo> remoteMissingStoreKeys =
          replicaThread.getMissingStoreMessages(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
      assertEquals("All INACTIVE blobs should be skipped during replication", 0, remoteMissingStoreKeys.size());
      Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap = replicaThread.batchConvertReplicaMetadataResponseKeys(response);
      replicaThread.processReplicaMetadataResponse(remoteMissingStoreKeys, replicaMetadataResponseInfo,
          remoteReplicaInfo, remoteNode, remoteKeyToLocalKeyMap);
    }
    //case 4 ACTIVE Container
    for (int i = 6; i < 8; i++) {
      RemoteReplicaInfo remoteReplicaInfo = remoteReplicaInfos.get(i);
      ReplicaMetadataResponseInfo replicaMetadataResponseInfo = response.getReplicaMetadataResponseInfoList().get(i);
      new ResponseHandler(clusterMap).onEvent(remoteReplicaInfo.getReplicaId(), replicaMetadataResponseInfo.getError());
      for (int j = 0; j < replicaMetadataResponseInfo.getMessageInfoList().size(); j++) {
        short accountId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getAccountId();
        short containerId = replicaMetadataResponseInfo.getMessageInfoList().get(j).getContainerId();
        Container container = Mockito.mock(Container.class);
        Account account = Mockito.mock(Account.class);
        Mockito.when(account.getContainerById(containerId)).thenReturn(container);
        Mockito.when(accountService.getAccountById(accountId)).thenReturn(account);
        Mockito.when(container.getStatus()).thenReturn(Container.ContainerStatus.ACTIVE);
      }
      Set<MessageInfo> remoteMissingStoreKeys =
          replicaThread.getMissingStoreMessages(replicaMetadataResponseInfo, remoteNode, remoteReplicaInfo);
      assertEquals("All non-deprecated blobs should not be skipped during replication", 2,
          remoteMissingStoreKeys.size());
      Map<StoreKey, StoreKey> remoteKeyToLocalKeyMap = replicaThread.batchConvertReplicaMetadataResponseKeys(response);
      replicaThread.processReplicaMetadataResponse(remoteMissingStoreKeys, replicaMetadataResponseInfo,
          remoteReplicaInfo, remoteNode, remoteKeyToLocalKeyMap);
    }
  }

  /**
   * Tests replication of TTL updates
   * @throws Exception
   */
  @Test
  public void ttlUpdateReplicationTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockHost expectedLocalHost = new MockHost(localHost.dataNodeId, clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    storeKeyConverter.setConversionMap(conversionMap);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    int numMessagesInEachPart = 0;
    Map<PartitionId, StoreKey> idsDeletedLocallyByPartition = new HashMap<>();
    List<MockHost> remoteHostOnly = Collections.singletonList(remoteHost);
    List<MockHost> expectedLocalHostOnly = Collections.singletonList(expectedLocalHost);
    List<MockHost> localHostAndExpectedLocalHost = Arrays.asList(localHost, expectedLocalHost);
    List<MockHost> remoteHostAndExpectedLocalHost = Arrays.asList(remoteHost, expectedLocalHost);
    List<MockHost> allHosts = Arrays.asList(localHost, expectedLocalHost, remoteHost);
    for (PartitionId pid : partitionIds) {
      // add 3 put messages to both hosts (also add to expectedLocal)
      List<StoreKey> ids = addPutMessagesToReplicasOfPartition(pid, allHosts, 3);
      // delete 1 of the messages in the local host only
      addDeleteMessagesToReplicasOfPartition(pid, ids.get(0), localHostAndExpectedLocalHost);
      idsDeletedLocallyByPartition.put(pid, ids.get(0));
      // ttl update 1 of the messages in the local host only
      addTtlUpdateMessagesToReplicasOfPartition(pid, ids.get(1), localHostAndExpectedLocalHost, UPDATED_EXPIRY_TIME_MS);

      // remote host only
      // add 2 put messages
      ids.addAll(addPutMessagesToReplicasOfPartition(pid, remoteHostOnly, 1));
      ids.addAll(addPutMessagesToReplicasOfPartition(pid, remoteHostAndExpectedLocalHost, 1));
      // ttl update all 5 put messages
      for (int i = ids.size() - 1; i >= 0; i--) {
        List<MockHost> hostList = remoteHostOnly;
        if (i == 2 || i == 4) {
          hostList = remoteHostAndExpectedLocalHost;
        }
        // doing it in reverse order so that a put and ttl update arrive in the same batch
        addTtlUpdateMessagesToReplicasOfPartition(pid, ids.get(i), hostList, UPDATED_EXPIRY_TIME_MS);
      }
      // delete one of the keys that has put and ttl update on local host
      addDeleteMessagesToReplicasOfPartition(pid, ids.get(1), remoteHostAndExpectedLocalHost);
      // delete one of the keys that has put and ttl update on remote only
      addDeleteMessagesToReplicasOfPartition(pid, ids.get(3), remoteHostOnly);

      // add a TTL update and delete message without a put msg (compaction can create such a situation)
      BlobId id = generateRandomBlobId(pid);
      addTtlUpdateMessagesToReplicasOfPartition(pid, id, remoteHostOnly, UPDATED_EXPIRY_TIME_MS);
      addDeleteMessagesToReplicasOfPartition(pid, id, remoteHostOnly);

      // message transformation test cases
      // a blob ID with PUT and TTL update in both remote and local
      BlobId b0 = generateRandomBlobId(pid);
      BlobId b0p = generateRandomBlobId(pid);
      // a blob ID with a PUT in the local and PUT and TTL update in remote (with mapping)
      BlobId b1 = generateRandomBlobId(pid);
      BlobId b1p = generateRandomBlobId(pid);
      // a blob ID with PUT and TTL update in remote only (with mapping)
      BlobId b2 = generateRandomBlobId(pid);
      BlobId b2p = generateRandomBlobId(pid);
      // a blob ID with PUT and TTL update in remote (no mapping)
      BlobId b3 = generateRandomBlobId(pid);
      conversionMap.put(b0, b0p);
      conversionMap.put(b1, b1p);
      conversionMap.put(b2, b2p);
      conversionMap.put(b3, null);

      storeKeyConverter.convert(conversionMap.keySet());
      // add as required on local, remote and expected local
      // only PUT of b0p and b1p on local
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0p, b1p), localHostAndExpectedLocalHost);
      // PUT of b0,b1,b2,b3 on remote
      addPutMessagesToReplicasOfPartition(Arrays.asList(b0, b1, b2, b3), remoteHostOnly);
      // PUT of b0, b1, b2 expected in local at the end
      addPutMessagesToReplicasOfPartition(Collections.singletonList(b2), Collections.singletonList(transformer),
          expectedLocalHostOnly);

      // TTL update of b0 on all hosts
      addTtlUpdateMessagesToReplicasOfPartition(pid, b0p, localHostAndExpectedLocalHost, UPDATED_EXPIRY_TIME_MS);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b0, remoteHostOnly, UPDATED_EXPIRY_TIME_MS);
      // TTL update on b1, b2 and b3 on remote
      addTtlUpdateMessagesToReplicasOfPartition(pid, b1, remoteHostOnly, UPDATED_EXPIRY_TIME_MS);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b1p, expectedLocalHostOnly, UPDATED_EXPIRY_TIME_MS);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b2, remoteHostOnly, UPDATED_EXPIRY_TIME_MS);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b2p, expectedLocalHostOnly, UPDATED_EXPIRY_TIME_MS);
      addTtlUpdateMessagesToReplicasOfPartition(pid, b3, remoteHostOnly, UPDATED_EXPIRY_TIME_MS);

      numMessagesInEachPart = remoteHost.infosByPartition.get(pid).size();
    }

    // After the for loop above, we have records in hosts just like below
    // L|id0|id1|id2|id0D|id1T|   |   |    |    |    |    |    |    |    |   |   |b0p|b1p|   |  |b0pT|    |    |    |
    // R|id0|id1|id2|    |    |id3|id4|id4T|id3T|id2T|id1T|id0T|id1D|id3D|idT|idD|b0 |b1 |b2 |b3|b0T |b1T |b2T | b3T|
    // E|id0|id1|id2|id0D|id1T|   |id4|id4T|    |id2T|    |    |id1D|    |   |   |b0p|b1p|b2p|  |b0pT|b1pT|b2pT|    |
    //
    // converter map: b0->b0p, b1->b1p, b2->b2p, b3->null

    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasAndThread.getFirst().get(remoteHost.dataNodeId);
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    Map<PartitionId, List<ByteBuffer>> missingBuffers =
        expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    // We can see from the table in the comments above, Local has 7 records less than expected local.
    for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
      assertEquals("Missing buffers count mismatch", 7, entry.getValue().size());
    }

    // 1st iteration - 0 missing keys (3 puts already present, one put missing but del in remote, 1 ttl update will be
    // applied, 1 delete will be applied): Remote returns: id0T, id1TD, id2T, id3TD. id3 put missing, but it's deleted.
    // id1 apply delete, id2 apply ttl update. Token index is pointing to id3.
    // 2nd iteration - 1 missing key, 1 of which will also be ttl updated (one key with put + ttl update missing but
    // del in remote, one put and ttl update replicated): Remote returns: id3TD, id4T. id4 put missing, id3 deleted.
    // Token index is pointing to id3T.
    // 3rd iteration - 0 missing keys (1 ttl update missing but del in remote, 1 already ttl updated in iter 1, 1 key
    // already ttl updated in local, 1 key del local): Remote returns: id3TD, id2T, id1TD, id0T. Token index is pointing
    // to id0T.
    // 4th iteration - 0 missing keys (1 key del local, 1 key already deleted, 1 key missing but del in remote, 1 key
    // with ttl update missing but del remote): Remote returns: id0T, id1D, id3TD, idTD. Token index is pointing to idT.
    // 5th iteration - 0 missing keys (1 key - two records - missing but del remote, 2 puts already present but TTL
    // update of one of them is applied): Remote returns: idTD, b0T, b1T. b1 apply ttl update. Token index is pointing to
    // b1.
    // 6th iteration - 1 missing key (put + ttl update for a key, 1 deprecated id ignored, 1 TTL update already applied):
    // Remote returns: b1T, b2T, b3T, b0T. b2 missing, and ttl updated. b3 has no local key.
    // 7th iteration - 0 missing keys (2 TTL updates already applied, 1 TTL update of a deprecated ID ignored)
    //                                                                                                              |1st iter |2nd iter|3rd iter|4th iter|5th iter|6th iter|7th iter|
    // L|id0|id1|id2|id0D|id1T|   |   |    |    |    |    |    |    |    |   |   |b0p|b1p|   |  |b0pT|    |    |    |id1D|id2T|id4|id4T|        |        |b1pT    |b2p|b2pT|
    // R|id0|id1|id2|    |    |id3|id4|id4T|id3T|id2T|id1T|id0T|id1D|id3D|idT|idD|b0 |b1 |b2 |b3|b0T |b1T |b2T | b3T|
    // E|id0|id1|id2|id0D|id1T|   |id4|id4T|    |id2T|    |    |id1D|    |   |   |b0p|b1p|b2p|  |b0pT|b1pT|b2pT|    |

    int[] missingKeysCounts = {0, 1, 0, 0, 0, 1, 0};
    int[] missingBuffersCount = {5, 3, 3, 3, 2, 0, 0};
    int expectedIndex = 0;
    int missingBuffersIndex = 0;

    for (int missingKeysCount : missingKeysCounts) {
      expectedIndex = Math.min(expectedIndex + batchSize, numMessagesInEachPart) - 1;
      replicaThread.replicate();
      List<ReplicaThread.ExchangeMetadataResponse> response =
          replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
      assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
      for (int i = 0; i < response.size(); i++) {
        assertEquals(missingKeysCount, response.get(i).missingStoreMessages.size());
        assertEquals(expectedIndex, ((MockFindToken) response.get(i).remoteToken).getIndex());
      }
      missingBuffers = expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
        assertEquals("Missing buffers count mismatch for iteration count " + missingBuffersIndex,
            missingBuffersCount[missingBuffersIndex], entry.getValue().size());
      }
      missingBuffersIndex++;
    }

    // no more missing keys
    replicaThread.replicate();
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
    for (ReplicaThread.ExchangeMetadataResponse metadata : response) {
      assertEquals(0, metadata.missingStoreMessages.size());
      assertEquals(expectedIndex, ((MockFindToken) metadata.remoteToken).getIndex());
    }

    missingBuffers = expectedLocalHost.getMissingBuffers(localHost.buffersByPartition);
    assertEquals("There should be no missing buffers", 0, missingBuffers.size());

    // validate everything
    for (Map.Entry<PartitionId, List<MessageInfo>> remoteInfoEntry : remoteHost.infosByPartition.entrySet()) {
      List<MessageInfo> remoteInfos = remoteInfoEntry.getValue();
      List<MessageInfo> localInfos = localHost.infosByPartition.get(remoteInfoEntry.getKey());
      Set<StoreKey> seen = new HashSet<>();
      for (MessageInfo remoteInfo : remoteInfos) {
        StoreKey remoteId = remoteInfo.getStoreKey();
        if (seen.add(remoteId)) {
          StoreKey localId = storeKeyConverter.convert(Collections.singleton(remoteId)).get(remoteId);
          MessageInfo localInfo = getMessageInfo(localId, localInfos, false, false, false);
          if (localId == null) {
            // this is a deprecated ID. There should be no messages locally
            assertNull(remoteId + " is deprecated and should have no entries", localInfo);
          } else {
            MessageInfo mergedRemoteInfo = getMergedMessageInfo(remoteId, remoteInfos);
            if (localInfo == null) {
              // local has no put, must be deleted on remote
              assertTrue(localId + ":" + remoteId + " not replicated", mergedRemoteInfo.isDeleted());
            } else {
              // local has a put and must be either at or beyond the state of the remote (based on ops above)
              MessageInfo mergedLocalInfo = getMergedMessageInfo(localId, localInfos);
              if (mergedRemoteInfo.isDeleted()) {
                // delete on remote, should be deleted locally too
                assertTrue(localId + ":" + remoteId + " is deleted on remote but not locally",
                    mergedLocalInfo.isDeleted());
              } else if (mergedRemoteInfo.isTtlUpdated() && !idsDeletedLocallyByPartition.get(remoteInfoEntry.getKey())
                  .equals(localId)) {
                // ttl updated on remote, should be ttl updated locally too
                assertTrue(localId + ":" + remoteId + " is updated on remote but not locally",
                    mergedLocalInfo.isTtlUpdated());
              } else if (!idsDeletedLocallyByPartition.get(remoteInfoEntry.getKey()).equals(localId)) {
                // should not be updated or deleted locally
                assertFalse(localId + ":" + remoteId + " has been updated", mergedLocalInfo.isTtlUpdated());
                assertFalse(localId + ":" + remoteId + " has been deleted", mergedLocalInfo.isDeleted());
              }
            }
          }
        }
      }
    }
  }

  /**
   * Test the case where a blob gets deleted after a replication metadata exchange completes and identifies the blob as
   * a candidate. The subsequent GetRequest should succeed as Replication makes a Include_All call, and
   * fixMissingStoreKeys() should succeed without exceptions. The blob should not be put locally.
   */
  @Test
  public void deletionAfterMetadataExchangeTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    int batchSize = 400;
    ReplicationTestSetup testSetup = new ReplicationTestSetup(batchSize);
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    List<PartitionId> partitionIds = testSetup.partitionIds;
    MockHost remoteHost = testSetup.remoteHost;
    MockHost localHost = testSetup.localHost;
    Map<PartitionId, Set<StoreKey>> idsToExpectByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);

      // add 5 messages to remote host only.
      Set<StoreKey> expectedIds =
          new HashSet<>(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 5));

      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      boolean toEncrypt = TestUtils.RANDOM.nextBoolean();

      // add an expired message to the remote host only
      StoreKey id =
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
              partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      PutMsgInfoAndBuffer msgInfoAndBuffer = createPutMessage(id, accountId, containerId, toEncrypt);
      remoteHost.addMessage(partitionId,
          new MessageInfo(id, msgInfoAndBuffer.byteBuffer.remaining(), 1, accountId, containerId,
              msgInfoAndBuffer.messageInfo.getOperationTimeMs()), msgInfoAndBuffer.byteBuffer);

      // add 3 messages to the remote host only
      expectedIds.addAll(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3));

      // delete the very first blob in the remote host only (and delete it from expected list)
      Iterator<StoreKey> iter = expectedIds.iterator();
      addDeleteMessagesToReplicasOfPartition(partitionId, iter.next(), Collections.singletonList(remoteHost));
      iter.remove();

      // PUT and DELETE a blob in the remote host only
      id = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1).get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost));

      idsToExpectByPartition.put(partitionId, expectedIds);
    }

    // Do the replica metadata exchange.
    List<ReplicaThread.ExchangeMetadataResponse> responses =
        testSetup.replicaThread.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHost, batchSize),
            testSetup.replicasToReplicate.get(remoteHost.dataNodeId));

    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        responses.stream()
            .map(ReplicaThread.ExchangeMetadataResponse::getMissingStoreKeys)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet()));

    // Now delete a message in the remote before doing the Get requests (for every partition). Remove these keys from
    // expected key set. Even though they are requested, they should not go into the local store. However, this cycle
    // of replication must be successful.
    for (PartitionId partitionId : partitionIds) {
      Iterator<StoreKey> iter = idsToExpectByPartition.get(partitionId).iterator();
      iter.next();
      StoreKey keyToDelete = iter.next();
      addDeleteMessagesToReplicasOfPartition(partitionId, keyToDelete, Collections.singletonList(remoteHost));
      iter.remove();
    }

    testSetup.replicaThread.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHost, batchSize),
        testSetup.replicasToReplicate.get(remoteHost.dataNodeId), responses, false);

    Assert.assertEquals(idsToExpectByPartition.keySet(), localHost.infosByPartition.keySet());
    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        localHost.infosByPartition.values()
            .stream()
            .flatMap(Collection::stream)
            .map(MessageInfo::getStoreKey)
            .collect(Collectors.toSet()));
  }

  /**
   * Test the case where a blob expires after a replication metadata exchange completes and identifies the blob as
   * a candidate. The subsequent GetRequest should succeed as Replication makes a Include_All call, and
   * fixMissingStoreKeys() should succeed without exceptions. The blob should not be put locally.
   */
  @Test
  public void expiryAfterMetadataExchangeTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    int batchSize = 400;
    ReplicationTestSetup testSetup = new ReplicationTestSetup(batchSize);
    List<PartitionId> partitionIds = testSetup.partitionIds;
    MockHost remoteHost = testSetup.remoteHost;
    MockHost localHost = testSetup.localHost;
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    Map<PartitionId, Set<StoreKey>> idsToExpectByPartition = new HashMap<>();
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);

      // add 5 messages to remote host only.
      Set<StoreKey> expectedIds =
          new HashSet<>(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 5));

      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      boolean toEncrypt = TestUtils.RANDOM.nextBoolean();

      // add an expired message to the remote host only
      StoreKey id =
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
              partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      PutMsgInfoAndBuffer msgInfoAndBuffer = createPutMessage(id, accountId, containerId, toEncrypt);
      remoteHost.addMessage(partitionId,
          new MessageInfo(id, msgInfoAndBuffer.byteBuffer.remaining(), 1, accountId, containerId,
              msgInfoAndBuffer.messageInfo.getOperationTimeMs()), msgInfoAndBuffer.byteBuffer);

      // add 3 messages to the remote host only
      expectedIds.addAll(addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3));

      // delete the very first blob in the remote host only (and delete it from expected list)
      Iterator<StoreKey> iter = expectedIds.iterator();
      addDeleteMessagesToReplicasOfPartition(partitionId, iter.next(), Collections.singletonList(remoteHost));
      iter.remove();

      // PUT and DELETE a blob in the remote host only
      id = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1).get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost));

      idsToExpectByPartition.put(partitionId, expectedIds);
    }

    // Do the replica metadata exchange.
    List<ReplicaThread.ExchangeMetadataResponse> responses =
        testSetup.replicaThread.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHost, batchSize),
            testSetup.replicasToReplicate.get(remoteHost.dataNodeId));

    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        responses.stream().map(k -> k.getMissingStoreKeys()).flatMap(Collection::stream).collect(Collectors.toSet()));

    // Now expire a message in the remote before doing the Get requests (for every partition). Remove these keys from
    // expected key set. Even though they are requested, they should not go into the local store. However, this cycle
    // of replication must be successful.
    PartitionId partitionId = idsToExpectByPartition.keySet().iterator().next();
    Iterator<StoreKey> keySet = idsToExpectByPartition.get(partitionId).iterator();
    StoreKey keyToExpire = keySet.next();
    keySet.remove();
    MessageInfo msgInfoToExpire = null;

    for (MessageInfo info : remoteHost.infosByPartition.get(partitionId)) {
      if (info.getStoreKey().equals(keyToExpire)) {
        msgInfoToExpire = info;
        break;
      }
    }

    int i = remoteHost.infosByPartition.get(partitionId).indexOf(msgInfoToExpire);
    remoteHost.infosByPartition.get(partitionId)
        .set(i, new MessageInfo(msgInfoToExpire.getStoreKey(), msgInfoToExpire.getSize(), msgInfoToExpire.isDeleted(),
            msgInfoToExpire.isTtlUpdated(), msgInfoToExpire.isUndeleted(), 1, null, msgInfoToExpire.getAccountId(),
            msgInfoToExpire.getContainerId(), msgInfoToExpire.getOperationTimeMs(), msgInfoToExpire.getLifeVersion()));

    testSetup.replicaThread.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHost, batchSize),
        testSetup.replicasToReplicate.get(remoteHost.dataNodeId), responses, false);

    Assert.assertEquals(idsToExpectByPartition.keySet(), localHost.infosByPartition.keySet());
    Assert.assertEquals("Actual keys in Exchange Metadata Response different from expected",
        idsToExpectByPartition.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        localHost.infosByPartition.values()
            .stream()
            .flatMap(Collection::stream)
            .map(MessageInfo::getStoreKey)
            .collect(Collectors.toSet()));
  }

  /**
   * Test the case where remote host has a sequence of Old_Put, Old_Delete, New_Put messages and local host is initially
   * empty. Verify that local host is empty after replication.
   */
  @Test
  public void replicateWithOldPutDeleteAndNewPutTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    ReplicationTestSetup testSetup = new ReplicationTestSetup(10);
    Pair<String, String> testCaseAndExpectResult = new Pair<>("OP OD NP", "");
    createMixedMessagesOnRemoteHost(testSetup, testCaseAndExpectResult.getFirst());
    replicateAndVerify(testSetup, testCaseAndExpectResult.getSecond());
  }

  /**
   * Test the case where remote host has a sequence of Old_Put, New_Delete messages and local host is initially empty.
   * Verify that local host only has New_Put after replication.
   */
  @Test
  public void replicateWithOldPutAndNewDeleteTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    ReplicationTestSetup testSetup = new ReplicationTestSetup(10);
    Pair<String, String> testCaseAndExpectResult = new Pair<>("OP ND", "NP");
    createMixedMessagesOnRemoteHost(testSetup, testCaseAndExpectResult.getFirst());
    replicateAndVerify(testSetup, testCaseAndExpectResult.getSecond());
  }

  /**
   * Test the case where remote host has a sequence of Old_Put, New_Put, Old_Delete messages and local host is initially empty.
   * Verify that local host is empty after replication.
   */
  @Test
  public void replicateWithOldPutNewPutAndOldDeleteTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    ReplicationTestSetup testSetup = new ReplicationTestSetup(10);
    Pair<String, String> testCaseAndExpectResult = new Pair<>("OP NP OD", "");
    createMixedMessagesOnRemoteHost(testSetup, testCaseAndExpectResult.getFirst());
    replicateAndVerify(testSetup, testCaseAndExpectResult.getSecond());
  }

  /**
   * Test the case where remote host has a sequence of Old_Put, New_Put messages and local host is initially empty.
   * Verify that local host only has New_Put after replication.
   */
  @Test
  public void replicateWithOldPutAndNewPutTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    ReplicationTestSetup testSetup = new ReplicationTestSetup(10);
    Pair<String, String> testCaseAndExpectResult = new Pair<>("OP NP", "NP");
    createMixedMessagesOnRemoteHost(testSetup, testCaseAndExpectResult.getFirst());
    replicateAndVerify(testSetup, testCaseAndExpectResult.getSecond());
  }

  /**
   * Test the case where remote host has a sequence of Old_Put, New_Put, Old_Delete, New_Delete messages and local host
   * is initially empty. Verify that local host is empty after replication.
   */
  @Test
  public void replicateWithOldPutNewPutOldDeleteAndNewDeleteTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    ReplicationTestSetup testSetup = new ReplicationTestSetup(10);
    Pair<String, String> testCaseAndExpectResult = new Pair<>("OP NP OD ND", "");
    createMixedMessagesOnRemoteHost(testSetup, testCaseAndExpectResult.getFirst());
    replicateAndVerify(testSetup, testCaseAndExpectResult.getSecond());
  }

  /**
   * Test the case where remote host has a sequence of New_Put, New_Delete, Old_Put messages and local host is initially empty.
   * Verify that local host only has New_Put after replication.
   */
  @Test
  public void replicateWithNewPutDeleteAndOldPutTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    ReplicationTestSetup testSetup = new ReplicationTestSetup(10);
    Pair<String, String> testCaseAndExpectResult = new Pair<>("NP ND OP", "NP");
    createMixedMessagesOnRemoteHost(testSetup, testCaseAndExpectResult.getFirst());
    replicateAndVerify(testSetup, testCaseAndExpectResult.getSecond());
  }

  /**
   * Tests {@link ReplicaThread#exchangeMetadata(ConnectedChannel, List)} and
   * {@link ReplicaThread#fixMissingStoreKeys(ConnectedChannel, List, List, boolean)} for valid puts, deletes, expired keys and
   * corrupt blobs.
   * @throws Exception
   */
  @Test
  public void replicaThreadTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    List<MockHost> remoteHosts = new ArrayList<>();
    remoteHosts.add(remoteHost);
    if (shouldUseNetworkClient) {
      // Add one more remote host
      PartitionId specialPartitionId =
          clusterMap.getWritablePartitionIds(MockClusterMap.SPECIAL_PARTITION_CLASS).get(0);
      MockHost remoteHost2 = new MockHost(specialPartitionId.getReplicaIds().get(2).getDataNodeId(), clusterMap);
      remoteHosts.add(remoteHost2);
    }

    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition = new HashMap<>();
    List<MockHost> allHosts = new ArrayList<>();
    allHosts.add(localHost);
    allHosts.addAll(remoteHosts);
    for (int i = 0; i < partitionIds.size(); i++) {
      List<StoreKey> idsToBeIgnored = new ArrayList<>();
      PartitionId partitionId = partitionIds.get(i);
      // add 6 messages to both hosts.
      StoreKey toDeleteId = addPutMessagesToReplicasOfPartition(partitionId, allHosts, 6).get(0);

      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      boolean toEncrypt = TestUtils.RANDOM.nextBoolean();
      // add an expired message to the remote host only
      StoreKey id =
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
              partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      PutMsgInfoAndBuffer msgInfoAndBuffer = createPutMessage(id, accountId, containerId, toEncrypt);
      for (MockHost rHost : remoteHosts) {
        rHost.addMessage(partitionId,
            new MessageInfo(id, msgInfoAndBuffer.byteBuffer.remaining(), 1, accountId, containerId,
                msgInfoAndBuffer.messageInfo.getOperationTimeMs()), msgInfoAndBuffer.byteBuffer);
      }
      idsToBeIgnored.add(id);

      // add 3 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, remoteHosts, 3);

      accountId = Utils.getRandomShort(TestUtils.RANDOM);
      containerId = Utils.getRandomShort(TestUtils.RANDOM);
      toEncrypt = TestUtils.RANDOM.nextBoolean();
      // add a corrupt message to the remote host only
      id = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
          partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK);
      msgInfoAndBuffer = createPutMessage(id, accountId, containerId, toEncrypt);
      byte[] data = msgInfoAndBuffer.byteBuffer.array();
      // flip every bit in the array
      for (int j = 0; j < data.length; j++) {
        data[j] ^= 0xFF;
      }
      for (MockHost rHost : remoteHosts) {
        rHost.addMessage(partitionId, msgInfoAndBuffer.messageInfo, msgInfoAndBuffer.byteBuffer);
      }
      idsToBeIgnored.add(id);

      // add 3 messages to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, remoteHosts, 3);

      // add delete record for the very first blob in the remote host only
      addDeleteMessagesToReplicasOfPartition(partitionId, toDeleteId, remoteHosts);
      // PUT and DELETE a blob in the remote host only
      id = addPutMessagesToReplicasOfPartition(partitionId, remoteHosts, 1).get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, remoteHosts);
      idsToBeIgnored.add(id);

      // add 2 or 3 messages (depending on whether partition is even-numbered or odd-numbered) to the remote host only
      addPutMessagesToReplicasOfPartition(partitionId, remoteHosts, i % 2 == 0 ? 2 : 3);

      idsToBeIgnoredByPartition.put(partitionId, idsToBeIgnored);

      // ensure that the first key is not deleted in the local host
      assertNull(toDeleteId + " should not be deleted in the local host",
          getMessageInfo(toDeleteId, localHost.infosByPartition.get(partitionId), true, false, false));
    }

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHosts.toArray(new MockHost[0]));
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    for (MockHost rHost : remoteHosts) {
      Map<PartitionId, List<ByteBuffer>> missingBuffers = rHost.getMissingBuffers(localHost.buffersByPartition);
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
        if (partitionIds.indexOf(entry.getKey()) % 2 == 0) {
          assertEquals("Missing buffers count mismatch", 13, entry.getValue().size());
        } else {
          assertEquals("Missing buffers count mismatch", 14, entry.getValue().size());
        }
      }
    }

    // 1st and 2nd iterations - no keys missing because all data is in both hosts
    // 3rd iteration - 3 missing keys (one expired)
    // 4th iteration - 3 missing keys (one expired) - the corrupt key also shows up as missing but is ignored later
    // 5th iteration - 1 missing key (1 key from prev cycle, 1 deleted key, 1 never present key but deleted in remote)
    // 6th iteration - 2 missing keys (2 entries i.e put,delete of never present key)
    int[] missingKeysCounts = {0, 0, 3, 3, 1, 2};
    int[] missingBuffersCount = {12, 12, 9, 7, 6, 4};
    int expectedIndex = 0;
    int missingBuffersIndex = 0;

    for (int missingKeysCount : missingKeysCounts) {
      expectedIndex =
          assertMissingKeysAndFixMissingStoreKeys(expectedIndex, batchSize - 1, missingKeysCount, replicaThread,
              replicasToReplicate);

      Map<PartitionId, List<ByteBuffer>> missingBuffers = remoteHost.getMissingBuffers(localHost.buffersByPartition);
      for (Map.Entry<PartitionId, List<ByteBuffer>> entry : missingBuffers.entrySet()) {
        if (partitionIds.indexOf(entry.getKey()) % 2 == 0) {
          assertEquals("Missing buffers count mismatch for iteration count " + missingBuffersIndex,
              missingBuffersCount[missingBuffersIndex], entry.getValue().size());
        } else {
          assertEquals("Missing buffers count mismatch for iteration count " + missingBuffersIndex,
              missingBuffersCount[missingBuffersIndex] + 1, entry.getValue().size());
        }
      }
      missingBuffersIndex++;
    }

    // Test the case where some partitions have missing keys, but not all.
    replicaThread.replicate();
    for (MockHost rHost : remoteHosts) {
      List<ReplicaThread.ExchangeMetadataResponse> response =
          replicaThread.getExchangeMetadataResponsesInEachCycle().get(rHost.dataNodeId);
      List<RemoteReplicaInfo> remoteReplicaInfos = replicasToReplicate.get(rHost.dataNodeId);
      assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
      for (int i = 0; i < response.size(); i++) {
        if (i % 2 == 0) {
          assertEquals(0, response.get(i).missingStoreMessages.size());
          assertEquals(expectedIndex, ((MockFindToken) response.get(i).remoteToken).getIndex());
        } else {
          assertEquals(1, response.get(i).missingStoreMessages.size());
          assertEquals(expectedIndex + 1, ((MockFindToken) response.get(i).remoteToken).getIndex());
        }
        assertEquals("Token should have been set correctly in fixMissingStoreKeys()", response.get(i).remoteToken,
            replicasToReplicate.get(rHost.dataNodeId).get(i).getToken());
      }
    }
    time.sleep(10 * 1000);
    replicaThread.replicate();
    for (MockHost rHost : remoteHosts) {
      // 1 expired + 1 corrupt + 1 put (never present) + 1 deleted (never present) expected missing buffers
      verifyNoMoreMissingKeysAndExpectedMissingBufferCount(rHost, localHost, replicaThread, replicasToReplicate,
          idsToBeIgnoredByPartition, storeKeyConverter, expectedIndex, expectedIndex + 1, 4);
    }
  }

  @Test
  public void replicaThreadSleepTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    long expectedThrottleDurationMs =
        localHost.dataNodeId.getDatacenterName().equals(remoteHost.dataNodeId.getDatacenterName())
            ? replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs
            : replicationConfig.replicationInterReplicaThreadThrottleSleepDurationMs;
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    int batchSize = 4;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate = replicasAndThread.getFirst();
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    // populate data, add 1 messages to both hosts.
    for (PartitionId partitionId : clusterMap.getAllPartitionIds(null)) {
      addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(localHost, remoteHost), 1);
    }
    // tests to verify replica thread throttling and idling functions in the following steps:
    // 1. all replicas are in sync, thread level sleep and replica quarantine are both enabled.
    // 2. add put messages to some replica and verify that replication for replicas remain disabled.
    // 3. forward the time so replication for replicas are re-enabled and check replication resumes.
    // 4. add more put messages to ensure replication happens continuously when needed and is throttled appropriately.

    // 1. verify that the replica thread sleeps and replicas are temporarily disable when all replicas are synced.
    List<List<RemoteReplicaInfo>> replicasToReplicateList = new ArrayList<>(replicasToReplicate.values());
    // replicate is called and time is moved forward to prepare the replicas for testing.
    replicaThread.replicate();
    time.sleep(replicationConfig.replicationSyncedReplicaBackoffDurationMs + 1);
    long currentTimeMs = time.milliseconds();
    replicaThread.replicate();
    for (List<RemoteReplicaInfo> replicaInfos : replicasToReplicateList) {
      for (RemoteReplicaInfo replicaInfo : replicaInfos) {
        assertEquals("Unexpected re-enable replication time",
            currentTimeMs + replicationConfig.replicationSyncedReplicaBackoffDurationMs,
            replicaInfo.getReEnableReplicationTime());
      }
    }
    currentTimeMs = time.milliseconds();
    replicaThread.replicate();
    assertEquals("Replicas are in sync, replica thread should sleep by replication.thread.idle.sleep.duration.ms",
        currentTimeMs + replicationConfig.replicationReplicaThreadIdleSleepDurationMs, time.milliseconds());

    // 2. add 3 messages to a partition in the remote host only and verify replication for all replicas should be disabled.
    PartitionId partitionId = clusterMap.getWritablePartitionIds(null).get(0);
    addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 3);
    int[] missingKeys = new int[replicasToReplicate.get(remoteHost.dataNodeId).size()];
    for (int i = 0; i < missingKeys.length; i++) {
      missingKeys[i] = replicasToReplicate.get(remoteHost.dataNodeId)
          .get(i)
          .getReplicaId()
          .getPartitionId()
          .isEqual(partitionId.toPathString()) ? 3 : 0;
    }
    currentTimeMs = time.milliseconds();
    replicaThread.replicate();
    assertEquals("Replication for all replicas should be disabled and the thread should sleep",
        currentTimeMs + replicationConfig.replicationReplicaThreadIdleSleepDurationMs, time.milliseconds());
    assertMissingKeys(missingKeys, batchSize, replicaThread, remoteHost, replicasToReplicate);

    // 3. forward the time and run replicate and verify the replication.
    time.sleep(replicationConfig.replicationSyncedReplicaBackoffDurationMs);
    replicaThread.replicate();
    missingKeys = new int[replicasToReplicate.get(remoteHost.dataNodeId).size()];
    assertMissingKeys(missingKeys, batchSize, replicaThread, remoteHost, replicasToReplicate);

    // Since, now we moved setting of remoteReplicaInfo::setReEnableReplicationTime inside replicaThread::exchangeMetaData and
    // above assertMissingKeys() does exchangeMetadata() for replicas up to date, each replica will have
    // ReEnableReplicationTime set by replicationSyncedReplicaBackoffDurationMs. Forward the time here.
    time.sleep(replicationConfig.replicationSyncedReplicaBackoffDurationMs);

    // 4. add more put messages and verify that replication continues and is throttled appropriately.
    addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(localHost, remoteHost), 3);
    currentTimeMs = time.milliseconds();
    replicaThread.replicate();
    assertEquals("Replica thread should sleep exactly " + expectedThrottleDurationMs + " since remote has new token",
        currentTimeMs + expectedThrottleDurationMs, time.milliseconds());

    assertMissingKeys(missingKeys, batchSize, replicaThread, remoteHost, replicasToReplicate);

    // Since, now we moved setting of remoteReplicaInfo::setReEnableReplicationTime inside replicaThread::exchangeMetaData and
    // above assertMissingKeys() does exchangeMetadata() for replicas up to date, each replica will have
    // ReEnableReplicationTime set by replicationSyncedReplicaBackoffDurationMs. Forward the time here.
    time.sleep(replicationConfig.replicationSyncedReplicaBackoffDurationMs);

    // verify that throttling on the replica thread is disabled when relevant configs are 0.
    Properties properties = new Properties();
    properties.setProperty("replication.intra.replica.thread.throttle.sleep.duration.ms", "0");
    properties.setProperty("replication.inter.replica.thread.throttle.sleep.duration.ms", "0");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));
    replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    replicaThread = replicasAndThread.getSecond();
    currentTimeMs = time.milliseconds();
    replicaThread.replicate();
    assertEquals("Replica thread should not sleep when throttling is disabled and replicas are out of sync",
        currentTimeMs, time.milliseconds());
  }

  /**
   * Test that metrics that track remote replicas lag behind local replicas in each dc.
   * @throws Exception
   */
  @Test
  public void dcLevelReplicationLagMetricsTest() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, null);
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();
    Set<String> remoteDcNames = new HashSet<>(Arrays.asList("DC1", "DC2", "DC3"));
    String localDcName = clusterMap.getDataNodeIds().get(0).getDatacenterName();
    remoteDcNames.remove(localDcName);

    // before updating replication lag, make sure avg lag in each dc is 0
    MetricRegistry metricRegistry = replicationManager.getMetricRegistry();
    String prefix = ReplicaThread.class.getName() + ".";
    String avgMetricSuffix = "-avgReplicaLagFromLocalInBytes";
    assertEquals("Average replication lag in local dc is not expected", 18.0,
        metricRegistry.getGauges().get(prefix + localDcName + avgMetricSuffix).getValue());
    for (String remoteDc : remoteDcNames) {
      assertEquals("Average replication lag in remote dc is not expected", 18.0,
          metricRegistry.getGauges().get(prefix + remoteDc + avgMetricSuffix).getValue());
    }

    // iterate over all partitions on current node and make sure all their peer replicas in local dc have fully caught up
    for (Map.Entry<PartitionId, PartitionInfo> entry : replicationManager.partitionToPartitionInfo.entrySet()) {
      PartitionId localPartition = entry.getKey();
      PartitionInfo partitionInfo = entry.getValue();
      List<RemoteReplicaInfo> remoteReplicaInfos = partitionInfo.getRemoteReplicaInfos()
          .stream()
          .filter(info -> info.getReplicaId().getDataNodeId().getDatacenterName().equals(localDcName))
          .collect(Collectors.toList());
      for (RemoteReplicaInfo remoteReplicaInfoInLocalDc : remoteReplicaInfos) {
        ReplicaId peerReplicaInLocalDc = remoteReplicaInfoInLocalDc.getReplicaId();
        replicationManager.updateTotalBytesReadByRemoteReplica(localPartition,
            peerReplicaInLocalDc.getDataNodeId().getHostname(), peerReplicaInLocalDc.getReplicaPath(), 18);
      }
    }
    // verify that after updating replication lag for all peer replicas in local dc, the avg lag in local dc has updated
    assertEquals("Average replication lag in local dc is not expected", 0.0,
        metricRegistry.getGauges().get(prefix + localDcName + avgMetricSuffix).getValue());
    // for remote dc, the avg lag is still 18.0
    for (String remoteDc : remoteDcNames) {
      assertEquals("Average replication lag in remote dc is not expected", 18.0,
          metricRegistry.getGauges().get(prefix + remoteDc + avgMetricSuffix).getValue());
    }
  }

  /**
   * Tests {@link ReplicationMetrics#getMaxLagForPartition(PartitionId)}
   * @throws Exception
   */
  @Test
  public void replicationLagMetricAndSyncUpTest() throws Exception {
    assumeFalse(shouldUseNetworkClient);
    MockClusterMap clusterMap = new MockClusterMap();
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    AmbryReplicaSyncUpManager replicaSyncUpService = new AmbryReplicaSyncUpManager(clusterMapConfig);
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost1 = localAndRemoteHosts.getSecond();
    // create another remoteHost2 that shares spacial partition with localHost and remoteHost1
    PartitionId specialPartitionId = clusterMap.getWritablePartitionIds(MockClusterMap.SPECIAL_PARTITION_CLASS).get(0);
    MockHost remoteHost2 = new MockHost(specialPartitionId.getReplicaIds().get(2).getDataNodeId(), clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    int batchSize = 4;

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      // add batchSize + 1 messages to the remoteHost1 so that two rounds of replication is needed.
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost1), batchSize + 1);
    }
    // add batchSize - 1 messages to the remoteHost2 so that localHost can catch up during one cycle of replication
    for (ReplicaId replicaId : clusterMap.getReplicaIds(remoteHost2.dataNodeId)) {
      addPutMessagesToReplicasOfPartition(replicaId.getPartitionId(), Collections.singletonList(remoteHost2),
          batchSize - 1);
    }

    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread1 =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null,
            replicaSyncUpService, remoteHost1);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate1 = replicasAndThread1.getFirst();
    ReplicaThread replicaThread1 = replicasAndThread1.getSecond();
    // mock Bootstrap-To-Standby transition in ReplicationManager: 1. update store current state; 2. initiate bootstrap
    replicasToReplicate1.get(remoteHost1.dataNodeId)
        .forEach(info -> info.getLocalStore().setCurrentState(ReplicaState.BOOTSTRAP));
    clusterMap.getReplicaIds(localHost.dataNodeId).forEach(replicaSyncUpService::initiateBootstrap);

    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread1.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHost1, batchSize),
            replicasToReplicate1.get(remoteHost1.dataNodeId));
    replicaThread1.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHost1, batchSize),
        replicasToReplicate1.get(remoteHost1.dataNodeId), response, false);
    for (PartitionId partitionId : partitionIds) {
      List<MessageInfo> allMessageInfos = localAndRemoteHosts.getSecond().infosByPartition.get(partitionId);
      long expectedLag =
          allMessageInfos.subList(batchSize, allMessageInfos.size()).stream().mapToLong(MessageInfo::getSize).sum();
      assertEquals("Replication lag doesn't match expected value", expectedLag,
          replicaThread1.getReplicationMetrics().getMaxLagForPartition(partitionId));
    }

    response = replicaThread1.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHost1, batchSize),
        replicasToReplicate1.get(remoteHost1.dataNodeId));
    replicaThread1.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHost1, batchSize),
        replicasToReplicate1.get(remoteHost1.dataNodeId), response, false);
    for (PartitionId partitionId : partitionIds) {
      assertEquals("Replication lag should equal to 0", 0,
          replicaThread1.getReplicationMetrics().getMaxLagForPartition(partitionId));
    }

    // replicate with remoteHost2 to ensure special replica has caught up with enough peers
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread2 =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null,
            replicaSyncUpService, remoteHost2);
    Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicate2 = replicasAndThread2.getFirst();
    ReplicaThread replicaThread2 = replicasAndThread2.getSecond();
    // initiate bootstrap on replica of special partition
    RemoteReplicaInfo specialReplicaInfo = replicasToReplicate2.get(remoteHost2.dataNodeId)
        .stream()
        .filter(info -> info.getReplicaId().getPartitionId() == specialPartitionId)
        .findFirst()
        .get();
    specialReplicaInfo.getLocalStore().setCurrentState(ReplicaState.BOOTSTRAP);
    replicaSyncUpService.initiateBootstrap(specialReplicaInfo.getLocalReplicaId());
    response = replicaThread2.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHost2, batchSize),
        replicasToReplicate2.get(remoteHost2.dataNodeId));
    replicaThread2.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHost2, batchSize),
        replicasToReplicate2.get(remoteHost2.dataNodeId), response, false);
    // verify replica of special partition has completed bootstrap and becomes standby
    assertEquals("Store state is not expected", ReplicaState.STANDBY,
        specialReplicaInfo.getLocalStore().getCurrentState());
  }

  /**
   * Tests that replica tokens are set correctly and go through different stages correctly.
   * @throws InterruptedException
   */
  @Test
  public void replicaTokenTest() throws InterruptedException {
    final long tokenPersistInterval = 100;
    Time time = new MockTime();
    MockFindToken token1 = new MockFindToken(0, 0);
    RemoteReplicaInfo remoteReplicaInfo =
        new RemoteReplicaInfo(new MockReplicaId(ReplicaType.DISK_BACKED), new MockReplicaId(ReplicaType.DISK_BACKED),
            new InMemoryStore(null, Collections.emptyList(), Collections.emptyList(), null), token1,
            tokenPersistInterval, time, new Port(5000, PortType.PLAINTEXT));

    // The equality check is for the reference, which is fine.
    // Initially, the current token and the token to persist are the same.
    assertEquals(token1, remoteReplicaInfo.getToken());
    assertEquals(token1, remoteReplicaInfo.getTokenToPersist());
    MockFindToken token2 = new MockFindToken(100, 100);

    remoteReplicaInfo.initializeTokens(token2);
    // Both tokens should be the newly initialized token.
    assertEquals(token2, remoteReplicaInfo.getToken());
    assertEquals(token2, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    MockFindToken token3 = new MockFindToken(200, 200);

    remoteReplicaInfo.setToken(token3);
    // Token to persist should still be the old token.
    assertEquals(token3, remoteReplicaInfo.getToken());
    assertEquals(token2, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    // Sleep for shorter than token persist interval.
    time.sleep(tokenPersistInterval - 1);
    // Token to persist should still be the old token.
    assertEquals(token3, remoteReplicaInfo.getToken());
    assertEquals(token2, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    MockFindToken token4 = new MockFindToken(200, 200);
    remoteReplicaInfo.setToken(token4);

    time.sleep(2);
    // Token to persist should be the most recent token as of currentTime - tokenToPersistInterval
    // which is token3 at this time.
    assertEquals(token4, remoteReplicaInfo.getToken());
    assertEquals(token3, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();

    time.sleep(tokenPersistInterval + 1);
    // The most recently set token as of currentTime - tokenToPersistInterval is token4
    assertEquals(token4, remoteReplicaInfo.getToken());
    assertEquals(token4, remoteReplicaInfo.getTokenToPersist());
    remoteReplicaInfo.onTokenPersisted();
  }

  /**
   * Tests when the local records has higher lifeVersion than remote records.
   */
  @Test
  public void replicaThreadLifeVersionLocalGreaterThanRemote() throws Exception {
    lifeVersionLocalGreaterThanRemote_Delete(false, false);
    lifeVersionLocalGreaterThanRemote_Delete(false, true);
    lifeVersionLocalGreaterThanRemote_Delete(true, false);
    lifeVersionLocalGreaterThanRemote_Delete(true, true);
  }

  /**
   * Tests when the local store missing put records with lifeVersion greater than 0
   */
  @Test
  public void replicaThreadLifeVersionLocalLessThanRemote_MissingPuts() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    storeKeyConverter.setConversionMap(conversionMap);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition = new HashMap<>();
    Map<PartitionId, List<StoreKey>> idsToBeTtlUpdatedByPartition = new HashMap<>();
    short lifeVersion = 1;
    for (int i = 0; i < partitionIds.size(); i++) {
      List<StoreKey> toBeIgnored = new ArrayList<>();
      List<StoreKey> toBeTtlUpdated = new ArrayList<>();
      PartitionId partitionId = partitionIds.get(i);
      // Adding 1 put to remoteHost at lifeVersion 0
      List<StoreKey> ids = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1);
      // Adding 1 put to remoteHost at lifeVersion 1
      ids.addAll(
          addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), lifeVersion, 1));

      // Adding one put to remoteHost at lifeVersion 1, which would be ttl updated later at lifeVersion 1
      StoreKey toTtlUpdateId =
          addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), lifeVersion, 1).get(
              0);
      ids.add(toTtlUpdateId);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, toTtlUpdateId, Collections.singletonList(remoteHost),
          UPDATED_EXPIRY_TIME_MS, lifeVersion);
      toBeTtlUpdated.add(toTtlUpdateId);

      // Adding one put to remoteHost at lifeVersion 0, which would be ttl updated later at lifeVersion 1
      toTtlUpdateId = addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1).get(0);
      ids.add(toTtlUpdateId);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, toTtlUpdateId, Collections.singletonList(remoteHost),
          UPDATED_EXPIRY_TIME_MS, lifeVersion);
      toBeTtlUpdated.add(toTtlUpdateId);

      // Adding one put to remoteHost, which would be deleted later
      StoreKey toDeleteId =
          addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), lifeVersion, 1).get(0);
      ids.add(toDeleteId);
      addDeleteMessagesToReplicasOfPartition(partitionId, toDeleteId, Collections.singletonList(remoteHost),
          lifeVersion, EXPIRY_TIME_MS);
      toBeIgnored.add(toDeleteId);

      // Adding one put to remoteHost, which would be ttl updated and deleted later
      StoreKey toDeleteAndTtlUpdateId =
          addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), lifeVersion, 1).get(0);
      ids.add(toDeleteAndTtlUpdateId);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, toDeleteAndTtlUpdateId,
          Collections.singletonList(remoteHost), UPDATED_EXPIRY_TIME_MS, lifeVersion);
      toBeTtlUpdated.add(toDeleteAndTtlUpdateId);
      addDeleteMessagesToReplicasOfPartition(partitionId, toDeleteAndTtlUpdateId, Collections.singletonList(remoteHost),
          lifeVersion, UPDATED_EXPIRY_TIME_MS);
      toBeIgnored.add(toDeleteAndTtlUpdateId);

      // Adding one put to remoteHost at lifeVersion 0, delete it and then add undelete at lifeVersion 1
      StoreKey deleteAndUndeleteId =
          addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHost), 1).get(0);
      ids.add(deleteAndUndeleteId);
      addDeleteMessagesToReplicasOfPartition(partitionId, deleteAndUndeleteId, Collections.singletonList(remoteHost),
          (short) 0, EXPIRY_TIME_MS);
      addUndeleteMessagesToReplicasOfPartition(partitionId, deleteAndUndeleteId, Collections.singletonList(remoteHost),
          lifeVersion);

      idsToBeIgnoredByPartition.put(partitionId, toBeIgnored);
      idsToBeTtlUpdatedByPartition.put(partitionId, toBeTtlUpdated);

      // Adding one put to both remote and local host.
      ids.addAll(
          addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(localHost, remoteHost), lifeVersion, 1));
    }

    int batchSize = 100;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasAndThread.getFirst().get(remoteHost.dataNodeId);
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    int missingKeyCount = 5;
    replicaThread.replicate();
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(missingKeyCount, response.get(i).missingStoreMessages.size());
      assertEquals("Token should have been set correctly in fixMissingStoreKeys()", response.get(i).remoteToken,
          remoteReplicaInfos.get(i).getToken());
    }
    // Don't compare buffers here, PutBuffer might be different since we might change the lifeVersion.
    for (Map.Entry<PartitionId, List<MessageInfo>> localInfoEntry : localHost.infosByPartition.entrySet()) {
      assertEquals("MessageInfo number mismatch", 8, localInfoEntry.getValue().size());
    }

    checkBlobMessagesAreEqualInLocalAndRemoteHosts(localHost, remoteHost, idsToBeIgnoredByPartition,
        idsToBeTtlUpdatedByPartition);
  }

  /**
   * Tests when the lifeVersion in local is less than the lifeVersion in remote and the final state from remote
   * is delete.
   * @throws Exception
   */
  @Test
  public void replicaThreadLifeVersionLocalLessThanRemote_FinalState_Delete() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    storeKeyConverter.setConversionMap(conversionMap);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);

    Map<PartitionId, List<StoreKey>> idsByPartition = new HashMap<>();
    Map<PartitionId, StoreKey> idsToBeIgnoredByPartition = new HashMap<>();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    // When the final state is delete in remote host, then local host might have several different states.
    // 1 missing 2 Delete 3 Put(w/ or w/o ttl update)
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      List<StoreKey> ids = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1);
      // Adding a Put and Delete to remote but nothing in local
      StoreKey id = ids.get(0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          EXPIRY_TIME_MS);
      idsToBeIgnoredByPartition.put(partitionId, id);

      // Adding one Delete to remote and add delete to local but with lower lifeVersion
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(localHost), (short) 0,
          EXPIRY_TIME_MS);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          EXPIRY_TIME_MS);

      // Adding one Put and Delete to remote and add the same put to local host
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          EXPIRY_TIME_MS);

      // Adding one Put and Delete to remote and add same Put and a TtlUpdate to local host
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(localHost),
          UPDATED_EXPIRY_TIME_MS, (short) 0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          EXPIRY_TIME_MS);

      // Adding one Put and Delete to remote and add same Put and a Delete and Undelete to local.
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(localHost), (short) 0,
          EXPIRY_TIME_MS);
      addUndeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 2,
          EXPIRY_TIME_MS);
      ids.add(id);
      idsByPartition.put(partitionId, ids);
    }

    int batchSize = 100;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasAndThread.getFirst().get(remoteHost.dataNodeId);
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    // It's all deletes, there is no missing key.
    replicaThread.replicate();
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(0, response.get(i).missingStoreMessages.size());
    }

    // Before exchange metadata, the number of message infos in local host is 7. Exchange metadata would add another 4(all deletes).
    for (Map.Entry<PartitionId, List<MessageInfo>> localInfoEntry : localHost.infosByPartition.entrySet()) {
      assertEquals("MessageInfo number mismatch", 11, localInfoEntry.getValue().size());
    }

    for (Map.Entry<PartitionId, List<StoreKey>> idsEntry : idsByPartition.entrySet()) {
      List<MessageInfo> remoteInfos = remoteHost.infosByPartition.get(idsEntry.getKey());
      List<MessageInfo> localInfos = localHost.infosByPartition.get(idsEntry.getKey());

      for (StoreKey id : idsEntry.getValue()) {
        if (!idsToBeIgnoredByPartition.get(idsEntry.getKey()).equals(id)) {
          MessageInfo localInfo = getMergedMessageInfo(id, localInfos);
          MessageInfo remoteInfo = getMergedMessageInfo(id, remoteInfos);
          assertTrue(localInfo.isDeleted());
          assertTrue(remoteInfo.isDeleted());
          assertEquals(localInfo.getLifeVersion(), remoteInfo.getLifeVersion());
        }
      }
    }
  }

  /**
   * Tests when the lifeVersion in local is less than the lifeVersion in remote and the final state from remote
   * is delete with ttl update.
   * @throws Exception
   */
  @Test
  public void replicaThreadLifeVersionLocalLessThanRemote_FinalState_TtlUpdateDelete() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    storeKeyConverter.setConversionMap(conversionMap);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);

    Map<PartitionId, List<StoreKey>> idsByPartition = new HashMap<>();
    Map<PartitionId, StoreKey> idsToBeIgnoredByPartition = new HashMap<>();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    // When the remote host has P, T, D, then local host might have several different states.
    // 1 Missing -> []
    // 2 P -> [T, D]
    // 3 P, T -> [D]
    // 4 P, T, D -> [D]
    // 5 P, D -> [U, T, D]
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      List<StoreKey> ids = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), 1);
      // 1 Missing
      StoreKey id = ids.get(0);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
          UPDATED_EXPIRY_TIME_MS, (short) 0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          UPDATED_EXPIRY_TIME_MS);
      idsToBeIgnoredByPartition.put(partitionId, id);

      // 2 P -> [T, D]
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
          UPDATED_EXPIRY_TIME_MS, (short) 0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          UPDATED_EXPIRY_TIME_MS);

      // 3 P, T -> [D]
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Arrays.asList(localHost, remoteHost),
          UPDATED_EXPIRY_TIME_MS, (short) 0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          UPDATED_EXPIRY_TIME_MS);

      // 4 P, T, D -> [D]
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Arrays.asList(localHost, remoteHost),
          UPDATED_EXPIRY_TIME_MS, (short) 0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(localHost), (short) 0,
          UPDATED_EXPIRY_TIME_MS);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          UPDATED_EXPIRY_TIME_MS);

      // 5 P, D -> [U, T, D]
      id = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost, localHost), 1).get(0);
      ids.add(id);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(localHost), (short) 0,
          EXPIRY_TIME_MS);
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
          UPDATED_EXPIRY_TIME_MS, (short) 0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost), (short) 1,
          UPDATED_EXPIRY_TIME_MS);
      ids.add(id);
      idsByPartition.put(partitionId, ids);
    }

    int batchSize = 100;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasAndThread.getFirst().get(remoteHost.dataNodeId);
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    // It's all deletes, there is no missing key.
    replicaThread.replicate();
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(0, response.get(i).missingStoreMessages.size());
    }

    // Before exchange metadata, the number of message infos in local host is 8. Exchange metadata would add another 7.
    for (Map.Entry<PartitionId, List<MessageInfo>> localInfoEntry : localHost.infosByPartition.entrySet()) {
      assertEquals("MessageInfo number mismatch", 15, localInfoEntry.getValue().size());
    }

    for (Map.Entry<PartitionId, List<StoreKey>> idsEntry : idsByPartition.entrySet()) {
      List<MessageInfo> remoteInfos = remoteHost.infosByPartition.get(idsEntry.getKey());
      List<MessageInfo> localInfos = localHost.infosByPartition.get(idsEntry.getKey());

      for (StoreKey id : idsEntry.getValue()) {
        if (!idsToBeIgnoredByPartition.get(idsEntry.getKey()).equals(id)) {
          MessageInfo localInfo = getMergedMessageInfo(id, localInfos);
          MessageInfo remoteInfo = getMergedMessageInfo(id, remoteInfos);
          assertTrue(localInfo.isDeleted());
          assertTrue(remoteInfo.isDeleted());
          assertTrue(localInfo.isTtlUpdated());
          assertTrue(remoteInfo.isTtlUpdated());
          assertEquals(localInfo.getLifeVersion(), remoteInfo.getLifeVersion());
        }
      }
    }
  }

  /**
   * Tests when lifeVersion in local is less than the lifeVersion in remote and the final state is not
   * delete, it would be Put, TtlUpdate or Undelete.
   * @throws Exception
   */
  @Test
  public void replicaThreadLifeVersionLocalLessThanRemote_FinalState_NotDelete() throws Exception {
    MockClusterMap clusterMap = new MockClusterMap();
    Pair<MockHost, MockHost> localAndRemoteHosts = getLocalAndRemoteHosts(clusterMap);
    MockHost localHost = localAndRemoteHosts.getFirst();
    MockHost remoteHost = localAndRemoteHosts.getSecond();
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Map<StoreKey, StoreKey> conversionMap = new HashMap<>();
    storeKeyConverter.setConversionMap(conversionMap);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);

    Map<PartitionId, List<StoreKey>> idsByPartition = new HashMap<>();
    Map<PartitionId, StoreKey> idsToBeIgnoredByPartition = new HashMap<>();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    // Remote host has final state as "Not Delete", it would be Put, Ttl update or Undelete. Put and Undelete are practically
    // the same. So we can create two separate set of remote host, with ttl update or without ttl update.
    //
    // When the remote has ttl update, assuming it's P1, T1, then the local can be
    // 1 Missing -> [P1, T1]
    // 2 P0 -> [U1, T1]
    // 3 P0, T0 -> [U1]
    // 4 P0, T0, D0 -> [U1]
    // 5 P0, D0 -> [U1, T1]
    //
    // When the remote has not ttl update, assuming it's P1, then the local can be
    // 1 Missing -> [P1]
    // 2 P0 -> [U1]
    // 3 P0, T0 -> [U1]
    // 4 P0, T0, D0 -> [U1]
    // 5 P0, D0 -> [U1]
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      List<StoreKey> ids = addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHost), (short) 1, 5);
      for (StoreKey id : ids) {
        addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
            UPDATED_EXPIRY_TIME_MS, (short) 1);
      }

      // 1 Missing
      StoreKey id = ids.get(0);

      // 2 P0 -> [U1, T1]
      id = ids.get(1);
      addPutMessagesToReplicasOfPartition(Collections.singletonList(id), Collections.singletonList(localHost));

      // 3 P0, T0 -> [U1]
      id = ids.get(2);
      addPutMessagesToReplicasOfPartition(Collections.singletonList(id), Collections.singletonList(localHost));
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Arrays.asList(localHost), UPDATED_EXPIRY_TIME_MS,
          (short) 0);

      // 4 P0, T0, D0 -> [U1]
      id = ids.get(3);
      addPutMessagesToReplicasOfPartition(Collections.singletonList(id), Collections.singletonList(localHost));
      addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Arrays.asList(localHost), UPDATED_EXPIRY_TIME_MS,
          (short) 0);
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(localHost), (short) 0,
          UPDATED_EXPIRY_TIME_MS);

      // 5 P, D -> [U, T, D]
      id = ids.get(4);
      addPutMessagesToReplicasOfPartition(Collections.singletonList(id), Collections.singletonList(localHost));
      addDeleteMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(localHost), (short) 0,
          UPDATED_EXPIRY_TIME_MS);
    }

    int batchSize = 100;
    Pair<Map<DataNodeId, List<RemoteReplicaInfo>>, ReplicaThread> replicasAndThread =
        getRemoteReplicasAndReplicaThread(batchSize, clusterMap, localHost, storeKeyConverter, transformer, null, null,
            remoteHost);
    List<RemoteReplicaInfo> remoteReplicaInfos = replicasAndThread.getFirst().get(remoteHost.dataNodeId);
    ReplicaThread replicaThread = replicasAndThread.getSecond();

    // There is one missing key
    replicaThread.replicate();
    List<ReplicaThread.ExchangeMetadataResponse> response =
        replicaThread.getExchangeMetadataResponsesInEachCycle().get(remoteHost.dataNodeId);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfos.size(), response.size());
    for (int i = 0; i < response.size(); i++) {
      assertEquals(1, response.get(i).missingStoreMessages.size());
    }

    // Before exchange metadata, the number of message infos in local host is 8. Exchange metadata would add another 8.
    for (Map.Entry<PartitionId, List<MessageInfo>> localInfoEntry : localHost.infosByPartition.entrySet()) {
      assertEquals("MessageInfo number mismatch", 16, localInfoEntry.getValue().size());
    }

    for (Map.Entry<PartitionId, List<StoreKey>> idsEntry : idsByPartition.entrySet()) {
      List<MessageInfo> remoteInfos = remoteHost.infosByPartition.get(idsEntry.getKey());
      List<MessageInfo> localInfos = localHost.infosByPartition.get(idsEntry.getKey());

      for (StoreKey id : idsEntry.getValue()) {
        if (!idsToBeIgnoredByPartition.get(idsEntry.getKey()).equals(id)) {
          MessageInfo localInfo = getMergedMessageInfo(id, localInfos);
          MessageInfo remoteInfo = getMergedMessageInfo(id, remoteInfos);
          assertTrue(localInfo.isDeleted());
          assertTrue(remoteInfo.isDeleted());
          assertTrue(localInfo.isTtlUpdated());
          assertTrue(remoteInfo.isTtlUpdated());
          assertEquals(localInfo.getLifeVersion(), remoteInfo.getLifeVersion());
        }
      }
    }
  }
}

