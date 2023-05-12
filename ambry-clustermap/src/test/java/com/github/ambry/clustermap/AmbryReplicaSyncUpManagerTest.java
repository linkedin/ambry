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
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.model.Message;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link AmbryReplicaSyncUpManager}.
 */
public class AmbryReplicaSyncUpManagerTest {
  private static final String RESOURCE_NAME = "0";
  private final List<ReplicaId> localDcPeerReplicas;
  private final List<ReplicaId> remoteDcPeerReplicas;
  private final MockClusterMap clusterMap;
  private AmbryPartitionStateModel stateModel;
  private AmbryReplicaSyncUpManager replicaSyncUpService;
  private ReplicaId currentReplica;
  private MockHelixParticipant mockHelixParticipant;
  private Message mockMessage;

  public AmbryReplicaSyncUpManagerTest() throws IOException {
    clusterMap = new MockClusterMap();
    // clustermap setup: 3 data centers(DC1/DC2/DC3), each data center has 3 replicas
    PartitionId partition = clusterMap.getAllPartitionIds(null).get(0);
    Set<ReplicaId> replicas = new HashSet<>(partition.getReplicaIds());
    currentReplica = replicas.iterator().next();
    String localDcName = currentReplica.getDataNodeId().getDatacenterName();
    replicas.remove(currentReplica);
    Set<ReplicaId> localDcPeers = replicas.stream()
        .filter(r -> ((ReplicaId) r).getDataNodeId().getDatacenterName().equals(localDcName))
        .collect(Collectors.toSet());
    replicas.removeAll(localDcPeers);
    localDcPeerReplicas = new ArrayList<>(localDcPeers);
    remoteDcPeerReplicas = new ArrayList<>(replicas);
    List<TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new TestUtils.ZkInfo(null, "DC1", (byte) 0, 2199, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    Properties properties = new Properties();
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.replica.catchup.acceptable.lag.bytes", Long.toString(100L));
    properties.setProperty("clustermap.enable.state.model.listener", Boolean.toString(true));
    properties.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);
    replicaSyncUpService = (AmbryReplicaSyncUpManager) mockHelixParticipant.getReplicaSyncUpManager();
    mockHelixParticipant.currentReplica = currentReplica;
    mockHelixParticipant.replicaSyncUpService = replicaSyncUpService;
    stateModel =
        new AmbryPartitionStateModel(RESOURCE_NAME, partition.toPathString(), mockHelixParticipant, clusterMapConfig,
            new ConcurrentHashMap<>());
    mockMessage = Mockito.mock(Message.class);
    when(mockMessage.getPartitionName()).thenReturn(partition.toPathString());
    when(mockMessage.getResourceName()).thenReturn(RESOURCE_NAME);
  }

  /**
   * Test workflow:
   * 1. bootstrap is initiated by state model transition
   * 2. current replica has caught up with one local DC peer (sync up is still in progress)
   * 3. current replica is still behind one remote DC peer for more than acceptable threshold
   * 4. current replica has caught up with another two peer replicas (sync up is complete)
   * @throws Exception
   */
  @Test
  public void bootstrapBasicTest() throws Exception {
    CountDownLatch stateModelLatch = new CountDownLatch(1);
    // the listener latch is to ensure state change listener in ReplicationManager or StorageManager is invoked before
    // moving forward to rest tests.
    mockHelixParticipant.listenerLatch = new CountDownLatch(1);
    mockHelixParticipant.registerMockStateChangeListeners();
    // create a new thread and trigger BOOTSTRAP -> STANDBY transition
    Utils.newThread(() -> {
      stateModel.onBecomeStandbyFromBootstrap(mockMessage, null);
      stateModelLatch.countDown();
    }, false).start();
    assertTrue("State change listener didn't get invoked within 1 sec.",
        mockHelixParticipant.listenerLatch.await(1, TimeUnit.SECONDS));
    assertEquals("current replica should be in BOOTSTRAP state", ReplicaState.BOOTSTRAP,
        mockHelixParticipant.replicaState);
    assertFalse("Catchup shouldn't complete on current replica", replicaSyncUpService.isSyncUpComplete(currentReplica));
    ReplicaId localPeer1 = localDcPeerReplicas.get(0);
    ReplicaId localPeer2 = localDcPeerReplicas.get(1);
    ReplicaId remotePeer1 = remoteDcPeerReplicas.get(0);
    ReplicaId remotePeer2 = remoteDcPeerReplicas.get(1);
    // make current replica catch up with one peer replica in local DC
    assertFalse("Catchup shouldn't complete on current replica because only one peer replica is caught up",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, localPeer1, 50L, ReplicaState.STANDBY));
    // update lag between current replica and one remote DC peer replica but lag is still > acceptable threshold (100L)
    assertFalse("Catchup shouldn't complete on current replica because only one peer replica is caught up",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, remotePeer1, 110L,
            ReplicaState.STANDBY));
    // make current replica catch up with second peer replica in local DC and one more remote DC replica
    assertTrue("Catch up should be complete on current replica because it has caught up at least 2 peer replicas",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, localPeer2, 10L, ReplicaState.STANDBY));
    assertFalse("Catchup is completed by previous update not this one",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, remotePeer2, 10L,
            ReplicaState.STANDBY));
    // make current replica fall behind first peer replica in local DC again (update lag to 150 > 100)
    assertFalse("Catchup is completed by previous update. Current update should return false.",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, localPeer1, 150L,
            ReplicaState.STANDBY));
    assertTrue("Bootstrap-To-Standby transition didn't complete within 1 sec.",
        stateModelLatch.await(1, TimeUnit.SECONDS));
    // reset ReplicaSyncUpManager
    replicaSyncUpService.reset();
  }

  /**
   * Basic tests for deactivation process (STANDBY -> INACTIVE transition)
   * @throws Exception
   */
  @Test
  public void deactivationBasicTest() throws Exception {
    CountDownLatch stateModelLatch = new CountDownLatch(1);
    // the listener latch is to ensure state change listener in ReplicationManager or StorageManager is invoked before
    // moving forward to rest tests.
    mockHelixParticipant.listenerLatch = new CountDownLatch(1);
    mockHelixParticipant.registerMockStateChangeListeners();
    // create a new thread and trigger STANDBY -> INACTIVE transition
    Utils.newThread(() -> {
      stateModel.onBecomeInactiveFromStandby(mockMessage, null);
      stateModelLatch.countDown();
    }, false).start();
    assertTrue("State change listener didn't get invoked within 1 sec.",
        mockHelixParticipant.listenerLatch.await(1, TimeUnit.SECONDS));
    assertEquals("current replica should be in INACTIVE state", ReplicaState.INACTIVE,
        mockHelixParticipant.replicaState);
    assertFalse("Catchup shouldn't complete on current replica", replicaSyncUpService.isSyncUpComplete(currentReplica));
    ReplicaId localPeer1 = localDcPeerReplicas.get(0);
    ReplicaId localPeer2 = localDcPeerReplicas.get(1);
    // make localPeer1 catch up with current replica but localPeer2 still falls behind.
    assertFalse("Catchup shouldn't complete on current replica because only one peer replica has caught up",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, localPeer1, 0L, ReplicaState.INACTIVE));
    assertFalse("Catchup shouldn't complete on current replica because only one peer replica has caught up",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, localPeer2, 5L, ReplicaState.INACTIVE));
    // make localPeer2 catch up with current replica.
    assertTrue("Sync up should be complete on current replica because 2 peer replicas have caught up with it",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(currentReplica, localPeer2, 0L, ReplicaState.INACTIVE));
    assertTrue("Standby-To-Inactive transition didn't complete within 1 sec.",
        stateModelLatch.await(1, TimeUnit.SECONDS));
    // reset ReplicaSyncUpManager
    replicaSyncUpService.reset();
  }

  /**
   * Test several failure cases where replica is not present in ReplicaSyncUpManager.
   * @throws Exception
   */
  @Test
  public void replicaNotFoundFailureTest() throws Exception {
    // get another partition that is not present in ReplicaSyncUpManager
    PartitionId partition = clusterMap.getAllPartitionIds(null).get(1);
    ReplicaId replicaToTest = partition.getReplicaIds().get(0);
    ReplicaId peerReplica = replicaToTest.getPeerReplicaIds().get(0);
    try {
      replicaSyncUpService.isSyncUpComplete(replicaToTest);
      fail("should fail because replica is not present in ReplicaSyncUpManager");
    } catch (IllegalStateException e) {
      // expected
    }
    assertFalse("Updating lag should return false because replica is not present",
        replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(replicaToTest, peerReplica, 100L,
            ReplicaState.STANDBY));
    try {
      replicaSyncUpService.onBootstrapError(replicaToTest);
      fail("should fail because replica is not present");
    } catch (IllegalStateException e) {
      // expected
    }
    // wait for bootstrap to complete should be no op
    replicaSyncUpService.waitBootstrapCompleted(partition.toPathString());
    replicaSyncUpService.reset();
  }

  /**
   * Test failure cases during STANDBY -> INACTIVE transition
   */
  @Test
  public void deactivationFailureTest() throws Exception {
    // test deactivation failure for some reason (triggered by calling onDeactivationError)
    CountDownLatch stateModelLatch = new CountDownLatch(1);
    mockHelixParticipant.listenerLatch = new CountDownLatch(1);
    mockHelixParticipant.registerMockStateChangeListeners();
    // create a new thread and trigger STANDBY -> INACTIVE transition
    Utils.newThread(() -> {
      try {
        stateModel.onBecomeInactiveFromStandby(mockMessage, null);
      } catch (StateTransitionException e) {
        assertEquals("Error code doesn't match", DeactivationFailure, e.getErrorCode());
        stateModelLatch.countDown();
      }
    }, false).start();
    assertTrue("State change listener didn't get invoked within 1 sec.",
        mockHelixParticipant.listenerLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.onDeactivationError(currentReplica);
    assertTrue("Standby-To-Inactive transition didn't complete within 1 sec.",
        stateModelLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.reset();
  }

  /**
   * Test BOOTSTRAP -> STANDBY transition failure
   */
  @Test
  public void bootstrapFailureTest() throws Exception {
    CountDownLatch stateModelLatch = new CountDownLatch(1);
    mockHelixParticipant.listenerLatch = new CountDownLatch(1);
    mockHelixParticipant.registerMockStateChangeListeners();
    // create a new thread and trigger BOOTSTRAP -> STANDBY transition
    Utils.newThread(() -> {
      try {
        stateModel.onBecomeStandbyFromBootstrap(mockMessage, null);
      } catch (StateTransitionException e) {
        assertEquals("Error code doesn't match", BootstrapFailure, e.getErrorCode());
        stateModelLatch.countDown();
      }
    }, false).start();
    assertTrue("State change listener didn't get invoked within 1 sec.",
        mockHelixParticipant.listenerLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.onBootstrapError(currentReplica);
    assertTrue("Bootstrap-To-Standby transition didn't complete within 1 sec.",
        stateModelLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.reset();
  }

  /**
   * Test INACTIVE -> OFFLINE transition failure.
   * @throws Exception
   */
  @Test
  public void disconnectionFailureTest() throws Exception {
    // test disconnection failure (this is induced by call onDisconnectionError)
    CountDownLatch stateModelLatch = new CountDownLatch(1);
    mockHelixParticipant.listenerLatch = new CountDownLatch(1);
    mockHelixParticipant.registerMockStateChangeListeners();
    // create a new thread and trigger STANDBY -> INACTIVE transition
    Utils.newThread(() -> {
      try {
        stateModel.onBecomeOfflineFromInactive(mockMessage, null);
      } catch (StateTransitionException e) {
        assertEquals("Error code doesn't match", DisconnectionFailure, e.getErrorCode());
        stateModelLatch.countDown();
      }
    }, false).start();
    assertTrue("State change listener didn't get invoked within 1 sec.",
        mockHelixParticipant.listenerLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.onDisconnectionError(currentReplica);
    assertTrue("Inactive-To-Offline transition didn't complete within 1 sec.",
        stateModelLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.reset();
  }
}
