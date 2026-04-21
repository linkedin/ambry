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
    this(new Properties());
  }

  /**
   * Constructor that accepts additional properties for test customization.
   */
  AmbryReplicaSyncUpManagerTest(Properties extraProperties) throws IOException {
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
    properties.putAll(extraProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);
    replicaSyncUpService = (AmbryReplicaSyncUpManager) mockHelixParticipant.getReplicaSyncUpManager();
    mockHelixParticipant.currentReplica = currentReplica;
    mockHelixParticipant.replicaSyncUpService = replicaSyncUpService;
    stateModel =
        new AmbryPartitionStateModel(RESOURCE_NAME, partition.toPathString(), mockHelixParticipant, clusterMapConfig,
            mock(HelixClusterManager.class));
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

  /**
   * Test that bootstrap, deactivation, and disconnection complete immediately for a single-replica partition.
   * When a partition has only one replica, there are no peers to replicate from/to, so
   * updateReplicaLagAndCheckSyncStatus is never called. The sync-up manager should detect this at initiation
   * time and complete the operation immediately instead of blocking forever.
   * @throws Exception
   */
  @Test
  public void singleReplicaPartitionTest() throws Exception {
    // Create a partition with a single replica by using a single data node
    List<MockDataNodeId> singleNodeList = new ArrayList<>();
    singleNodeList.add(clusterMap.getDataNodes().get(0));
    MockPartitionId singleReplicaPartition = new MockPartitionId(999, MockClusterMap.DEFAULT_PARTITION_CLASS,
        singleNodeList, 0);
    ReplicaId singleReplica = singleReplicaPartition.getReplicaIds().get(0);
    assertEquals("Single-replica partition should have exactly 1 replica", 1,
        singleReplicaPartition.getReplicaIds().size());

    // Test bootstrap: should complete immediately without blocking
    replicaSyncUpService.initiateBootstrap(singleReplica);
    // waitBootstrapCompleted should return immediately since there are no peers
    CountDownLatch bootstrapLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      try {
        replicaSyncUpService.waitBootstrapCompleted(singleReplicaPartition.toPathString());
        bootstrapLatch.countDown();
      } catch (InterruptedException e) {
        // should not happen
      }
    }, false).start();
    assertTrue("Bootstrap should complete immediately for single-replica partition",
        bootstrapLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.reset();

    // Test deactivation: should complete immediately without blocking
    replicaSyncUpService.initiateDeactivation(singleReplica);
    CountDownLatch deactivationLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      try {
        replicaSyncUpService.waitDeactivationCompleted(singleReplicaPartition.toPathString());
        deactivationLatch.countDown();
      } catch (InterruptedException e) {
        // should not happen
      }
    }, false).start();
    assertTrue("Deactivation should complete immediately for single-replica partition",
        deactivationLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.reset();

    // Test disconnection: should complete immediately without blocking
    replicaSyncUpService.initiateDisconnection(singleReplica);
    CountDownLatch disconnectionLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      try {
        replicaSyncUpService.waitDisconnectionCompleted(singleReplicaPartition.toPathString());
        disconnectionLatch.countDown();
      } catch (InterruptedException e) {
        // should not happen
      }
    }, false).start();
    assertTrue("Disconnection should complete immediately for single-replica partition",
        disconnectionLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.reset();
  }

  /**
   * Test that when multi-DC bootstrap safety check is enabled and all local DC peers are bootstrapping (not in
   * STANDBY/LEADER), catching up with peers from only one remote DC is insufficient. The bootstrapping replica must
   * catch up with peers from at least 2 distinct remote DCs before completing bootstrap.
   *
   * This simulates a multi-fabric crash where lor1 and lva1 are both down: a lor1 replica could sync with empty
   * lva1 replicas (stale ExternalView shows them as STANDBY) and prematurely transition to STANDBY with no data.
   * Requiring 2 distinct remote DCs ensures at least one healthy fabric contributes to catchup.
   */
  @Test
  public void testMultiDcBootstrapSafetyCheck() throws Exception {
    // Create a new SyncUpManager with the multi-DC safety check enabled and catchup target=2
    Properties multiDcProps = new Properties();
    multiDcProps.setProperty("clustermap.replica.catchup.require.multi.dc.for.bootstrap", "true");
    multiDcProps.setProperty("clustermap.replica.catchup.target", "2");
    AmbryReplicaSyncUpManagerTest testWithMultiDc = new AmbryReplicaSyncUpManagerTest(multiDcProps);

    // Simulate: all local DC peers are in BOOTSTRAP (not STANDBY/LEADER), so they won't appear in peer list.
    // Set local peers' state to BOOTSTRAP so getReplicaIdsByState(STANDBY/LEADER) won't include them.
    MockPartitionId partition = (MockPartitionId) testWithMultiDc.currentReplica.getPartitionId();
    for (ReplicaId localPeer : testWithMultiDc.localDcPeerReplicas) {
      partition.replicaAndState.put(localPeer, ReplicaState.BOOTSTRAP);
    }

    testWithMultiDc.replicaSyncUpService.initiateBootstrap(testWithMultiDc.currentReplica);

    // Separate remote peers by DC
    List<ReplicaId> dc2Peers = testWithMultiDc.remoteDcPeerReplicas.stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals("DC2"))
        .collect(Collectors.toList());
    List<ReplicaId> dc3Peers = testWithMultiDc.remoteDcPeerReplicas.stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals("DC3"))
        .collect(Collectors.toList());
    assertTrue("Test requires peers in DC2", dc2Peers.size() > 0);
    assertTrue("Test requires peers in DC3", dc3Peers.size() > 0);

    // Catch up with 2 peers from DC2 only (simulates syncing with a single recovering fabric)
    assertFalse("Should not complete: caught up with only 1 DC2 peer",
        testWithMultiDc.replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(
            testWithMultiDc.currentReplica, dc2Peers.get(0), 0L, ReplicaState.STANDBY));
    assertFalse("Should not complete: caught up with 2 DC2 peers but all from same DC",
        testWithMultiDc.replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(
            testWithMultiDc.currentReplica, dc2Peers.get(1), 0L, ReplicaState.STANDBY));

    // Now catch up with 1 peer from DC3 — should complete because we have 2 distinct DCs
    assertTrue("Should complete: caught up with peers from 2 distinct remote DCs",
        testWithMultiDc.replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(
            testWithMultiDc.currentReplica, dc3Peers.get(0), 0L, ReplicaState.STANDBY));

    testWithMultiDc.replicaSyncUpService.reset();
  }

  /**
   * Test that the multi-DC safety check does NOT apply when local DC peers are available (normal case).
   * Even with the flag enabled, if local peers are caught up, bootstrap should complete normally.
   */
  @Test
  public void testMultiDcCheckSkippedWhenLocalPeersAvailable() throws Exception {
    Properties multiDcProps = new Properties();
    multiDcProps.setProperty("clustermap.replica.catchup.require.multi.dc.for.bootstrap", "true");
    multiDcProps.setProperty("clustermap.replica.catchup.target", "2");
    AmbryReplicaSyncUpManagerTest testWithMultiDc = new AmbryReplicaSyncUpManagerTest(multiDcProps);

    testWithMultiDc.replicaSyncUpService.initiateBootstrap(testWithMultiDc.currentReplica);

    // Catch up with 2 local DC peers — should complete without needing multi-DC remote peers
    ReplicaId localPeer1 = testWithMultiDc.localDcPeerReplicas.get(0);
    ReplicaId localPeer2 = testWithMultiDc.localDcPeerReplicas.get(1);
    assertFalse("Should not complete: only 1 local peer caught up",
        testWithMultiDc.replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(
            testWithMultiDc.currentReplica, localPeer1, 50L, ReplicaState.STANDBY));
    assertTrue("Should complete: 2 local peers caught up, multi-DC check should not apply",
        testWithMultiDc.replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(
            testWithMultiDc.currentReplica, localPeer2, 10L, ReplicaState.STANDBY));

    testWithMultiDc.replicaSyncUpService.reset();
  }

  /**
   * Test that the multi-DC safety check is not enforced when the flag is disabled (default behavior).
   * Catching up with peers from a single remote DC should be sufficient.
   */
  @Test
  public void testMultiDcCheckDisabledByDefault() throws Exception {
    // Create a SyncUpManager with catchup target=2 but multi-DC check disabled
    Properties props = new Properties();
    props.setProperty("clustermap.replica.catchup.target", "2");
    AmbryReplicaSyncUpManagerTest testWithoutMultiDc = new AmbryReplicaSyncUpManagerTest(props);

    // Simulate: all local DC peers are in BOOTSTRAP
    MockPartitionId partition = (MockPartitionId) testWithoutMultiDc.currentReplica.getPartitionId();
    for (ReplicaId localPeer : testWithoutMultiDc.localDcPeerReplicas) {
      partition.replicaAndState.put(localPeer, ReplicaState.BOOTSTRAP);
    }

    testWithoutMultiDc.replicaSyncUpService.initiateBootstrap(testWithoutMultiDc.currentReplica);

    // Separate remote peers by DC
    List<ReplicaId> dc2Peers = testWithoutMultiDc.remoteDcPeerReplicas.stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals("DC2"))
        .collect(Collectors.toList());
    assertTrue("Test requires at least 2 DC2 peers", dc2Peers.size() >= 2);

    // Catch up with 2 peers from DC2 only — should complete because flag is off
    testWithoutMultiDc.replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(
        testWithoutMultiDc.currentReplica, dc2Peers.get(0), 0L, ReplicaState.STANDBY);
    assertTrue("Should complete with single-DC remote peers when multi-DC check is disabled",
        testWithoutMultiDc.replicaSyncUpService.updateReplicaLagAndCheckSyncStatus(
            testWithoutMultiDc.currentReplica, dc2Peers.get(1), 0L, ReplicaState.STANDBY));

    testWithoutMultiDc.replicaSyncUpService.reset();
  }
}
