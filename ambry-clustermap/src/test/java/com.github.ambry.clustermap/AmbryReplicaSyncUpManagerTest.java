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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
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
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link AmbryReplicaSyncUpManager}.
 */
public class AmbryReplicaSyncUpManagerTest {
  private static final String RESOURCE_NAME = "0";
  private AmbryPartitionStateModel stateModel;
  private AmbryReplicaSyncUpManager replicaSyncUpService;
  private ReplicaId currentReplica;
  private MockPartitionStateChangeListener mockStateChangeListener;
  private Message mockMessage;
  private final List<ReplicaId> localDcPeerReplicas;
  private final List<ReplicaId> remoteDcPeerReplicas;
  private final MockClusterMap clusterMap;
  private CountDownLatch listenerLatch;
  private ReplicaState replicaState = ReplicaState.OFFLINE;

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
    Properties properties = new Properties();
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.replica.catchup.acceptable.lag.bytes", Long.toString(100L));
    properties.setProperty("clustermap.enable.state.model.listener", Boolean.toString(true));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    replicaSyncUpService = new AmbryReplicaSyncUpManager(clusterMapConfig);
    mockStateChangeListener = new MockPartitionStateChangeListener();
    stateModel =
        new AmbryPartitionStateModel(RESOURCE_NAME, partition.toPathString(), mockStateChangeListener, clusterMapConfig,
            replicaSyncUpService);
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
    listenerLatch = new CountDownLatch(1);
    // create a new thread and trigger BOOTSTRAP -> STANDBY transition
    Utils.newThread(() -> {
      stateModel.onBecomeStandbyFromBootstrap(mockMessage, null);
      stateModelLatch.countDown();
    }, false).start();
    assertTrue("State change listener didn't get invoked within 1 sec.", listenerLatch.await(1, TimeUnit.SECONDS));
    assertEquals("current replica should be in BOOTSTRAP state", ReplicaState.BOOTSTRAP, replicaState);
    assertFalse("Catchup shouldn't complete on current replica", replicaSyncUpService.isSyncUpComplete(currentReplica));
    ReplicaId localPeer1 = localDcPeerReplicas.get(0);
    ReplicaId localPeer2 = localDcPeerReplicas.get(1);
    ReplicaId remotePeer1 = remoteDcPeerReplicas.get(0);
    ReplicaId remotePeer2 = remoteDcPeerReplicas.get(1);
    // make current replica catch up with one peer replica in local DC
    replicaSyncUpService.updateLagBetweenReplicas(currentReplica, localPeer1, 50L);
    assertFalse("Catchup shouldn't complete on current replica because only one peer replica is caught up",
        replicaSyncUpService.isSyncUpComplete(currentReplica));
    // update lag between current replica and one remote DC peer replica but lag is still > acceptable threshold (100L)
    replicaSyncUpService.updateLagBetweenReplicas(currentReplica, remotePeer1, 110L);
    assertFalse("Catchup shouldn't complete on current replica because only one peer replica is caught up",
        replicaSyncUpService.isSyncUpComplete(currentReplica));
    // make current replica catch up with second peer replica in local DC and one more remote DC replica
    replicaSyncUpService.updateLagBetweenReplicas(currentReplica, localPeer2, 10L);
    replicaSyncUpService.updateLagBetweenReplicas(currentReplica, remotePeer2, 10L);
    // make current replica fall behind first peer replica in local DC again (update lag to 150 > 100)
    replicaSyncUpService.updateLagBetweenReplicas(currentReplica, localPeer1, 150L);
    // at this time, current replica has caught up with two replicas in local DC, so SyncUp is complete
    assertTrue("Catch up should be complete on current replica because it has caught up at least 2 peer replicas",
        replicaSyncUpService.isSyncUpComplete(currentReplica));
    replicaSyncUpService.onBootstrapComplete(currentReplica);
    assertTrue("Bootstrap-To-Standby transition didn't complete within 1 sec.",
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
        replicaSyncUpService.updateLagBetweenReplicas(replicaToTest, peerReplica, 100L));
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
   * Test BOOTSTRAP -> STANDBY transition failure
   */
  @Test
  public void bootstrapFailureTest() throws Exception {
    CountDownLatch stateModelLatch = new CountDownLatch(1);
    listenerLatch = new CountDownLatch(1);
    // create a new thread and trigger BOOTSTRAP -> STANDBY transition
    Utils.newThread(() -> {
      try {
        stateModel.onBecomeStandbyFromBootstrap(mockMessage, null);
      } catch (StateTransitionException e) {
        assertEquals("Error code doesn't match", StateTransitionException.TransitionErrorCode.BootstrapFailure,
            e.getErrorCode());
        stateModelLatch.countDown();
      }
    }, false).start();
    assertTrue("State change listener didn't get invoked within 1 sec.", listenerLatch.await(1, TimeUnit.SECONDS));
    replicaSyncUpService.onBootstrapError(currentReplica);
    assertTrue("Bootstrap-To-Standby transition didn't complete within 1 sec.",
        stateModelLatch.await(1, TimeUnit.SECONDS));
  }

  /**
   * Implementation of {@link PartitionStateChangeListener} to help {@link ReplicaSyncUpManager} tests
   */
  private class MockPartitionStateChangeListener implements PartitionStateChangeListener {
    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      replicaState = ReplicaState.BOOTSTRAP;
      replicaSyncUpService.initiateBootstrap(currentReplica);
      listenerLatch.countDown();
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
    }
  }
}
