/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockHelixParticipant;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class LeaderBasedReplicationTest extends ReplicationTestHelper {

  private MockClusterMap clusterMap;
  private ClusterMapConfig clusterMapConfig;
  private MockHelixParticipant mockHelixParticipant;
  // remote node in local data center that shares same partitions as local node
  private DataNodeId remoteNodeInLocalDC;
  // remote node in remote data center that shares same partitions as local node
  private DataNodeId remoteNodeInRemoteDC;
  // mock host representing local host
  private MockHost localHost;
  // mock host representing remote host in local data center
  private MockHost remoteHostInLocalDC;
  // mock host representing remote host in remote data center
  private MockHost remoteHostInRemoteDC;

  /**
   * Constructor to set the configs
   */
  public LeaderBasedReplicationTest(short requestVersion, short responseVersion) throws IOException {
    super(requestVersion, responseVersion, false);
    setUp();
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
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5},
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V2, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6},
    });
    //@formatter:on
  }

  public void setUp() throws IOException {
    properties.setProperty("replication.model.across.datacenters", "LEADER_BASED");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));

    clusterMap = new MockClusterMap();
    clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);

    /*
      Setup:
      we have 3 nodes that have replicas belonging to same partitions:
      a) localNode (local node that hosts partitions)
      b) remoteNodeInLocalDC (remote node in local data center that shares the partitions)
      c) remoteNodeInRemoteDC (remote node in remote data center that shares the partitions)

      Each node have few of its partitions as leaders and others are standby. They are randomly assigned during creation
      of replicas for mock partitions.
     */
    DataNodeId localNode = clusterMap.getDataNodeIds().get(0);
    List<DataNodeId> remoteNodes = getRemoteNodesFromLocalAndRemoteDCs(clusterMap, localNode);
    remoteNodeInLocalDC = remoteNodes.get(0);
    remoteNodeInRemoteDC = remoteNodes.get(1);

    // mock hosts for remote nodes
    localHost = new MockHost(localNode, clusterMap);
    remoteHostInLocalDC = new MockHost(remoteNodeInLocalDC, clusterMap);
    remoteHostInRemoteDC = new MockHost(remoteNodeInRemoteDC, clusterMap);
  }

  /**
   * Tests to verify replication model is correctly reflected in code based on config properties
   */
  @Test
  public void replicationTypeFromConfigTest() {

    ReplicationConfig initialReplicationConfig = replicationConfig;
    properties.remove("replication.model.across.datacenters");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));

    //When replication config is missing, replicationModelType should be defaulted to ALL_TO_ALL
    assertEquals("Replication model mismatch from the value present in config",
        replicationConfig.replicationModelAcrossDatacenters, ReplicationModelType.ALL_TO_ALL);

    //When the config set is "LEADER_BASED", replicationModelType should be LEADER_BASED
    properties.setProperty("replication.model.across.datacenters", "LEADER_BASED");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));
    assertEquals("Replication model mismatch from the value present in config",
        replicationConfig.replicationModelAcrossDatacenters, ReplicationModelType.LEADER_BASED);

    //When the config set is "ALL_TO_ALL", replicationModelType should be ALL_TO_ALL
    properties.setProperty("replication.model.across.datacenters", "ALL_TO_ALL");
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));
    assertEquals("Replication model mismatch from the value present in config",
        replicationConfig.replicationModelAcrossDatacenters, ReplicationModelType.ALL_TO_ALL);

    replicationConfig = initialReplicationConfig;
  }

  /**
   * Test cluster map change callback in {@link ReplicationManager} for routing table updates.
   * Test setup: When creating partitions, have one replica in LEADER state and rest in STANDBY states on each data center and
   * later switch the states of replicas (LEADER to STANDBY and STANDBY to LEADER) on one of the DCs during the test
   * Test condition: When replication manager receives onRoutingTableUpdate() indication after the remote replica states were updated,
   * map of partition to peer leader replicas stored in replication manager should be updated correctly
   * @throws Exception
   */
  @Test
  public void onRoutingTableUpdateCallbackTest() throws Exception {
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();
    replicationManager.start();

    //Trigger PartitionStateChangeListener callback to replication manager to notify that a local replica state has changed from STANDBY to LEADER
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      if (mockReplicaId.getReplicaState() == ReplicaState.LEADER) {
        MockPartitionId existingPartition = (MockPartitionId) mockReplicaId.getPartitionId();
        mockHelixParticipant.onPartitionBecomeLeaderFromStandby(existingPartition.toPathString());

        //verify that map of peerLeaderReplicasByPartition in PartitionLeaderInfo is updated correctly
        Set<ReplicaId> peerLeaderReplicasInPartitionLeaderInfo =
            replicationManager.leaderBasedReplicationAdmin.getLeaderPartitionToPeerLeaderReplicas()
                .get(existingPartition.toPathString());
        Set<ReplicaId> peerLeaderReplicasInClusterMap =
            new HashSet<>(existingPartition.getReplicaIdsByState(ReplicaState.LEADER, null));
        peerLeaderReplicasInClusterMap.remove(mockReplicaId);
        assertThat(
            "Mismatch in list of leader peer replicas stored by partition in replication manager with cluster map",
            peerLeaderReplicasInPartitionLeaderInfo, is(peerLeaderReplicasInClusterMap));

        //Switch the LEADER/STANDBY states for remote replicas on one of the remote data centers
        ReplicaId peerLeaderReplica = peerLeaderReplicasInClusterMap.iterator().next();
        ReplicaId peerStandByReplica = existingPartition.getReplicaIdsByState(ReplicaState.STANDBY,
            peerLeaderReplica.getDataNodeId().getDatacenterName()).get(0);
        existingPartition.setReplicaState(peerLeaderReplica, ReplicaState.STANDBY);
        existingPartition.setReplicaState(peerStandByReplica, ReplicaState.LEADER);

        //Trigger routing table change callback to replication manager
        ClusterMapChangeListener clusterMapChangeListener = clusterMap.getClusterMapChangeListener();
        clusterMapChangeListener.onRoutingTableChange();

        //verify that new remote leader is reflected in the peerLeaderReplicasByPartition map
        peerLeaderReplicasInPartitionLeaderInfo =
            replicationManager.leaderBasedReplicationAdmin.getLeaderPartitionToPeerLeaderReplicas()
                .get(existingPartition.toPathString());
        peerLeaderReplicasInClusterMap =
            new HashSet<>(existingPartition.getReplicaIdsByState(ReplicaState.LEADER, null));
        peerLeaderReplicasInClusterMap.remove(mockReplicaId);
        assertThat(
            "Mismatch in map of peer leader replicas stored by partition in replication manager with cluster map after routing table update",
            peerLeaderReplicasInPartitionLeaderInfo, is(peerLeaderReplicasInClusterMap));
      }
    }

    storageManager.shutdown();
  }

  /**
   * Test leader based replication to ensure token is advanced correctly for standby replicas when missing PUT messages
   * are fetched via intra-dc replication.
   * @throws Exception
   */
  @Test
  public void replicaThreadLeaderBasedReplicationForPUTMessagesTest() throws Exception {

    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();

    /*
    Scenario:

    we have 3 nodes that have replicas belonging to same partitions:
       a) local node
       b) remote node in local DC
       c) remote node in remote DC

    Each node have few of its partitions as leaders and others are standby. They are randomly assigned during creation
    of replicas for mock partitions.

    We have 4 PUT messages in each of the partitions in remote nodes of local DC and remote DC that needs to be
    replicated at local node.

    Steps:

    1. Replicate (send metadata exchange and get messages) with remote node in remote DC (cross-colo replication).

       Expectations:
        a) We should see that metadata exchange is sent for all replicas while GET messages are only sent for leader replicas.
        b) All the PUT messages should be replicated locally and remote token should be moved forward for leader partitions.
        c) For non-leader replicas, metadata response should be stored locally.

    2. Replicate (send metadata exchange and get messages) with remote node in local DC (intra-colo replication).

       Expectations:
        a) Metadata exchange and GET messages are sent for all replicas.
        b) PUT messages should be replicated locally for all replicas.
        c) Missing messages in stored metadata response of non-leader replicas for remoteNodeInRemoteDC should become empty
           and remote token should be advanced.

     */

    int batchSize = 4;

    // set mock local stores on all remoteReplicaInfos which will used during replication.
    for (PartitionId partitionId : replicationManager.partitionToPartitionInfo.keySet()) {
      localHost.addStore(partitionId, null);
      Store localStore = localHost.getStore(partitionId);
      localStore.start();
      List<RemoteReplicaInfo> remoteReplicaInfos =
          replicationManager.partitionToPartitionInfo.get(partitionId).getRemoteReplicaInfos();
      remoteReplicaInfos.forEach(remoteReplicaInfo -> remoteReplicaInfo.setLocalStore(localStore));
    }

    // get remote replicas and replica thread for remote host on local datacenter
    ReplicaThread intraColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInLocalDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForLocalDC =
        intraColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInLocalDC);

    // get remote replicas and replica thread for remote host on remote datacenter
    ReplicaThread crossColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInRemoteDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForRemoteDC =
        crossColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInRemoteDC);

    // mock helix transition state from standby to leader for local leader partitions
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      if (mockReplicaId.getReplicaState() == ReplicaState.LEADER) {
        MockPartitionId mockPartitionId = (MockPartitionId) replicaId.getPartitionId();
        mockHelixParticipant.onPartitionBecomeLeaderFromStandby(mockPartitionId.toPathString());
      }
    }

    //Add put messages to all partitions on remoteHost1 and remoteHost2
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add batchSize messages to the remoteHost1 and remote host 2 from which local host will replicate.
      addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHostInLocalDC, remoteHostInRemoteDC),
          batchSize);
    }

    // Choose partitions that are leaders on both local and remote nodes
    Set<ReplicaId> leaderReplicasOnLocalAndRemoteNodes =
        getRemoteLeaderReplicasWithLeaderPartitionsOnLocalNode(clusterMap, replicationManager.dataNodeId,
            remoteNodeInRemoteDC);

    // Replicate with remoteHost2 in remote data center.
    List<ReplicaThread.ExchangeMetadataResponse> responseListForRemoteNodeInRemoteDC =
        crossColoReplicaThread.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHostInRemoteDC, batchSize),
            remoteReplicaInfosForRemoteDC);

    // Metadata requests should be sent to both leader and standby replicas.
    assertEquals("Response should contain a response for each replica", remoteReplicaInfosForRemoteDC.size(),
        responseListForRemoteNodeInRemoteDC.size());

    // verify that missing messages size equals to the min{batch size, number of PUT messages} placed on remote hosts
    int expectedIndex = batchSize - 1;
    for (ReplicaThread.ExchangeMetadataResponse exchangeMetadataResponse : responseListForRemoteNodeInRemoteDC) {
      assertEquals("mismatch in number of missing messages", batchSize,
          exchangeMetadataResponse.missingStoreMessages.size());
    }

    // Filter leader replicas to fetch missing keys
    List<RemoteReplicaInfo> leaderReplicas = new ArrayList<>();
    List<ReplicaThread.ExchangeMetadataResponse> exchangeMetadataResponseListForLeaderReplicas = new ArrayList<>();
    crossColoReplicaThread.getLeaderReplicaList(remoteReplicaInfosForRemoteDC, responseListForRemoteNodeInRemoteDC,
        leaderReplicas, exchangeMetadataResponseListForLeaderReplicas);

    // verify that only leader replicas in remoteHost2 are chosen for fetching missing messages.
    Set<ReplicaId> remoteReplicasToFetchInReplicaThread =
        leaderReplicas.stream().map(RemoteReplicaInfo::getReplicaId).collect(Collectors.toSet());
    assertThat("mismatch in leader remote replicas to fetch missing keys", leaderReplicasOnLocalAndRemoteNodes,
        is(remoteReplicasToFetchInReplicaThread));

    // fetch missing keys for leader replicas from remoteHost2
    if (leaderReplicas.size() > 0) {
      crossColoReplicaThread.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHostInRemoteDC, batchSize),
          leaderReplicas, exchangeMetadataResponseListForLeaderReplicas, false);
    }

    // verify that the remote token will be moved for leader replicas and will remain 0 for standby replicas as
    // missing messages are not fetched yet.
    for (int i = 0; i < remoteReplicaInfosForRemoteDC.size(); i++) {
      if (leaderReplicasOnLocalAndRemoteNodes.contains(remoteReplicaInfosForRemoteDC.get(i).getReplicaId())) {
        assertEquals("remote token mismatch for leader replicas", remoteReplicaInfosForRemoteDC.get(i).getToken(),
            responseListForRemoteNodeInRemoteDC.get(i).remoteToken);
      } else {
        assertEquals("missing keys in metadata response should be stored for standby replicas",
            remoteReplicaInfosForRemoteDC.get(i).getExchangeMetadataResponse().missingStoreMessages.size(),
            responseListForRemoteNodeInRemoteDC.get(i).missingStoreMessages.size());
        assertThat("remote token should not move forward for standby replicas until missing keys are fetched",
            remoteReplicaInfosForRemoteDC.get(i).getToken(),
            not(responseListForRemoteNodeInRemoteDC.get(i).remoteToken));
      }
    }

    // Replication with remoteHost1 in local data center
    List<ReplicaThread.ExchangeMetadataResponse> responseForRemoteNodeInLocalDC =
        intraColoReplicaThread.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHostInLocalDC, batchSize),
            remoteReplicaInfosForLocalDC);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfosForLocalDC.size(),
        responseForRemoteNodeInLocalDC.size());

    // fetch missing keys from remoteHost1
    intraColoReplicaThread.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHostInLocalDC, batchSize),
        remoteReplicaInfosForLocalDC, responseForRemoteNodeInLocalDC, false);

    for (int i = 0; i < responseForRemoteNodeInLocalDC.size(); i++) {
      assertEquals("mismatch in remote token set for intra colo replicas",
          remoteReplicaInfosForLocalDC.get(i).getToken(), (responseForRemoteNodeInLocalDC.get(i).remoteToken));
    }

    // process missing keys for cross colo standby replicas from previous metadata exchange
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      crossColoReplicaThread.processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
    }

    // Remote token for all cross-colo replicas (leader and standby) should have moved forward now as the missing keys
    // for standbys are received via intra-dc replication.
    for (int i = 0; i < responseListForRemoteNodeInRemoteDC.size(); i++) {
      assertEquals("mismatch in remote token set for cross colo replicas",
          remoteReplicaInfosForRemoteDC.get(i).getToken(), (responseListForRemoteNodeInRemoteDC.get(i).remoteToken));
    }

    // verify replication metrics to track number of cross colo get requests and cross colo bytes fetch rate for standby
    // replicas should be 0 since all missing blobs are obtained via local leader in intra-dc replication.
    String remoteDataCenter = remoteReplicaInfosForRemoteDC.get(0).getReplicaId().getDataNodeId().getDatacenterName();
    assertEquals("mismatch in number of cross colo get requests tracked for standby replicas",
        crossColoReplicaThread.getReplicationMetrics().interColoReplicationGetRequestCountForStandbyReplicas.get(
            remoteDataCenter).getCount(), 0);
    assertEquals("mismatch in bytes fetch rate for cross colo get requests tracked for standby replicas",
        crossColoReplicaThread.getReplicationMetrics().interColoReplicationFetchBytesRateForStandbyReplicas.get(
            remoteDataCenter).getCount(), 0);

    storageManager.shutdown();
  }

  /**
   * Test leader based replication to verify remote token is caught up for standby replicas and updated token is used
   * when their state transitions to leader.
   * @throws Exception
   */
  @Test
  public void replicaThreadLeaderBasedReplicationTokenCatchUpForStandbyToLeaderTest() throws Exception {

    /*
      Setup:
      we have 3 nodes that have replicas belonging to same partitions:
      a) localNode (local node that hosts partitions)
      b) remoteNodeInLocalDC (remote node in local data center that shares the partitions)
      c) remoteNodeInRemoteDC (remote node in remote data center that shares the partitions)

      Each node have few of its partitions as leaders and others are standby. They are randomly assigned during creation
      of replicas for mock partitions.
     */

    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    hosts.put(remoteNodeInLocalDC, remoteHostInLocalDC);
    hosts.put(remoteNodeInRemoteDC, remoteHostInRemoteDC);
    int batchSize = 5;

    ConnectionPool mockConnectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);

    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant,
            mockConnectionPool);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();

    /*
    Scenario:

    We have 10 PUT messages in each of the partitions in remote nodes of local DC and remote DC that needs to be
    replicated at local node. They will be fetched in 2 cycles by having batch size of replication as 5.

    After first cycle, we will trigger partition state change for local standby to leader. We should see that new leader
    has updated token info, i.e. it should start fetching from message number 6.
     */

    // set mock local stores on all remoteReplicaInfos which will used during replication.
    for (PartitionId partitionId : replicationManager.partitionToPartitionInfo.keySet()) {
      localHost.addStore(partitionId, null);
      Store localStore = localHost.getStore(partitionId);
      localStore.start();
      List<RemoteReplicaInfo> remoteReplicaInfos =
          replicationManager.partitionToPartitionInfo.get(partitionId).getRemoteReplicaInfos();
      remoteReplicaInfos.forEach(remoteReplicaInfo -> remoteReplicaInfo.setLocalStore(localStore));
    }

    // get remote replicas and replica thread for remote host on local datacenter
    ReplicaThread intraColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInLocalDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForLocalDC =
        intraColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInLocalDC);

    // get remote replicas and replica thread for remote host on remote datacenter
    ReplicaThread crossColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInRemoteDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForRemoteDC =
        crossColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInRemoteDC);

    // mock helix transition state from standby to leader for local leader partitions
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      if (mockReplicaId.getReplicaState() == ReplicaState.LEADER) {
        MockPartitionId mockPartitionId = (MockPartitionId) replicaId.getPartitionId();
        mockHelixParticipant.onPartitionBecomeLeaderFromStandby(mockPartitionId.toPathString());
      }
    }

    //Add put messages to all partitions on remoteHost1 and remoteHost2
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add batchSize messages to the remoteHost1 and remote host 2 from which local host will replicate.
      addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHostInLocalDC, remoteHostInRemoteDC),
          batchSize + batchSize);
    }

    // Choose partitions that are leaders on both local and remote nodes
    Set<ReplicaId> leaderReplicasOnLocalAndRemoteNodes = new HashSet<>();

    // Track a standby replica which has leader partition on remote node. We will update the state of replica to leader after one cycle of replication
    // and verify that replication resumes from remote token.
    MockReplicaId localStandbyReplicaWithLeaderPartitionOnRemoteNode = null;

    List<? extends ReplicaId> localReplicas = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    List<? extends ReplicaId> remoteReplicas = clusterMap.getReplicaIds(remoteNodeInRemoteDC);
    for (int i = 0; i < localReplicas.size(); i++) {
      MockReplicaId localReplica = (MockReplicaId) localReplicas.get(i);
      MockReplicaId remoteReplica = (MockReplicaId) remoteReplicas.get(i);
      if (localReplica.getReplicaState() == ReplicaState.LEADER
          && remoteReplica.getReplicaState() == ReplicaState.LEADER) {
        leaderReplicasOnLocalAndRemoteNodes.add(remoteReplicas.get(i));
      }
      if (localReplica.getReplicaState() == ReplicaState.STANDBY
          && remoteReplica.getReplicaState() == ReplicaState.LEADER
          && localStandbyReplicaWithLeaderPartitionOnRemoteNode == null) {
        localStandbyReplicaWithLeaderPartitionOnRemoteNode = localReplica;
      }
    }

    // replicate with remote node in remote DC
    crossColoReplicaThread.replicate();

    // verify that the remote token will be moved for leader replicas and will remain 0 for standby replicas as
    // missing messages are not fetched yet.
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      if (leaderReplicasOnLocalAndRemoteNodes.contains(remoteReplicaInfo.getReplicaId())) {
        assertEquals("remote token mismatch for leader replicas",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), batchSize - 1);
      } else {
        assertEquals("remote token should not move forward for standby replicas until missing keys are fetched",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), 0);
      }
    }

    //Replicate with remote node in local dc
    intraColoReplicaThread.replicate();

    // verify that remote token will be moved for all intra-colo replicas with token index = batchSize-1
    for (RemoteReplicaInfo replicaInfo : remoteReplicaInfosForLocalDC) {
      assertEquals("mismatch in remote token set for intra colo replicas",
          ((MockFindToken) replicaInfo.getToken()).getIndex(), batchSize - 1);
    }

    // process missing keys for cross colo replicas from previous metadata exchange
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      crossColoReplicaThread.processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
    }

    // verify that remote token will be moved for all (leader and standby) inter-colo replicas with token index = batchSize-1
    // as missing keys must now be obtained via intra-dc replication
    for (RemoteReplicaInfo replicaInfo : remoteReplicaInfosForRemoteDC) {
      assertEquals("mismatch in remote token set for inter colo replicas",
          ((MockFindToken) replicaInfo.getToken()).getIndex(), batchSize - 1);
    }

    // If we have a local standby replica with leader partition on remote node, change its state to leader
    if (localStandbyReplicaWithLeaderPartitionOnRemoteNode != null) {
      MockPartitionId mockPartitionId =
          (MockPartitionId) localStandbyReplicaWithLeaderPartitionOnRemoteNode.getPartitionId();
      mockHelixParticipant.onPartitionBecomeLeaderFromStandby(mockPartitionId.toPathString());
    }

    // Trigger replication again with remote node in remote DC
    crossColoReplicaThread.replicate();

    // verify that remaining missing blobs of count batchSize have been fetched (token index = batchSize*2-2) for all leader replicas including new
    // leader localStandbyReplicaWithLeaderPartitionOnRemoteNode whose replication should be have resumed with remote token index = batchSize-1
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      if (leaderReplicasOnLocalAndRemoteNodes.contains(remoteReplicaInfo.getReplicaId())
          || (remoteReplicaInfo.getLocalReplicaId().equals(localStandbyReplicaWithLeaderPartitionOnRemoteNode))) {
        assertEquals("remote token mismatch for leader replicas",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), batchSize * 2 - 2);
      } else {
        assertEquals("remote token should not move forward for standby replicas until missing keys are fetched",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), batchSize - 1);
      }
    }

    // Trigger replication again with remote node in local DC
    intraColoReplicaThread.replicate();

    // verify that remote token is moved forward for all intra-colo replicas.
    for (RemoteReplicaInfo replicaInfo : remoteReplicaInfosForLocalDC) {
      assertEquals("mismatch in remote token set for intra colo replicas",
          ((MockFindToken) replicaInfo.getToken()).getIndex(), batchSize * 2 - 2);
    }

    // process missing keys for cross colo replicas from previous metadata exchange
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      crossColoReplicaThread.processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
    }

    // verify that remote token is moved forward for all inter-colo replicas as missing keys must now be obtained
    // via intra-dc replication.
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      assertEquals("mismatch in remote token set for intra colo replicas",
          ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), batchSize * 2 - 2);
    }

    storageManager.shutdown();
  }

  /**
   * Test leader based replication to verify cross colo gets for standby replicas after they have have timed out
   * waiting for missing keys.
   * @throws Exception
   */
  @Test
  public void replicaThreadLeaderBasedReplicationStandByCrossColoFetchTest() throws Exception {

    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    hosts.put(remoteNodeInLocalDC, remoteHostInLocalDC);
    hosts.put(remoteNodeInRemoteDC, remoteHostInRemoteDC);
    int batchSize = 5;
    int numOfMessagesOnRemoteNodeInLocalDC = 3;
    int numOfMessagesOnRemoteNodeInRemoteDC = 10;

    ConnectionPool mockConnectionPool = new MockConnectionPool(hosts, clusterMap, batchSize);

    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant,
            mockConnectionPool);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();

    /*
    Scenario:

    we have 3 nodes that have replicas belonging to same partitions:
       a) local node
       b) remote node in local DC
       c) remote node in remote DC

    Each node have few of its partitions as leaders and others are standby. They are randomly assigned during creation
    of replicas for mock partitions.

    We have 3 PUT messages in each of the partition which needs to be replicated from remote node in local DC and remote node in remote DC

    We have additional 1 PUT message in each of the partition that is present only in remote node in remote DC

    Steps:

    1. Replicate (send metadata exchange and get messages) with remote node in remote DC (cross-colo replication).

       Expectations:
        a) We should see that metadata exchange is sent for all replicas while GET messages are only sent for leader replicas.
        b) All the 4 PUT messages should be replicated locally and remote token should be moved forward for leader partitions.
        c) For non-leader replicas, metadata response should be stored locally.

    2. Replicate (send metadata exchange and get messages) with remote node in local DC (intra-colo replication).

       Expectations:
        a) Metadata exchange and GET messages are sent for all replicas.
        b) All the 3 PUT messages should be replicated locally for all replicas.
        c) We should still have 1 missing message in stored metadata response of non-leader replicas for remoteNodeInRemoteDC
           and remote token should not be advanced since all messages are not received yet.

    3. Replicate with remote node in remote DC (cross-colo replication) after time replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds

       Expectations:
        a) We should see cross-colo GET requests sent for standby replicas since their missing messages haven't arrived for
        replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds
        b) All the missing messages for standby replicas should have arrived and remote token should be advanced for all replicas.
     */

    // set mock local stores on all remoteReplicaInfos which will used during replication.
    for (PartitionId partitionId : replicationManager.partitionToPartitionInfo.keySet()) {
      localHost.addStore(partitionId, null);
      Store localStore = localHost.getStore(partitionId);
      localStore.start();
      List<RemoteReplicaInfo> remoteReplicaInfos =
          replicationManager.partitionToPartitionInfo.get(partitionId).getRemoteReplicaInfos();
      remoteReplicaInfos.forEach(remoteReplicaInfo -> remoteReplicaInfo.setLocalStore(localStore));
    }

    // get remote replicas and replica thread for remote host on local datacenter
    ReplicaThread intraColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInLocalDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForLocalDC =
        intraColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInLocalDC);

    // get remote replicas and replica thread for remote host on remote datacenter
    ReplicaThread crossColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInRemoteDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForRemoteDC =
        crossColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInRemoteDC);

    // mock helix transition state from standby to leader for local leader partitions
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      if (mockReplicaId.getReplicaState() == ReplicaState.LEADER) {
        MockPartitionId mockPartitionId = (MockPartitionId) replicaId.getPartitionId();
        mockHelixParticipant.onPartitionBecomeLeaderFromStandby(mockPartitionId.toPathString());
      }
    }

    //Add put messages to all partitions on remoteHost1 and remoteHost2
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      // add 3 put messages to the remoteNodeInLocalDC and remoteNodeInRemoteDC from which local host will replicate.
      addPutMessagesToReplicasOfPartition(partitionId, Arrays.asList(remoteHostInLocalDC, remoteHostInRemoteDC),
          numOfMessagesOnRemoteNodeInLocalDC);

      // add 1 put message to the remoteNodeInRemoteDC only. Since this message is not present in remoteNodeInLocalDC, it
      // doesn't come to local node via intra-dc replication. We should see time out for remote standby replicas waiting for this
      // message and see a cross colo fetch happening.
      addPutMessagesToReplicasOfPartition(partitionId, Collections.singletonList(remoteHostInRemoteDC),
          numOfMessagesOnRemoteNodeInRemoteDC - numOfMessagesOnRemoteNodeInLocalDC);
    }

    // Choose partitions that are leaders on both local and remote nodes
    Set<ReplicaId> leaderReplicasOnLocalAndRemoteNodes =
        getRemoteLeaderReplicasWithLeaderPartitionsOnLocalNode(clusterMap, replicationManager.dataNodeId,
            remoteNodeInRemoteDC);

    // replicate with remote node in remote DC
    crossColoReplicaThread.replicate();

    // verify that the remote token will be moved for leader replicas and will remain 0 for standby replicas as
    // missing messages are not fetched yet.
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      if (leaderReplicasOnLocalAndRemoteNodes.contains(remoteReplicaInfo.getReplicaId())) {
        assertEquals("remote token mismatch for leader replicas",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), batchSize - 1);
      } else {
        assertEquals("remote token should not move forward for standby replicas until missing keys are fetched",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), 0);
      }
    }

    //Replicate with remote node in local dc
    intraColoReplicaThread.replicate();

    // verify that remote token will be moved for all replicas as it is intra-dc replication
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForLocalDC) {
      assertEquals("mismatch in remote token set for intra colo replicas",
          ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), numOfMessagesOnRemoteNodeInLocalDC - 1);
    }

    // process missing messages if any from previous metadata exchange for cross colo replicas as they must now be obtained
    // via intra-dc replication
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      crossColoReplicaThread.processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
    }

    // verify that the remote token will remain 0 for standby replicas as one message in its missing set is not fetched yet.
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      if (!leaderReplicasOnLocalAndRemoteNodes.contains(remoteReplicaInfo.getReplicaId())) {
        assertTrue("missing store messages should still exist for standby replicas",
            crossColoReplicaThread.containsMissingKeysFromPreviousMetadataExchange(remoteReplicaInfo));
        assertEquals("remote token should not move forward for standby replicas until missing keys are fetched",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), 0);
        assertEquals("incorrect number of missing store messages found for standby replicas",
            remoteReplicaInfo.getExchangeMetadataResponse().missingStoreMessages.size(),
            batchSize - numOfMessagesOnRemoteNodeInLocalDC);
      }
    }

    // Attempt replication with remoteNodeInRemoteDC, we should not see any replication attempt for standby replicas
    // and their remote token stays as 0.
    crossColoReplicaThread.replicate();
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      if (!leaderReplicasOnLocalAndRemoteNodes.contains(remoteReplicaInfo.getReplicaId())) {
        assertEquals("remote token should not move forward for standby replicas until missing keys are fetched",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), 0);
        assertTrue("missing store messages should still exist for standby replicas",
            crossColoReplicaThread.containsMissingKeysFromPreviousMetadataExchange(remoteReplicaInfo));
      }
    }

    // Move time forward by replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds+1 seconds and attempt replication.
    // We should see cross colo fetch for standby replicas now since missing keys haven't arrived for
    // replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds.
    time.sleep((replicationConfig.replicationStandbyWaitTimeoutToTriggerCrossColoFetchSeconds + 1) * 1000);

    // verify that we get the list of standby replicas that timed out on no progress
    Set<RemoteReplicaInfo> allStandbyReplicas = remoteReplicaInfosForRemoteDC.stream()
        .filter(info -> !leaderReplicasOnLocalAndRemoteNodes.contains(info.getReplicaId()))
        .collect(Collectors.toSet());
    assertEquals("mismatch in list of standby replicas timed out on no progress", new HashSet<>(
            crossColoReplicaThread.getRemoteStandbyReplicasTimedOutOnNoProgress(remoteReplicaInfosForRemoteDC)),
        allStandbyReplicas);

    crossColoReplicaThread.replicate();

    // token index for all standby replicas will move forward after fetching missing keys themselves
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      if (!leaderReplicasOnLocalAndRemoteNodes.contains(remoteReplicaInfo.getReplicaId())) {
        assertEquals("mismatch in remote token set for standby cross colo replicas",
            ((MockFindToken) remoteReplicaInfo.getToken()).getIndex(), batchSize - 1);
        assertFalse("missing store messages should be empty for standby replicas now",
            crossColoReplicaThread.containsMissingKeysFromPreviousMetadataExchange(remoteReplicaInfo));
      }
    }

    // verify replication metrics to track number of cross colo get requests for standby replicas. If all replicas are
    // leaders, we should have 0 cross colo get requests.
    String remoteDataCenter = remoteReplicaInfosForRemoteDC.get(0).getReplicaId().getDataNodeId().getDatacenterName();
    assertEquals("mismatch in number of cross colo get requests tracked for standby replicas",
        crossColoReplicaThread.getReplicationMetrics().interColoReplicationGetRequestCountForStandbyReplicas.get(
            remoteDataCenter).getCount(),
        leaderReplicasOnLocalAndRemoteNodes.size() != remoteReplicaInfosForRemoteDC.size() ? 1 : 0);

    storageManager.shutdown();
  }

  /**
   * Test leader based replication to ensure token is advanced correctly for standby replicas when missing messages
   * are fetched via intra-dc replication in multiple cycles.
   * @throws Exception
   */
  @Test
  public void replicaThreadLeaderBasedReplicationFetchMissingKeysInMultipleCyclesTest() throws Exception {

    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();

    /*
    Scenario:
      we have 3 nodes that have replicas belonging to same partitions:
        a) local node
        b) remote node in local DC
        c) remote node in remote DC

      Each node have few of its partitions as leaders and others are standby. They are randomly assigned during creation
      of replicas for mock partitions.

      we have 6 records (5 put messages + 1 ttl update message) to be fetched in each partition from remoteNodeInLocalDC and
      remoteNodeInRemoteDC.

      We will first fetch all 6 records in metadata exchange from remoteNodeInRemoteDC and store them for non-leader
      replicas and check if they are tracked correctly as they are fetched from remoteNodeInLocalDC in multiple cycles.

      Records to be fetched:
      1. put b0
      2. put b1
      3. put b2
      4. put b3
      5. put b4
      6. TTLUpdate b0

      Sequence of steps:
        1. Send metadata request to remoteNodeInRemoteDC with batch size of 6.
        2. We should get all messages in the metadata response b0, b1, b2, b3, b4 with b0 having ttlupdate flag set as well.
        3. Send get request for leader partitions. Non-leader partitions will store b0,b1,b2,b3 and b4 in
           remoteReplicaInfo.exchangeMetadataResponse.missingMessages
        4. Send metadata request to remoteNodeInLocalDC with batch size of 4.
        5. We should get only b0,b1,b2,b3.
        6. Send get request for all partitions since it is intra-dc replication.
        7. After the messages are written b0,b1,b2,b3 to local store, we should see them removed from
           remoteReplicaInfo.exchangeMetadataResponse.missingMessages for replica in remoteNodeInRemoteDC.
        8. Fetch remaining messages b4 and TTLUpdate of b0 from remoteNodeInLocalDC.
        9. remoteReplicaInfo.exchangeMetadataResponse.missingMessages for replica in remoteNodeInRemoteDC should be completely empty.

    */

    // set mock local stores on all remoteReplicaInfos which will used during replication.
    for (PartitionId partitionId : replicationManager.partitionToPartitionInfo.keySet()) {
      localHost.addStore(partitionId, null);
      Store localStore = localHost.getStore(partitionId);
      localStore.start();
      List<RemoteReplicaInfo> remoteReplicaInfos =
          replicationManager.partitionToPartitionInfo.get(partitionId).getRemoteReplicaInfos();
      remoteReplicaInfos.forEach(remoteReplicaInfo -> remoteReplicaInfo.setLocalStore(localStore));
    }

    // get remote replicas and replica thread for remote host on local datacenter
    ReplicaThread intraColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInLocalDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForLocalDC =
        intraColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInLocalDC);

    // get remote replicas and replica thread for remote host on remote datacenter
    ReplicaThread crossColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInRemoteDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForRemoteDC =
        crossColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInRemoteDC);

    // mock helix transition state from standby to leader for local leader partitions
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      if (mockReplicaId.getReplicaState() == ReplicaState.LEADER) {
        MockPartitionId mockPartitionId = (MockPartitionId) replicaId.getPartitionId();
        mockHelixParticipant.onPartitionBecomeLeaderFromStandby(mockPartitionId.toPathString());
      }
    }

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition = new HashMap<>();
    Map<PartitionId, List<StoreKey>> idsToBeTtlUpdatedByPartition = new HashMap<>();

    int numOfMessages = 5;
    for (PartitionId id : partitionIds) {
      List<StoreKey> toBeTtlUpdated = new ArrayList<>();
      // Adding 5 put messages to remote hosts
      List<StoreKey> ids =
          addPutMessagesToReplicasOfPartition(id, Arrays.asList(remoteHostInLocalDC, remoteHostInRemoteDC),
              numOfMessages);
      //update the TTL of first put message
      StoreKey toTtlUpdateId = ids.get(0);
      addTtlUpdateMessagesToReplicasOfPartition(id, toTtlUpdateId,
          Arrays.asList(remoteHostInLocalDC, remoteHostInRemoteDC), UPDATED_EXPIRY_TIME_MS);
      toBeTtlUpdated.add(toTtlUpdateId);
      idsToBeTtlUpdatedByPartition.put(id, toBeTtlUpdated);
    }

    // Choose partitions that are leaders on both local and remote nodes
    Set<ReplicaId> remoteLeaderReplicasWithLeaderPartitionsOnLocalNode =
        getRemoteLeaderReplicasWithLeaderPartitionsOnLocalNode(clusterMap, replicationManager.dataNodeId,
            remoteNodeInRemoteDC);

    // fetch all messages in the metadata exchange from remote replica on remote datacenter by setting batchSize
    // in the connection to 6
    // There are 6 records in the remote host. 5 put records + 1 ttl update record. We will receive 5 messages in the
    // metadata exchange as ttl update will be merged with its put record and sent as single record.
    List<ReplicaThread.ExchangeMetadataResponse> responseForRemoteNodeInRemoteDC =
        crossColoReplicaThread.exchangeMetadata(
            new MockConnectionPool.MockConnection(remoteHostInRemoteDC, numOfMessages), remoteReplicaInfosForRemoteDC);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfosForRemoteDC.size(),
        responseForRemoteNodeInRemoteDC.size());
    for (ReplicaThread.ExchangeMetadataResponse exchangeMetadataResponse : responseForRemoteNodeInRemoteDC) {
      // we should have received 5 messages bo, b1, b2, b3, b4 (ttl_update for b0 will be merged with b0 put message)
      assertEquals("mismatch in number of messages received from remote replica", numOfMessages,
          exchangeMetadataResponse.missingStoreMessages.size());
    }

    // Filter leader replicas to fetch missing keys
    List<RemoteReplicaInfo> leaderReplicas = new ArrayList<>();
    List<ReplicaThread.ExchangeMetadataResponse> exchangeMetadataResponseListForLeaderReplicas = new ArrayList<>();
    crossColoReplicaThread.getLeaderReplicaList(remoteReplicaInfosForRemoteDC, responseForRemoteNodeInRemoteDC,
        leaderReplicas, exchangeMetadataResponseListForLeaderReplicas);

    // verify that only leader replicas in remoteHost2 are chosen for fetching missing messages.
    Set<ReplicaId> remoteReplicasToFetchInReplicaThread =
        leaderReplicas.stream().map(RemoteReplicaInfo::getReplicaId).collect(Collectors.toSet());
    assertThat("mismatch in leader remote replicas to fetch missing keys",
        remoteLeaderReplicasWithLeaderPartitionsOnLocalNode, is(remoteReplicasToFetchInReplicaThread));

    // fetch missing keys for leader replicas from remoteHost2
    if (leaderReplicas.size() > 0) {
      crossColoReplicaThread.fixMissingStoreKeys(
          new MockConnectionPool.MockConnection(remoteHostInRemoteDC, numOfMessages), leaderReplicas,
          exchangeMetadataResponseListForLeaderReplicas, false);
    }

    // verify that the remote token will be moved for leader replicas and will remain 0 for standby replicas as
    // missing messages are not fetched yet.
    for (int i = 0; i < remoteReplicaInfosForRemoteDC.size(); i++) {
      if (remoteLeaderReplicasWithLeaderPartitionsOnLocalNode.contains(
          remoteReplicaInfosForRemoteDC.get(i).getReplicaId())) {
        assertThat("remote Token should be updated for leader replica", remoteReplicaInfosForRemoteDC.get(i).getToken(),
            is(responseForRemoteNodeInRemoteDC.get(i).remoteToken));
      } else {
        assertThat("remote Token should not be updated for standby replica",
            remoteReplicaInfosForRemoteDC.get(i).getToken(), not(responseForRemoteNodeInRemoteDC.get(i).remoteToken));
      }
    }

    int numOfMessagesToBeFetchedFromRemoteHost1 = numOfMessages - 1;

    // Fetch only first 4 messages from remote host in local datacenter
    List<ReplicaThread.ExchangeMetadataResponse> responseForRemoteNodeInLocalDC =
        intraColoReplicaThread.exchangeMetadata(
            new MockConnectionPool.MockConnection(remoteHostInLocalDC, numOfMessagesToBeFetchedFromRemoteHost1),
            remoteReplicaInfosForLocalDC);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfosForLocalDC.size(),
        responseForRemoteNodeInLocalDC.size());

    // fetch missing keys from remoteHost1
    intraColoReplicaThread.fixMissingStoreKeys(
        new MockConnectionPool.MockConnection(remoteHostInLocalDC, numOfMessagesToBeFetchedFromRemoteHost1),
        remoteReplicaInfosForLocalDC, responseForRemoteNodeInLocalDC, false);

    // verify that remote token has moved forward for all partitions since it is intra-dc replication.
    for (int i = 0; i < responseForRemoteNodeInLocalDC.size(); i++) {
      assertEquals("token mismatch for intra-dc replication", remoteReplicaInfosForLocalDC.get(i).getToken(),
          responseForRemoteNodeInLocalDC.get(i).remoteToken);
    }

    // process metadata response for cross-colo replicas. Missing keys b0, b1, b2 and b3 must have come now via intra-dc replication
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      crossColoReplicaThread.processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
    }

    // verify that the missingStoreMessages size in standby replicas is 1 as b0, b1, b2 and b3 are fetched and b4 is pending
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      if (!remoteLeaderReplicasWithLeaderPartitionsOnLocalNode.contains(remoteReplicaInfo.getReplicaId())) {
        assertEquals("mismatch in number of missing messages for remote standby replicas after intra-dc replication",
            remoteReplicaInfo.getExchangeMetadataResponse().missingStoreMessages.size(), 1);
      }
    }

    //Fetch remaining one message (b4) from remote host in local datacenter
    responseForRemoteNodeInLocalDC = intraColoReplicaThread.exchangeMetadata(
        new MockConnectionPool.MockConnection(remoteHostInLocalDC, numOfMessagesToBeFetchedFromRemoteHost1),
        remoteReplicaInfosForLocalDC);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfosForLocalDC.size(),
        responseForRemoteNodeInLocalDC.size());

    // fetch missing keys from remoteHost1
    intraColoReplicaThread.fixMissingStoreKeys(
        new MockConnectionPool.MockConnection(remoteHostInLocalDC, numOfMessagesToBeFetchedFromRemoteHost1),
        remoteReplicaInfosForLocalDC, responseForRemoteNodeInLocalDC, false);

    // verify remote token for intra-dc replicas
    for (int i = 0; i < responseForRemoteNodeInLocalDC.size(); i++) {
      assertThat("mismatch in remote token after all missing messages are received",
          remoteReplicaInfosForLocalDC.get(i).getToken(), is(responseForRemoteNodeInLocalDC.get(i).remoteToken));
    }

    // process metadata response for cross-colo replicas. Missing keys b0, b1, b2 and b3 must have come now via intra-dc replication
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      crossColoReplicaThread.processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
    }

    // Since all the missing messages (b0,b1,b2,b3,b4) for all partitions are fetched now via intra-dc replication,
    // remote token for all replicas (leader and standby) should move forward.
    for (int i = 0; i < responseForRemoteNodeInRemoteDC.size(); i++) {
      assertThat("mismatch in remote token after all missing messages are received",
          remoteReplicaInfosForRemoteDC.get(i).getToken(), is(responseForRemoteNodeInRemoteDC.get(i).remoteToken));
    }

    // compare the final state of all messages and buffers are in sync at local node and remoteNodeInLocalDC
    checkBlobMessagesAreEqualInLocalAndRemoteHosts(localHost, remoteHostInLocalDC, idsToBeIgnoredByPartition,
        idsToBeTtlUpdatedByPartition);
    // compare the final state of all messages and buffers are in sync at local node and remoteNodeInRemoteDC
    checkBlobMessagesAreEqualInLocalAndRemoteHosts(localHost, remoteHostInRemoteDC, idsToBeIgnoredByPartition,
        idsToBeTtlUpdatedByPartition);

    storageManager.shutdown();
  }

  /**
   * Test leader based replication to ensure token is advanced correctly and blob properties ttl
   * _update, delete, undelete are applied correctly when missing messages in standby replicas
   * are fetched via intra-dc replication.
   * @throws Exception
   */
  @Test
  public void replicaThreadLeaderBasedReplicationForTTLUpdatesDeleteAndUndeleteMessagesTest() throws Exception {

    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant);
    StorageManager storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();

    int batchSize = 10;

    // set mock local stores on all remoteReplicaInfos which will used during replication.
    for (PartitionId partitionId : replicationManager.partitionToPartitionInfo.keySet()) {
      localHost.addStore(partitionId, null);
      Store localStore = localHost.getStore(partitionId);
      localStore.start();
      List<RemoteReplicaInfo> remoteReplicaInfos =
          replicationManager.partitionToPartitionInfo.get(partitionId).getRemoteReplicaInfos();
      remoteReplicaInfos.forEach(remoteReplicaInfo -> remoteReplicaInfo.setLocalStore(localStore));
    }

    // get remote replicas and replica thread for remote host on local datacenter
    ReplicaThread intraColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInLocalDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForLocalDC =
        intraColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInLocalDC);

    // get remote replicas and replica thread for remote host on remote datacenter
    ReplicaThread crossColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInRemoteDC);
    List<RemoteReplicaInfo> remoteReplicaInfosForRemoteDC =
        crossColoReplicaThread.getRemoteReplicaInfos().get(remoteNodeInRemoteDC);

    // mock helix transition state from standby to leader for local leader partitions
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(replicationManager.dataNodeId);
    for (ReplicaId replicaId : replicaIds) {
      MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
      if (mockReplicaId.getReplicaState() == ReplicaState.LEADER) {
        MockPartitionId mockPartitionId = (MockPartitionId) replicaId.getPartitionId();
        mockHelixParticipant.onPartitionBecomeLeaderFromStandby(mockPartitionId.toPathString());
      }
    }

    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    Map<PartitionId, List<StoreKey>> idsToBeIgnoredByPartition = new HashMap<>();
    Map<PartitionId, List<StoreKey>> idsToBeTtlUpdatedByPartition = new HashMap<>();

    /*
    Scenario:
    we have 3 nodes that have replicas belonging to same partitions:
      a) local node
      b) remote node in local DC
      c) remote node in remote DC

    We will have following blob entries in remoteNodeInLocalDC and remoteNodeInRemoteDC:
     1. b0 (PUT)
     2. b1 (PUT) + b1 (TTL_UPDATE)
     3. b2 (PUT) + b2 (TTL_UPDATE) + b2 (DELETE)
     4. b3 (PUT) + b3 (TTL_UPDATE) + b3 (DELETE) + b3 (UN_DELETE)

     Messages order in remoteNodeInLocalDC:
     b0, b1(PUT), b2(PUT), b3(PUT), b1(TTL_UPDATE), b2(TTL_UPDATE), b3(TTL_UPDATE), b2(DELETE),  b3(DELETE), b3(UN_DELETE).

     Messages order in remoteNodeInRemoteDC:
     b0, b1(PUT), b2(PUT), b3(PUT), b3(TTL_UPDATE), b2(TTL_UPDATE), b1(TTL_UPDATE), b3(DELETE), b2(DELETE), b3(UN_DELETE).

     Set batch size to be fetched during each replication cycle = 10 so that all messages will be fetched in one replication cycle.

     Steps:
     1. Exchange metadata with remoteNodeInRemoteDC to fetch all the messages (set the batch_size in connection to large value).
     2. Verify that GET requests will be only be sent for leader replicas while standby replicas will store the metadata exchange information.
     3. Exchange metadata with remoteNodeInLocalDC to fetch all the messages (set the batch_size in connection to large value).
     4. Verify that GET requests will be sent for all replicas as it is intra-dc replication.
     5. After the intra-dc replication, verify that remote token would move forward for all replicas in remoteNodeInRemoteDC.
     6. Verify that message information and buffers are in sync at local host and remote hosts in local and remote data centers.
     */

    for (PartitionId id : partitionIds) {
      List<StoreKey> toBeIgnored = new ArrayList<>();
      List<StoreKey> toBeUndeleted = new ArrayList<>();

      // Adding 4 PUT messages b0, b1, b2, b3 to remoteNodeInLocalDC and remoteNodeInRemoteDC
      List<StoreKey> ids =
          addPutMessagesToReplicasOfPartition(id, Arrays.asList(remoteHostInLocalDC, remoteHostInRemoteDC), 4);

      // ttl update to be added to b1, b2, b3
      List<StoreKey> toBeTtlUpdated = new ArrayList<>(ids);
      toBeTtlUpdated.remove(ids.get(0));

      // delete to be added to b2, b3
      toBeIgnored.add(ids.get(2));
      toBeIgnored.add(ids.get(3));

      // un-delete to be added to b3
      toBeUndeleted.add(ids.get(3));
      toBeIgnored.remove(ids.get(3));

      // Add TTLUpdate records for blobs b1,b2,b3 in remoteNodeInLocalDC and remoteNodeInRemoteDC
      for (int j = 0; j < toBeTtlUpdated.size(); j++) {
        addTtlUpdateMessagesToReplicasOfPartition(id, toBeTtlUpdated.get(j),
            Collections.singletonList(remoteHostInLocalDC), UPDATED_EXPIRY_TIME_MS);
        addTtlUpdateMessagesToReplicasOfPartition(id, toBeTtlUpdated.get(toBeTtlUpdated.size() - 1 - j),
            Collections.singletonList(remoteHostInRemoteDC), UPDATED_EXPIRY_TIME_MS);
      }

      // Add delete records for blobs b2,b3 in remoteNodeInLocalDC and remoteNodeInRemoteDC
      for (int j = 0; j < toBeIgnored.size(); j++) {
        addDeleteMessagesToReplicasOfPartition(id, toBeIgnored.get(j), Collections.singletonList(remoteHostInLocalDC),
            (short) 0, EXPIRY_TIME_MS);
        addDeleteMessagesToReplicasOfPartition(id, toBeIgnored.get(toBeIgnored.size() - 1 - j),
            Collections.singletonList(remoteHostInRemoteDC), (short) 0, EXPIRY_TIME_MS);
      }

      // Add un-delete records for blob b3 with life_version as 1 in remoteNodeInLocalDC and remoteNodeInRemoteDC
      for (StoreKey storeKey : toBeUndeleted) {
        addUndeleteMessagesToReplicasOfPartition(id, storeKey, Arrays.asList(remoteHostInLocalDC, remoteHostInRemoteDC),
            (short) 1);
      }

      // will be used later while comparing the final message records in local and remote nodes
      idsToBeIgnoredByPartition.put(id, toBeIgnored);
      idsToBeTtlUpdatedByPartition.put(id, toBeTtlUpdated);
    }

    // Inter-dc replication
    // Send metadata request to remoteNodeInRemoteDC to fetch missing keys information.
    List<ReplicaThread.ExchangeMetadataResponse> responseForRemoteNodeInRemoteDC =
        crossColoReplicaThread.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHostInRemoteDC, batchSize),
            remoteReplicaInfosForRemoteDC);
    assertEquals("Response should contain a response for each replica", remoteReplicaInfosForRemoteDC.size(),
        responseForRemoteNodeInRemoteDC.size());

    // Filter leader replicas to fetch missing keys
    List<RemoteReplicaInfo> leaderReplicas = new ArrayList<>();
    List<ReplicaThread.ExchangeMetadataResponse> exchangeMetadataResponseListForLeaderReplicas = new ArrayList<>();
    crossColoReplicaThread.getLeaderReplicaList(remoteReplicaInfosForRemoteDC, responseForRemoteNodeInRemoteDC,
        leaderReplicas, exchangeMetadataResponseListForLeaderReplicas);

    // verify that only leader partitions in local and remote nodes are chosen for fetching missing messages.
    Set<ReplicaId> remoteLeaderReplicasWithLeaderPartitionsOnLocalNode =
        getRemoteLeaderReplicasWithLeaderPartitionsOnLocalNode(clusterMap, replicationManager.dataNodeId,
            remoteNodeInRemoteDC);
    Set<ReplicaId> leaderReplicaSetInReplicaThread =
        leaderReplicas.stream().map(RemoteReplicaInfo::getReplicaId).collect(Collectors.toSet());
    assertThat("mismatch in leader remote replicas to fetch missing keys",
        remoteLeaderReplicasWithLeaderPartitionsOnLocalNode, is(leaderReplicaSetInReplicaThread));

    // fetch missing keys for leader replicas from remoteNodeInRemoteDC
    if (leaderReplicas.size() > 0) {
      crossColoReplicaThread.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHostInRemoteDC, batchSize),
          leaderReplicas, exchangeMetadataResponseListForLeaderReplicas, false);
    }

    // Verify that the remote token will move forward only for leader replicas
    // For standby replicas, token index will remain 0 and metadata information would be stored.
    for (int i = 0; i < remoteReplicaInfosForRemoteDC.size(); i++) {
      if (remoteLeaderReplicasWithLeaderPartitionsOnLocalNode.contains(
          remoteReplicaInfosForRemoteDC.get(i).getReplicaId())) {
        assertEquals("remote Token should be updated for leader replica",
            remoteReplicaInfosForRemoteDC.get(i).getToken(), (responseForRemoteNodeInRemoteDC.get(i).remoteToken));
      } else {
        assertThat("remote Token should not be updated for standby replica",
            remoteReplicaInfosForRemoteDC.get(i).getToken(), not(responseForRemoteNodeInRemoteDC.get(i).remoteToken));
        assertEquals("missing messages in metadata exchange should be stored for standby replica",
            remoteReplicaInfosForRemoteDC.get(i).getExchangeMetadataResponse().missingStoreMessages.size(),
            responseForRemoteNodeInRemoteDC.get(i).missingStoreMessages.size());
      }
    }

    // Intra-dc replication
    List<ReplicaThread.ExchangeMetadataResponse> responseForRemoteNodeInLocalDC =
        intraColoReplicaThread.exchangeMetadata(new MockConnectionPool.MockConnection(remoteHostInLocalDC, batchSize),
            remoteReplicaInfosForLocalDC);
    intraColoReplicaThread.fixMissingStoreKeys(new MockConnectionPool.MockConnection(remoteHostInLocalDC, batchSize),
        remoteReplicaInfosForLocalDC, responseForRemoteNodeInLocalDC, false);

    // Verify that the remote token for all intra-colo replicas has been moved
    for (int i = 0; i < responseForRemoteNodeInLocalDC.size(); i++) {
      assertEquals(remoteReplicaInfosForLocalDC.get(i).getToken(), responseForRemoteNodeInLocalDC.get(i).remoteToken);
    }

    // process missing keys in previous metadata exchange for cross-colo replicas (standby) as they must have
    // arrived via intra-dc replication
    for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfosForRemoteDC) {
      crossColoReplicaThread.processMissingKeysFromPreviousMetadataResponse(remoteReplicaInfo);
    }

    // Verify that the remote token for all inter-colo replicas (standby) has been moved as they must have
    // arrived via intra-dc replication
    for (int i = 0; i < responseForRemoteNodeInLocalDC.size(); i++) {
      assertEquals(remoteReplicaInfosForRemoteDC.get(i).getToken(), responseForRemoteNodeInRemoteDC.get(i).remoteToken);
    }

    // compare the messages and buffers are in sync at local host and remote host1
    checkBlobMessagesAreEqualInLocalAndRemoteHosts(localHost, remoteHostInLocalDC, idsToBeIgnoredByPartition,
        idsToBeTtlUpdatedByPartition);
    // compare the messages and buffers are in sync at local host and remote host1
    checkBlobMessagesAreEqualInLocalAndRemoteHosts(localHost, remoteHostInRemoteDC, idsToBeIgnoredByPartition,
        idsToBeTtlUpdatedByPartition);

    storageManager.shutdown();
  }
}
