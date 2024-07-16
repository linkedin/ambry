/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockHelixParticipant;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.replication.ReplicaThread.RemoteReplicaGroup;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

//TODO rename group generation
@RunWith(Parameterized.class)
public class ReplicaThreadGroupGenerationTest extends ReplicationTestHelper {
  ReplicaThread intraColoReplicaThread;

  ReplicaThread crossColoReplicaThread;

  StorageManager storageManager;

  public ReplicaThreadGroupGenerationTest(short requestVersion, short responseVersion) throws Exception {
    super(requestVersion, responseVersion, true);
    setUp();
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    //@formatter:off
    return Arrays.asList(new Object[][]{
        {ReplicaMetadataRequest.Replica_Metadata_Request_Version_V1, ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5},
    });
    //@formatter:on
  }

  @Before
  public void before() throws Exception {
    setUp();
  }

  @After
  public void after() throws Exception {
    storageManager.shutdown();
  }

  public void setUp() throws Exception {
    properties.setProperty("replication.model.across.datacenters", "LEADER_BASED");
    properties.setProperty("replication.max.partition.count.per.request", Integer.toString(3));

    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));
    // setting number of mount points to partition limit so each node can have replicas higher that limit to test group partitioning
    MockClusterMap clusterMap =
        new MockClusterMap(false, true, 5, replicationConfig.replicationMaxPartitionCountPerRequest, 3, false, false,
            null);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    MockHelixParticipant mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);

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
    DataNodeId remoteNodeInLocalDC = remoteNodes.get(0);
    DataNodeId remoteNodeInRemoteDC = remoteNodes.get(1);

    // mock hosts for remote nodes
    MockHost localHost = new MockHost(localNode, clusterMap);
    MockHost remoteHostInLocalDC = new MockHost(remoteNodeInLocalDC, clusterMap);
    MockHost remoteHostInRemoteDC = new MockHost(remoteNodeInRemoteDC, clusterMap);

    int batchSize = 4;

    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    hosts.put(remoteNodeInLocalDC, remoteHostInLocalDC);
    hosts.put(remoteNodeInRemoteDC, remoteHostInRemoteDC);
    MockNetworkClientFactory mockNetworkClientFactory = new MockNetworkClientFactory(hosts, clusterMap, batchSize,
        new MockFindTokenHelper(new BlobIdFactory(clusterMap), replicationConfig));
    Pair<StorageManager, ReplicationManager> managers =
        createStorageManagerAndReplicationManager(clusterMap, clusterMapConfig, mockHelixParticipant,
            mockNetworkClientFactory);
    storageManager = managers.getFirst();
    MockReplicationManager replicationManager = (MockReplicationManager) managers.getSecond();

    for (PartitionId partitionId : replicationManager.partitionToPartitionInfo.keySet()) {
      localHost.addStore(partitionId, null);
      Store localStore = localHost.getStore(partitionId);
      localStore.start();
      List<RemoteReplicaInfo> remoteReplicaInfos =
          replicationManager.partitionToPartitionInfo.get(partitionId).getRemoteReplicaInfos();
      remoteReplicaInfos.forEach(remoteReplicaInfo -> remoteReplicaInfo.setLocalStore(localStore));
    }

    intraColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInLocalDC);
    crossColoReplicaThread = replicationManager.dataNodeIdToReplicaThread.get(remoteNodeInRemoteDC);
  }

  /**
   * Testing to check if group are getting generated correctly
   * Tested on intraColoReplicaThread Only as logic is same for intra-colo and cross-colo
   */
  @Test
  public void testGroupIdGeneration() throws Exception {
    ReplicaThread replicaThread = intraColoReplicaThread;

    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = replicaThread.getRemoteReplicaInfos();

    Map<Integer, List<RemoteReplicaInfo>> groupIdToRemoteReplicaMap = new HashMap<>();
    Map<DataNodeId, Integer> remoteHostToStandbyNoProgressReplicaGroupId = new HashMap<>();

    //check if stand by group ids are generated for all data nodes
    replicaThread.generateGroupIdsForReplicas(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId);
    assertEquals("Standby groups not generated for all data nodes", remoteHostToStandbyNoProgressReplicaGroupId.size(),
        dataNodeToReplicaMap.size());

    //check if  all group ids are unique across standby group ids and replication group ids
    List<Integer> groupIds = new ArrayList<>();
    groupIdToRemoteReplicaMap.forEach((key, value) -> groupIds.add(key));
    remoteHostToStandbyNoProgressReplicaGroupId.forEach((key, value) -> groupIds.add(value));
    assertEquals("All group ids are not unique as size of unique group ids and group ids is different",
        new HashSet<>(groupIds).size(), groupIds.size());

    //check for every group,if all assigned replicas are pointing to same data node
    Set<RemoteReplicaInfo> remoteReplicasInGroup = new HashSet<>();
    groupIdToRemoteReplicaMap.forEach((groupId, remoteReplicas) -> {
      Set<DataNodeId> dataNodesUsed = new HashSet<>();
      remoteReplicas.forEach(remoteReplica -> {
        dataNodesUsed.add(remoteReplica.getReplicaId().getDataNodeId());
      });
      assertEquals("a group needs to point to same data node", 1, dataNodesUsed.size());

      remoteReplicasInGroup.addAll(remoteReplicas);
    });

    //check if all remote replicas are assigned to a group id
    Set<RemoteReplicaInfo> allRemoteReplicas = new HashSet<>();
    dataNodeToReplicaMap.forEach((dataNode, remoteReplicas) -> {
      allRemoteReplicas.addAll(remoteReplicas);
    });
    assertEquals("all remote replicas should be added in a group", remoteReplicasInGroup.size(),
        allRemoteReplicas.size());
  }

  /**
   * Tests for group generation until limit is reached, tests fot throttling replicas
   * Tested on intraColoReplicaThread Only as logic is same for intra-colo and cross-colo
   * @throws Exception
   */
  @Test
  public void testRemoteReplicaGroupGenerationIntraColo() throws Exception {

    ReplicaThread replicaThread = intraColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = intraColoReplicaThread.getRemoteReplicaInfos();

    Map<Integer, List<RemoteReplicaInfo>> groupIdToRemoteReplicaMap = new HashMap<>();
    Map<DataNodeId, Integer> remoteHostToStandbyNoProgressReplicaGroupId = new HashMap<>();

    replicaThread.generateGroupIdsForReplicas(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId);

    Map<Integer, RemoteReplicaGroup> inflightRemoteReplicaGroups = new HashMap<>();
    Map<Integer, Integer> groupIdIterationCountMap = new HashMap<>();
    Map<DataNodeId, Set<RemoteReplicaInfo>> dataNodeIdToPendingReplicasMarkedForStandByNoProgress = new HashMap<>();
    Map<RemoteReplicaInfo, Long> remoteReplicaToThrottledTill = new HashMap<>();
    MutableBoolean maxIterationsReached = new MutableBoolean(false);
    MutableBoolean allReplicasCaughtUpEarly = new MutableBoolean(false);

    replicaThread.shouldTerminateCurrentCycle(false);
    //set max iteration limit to 3
    replicaThread.setMaxIterationsPerGroupPerCycle(3);

    //check after 1st call whether all inflight remote replica have first iteration running
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    inflightRemoteReplicaGroups.forEach((groupId, inflightRemoteReplicaGroup) -> {
      assertEquals("Groups first iteration is going on", groupIdIterationCountMap.get(groupId), (Integer) 1);
    });

    //make one replica group as done
    RemoteReplicaGroup remoteReplicaGroupForDone = new ArrayList<>(inflightRemoteReplicaGroups.values()).get(0);
    remoteReplicaGroupForDone.setState(ReplicaThread.ReplicaGroupReplicationState.DONE);

    long startTime = time.milliseconds();

    //check if all replicas of replica group in done state are throttled for and group is removed from inflight groups
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    assertNotEquals("group with done state should be removed from inflight groups", remoteReplicaGroupForDone,
        inflightRemoteReplicaGroups.get(remoteReplicaGroupForDone.getId()));

    assertTrue("Replica should be throttled till at least value in config",
        startTime + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs
            <= remoteReplicaToThrottledTill.get(remoteReplicaGroupForDone.getRemoteReplicaInfos().get(0)));

    //move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    //check if previously throttled replicas are added to same group and iteration count is increased
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    assertTrue("New group should be created for the group that is done",
        inflightRemoteReplicaGroups.containsKey(remoteReplicaGroupForDone.getId()));
    assertEquals("New group should have incremented iteration count", (Integer) 2,
        groupIdIterationCountMap.get(remoteReplicaGroupForDone.getId()));

    // We want to test when iteration limit is reached for any group, new group's iteration is not created

    //since maxIteration limit is 3, we will make group id with  remoteReplicaGroupForDone group id we stored earlier done 1 more times
    Integer groupIdDone = remoteReplicaGroupForDone.getId();
    inflightRemoteReplicaGroups.get(groupIdDone).setState(ReplicaThread.ReplicaGroupReplicationState.DONE);

    // now the marked group will be throttled and new group will not be created
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    //move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    // now group with id marked should reach iteration limit
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    //make all other groups  as done
    inflightRemoteReplicaGroups.entrySet().stream().filter(entry -> {
      return !entry.getKey().equals(groupIdDone);
    }).forEach((entry) -> {
      entry.getValue().setState(ReplicaThread.ReplicaGroupReplicationState.DONE);
    });

    // now all done groups will be throttled
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    //move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    //now in this call no new groups should be generated
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    assertEquals("only 1 group should be inflight", inflightRemoteReplicaGroups.size(), 1);
    assertTrue("marked group id should be inflight", inflightRemoteReplicaGroups.containsKey(groupIdDone));

    //now we mark the remaining group as done too
    inflightRemoteReplicaGroups.get(groupIdDone).setState(ReplicaThread.ReplicaGroupReplicationState.DONE);

    //now after this call no inflight groups should be remaining and
    // maxIterationsReached should be true allReplicasCaughtUpEarly should be false
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    assertEquals("No inflight groups should be remaining", 0 , inflightRemoteReplicaGroups.size());
    assertTrue("max iterations are reached", maxIterationsReached.booleanValue());
    assertFalse("all replicas did not catch up early", allReplicasCaughtUpEarly.booleanValue());
  }


  /**
   * Tests for when all replicas have caught up early or are in backoff mode or throttled
   * Tested on intraColoReplicaThread Only as logic is same for intra-colo and cross-colo
   * @throws Exception
   */
  @Test
  public void testRemoteReplicaGroupGenerationEarlyFinishIntraColo() throws Exception{
    ReplicaThread replicaThread = intraColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = intraColoReplicaThread.getRemoteReplicaInfos();

    Map<Integer, List<RemoteReplicaInfo>> groupIdToRemoteReplicaMap = new HashMap<>();
    Map<DataNodeId, Integer> remoteHostToStandbyNoProgressReplicaGroupId = new HashMap<>();

    replicaThread.generateGroupIdsForReplicas(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId);

    Map<Integer, RemoteReplicaGroup> inflightRemoteReplicaGroups = new HashMap<>();
    Map<Integer, Integer> groupIdIterationCountMap = new HashMap<>();
    Map<DataNodeId, Set<RemoteReplicaInfo>> dataNodeIdToPendingReplicasMarkedForStandByNoProgress = new HashMap<>();
    Map<RemoteReplicaInfo, Long> remoteReplicaToThrottledTill = new HashMap<>();
    MutableBoolean maxIterationsReached = new MutableBoolean(false);
    MutableBoolean allReplicasCaughtUpEarly = new MutableBoolean(false);

    replicaThread.shouldTerminateCurrentCycle(false);
    //set max iteration limit to 3
    replicaThread.setMaxIterationsPerGroupPerCycle(3);

    //after 1st call whether all inflight remote replica have first iteration running and there will be inflight groups
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    //set all remote replicas groups to done
    inflightRemoteReplicaGroups.values().forEach(remoteReplicaGroup -> remoteReplicaGroup.setState(
        ReplicaThread.ReplicaGroupReplicationState.DONE));

    //all groups will be throttled now, no new groups will be created, so all replica will have caught up early
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    assertTrue("all replicas should have caught up early" ,allReplicasCaughtUpEarly.booleanValue());
    assertEquals("no inflight groups should be created",0 ,inflightRemoteReplicaGroups.size());

    //move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    //now we make all replicas in backoff mode
    dataNodeToReplicaMap.values().stream().flatMap(Collection::stream).forEach(remoteReplicaInfo -> {
      remoteReplicaInfo.setReEnableReplicationTime(time.milliseconds() +replicationConfig.replicationSyncedReplicaBackoffDurationMs +1);});

    //all groups will be throttled now, no new groups will be created, so all replica will have caught up early
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    assertTrue("all replicas should have caught up early" ,allReplicasCaughtUpEarly.booleanValue());
    assertEquals("no inflight groups should be created",0 ,inflightRemoteReplicaGroups.size());

    System.out.println("done!");
  }

  /**
   * Tests for whether standby replica group logic is working as expected.
   * Tested on crossColo replica thread
   * @throws Exception
   */
  @Test
  public void testRemoteReplicaStandByGroupGenerationCrossColo() throws Exception {
    ReplicaThread replicaThread = crossColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = crossColoReplicaThread.getRemoteReplicaInfos();

    Map<Integer, List<RemoteReplicaInfo>> groupIdToRemoteReplicaMap = new HashMap<>();
    Map<DataNodeId, Integer> remoteHostToStandbyNoProgressReplicaGroupId = new HashMap<>();

    replicaThread.generateGroupIdsForReplicas(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId);

    Map<Integer, RemoteReplicaGroup> inflightRemoteReplicaGroups = new HashMap<>();
    Map<Integer, Integer> groupIdIterationCountMap = new HashMap<>();
    Map<DataNodeId, Set<RemoteReplicaInfo>> dataNodeIdToPendingReplicasMarkedForStandByNoProgress = new HashMap<>();
    Map<RemoteReplicaInfo, Long> remoteReplicaToThrottledTill = new HashMap<>();
    MutableBoolean maxIterationsReached = new MutableBoolean(false);
    MutableBoolean allReplicasCaughtUpEarly = new MutableBoolean(false);

    replicaThread.shouldTerminateCurrentCycle(false);
    //set max iteration limit to 3
    replicaThread.setMaxIterationsPerGroupPerCycle(3);


    List<RemoteReplicaInfo> remoteReplicasForStandBy = new ArrayList<>();

    //add replicas for stand by no progress, take one replica from each datanode
    dataNodeToReplicaMap.entrySet().forEach((dataNodeIdListEntry -> {
      DataNodeId dataNodeId = dataNodeIdListEntry.getKey();
      RemoteReplicaInfo remoteReplica = dataNodeIdListEntry.getValue().get(0);
      remoteReplicasForStandBy.add(remoteReplica);
      dataNodeIdToPendingReplicasMarkedForStandByNoProgress.putIfAbsent(dataNodeId, new HashSet<>());
      dataNodeIdToPendingReplicasMarkedForStandByNoProgress.get(dataNodeId).add(remoteReplica);
    }));

    //after this call, standby replicas will be added to inflight groups
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);


    //verify if all stand by group ids are created and created correctly
    remoteHostToStandbyNoProgressReplicaGroupId.forEach((dataNodeId, standByGroupId) -> {
      assertTrue("standby group id must be in flight" , inflightRemoteReplicaGroups.containsKey(standByGroupId));
      assertTrue("standby group should be marked no progress", inflightRemoteReplicaGroups.get(standByGroupId).isNonProgressStandbyReplicaGroup());
      assertEquals("datanode must be correct", dataNodeId, inflightRemoteReplicaGroups.get(standByGroupId).getRemoteDataNode());
    });

    //mark all non standby groups as done
    inflightRemoteReplicaGroups.forEach((groupId, remoteReplicaGroup) -> {
      if(!remoteReplicaGroup.isNonProgressStandbyReplicaGroup())
        remoteReplicaGroup.setState(ReplicaThread.ReplicaGroupReplicationState.DONE);
    });

    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);

    //move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    //after this call new remote replica groups will be created and added to inflight groups
    replicaThread.generateRemoteReplicaGroups(groupIdToRemoteReplicaMap, remoteHostToStandbyNoProgressReplicaGroupId,
        inflightRemoteReplicaGroups, dataNodeIdToPendingReplicasMarkedForStandByNoProgress,
        remoteReplicaToThrottledTill, groupIdIterationCountMap, allReplicasCaughtUpEarly, maxIterationsReached);


    //verify if standby replicas are not added to a group again, i.e all replicas should be unique
    Set<RemoteReplicaInfo> remoteReplicaInfoSet = new HashSet<>();
    inflightRemoteReplicaGroups.forEach((groupId, remoteReplicaGroup) -> {
      remoteReplicaGroup.getRemoteReplicaInfos().forEach(remoteReplicaInfo -> {
        assertFalse("Remote replicas should not be repeated", remoteReplicaInfoSet.contains(remoteReplicaInfo));
        remoteReplicaInfoSet.add(remoteReplicaInfo);
      });
    });
  }
}
