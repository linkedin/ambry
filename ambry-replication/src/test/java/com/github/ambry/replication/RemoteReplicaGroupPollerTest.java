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
import com.github.ambry.replication.ReplicaThread.RemoteReplicaGroupPoller;
import com.github.ambry.replication.continuous.ActiveGroupTracker;
import com.github.ambry.replication.continuous.DataNodeTracker;
import com.github.ambry.replication.continuous.GroupTracker;
import com.github.ambry.replication.continuous.ReplicaStatus;
import com.github.ambry.replication.continuous.ReplicaTracker;
import com.github.ambry.replication.continuous.StandByGroupTracker;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.utils.Pair;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class RemoteReplicaGroupPollerTest extends ReplicationTestHelper {
  ReplicaThread intraColoReplicaThread;

  ReplicaThread crossColoReplicaThread;

  StorageManager storageManager;

  int maxPartitionCountPerRequest;

  int defaultIterationLimit;

  public RemoteReplicaGroupPollerTest(short requestVersion, short responseVersion) {
    super(requestVersion, responseVersion);
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

  private void setUp() throws Exception {
    properties.setProperty("replication.model.across.datacenters", "LEADER_BASED");
    maxPartitionCountPerRequest = 3;
    properties.setProperty("replication.max.partition.count.per.request", String.valueOf(maxPartitionCountPerRequest));
    defaultIterationLimit = 2;
    properties.setProperty(ReplicationConfig.REPLICATION_CONTINUOUS_GROUP_ITERATION_LIMIT,
        String.valueOf(defaultIterationLimit));

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
   * Testing to check if group trackers, data node trackers and replica trackers are getting generated correctly
   * Tested on intraColoReplicaThread Only as logic is same for intra-colo and cross-colo
   */
  @Test
  public void trackersGenerationTest() {
    ReplicaThread replicaThread = intraColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = replicaThread.getRemoteReplicaInfos();

    RemoteReplicaGroupPoller remoteReplicaGroupPoller = replicaThread.new RemoteReplicaGroupPoller();

    List<DataNodeTracker> dataNodeTrackers = remoteReplicaGroupPoller.getDataNodeTrackers();

    // check if data node trackers are generated correctly

    Assert.assertEquals("Count of data nodes assigned to thread and count of data node trackers should be same",
        dataNodeToReplicaMap.size(), dataNodeTrackers.size());

    Set<DataNodeId> dataNodeIdsAddedInTracker =
        dataNodeTrackers.stream().map(DataNodeTracker::getDataNodeId).collect(Collectors.toSet());

    // all data nodes should have trackers created
    dataNodeToReplicaMap.keySet().forEach(dataNodeId -> {
      Assert.assertTrue("DataNode should be present in data node trackers created",
          dataNodeIdsAddedInTracker.contains(dataNodeId));
    });

    // check if all group trackers are generated correctly inside data node tracker
    dataNodeTrackers.forEach(dataNodeTracker -> {
      TreeSet<Integer> allActiveGroupIds = dataNodeTracker.getActiveGroupTrackers()
          .stream()
          .map(GroupTracker::getGroupId)
          .distinct()
          .collect(Collectors.toCollection(TreeSet::new));
      int minimumActiveGroupId = allActiveGroupIds.first();
      int maximumActiveGroupId = allActiveGroupIds.last();

      Assert.assertEquals("All groups ids should exits from start and should be in serial consecutive order",
          maximumActiveGroupId - minimumActiveGroupId + 1, dataNodeTracker.getActiveGroupTrackers().size());

      Assert.assertEquals("Standby group id should be maximum and one more than maximum active group id",
          maximumActiveGroupId + 1, dataNodeTracker.getStandByGroupTracker().getGroupId());
    });

    // check if group tackers are generated correctly

    // check if all groupIds are unique across datanode tracker
    Set<Integer> allGroupIds = new HashSet<>();

    dataNodeTrackers.forEach(dataNodeTracker -> {
      dataNodeTracker.getActiveGroupTrackers().forEach(activeGroupTracker -> {
        Assert.assertFalse("Group id for group trackers should be unique",
            allGroupIds.contains(activeGroupTracker.getGroupId()));
        allGroupIds.add(activeGroupTracker.getGroupId());
      });

      Assert.assertFalse("Group id for group trackers should be unique",
          allGroupIds.contains(dataNodeTracker.getStandByGroupTracker().getGroupId()));
      allGroupIds.add(dataNodeTracker.getStandByGroupTracker().getGroupId());
    });

    // check if all replicas are added to correct datanode tracker and all replicas are added to active group trackers

    Set<RemoteReplicaInfo> allRemoteReplicasInTrackers = new HashSet<>();
    dataNodeTrackers.forEach(dataNodeTracker -> {
      dataNodeTracker.getActiveGroupTrackers().forEach(activeGroupTracker -> {
        activeGroupTracker.getPreAssignedReplicas().forEach(replicaTracker -> {
          Assert.assertEquals("Replica should be added to the tracker of data node on which it exists",
              dataNodeTracker.getDataNodeId(), replicaTracker.getRemoteReplicaInfo().getReplicaId().getDataNodeId());
          allRemoteReplicasInTrackers.add(replicaTracker.getRemoteReplicaInfo());
        });
      });
    });

    dataNodeToReplicaMap.forEach((dataNodeId, remoteReplicaInfos) -> {
      remoteReplicaInfos.forEach(remoteReplicaInfo -> {
        Assert.assertTrue("Replicas in replica thread should be present in trackers",
            allRemoteReplicasInTrackers.contains(remoteReplicaInfo));
      });
    });

    // check if active groups have maximum replicas as maxPartitionCountPerRequest
    dataNodeTrackers.forEach(dataNodeTracker -> {
      dataNodeTracker.getActiveGroupTrackers().forEach(activeGroupTracker -> {
        Assert.assertTrue(
            "active groups should have max preassigned replicas less than or equal to max partition per request",
            maxPartitionCountPerRequest >= activeGroupTracker.getPreAssignedReplicas().size());
      });
    });
  }

  /**
   * Tests for group generation until limit is reached, tests for throttling replicas
   * Tested on intraColoReplicaThread Only as logic is same for intra-colo and cross-colo
   */
  @Test
  public void testRemoteReplicaGroupGenerationIntraColo() {

    ReplicaThread replicaThread = intraColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = intraColoReplicaThread.getRemoteReplicaInfos();

    replicaThread.setTerminateCurrentContinuousReplicationCycle(false);

    RemoteReplicaGroupPoller poller = replicaThread.new RemoteReplicaGroupPoller();
    List<DataNodeTracker> dataNodeTrackers = poller.getDataNodeTrackers();
    Map<Integer, GroupTracker> allGroupTrackers = new HashMap<>();
    dataNodeTrackers.forEach(dataNodeTracker -> dataNodeTracker.getGroupTrackers()
        .forEach(groupTracker -> allGroupTrackers.put(groupTracker.getGroupId(), groupTracker)));

    // check after 1st call whether all inflight remote replica have first iteration running
    List<RemoteReplicaGroup> inflightGroups = poller.enqueue();
    inflightGroups.forEach(inflightGroup -> {
      Assert.assertEquals("Groups first iteration is going on", 1,
          allGroupTrackers.get(inflightGroup.getId()).getIterations());
    });

    // make one replica group as done
    RemoteReplicaGroup groupForDone = inflightGroups.get(0);
    groupForDone.setState(ReplicaThread.ReplicaGroupReplicationState.DONE);

    // check if all replicas of replica group in done state are throttled for and group is removed from inflight groups
    inflightGroups = poller.enqueue();

    Assert.assertFalse("group with done state should be removed from inflight groups",
        inflightGroups.stream().anyMatch(inflightGroup -> inflightGroup.getId() == groupForDone.getId()));

    // move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    // check if previously throttled replicas are added to same group and iteration count is increased
    inflightGroups = poller.enqueue();

    Assert.assertTrue("New group should be created for the group that is done",
        inflightGroups.stream().anyMatch(remoteReplicaGroup -> remoteReplicaGroup.getId() == groupForDone.getId()));

    Assert.assertEquals("New group should have incremented iteration count", 2,
        allGroupTrackers.get(groupForDone.getId()).getIterations());

    // We want to test when iteration limit is reached for any group, new group's iteration is not created
    // since maxIteration limit is 3, we will make group id with  groupForDone group id we stored earlier in DONE state 1 more time
    int groupIdDone = groupForDone.getId();
    inflightGroups.stream()
        .filter(g -> g.getId() == groupIdDone)
        .collect(Collectors.toList())
        .get(0)
        .setState(ReplicaThread.ReplicaGroupReplicationState.DONE);

    // now the marked group will be throttled and new group will not be created
    inflightGroups = poller.enqueue();

    // move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    // now group with id marked should reach iteration limit
    inflightGroups = poller.enqueue();

    // make all other groups  as done
    inflightGroups.stream().filter(group -> (group.getId() != groupIdDone)).forEach((group) -> {
      group.setState(ReplicaThread.ReplicaGroupReplicationState.DONE);
    });

    // now all done groups will be throttled
    inflightGroups = poller.enqueue();

    // move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    //now in this call no new groups should be generated
    inflightGroups = poller.enqueue();

    Assert.assertEquals("only 1 group should be inflight", inflightGroups.size(), 1);
    Assert.assertTrue("marked group id should be inflight",
        inflightGroups.stream().anyMatch(group -> group.getId() == groupIdDone));

    // now we mark the remaining group as done too
    inflightGroups.stream()
        .filter(group -> group.getId() == groupIdDone)
        .collect(Collectors.toList())
        .get(0)
        .setState(ReplicaThread.ReplicaGroupReplicationState.DONE);

    // now after this call no inflight groups should be remaining and
    // maxIterationsReached should be true allReplicasCaughtUpEarly should be false
    inflightGroups = poller.enqueue();

    Assert.assertEquals("No inflight groups should be remaining", 0, inflightGroups.size());
    Assert.assertFalse("all replicas did not catch up early", poller.allReplicasCaughtUpEarly());
  }

  /**
   * Tests for when all replicas have caught up early or are in backoff mode or throttled
   * Tested on intraColoReplicaThread Only as logic is same for intra-colo and cross-colo
   */
  @Test
  public void testRemoteReplicaGroupGenerationEarlyFinishIntraColo() {
    ReplicaThread replicaThread = intraColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = intraColoReplicaThread.getRemoteReplicaInfos();

    replicaThread.setTerminateCurrentContinuousReplicationCycle(false);

    RemoteReplicaGroupPoller poller = replicaThread.new RemoteReplicaGroupPoller();

    List<RemoteReplicaGroup> inflightGroups = poller.enqueue();

    // set all remote replicas groups to done
    inflightGroups.forEach(group -> group.setState(ReplicaThread.ReplicaGroupReplicationState.DONE));

    // all groups will be throttled now, no new groups will be created, so all replica will have caught up early
    inflightGroups = poller.enqueue();

    Assert.assertTrue("all replicas should have caught up early", poller.allReplicasCaughtUpEarly());
    Assert.assertEquals("no inflight groups should be created", 0, inflightGroups.size());

    // move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    // now we make all replicas in backoff mode
    dataNodeToReplicaMap.values().stream().flatMap(Collection::stream).forEach(remoteReplicaInfo -> {
      remoteReplicaInfo.setReEnableReplicationTime(
          time.milliseconds() + replicationConfig.replicationSyncedReplicaBackoffDurationMs + 1);
    });

    // all groups will be throttled now, no new groups will be created, so all replica will have caught up early
    inflightGroups = poller.enqueue();

    Assert.assertTrue("all replicas should have caught up early", poller.allReplicasCaughtUpEarly());
    Assert.assertEquals("no inflight groups should be created", 0, inflightGroups.size());
  }

  /**
   * Tests for whether standby replica group logic is working as expected.
   * Tested on crossColo replica thread
   */
  @Test
  public void testRemoteReplicaStandByGroupGenerationCrossColo() {
    ReplicaThread replicaThread = crossColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = crossColoReplicaThread.getRemoteReplicaInfos();

    replicaThread.setTerminateCurrentContinuousReplicationCycle(false);

    RemoteReplicaGroupPoller poller = replicaThread.new RemoteReplicaGroupPoller();
    List<DataNodeTracker> dataNodeTrackers = poller.getDataNodeTrackers();

    //add replicas for stand by no progress, take one replica from each datanode
    dataNodeTrackers.forEach(dataNodeTracker -> {
      List<ActiveGroupTracker> activeGroupTrackers = dataNodeTracker.getActiveGroupTrackers();
      ReplicaTracker replicaTracker = activeGroupTrackers.get(0).getPreAssignedReplicas().get(0);
      replicaTracker.setReplicaStatus(ReplicaStatus.STANDBY_NO_PROGRESS_TIMED_OUT);
    });

    // after this call, standby replicas will be added to inflight groups
    List<RemoteReplicaGroup> inflightGroups = poller.enqueue();

    // verify if all stand by group ids are created and created correctly
    dataNodeTrackers.forEach(dataNodeTracker -> {
      DataNodeId dataNodeId = dataNodeTracker.getDataNodeId();
      StandByGroupTracker standByGroupTracker = dataNodeTracker.getStandByGroupTracker();

      Assert.assertTrue("standby group id must be in flight", standByGroupTracker.isInFlight());
      Assert.assertFalse("standby group id must be in flight and should not be done ",
          standByGroupTracker.isGroupDone());
      Assert.assertTrue("standby group should be marked no progress",
          standByGroupTracker.getRemoteReplicaGroup().isNonProgressStandbyReplicaGroup());
      Assert.assertEquals("datanode must be correct", dataNodeId,
          standByGroupTracker.getRemoteReplicaGroup().getRemoteDataNode());
    });

    // mark all non standby groups as done
    inflightGroups.forEach((remoteReplicaGroup) -> {
      if (!remoteReplicaGroup.isNonProgressStandbyReplicaGroup()) {
        remoteReplicaGroup.setState(ReplicaThread.ReplicaGroupReplicationState.DONE);
      }
    });

    inflightGroups = poller.enqueue();

    // move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    // after this call new remote replica groups will be created and added to inflight groups
    inflightGroups = poller.enqueue();

    // verify if standby replicas are not added to a group again, i.e all replicas should be unique
    Set<RemoteReplicaInfo> remoteReplicaInfoSet = new HashSet<>();
    inflightGroups.forEach((remoteReplicaGroup) -> {
      remoteReplicaGroup.getRemoteReplicaInfos().forEach(remoteReplicaInfo -> {
        Assert.assertFalse("Remote replicas should not be repeated", remoteReplicaInfoSet.contains(remoteReplicaInfo));
        remoteReplicaInfoSet.add(remoteReplicaInfo);
      });
    });
  }

  /**
   * Testing if setting shouldTerminateCurrentCycle, results in termination of group generation
   * Testing on intraColo thread, as behaviour will be same in crossColo
   */
  @Test
  public void testTerminateCycle() {
    ReplicaThread replicaThread = intraColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = intraColoReplicaThread.getRemoteReplicaInfos();

    replicaThread.setTerminateCurrentContinuousReplicationCycle(false);

    RemoteReplicaGroupPoller poller = replicaThread.new RemoteReplicaGroupPoller();

    List<RemoteReplicaGroup> inflightGroups = poller.enqueue();

    Assert.assertFalse("There should be inflight replica groups", inflightGroups.isEmpty());

    // now we will signal that cycle should be terminated
    replicaThread.setTerminateCurrentContinuousReplicationCycle(true);

    // now any new groups should not be generated

    //make all groups in done state
    inflightGroups.forEach((remoteReplicaGroup) -> {
      remoteReplicaGroup.setState(ReplicaThread.ReplicaGroupReplicationState.DONE);
    });

    // after this call all groups will be throttled
    inflightGroups = poller.enqueue();

    // move time forward till throttling limit
    time.setCurrentMilliseconds(
        time.milliseconds() + replicationConfig.replicationIntraReplicaThreadThrottleSleepDurationMs + 1);

    // limit is 3 , but no new groups should be created after this call
    inflightGroups = poller.enqueue();

    Assert.assertTrue("There should be no inflight groups", inflightGroups.isEmpty());
    Assert.assertFalse("All replicas haven't caught up early", poller.allReplicasCaughtUpEarly());
  }
}
