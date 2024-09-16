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
import com.github.ambry.replication.continuous.DataNodeTracker;
import com.github.ambry.replication.continuous.GroupTracker;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.utils.Pair;
import java.util.Arrays;
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

  @Test
  public void trackersGenerationTest() {
    ReplicaThread replicaThread = intraColoReplicaThread;
    Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaMap = replicaThread.getRemoteReplicaInfos();

    ReplicaThread.RemoteReplicaGroupPoller remoteReplicaGroupPoller = replicaThread.new RemoteReplicaGroupPoller();

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
}
