/*
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
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.ZNRecord;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.HelixClusterManagerTest.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Test {@link DynamicClusterChangeHandler} to verify its behavior is expected when handling cluster changes.
 */
@RunWith(Parameterized.class)
public class ClusterChangeHandlerTest {
  private final HashMap<String, com.github.ambry.utils.TestUtils.ZkInfo> dcsToZkInfo = new HashMap<>();
  private final String[] dcs = new String[]{"DC0", "DC1"};
  private final String clusterNameStatic = "ClusterChangeHandlerTest";
  private final MockHelixCluster helixCluster;
  private final String selfInstanceName;
  private final String localDc;
  private final String remoteDc;
  private final boolean overrideEnabled;
  private final String hardwareLayoutPath;
  private final String partitionLayoutPath;
  private final TestHardwareLayout testHardwareLayout;
  private final TestPartitionLayout testPartitionLayout;
  private final Map<String, Map<String, String>> partitionOverrideMap = new HashMap<>();
  private final ZNRecord znRecord = new ZNRecord(PARTITION_OVERRIDE_STR);
  private final HelixClusterManagerTest.MockHelixManagerFactory helixManagerFactory;
  private final Properties props = new Properties();

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  // set up a mock helix cluster, create separate HelixClusterManager with both Simple and Dynamic cluster change handler
  public ClusterChangeHandlerTest(boolean overrideEnabled) throws Exception {
    this.overrideEnabled = overrideEnabled;
    File tempDir = Files.createTempDirectory("ClusterChangeHandlerTest-").toFile();
    String tempDirPath = tempDir.getAbsolutePath();
    tempDir.deleteOnExit();
    hardwareLayoutPath = tempDirPath + File.separator + "hardwareLayoutTest.json";
    partitionLayoutPath = tempDirPath + File.separator + "partitionLayoutTest.json";
    String zkLayoutPath = tempDirPath + File.separator + "zkLayoutPath.json";
    localDc = dcs[0];
    remoteDc = dcs[1];
    int basePort = 2300;
    byte dcId = (byte) 0;
    for (String dc : dcs) {
      dcsToZkInfo.put(dc, new com.github.ambry.utils.TestUtils.ZkInfo(tempDirPath, dc, dcId++, basePort++, true));
    }
    JSONObject zkJson = constructZkLayoutJSON(dcsToZkInfo.values());
    // initial partition count = 3, all three partitions are Read_Write
    // cluster default setup:  6 nodes per data center. Each partition has 6 replicas (3 in local dc, 3 in remote dc)
    testHardwareLayout = constructInitialHardwareLayoutJSON(clusterNameStatic);
    testPartitionLayout = constructInitialPartitionLayoutJSON(testHardwareLayout, 3, localDc);
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // pick one partition to override its SEAL state (mark it Read_Only)
    Random random = new Random();
    int partitionIndexToOverride = random.nextInt(testPartitionLayout.getPartitionCount());
    partitionOverrideMap.computeIfAbsent(String.valueOf(partitionIndexToOverride), k -> new HashMap<>())
        .put(ClusterMapUtils.PARTITION_STATE, ClusterMapUtils.READ_ONLY_STR);
    znRecord.setMapFields(partitionOverrideMap);
    Map<String, ZNRecord> znRecordMap = new HashMap<>();
    znRecordMap.put(PARTITION_OVERRIDE_ZNODE_PATH, znRecord);

    DataNode currentNode = testHardwareLayout.getRandomDataNodeFromDc(localDc);
    String hostname = currentNode.getHostname();
    int portNum = currentNode.getPort();
    selfInstanceName = getInstanceName(hostname, portNum);
    props.setProperty("clustermap.host.name", hostname);
    props.setProperty("clustermap.cluster.name", clusterNamePrefixInHelix + clusterNameStatic);
    props.setProperty("clustermap.datacenter.name", localDc);
    props.setProperty("clustermap.port", Integer.toString(portNum));
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.current.xid", Long.toString(CURRENT_XID));
    props.setProperty("clustermap.enable.partition.override", Boolean.toString(overrideEnabled));
    props.setProperty("clustermap.listen.cross.colo", Boolean.toString(true));
    helixCluster =
        new MockHelixCluster(clusterNamePrefixInHelix, hardwareLayoutPath, partitionLayoutPath, zkLayoutPath);
    helixManagerFactory = new HelixClusterManagerTest.MockHelixManagerFactory(helixCluster, znRecordMap, null);
  }

  /**
   * Close the zk servers.
   */
  @After
  public void after() {
    for (com.github.ambry.utils.TestUtils.ZkInfo zkInfo : dcsToZkInfo.values()) {
      zkInfo.shutdown();
    }
  }

  /**
   * 1. test static initialization success case
   * 2. verify live instance change is able to make node HardwareState.AVAILABLE
   * 3. verify partition override behaves correctly (if enabled)
   * 4. verify equivalence between {@link SimpleClusterChangeHandler} and {@link DynamicClusterChangeHandler} in terms of
   *    in-memory cluster info.
   * @throws Exception
   */
  @Test
  public void initializationSuccessTest() throws Exception {
    // After Helix bootstrap tool adds instances to cluster, MockHelixAdmin makes them up by default. Let's test a more
    // realistic case where all instances are added but no node has participated yet. For dynamic cluster change handler,
    // all instances in this case should be initialized to UNAVAILABLE. Until they have participated into cluster, the
    // subsequent live instance changes will make them up.
    helixCluster.bringAllInstancesDown();
    ClusterMapConfig clusterMapConfig1 = new ClusterMapConfig(new VerifiableProperties(props));
    HelixClusterManager managerWithSimpleHandler =
        new HelixClusterManager(clusterMapConfig1, selfInstanceName, helixManagerFactory, new MetricRegistry());
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.cluster.change.handler.type", "DynamicClusterChangeHandler");
    ClusterMapConfig clusterMapConfig2 = new ClusterMapConfig(new VerifiableProperties(properties));
    HelixClusterManager managerWithDynamicHandler =
        new HelixClusterManager(clusterMapConfig2, selfInstanceName, helixManagerFactory, new MetricRegistry());
    Set<String> partitionsInStaticMap = new HashSet<>(testPartitionLayout.getPartitionLayout().getAllPartitionNames());
    Set<String> partitionsInSimpleHandler = managerWithSimpleHandler.getAllPartitionIds(null)
        .stream()
        .map(PartitionId::toPathString)
        .collect(Collectors.toSet());
    Set<String> partitionsInDynamicHandler = managerWithDynamicHandler.getAllPartitionIds(null)
        .stream()
        .map(PartitionId::toPathString)
        .collect(Collectors.toSet());
    assertEquals("Partitions from dynamic change handler don't match those in static layout", partitionsInStaticMap,
        partitionsInDynamicHandler);
    assertEquals("Partitions from two HelixClusterManagers don't match", partitionsInSimpleHandler,
        partitionsInDynamicHandler);
    // verify metrics in managers with simple/dynamic handler are same
    HelixClusterManager.HelixClusterManagerCallback dynamicHandlerCallback =
        managerWithDynamicHandler.getManagerCallback();
    HelixClusterManager.HelixClusterManagerCallback simpleHandlerCallback =
        managerWithSimpleHandler.getManagerCallback();
    assertEquals("Datacenter count doesn't match", simpleHandlerCallback.getDatacenterCount(),
        dynamicHandlerCallback.getDatacenterCount());
    assertEquals("Node count doesn't match", simpleHandlerCallback.getDatanodeCount(),
        dynamicHandlerCallback.getDatanodeCount());
    assertEquals("Disk count doesn't match", simpleHandlerCallback.getDiskCount(),
        dynamicHandlerCallback.getDiskCount());
    assertEquals("Sealed count doesn't match", simpleHandlerCallback.getPartitionSealedCount(),
        dynamicHandlerCallback.getPartitionSealedCount());
    assertEquals("Raw capacity doesn't match", simpleHandlerCallback.getRawCapacity(),
        dynamicHandlerCallback.getRawCapacity());
    assertEquals("Allocated raw capacity doesn't match", simpleHandlerCallback.getAllocatedRawCapacity(),
        dynamicHandlerCallback.getAllocatedRawCapacity());
    assertEquals("Allocated usable capacity doesn't match", simpleHandlerCallback.getAllocatedUsableCapacity(),
        dynamicHandlerCallback.getAllocatedUsableCapacity());

    // verify that all nodes (except for current node) are down in HelixClusterManager with dynamic cluster change handler
    assertEquals("All nodes (except for self node) should be down", helixCluster.getDownInstances().size() - 1,
        dynamicHandlerCallback.getDownDatanodesCount());

    // then we bring all instances up and trigger live instance change again
    helixCluster.bringAllInstancesUp();
    // verify all nodes are up now up
    assertEquals("All nodes should be up now", 0, dynamicHandlerCallback.getDownDatanodesCount());
    // verify partition override, for now we have 3 partitions and one of them is overridden to Read_Only (if enabled)
    int partitionCnt = testPartitionLayout.getPartitionCount();
    assertEquals("Number of writable partitions is not correct", overrideEnabled ? partitionCnt - 1 : partitionCnt,
        dynamicHandlerCallback.getPartitionReadWriteCount());
    // close helix cluster managers
    managerWithDynamicHandler.close();
    managerWithSimpleHandler.close();
  }

  /**
   * Test failure case when initializing {@link HelixClusterManager} with dynamic cluster change handler
   */
  @Test
  public void initializationFailureTest() {
    // save current local dc for restore purpose
    int savedport = dcsToZkInfo.get(localDc).getPort();
    // mock local dc connectivity issue
    dcsToZkInfo.get(localDc).setPort(0);
    JSONObject invalidZkJson = constructZkLayoutJSON(dcsToZkInfo.values());
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.dcs.zk.connect.strings", invalidZkJson.toString(2));
    properties.setProperty("clustermap.cluster.change.handler.type", "DynamicClusterChangeHandler");
    ClusterMapConfig invalidClusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    try {
      new HelixClusterManager(invalidClusterMapConfig, selfInstanceName, helixManagerFactory, metricRegistry);
      fail("Instantiation with dynamic cluster change handler should fail due to connection issue to zk");
    } catch (IOException e) {
      assertEquals(1L,
          metricRegistry.getGauges().get(HelixClusterManager.class.getName() + ".instantiationFailed").getValue());
      assertEquals(1L, metricRegistry.getGauges()
          .get(HelixClusterManager.class.getName() + ".instantiationExceptionCount")
          .getValue());
    }
    // restore original setup
    dcsToZkInfo.get(localDc).setPort(savedport);
  }

  /**
   * Test that invalid cluster change handler type will cause instantiation failure.
   */
  @Test
  public void invalidClusterChangeHandlerTest() {
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.cluster.change.handler.type", "InvalidClusterChangeHandler");
    ClusterMapConfig invalidConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    MetricRegistry metricRegistry = new MetricRegistry();
    try {
      new HelixClusterManager(invalidConfig, selfInstanceName, helixManagerFactory, metricRegistry);
      fail("Should fail because the cluster change handler type is invalid.");
    } catch (IOException e) {
      assertEquals(1L,
          metricRegistry.getGauges().get(HelixClusterManager.class.getName() + ".instantiationFailed").getValue());
    }
  }

  /**
   * Test new instances/partitions are added to cluster dynamically. {@link HelixClusterManager} with
   * {@link DynamicClusterChangeHandler} should absorb the change and update in-mem cluster map.
   * 1. add new instance
   * 2. add new partition onto new instance
   * 3. add new partition onto existing instance
   */
  @Test
  public void addNewInstancesAndPartitionsTest() throws Exception {
    // create a HelixClusterManager with DynamicClusterChangeHandler
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.cluster.change.handler.type", "DynamicClusterChangeHandler");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    HelixClusterManager helixClusterManager =
        new HelixClusterManager(clusterMapConfig, selfInstanceName, helixManagerFactory, new MetricRegistry());
    // before adding new instances, let's first ensure current number of nodes is correct.
    List<DataNode> dataNodesInLayout = new ArrayList<>();
    testHardwareLayout.getHardwareLayout().getDatacenters().forEach(dc -> dataNodesInLayout.addAll(dc.getDataNodes()));
    assertEquals("Number of data nodes is not expected", dataNodesInLayout.size(),
        helixClusterManager.getDataNodeIds().size());

    // pick up 2 existing nodes from each dc
    List<DataNode> nodesToHostNewPartition = new ArrayList<>();
    DataNode localDcNode1 = testHardwareLayout.getRandomDataNodeFromDc(localDc);
    DataNode localDcNode2;
    do {
      localDcNode2 = testHardwareLayout.getRandomDataNodeFromDc(localDc);
    } while (localDcNode1 == localDcNode2);
    DataNode remoteDcNode1 = testHardwareLayout.getRandomDataNodeFromDc(remoteDc);
    DataNode remoteDcNode2;
    do {
      remoteDcNode2 = testHardwareLayout.getRandomDataNodeFromDc(remoteDc);
    } while (remoteDcNode1 == remoteDcNode2);

    // add a new node into static layout
    testHardwareLayout.addNewDataNodes(1);
    // add a new partition to static layout and put its replicas to both existing nodes and new node
    List<DataNode> newAddedNodes = new ArrayList<>();
    testHardwareLayout.getHardwareLayout().getDatacenters().forEach(dc -> newAddedNodes.addAll(dc.getDataNodes()));
    newAddedNodes.removeAll(dataNodesInLayout);
    // pick 2 existing nodes and 1 new node from each dc to place replica of new partition
    nodesToHostNewPartition.addAll(Arrays.asList(localDcNode1, localDcNode2));
    nodesToHostNewPartition.addAll(Arrays.asList(remoteDcNode1, remoteDcNode2));
    nodesToHostNewPartition.addAll(newAddedNodes);
    testPartitionLayout.addNewPartition(testHardwareLayout, nodesToHostNewPartition, DEFAULT_PARTITION_CLASS);

    // write new HardwareLayout and PartitionLayout into files
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // this triggers a InstanceConfig change notification.
    // In each dc, 2 existing instance configs are updated and 1 new instance is added as well as 1 new partition
    helixCluster.upgradeWithNewHardwareLayout(hardwareLayoutPath);

    // verify after InstanceConfig change, HelixClusterManager contains the one more node per dc.
    assertEquals("Number of data nodes after instance addition is not correct",
        testHardwareLayout.getAllExistingDataNodes().size(), helixClusterManager.getDataNodeIds().size());
    // verify number of partitions in cluster manager has increased by 1
    assertEquals("Number of partitions after partition addition is not correct",
        testPartitionLayout.getPartitionCount(), helixClusterManager.getAllPartitionIds(null).size());
    // verify writable partitions in HelixClusterManager with partition override enabled/disabled (to call getWritablePartitionIds,
    // we need to bring new added instances up because that method checks if all local replicas are up)
    for (DataNode newNode : newAddedNodes) {
      helixCluster.bringInstanceUp(getInstanceName(newNode.getHostname(), newNode.getPort()));
    }
    assertEquals("Number of writable partitions after partition addition is not correct",
        overrideEnabled ? testPartitionLayout.getPartitionCount() - 1 : testPartitionLayout.getPartitionCount(),
        helixClusterManager.getWritablePartitionIds(null).size());

    // verify capacity stats are updated
    HelixClusterManager.HelixClusterManagerCallback clusterManagerCallback = helixClusterManager.getManagerCallback();
    // note that we add one node to each dc, so the raw capacity = (# of nodes) * (# of disks) * (disk capacity)
    long rawCapacityInStaticLayout =
        testHardwareLayout.getAllExistingDataNodes().size() * testHardwareLayout.getDiskCount()
            * testHardwareLayout.getDiskCapacityInBytes();
    assertEquals("Raw capacity of entire cluster is not expected", rawCapacityInStaticLayout,
        clusterManagerCallback.getRawCapacity());
    // we have added one more partition, so now the allocated raw capacity in cluster is 4 (partition count) * 6 * ReplicaCapacity
    assertEquals("Allocated raw capacity of entire cluster is not correct",
        testPartitionLayout.getAllocatedRawCapacityInBytes(), clusterManagerCallback.getAllocatedRawCapacity());
    // verify usable capacity
    assertEquals("Allocated usable capacity of entire cluster is not correct",
        testPartitionLayout.getAllocatedUsableCapacityInBytes(), clusterManagerCallback.getAllocatedUsableCapacity());

    // additional tests to verify getting replicas, disks and resources etc returns correct results.
    for (DataNode newNode : newAddedNodes) {
      AmbryDataNode ambryNode = helixClusterManager.getDataNodeId(newNode.getHostname(), newNode.getPort());
      assertNotNull("New added node should exist in HelixClusterManager", ambryNode);
      List<AmbryReplica> ambryReplicas = helixClusterManager.getReplicaIds(ambryNode);
      assertEquals("There should be one replica on the new node", 1, ambryReplicas.size());
      Set<AmbryDisk> ambryDisks = new HashSet<>(clusterManagerCallback.getDisks(ambryNode));
      assertEquals("Disk count on the new node is not correct", localDcNode1.getDisks().size(), ambryDisks.size());
      // verify that get a non-existent partition on new node should return null
      assertNull("Should return null when getting a non-existent replica on new node",
          helixClusterManager.getReplicaForPartitionOnNode(ambryNode, "0"));
    }
    // trigger IdealState change and refresh partition-to-resource mapping (bring in the new partition in resource map)
    helixCluster.refreshIdealState();
    Map<String, String> partitionNameToResource = helixClusterManager.getPartitionToResourceMap().get(localDc);
    List<PartitionId> partitionIds = testPartitionLayout.getPartitionLayout().getPartitions(null);
    // verify all partitions (including the new added one) are present in partition-to-resource map
    Set<String> partitionNames = partitionIds.stream().map(PartitionId::toPathString).collect(Collectors.toSet());
    assertEquals("Some partitions are not present in partition-to-resource map", partitionNames,
        partitionNameToResource.keySet());
    helixClusterManager.close();
  }

  /**
   * Test the case where a current replica is moved between existing nodes.
   */
  @Test
  public void moveReplicaTest() throws Exception {
    // create a HelixClusterManager with DynamicClusterChangeHandler
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.cluster.change.handler.type", "DynamicClusterChangeHandler");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    HelixClusterManager helixClusterManager =
        new HelixClusterManager(clusterMapConfig, selfInstanceName, helixManagerFactory, new MetricRegistry());
    // pick a partition and move one of its replicas
    Partition testPartition =
        (Partition) testPartitionLayout.getPartitionLayout().getRandomWritablePartition(null, null);
    int previousReplicaCnt = testPartition.getReplicaIds().size();
    // 1. find out nodes in local dc that host this partition
    Set<DataNode> localDcNodes = new HashSet<>();
    testPartition.getReplicaIds().forEach(r -> {
      if (r.getDataNodeId().getDatacenterName().equals(localDc)) {
        localDcNodes.add((DataNode) r.getDataNodeId());
      }
    });
    // 2. then find a node in local dc that doesn't host this partition (this is the node we will add replica to)
    Datacenter localDatacenter = testHardwareLayout.getHardwareLayout()
        .getDatacenters()
        .stream()
        .filter(dc -> dc.getName().equals(localDc))
        .findFirst()
        .get();
    DataNode nodeToAddReplica =
        localDatacenter.getDataNodes().stream().filter(node -> !localDcNodes.contains(node)).findFirst().get();
    testPartitionLayout.addReplicaToPartition(nodeToAddReplica, testPartition);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // 3. We upgrade helix by adding new replica to the chosen node in local dc. This is to mock "replica addition" on
    //    chosen node and chosen node updates its instanceConfig in Helix. There should be 7 (= 6+1) replicas in the
    //    intermediate state.
    helixCluster.upgradeWithNewPartitionLayout(partitionLayoutPath);
    PartitionId partitionInManager = helixClusterManager.getAllPartitionIds(null)
        .stream()
        .filter(p -> p.toPathString().equals(testPartition.toPathString()))
        .findFirst()
        .get();
    assertEquals("Replica count of testing partition is not correct", previousReplicaCnt + 1,
        partitionInManager.getReplicaIds().size());
    // 4. find a replica (from same partition) in local dc that is not just added one
    Replica oldReplica = (Replica) testPartition.getReplicaIds()
        .stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals(localDc) && r.getDataNodeId() != nodeToAddReplica)
        .findFirst()
        .get();
    testPartitionLayout.removeReplicaFromPartition(oldReplica);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // 5. upgrade Helix again to mock one of the old replicas is removed and the node (where replica previously resides)
    //    updates the InstanceConfig in Helix. The number of replicas should become 6 again.
    helixCluster.upgradeWithNewPartitionLayout(partitionLayoutPath);
    assertEquals("Replica count of testing partition is not correct", previousReplicaCnt,
        partitionInManager.getReplicaIds().size());

    helixClusterManager.close();
  }
}
