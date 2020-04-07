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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.HelixClusterManagerTest.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.AdditionalAnswers.*;
import static org.mockito.Mockito.*;


/**
 * Test {@link DynamicClusterChangeHandler} to verify its behavior is expected when handling cluster changes.
 */
@RunWith(Parameterized.class)
public class ClusterChangeHandlerTest {
  private static final String DEFAULT_PARTITION_CLASS = "defaultPartitionClass";
  private final HashMap<String, com.github.ambry.utils.TestUtils.ZkInfo> dcsToZkInfo = new HashMap<>();
  private final String[] dcs = new String[]{"DC0", "DC1"};
  private final String clusterNameStatic = "ClusterChangeHandlerTest";
  private final MockHelixCluster helixCluster;
  private final String selfInstanceName;
  private final DataNode currentNode;
  private final String localDc;
  private final String remoteDc;
  private final boolean overrideEnabled;
  private final String hardwareLayoutPath;
  private final String partitionLayoutPath;
  private final TestHardwareLayout testHardwareLayout;
  private final TestPartitionLayout testPartitionLayout;
  private final Map<String, Map<String, String>> partitionOverrideMap = new HashMap<>();
  private final Map<String, ZNRecord> znRecordMap = new HashMap<>();
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
    ZNRecord znRecord = new ZNRecord(PARTITION_OVERRIDE_STR);
    znRecord.setMapFields(partitionOverrideMap);
    znRecordMap.put(PARTITION_OVERRIDE_ZNODE_PATH, znRecord);

    currentNode = testHardwareLayout.getRandomDataNodeFromDc(localDc);
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
   * Test that {@link DynamicClusterChangeHandler} is able to handle invalid info entry in the InstanceConfig at runtime
   * or during initialization.
   */
  @Test
  public void instanceConfigInvalidInfoEntryTest() {
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.cluster.change.handler.type", "DynamicClusterChangeHandler");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    HelixClusterManager.HelixClusterManagerCallback mockManagerCallback =
        Mockito.mock(HelixClusterManager.HelixClusterManagerCallback.class);
    HelixClusterManager.ClusterChangeHandlerCallback mockHandlerCallback =
        Mockito.mock(HelixClusterManager.ClusterChangeHandlerCallback.class);
    doAnswer(returnsFirstArg()).when(mockHandlerCallback).addPartitionIfAbsent(any(), anyLong());

    Counter initFailureCount = new Counter();
    DynamicClusterChangeHandler dynamicChangeHandler =
        new DynamicClusterChangeHandler(clusterMapConfig, localDc, selfInstanceName, Collections.emptyMap(),
            mockManagerCallback, mockHandlerCallback,
            new HelixClusterManagerMetrics(new MetricRegistry(), mockManagerCallback), e -> initFailureCount.inc(),
            new AtomicLong());
    // create an InstanceConfig with invalid entry that mocks error info added by Helix controller
    PartitionId selectedPartition = testPartitionLayout.getPartitionLayout().getPartitions(null).get(0);
    Replica testReplica = (Replica) selectedPartition.getReplicaIds().get(0);
    DataNode testNode = (DataNode) testReplica.getDataNodeId();
    InstanceConfig instanceConfig = new InstanceConfig(getInstanceName(testNode.getHostname(), testNode.getPort()));
    instanceConfig.setHostName(testNode.getHostname());
    instanceConfig.setPort(Integer.toString(testNode.getPort()));
    instanceConfig.getRecord().setSimpleField(ClusterMapUtils.DATACENTER_STR, testNode.getDatacenterName());
    instanceConfig.getRecord().setSimpleField(ClusterMapUtils.RACKID_STR, testNode.getRackId());
    instanceConfig.getRecord()
        .setSimpleField(ClusterMapUtils.SCHEMA_VERSION_STR, Integer.toString(ClusterMapUtils.CURRENT_SCHEMA_VERSION));
    instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, Collections.emptyList());
    instanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, Collections.emptyList());

    Map<String, Map<String, String>> diskInfos = new HashMap<>();
    assertNotNull("testReplica should not be null", testReplica);
    Map<String, String> diskInfo = new HashMap<>();
    diskInfo.put(ClusterMapUtils.DISK_CAPACITY_STR, Long.toString(testReplica.getDiskId().getRawCapacityInBytes()));
    diskInfo.put(ClusterMapUtils.DISK_STATE, ClusterMapUtils.AVAILABLE_STR);
    String replicasStrBuilder =
        testReplica.getPartition().getId() + ClusterMapUtils.REPLICAS_STR_SEPARATOR + testReplica.getCapacityInBytes()
            + ClusterMapUtils.REPLICAS_STR_SEPARATOR + testReplica.getPartition().getPartitionClass()
            + ClusterMapUtils.REPLICAS_DELIM_STR;
    diskInfo.put(ClusterMapUtils.REPLICAS_STR, replicasStrBuilder);
    diskInfos.put(testReplica.getDiskId().getMountPath(), diskInfo);
    // add an invalid entry at the end of diskInfos
    Map<String, String> invalidEntry = new HashMap<>();
    invalidEntry.put("INVALID_KEY", "INVALID_VALUE");
    diskInfos.put("INVALID_MOUNT_PATH", invalidEntry);
    instanceConfig.getRecord().setMapFields(diskInfos);
    // we call onInstanceConfigChange() twice
    // 1st call, to verify initialization code path
    dynamicChangeHandler.onInstanceConfigChange(Collections.singletonList(instanceConfig), null);
    // 2nd call, to verify dynamic update code path
    dynamicChangeHandler.onInstanceConfigChange(Collections.singletonList(instanceConfig), null);
    assertEquals("There shouldn't be initialization errors", 0, initFailureCount.getCount());
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

    // pick 2 existing nodes from each dc (also the nodes from local dc should be different from currentNode)
    List<DataNode> nodesToHostNewPartition = new ArrayList<>();
    List<DataNode> localDcNodes = testHardwareLayout.getAllDataNodesFromDc(localDc);
    localDcNodes.remove(currentNode);
    DataNode localDcNode1 = localDcNodes.get(0);
    DataNode localDcNode2 = localDcNodes.get(localDcNodes.size() - 1);
    List<DataNode> remoteDcNodes = testHardwareLayout.getAllDataNodesFromDc(remoteDc);
    DataNode remoteDcNode1 = remoteDcNodes.get(0);
    DataNode remoteDcNode2 = remoteDcNodes.get(remoteDcNodes.size() - 1);

    // add a new node into static layout
    testHardwareLayout.addNewDataNodes(1);
    // add a new partition to static layout and put its replicas to both existing nodes and new node
    List<DataNode> newAddedNodes = new ArrayList<>();
    testHardwareLayout.getHardwareLayout().getDatacenters().forEach(dc -> newAddedNodes.addAll(dc.getDataNodes()));
    newAddedNodes.removeAll(dataNodesInLayout);
    // pick 2 existing nodes and 1 new node from each dc to add a replica from new partition
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
   * Test the case where replica is added or removed and {@link PartitionSelectionHelper} is able to incorporate cluster
   * map changes.
   * Test setup: (1) remove one replica of Partition1 from local dc;
   *             (2) add another replica of Partition2 to local dc;
   *             (3) add one replica of Partition1 to remote dc;
   * @throws Exception
   */
  @Test
  public void partitionSelectionOnReplicaAddedOrRemovedTest() throws Exception {
    assumeTrue(!overrideEnabled);
    // create a HelixClusterManager with DynamicClusterChangeHandler
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.cluster.change.handler.type", "DynamicClusterChangeHandler");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    HelixClusterManager helixClusterManager =
        new HelixClusterManager(clusterMapConfig, selfInstanceName, helixManagerFactory, new MetricRegistry());

    List<PartitionId> partitions = testPartitionLayout.getPartitionLayout().getPartitions(null);
    // partition1 has a replica to remove in local dc; partition2 has a replica to add in local dc
    // also add an extra replica from partition1 in remote dc
    PartitionId partition1 = partitions.get(0);
    PartitionId partition2 = partitions.get(partitions.size() - 1);
    int replicaCountForPart1 = partition1.getReplicaIds().size();
    int replicaCountForPart2 = partition2.getReplicaIds().size();
    List<DataNodeId> remoteNodesForPartition1 = getPartitionDataNodesFromDc(partition1, remoteDc);
    List<DataNodeId> localNodesForPartition2 = getPartitionDataNodesFromDc(partition2, localDc);
    // remove one replica from partition1 in local dc
    Replica replicaToRemove = (Replica) partition1.getReplicaIds()
        .stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals(localDc))
        .findFirst()
        .get();
    testPartitionLayout.removeReplicaFromPartition(replicaToRemove);
    // add one replica of partition1 to remote dc
    Datacenter localDataCenter = null, remoteDataCenter = null;
    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      if (datacenter.getName().equals(localDc)) {
        localDataCenter = datacenter;
      } else {
        remoteDataCenter = datacenter;
      }
    }
    assertNotNull("Remote data center is null", remoteDataCenter);
    assertNotNull("Local data center is null", localDataCenter);
    DataNode nodeToAddPartition1 =
        remoteDataCenter.getDataNodes().stream().filter(n -> !remoteNodesForPartition1.contains(n)).findFirst().get();
    testPartitionLayout.addReplicaToPartition(nodeToAddPartition1, (Partition) partition1);
    // add one replica of partition2 to node in local dc (the node should be different to currentNode as we didn't
    // populate bootstrap replica map for this test)
    DataNode nodeToAddPartition2 = localDataCenter.getDataNodes()
        .stream()
        .filter(n -> !localNodesForPartition2.contains(n) && n != currentNode)
        .findFirst()
        .get();
    testPartitionLayout.addReplicaToPartition(nodeToAddPartition2, (Partition) partition2);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);

    helixCluster.upgradeWithNewPartitionLayout(partitionLayoutPath);

    List<PartitionId> partitionsInManager = helixClusterManager.getAllPartitionIds(null);
    PartitionId ambryPartition1 =
        partitionsInManager.stream().filter(p -> p.toPathString().equals(partition1.toPathString())).findFirst().get();
    PartitionId ambryPartition2 =
        partitionsInManager.stream().filter(p -> p.toPathString().equals(partition2.toPathString())).findFirst().get();
    assertEquals("Replica count of partition1 is not expected", replicaCountForPart1,
        ambryPartition1.getReplicaIds().size());
    assertEquals("Replica count of partition2 is not expected", replicaCountForPart2 + 1,
        ambryPartition2.getReplicaIds().size());
    // Note that there are 3 partitions in total in cluster, partition[0] has 2 replicas, partition[1] has 3 replicas
    // and partition[2] has 4 replicas in local dc. Also the minimumLocalReplicaCount = 3, so this is to test if partition
    // selection helper is able to pick right partitions with local replica count >= 3.
    List<PartitionId> writablePartitions = helixClusterManager.getWritablePartitionIds(DEFAULT_PARTITION_CLASS);
    assertEquals("Number of writable partition is not expected", 2, writablePartitions.size());
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
    List<DataNodeId> localDcNodes = getPartitionDataNodesFromDc(testPartition, localDc);
    // 2. then find a node in local dc that doesn't host this partition (this is the node we will add replica to)
    Datacenter localDatacenter = testHardwareLayout.getHardwareLayout()
        .getDatacenters()
        .stream()
        .filter(dc -> dc.getName().equals(localDc))
        .findFirst()
        .get();
    // since we didn't populate bootstrap replica map, we have to avoid adding replica to currentNode
    DataNode nodeToAddReplica = localDatacenter.getDataNodes()
        .stream()
        .filter(node -> !localDcNodes.contains(node) && node != currentNode)
        .findFirst()
        .get();
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

  /**
   * Test the case where current node receives InstanceConfig change triggered by itself due to replica addition. We need
   * to verify {@link DynamicClusterChangeHandler} will check if new replica from InstanceConfig exists in bootstrap
   * replica map. The intention here is to avoid creating a second instance of replica on current node.
   */
  @Test
  public void replicaAdditionOnCurrentNodeTest() throws Exception {
    // create a HelixClusterManager with DynamicClusterChangeHandler
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty("clustermap.cluster.change.handler.type", "DynamicClusterChangeHandler");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(properties));
    HelixClusterManager helixClusterManager =
        new HelixClusterManager(clusterMapConfig, selfInstanceName, helixManagerFactory, new MetricRegistry());
    // test setup: create 2 new partitions and place their replicas onto nodes that exclude currentNode. This is to avoid
    // edge case where currentNode already has all partitions in cluster
    Set<PartitionId> initialPartitionSet = new HashSet<>(testPartitionLayout.getPartitionLayout().getPartitions(null));
    List<DataNode> nodesToHostNewPartition = new ArrayList<>();
    List<DataNode> localDcNodes = testHardwareLayout.getAllDataNodesFromDc(localDc)
        .stream()
        .filter(node -> node != currentNode)
        .collect(Collectors.toList());
    List<DataNode> remoteDcNodes = testHardwareLayout.getAllDataNodesFromDc(remoteDc);
    nodesToHostNewPartition.addAll(localDcNodes.subList(0, 3));
    nodesToHostNewPartition.addAll(remoteDcNodes.subList(0, 3));
    testPartitionLayout.addNewPartition(testHardwareLayout, nodesToHostNewPartition, DEFAULT_PARTITION_CLASS);
    // add one more new partition
    testPartitionLayout.addNewPartition(testHardwareLayout, nodesToHostNewPartition, DEFAULT_PARTITION_CLASS);
    // write new HardwareLayout and PartitionLayout into files
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // this triggers a InstanceConfig change notification.
    helixCluster.upgradeWithNewHardwareLayout(hardwareLayoutPath);
    Set<PartitionId> updatedPartitionSet = new HashSet<>(testPartitionLayout.getPartitionLayout().getPartitions(null));
    updatedPartitionSet.removeAll(initialPartitionSet);
    List<PartitionId> addedPartitions = new ArrayList<>(updatedPartitionSet);
    assertEquals("There should be 2 added partitions", 2, addedPartitions.size());
    Partition addedPartition1 = (Partition) addedPartitions.get(0);
    Partition addedPartition2 = (Partition) addedPartitions.get(1);
    // add one replica of this newly added partition1 to currentNode
    testPartitionLayout.addReplicaToPartition(currentNode, addedPartition1);
    // before upgrading Helix, let's save the replica count of test partition to a variable
    PartitionId partitionInManager = helixClusterManager.getAllPartitionIds(null)
        .stream()
        .filter(p -> p.toPathString().equals(addedPartition1.toPathString()))
        .findFirst()
        .get();
    int previousReplicaCnt = partitionInManager.getReplicaIds().size();
    // test case 1: without populating bootstrap replica, new replica in InstanceConfig will trigger exception on current
    // node (this shouldn't happen in practice but we still mock this situation to perform exhaustive testing)
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    helixCluster.upgradeWithNewPartitionLayout(partitionLayoutPath);
    assertEquals("Replica count of testing partition shouldn't change", previousReplicaCnt,
        partitionInManager.getReplicaIds().size());
    // verify there is an exception when handling instance config change due to replica not found in bootstrap replica map
    assertEquals("Instance config change error count should be 1", 1,
        helixClusterManager.helixClusterManagerMetrics.instanceConfigChangeErrorCount.getCount());
    helixClusterManager.close();
    // test case 2: call getBootstrapReplica in HelixClusterManager to populate bootstrap replica map and then upgrade
    // Helix again.
    Map<String, Map<String, String>> partitionToReplicaInfosMap = new HashMap<>();
    Map<String, String> newReplicaInfos = new HashMap<>();
    newReplicaInfos.put(PARTITION_CLASS_STR, DEFAULT_PARTITION_CLASS);
    newReplicaInfos.put(REPLICAS_CAPACITY_STR, String.valueOf(TestPartitionLayout.defaultReplicaCapacityInBytes));
    newReplicaInfos.put(currentNode.getHostname() + "_" + currentNode.getPort(),
        currentNode.getDisks().get(0).getMountPath());
    partitionToReplicaInfosMap.put(addedPartition2.toPathString(), newReplicaInfos);
    // set ZNRecord
    ZNRecord replicaInfosZNRecord = new ZNRecord(REPLICA_ADDITION_STR);
    replicaInfosZNRecord.setMapFields(partitionToReplicaInfosMap);
    znRecordMap.put(REPLICA_ADDITION_ZNODE_PATH, replicaInfosZNRecord);
    // create a new HelixClusterManager with replica addition info in Helix
    helixClusterManager = new HelixClusterManager(clusterMapConfig, selfInstanceName,
        new HelixClusterManagerTest.MockHelixManagerFactory(helixCluster, znRecordMap, null), new MetricRegistry());
    ReplicaId bootstrapReplica = helixClusterManager.getBootstrapReplica(addedPartition2.toPathString(), currentNode);
    assertNotNull("Getting bootstrap replica should succeed", bootstrapReplica);
    // add replica of new partition2 to currentNode
    testPartitionLayout.addReplicaToPartition(currentNode, addedPartition2);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    helixCluster.upgradeWithNewPartitionLayout(partitionLayoutPath);

    partitionInManager = helixClusterManager.getAllPartitionIds(null)
        .stream()
        .filter(p -> p.toPathString().equals(addedPartition2.toPathString()))
        .findFirst()
        .get();
    // this time the new replica should be present in bootstrap replica map and therefore replica count should increase
    assertEquals("Replica count of testing partition shouldn't change", previousReplicaCnt + 1,
        partitionInManager.getReplicaIds().size());
    // verify that the replica instance in HelixClusterManager is same with bootstrap replica instance
    ReplicaId replicaInManager = helixClusterManager.getReplicaIds(
        helixClusterManager.getDataNodeId(currentNode.getHostname(), currentNode.getPort()))
        .stream()
        .filter(r -> r.getPartitionId().toPathString().equals(addedPartition2.toPathString()))
        .findFirst()
        .get();
    assertSame("There should be exactly one instance for added replica", replicaInManager, bootstrapReplica);
    helixClusterManager.close();
  }

  /**
   * Get data nodes that hold given partition in certain datacenter
   * @param partitionId the partition which the nodes should hold.
   * @param dcName the data center in which these nodes are.
   * @return a list of nodes that hold given partition.
   */
  private List<DataNodeId> getPartitionDataNodesFromDc(PartitionId partitionId, String dcName) {
    return partitionId.getReplicaIds()
        .stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals(dcName))
        .map(ReplicaId::getDataNodeId)
        .collect(Collectors.toList());
  }
}
