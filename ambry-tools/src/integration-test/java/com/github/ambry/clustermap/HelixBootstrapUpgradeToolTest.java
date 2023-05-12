/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.HelixBootstrapUpgradeUtil.*;
import static com.github.ambry.clustermap.HelixBootstrapUpgradeUtil.getInstanceName;
import static com.github.ambry.clustermap.HelixBootstrapUpgradeUtil.HelixAdminOperation.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;


@RunWith(Parameterized.class)
public class HelixBootstrapUpgradeToolTest {
  private static String tempDirPath;
  private static final Map<String, ZkInfo> dcsToZkInfo = new HashMap<>();
  private static final String[] dcs = new String[]{"DC0", "DC1"};
  private static final byte[] ids = new byte[]{(byte) 0, (byte) 1};
  private final String hardwareLayoutPath;
  private final String partitionLayoutPath;
  private final String zkLayoutPath;
  private final String adminConfigFilePath;
  private final JSONObject zkJson;
  private final String dcStr;
  private final DataNodeConfigSourceType dataNodeConfigSourceType;
  private Set<String> activeDcSet;
  private TestHardwareLayout testHardwareLayout;
  private TestPartitionLayout testPartitionLayout;
  private static final String CLUSTER_NAME_IN_STATIC_CLUSTER_MAP = "ToolTestStatic";
  private static final String CLUSTER_NAME_PREFIX = "Ambry-";
  private static final String ROOT_PATH =
      "/" + CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP + "/" + ClusterMapUtils.PROPERTYSTORE_STR;
  private static HelixPropertyStoreConfig propertyStoreConfig;

  /**
   * Shutdown all Zk servers before exit.
   */
  @AfterClass
  public static void destroy() {
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      zkInfo.shutdown();
    }
  }

  @BeforeClass
  public static void initialize() throws IOException {
    tempDirPath = getTempDir("helixBootstrapUpgrade-");
    Properties storeProps = new Properties();
    storeProps.setProperty("helix.property.store.root.path", ROOT_PATH);
    propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(storeProps));
    int port = 2200;
    for (int i = 0; i < dcs.length; i++) {
      dcsToZkInfo.put(dcs[i], new ZkInfo(tempDirPath, dcs[i], ids[i], port++, true));
    }
  }

  @After
  public void clear() {
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
      admin.dropCluster(CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP);
    }
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{"DC1", DataNodeConfigSourceType.INSTANCE_CONFIG},
        {"DC1", DataNodeConfigSourceType.PROPERTY_STORE}, {"DC1, DC0", DataNodeConfigSourceType.INSTANCE_CONFIG},
        {"DC1, DC0", DataNodeConfigSourceType.PROPERTY_STORE}, {"all", DataNodeConfigSourceType.INSTANCE_CONFIG},
        {"all", DataNodeConfigSourceType.PROPERTY_STORE},});
  }

  /**
   * Initialize ZKInfos for all dcs and start the ZK server.
   */
  public HelixBootstrapUpgradeToolTest(String dcStr, DataNodeConfigSourceType configSourceType) {
    hardwareLayoutPath = tempDirPath + "/hardwareLayoutTest.json";
    partitionLayoutPath = tempDirPath + "/partitionLayoutTest.json";
    zkLayoutPath = tempDirPath + "/zkLayoutPath.json";
    adminConfigFilePath = tempDirPath + "/adminConfigFile.txt";
    testHardwareLayout = constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP);
    testPartitionLayout =
        constructInitialPartitionLayoutJSON(testHardwareLayout, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, null);
    this.dcStr = dcStr;
    dataNodeConfigSourceType = configSourceType;
    if (!dcStr.equalsIgnoreCase("all")) {
      activeDcSet = Arrays.stream(dcStr.replaceAll("\\p{Space}", "").split(",")).collect(Collectors.toSet());
    } else {
      activeDcSet = new HashSet<>(Arrays.asList(dcs));
    }
    // Create a json involving only the active set.
    zkJson = constructZkLayoutJSON(dcsToZkInfo.entrySet()
        .stream()
        .filter(k -> activeDcSet.contains(k.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        .values());
  }

  /**
   * Test {@link HelixBootstrapUpgradeUtil#parseAndUpdateDcInfoFromArg(String, String)} method.
   */
  @Test
  public void testParseDcSet() throws Exception {
    assumeTrue(dcStr.equals("all"));
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    try {
      HelixBootstrapUpgradeUtil.parseAndUpdateDcInfoFromArg(null, zkLayoutPath);
      fail("Null dc string should fail");
    } catch (IllegalArgumentException e) {
    }
    try {
      HelixBootstrapUpgradeUtil.parseAndUpdateDcInfoFromArg("", zkLayoutPath);
      fail("Empty dc string should fail");
    } catch (IllegalArgumentException e) {
    }
    try {
      HelixBootstrapUpgradeUtil.parseAndUpdateDcInfoFromArg("inv, inv1", zkLayoutPath);
      fail("Invalid dc strings should fail");
    } catch (IllegalArgumentException e) {
    }
    Set<String> expected = new HashSet<>(Collections.singletonList("DC1"));
    Assert.assertEquals(expected, HelixBootstrapUpgradeUtil.parseAndUpdateDcInfoFromArg("DC1", zkLayoutPath).keySet());
    expected.add("DC0");
    Assert.assertEquals(expected,
        HelixBootstrapUpgradeUtil.parseAndUpdateDcInfoFromArg("DC0, DC1", zkLayoutPath).keySet());
    Assert.assertEquals(expected, HelixBootstrapUpgradeUtil.parseAndUpdateDcInfoFromArg("all", zkLayoutPath).keySet());
  }

  /**
   * Test {@link HelixBootstrapUpgradeUtil} addStateModelDef() method.
   */
  @Test
  public void testAddStateModelDef() throws Exception {
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // test add state model to non-exist cluster, which should fail
    try {
      HelixBootstrapUpgradeUtil.addStateModelDef(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
          CLUSTER_NAME_PREFIX, dcStr, ClusterMapConfig.AMBRY_STATE_MODEL_DEF);
      fail("should fail due to non-exist cluster");
    } catch (IllegalStateException e) {
      // expected
    }
    // bootstrap a cluster
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, new HelixAdminFactory(), false,
        ClusterMapConfig.OLD_STATE_MODEL_DEF, BootstrapCluster, dataNodeConfigSourceType, false, 0);
    // add new state model def
    HelixBootstrapUpgradeUtil.addStateModelDef(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, ClusterMapConfig.AMBRY_STATE_MODEL_DEF);
    // add existing state model def should be no-op
    HelixBootstrapUpgradeUtil.addStateModelDef(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, ClusterMapConfig.OLD_STATE_MODEL_DEF);
    // ensure that active dcs have new state model def
    String clusterName = CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
    for (Datacenter dc : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      ZkInfo zkInfo = dcsToZkInfo.get(dc.getName());
      ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
      if (!activeDcSet.contains(dc.getName())) {
        Assert.assertFalse("Cluster should not be present, as dc " + dc.getName() + " is not enabled",
            admin.getClusters().contains(CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP));
      } else {
        assertEquals("Mismatch in number of state model defs in cluster", 2,
            admin.getStateModelDefs(clusterName).size());
        assertTrue("Missing ambry state model in cluster",
            admin.getStateModelDefs(clusterName).contains(ClusterMapConfig.AMBRY_STATE_MODEL_DEF));
      }
    }
  }

  /**
   * Test the case where the zkHosts JSON does not have an entry for every Datacenter in the static clustermap.
   */
  @Test
  public void testIncompleteZKHostInfo() throws Exception {
    assumeTrue(dcStr.equalsIgnoreCase("all"));
    if (testHardwareLayout.getDatacenterCount() > 1) {
      JSONObject partialZkJson =
          constructZkLayoutJSON(Collections.singleton(dcsToZkInfo.entrySet().iterator().next().getValue()));
      Utils.writeJsonObjectToFile(partialZkJson, zkLayoutPath);
      Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
      Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
      try {
        HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
            CLUSTER_NAME_PREFIX, dcStr, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, new HelixAdminFactory(),
            false, ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, BootstrapCluster, dataNodeConfigSourceType, false, 0);
        fail("Should have thrown IllegalArgumentException as a zk host is missing for one of the dcs");
      } catch (IllegalArgumentException e) {
        // OK
      }
    }
  }

  /**
   * Test that dropping cluster fails because of invalid zk endpoints count. (HelixBootstrapTool should perform operation
   * against one ZK endpoint in certain dc at a time. Bootstrapping multiple clusters concurrently is not allowed)
   * @throws Exception
   */
  @Test
  public void testInvalidZKEndpointCount() throws Exception {
    JSONArray zkInfosJson = new JSONArray();
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      JSONObject zkInfoJson = new JSONObject();
      zkInfoJson.put(ClusterMapUtils.DATACENTER_STR, zkInfo.getDcName());
      zkInfoJson.put(ClusterMapUtils.DATACENTER_ID_STR, zkInfo.getId());
      zkInfoJson.put(ClusterMapUtils.ZKCONNECT_STR,
          "localhost1:" + zkInfo.getPort() + ClusterMapUtils.ZKCONNECT_STR_DELIMITER + "localhost2:"
              + zkInfo.getPort());
      zkInfosJson.put(zkInfoJson);
    }
    JSONObject jsonObject = new JSONObject().put(ClusterMapUtils.ZKINFO_STR, zkInfosJson);
    Utils.writeJsonObjectToFile(jsonObject, zkLayoutPath);
    try {
      HelixBootstrapUpgradeUtil.dropCluster(zkLayoutPath, CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP,
          dcStr, new HelixAdminFactory());
      fail("should fail because of invalid zk endpoint count");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Tests the method to create instance config and ensures that derived fields are set correctly.
   */
  @Test
  public void testCreateInstanceConfig() {
    DataNode dataNode = (DataNode) ((Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(DEFAULT_PARTITION_CLASS)
        .get(0)).getReplicas().get(0).getDataNodeId();
    Map<String, Set<String>> partitionToInstances = new HashMap<>();
    JSONObject jsonObject = dataNode.toJSONObject();
    jsonObject.put("xid", ClusterMapUtils.DEFAULT_XID);
    ClusterMapConfig clusterMapConfig = testHardwareLayout.clusterMapConfig;
    dataNode = new DataNode(dataNode.getDatacenter(), jsonObject, clusterMapConfig);
    InstanceConfig referenceInstanceConfig =
        HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
            new ConcurrentHashMap<>(), null);
    // Assert that xid field does not get set in InstanceConfig when it is the default.
    assertNull(referenceInstanceConfig.getRecord().getSimpleField(ClusterMapUtils.XID_STR));

    // Assert that xid field does get set if not the default.
    jsonObject.put("xid", "10");
    dataNode = new DataNode(dataNode.getDatacenter(), jsonObject, clusterMapConfig);
    InstanceConfig instanceConfig =
        HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
            new ConcurrentHashMap<>(), null);
    assertEquals("10", instanceConfig.getRecord().getSimpleField(ClusterMapUtils.XID_STR));
    assertThat(referenceInstanceConfig.getRecord(), not(equalTo(instanceConfig.getRecord())));

    referenceInstanceConfig = instanceConfig;

    // Assert that sealed list being different does not affect equality
    List<String> sealedList = Arrays.asList("5", "10");
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedList);
    // set the field to null. The created InstanceConfig should not have null fields.
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, null);
    referenceInstanceConfig.getRecord().setListField(PARTIALLY_SEALED_STR, null);
    instanceConfig = HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
        new ConcurrentHashMap<>(), referenceInstanceConfig);
    // Stopped replicas and partially sealed replicas should be an empty list and not null, so set that in referenceInstanceConfig for comparison.
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, Collections.emptyList());
    referenceInstanceConfig.getRecord().setListField(PARTIALLY_SEALED_STR, Collections.emptyList());
    assertEquals(instanceConfig.getRecord(), referenceInstanceConfig.getRecord());

    // Assert that stopped list being different does not affect equality
    List<String> stoppedReplicas = Arrays.asList("11", "15");
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, stoppedReplicas);
    instanceConfig = HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
        new ConcurrentHashMap<>(), referenceInstanceConfig);
    assertEquals(instanceConfig.getRecord(), referenceInstanceConfig.getRecord());

    // Assert that partially sealed list being different does not affect equality
    List<String> partiallySealedReplicas = Arrays.asList("11", "15");
    referenceInstanceConfig.getRecord().setListField(PARTIALLY_SEALED_STR, partiallySealedReplicas);
    instanceConfig = HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
        new ConcurrentHashMap<>(), referenceInstanceConfig);
    assertEquals(instanceConfig.getRecord(), referenceInstanceConfig.getRecord());
  }

  /**
   * A single test (for convenience) that tests bootstrap and upgrades.
   */
  @Test
  public void testEverything() throws Exception {
    // keep initial data nodes in default Hardware Layout
    List<DataNode> nodesInDefaultHardwareLayout = testHardwareLayout.getAllExistingDataNodes();

    /* Test bootstrap */
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);
    uploadClusterConfigsAndVerify();

    /* Test Simple Upgrade */
    int numNewNodes = 4;
    int numNewPartitions = 220;
    testHardwareLayout.addNewDataNodes(numNewNodes);
    testPartitionLayout.addNewPartitions(numNewPartitions, DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE, null);
    expectedResourceCount += (numNewPartitions - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);

    /* Test sealed state update. */
    Set<Long> partitionIdsBeforeAddition = new HashSet<>();
    for (PartitionId partitionId : testPartitionLayout.getPartitionLayout().getPartitions(null)) {
      partitionIdsBeforeAddition.add(((Partition) partitionId).getId());
    }

    // First, add new nodes and partitions (which are default READ_WRITE)
    numNewNodes = 2;
    numNewPartitions = 50;
    testHardwareLayout.addNewDataNodes(numNewNodes);
    testPartitionLayout.addNewPartitions(numNewPartitions, DEFAULT_PARTITION_CLASS, PartitionState.READ_WRITE, null);

    // Next, mark all previous partitions as READ_ONLY, and change their replica capacities and partition classes.
    for (PartitionId partitionId : testPartitionLayout.getPartitionLayout().getPartitions(null)) {
      if (partitionIdsBeforeAddition.contains(((Partition) partitionId).getId())) {
        Partition partition = (Partition) partitionId;
        partition.partitionState = PartitionState.READ_ONLY;
        partition.replicaCapacityInBytes += 1;
        partition.partitionClass = "specialPartitionClass";
      }
    }

    expectedResourceCount += (numNewPartitions - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);
    uploadClusterConfigsAndVerify();

    // Now, mark the ones that were READ_ONLY as READ_WRITE and vice versa
    for (PartitionId partitionId : testPartitionLayout.getPartitionLayout().getPartitions(null)) {
      Partition partition = (Partition) partitionId;
      if (partitionIdsBeforeAddition.contains(((Partition) partitionId).getId())) {
        partition.partitionState = PartitionState.READ_WRITE;
      } else {
        partition.partitionState = PartitionState.READ_ONLY;
      }
    }
    writeBootstrapOrUpgrade(expectedResourceCount, false);
    uploadClusterConfigsAndVerify();

    // Now, change the replica count for a partition.
    Partition partition1 = (Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .get(RANDOM.nextInt(testPartitionLayout.getPartitionCount()));
    Partition partition2 = (Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .get(RANDOM.nextInt(testPartitionLayout.getPartitionCount()));

    // Add a new replica for partition1. Find a disk on a data node that does not already have a replica for partition1.
    HashSet<DataNodeId> partition1Nodes = new HashSet<>();
    for (ReplicaId replica : partition1.getReplicas()) {
      partition1Nodes.add(replica.getDataNodeId());
    }
    Disk diskForNewReplica;
    do {
      if (partition1.getId() < 100L) {
        // this is to ensure that if partition1 comes from initial partitions [0, 99], we add new replica to the disk
        // residing on the initial nodes in default hardware layout. The reason here is, if new replica was added to a
        // new node (not initially in hardware layout), the replica size of all initial partitions would be reset when
        // first calling writeBootstrapOrUpgrade() at the end of testing. However the new added replica stays unchanged.
        // So when writeBootstrapOrUpgrade() is called second time, validating cluster manager in Helix tool will see
        // inconsistent replica size of same partition and an exception will be thrown which makes test failed.
        DataNode randomNode = nodesInDefaultHardwareLayout.get(RANDOM.nextInt(nodesInDefaultHardwareLayout.size()));
        diskForNewReplica = randomNode.getDisks().get(RANDOM.nextInt(randomNode.getDisks().size()));
      } else {
        diskForNewReplica = testHardwareLayout.getRandomDisk();
      }
    } while (partition1Nodes.contains(diskForNewReplica.getDataNode()));

    partition1.addReplica(new Replica(partition1, diskForNewReplica, testHardwareLayout.clusterMapConfig));
    // Remove a replica from partition2.
    partition2.getReplicas().remove(0);
    writeBootstrapOrUpgrade(expectedResourceCount, false);

    long expectedResourceCountWithoutRemovals = expectedResourceCount;
    /* Test instance, partition and resource removal */
    // Use the initial static clustermap that does not have the upgrades.
    testHardwareLayout = constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP);
    testPartitionLayout =
        constructInitialPartitionLayoutJSON(testHardwareLayout, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, null);
    long expectedResourceCountWithRemovals =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCountWithoutRemovals, false);
    writeBootstrapOrUpgrade(expectedResourceCountWithRemovals, true);
  }

  /**
   * Test adding partitions and instances in FULL_AUTO compatible way.
   * @throws Exception
   */
  @Test
  public void testAddNewPartitionsAndDataNodesBeingFullAutoCompatible() throws Exception {
    Map<String, Map<String, Set<String>>> instancesUnderResourcesInDc = new HashMap<>();
    Map<String, Map<String, Set<Integer>>> partitionsUnderResourcesInDc = new HashMap<>();
    int basePort = 18088;
    int basePartitionId = 0;
    int dataNode = 6;
    int numPartition = 200;
    testHardwareLayout = constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP, dataNode, basePort);
    testPartitionLayout = constructInitialPartitionLayoutJSON(testHardwareLayout, numPartition, null);
    testHardwareLayout.getHardwareLayout().getDatacenters().forEach(dc -> {
      instancesUnderResourcesInDc.put(dc.getName(), new HashMap<>());
      partitionsUnderResourcesInDc.put(dc.getName(), new HashMap<>());
    });

    // Test 1: Add initial partition to helix
    long expectedResourceCount = 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false, dataNode);
    addExpectedPartitionAndInstanceToResource(testPartitionLayout, FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER,
        instancesUnderResourcesInDc, partitionsUnderResourcesInDc);
    verifyResourceInFullAuto(testHardwareLayout.getHardwareLayout(), instancesUnderResourcesInDc,
        partitionsUnderResourcesInDc);

    // Test 2: Add a new group of partitions and instances.
    basePort += dataNode * 2; // it has two datacenter
    basePartitionId += numPartition;
    TestHardwareLayout secondTestHardwareLayout =
        constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP, dataNode, basePort);
    TestPartitionLayout secondTestPartitionLayout =
        constructInitialPartitionLayoutJSON(secondTestHardwareLayout, numPartition, null, basePartitionId);

    // This two layouts have different set of datanodes, and different set of partitions. Merge this two layouts, and
    // we should create two different resources for these two layouts
    testPartitionLayout.merge(secondTestPartitionLayout);
    expectedResourceCount = 2;
    writeBootstrapOrUpgrade(expectedResourceCount, false, dataNode);
    addExpectedPartitionAndInstanceToResource(secondTestPartitionLayout,
        FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER + 1, instancesUnderResourcesInDc, partitionsUnderResourcesInDc);
    verifyResourceInFullAuto(testHardwareLayout.getHardwareLayout(), instancesUnderResourcesInDc,
        partitionsUnderResourcesInDc);

    // Test 3: Create another set of data nodes and partitions, but this time, only half the size as last time.
    basePort += dataNode * 2; // it has two datacenter
    basePartitionId += numPartition;
    TestHardwareLayout thirdTestHardwareLayout =
        constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP, dataNode / 2, basePort);
    TestPartitionLayout thirdTestPartitionLayout =
        constructInitialPartitionLayoutJSON(thirdTestHardwareLayout, numPartition / 2, null, basePartitionId);

    testPartitionLayout.merge(thirdTestPartitionLayout);
    expectedResourceCount = 3;
    writeBootstrapOrUpgrade(expectedResourceCount, false, dataNode);
    addExpectedPartitionAndInstanceToResource(thirdTestPartitionLayout,
        FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER + 2, instancesUnderResourcesInDc, partitionsUnderResourcesInDc);
    verifyResourceInFullAuto(testHardwareLayout.getHardwareLayout(), instancesUnderResourcesInDc,
        partitionsUnderResourcesInDc);

    // Test 4: Create another set of data nodes with only the half size, this would be merged in the latest resource
    basePort += dataNode; // it has two datacenter
    basePartitionId += numPartition / 2;
    TestHardwareLayout fourthTestHardwareLayout =
        constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP, dataNode / 2, basePort);
    TestPartitionLayout fourthTestPartitionLayout =
        constructInitialPartitionLayoutJSON(fourthTestHardwareLayout, numPartition / 2, null, basePartitionId);
    testPartitionLayout.merge(fourthTestPartitionLayout);
    expectedResourceCount = 3; // still expecting 3 resource, the latest resource would merge third and four layout
    writeBootstrapOrUpgrade(expectedResourceCount, false, dataNode);
    addExpectedPartitionAndInstanceToResource(fourthTestPartitionLayout,
        FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER + 2, instancesUnderResourcesInDc, partitionsUnderResourcesInDc);
    verifyResourceInFullAuto(testHardwareLayout.getHardwareLayout(), instancesUnderResourcesInDc,
        partitionsUnderResourcesInDc);

    // Test 5: Adding a new partition from third and four layout
    basePartitionId += numPartition / 2;
    List<DataNode> dataNodesInThird = thirdTestHardwareLayout.getIndependentDataNodes(3);
    List<DataNode> dataNodesInFourth = fourthTestHardwareLayout.getIndependentDataNodes(3);
    assertEquals(dataNodesInFourth.size(), dataNodesInThird.size());
    List<DataNode> finalDataNodes = new ArrayList<>(dataNodesInFourth.size());
    for (int i = 0; i < dataNodesInFourth.size(); i++) {
      finalDataNodes.add(i % 2 == 0 ? dataNodesInThird.get(i) : dataNodesInFourth.get(i));
    }

    testPartitionLayout.addNewPartition(testHardwareLayout, finalDataNodes, DEFAULT_PARTITION_CLASS);
    writeBootstrapOrUpgrade(expectedResourceCount, false, dataNode);
    for (Datacenter dc : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      String resourceName = String.valueOf(FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER + 2);
      partitionsUnderResourcesInDc.get(dc.getName()).get(resourceName).add(testPartitionLayout.getPartitionCount()-1);
    }
    verifyResourceInFullAuto(testHardwareLayout.getHardwareLayout(), instancesUnderResourcesInDc,
        partitionsUnderResourcesInDc);

    // Test 6: Add several groups together
    basePort += dataNode;
    basePartitionId += 1;
    int numGroups = 4;
    for (int i = 0; i < numGroups; i++) {
      int numNewDataNode = i == numGroups - 1 ? dataNode / 2 : dataNode;
      int numNewPartition = i == numGroups - 1 ? numPartition / 2 : numPartition;
      TestHardwareLayout newHardwareLayout =
          constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP, numNewDataNode, basePort);
      TestPartitionLayout newPartitionLayout =
          constructInitialPartitionLayoutJSON(newHardwareLayout, numNewPartition, null, basePartitionId);
      testPartitionLayout.merge(newPartitionLayout);
      addExpectedPartitionAndInstanceToResource(newPartitionLayout,
          FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER + 3 + i, instancesUnderResourcesInDc,
          partitionsUnderResourcesInDc);
      basePort += numNewDataNode * 2;
      basePartitionId += numNewPartition;
    }
    expectedResourceCount += 4;
    writeBootstrapOrUpgrade(expectedResourceCount, false, dataNode);
    verifyResourceInFullAuto(testHardwareLayout.getHardwareLayout(), instancesUnderResourcesInDc,
        partitionsUnderResourcesInDc);
  }

  /**
   * Test that new replicas can be correctly extracted from the diff between static clustermap and clustermap in Helix.
   * Verify that new added replica infos are correct (i.e. partition class, replica size, data node and mount path)
   * Also, this method tests replica addition config can be deleted correctly.
   * @throws Exception
   */
  @Test
  public void testReplicaAdditionConfigUploadAndDelete() throws Exception {
    // Test bootstrap
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);

    // Now, change the replica count for two partitions.
    Partition partition1 = (Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .get(RANDOM.nextInt(testPartitionLayout.getPartitionCount()));
    Partition partition2 = (Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .get(RANDOM.nextInt(testPartitionLayout.getPartitionCount()));

    // Add a new replica for partition1. Find a disk on a data node that does not already have a replica for partition1.
    HashSet<DataNodeId> partition1Nodes = new HashSet<>();
    for (ReplicaId replica : partition1.getReplicas()) {
      partition1Nodes.add(replica.getDataNodeId());
    }
    Disk diskForNewReplica;
    do {
      diskForNewReplica = testHardwareLayout.getRandomDisk();
    } while (partition1Nodes.contains(diskForNewReplica.getDataNode()));
    // Add new replica into partition1
    partition1.addReplica(new Replica(partition1, diskForNewReplica, testHardwareLayout.clusterMapConfig));
    String dcNameForNewReplica = diskForNewReplica.getDataNode().getDatacenterName();
    // Remove a replica from partition2.
    partition2.getReplicas().remove(0);

    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // test invalid admin type
    try {
      HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
          CLUSTER_NAME_PREFIX, dcStr, false, new String[]{"invalid_admin_type"}, null);
      fail("should fail because of invalid admin type");
    } catch (IllegalArgumentException e) {
      // expected
    }
    // upload replica addition admin config
    HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, false, new String[]{ClusterMapUtils.REPLICA_ADDITION_STR}, null);
    // verify replica addition znode in Helix PropertyStore
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      HelixPropertyStore<ZNRecord> propertyStore =
          CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig,
              Collections.singletonList(propertyStoreConfig.rootPath));
      ZNRecord zNRecord = propertyStore.get(ClusterMapUtils.REPLICA_ADDITION_ZNODE_PATH, null, AccessOption.PERSISTENT);
      if (!activeDcSet.contains(zkInfo.getDcName())) {
        // if data center is not enabled, no admin config should be uploaded to Helix.
        assertNull(zNRecord);
      } else if (!zkInfo.getDcName().equals(dcNameForNewReplica)) {
        // if data center doesn't equal to
        Map<String, Map<String, String>> replicaAddition = zNRecord.getMapFields();
        assertTrue(replicaAddition.isEmpty());
      } else {
        assertNotNull(zNRecord);
        Map<String, Map<String, String>> replicaAddition = zNRecord.getMapFields();
        assertEquals("There should be only one partition in new replica map", 1, replicaAddition.size());
        assertNotNull("Partition id in new replica map is not expected",
            replicaAddition.get(partition1.toPathString()));
        Map<String, String> newReplicaMap = replicaAddition.get(partition1.toPathString());
        assertEquals("Partition class is not expected", partition1.getPartitionClass(),
            newReplicaMap.get(ClusterMapUtils.PARTITION_CLASS_STR));
        assertEquals("Replica capacity is not expected", String.valueOf(partition1.getReplicaCapacityInBytes()),
            newReplicaMap.get(ClusterMapUtils.REPLICAS_CAPACITY_STR));
        String instanceName =
            diskForNewReplica.getDataNode().getHostname() + "_" + diskForNewReplica.getDataNode().getPort();
        assertTrue("Instance name doesn't exist", newReplicaMap.containsKey(instanceName));
        String[] mountPathAndDiskCapacity = newReplicaMap.get(instanceName).split(DISK_CAPACITY_DELIM_STR);
        String mountPath = mountPathAndDiskCapacity[0];
        String diskCapacity = mountPathAndDiskCapacity[1];
        assertEquals("Mount path is not expected", diskForNewReplica.getMountPath(), mountPath);
        assertEquals("Disk capacity is not expected", diskForNewReplica.getRawCapacityInBytes(),
            Long.parseLong(diskCapacity));
      }
    }
    // test deleting replica addition config (failure case)
    try {
      HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
          CLUSTER_NAME_PREFIX, dcStr, true, new String[]{"invalid_admin_type"}, null);
      fail("should fail because of invalid admin type");
    } catch (IllegalArgumentException e) {
      // expected
    }
    // test deleting replica addition config (success case)
    HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, true, new String[]{ClusterMapUtils.REPLICA_ADDITION_STR}, null);
    // verify config no longer exists
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      HelixPropertyStore<ZNRecord> propertyStore =
          CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig,
              Collections.singletonList(propertyStoreConfig.rootPath));
      ZNRecord zNRecord = propertyStore.get(ClusterMapUtils.REPLICA_ADDITION_ZNODE_PATH, null, AccessOption.PERSISTENT);
      assertNull("ZNode associated with admin config should not exist", zNRecord);
    }
  }

  /**
   * Test that when AdminOperation is specified to UpdateIdealState, Helix bootstrap tool updates IdealState only without
   * changing InstanceConfig.
   */
  @Test
  public void testUpdateIdealStateAdminOp() throws Exception {
    String clusterName = CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
    // Test regular bootstrap. This is to ensure InstanceConfig and IdealState are there before testing changing
    // IdealState (to trigger replica movement)
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);

    // Now, change the replica count for two partitions.
    int totalPartitionCount = testPartitionLayout.getPartitionCount();
    int firstPartitionIndex = RANDOM.nextInt(totalPartitionCount);
    int secondPartitionIndex = (firstPartitionIndex + 1) % totalPartitionCount;
    List<PartitionId> allPartitions = testPartitionLayout.getPartitionLayout().getPartitions(null);
    Partition partition1 = (Partition) allPartitions.get(firstPartitionIndex);
    Partition partition2 = (Partition) allPartitions.get(secondPartitionIndex);

    // Add a new replica for partition1. Find a disk on a data node that does not already have a replica for partition1.
    HashSet<DataNodeId> partition1Nodes = new HashSet<>();
    for (ReplicaId replica : partition1.getReplicas()) {
      partition1Nodes.add(replica.getDataNodeId());
    }
    Disk diskForNewReplica;
    do {
      diskForNewReplica = testHardwareLayout.getRandomDisk();
    } while (partition1Nodes.contains(diskForNewReplica.getDataNode()) || !diskForNewReplica.getDataNode()
        .getDatacenterName()
        .equals("DC1"));
    // Add new replica into partition1
    ReplicaId replicaToAdd = new Replica(partition1, diskForNewReplica, testHardwareLayout.clusterMapConfig);
    partition1.addReplica(replicaToAdd);
    // Remove a replica from partition2.
    ReplicaId removedReplica = partition2.getReplicas().remove(0);

    String dcName = replicaToAdd.getDataNodeId().getDatacenterName();
    ZkInfo zkInfo = dcsToZkInfo.get(dcName);
    ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, getInstanceName(replicaToAdd.getDataNodeId()));
    // deep copy for subsequent verification
    InstanceConfig previousInstanceConfig = new InstanceConfig(instanceConfig.getRecord());
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // upgrade Helix by updating IdealState: AdminOperation = UpdateIdealState
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, new HelixAdminFactory(), false,
        ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, UpdateIdealState, dataNodeConfigSourceType, false, 0);
    verifyResourceCount(testHardwareLayout.getHardwareLayout(), expectedResourceCount);

    // verify IdealState has been updated
    // 1. new added replica is indeed present in the IdealState
    verifyIdealStateForPartition(replicaToAdd, true, 4, expectedResourceCount);
    // 2. removed old replica is no longer present in the IdealState
    verifyIdealStateForPartition(removedReplica, false, 2, expectedResourceCount);

    // verify the InstanceConfig stays unchanged
    InstanceConfig currentInstanceConfig =
        admin.getInstanceConfig(clusterName, getInstanceName(replicaToAdd.getDataNodeId()));
    assertEquals("InstanceConfig should stay unchanged", previousInstanceConfig.getRecord(),
        currentInstanceConfig.getRecord());
  }

  /**
   * Test when AdminOperation is specified to DisablePartition, Helix bootstrap tool is able to disable certain partition
   * only without changing IdealState and InstanceConfig. (In practice, this is first step to decommission a replica)
   * @throws Exception
   */
  @Test
  public void testDisablePartitionAdminOp() throws Exception {
    String clusterName = CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
    // Test regular bootstrap. This is to ensure DataNodeConfig and IdealState are there before testing disabling
    // certain replica on specific node.
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);
    int totalPartitionCount = testPartitionLayout.getPartitionCount();
    // Randomly pick a partition to remove one of its replicas
    Partition testPartition = (Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .get(RANDOM.nextInt(totalPartitionCount));

    ReplicaId removedReplica = testPartition.getReplicaIds()
        .stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals("DC1"))
        .findFirst()
        .get();
    testPartition.getReplicas().remove(removedReplica);

    ZkInfo zkInfo = dcsToZkInfo.get(removedReplica.getDataNodeId().getDatacenterName());
    // create a participant that hosts this removed replica
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.port", String.valueOf(removedReplica.getDataNodeId().getPort()));
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.update.datanode.info", Boolean.toString(true));
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    props.setProperty("clustermap.retry.disable.partition.completion.backoff.ms", Integer.toString(100));
    props.setProperty("clustermap.data.node.config.source.type", dataNodeConfigSourceType.name());
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixParticipant helixParticipant = new HelixParticipant(clusterMapConfig, new HelixFactory(), new MetricRegistry(),
        "localhost:" + zkInfo.getPort(), true);
    PropertyStoreToDataNodeConfigAdapter propertyStoreAdapter =
        dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG ? null
            : new PropertyStoreToDataNodeConfigAdapter("localhost:" + zkInfo.getPort(), clusterMapConfig);
    InstanceConfigToDataNodeConfigAdapter.Converter instanceConfigConverter =
        new InstanceConfigToDataNodeConfigAdapter.Converter(clusterMapConfig);
    // create HelixAdmin
    ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
    // Write changes to static files
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // make bootstrap tool blocked by count down latch before removing znodes for disabling partitions
    blockRemovingNodeLatch = new CountDownLatch(1);
    disablePartitionLatch = new CountDownLatch(activeDcSet.size());
    CountDownLatch bootstrapCompletionLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      try {
        // Upgrade Helix by updating IdealState: AdminOperation = DisablePartition
        HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
            CLUSTER_NAME_PREFIX, dcStr, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, new HelixAdminFactory(),
            false, ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, DisablePartition, dataNodeConfigSourceType, false, 0);
        bootstrapCompletionLatch.countDown();
      } catch (Exception e) {
        // do nothing, if there is any exception subsequent test should fail.
      }
    }, false).start();
    assertTrue("Disable partition latch didn't come down within 5 seconds",
        disablePartitionLatch.await(5, TimeUnit.SECONDS));
    // Let's attempt to update InstanceConfig/DataNodeConfig via HelixParticipant, which should be blocked
    CountDownLatch updateCompletionLatch = new CountDownLatch(1);
    Utils.newThread(() -> {
      helixParticipant.updateDataNodeInfoInCluster(removedReplica, false);
      updateCompletionLatch.countDown();
    }, false).start();
    // sleep 100 ms to ensure updateDataNodeInfoInCluster is blocked due to disabling partition hasn't completed yet
    Thread.sleep(100);
    // Ensure the DataNodeConfig still has the replica
    String instanceName = getInstanceName(removedReplica.getDataNodeId());
    DataNodeConfig currentDataNodeConfig =
        dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG ? instanceConfigConverter.convert(
            admin.getInstanceConfig(clusterName, instanceName)) : propertyStoreAdapter.get(instanceName);
    verifyReplicaInfoInDataNodeConfig(currentDataNodeConfig, removedReplica, true);

    // verify the znode is created for the node on which partition has been disabled.
    Properties properties = new Properties();
    properties.setProperty("helix.property.store.root.path", "/" + clusterName + "/" + PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(properties));
    HelixPropertyStore<ZNRecord> helixPropertyStore =
        CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig, null);
    String path = PARTITION_DISABLED_ZNODE_PATH + getInstanceName(removedReplica.getDataNodeId());
    assertTrue("ZNode is not found for disabled partition node.",
        helixPropertyStore.exists(path, AccessOption.PERSISTENT));
    helixPropertyStore.stop();

    // unblock HelixBootstrapTool
    blockRemovingNodeLatch.countDown();
    // waiting for bootstrap tool to complete
    assertTrue("Helix tool didn't complete within 5 seconds", bootstrapCompletionLatch.await(5, TimeUnit.SECONDS));
    verifyResourceCount(testHardwareLayout.getHardwareLayout(), expectedResourceCount);
    assertTrue("Helix participant didn't complete update within 5 seconds",
        updateCompletionLatch.await(5, TimeUnit.SECONDS));
    InstanceConfig currentInstanceConfig =
        admin.getInstanceConfig(clusterName, getInstanceName(removedReplica.getDataNodeId()));
    // Verify that replica has been disabled
    String resourceName = null;
    for (String rs : admin.getResourcesInCluster(clusterName)) {
      IdealState is = admin.getResourceIdealState(clusterName, rs);
      if (is.getPartitionSet().contains(removedReplica.getPartitionId().toPathString())) {
        resourceName = rs;
        break;
      }
    }
    List<String> disabledPartition = currentInstanceConfig.getDisabledPartitions(resourceName);
    assertEquals("Disabled partition not as expected",
        Collections.singletonList(removedReplica.getPartitionId().toPathString()), disabledPartition);
    // Verify that IdealState has no change
    verifyIdealStateForPartition(removedReplica, true, 3, expectedResourceCount);
    // Verify the InstanceConfig is changed in MapFields (Disabled partitions are added to this field, also the replica entry has been removed)
    String disabledPartitionStr = currentInstanceConfig.getRecord()
        .getMapFields()
        .keySet()
        .stream()
        .filter(k -> !k.startsWith("/mnt"))
        .findFirst()
        .get();
    // Verify the disabled partition string contains correct partition
    Map<String, String> expectedDisabledPartitionMap = new HashMap<>();
    expectedDisabledPartitionMap.put(resourceName, removedReplica.getPartitionId().toPathString());

    assertEquals("Mismatch in disabled partition string in InstanceConfig", expectedDisabledPartitionMap,
        currentInstanceConfig.getRecord().getMapField(disabledPartitionStr));
    // verify the removed replica is no longer in InstanceConfig
    currentDataNodeConfig =
        dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG ? instanceConfigConverter.convert(
            admin.getInstanceConfig(clusterName, instanceName)) : propertyStoreAdapter.get(instanceName);
    verifyReplicaInfoInDataNodeConfig(currentDataNodeConfig, removedReplica, false);
    if (propertyStoreAdapter != null) {
      propertyStoreAdapter.close();
    }
  }

  /**
   * Test that partition is correctly enabled on given node. The partition is first disabled and then enabled.
   * @throws Exception
   */
  @Test
  public void testEnablePartitionAdminOp() throws Exception {
    assumeTrue(!dcStr.equals("DC1") && !dcStr.equals("DC0"));
    String clusterName = CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
    // Test regular bootstrap.
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);
    int totalPartitionCount = testPartitionLayout.getPartitionCount();
    // Randomly pick a partition to disable/enable
    Partition testPartition = (Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .get(RANDOM.nextInt(totalPartitionCount));
    // Randomly pick a replica from this partition
    List<ReplicaId> replicaIds = testPartition.getReplicaIds();
    DataNodeId dataNodeId = replicaIds.get(RANDOM.nextInt(replicaIds.size())).getDataNodeId();
    // Disable partition on chosen node
    HelixBootstrapUpgradeUtil.controlPartitionState(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dataNodeId.getDatacenterName(), dataNodeId.getHostname(), dataNodeId.getPort(),
        DisablePartition, testPartition.toPathString());
    // Verify the InstanceConfig is changed only in MapFields (Disabled partition is added to this field)
    ZkInfo zkInfo = dcsToZkInfo.get(dataNodeId.getDatacenterName());
    ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
    InstanceConfig currentInstanceConfig = admin.getInstanceConfig(clusterName, getInstanceName(dataNodeId));
    assertTrue("There should be additional string in InstanceConfig due to disabling partition",
        currentInstanceConfig.getRecord().getMapFields().keySet().stream().anyMatch(k -> !k.startsWith("/mnt")));
    // Verify given partition is indeed disabled on specified node
    String resourceName = getResourceNameOfPartition(admin, clusterName, testPartition.toPathString());
    List<String> disabledPartitions = currentInstanceConfig.getDisabledPartitions(resourceName);
    assertEquals("Disabled partition is not expected", Collections.singletonList(testPartition.toPathString()),
        disabledPartitions);
    // Enable the same partition on same node
    HelixBootstrapUpgradeUtil.controlPartitionState(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dataNodeId.getDatacenterName(), dataNodeId.getHostname(), dataNodeId.getPort(),
        EnablePartition, testPartition.toPathString());
    // Verify instanceConfig has been updated (disabled partition is removed)
    currentInstanceConfig = admin.getInstanceConfig(clusterName, getInstanceName(dataNodeId));
    assertFalse("There shouldn't be any additional string in InstanceConfig",
        currentInstanceConfig.getRecord().getMapFields().keySet().stream().anyMatch(k -> !k.startsWith("/mnt")));
    // Verify there is no disabled partition
    assertNull("There shouldn't be any disabled partition", currentInstanceConfig.getDisabledPartitions(resourceName));
  }

  /**
   * Test resetting partition on given node. For now, we only test the code path which would throw exception as node is
   * not alive.
   * @throws Exception
   */
  @Test
  public void testResetPartitionAdminOp() throws Exception {
    assumeTrue(!dcStr.equals("DC1") && !dcStr.equals("DC0"));
    // Test regular bootstrap.
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);
    int totalPartitionCount = testPartitionLayout.getPartitionCount();
    // Randomly pick a partition to disable/enable
    Partition testPartition = (Partition) testPartitionLayout.getPartitionLayout()
        .getPartitions(null)
        .get(RANDOM.nextInt(totalPartitionCount));
    // Randomly pick a replica from this partition
    List<ReplicaId> replicaIds = testPartition.getReplicaIds();
    DataNodeId dataNodeId = replicaIds.get(RANDOM.nextInt(replicaIds.size())).getDataNodeId();
    // Reset partition on chosen node which should throw exception as the node hasn't participated
    try {
      HelixBootstrapUpgradeUtil.controlPartitionState(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
          CLUSTER_NAME_PREFIX, dataNodeId.getDatacenterName(), dataNodeId.getHostname(), dataNodeId.getPort(),
          ResetPartition, testPartition.toPathString());
      fail("should fail on resetting partition");
    } catch (HelixException e) {
      // expected
    }
  }

  /**
   * Test listing sealed partitions in Helix cluster.
   * @throws Exception
   */
  @Test
  public void testListSealedPartitions() throws Exception {
    assumeTrue(!dcStr.equals("DC1"));
    String clusterName = CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
    ClusterMapConfig clusterMapConfig = getClusterMapConfig(clusterName, "DC1", null);
    InstanceConfigToDataNodeConfigAdapter.Converter instanceConfigConverter =
        new InstanceConfigToDataNodeConfigAdapter.Converter(clusterMapConfig);
    // Test regular bootstrap
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);
    Set<String> sealedPartitions =
        HelixBootstrapUpgradeUtil.listSealedPartition(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
            CLUSTER_NAME_PREFIX, dcStr, dataNodeConfigSourceType);
    assertEquals("Sealed partition set should be empty initially", Collections.emptySet(), sealedPartitions);
    // randomly choose 20 partitions to mark as sealed
    int[] intArray = new Random().ints(0, 100).distinct().limit(20).toArray();
    Set<String> selectedSealedPartitionSet = new HashSet<>();
    for (int id : intArray) {
      selectedSealedPartitionSet.add(String.valueOf(id));
    }
    // update the sealed lists in Helix
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
      PropertyStoreToDataNodeConfigAdapter propertyStoreAdapter =
          dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG ? null
              : new PropertyStoreToDataNodeConfigAdapter("localhost:" + zkInfo.getPort(), clusterMapConfig);
      for (String instanceName : admin.getInstancesInCluster(clusterName)) {
        DataNodeConfig dataNodeConfig =
            dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG ? instanceConfigConverter.convert(
                admin.getInstanceConfig(clusterName, instanceName)) : propertyStoreAdapter.get(instanceName);
        Set<String> localReplicas = new HashSet<>();
        for (DataNodeConfig.DiskConfig diskConfig : dataNodeConfig.getDiskConfigs().values()) {
          localReplicas.addAll(diskConfig.getReplicaConfigs().keySet());
        }
        // derive the intersection of localReplicas set and selectedSealedPartitionSet
        localReplicas.retainAll(selectedSealedPartitionSet);
        if (dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG) {
          InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
          instanceConfig.getRecord().setListField(SEALED_STR, new ArrayList<>(localReplicas));
          admin.setInstanceConfig(clusterName, instanceName, instanceConfig);
        } else {
          dataNodeConfig.getSealedReplicas().addAll(localReplicas);
          propertyStoreAdapter.set(dataNodeConfig);
        }
      }
    }
    // query sealed partition in Helix again
    sealedPartitions =
        HelixBootstrapUpgradeUtil.listSealedPartition(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
            CLUSTER_NAME_PREFIX, dcStr, dataNodeConfigSourceType);
    assertEquals("Mismatch in sealed partition set", selectedSealedPartitionSet, sealedPartitions);
  }

  /**
   * Test migrating instance configs to HelixPropertyStore
   */
  @Test
  public void testMigrateToPropertyStore() throws Exception {
    // This test is tested only when DataNodeConfigSourceType.INSTANCE_CONFIG. The reason is: PROPERTY_STORE source type
    // already writes dataNodeConfigs to Helix PropertyStore when bootstrapping cluster and creates empty InstanceConfig
    // for each node. Once copied from InstanceConfig, the dataNodeConfig will be overridden to empty.
    assumeTrue(dcStr.equals("all") && dataNodeConfigSourceType == DataNodeConfigSourceType.INSTANCE_CONFIG);
    // build a cluster first with metadata in the instance configs
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount, false);

    // copy the configs
    HelixBootstrapUpgradeUtil.migrateToPropertyStore(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr);

    // verify that they are present in the property store and that the basic values match expectations
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      String dcName = zkInfo.getDcName();
      try (DataNodeConfigSource source = new PropertyStoreToDataNodeConfigAdapter("localhost:" + zkInfo.getPort(),
          HelixBootstrapUpgradeUtil.getClusterMapConfig(CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP,
              dcName, null))) {
        Map<DataNodeId, Set<String>> dataNodeToReplicas = testPartitionLayout.getPartitionLayout()
            .getPartitions(null)
            .stream()
            .flatMap(partition -> partition.getReplicaIds().stream())
            .collect(Collectors.groupingBy(ReplicaId::getDataNodeId,
                Collectors.mapping(replica -> replica.getPartitionId().toPathString(), Collectors.toSet())));
        List<DataNode> expectedNodes = testHardwareLayout.getAllDataNodesFromDc(zkInfo.getDcName());
        for (DataNode expectedNode : expectedNodes) {
          String instanceName = getInstanceName(expectedNode);
          DataNodeConfig config = source.get(instanceName);
          assertNotNull("Config for " + instanceName + " not found", config);
          assertEquals("Unexpected instance name", instanceName, config.getInstanceName());
          assertEquals("Unexpected host", expectedNode.getHostname(), config.getHostName());
          assertEquals("Unexpected port", expectedNode.getPort(), config.getPort());
          Set<String> expectedReplicas = dataNodeToReplicas.get(expectedNode);
          Set<String> replicasInConfig = config.getDiskConfigs()
              .values()
              .stream()
              .flatMap(diskConfig -> diskConfig.getReplicaConfigs().keySet().stream())
              .collect(Collectors.toSet());
          assertEquals("Unexpected partitions in config", expectedReplicas, replicasInConfig);
        }
      }
    }
  }

  /**
   * Test that bootstrap tool is able to generate partition override map from static file (if adminConfigFilePath is not
   * null). It tests both success and failure cases.
   * @throws Exception
   */
  @Test
  public void testGeneratePartitionOverrideMapFromStaticFile() throws Exception {
    List<PartitionId> writablePartitions =
        new ArrayList<>(testPartitionLayout.getPartitionLayout().getWritablePartitions(null));
    int partitionCount = testPartitionLayout.getPartitionCount();
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    Utils.writeStringToFile(String.valueOf(partitionCount + 1), adminConfigFilePath);
    // failure case 1: partition id is out of valid range
    try {
      HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
          CLUSTER_NAME_PREFIX, dcStr, false, new String[]{ClusterMapUtils.PARTITION_OVERRIDE_STR}, adminConfigFilePath);
      fail("should fail because input partition id is out of valid range");
    } catch (IllegalArgumentException e) {
      // expected
    }
    // failure case 2: partition id is non-numeric
    Utils.writeStringToFile("non-numeric", adminConfigFilePath);
    try {
      HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
          CLUSTER_NAME_PREFIX, dcStr, false, new String[]{ClusterMapUtils.PARTITION_OVERRIDE_STR}, adminConfigFilePath);
      fail("should fail because input partition id is not numeric");
    } catch (NumberFormatException e) {
      // expected
    }
    // success case: mark 1/10 partitions as RO
    Collections.shuffle(writablePartitions);
    int readOnlyCount = writablePartitions.size() / 10;
    StringBuilder sb = new StringBuilder();
    Set<String> readOnlyInFile = new HashSet<>();
    for (int i = 0; i < readOnlyCount; ++i) {
      sb.append(writablePartitions.get(i).toPathString()).append(",");
      readOnlyInFile.add(writablePartitions.get(i).toPathString());
    }
    Utils.writeStringToFile(sb.toString(), adminConfigFilePath);
    HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, false, new String[]{ClusterMapUtils.PARTITION_OVERRIDE_STR}, adminConfigFilePath);
    // verify overridden partitions in Helix
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      HelixPropertyStore<ZNRecord> propertyStore =
          CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig,
              Collections.singletonList(propertyStoreConfig.rootPath));
      ZNRecord zNRecord =
          propertyStore.get(ClusterMapUtils.PARTITION_OVERRIDE_ZNODE_PATH, null, AccessOption.PERSISTENT);
      if (!activeDcSet.contains(zkInfo.getDcName())) {
        assertNull(zNRecord);
      } else {
        assertNotNull(zNRecord);
        Map<String, Map<String, String>> overridePartition = zNRecord.getMapFields();
        Set<String> readOnlyInDc = new HashSet<>();
        for (Map.Entry<String, Map<String, String>> entry : overridePartition.entrySet()) {
          if (entry.getValue().get(ClusterMapUtils.PARTITION_STATE).equals(ClusterMapUtils.READ_ONLY_STR)) {
            readOnlyInDc.add(entry.getKey());
          }
        }
        // Verify ReadOnly partitions in DC match that in static file
        assertEquals("Mismatch in ReadOnly partitions for static file and propertyStore", readOnlyInFile, readOnlyInDc);
      }
    }
  }

  /***
   * A helper method to verify IdealState for given replica's partition. It checks number of replicas (represented by
   * instances) under certain partition and verifies new added replica exists or removed old replica is no longer present.
   * @param replica the replica to check if it exists (could be either added replica or removed replica)
   * @param shouldExist if {@code true}, it means the given replica is newly added and should exist in IdealState.
   *                    {@code false} otherwise.
   * @param expectedReplicaCountForPartition expected number of replicas under certain partition
   * @param expectedResourceCount expected total number resource count in this cluster.
   */
  private void verifyIdealStateForPartition(ReplicaId replica, boolean shouldExist,
      int expectedReplicaCountForPartition, long expectedResourceCount) {
    String dcName = replica.getDataNodeId().getDatacenterName();
    ZkInfo zkInfo = dcsToZkInfo.get(dcName);
    String clusterName = CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
    ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
    if (!activeDcSet.contains(dcName)) {
      Assert.assertFalse("Cluster should not be present, as dc " + dcName + " is not enabled",
          admin.getClusters().contains(clusterName));
    } else {
      List<String> resources = admin.getResourcesInCluster(clusterName);
      assertEquals("Resource count mismatch", expectedResourceCount, resources.size());
      String partitionName = replica.getPartitionId().toPathString();
      boolean resourceFound = false;
      for (String resource : resources) {
        IdealState idealState = admin.getResourceIdealState(clusterName, resource);
        if (idealState.getPartitionSet().contains(partitionName)) {
          resourceFound = true;
          Set<String> instances = idealState.getInstanceSet(partitionName);
          assertEquals("Replica number is not expected", expectedReplicaCountForPartition, instances.size());
          String instanceNameOfNewReplica = getInstanceName(replica.getDataNodeId());
          if (shouldExist) {
            assertTrue("Instance set doesn't include new instance that holds added replica",
                instances.contains(instanceNameOfNewReplica));
          } else {
            assertFalse("Instance that holds deleted replica should not exist in the set",
                instances.contains(instanceNameOfNewReplica));
          }
          break;
        }
      }
      assertTrue("No corresponding resource found for partition " + partitionName, resourceFound);
    }
  }

  /**
   * Write the layout files out from the constructed in-memory hardware and partition layouts; use the bootstrap tool
   * to update the contents in Helix; verify that the information is consistent between the two.
   * @param expectedResourceCount number of resources expected in Helix for this cluster in each datacenter.
   * @param forceRemove whether the forceRemove option should be passed when doing the bootstrap/upgrade.
   * @throws IOException if a file read error is encountered.
   * @throws JSONException if a JSON parse error is encountered.
   */
  private void writeBootstrapOrUpgrade(long expectedResourceCount, boolean forceRemove) throws Exception {
    writeBootstrapOrUpgrade(expectedResourceCount, forceRemove, 0);
  }

  /**
   * Write the layout files out from the constructed in-memory hardware and partition layouts; use the bootstrap tool
   * to update the contents in Helix; verify that the information is consistent between the two.
   * @param expectedResourceCount number of resources expected in Helix for this cluster in each datacenter.
   * @param forceRemove whether the forceRemove option should be passed when doing the bootstrap/upgrade.
   * @param maxInstancesInOneResource the maximum number of data nodes to place under a resource, this is only meaningful
   *                                  when the resources are constructed to be full auto compatible.
   * @throws IOException if a file read error is encountered.
   * @throws JSONException if a JSON parse error is encountered.
   */
  private void writeBootstrapOrUpgrade(long expectedResourceCount, boolean forceRemove, int maxInstancesInOneResource)
      throws Exception {
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // This updates and verifies that the information in Helix is consistent with the one in the static cluster map.
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, forceRemove, new HelixAdminFactory(),
        false, ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, BootstrapCluster, dataNodeConfigSourceType, false,
        maxInstancesInOneResource);
    verifyResourceCount(testHardwareLayout.getHardwareLayout(), expectedResourceCount);
  }

  /**
   * Write the layout files out from the constructed in-memory hardware and partition layouts; use the upload cluster config
   * tool to upload the partition seal states onto Zookeeper; verify that the writable partitions are consistent between the two.
   * After verification is done, partition override config will be safely deleted.
   * @throws IOException if a file read error is encountered.
   * @throws JSONException if a JSON parse error is encountered.
   */
  private void uploadClusterConfigsAndVerify() throws Exception {
    List<PartitionId> writablePartitions = testPartitionLayout.getPartitionLayout().getWritablePartitions(null);
    Set<String> writableInPartitionLayout = new HashSet<>();
    writablePartitions.forEach(k -> writableInPartitionLayout.add(k.toPathString()));
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, false, new String[]{ClusterMapUtils.PARTITION_OVERRIDE_STR}, null);
    // Check writable partitions in each datacenter
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      HelixPropertyStore<ZNRecord> propertyStore =
          CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig,
              Collections.singletonList(propertyStoreConfig.rootPath));
      try {
        ZNRecord zNRecord =
            propertyStore.get(ClusterMapUtils.PARTITION_OVERRIDE_ZNODE_PATH, null, AccessOption.PERSISTENT);
        if (!activeDcSet.contains(zkInfo.getDcName())) {
          assertNull(zNRecord);
        } else {
          assertNotNull(zNRecord);
          Map<String, Map<String, String>> overridePartition = zNRecord.getMapFields();
          Set<String> writableInDC = new HashSet<>();
          for (Map.Entry<String, Map<String, String>> entry : overridePartition.entrySet()) {
            if (entry.getValue().get(ClusterMapUtils.PARTITION_STATE).equals(ClusterMapUtils.READ_WRITE_STR)) {
              writableInDC.add(entry.getKey());
            }
          }
          // Verify writable partitions in DC match writable partitions in Partition Layout
          assertEquals("Mismatch in writable partitions for partitionLayout and propertyStore",
              writableInPartitionLayout, writableInDC);
        }
      } finally {
        propertyStore.stop();
      }
    }
    // delete partition override config
    HelixBootstrapUpgradeUtil.uploadOrDeleteAdminConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, dcStr, true, new String[]{ClusterMapUtils.PARTITION_OVERRIDE_STR}, null);
    // verify that the config is cleaned up
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      HelixPropertyStore<ZNRecord> propertyStore =
          CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig,
              Collections.singletonList(propertyStoreConfig.rootPath));
      try {
        ZNRecord zNRecord =
            propertyStore.get(ClusterMapUtils.PARTITION_OVERRIDE_ZNODE_PATH, null, AccessOption.PERSISTENT);
        assertNull("Partition override config should no longer exist", zNRecord);
      } finally {
        propertyStore.stop();
      }
    }
  }

  /**
   * Verify that the number of resources in Helix is as expected.
   * @param hardwareLayout the {@link HardwareLayout} of the static clustermap.
   * @param expectedResourceCount the expected number of resources in Helix.
   */
  private void verifyResourceCount(HardwareLayout hardwareLayout, long expectedResourceCount) {
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      ZkInfo zkInfo = dcsToZkInfo.get(dc.getName());
      ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
      if (!activeDcSet.contains(dc.getName())) {
        Assert.assertFalse("Cluster should not be present, as dc " + dc.getName() + " is not enabled",
            admin.getClusters().contains(CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP));
      } else {
        assertEquals("Resource count mismatch", expectedResourceCount,
            admin.getResourcesInCluster(CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP).size());
      }
    }
  }

  private void addExpectedPartitionAndInstanceToResource(TestPartitionLayout testPartitionLayout, int resourceId,
      Map<String, Map<String, Set<String>>> instancesUnderResourcesInDc,
      Map<String, Map<String, Set<Integer>>> partitionsUnderResourcesInDc) {
    for (Datacenter dc : testPartitionLayout.testHardwareLayout.getHardwareLayout().getDatacenters()) {
      String resourceName = String.valueOf(resourceId);
      Set<String> instances = testPartitionLayout.testHardwareLayout.getAllDataNodesFromDc(dc.getName())
          .stream()
          .map(dn -> ClusterMapUtils.getInstanceName(dn.getHostname(), dn.getPort()))
          .collect(Collectors.toSet());
      instancesUnderResourcesInDc.get(dc.getName())
          .computeIfAbsent(resourceName, k -> new HashSet<>())
          .addAll(instances);

      Set<Integer> partitions = testPartitionLayout.getPartitionLayout()
          .getAllPartitionNames()
          .stream()
          .map(Integer::parseInt)
          .collect(Collectors.toSet());
      partitionsUnderResourcesInDc.get(dc.getName())
          .computeIfAbsent(resourceName, k -> new HashSet<>())
          .addAll(partitions);
    }
  }

  private void verifyResourceInFullAuto(HardwareLayout hardwareLayout,
      Map<String, Map<String, Set<String>>> instancesUnderResourcesInDc,
      Map<String, Map<String, Set<Integer>>> partitionsUnderResourcesInDc) {
    String clusterName = CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      ZkInfo zkInfo = dcsToZkInfo.get(dc.getName());
      ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.getPort());
      if (activeDcSet.contains(dc.getName())) {
        Map<String, Set<String>> instancesUnderResources = instancesUnderResourcesInDc.get(dc.getName());
        Map<String, Set<Integer>> partitionsUnderResources = partitionsUnderResourcesInDc.get(dc.getName());
        assertEquals("Instances and partitions map must have the same size", instancesUnderResources.size(),
            partitionsUnderResources.size());
        List<String> resources = admin.getResourcesInCluster(clusterName);
        assertEquals("Resource count mismatch", resources.size(), instancesUnderResources.size());
        for (String resource : resources) {
          IdealState idealState = admin.getResourceIdealState(clusterName, resource);
          assertNotNull("Expect resource " + resource + " exist", idealState);
          Set<Integer> partitions =
              idealState.getPartitionSet().stream().map(Integer::valueOf).collect(Collectors.toSet());
          Set<Integer> expectedPartitions = partitionsUnderResources.get(resource);
          assertEquals("Partition mismatch for resource " + resource, expectedPartitions, partitions);

          Set<String> instances = idealState.getPartitionSet()
              .stream()
              .flatMap(pn -> idealState.getInstanceSet(pn).stream())
              .collect(Collectors.toSet());
          Set<String> expectedInstances = instancesUnderResources.get(resource);
          assertEquals("Instances mismatch for resource " + resource, expectedInstances, instances);
        }
      }
    }
  }
}
