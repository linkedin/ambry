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

import com.github.ambry.clustermap.TestUtils.*;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.clustermap.HelixBootstrapUpgradeTool.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;


public class HelixBootstrapUpgradeToolTest {

  private static String tempDirPath;
  private static final Map<String, ZkInfo> dcsToZkInfo = new HashMap<>();
  private static final Map<String, HelixPropertyStore<ZNRecord>> dcsToPropertyStore = new HashMap<>();
  private static final String dcs[] = new String[]{"DC0", "DC1"};
  private static final byte ids[] = new byte[]{(byte) 0, (byte) 1};
  private final String hardwareLayoutPath;
  private final String partitionLayoutPath;
  private final String zkLayoutPath;
  private final JSONObject zkJson;
  private TestHardwareLayout testHardwareLayout;
  private TestPartitionLayout testPartitionLayout;
  private static final String CLUSTER_NAME_IN_STATIC_CLUSTER_MAP = "ToolTestStatic";
  private static final String CLUSTER_NAME_PREFIX = "Ambry-";
  private static final String ROOT_PATH = "/" + CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP;
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

  /**
   * Initialize ZKInfos for all dcs and start the ZK server.
   */
  public HelixBootstrapUpgradeToolTest() throws Exception {
    hardwareLayoutPath = tempDirPath + "/hardwareLayoutTest.json";
    partitionLayoutPath = tempDirPath + "/partitionLayoutTest.json";
    zkLayoutPath = tempDirPath + "/zkLayoutPath.json";
    zkJson = constructZkLayoutJSON(dcsToZkInfo.values());
    testHardwareLayout = constructInitialHardwareLayoutJSON(CLUSTER_NAME_IN_STATIC_CLUSTER_MAP);
    testPartitionLayout =
        constructInitialPartitionLayoutJSON(testHardwareLayout, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, null);
  }

  /**
   * Test the case where the zkHosts JSON does not have an entry for every Datacenter in the static clustermap.
   */
  @Test
  public void testIncompleteZKHostInfo() throws Exception {
    if (testHardwareLayout.getDatacenterCount() > 1) {
      JSONObject partialZkJson =
          constructZkLayoutJSON(Collections.singleton(dcsToZkInfo.entrySet().iterator().next().getValue()));
      Utils.writeJsonObjectToFile(partialZkJson, zkLayoutPath);
      Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
      Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
      try {
        HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
            CLUSTER_NAME_PREFIX, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, new HelixAdminFactory());
        fail("Should have thrown IllegalArgumentException as a zk host is missing for one of the dcs");
      } catch (IllegalArgumentException e) {
        // OK
      }
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
            Collections.emptyMap(), null);
    // Assert that xid field does not get set in InstanceConfig when it is the default.
    assertNull(referenceInstanceConfig.getRecord().getSimpleField(ClusterMapUtils.XID_STR));

    // Assert that xid field does get set if not the default.
    jsonObject.put("xid", "10");
    dataNode = new DataNode(dataNode.getDatacenter(), jsonObject, clusterMapConfig);
    InstanceConfig instanceConfig =
        HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
            Collections.emptyMap(), null);
    assertEquals("10", instanceConfig.getRecord().getSimpleField(ClusterMapUtils.XID_STR));
    assertThat(referenceInstanceConfig.getRecord(), not(equalTo(instanceConfig.getRecord())));

    referenceInstanceConfig = instanceConfig;

    // Assert that sealed list being different does not affect equality
    List<String> sealedList = Arrays.asList("5", "10");
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedList);
    // set the field to null. The created InstanceConfig should not have null fields.
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, null);
    instanceConfig = HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
        Collections.emptyMap(), referenceInstanceConfig);
    // Stopped replicas should be an empty list and not null, so set that in referenceInstanceConfig for comparison.
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, Collections.emptyList());
    assertEquals(instanceConfig.getRecord(), referenceInstanceConfig.getRecord());

    // Assert that stopped list being different does not affect equality
    List<String> stoppedReplicas = Arrays.asList("11", "15");
    referenceInstanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, stoppedReplicas);
    instanceConfig = HelixBootstrapUpgradeUtil.createInstanceConfigFromStaticInfo(dataNode, partitionToInstances,
        Collections.emptyMap(), referenceInstanceConfig);
    assertEquals(instanceConfig.getRecord(), referenceInstanceConfig.getRecord());
  }

  /**
   * A single test (for convenience) that tests bootstrap and upgrades.
   */
  @Test
  public void testEverything() throws Exception {
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
    for (Replica replica : partition1.getReplicas()) {
      partition1Nodes.add(replica.getDataNodeId());
    }
    Disk diskForNewReplica;
    do {
      diskForNewReplica = testHardwareLayout.getRandomDisk();
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
   * Write the layout files out from the constructed in-memory hardware and partition layouts; use the bootstrap tool
   * to update the contents in Helix; verify that the information is consistent between the two.
   * @param expectedResourceCount number of resources expected in Helix for this cluster in each datacenter.
   * @param forceRemove whether the forceRemove option should be passed when doing the bootstrap/upgrade.
   * @throws IOException if a file read error is encountered.
   * @throws JSONException if a JSON parse error is encountered.
   */
  private void writeBootstrapOrUpgrade(long expectedResourceCount, boolean forceRemove) throws Exception {
    Utils.writeJsonObjectToFile(zkJson, zkLayoutPath);
    Utils.writeJsonObjectToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // This updates and verifies that the information in Helix is consistent with the one in the static cluster map.
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, forceRemove, new HelixAdminFactory());
    verifyResourceCount(testHardwareLayout.getHardwareLayout(), expectedResourceCount);
  }

  /**
   * Write the layout files out from the constructed in-memory hardware and partition layouts; use the upload cluster config
   * tool to upload the partition seal states onto Zookeeper; verify that the writable partitions are consistent between the two.
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
    HelixBootstrapUpgradeUtil.uploadClusterConfigs(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        CLUSTER_NAME_PREFIX, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, new HelixAdminFactory());
    // Check writable partitions in each datacenter
    for (ZkInfo zkInfo : dcsToZkInfo.values()) {
      HelixPropertyStore<ZNRecord> propertyStore =
          CommonUtils.createHelixPropertyStore("localhost:" + zkInfo.getPort(), propertyStoreConfig,
              Collections.singletonList(propertyStoreConfig.rootPath));
      String getPath = ClusterMapUtils.PROPERTYSTORE_ZNODE_PATH;
      ZNRecord zNRecord = propertyStore.get(getPath, null, AccessOption.PERSISTENT);
      assertNotNull(zNRecord);
      Map<String, Map<String, String>> overridePartition = zNRecord.getMapFields();
      Set<String> writableInDC = new HashSet<>();
      for (Map.Entry<String, Map<String, String>> entry : overridePartition.entrySet()) {
        if (entry.getValue().get(ClusterMapUtils.PARTITION_STATE).equals(ClusterMapUtils.READ_WRITE_STR)) {
          writableInDC.add(entry.getKey());
        }
      }
      // Verify writable partitions in DC match writable partitions in Partition Layout
      assertEquals("Mismatch in writable partitions for partitionLayout and propertyStore", writableInPartitionLayout,
          writableInDC);
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
      assertEquals("Resource count mismatch", expectedResourceCount,
          admin.getResourcesInCluster(CLUSTER_NAME_PREFIX + CLUSTER_NAME_IN_STATIC_CLUSTER_MAP).size());
    }
  }
}
