/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.TestUtils.TestHardwareLayout;
import com.github.ambry.clustermap.TestUtils.TestPartitionLayout;
import com.github.ambry.utils.Utils;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.clustermap.HelixBootstrapUpgradeTool.*;


public class HelixBootstrapUpgradeToolTest {
  /**
   * A class to initialize and hold information about each Zk Server.
   */
  private static class ZkInfo {
    String dcName;
    int port;
    String dataDir;
    String logDir;
    ZkServer zkServer;

    /**
     * Instantiate by starting a Zk server.
     * @param dcName the name of the datacenter.
     * @param port the port at which this Zk server should run on localhost.
     */
    ZkInfo(String dcName, int port) throws IOException {
      this.dcName = dcName;
      this.port = port;
      File tempDir = Files.createTempDirectory(dcName).toFile();
      tempDirPath = tempDir.getAbsolutePath();
      tempDir.deleteOnExit();
      this.dataDir = tempDirPath + "/dataDir";
      this.logDir = tempDirPath + "/logDir";
      startZkServer(port, dataDir, logDir);
    }

    private void startZkServer(int port, String dataDir, String logDir) {
      IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
        @Override
        public void createDefaultNameSpace(ZkClient zkClient) {
        }
      };
      // start zookeeper
      zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
      zkServer.start();
    }

    void shutdown() {
      zkServer.shutdown();
    }
  }

  private static String tempDirPath;
  private static final HashMap<String, ZkInfo> dcsToZkInfo = new HashMap<>();
  private static final String dcs[] = new String[]{"DC0", "DC1"};
  private final String hardwareLayoutPath;
  private final String partitionLayoutPath;
  private final String zkLayoutPath;
  private final JSONObject zkJson;
  private TestHardwareLayout testHardwareLayout;
  private TestPartitionLayout testPartitionLayout;

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
    int port = 2200;
    for (String dcName : dcs) {
      dcsToZkInfo.put(dcName, new ZkInfo(dcName, port++));
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
    testHardwareLayout = constructInitialHardwareLayoutJSON();
    testPartitionLayout = constructInitialPartitionLayoutJSON(testHardwareLayout);
  }

  /**
   * Construct a ZK layout JSON using predetermined information.
   * @return the constructed JSON.
   */
  private JSONObject constructZkLayoutJSON(Collection<ZkInfo> zkInfos) throws JSONException {
    JSONArray zkInfosJson = new JSONArray();
    for (ZkInfo zkInfo : zkInfos) {
      JSONObject zkInfoJson = new JSONObject();
      zkInfoJson.put("datacenter", zkInfo.dcName);
      zkInfoJson.put("hostname", "localhost");
      zkInfoJson.put("port", zkInfo.port);
      zkInfosJson.put(zkInfoJson);
    }
    return new JSONObject().put("zkHosts", zkInfosJson);
  }

  /**
   * Construct a {@link TestHardwareLayout}
   * @return return the constructed layout.
   */
  private TestHardwareLayout constructInitialHardwareLayoutJSON() throws JSONException {
    return new TestHardwareLayout("Proto", 6, 100L * 1024 * 1024 * 1024, 6, 2, 18088, 20, false);
  }

  /**
   * Construct a {@link TestPartitionLayout}
   * @return return the constructed layout.
   */
  private TestPartitionLayout constructInitialPartitionLayoutJSON(TestHardwareLayout testHardwareLayout)
      throws JSONException {
    return new TestPartitionLayout(testHardwareLayout, HelixBootstrapUpgradeTool.MAX_PARTITIONS_PER_RESOURCE * 3 + 20,
        PartitionState.READ_WRITE, 1024L * 1024 * 1024, 3);
  }

  /**
   * Test the case where the zkHosts JSON does not have an entry for every Datacenter in the static clustermap.
   */
  @Test
  public void testIncompleteZKHostInfo() throws Exception {
    if (testHardwareLayout.getDatacenterCount() > 1) {
      JSONObject partialZkJson =
          constructZkLayoutJSON(Collections.singleton(dcsToZkInfo.entrySet().iterator().next().getValue()));
      Utils.writeJsonToFile(partialZkJson, zkLayoutPath);
      Utils.writeJsonToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
      Utils.writeJsonToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
      try {
        HelixBootstrapUpgradeTool.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "DC1");
        Assert.fail("Should have thrown IllegalArgumentException as a zk host is missing for one of the dcs");
      } catch (IllegalArgumentException e) {
        // OK
      }
    }
  }

  /**
   * A single test (for convenience) that tests bootstrap and upgrades.
   */
  @Test
  public void testEverything() throws Exception {
    /* Test bootstrap */
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / MAX_PARTITIONS_PER_RESOURCE + 1;
    Utils.writeJsonToFile(zkJson, zkLayoutPath);
    Utils.writeJsonToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    HelixBootstrapUpgradeTool.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "DC1");
    verifyEquivalencyWithStatic(testHardwareLayout.getHardwareLayout(), testPartitionLayout.getPartitionLayout(),
        expectedResourceCount);

    /* Test Simple Upgrade */
    int numNewNodes = 4;
    int numNewPartitions = 220;
    testHardwareLayout.addNewDataNodes(numNewNodes);
    testPartitionLayout.addNewPartitions(numNewPartitions);
    expectedResourceCount += (numNewPartitions - 1) / MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgradeAndVerify(expectedResourceCount);

    /* Test sealed state update. */
    Set<Long> partitionIdsBeforeAddition = new HashSet<>();
    for (PartitionId partitionId : testPartitionLayout.getPartitionLayout().getPartitions()) {
      partitionIdsBeforeAddition.add(((Partition) partitionId).getId());
    }

    // First, add new nodes and partitions (which are default READ_WRITE)
    numNewNodes = 2;
    numNewPartitions = 50;
    testHardwareLayout.addNewDataNodes(numNewNodes);
    testPartitionLayout.addNewPartitions(numNewPartitions);

    // Next, mark all previous partitions as READ_ONLY
    for (PartitionId partitionId : testPartitionLayout.getPartitionLayout().getPartitions()) {
      if (partitionIdsBeforeAddition.contains(((Partition) partitionId).getId())) {
        Partition partition = (Partition) partitionId;
        partition.partitionState = PartitionState.READ_ONLY;
      }
    }

    expectedResourceCount += (numNewPartitions - 1) / MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgradeAndVerify(expectedResourceCount);

    // Now, mark the ones that were READ_ONLY as READ_WRITE and vice versa
    for (PartitionId partitionId : testPartitionLayout.getPartitionLayout().getPartitions()) {
      Partition partition = (Partition) partitionId;
      if (partitionIdsBeforeAddition.contains(((Partition) partitionId).getId())) {
        partition.partitionState = PartitionState.READ_WRITE;
      } else {
        partition.partitionState = PartitionState.READ_ONLY;
      }
    }
    writeBootstrapOrUpgradeAndVerify(expectedResourceCount);
  }

  /**
   * Write the layout files out from the constructed in-memory hardware and partition layouts; use the bootstrap tool
   * to update the contents in Helix; verify that the information is consistent between the two.
   * @param expectedResourceCount number of resources expected in Helix for this cluster in each datacenter.
   * @throws IOException if a file read error is encountered.
   * @throws JSONException if a JSON parse error is encountered.
   */
  private void writeBootstrapOrUpgradeAndVerify(long expectedResourceCount) throws IOException, JSONException {
    Utils.writeJsonToFile(zkJson, zkLayoutPath);
    Utils.writeJsonToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    HelixBootstrapUpgradeTool.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "DC1");
    verifyEquivalencyWithStatic(testHardwareLayout.getHardwareLayout(), testPartitionLayout.getPartitionLayout(),
        expectedResourceCount);
  }

  /**
   * Verify that the information in Helix and the information in the static clustermap are equivalent.
   * @param hardwareLayout the {@link HardwareLayout} of the static clustermap.
   * @param partitionLayout the {@link PartitionLayout} of the static clustermap.
   * @param expectedResourceCount the expected number of resources in Helix.
   */
  private void verifyEquivalencyWithStatic(HardwareLayout hardwareLayout, PartitionLayout partitionLayout,
      long expectedResourceCount) {
    String clusterName = hardwareLayout.getClusterName();
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      ZkInfo zkInfo = dcsToZkInfo.get(dc.getName());
      Assert.assertNotNull(zkInfo);
      ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.port);
      Assert.assertTrue(admin.getClusters().contains(clusterName));
      verifyResourcesAndPartitionEquivalencyInDc(dc, clusterName, partitionLayout, expectedResourceCount);
      verifyDataNodeAndDiskEquivalencyInDc(dc, clusterName, hardwareLayout, partitionLayout);
    }
  }

  /**
   * Verify that the hardware layout information is in sync - which includes the node and disk information. Also verify
   * that the replicas belonging to disks are in sync between the static cluster map and Helix.
   * @param dc the datacenter whose information is to be verified.
   * @param clusterName the cluster to be verified.
   * @param hardwareLayout the {@link HardwareLayout} of the static clustermap.
   * @param partitionLayout the {@link PartitionLayout} of the static clustermap.
   */
  private void verifyDataNodeAndDiskEquivalencyInDc(Datacenter dc, String clusterName, HardwareLayout hardwareLayout,
      PartitionLayout partitionLayout) {
    ClusterMapManager staticClusterMap = new ClusterMapManager(partitionLayout);
    String dcName = dc.getName();
    ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + dcsToZkInfo.get(dcName).port);
    List<String> allInstancesInHelix = admin.getInstancesInCluster(clusterName);
    for (DataNodeId dataNodeId : dc.getDataNodes()) {
      Map<String, List<String>> mountPathToReplicas = getMountPathToReplicas(staticClusterMap, dataNodeId);
      DataNode dataNode = (DataNode) dataNodeId;
      String instanceName = dataNode.getHostname() + "_" + dataNode.getPort();
      Assert.assertTrue(allInstancesInHelix.remove(instanceName));
      InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);

      Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
      for (Disk disk : dataNode.getDisks()) {
        Map<String, String> diskInfoInHelix = diskInfos.remove(disk.getMountPath());
        Assert.assertNotNull(diskInfoInHelix);
        Assert.assertEquals(disk.getRawCapacityInBytes(), Long.valueOf(diskInfoInHelix.get(CAPACITY_STR)).longValue());
        Set<String> replicasInHelix;
        String replicasStr = diskInfoInHelix.get(REPLICAS_STR);
        if (replicasStr.isEmpty()) {
          replicasInHelix = new HashSet<>();
        } else {
          replicasInHelix = Sets.newHashSet(replicasStr.split(REPLICAS_DELIM_STR));
        }
        Set<String> replicasInClusterMap;
        List<String> replicaList = mountPathToReplicas.get(disk.getMountPath());
        if (replicaList == null) {
          replicasInClusterMap = new HashSet<>();
        } else {
          replicasInClusterMap = Sets.newHashSet(replicaList);
        }
        Assert.assertTrue(replicasInClusterMap.equals(replicasInHelix));
      }

      Assert.assertEquals(dataNode.getSSLPort(),
          Integer.valueOf(instanceConfig.getRecord().getSimpleField(SSLPORT_STR)).intValue());
      Assert.assertEquals(dataNode.getDatacenterName(), instanceConfig.getRecord().getSimpleField(DATACENTER_STR));
      Assert.assertEquals(dataNode.getRackId(),
          Long.valueOf(instanceConfig.getRecord().getSimpleField(RACKID_STR)).longValue());
      Assert.assertEquals(dataNode.getDatacenterName(), instanceConfig.getRecord().getSimpleField(DATACENTER_STR));
      Set<String> sealedReplicasInHelix = Sets.newHashSet(instanceConfig.getRecord().getListField(SEALED_STR));
      Set<String> sealedReplicasInClusterMap = new HashSet<>();
      for (Replica replica : staticClusterMap.getReplicas(dataNodeId)) {
        if (replica.getPartition().partitionState.equals(PartitionState.READ_ONLY)) {
          sealedReplicasInClusterMap.add(Long.toString(replica.getPartition().getId()));
        }
      }
      Assert.assertTrue(sealedReplicasInClusterMap.equals(sealedReplicasInHelix));
    }
    Assert.assertTrue(allInstancesInHelix.isEmpty());
  }

  /**
   * Verify that the partition layout information is in sync.
   * @param dc the datacenter whose information is to be verified.
   * @param clusterName the cluster to be verified.
   * @param partitionLayout the {@link PartitionLayout} of the static clustermap.
   * @param expectedResourceCount the expected number of resources in Helix.
   */
  private void verifyResourcesAndPartitionEquivalencyInDc(Datacenter dc, String clusterName,
      PartitionLayout partitionLayout, long expectedResourceCount) {
    String dcName = dc.getName();
    ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + dcsToZkInfo.get(dcName).port);
    Map<String, Set<String>> allPartitionsToInstancesInHelix = new HashMap<>();
    long numResources = 0;
    for (String resourceName : admin.getResourcesInCluster(clusterName)) {
      IdealState resourceIS = admin.getResourceIdealState(clusterName, resourceName);
      Set<String> resourcePartitions = resourceIS.getPartitionSet();
      for (String resourcePartition : resourcePartitions) {
        Assert.assertNull(
            allPartitionsToInstancesInHelix.put(resourcePartition, resourceIS.getInstanceSet(resourcePartition)));
      }
      numResources++;
    }
    Assert.assertEquals(expectedResourceCount, numResources);
    for (PartitionId partitionId : partitionLayout.getPartitions()) {
      Partition partition = (Partition) partitionId;
      String partitionName = Long.toString(partition.getId());
      Set<String> replicaHostsInHelix = allPartitionsToInstancesInHelix.remove(partitionName);
      Assert.assertNotNull(replicaHostsInHelix);
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDataNodeId().getDatacenterName().equals(dcName)) {
          String instanceName = replica.getDataNodeId().getHostname() + "_" + replica.getDataNodeId().getPort();
          Assert.assertTrue(replicaHostsInHelix.remove(instanceName));
        }
      }
      Assert.assertTrue(replicaHostsInHelix.isEmpty());
    }
    Assert.assertTrue(allPartitionsToInstancesInHelix.isEmpty());
  }

  /**
   * A helper method that returns a map of mountPaths to a list of replicas for a given {@link DataNodeId}
   * @param staticClusterMap the static {@link ClusterMapManager}
   * @param dataNodeId the {@link DataNodeId} of interest.
   * @return the constructed map.
   */
  static Map<String, List<String>> getMountPathToReplicas(ClusterMapManager staticClusterMap, DataNodeId dataNodeId) {
    Map<String, List<String>> mountPathToReplicas = new HashMap<>();
    for (Replica replica : staticClusterMap.getReplicas(dataNodeId)) {
      List<String> replicaStrs = mountPathToReplicas.get(replica.getMountPath());
      if (replicaStrs != null) {
        replicaStrs.add(Long.toString(replica.getPartition().getId()));
      } else {
        replicaStrs = new ArrayList<>();
        replicaStrs.add(Long.toString(replica.getPartition().getId()));
        mountPathToReplicas.put(replica.getMountPath(), replicaStrs);
      }
    }
    return mountPathToReplicas;
  }
}
