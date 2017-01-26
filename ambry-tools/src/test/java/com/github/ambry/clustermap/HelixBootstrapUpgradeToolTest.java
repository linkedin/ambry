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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
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
      zkInfoJson.put("zkConnectStr", "localhost:" + zkInfo.port);
      zkInfosJson.put(zkInfoJson);
    }
    return new JSONObject().put("zkInfo", zkInfosJson);
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
    return new TestPartitionLayout(testHardwareLayout,
        HelixBootstrapUpgradeTool.DEFAULT_MAX_PARTITIONS_PER_RESOURCE * 3 + 20, PartitionState.READ_WRITE,
        1024L * 1024 * 1024, 3);
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
        HelixBootstrapUpgradeTool.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "DC1",
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE);
        Assert.fail("Should have thrown IllegalArgumentException as a zk host is missing for one of the dcs");
      } catch (IllegalArgumentException e) {
        // OK
      }
    }
  }

  /**
   * A single test (for convenience) that tests bootstrap and upgrades.
   */
  // @todo: uncomment when we move to Helix 0.6.7.
  // @Test
  public void testEverything() throws Exception {
    /* Test bootstrap */
    long expectedResourceCount =
        (testPartitionLayout.getPartitionLayout().getPartitionCount() - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount);

    /* Test Simple Upgrade */
    int numNewNodes = 4;
    int numNewPartitions = 220;
    testHardwareLayout.addNewDataNodes(numNewNodes);
    testPartitionLayout.addNewPartitions(numNewPartitions);
    expectedResourceCount += (numNewPartitions - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount);

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

    expectedResourceCount += (numNewPartitions - 1) / DEFAULT_MAX_PARTITIONS_PER_RESOURCE + 1;
    writeBootstrapOrUpgrade(expectedResourceCount);

    // Now, mark the ones that were READ_ONLY as READ_WRITE and vice versa
    for (PartitionId partitionId : testPartitionLayout.getPartitionLayout().getPartitions()) {
      Partition partition = (Partition) partitionId;
      if (partitionIdsBeforeAddition.contains(((Partition) partitionId).getId())) {
        partition.partitionState = PartitionState.READ_WRITE;
      } else {
        partition.partitionState = PartitionState.READ_ONLY;
      }
    }
    writeBootstrapOrUpgrade(expectedResourceCount);
  }

  /**
   * Write the layout files out from the constructed in-memory hardware and partition layouts; use the bootstrap tool
   * to update the contents in Helix; verify that the information is consistent between the two.
   * @param expectedResourceCount number of resources expected in Helix for this cluster in each datacenter.
   * @throws IOException if a file read error is encountered.
   * @throws JSONException if a JSON parse error is encountered.
   */
  private void writeBootstrapOrUpgrade(long expectedResourceCount) throws IOException, JSONException {
    Utils.writeJsonToFile(zkJson, zkLayoutPath);
    Utils.writeJsonToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    // This updates and verifies that the information in Helix is consistent with the one in the static cluster map.
    HelixBootstrapUpgradeTool.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, "DC1",
        DEFAULT_MAX_PARTITIONS_PER_RESOURCE);
    verifyResourceCount(testHardwareLayout.getHardwareLayout(), expectedResourceCount);
  }

  /**
   * Verify that the number of resources in Helix is as expected.
   * @param hardwareLayout the {@link HardwareLayout} of the static clustermap.
   * @param expectedResourceCount the expected number of resources in Helix.
   */
  private void verifyResourceCount(HardwareLayout hardwareLayout, long expectedResourceCount) {
    String clusterName = hardwareLayout.getClusterName();
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      ZkInfo zkInfo = dcsToZkInfo.get(dc.getName());
      ZKHelixAdmin admin = new ZKHelixAdmin("localhost:" + zkInfo.port);
      Assert.assertEquals("Resource count mismatch", expectedResourceCount,
          admin.getResourcesInCluster(clusterName).size());
    }
  }
}
