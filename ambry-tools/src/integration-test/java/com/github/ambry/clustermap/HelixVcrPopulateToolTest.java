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

import com.github.ambry.utils.TestUtils;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterSetup;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class HelixVcrPopulateToolTest {

  private static final String SRC_ZK_SERVER_HOSTNAME = "localhost";
  private static final int SRC_ZK_SERVER_PORT = 31900;
  private static final String SRC_ZK_CONNECT_STRING = SRC_ZK_SERVER_HOSTNAME + ":" + SRC_ZK_SERVER_PORT;
  private static TestUtils.ZkInfo srcZkInfo;
  private static final String SRC_CLUSTER_NAME = "srcCluster";
  private static HelixAdmin srcHelixAdmin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    srcZkInfo = new com.github.ambry.utils.TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), "DC1", (byte) 1,
        SRC_ZK_SERVER_PORT, true);

    HelixZkClient zkClient =
        SharedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(SRC_ZK_CONNECT_STRING));
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterSetup clusterSetup = new ClusterSetup(zkClient);
    clusterSetup.addCluster(SRC_CLUSTER_NAME, true);
    srcHelixAdmin = new HelixAdminFactory().getHelixAdmin(SRC_ZK_CONNECT_STRING);

    String resourceName = "1";
    Set<String> partitionSet = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      partitionSet.add(Integer.toString(i));
    }
    IdealState idealState = HelixVcrPopulateTool.buildIdealState(resourceName, partitionSet);
    srcHelixAdmin.addResource(SRC_CLUSTER_NAME, resourceName, idealState);
  }

  @AfterClass
  public static void afterClass() {
    srcZkInfo.shutdown();
  }

  /**
   * Test {@link HelixVcrPopulateTool#createCluster(String, String)} and
   * {@link HelixVcrPopulateTool#updateResourceAndPartition(String, String, String, String, boolean)} method.
   */
  @Test
  public void testCreateAndUpdateCluster() throws Exception {
    String destZkHostName = "localhost";
    int destZkServerPort = SRC_ZK_SERVER_PORT + 1;
    String destZkConnectString = destZkHostName + ":" + destZkServerPort;
    String destVcrClusterName = "DEST_VCR_CLUSTER1";
    // set up dest zk
    TestUtils.ZkInfo destZkInfo =
        new com.github.ambry.utils.TestUtils.ZkInfo(TestUtils.getTempDir("helixDestVcr"), "DC1", (byte) 1,
            destZkServerPort, true);
    HelixVcrPopulateTool.createCluster(destZkConnectString, destVcrClusterName);

    HelixVcrPopulateTool.updateResourceAndPartition(SRC_ZK_CONNECT_STRING, SRC_CLUSTER_NAME, destZkConnectString,
        destVcrClusterName, false);
    Assert.assertTrue("Dest and Src should be same",
        isSrcDestSync(SRC_ZK_CONNECT_STRING, SRC_CLUSTER_NAME, destZkConnectString, destVcrClusterName));

    // add one more partition to src cluster resource 1 and add one more resource to src cluster
    srcHelixAdmin.dropResource(SRC_CLUSTER_NAME, "1");
    String[] resourceNames = {"1", "2"};
    Set<String> partitionSet = new HashSet<>();
    for (int i = 0; i < 101; i++) {
      partitionSet.add(Integer.toString(i));
    }
    for (String resourceName : resourceNames) {
      IdealState idealState = HelixVcrPopulateTool.buildIdealState(resourceName, partitionSet);
      srcHelixAdmin.addResource(SRC_CLUSTER_NAME, resourceName, idealState);
    }

    HelixVcrPopulateTool.updateResourceAndPartition(SRC_ZK_CONNECT_STRING, SRC_CLUSTER_NAME, destZkConnectString,
        destVcrClusterName, false);
    Assert.assertTrue("Dest and Src should be same",
        isSrcDestSync(SRC_ZK_CONNECT_STRING, SRC_CLUSTER_NAME, destZkConnectString, destVcrClusterName));

    // Test the update-only option as well, make sure partitions are unchanged
    HelixVcrPopulateTool.updateResourceIdealState(destZkConnectString, destVcrClusterName, false);
    Assert.assertTrue("Dest and Src should be same",
        isSrcDestSync(SRC_ZK_CONNECT_STRING, SRC_CLUSTER_NAME, destZkConnectString, destVcrClusterName));

    destZkInfo.shutdown();
  }

  /**
   * A method to verify resources and partitions in src cluster and dest cluster are same.
   */
  private boolean isSrcDestSync(String srcZkString, String srcClusterName, String destZkString,
      String destClusterName) {

    HelixAdmin srcAdmin = new ZKHelixAdmin(srcZkString);
    Set<String> srcResources = new HashSet<>(srcAdmin.getResourcesInCluster(srcClusterName));
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkString);
    Set<String> destResources = new HashSet<>(destAdmin.getResourcesInCluster(destClusterName));

    for (String resource : srcResources) {
      if (HelixVcrPopulateTool.ignoreResourceKeyWords.stream().anyMatch(resource::contains)) {
        System.out.println("Resource " + resource + " from src cluster is ignored");
        continue;
      }
      if (destResources.contains(resource)) {
        // check if every partition exist.
        Set<String> srcPartitions = srcAdmin.getResourceIdealState(srcClusterName, resource).getPartitionSet();
        Set<String> destPartitions = destAdmin.getResourceIdealState(destClusterName, resource).getPartitionSet();
        for (String partition : srcPartitions) {
          if (!destPartitions.contains(partition)) {
            return false;
          }
        }
      } else {
        return false;
      }
    }
    return true;
  }
}
