/**
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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.clustermap.VirtualReplicatorClusterListener;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests of HelixVcrCluster.
 */
public class HelixVcrClusterTest {
  private final static Logger logger = LoggerFactory.getLogger(HelixVcrClusterTest.class);
  private static MockClusterAgentsFactory mockClusterAgentsFactory;
  private static MockClusterMap mockClusterMap;
  private static final String ZK_SERVER_HOSTNAME = "localhost";
  private static final int ZK_SERVER_PORT = 31900;
  private static final String ZK_CONNECT_STRING = ZK_SERVER_HOSTNAME + ":" + Integer.toString(ZK_SERVER_PORT);
  private static TestUtils.ZkInfo zkInfo;
  private static final String VCR_CLUSTER_NAME = "vcrTestCluster";
  private static HelixControllerManager helixControllerManager;
  private static final int NUM_PARTITIONS = 10;

  @BeforeClass
  public static void beforeClass() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, 1, 1, NUM_PARTITIONS);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
    zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), "DC1", (byte) 1, ZK_SERVER_PORT, true);
    helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(ZK_CONNECT_STRING, VCR_CLUSTER_NAME, mockClusterMap);
  }

  @AfterClass
  public static void afterClass() {
    helixControllerManager.syncStop();
    zkInfo.shutdown();
  }

  /**
   * Test addPartition and removePartition of {@link HelixVcrCluster}
   */
  @Test
  public void helixVcrClusterTest() throws Exception {
    // Create helixInstance1 and join the cluster. All partitions should be assigned to helixInstance1.
    VirtualReplicatorCluster helixInstance1 = createHelixInstance(8123, 10123);
    List<PartitionId> expectedPartitions = mockClusterMap.getAllPartitionIds(null);
    CountDownLatch latchForAdd = new CountDownLatch(expectedPartitions.size());
    CountDownLatch latchForRemove = new CountDownLatch(expectedPartitions.size() / 2);
    MockVcrListener mockVcrListener = new MockVcrListener(latchForAdd, latchForRemove);
    helixInstance1.addListener(mockVcrListener);
    Assert.assertTrue("Latch count is not correct.", latchForAdd.await(5, TimeUnit.SECONDS));
    Assert.assertArrayEquals("Partition assignments are not correct.", expectedPartitions.toArray(),
        mockVcrListener.getPartitionSet().toArray());
    Assert.assertEquals("Partition assignment are not correct.", helixInstance1.getAssignedPartitionIds(),
        expectedPartitions);

    // Create helixInstance2 and join the cluster. Half of partitions should be removed from helixInstance1.
    VirtualReplicatorCluster helixInstance2 = createHelixInstance(8124, 10124);
    Assert.assertTrue("Latch count is not correct.", latchForRemove.await(5, TimeUnit.SECONDS));
    Assert.assertEquals("Number of partitions removed are not correct.", expectedPartitions.size() / 2,
        mockVcrListener.getPartitionSet().size());
    helixInstance1.close();
    helixInstance2.close();
  }

  /**
   * Helper function to create helix instance and join helix cluster.
   * @param clusterMapPort The clusterMapPort of the instance.
   * @param vcrSslPort The vcrSslPort of this vcr.
   */
  private VirtualReplicatorCluster createHelixInstance(int clusterMapPort, int vcrSslPort) throws Exception {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.ssl.enabled.datacenters", "DC1,DC2");
    props.setProperty("clustermap.port", Integer.toString(clusterMapPort));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    props = new Properties();
    props.setProperty("vcr.ssl.port", Integer.toString(vcrSslPort));
    props.setProperty(CloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING, ZK_SERVER_HOSTNAME + ":" + ZK_SERVER_PORT);
    props.setProperty(CloudConfig.VCR_CLUSTER_NAME, VCR_CLUSTER_NAME);
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(props));
    return new HelixVcrClusterFactory(cloudConfig, clusterMapConfig, mockClusterMap).getVirtualReplicatorCluster();
  }

  private static class MockVcrListener implements VirtualReplicatorClusterListener {

    private final Set<PartitionId> partitionSet = ConcurrentHashMap.newKeySet();
    private final CountDownLatch latchForAdd;
    private final CountDownLatch latchForRemove;

    MockVcrListener(CountDownLatch latchForAdd, CountDownLatch latchForRemove) {
      this.latchForAdd = latchForAdd;
      this.latchForRemove = latchForRemove;
    }

    @Override
    public void onPartitionAdded(PartitionId partitionId) {
      partitionSet.add(partitionId);
      latchForAdd.countDown();
    }

    @Override
    public void onPartitionRemoved(PartitionId partitionId) {
      partitionSet.remove(partitionId);
      latchForRemove.countDown();
    }

    public Set getPartitionSet() {
      return partitionSet;
    }
  }
}
