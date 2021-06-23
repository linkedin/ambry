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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.clustermap.VcrClusterParticipantListener;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Tests of HixVcrClusterParticipant.
 */
public class HelixVcrClusterParticipantTest {
  private static final String ZK_SERVER_HOSTNAME = "localhost";
  private static final int ZK_SERVER_PORT = 31900;
  private static final String ZK_CONNECT_STRING = ZK_SERVER_HOSTNAME + ":" + ZK_SERVER_PORT;
  private static final String VCR_CLUSTER_NAME = "vcrTestCluster";
  private static final int NUM_PARTITIONS = 10;
  private static MockClusterAgentsFactory mockClusterAgentsFactory;
  private static MockClusterMap mockClusterMap;
  private static TestUtils.ZkInfo zkInfo;
  private static HelixControllerManager helixControllerManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, true, 1, 1, NUM_PARTITIONS);
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
   * Test addReplica and removeReplica of {@link HelixVcrClusterParticipant}
   */
  @Test
  public void helixVcrClusterParticipant() throws Exception {
    StrictMatchExternalViewVerifier helixBalanceVerifier =
        new StrictMatchExternalViewVerifier(ZK_CONNECT_STRING, VCR_CLUSTER_NAME,
            Collections.singleton(VcrTestUtil.helixResource), null);
    // Create helixInstance1 and join the cluster. All partitions should be assigned to helixInstance1.
    VcrClusterParticipant helixInstance1 = createHelixVcrClusterParticipant(8123, 10123);
    Collection<? extends PartitionId> expectedPartitions =
        Collections.unmodifiableCollection(mockClusterMap.getAllPartitionIds(null));
    MockVcrParticipantListener mockVcrListener = new MockVcrParticipantListener();
    helixInstance1.addListener(mockVcrListener);
    helixInstance1.participate();
    TestUtils.checkAndSleep(true, () -> helixInstance1.getAssignedPartitionIds().size() > 0, 1000);
    Assert.assertTrue("Helix balance timeout.", helixBalanceVerifier.verify(5000));
    Assert.assertTrue("Partition assignment are not correct.",
        collectionEquals(helixInstance1.getAssignedPartitionIds(), expectedPartitions));

    // Create helixInstance2 and join the cluster. Half of partitions should be removed from helixInstance1.
    VcrClusterParticipant helixInstance2 = createHelixVcrClusterParticipant(8124, 10124);
    helixInstance2.participate();
    // Detect any ideal state change first.
    TestUtils.checkAndSleep(true, () -> helixInstance1.getAssignedPartitionIds().size() < expectedPartitions.size(),
        1000);
    Assert.assertTrue("Helix balance timeout.", helixBalanceVerifier.verify(5000));
    Assert.assertEquals("Number of partitions removed are not correct.", expectedPartitions.size() / 2,
        mockVcrListener.getPartitionSet().size());

    // Close helixInstance2. All partitions should back to helixInstance1.
    helixInstance2.close();
    // Detect any ideal state change first.
    TestUtils.checkAndSleep(true, () -> helixInstance1.getAssignedPartitionIds().size() > expectedPartitions.size() / 2,
        500);
    Assert.assertTrue("Helix balance timeout.", helixBalanceVerifier.verify(5000));
    Assert.assertTrue("Partition assignment are not correct.",
        collectionEquals(helixInstance1.getAssignedPartitionIds(), expectedPartitions));

    helixInstance1.close();
  }

  /**
   * Helper function to create {@link HelixVcrClusterParticipant}.
   * @param clusterMapPort The clusterMapPort of the instance.
   * @param vcrSslPort The vcrSslPort of this vcr.
   */
  private VcrClusterParticipant createHelixVcrClusterParticipant(int clusterMapPort, int vcrSslPort) throws Exception {
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
    VerifiableProperties verifiableProperties = new VerifiableProperties(props);
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    return new HelixVcrClusterAgentsFactory(cloudConfig, clusterMapConfig, mockClusterMap, Mockito.mock(AccountService.class),
        new StoreConfig(verifiableProperties), null, new MetricRegistry()).getVcrClusterParticipant();
  }

  /**
   * Check if two collections have same contents.
   * @param collection1 {@link Collection} object to compare.
   * @param collection2 {@link Collection} object to compare.
   * @return true if contents are equals. false otherwise.
   */
  private boolean collectionEquals(Collection<? extends PartitionId> collection1,
      Collection<? extends PartitionId> collection2) {
    return collection1.size() == collection2.size() && collection1.containsAll(collection2) && collection2.containsAll(
        collection1);
  }

  private static class MockVcrParticipantListener implements VcrClusterParticipantListener {

    private final Set<PartitionId> partitionSet = ConcurrentHashMap.newKeySet();

    MockVcrParticipantListener() {

    }

    @Override
    public void onPartitionAdded(PartitionId partitionId) {
      partitionSet.add(partitionId);
    }

    @Override
    public void onPartitionRemoved(PartitionId partitionId) {
      partitionSet.remove(partitionId);
    }

    public Set<PartitionId> getPartitionSet() {
      return partitionSet;
    }
  }
}
