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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.TestUtils.*;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests the {@link HelixClusterManager} directly and also via the {@link CompositeClusterManager}.
 */
@RunWith(Parameterized.class)
public class HelixClusterManagerTest {
  private final HashMap<String, ZkInfo> dcsToZkInfo = new HashMap<>();
  private final String dcs[] = new String[]{"DC0", "DC1"};
  private final TestUtils.TestHardwareLayout testHardwareLayout;
  private final TestPartitionLayout testPartitionLayout;
  private final String clusterNameStatic = "HelixClusterManagerTestCluster";
  private final String clusterNamePrefixInHelix = "Ambry-";
  private final ClusterMapConfig clusterMapConfig;
  private final MockHelixCluster helixCluster;
  private final String hostname;
  private final ClusterMap clusterManager;
  private MetricRegistry metricRegistry;
  private Map<String, Gauge> gauges;
  private Map<String, Counter> counters;
  private final boolean useComposite;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Construct the static layout files and use that to instantiate a {@link MockHelixCluster}.
   * Instantiate a {@link MockHelixManagerFactory} for use by the cluster manager.
   * @param useComposite whether or not the test are to be done for the {@link CompositeClusterManager}
   * @throws Exception
   */
  public HelixClusterManagerTest(boolean useComposite) throws Exception {
    this.useComposite = useComposite;
    Random random = new Random();
    File tempDir = Files.createTempDirectory("helixClusterManager-" + random.nextInt(1000)).toFile();
    String tempDirPath = tempDir.getAbsolutePath();
    tempDir.deleteOnExit();
    int port = 2200;
    for (String dcName : dcs) {
      dcsToZkInfo.put(dcName, new ZkInfo(tempDirPath, dcName, port++, false));
    }
    String hardwareLayoutPath = tempDirPath + "/hardwareLayoutTest.json";
    String partitionLayoutPath = tempDirPath + "/partitionLayoutTest.json";
    String zkLayoutPath = tempDirPath + "/zkLayoutPath.json";
    JSONObject zkJson = constructZkLayoutJSON(dcsToZkInfo.values());
    testHardwareLayout = constructInitialHardwareLayoutJSON(clusterNameStatic);
    testPartitionLayout = constructInitialPartitionLayoutJSON(testHardwareLayout, 3);
    Utils.writeJsonToFile(zkJson, zkLayoutPath);
    Utils.writeJsonToFile(testHardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonToFile(testPartitionLayout.getPartitionLayout().toJSONObject(), partitionLayoutPath);
    helixCluster =
        new MockHelixCluster(clusterNamePrefixInHelix, hardwareLayoutPath, partitionLayoutPath, zkLayoutPath);
    hostname = "localhost";
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", hostname);
    props.setProperty("clustermap.cluster.name", clusterNamePrefixInHelix + clusterNameStatic);
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    MockHelixManagerFactory helixManagerFactory = new MockHelixManagerFactory(helixCluster);
    if (useComposite) {
      StaticClusterAgentsFactory staticClusterAgentsFactory =
          new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutPath, partitionLayoutPath);
      metricRegistry = staticClusterAgentsFactory.getMetricRegistry();
      clusterManager = new CompositeClusterManager(staticClusterAgentsFactory.getClusterMap(),
          new HelixClusterManager(clusterMapConfig, hostname, helixManagerFactory, metricRegistry));
    } else {
      metricRegistry = new MetricRegistry();
      clusterManager = new HelixClusterManager(clusterMapConfig, hostname, helixManagerFactory, metricRegistry);
    }
  }

  /**
   * Close the cluster managers created.
   */
  @After
  public void after() {
    if (clusterManager != null) {
      clusterManager.close();
    }
  }

  /**
   * Test bad instantiation.
   * @throws Exception
   */
  @Test
  public void badInstantiationTest() throws Exception {
    // Good test happened in the constructor

    // Bad test
    Set<ZkInfo> zkInfos = new HashSet<>(dcsToZkInfo.values());
    zkInfos.iterator().next().port = 0;
    JSONObject invalidZkJson = constructZkLayoutJSON(zkInfos);
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", hostname);
    props.setProperty("clustermap.cluster.name", clusterNamePrefixInHelix + clusterNameStatic);
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.dcs.zk.connect.strings", invalidZkJson.toString(2));
    ClusterMapConfig invalidClusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    metricRegistry = new MetricRegistry();
    try {
      new HelixClusterManager(invalidClusterMapConfig, hostname, new MockHelixManagerFactory(helixCluster),
          metricRegistry);
      fail("Instantiation should have failed with invalid zk addresses");
    } catch (IOException e) {
      assertEquals(true, metricRegistry.getGauges()
          .get(HelixClusterManager.class.getName() + ".helixClusterManagerInstantiationFailed")
          .getValue());
    }
  }

  /**
   * Tests all the interface methods.
   * @throws Exception
   */
  @Test
  public void basicInterfaceTest() throws Exception {
    for (String metricName : clusterManager.getMetricRegistry().getNames()) {
      System.out.println(metricName);
    }
    testPartitionReplicaConsistency();
    testInvalidPartitionId();
    testDatacenterDatanodeReplicas();
    assertStateEquivalency();
  }

  /**
   * Test that everything works as expected in the presence of liveness changes initiated by Helix itself.
   * @throws Exception
   */
  @Test
  public void helixInitiatedLivenessChangeTest() throws Exception {
    // this test is not intended for the composite cluster manager.
    if (useComposite) {
      return;
    }
    // all instances are up initially.
    assertStateEquivalency();

    // Bring one instance down in each dc.
    for (String zkAddr : helixCluster.getZkAddrs()) {
      helixCluster.bringInstanceDown(helixCluster.getUpInstances(zkAddr).get(0));
    }
    assertStateEquivalency();

    // Bring all instances down in all dcs.
    helixCluster.bringAllInstancesDown();
    assertStateEquivalency();

    // Bring one instance up in each dc.
    for (String zkAddr : helixCluster.getZkAddrs()) {
      helixCluster.bringInstanceUp(helixCluster.getDownInstances(zkAddr).get(0));
    }
    assertStateEquivalency();
  }

  /**
   * Test that everything works as expected in the presence of liveness changes initiated by clients of the cluster
   * manager.
   * @throws Exception
   */
  @Test
  public void clientInitiatedLivenessChangeTest() throws Exception {
    ReplicaId replica = clusterManager.getWritablePartitionIds().get(0).getReplicaIds().get(0);
    DataNodeId dataNode = replica.getDataNodeId();
    assertTrue(clusterManager.getReplicaIds(dataNode).contains(replica));
    DiskId disk = replica.getDiskId();

    // Verify that everything is up in the beginning.
    assertFalse(replica.isDown());
    assertEquals(HardwareState.AVAILABLE, dataNode.getState());
    assertEquals(HardwareState.AVAILABLE, disk.getState());

    // Trigger node failure events for the replica.
    for (int i = 0; i < clusterMapConfig.clusterMapFixedTimeoutDatanodeErrorThreshold; i++) {
      clusterManager.onReplicaEvent(replica, ReplicaEventType.Node_Timeout);
    }

    // When node times out, all replicas and all disks on the node should also become unavailable.
    assertTrue(replica.isDown());
    assertEquals(HardwareState.UNAVAILABLE, dataNode.getState());
    assertEquals(HardwareState.UNAVAILABLE, disk.getState());

    // Trigger a successful event to bring the resources up.
    clusterManager.onReplicaEvent(replica, ReplicaEventType.Node_Response);
    assertFalse(replica.isDown());
    assertEquals(HardwareState.AVAILABLE, dataNode.getState());
    assertEquals(HardwareState.AVAILABLE, disk.getState());

    // Similar tests for disks.
    for (int i = 0; i < clusterMapConfig.clusterMapFixedTimeoutDiskErrorThreshold; i++) {
      clusterManager.onReplicaEvent(replica, ReplicaEventType.Disk_Error);
    }
    assertTrue(replica.isDown());
    assertEquals(HardwareState.UNAVAILABLE, disk.getState());
    // node should still be available even on disk error.
    assertEquals(HardwareState.AVAILABLE, dataNode.getState());

    clusterManager.onReplicaEvent(replica, ReplicaEventType.Disk_Ok);
    assertFalse(replica.isDown());
    assertEquals(HardwareState.AVAILABLE, dataNode.getState());
    assertEquals(HardwareState.AVAILABLE, disk.getState());

    // The following does not do anything currently.
    clusterManager.onReplicaEvent(replica, ReplicaEventType.Partition_ReadOnly);
    assertStateEquivalency();
  }

  /**
   * Test that the metrics in {@link HelixClusterManagerMetrics} are updated as expected. This also tests and ensures
   * coverage of the methods in {@link HelixClusterManager} that are used only by {@link HelixClusterManagerMetrics}.
   */
  @Test
  public void metricsTest() throws Exception {
    counters = clusterManager.getMetricRegistry().getCounters();
    gauges = clusterManager.getMetricRegistry().getGauges();

    // live instance trigger happens once initially.
    long instanceTriggerCount = dcs.length;

    // Bring one instance down in each dc in order to test the metrics more generally.
    for (String zkAddr : helixCluster.getZkAddrs()) {
      helixCluster.bringInstanceDown(helixCluster.getUpInstances(zkAddr).get(0));
      instanceTriggerCount++;
    }

    // trigger for live instance change event should have come in twice per dc - the initial one, and the one due to a
    // node brought up in each DC.
    assertEquals(instanceTriggerCount, getCounterValue("liveInstanceChangeTriggerCount"));
    assertEquals(dcs.length, getCounterValue("externalViewChangeTriggerCount"));
    assertEquals(dcs.length, getCounterValue("instanceConfigChangeTriggerCount"));
    assertEquals(helixCluster.getDataCenterCount(), (long) getGaugeValue("datacenterCount", Long.class));
    assertEquals(helixCluster.getDownInstances().size() + helixCluster.getUpInstances().size(),
        (long) getGaugeValue("dataNodeCount", Long.class));
    assertEquals(helixCluster.getDownInstances().size(), (long) getGaugeValue("dataNodeDownCount", Long.class));
    assertEquals(helixCluster.getDiskCount(), (long) getGaugeValue("diskCount", Long.class));
    assertEquals(helixCluster.getDiskDownCount(), (long) getGaugeValue("diskDownCount", Long.class));
    assertEquals(helixCluster.getPartitions().size(), (long) getGaugeValue("partitionCount", Long.class));
    assertEquals(helixCluster.getPartitions().size(), (long) getGaugeValue("partitionReadWriteCount", Long.class));
    assertEquals(0, (long) getGaugeValue("partitionSealedCount", Long.class));
    assertEquals(helixCluster.getDiskCapacity(), (long) getGaugeValue("rawTotalCapacityBytes", Long.class));
    assertFalse(getGaugeValue("isMajorityReplicasDownForAnyPartition", Boolean.class));
    helixCluster.bringAllInstancesDown();
    assertTrue(getGaugeValue("isMajorityReplicasDownForAnyPartition", Boolean.class));
    if (useComposite) {
      helixCluster.bringAllInstancesUp();
      PartitionId partition = clusterManager.getWritablePartitionIds().get(0);
      assertEquals(0, getCounterValue("getPartitionIdFromStreamMismatchCount"));

      ReplicaId replicaId = partition.getReplicaIds().get(0);
      assertEquals(0, getCounterValue("getReplicaIdsMismatchCount"));

      // bring the replica down.
      for (int i = 0; i < clusterMapConfig.clusterMapFixedTimeoutDiskErrorThreshold; i++) {
        clusterManager.onReplicaEvent(replicaId, ReplicaEventType.Disk_Error);
      }
      clusterManager.getWritablePartitionIds();
      assertEquals(0, getCounterValue("getPartitionIdFromStreamMismatchCount"));

      InputStream partitionStream = new ByteBufferInputStream(ByteBuffer.wrap(partition.getBytes()));
      clusterManager.getPartitionIdFromStream(partitionStream);
      assertEquals(0, getCounterValue("getWritablePartitionIdsMismatchCount"));

      clusterManager.hasDatacenter("invalid");
      clusterManager.hasDatacenter(dcs[0]);
      assertEquals(0, getCounterValue("hasDatacenterMismatchCount"));

      DataNodeId dataNodeId = clusterManager.getDataNodeIds().get(0);
      assertEquals(0, getCounterValue("getDataNodeIdsMismatchCount"));

      clusterManager.getDataNodeId(dataNodeId.getHostname(), dataNodeId.getPort());
      assertEquals(0, getCounterValue("getDataNodeIdMismatchCount"));
    }
  }

  /**
   * Test {@link AmbryDataNode}, {@link AmbryDisk}, {@link AmbryPartition} and {@link AmbryReplica}.
   * @throws Exception
   */
  @Test
  public void helixClusterManagerComponentsTest() throws Exception {
    if (useComposite) {
      return;
    }
    String hostName = "localhost";
    int portNum = 2000;
    long rackId = 1;
    int sslPortNum = 3000;
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", hostName);
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.ssl.enabled.datacenters", "DC1");
    ClusterMapConfig clusterMapConfig1 = new ClusterMapConfig(new VerifiableProperties(props));
    props.setProperty("clustermap.datacenter.name", "DC1");
    ClusterMapConfig clusterMapConfig2 = new ClusterMapConfig(new VerifiableProperties(props));

    // AmbryDataNode test
    try {
      new AmbryDataNode("DC1", clusterMapConfig2, hostName, portNum, rackId, null);
      fail("Datanode construction should have failed when SSL is enabled and SSL port is null");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      new AmbryDataNode("DC1", clusterMapConfig1, hostName, MAX_PORT + 1, rackId, null);
      fail("Datanode construction should have failed when port num is outside the valid range");
    } catch (IllegalArgumentException e) {
      // OK
    }
    AmbryDataNode datanode1 = new AmbryDataNode("DC0", clusterMapConfig1, hostName, portNum, rackId, sslPortNum);
    AmbryDataNode datanode2 = new AmbryDataNode("DC1", clusterMapConfig2, hostName, portNum, rackId, sslPortNum);
    assertEquals(datanode1.getDatacenterName(), "DC0");
    assertEquals(datanode1.getHostname(), hostName);
    assertEquals(datanode1.getPort(), portNum);
    assertEquals(datanode1.getSSLPort(), sslPortNum);
    assertEquals(datanode1.getRackId(), rackId);
    assertTrue(datanode1.hasSSLPort());
    assertEquals(PortType.PLAINTEXT, datanode1.getPortToConnectTo().getPortType());
    assertTrue(datanode2.hasSSLPort());
    assertEquals(PortType.SSL, datanode2.getPortToConnectTo().getPortType());
    assertEquals(HardwareState.AVAILABLE, datanode1.getState());
    datanode1.setState(HardwareState.UNAVAILABLE);
    assertEquals(HardwareState.UNAVAILABLE, datanode1.getState());
    datanode1.setState(HardwareState.AVAILABLE);
    assertEquals(HardwareState.AVAILABLE, datanode1.getState());
    assertFalse(datanode1.compareTo(datanode2) != 0);

    // AmbryDisk tests
    String mountPath = "/mnt/0";
    try {
      new AmbryDisk(clusterMapConfig1, null, mountPath, HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, null, HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, "", HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, "0", HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, mountPath, HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES + 1);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    AmbryDisk disk1 =
        new AmbryDisk(clusterMapConfig1, datanode1, mountPath, HardwareState.AVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
    assertEquals(mountPath, disk1.getMountPath());
    assertEquals(MAX_DISK_CAPACITY_IN_BYTES, disk1.getRawCapacityInBytes());
    assertEquals(HardwareState.AVAILABLE, disk1.getState());
    disk1.setState(HardwareState.UNAVAILABLE);
    assertEquals(HardwareState.UNAVAILABLE, disk1.getState());
    disk1.setState(HardwareState.AVAILABLE);
    assertEquals(HardwareState.AVAILABLE, disk1.getState());
    assertTrue(disk1.getDataNode().equals(datanode1));
    datanode1.setState(HardwareState.UNAVAILABLE);
    assertEquals(HardwareState.UNAVAILABLE, disk1.getState());

    // AmbryPartition tests
    // All partitions are READ_WRITE initially.
    List<? extends PartitionId> partitions = clusterManager.getWritablePartitionIds();
    AmbryPartition partition1 = (AmbryPartition) partitions.get(0);
    AmbryPartition partition2 = (AmbryPartition) partitions.get(1);
    assertTrue(partition1.isEqual(partition1.toPathString()));
    assertTrue(partition1.compareTo(partition1) == 0);
    assertFalse(partition1.isEqual(partition2.toPathString()));
    assertTrue(partition1.compareTo(partition2) != 0);
    partition1.setState(PartitionState.READ_ONLY);
    assertEquals(partitions.size(), clusterManager.getWritablePartitionIds().size() + 1);
    assertFalse(clusterManager.getWritablePartitionIds().contains(partition1));

    // AmbryReplica tests
    try {
      new AmbryReplica(null, disk1, MAX_REPLICA_CAPACITY_IN_BYTES);
      fail("Replica initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    try {
      new AmbryReplica(partition1, null, MAX_REPLICA_CAPACITY_IN_BYTES);
      fail("Replica initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    try {
      new AmbryReplica(partition1, disk1, MAX_REPLICA_CAPACITY_IN_BYTES + 1);
      fail("Replica initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    AmbryReplica replica = partition1.getReplicaIds().get(0);
    assertEquals(replica.getDiskId().getMountPath(), replica.getMountPath());
    List<AmbryReplica> peerReplicas = replica.getPeerReplicaIds();
    assertEquals(3 * dcs.length - 1, peerReplicas.size());
    for (AmbryReplica peerReplica : peerReplicas) {
      assertEquals(replica.getPartitionId(), peerReplica.getPartitionId());
    }
  }

  // Helpers

  /**
   * Get the counter value for the metric in {@link HelixClusterManagerMetrics} with the given suffix.
   * @param suffix the suffix of the metric that distinguishes it from other metrics in the class.
   * @return the value of the counter.
   */
  private long getCounterValue(String suffix) {
    return counters.get(HelixClusterManager.class.getName() + "." + suffix).getCount();
  }

  /**
   * Get the gauge value for the metric in {@link HelixClusterManagerMetrics} with the given suffix.
   * @param suffix the suffix of the metric that distinguishes it from other metrics in the class.
   * @param type the type of the gauge.
   * @return the value of the gauge.
   */
  private <T> T getGaugeValue(String suffix, Class<T> type) {
    return type.cast(gauges.get(HelixClusterManager.class.getName() + "." + suffix).getValue());
  }

  /**
   * Tests that the writable partitions returned by the {@link HelixClusterManager} is the same as the writable
   * partitions in the cluster.
   */
  private void testWritablePartitions() {
    Set<String> writableInClusterManager = new HashSet<>();
    for (PartitionId partition : clusterManager.getWritablePartitionIds()) {
      String partitionStr =
          useComposite ? ((Partition) partition).toPathString() : ((AmbryPartition) partition).toPathString();
      writableInClusterManager.add(partitionStr);
    }
    Set<String> writableInCluster = helixCluster.getUpPartitions();
    if (writableInCluster.isEmpty()) {
      writableInCluster = helixCluster.getPartitions();
    }
    assertEquals(writableInCluster, writableInClusterManager);
  }

  /**
   * Tests that the replica count and replica to partition id mappings as reported by the cluster manager is the same as
   * those in the cluster.
   */
  private void testPartitionReplicaConsistency() throws Exception {
    for (PartitionId partition : clusterManager.getWritablePartitionIds()) {
      assertEquals(partition.getReplicaIds().size(), testPartitionLayout.getTotalReplicaCount());
      InputStream partitionStream = new ByteBufferInputStream(ByteBuffer.wrap(partition.getBytes()));
      PartitionId fetchedPartition = clusterManager.getPartitionIdFromStream(partitionStream);
      assertEquals(partition, fetchedPartition);
    }
  }

  /**
   * Test that invalid partition id deserialization fails as expected.
   */
  private void testInvalidPartitionId() {
    PartitionId partition = clusterManager.getWritablePartitionIds().get(0);
    try {
      byte[] fakePartition = Arrays.copyOf(partition.getBytes(), partition.getBytes().length);
      for (int i = fakePartition.length; i > fakePartition.length - Long.SIZE / Byte.SIZE; i--) {
        fakePartition[i - 1] = (byte) 0xff;
      }
      InputStream partitionStream = new ByteBufferInputStream(ByteBuffer.allocate(fakePartition.length));
      clusterManager.getPartitionIdFromStream(partitionStream);
      fail("partition id deserialization should have failed");
    } catch (IOException e) {
      // OK
    }
  }

  /**
   * Test clustermap interface methods related to datanodes and datacenter.
   */
  private void testDatacenterDatanodeReplicas() {
    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      assertTrue(clusterManager.hasDatacenter(datacenter.getName()));
      for (DataNode dataNode : datacenter.getDataNodes()) {
        DataNodeId dataNodeId = clusterManager.getDataNodeId(dataNode.getHostname(), dataNode.getPort());
        assertEquals(dataNode.toString(), dataNodeId.toString());
        if (!useComposite) {
          try {
            clusterManager.getReplicaIds(dataNode);
            fail("HelixClusterManager methods should throw when passed in a static manager datanode");
          } catch (IllegalArgumentException e) {
            // OK
          }
        } else {
          clusterManager.getReplicaIds(dataNode);
        }
        for (ReplicaId replica : clusterManager.getReplicaIds(dataNodeId)) {
          assertEquals(dataNodeId, replica.getDataNodeId());
        }
      }
    }
  }

  /**
   * Assert that the state of datanodes in the cluster manager's view are consistent with their actual states in the
   * cluster.
   */
  private void assertStateEquivalency() {
    Set<String> upInstancesInCluster = helixCluster.getUpInstances();
    Set<String> downInstancesInCluster = helixCluster.getDownInstances();

    Set<String> upInstancesInClusterManager = new HashSet<>();
    Set<String> downInstancesInClusterManager = new HashSet<>();
    for (DataNodeId dataNode : clusterManager.getDataNodeIds()) {
      if (dataNode.getState() == HardwareState.UNAVAILABLE) {
        downInstancesInClusterManager.add(ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort()));
      } else {
        upInstancesInClusterManager.add(ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort()));
      }
    }
    assertEquals(downInstancesInCluster, downInstancesInClusterManager);
    assertEquals(upInstancesInCluster, upInstancesInClusterManager);
    testWritablePartitions();
  }

  /**
   * A Mock implementaion of {@link HelixFactory} that returns the {@link MockHelixManager}
   */
  private static class MockHelixManagerFactory extends HelixFactory {
    private final MockHelixCluster helixCluster;

    /**
     * Construct this factory
     * @param helixCluster the {@link MockHelixCluster} that this factory's manager will be associated with.
     */
    MockHelixManagerFactory(MockHelixCluster helixCluster) {
      this.helixCluster = helixCluster;
    }

    /**
     * Return a {@link MockHelixManager}
     * @param clusterName the name of the cluster for which the manager is to be gotten.
     * @param instanceName the name of the instance on whose behalf the manager is to be gotten.
     * @param instanceType the {@link InstanceType} of the requester.
     * @param zkAddr the address identifying the zk service to which this request is to be made.
     * @return the {@link MockHelixManager}
     */
    HelixManager getZKHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddr) {
      if (helixCluster.getZkAddrs().contains(zkAddr)) {
        return new MockHelixManager(instanceName, instanceType, zkAddr, helixCluster);
      } else {
        throw new IllegalArgumentException("Invalid ZkAddr");
      }
    }
  }
}

