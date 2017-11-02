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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;


/**
 * Tests {@link StaticClusterManager} class.
 */
public class StaticClusterManagerTest {
  @Rule
  public org.junit.rules.TemporaryFolder folder = new TemporaryFolder();

  // Useful for understanding partition layout affect on free capacity across all hardware.
  public String freeCapacityDump(StaticClusterManager clusterMapManager, HardwareLayout hardwareLayout) {
    StringBuilder sb = new StringBuilder();
    sb.append("Free space dump for cluster.").append(System.getProperty("line.separator"));
    sb.append(hardwareLayout.getClusterName())
        .append(" : ")
        .append(clusterMapManager.getUnallocatedRawCapacityInBytes())
        .append(System.getProperty("line.separator"));
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      sb.append("\t")
          .append(datacenter)
          .append(" : ")
          .append(clusterMapManager.getUnallocatedRawCapacityInBytes(datacenter))
          .append(System.getProperty("line.separator"));
      for (DataNode dataNode : datacenter.getDataNodes()) {
        sb.append("\t\t")
            .append(dataNode)
            .append(" : ")
            .append(clusterMapManager.getUnallocatedRawCapacityInBytes(dataNode))
            .append(System.getProperty("line.separator"));
        for (Disk disk : dataNode.getDisks()) {
          sb.append("\t\t\t")
              .append(disk)
              .append(" : ")
              .append(clusterMapManager.getUnallocatedRawCapacityInBytes(disk))
              .append(System.getProperty("line.separator"));
        }
      }
    }
    return sb.toString();
  }

  @Test
  public void clusterMapInterface() throws Exception {
    // Exercise entire clusterMap interface

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    TestUtils.TestPartitionLayout testPartitionLayout = new TestUtils.TestPartitionLayout(testHardwareLayout);
    // add 3 partitions with read_only state.
    testPartitionLayout.partitionState = PartitionState.READ_ONLY;
    testPartitionLayout.addNewPartitions(3);
    testPartitionLayout.partitionState = PartitionState.READ_WRITE;

    Datacenter localDatacenter = testHardwareLayout.getRandomDatacenter();
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "cluster");
    props.setProperty("clustermap.datacenter.name", localDatacenter.getName());
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    ClusterMap clusterMapManager =
        (new StaticClusterAgentsFactory(clusterMapConfig, testPartitionLayout.getPartitionLayout())).getClusterMap();
    for (String metricName : clusterMapManager.getMetricRegistry().getNames()) {
      System.out.println(metricName);
    }

    assertEquals("Incorrect local datacenter ID", localDatacenter.getId(), clusterMapManager.getLocalDatacenterId());
    List<? extends PartitionId> writablePartitionIds = clusterMapManager.getWritablePartitionIds();
    List<? extends PartitionId> partitionIds = clusterMapManager.getAllPartitionIds();
    assertEquals(writablePartitionIds.size(), testPartitionLayout.getPartitionCount() - 3);
    assertEquals(partitionIds.size(), testPartitionLayout.getPartitionCount());
    for (PartitionId partitionId : partitionIds) {
      if (partitionId.getPartitionState().equals(PartitionState.READ_WRITE)) {
        assertTrue("Partition not found in writable set ", writablePartitionIds.contains(partitionId));
      } else {
        assertFalse("READ_ONLY Partition found in writable set ", writablePartitionIds.contains(partitionId));
      }
    }
    for (int i = 0; i < partitionIds.size(); i++) {
      PartitionId partitionId = partitionIds.get(i);
      assertEquals(partitionId.getReplicaIds().size(), testPartitionLayout.getTotalReplicaCount());

      DataInputStream partitionStream =
          new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(partitionId.getBytes())));

      try {
        PartitionId fetchedPartitionId = clusterMapManager.getPartitionIdFromStream(partitionStream);
        assertEquals(partitionId, fetchedPartitionId);
      } catch (IOException e) {
        assertEquals(true, false);
      }
    }

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        DataNodeId dataNodeId = clusterMapManager.getDataNodeId(dataNode.getHostname(), dataNode.getPort());
        assertEquals(dataNodeId, dataNode);
        for (ReplicaId replicaId : clusterMapManager.getReplicaIds(dataNodeId)) {
          assertEquals(dataNodeId, replicaId.getDataNodeId());
        }
      }
    }
  }

  @Test
  public void findDatacenter() throws Exception {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    TestUtils.TestPartitionLayout testPartitionLayout = new TestUtils.TestPartitionLayout(testHardwareLayout);

    StaticClusterManager clusterMapManager = (new StaticClusterAgentsFactory(TestUtils.getDummyConfig(),
        testPartitionLayout.getPartitionLayout())).getClusterMap();

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      assertTrue(clusterMapManager.hasDatacenter(datacenter.getName()));
      assertFalse(clusterMapManager.hasDatacenter(datacenter.getName() + datacenter.getName()));
    }
  }

  @Test
  public void addNewPartition() throws Exception {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(TestUtils.getDummyConfig(), partitionLayout)).getClusterMap();
    int dcCount = testHardwareLayout.getDatacenterCount();

    List<PartitionId> partitionIds = clusterMapManager.getWritablePartitionIds();
    assertEquals(partitionIds.size(), 0);
    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(3), 100 * 1024 * 1024 * 1024L);
    partitionIds = clusterMapManager.getWritablePartitionIds();
    assertEquals(partitionIds.size(), 1);
    PartitionId partitionId = partitionIds.get(0);
    assertEquals(partitionId.getReplicaIds().size(), 3 * dcCount);
  }

  @Test
  public void nonRackAwareAllocationTest() throws Exception {
    int replicaCountPerDataCenter = 2;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(TestUtils.getDummyConfig(), partitionLayout)).getClusterMap();
    List<PartitionId> allocatedPartitions;

    try {
      // Test with retryIfNotRackAware set to false, this should throw an exception
      clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes, false);
      Assert.fail("allocatePartitions should not succeed when datacenters are missing rack info "
          + "and retryIfNotRackAware is false");
    } catch (IllegalArgumentException e) {
      // This should be thrown
    }
    // Allocate five partitions that fit within cluster's capacity
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getWritablePartitionIds().size(), 5);

    // Allocate "too many" partitions (1M) to exhaust capacity. Capacity is not exhausted evenly across nodes so some
    // "free" but unusable capacity may be left after trying to allocate these partitions.
    allocatedPartitions =
        clusterMapManager.allocatePartitions(1000 * 1000, replicaCountPerDataCenter, replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size() + 5, clusterMapManager.getWritablePartitionIds().size());
    System.out.println(freeCapacityDump(clusterMapManager, testHardwareLayout.getHardwareLayout()));

    // Capacity is already exhausted...
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 0);
  }

  @Test
  public void rackAwareAllocationTest() throws Exception {
    int replicaCountPerDataCenter = 3;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha", true);
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(TestUtils.getDummyConfig(), partitionLayout)).getClusterMap();
    List<PartitionId> allocatedPartitions;

    // Allocate five partitions that fit within cluster's capacity
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getWritablePartitionIds().size(), 5);
    checkRackUsage(allocatedPartitions);
    checkNumReplicasPerDatacenter(allocatedPartitions, replicaCountPerDataCenter);

    // Allocate "too many" partitions (1M) to exhaust capacity. Capacity is not exhausted evenly across nodes so some
    // "free" but unusable capacity may be left after trying to allocate these partitions.
    allocatedPartitions =
        clusterMapManager.allocatePartitions(1000 * 1000, replicaCountPerDataCenter, replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size() + 5, clusterMapManager.getWritablePartitionIds().size());
    System.out.println(freeCapacityDump(clusterMapManager, testHardwareLayout.getHardwareLayout()));
    checkRackUsage(allocatedPartitions);

    // Capacity is already exhausted...
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size(), 0);
  }

  @Test
  public void rackAwareOverAllocationTest() throws Exception {
    int replicaCountPerDataCenter = 4;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha", true);
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(TestUtils.getDummyConfig(), partitionLayout)).getClusterMap();
    List<PartitionId> allocatedPartitions;
    // Require more replicas than there are racks
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size(), 5);
    checkNumReplicasPerDatacenter(allocatedPartitions, 3);
    checkRackUsage(allocatedPartitions);

    // Test with retryIfNotRackAware enabled.  We should be able to allocate 4 replicas per datacenter b/c we no
    // longer require unique racks
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 5);
    checkNumReplicasPerDatacenter(allocatedPartitions, 4);
  }

  @Test
  public void capacities() throws Exception {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(TestUtils.getDummyConfig(), partitionLayout)).getClusterMap();

    // Confirm initial capacity is available for use
    long raw = clusterMapManager.getRawCapacityInBytes();
    long allocated = clusterMapManager.getAllocatedRawCapacityInBytes();
    long free = clusterMapManager.getUnallocatedRawCapacityInBytes();

    assertEquals(free, raw);
    assertEquals(allocated, 0);

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        long dataNodeFree = clusterMapManager.getUnallocatedRawCapacityInBytes(dataNode);
        assertEquals(dataNodeFree, testHardwareLayout.getDiskCapacityInBytes() * testHardwareLayout.getDiskCount());
        for (Disk disk : dataNode.getDisks()) {
          long diskFree = clusterMapManager.getUnallocatedRawCapacityInBytes(disk);
          assertEquals(diskFree, testHardwareLayout.getDiskCapacityInBytes());
        }
      }
    }

    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(3), 100 * 1024 * 1024 * 1024L);
    int dcCount = testHardwareLayout.getDatacenterCount();

    // Confirm 100GB has been used on 3 distinct DataNodes / Disks in each datacenter.
    assertEquals(clusterMapManager.getRawCapacityInBytes(), raw);
    assertEquals(clusterMapManager.getAllocatedRawCapacityInBytes(), dcCount * 3 * 100 * 1024 * 1024 * 1024L);
    assertEquals(clusterMapManager.getUnallocatedRawCapacityInBytes(),
        free - (dcCount * 3 * 100 * 1024 * 1024 * 1024L));

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        long dataNodeFree = clusterMapManager.getUnallocatedRawCapacityInBytes(dataNode);
        assertTrue(dataNodeFree <= testHardwareLayout.getDiskCapacityInBytes() * testHardwareLayout.getDiskCount());
        assertTrue(
            dataNodeFree >= testHardwareLayout.getDiskCapacityInBytes() * testHardwareLayout.getDiskCount() - (100
                * 1024 * 1024 * 1024L));
        for (Disk disk : dataNode.getDisks()) {
          long diskFree = clusterMapManager.getUnallocatedRawCapacityInBytes(disk);
          assertTrue(diskFree <= testHardwareLayout.getDiskCapacityInBytes());
          assertTrue(diskFree >= testHardwareLayout.getDiskCapacityInBytes() - (100 * 1024 * 1024 * 1024L));
        }
      }
    }
  }

  @Test
  public void persistAndReadBack() throws Exception {
    String tmpDir = folder.getRoot().getPath();
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "dc1");
    props.setProperty("clustermap.host.name", "localhost");

    String hardwareLayoutSer = tmpDir + "/hardwareLayoutSer.json";
    String partitionLayoutSer = tmpDir + "/partitionLayoutSer.json";
    String hardwareLayoutDe = tmpDir + "/hardwareLayoutDe.json";
    String partitionLayoutDe = tmpDir + "/partitionLayoutDe.json";

    StaticClusterManager clusterMapManagerSer = TestUtils.getTestClusterMap();
    clusterMapManagerSer.persist(hardwareLayoutSer, partitionLayoutSer);

    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    StaticClusterManager clusterMapManagerDe =
        (new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutSer, partitionLayoutSer)).getClusterMap();
    assertEquals(clusterMapManagerSer, clusterMapManagerDe);

    clusterMapManagerDe.persist(hardwareLayoutDe, partitionLayoutDe);
    StaticClusterManager clusterMapManagerDeDe =
        (new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutDe, partitionLayoutDe)).getClusterMap();
    assertEquals(clusterMapManagerDe, clusterMapManagerDeDe);
  }

  @Test
  public void validateSimpleConfig() throws Exception {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", "OneDiskOneReplica");
    props.setProperty("clustermap.datacenter.name", "Datacenter");
    props.setProperty("clustermap.host.name", "localhost");
    String configDir = System.getProperty("user.dir");
    // intelliJ and gradle return different values for user.dir: gradle includes the sub-project directory. To handle
    // this, we check the string suffix for the sub-project directory and append ".." to correctly set configDir.
    if (configDir.endsWith("ambry-clustermap")) {
      configDir += "/..";
    }
    configDir += "/config";
    String hardwareLayoutSer = configDir + "/HardwareLayout.json";
    String partitionLayoutSer = configDir + "/PartitionLayout.json";
    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(new ClusterMapConfig(new VerifiableProperties(props)), hardwareLayoutSer,
            partitionLayoutSer)).getClusterMap();
    assertEquals(clusterMapManager.getWritablePartitionIds().size(), 1);
    assertEquals(clusterMapManager.getUnallocatedRawCapacityInBytes(), 10737418240L);
    assertNotNull(clusterMapManager.getDataNodeId("localhost", 6667));
  }

  /**
   * Verify that the partitions in the list are on unique racks for each datacenter.
   *
   * @param allocatedPartitions the list of partitions to check
   */
  private static void checkRackUsage(List<PartitionId> allocatedPartitions) {
    for (PartitionId partition : allocatedPartitions) {
      Map<String, Set<Long>> rackSetByDatacenter = new HashMap<>();
      for (ReplicaId replica : partition.getReplicaIds()) {
        String datacenter = replica.getDataNodeId().getDatacenterName();
        Set<Long> rackSet = rackSetByDatacenter.get(datacenter);
        if (rackSet == null) {
          rackSet = new HashSet<>();
          rackSetByDatacenter.put(datacenter, rackSet);
        }

        long rackId = replica.getDataNodeId().getRackId();
        if (rackId >= 0) {
          assertFalse("Allocation was not on unique racks", rackSet.contains(rackId));
          rackSet.add(rackId);
        }
      }
    }
  }

  /**
   * Verify that the partitions in the list have {@code numReplicas} per datacenter
   *
   * @param allocatedPartitions the list of partitions to check
   * @param numReplicas how many replicas a partition should have in each datacenter
   */
  private static void checkNumReplicasPerDatacenter(List<PartitionId> allocatedPartitions, int numReplicas) {
    for (PartitionId partition : allocatedPartitions) {
      Map<String, Integer> numReplicasMap = new HashMap<>();
      for (ReplicaId replica : partition.getReplicaIds()) {
        String datacenter = replica.getDataNodeId().getDatacenterName();
        Integer replicasInDatacenter = numReplicasMap.containsKey(datacenter) ? numReplicasMap.get(datacenter) : 0;
        numReplicasMap.put(datacenter, replicasInDatacenter + 1);
      }
      for (int replicasInDatacenter : numReplicasMap.values()) {
        assertEquals("Datacenter does not have expected number of replicas", numReplicas, replicasInDatacenter);
      }
    }
  }
}

