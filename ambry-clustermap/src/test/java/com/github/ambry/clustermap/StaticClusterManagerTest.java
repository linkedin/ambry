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
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import junit.framework.Assert;
import org.json.JSONException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests {@link StaticClusterManager} class.
 */
public class StaticClusterManagerTest {
  private final ClusterMapConfig clusterMapConfig;
  @Rule
  public org.junit.rules.TemporaryFolder folder = new TemporaryFolder();

  public StaticClusterManagerTest() {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "cluster");
    props.setProperty("clustermap.datacenter.name", "dc1");
    clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
  }

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
  public void clusterMapInterface() {
    // Exercise entire clusterMap interface

    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    TestPartitionLayout testPartitionLayout = new TestPartitionLayout(testHardwareLayout, null);
    // add 3 partitions with read_only state.
    testPartitionLayout.partitionState = PartitionState.READ_ONLY;
    testPartitionLayout.addNewPartitions(3, DEFAULT_PARTITION_CLASS, testPartitionLayout.partitionState, null);
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
    List<? extends PartitionId> writablePartitionIds = clusterMapManager.getWritablePartitionIds(null);
    List<? extends PartitionId> partitionIds = clusterMapManager.getAllPartitionIds(null);
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
  public void findDatacenter() {
    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    TestPartitionLayout testPartitionLayout = new TestPartitionLayout(testHardwareLayout, null);

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(getDummyConfig(), testPartitionLayout.getPartitionLayout())).getClusterMap();

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      assertTrue(clusterMapManager.hasDatacenter(datacenter.getName()));
      assertFalse(clusterMapManager.hasDatacenter(datacenter.getName() + datacenter.getName()));
    }
  }

  @Test
  public void addNewPartition() {
    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), clusterMapConfig);

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(getDummyConfig(), partitionLayout)).getClusterMap();
    int dcCount = testHardwareLayout.getDatacenterCount();

    List<PartitionId> partitionIds = clusterMapManager.getWritablePartitionIds(null);
    assertEquals(partitionIds.size(), 0);
    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(3), 100 * 1024 * 1024 * 1024L,
        MockClusterMap.DEFAULT_PARTITION_CLASS);
    partitionIds = clusterMapManager.getWritablePartitionIds(null);
    assertEquals(partitionIds.size(), 1);
    PartitionId partitionId = partitionIds.get(0);
    assertEquals(partitionId.getReplicaIds().size(), 3 * dcCount);
  }

  @Test
  public void nonRackAwareAllocationTest() {
    int replicaCountPerDataCenter = 2;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), clusterMapConfig);

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(getDummyConfig(), partitionLayout)).getClusterMap();
    List<PartitionId> allocatedPartitions;

    try {
      // Test with retryIfNotRackAware set to false, this should throw an exception
      clusterMapManager.allocatePartitions(5, MockClusterMap.DEFAULT_PARTITION_CLASS, replicaCountPerDataCenter,
          replicaCapacityInBytes, false);
      Assert.fail("allocatePartitions should not succeed when datacenters are missing rack info "
          + "and retryIfNotRackAware is false");
    } catch (IllegalArgumentException e) {
      // This should be thrown
    }
    // Allocate five partitions that fit within cluster's capacity
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, MockClusterMap.DEFAULT_PARTITION_CLASS, replicaCountPerDataCenter,
            replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getWritablePartitionIds(null).size(), 5);

    // Allocate "too many" partitions (1M) to exhaust capacity. Capacity is not exhausted evenly across nodes so some
    // "free" but unusable capacity may be left after trying to allocate these partitions.
    allocatedPartitions = clusterMapManager.allocatePartitions(1000 * 1000, MockClusterMap.DEFAULT_PARTITION_CLASS,
        replicaCountPerDataCenter, replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size() + 5, clusterMapManager.getWritablePartitionIds(null).size());
    System.out.println(freeCapacityDump(clusterMapManager, testHardwareLayout.getHardwareLayout()));

    // Capacity is already exhausted...
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, MockClusterMap.DEFAULT_PARTITION_CLASS, replicaCountPerDataCenter,
            replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 0);
  }

  @Test
  public void rackAwareAllocationTest() {
    int replicaCountPerDataCenter = 3;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha", true);
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), clusterMapConfig);

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(getDummyConfig(), partitionLayout)).getClusterMap();
    List<PartitionId> allocatedPartitions;

    // Allocate five partitions that fit within cluster's capacity
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, MockClusterMap.DEFAULT_PARTITION_CLASS, replicaCountPerDataCenter,
            replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getWritablePartitionIds(null).size(), 5);
    checkRackUsage(allocatedPartitions);
    checkNumReplicasPerDatacenter(allocatedPartitions, replicaCountPerDataCenter);

    // Allocate "too many" partitions (1M) to exhaust capacity. Capacity is not exhausted evenly across nodes so some
    // "free" but unusable capacity may be left after trying to allocate these partitions.
    allocatedPartitions = clusterMapManager.allocatePartitions(1000 * 1000, MockClusterMap.DEFAULT_PARTITION_CLASS,
        replicaCountPerDataCenter, replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size() + 5, clusterMapManager.getWritablePartitionIds(null).size());
    System.out.println(freeCapacityDump(clusterMapManager, testHardwareLayout.getHardwareLayout()));
    checkRackUsage(allocatedPartitions);

    // Capacity is already exhausted...
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, MockClusterMap.DEFAULT_PARTITION_CLASS, replicaCountPerDataCenter,
            replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size(), 0);
  }

  @Test
  public void rackAwareOverAllocationTest() {
    int replicaCountPerDataCenter = 4;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha", true);
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), clusterMapConfig);

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(getDummyConfig(), partitionLayout)).getClusterMap();
    List<PartitionId> allocatedPartitions;
    // Require more replicas than there are racks
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, MockClusterMap.DEFAULT_PARTITION_CLASS, replicaCountPerDataCenter,
            replicaCapacityInBytes, false);
    assertEquals(allocatedPartitions.size(), 5);
    checkNumReplicasPerDatacenter(allocatedPartitions, 3);
    checkRackUsage(allocatedPartitions);

    // Test with retryIfNotRackAware enabled.  We should be able to allocate 4 replicas per datacenter b/c we no
    // longer require unique racks
    allocatedPartitions =
        clusterMapManager.allocatePartitions(5, MockClusterMap.DEFAULT_PARTITION_CLASS, replicaCountPerDataCenter,
            replicaCapacityInBytes, true);
    assertEquals(allocatedPartitions.size(), 5);
    checkNumReplicasPerDatacenter(allocatedPartitions, 4);
  }

  @Test
  public void capacities() {
    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout(), clusterMapConfig);

    StaticClusterManager clusterMapManager =
        (new StaticClusterAgentsFactory(getDummyConfig(), partitionLayout)).getClusterMap();

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

    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(3), 100 * 1024 * 1024 * 1024L,
        MockClusterMap.DEFAULT_PARTITION_CLASS);
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

    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    StaticClusterManager clusterMapManagerSer = getTestClusterMap(clusterMapConfig);
    clusterMapManagerSer.persist(hardwareLayoutSer, partitionLayoutSer);

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
    assertEquals(clusterMapManager.getWritablePartitionIds(null).size(), 1);
    assertEquals(clusterMapManager.getUnallocatedRawCapacityInBytes(), 10737418240L);
    assertNotNull(clusterMapManager.getDataNodeId("localhost", 6667));
  }

  /**
   * Tests for {@link PartitionLayout#getPartitions(String)} and {@link PartitionLayout#getWritablePartitions(String)}.
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void getPartitionsTest() throws IOException, JSONException {
    String specialPartitionClass = "specialPartitionClass";
    TestHardwareLayout hardwareLayout = new TestHardwareLayout("Alpha");
    String dc = hardwareLayout.getRandomDatacenter().getName();
    TestPartitionLayout testPartitionLayout = new TestPartitionLayout(hardwareLayout, dc);
    assertTrue("There should be more than 1 replica per partition in each DC for this test to work",
        testPartitionLayout.replicaCountPerDc > 1);
    PartitionRangeCheckParams defaultRw =
        new PartitionRangeCheckParams(0, testPartitionLayout.partitionCount, DEFAULT_PARTITION_CLASS,
            PartitionState.READ_WRITE);
    // add 15 RW partitions for the special class
    PartitionRangeCheckParams specialRw =
        new PartitionRangeCheckParams(defaultRw.rangeEnd + 1, 15, specialPartitionClass, PartitionState.READ_WRITE);
    testPartitionLayout.addNewPartitions(specialRw.count, specialPartitionClass, PartitionState.READ_WRITE, dc);
    // add 10 RO partitions for the default class
    PartitionRangeCheckParams defaultRo =
        new PartitionRangeCheckParams(specialRw.rangeEnd + 1, 10, DEFAULT_PARTITION_CLASS, PartitionState.READ_ONLY);
    testPartitionLayout.addNewPartitions(defaultRo.count, DEFAULT_PARTITION_CLASS, PartitionState.READ_ONLY, dc);
    // add 5 RO partitions for the special class
    PartitionRangeCheckParams specialRo =
        new PartitionRangeCheckParams(defaultRo.rangeEnd + 1, 5, specialPartitionClass, PartitionState.READ_ONLY);
    testPartitionLayout.addNewPartitions(specialRo.count, specialPartitionClass, PartitionState.READ_ONLY, dc);

    PartitionLayout partitionLayout = testPartitionLayout.getPartitionLayout();

    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "cluster");
    props.setProperty("clustermap.datacenter.name", dc);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    File tempDir = Files.createTempDirectory("helixClusterManager-" + new Random().nextInt(1000)).toFile();
    String tempDirPath = tempDir.getAbsolutePath();
    String hardwareLayoutPath = tempDirPath + File.separator + "hardwareLayoutTest.json";
    String partitionLayoutPath = tempDirPath + File.separator + "partitionLayoutTest.json";
    Utils.writeJsonObjectToFile(hardwareLayout.getHardwareLayout().toJSONObject(), hardwareLayoutPath);
    Utils.writeJsonObjectToFile(partitionLayout.toJSONObject(), partitionLayoutPath);
    ClusterMap clusterMapManager =
        (new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutPath, partitionLayoutPath)).getClusterMap();

    // "good" cases for getPartitions() and getWritablePartitions() only
    // getPartitions(), class null
    List<? extends PartitionId> returnedPartitions = clusterMapManager.getAllPartitionIds(null);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(defaultRw, defaultRo, specialRw, specialRo));
    // getWritablePartitions(), class null
    returnedPartitions = clusterMapManager.getWritablePartitionIds(null);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(defaultRw, specialRw));

    // getPartitions(), class default
    returnedPartitions = clusterMapManager.getAllPartitionIds(DEFAULT_PARTITION_CLASS);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(defaultRw, defaultRo));
    // getWritablePartitions(), class default
    returnedPartitions = clusterMapManager.getWritablePartitionIds(DEFAULT_PARTITION_CLASS);
    checkReturnedPartitions(returnedPartitions, Collections.singletonList(defaultRw));

    // getPartitions(), class special
    returnedPartitions = clusterMapManager.getAllPartitionIds(specialPartitionClass);
    checkReturnedPartitions(returnedPartitions, Arrays.asList(specialRw, specialRo));
    // getWritablePartitions(), class special
    returnedPartitions = clusterMapManager.getWritablePartitionIds(specialPartitionClass);
    checkReturnedPartitions(returnedPartitions, Collections.singletonList(specialRw));

    // to test the dc affinity, we pick one datanode from "dc" and insert 1 replica for part1 (special class) in "dc"
    // and make sure that it is returned in getPartitions() but not in getWritablePartitions() (because all the other
    // partitions have more than 1 replica in "dc").
    DataNode dataNode = hardwareLayout.getRandomDataNodeFromDc(dc);
    Partition partition =
        partitionLayout.addNewPartition(dataNode.getDisks().subList(0, 1), testPartitionLayout.replicaCapacityInBytes,
            specialPartitionClass);
    Utils.writeJsonObjectToFile(partitionLayout.toJSONObject(), partitionLayoutPath);
    clusterMapManager =
        (new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
    PartitionRangeCheckParams extraPartCheckParams =
        new PartitionRangeCheckParams(specialRo.rangeEnd + 1, 1, specialPartitionClass, PartitionState.READ_WRITE);
    // getPartitions(), class special
    returnedPartitions = clusterMapManager.getAllPartitionIds(specialPartitionClass);
    assertTrue("Added partition should exist in returned partitions", returnedPartitions.contains(partition));
    checkReturnedPartitions(returnedPartitions, Arrays.asList(specialRw, specialRo, extraPartCheckParams));
    // getWritablePartitions(), class special
    returnedPartitions = clusterMapManager.getWritablePartitionIds(specialPartitionClass);
    assertFalse("Added partition should not exist in returned partitions", returnedPartitions.contains(partition));
    checkReturnedPartitions(returnedPartitions, Collections.singletonList(specialRw));
  }

  /**
   * Test that {@link StaticClusterManager#onReplicaEvent(ReplicaId, ReplicaEventType)} works as expected in the presence
   * of various types of server/replica events. This test also verifies the states of datanode, disk and replica are changed
   * correctly based on server event.
   */
  @Test
  public void onReplicaEventTest() {
    TestHardwareLayout testHardwareLayout = new TestHardwareLayout("Alpha");
    TestPartitionLayout testPartitionLayout = new TestPartitionLayout(testHardwareLayout, null);
    ClusterMap clusterMapManager =
        (new StaticClusterAgentsFactory(clusterMapConfig, testPartitionLayout.getPartitionLayout())).getClusterMap();
    // Test configuration: we select the disk from one datanode and select the replica on that disk

    // Initial state: only disk is down; Server event: Replica_Unavailable; Expected result: disk becomes available again and replica becomes down
    mockServerEventsAndVerify(clusterMapManager, clusterMapConfig,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Down, ResourceState.Replica_Up},
        ServerErrorCode.Replica_Unavailable,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Up, ResourceState.Replica_Down});

    // Initial state: only disk is down; Server event: Temporarily_Disabled; Expected result: disk becomes available again and replica becomes down
    mockServerEventsAndVerify(clusterMapManager, clusterMapConfig,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Down, ResourceState.Replica_Up},
        ServerErrorCode.Temporarily_Disabled,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Up, ResourceState.Replica_Down});

    // Initial state: disk and replica are down; Server event: Replica_Unavailable; Expected result: disk becomes available again
    mockServerEventsAndVerify(clusterMapManager, clusterMapConfig,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Down, ResourceState.Replica_Down},
        ServerErrorCode.Replica_Unavailable,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Up, ResourceState.Replica_Down});

    // Initial state: disk and replica are down; Server event: Temporarily_Disabled; Expected result: disk becomes available again
    mockServerEventsAndVerify(clusterMapManager, clusterMapConfig,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Down, ResourceState.Replica_Down},
        ServerErrorCode.Temporarily_Disabled,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Up, ResourceState.Replica_Down});

    // Initial state: disk and replica are down; Server event: Partition_ReadOnly; Expected result: disk and replica become available again
    mockServerEventsAndVerify(clusterMapManager, clusterMapConfig,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Down, ResourceState.Replica_Down},
        ServerErrorCode.Partition_ReadOnly,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Up, ResourceState.Replica_Up});

    // Initial state: everything is up; Server event: IO_Error; Expected result: disk and replica become unavailable
    mockServerEventsAndVerify(clusterMapManager, clusterMapConfig,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Up, ResourceState.Replica_Up},
        ServerErrorCode.IO_Error,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Down, ResourceState.Replica_Down});

    // Initial state: everything is up; Server event: Disk_Unavailable; Expected result: disk and replica become unavailable
    mockServerEventsAndVerify(clusterMapManager, clusterMapConfig,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Up, ResourceState.Replica_Up},
        ServerErrorCode.Disk_Unavailable,
        new ResourceState[]{ResourceState.Node_Up, ResourceState.Disk_Down, ResourceState.Replica_Down});
  }

  /**
   * Verify that the partitions in the list are on unique racks for each datacenter.
   *
   * @param allocatedPartitions the list of partitions to check
   */
  private static void checkRackUsage(List<PartitionId> allocatedPartitions) {
    for (PartitionId partition : allocatedPartitions) {
      Map<String, Set<String>> rackSetByDatacenter = new HashMap<>();
      for (ReplicaId replica : partition.getReplicaIds()) {
        String datacenter = replica.getDataNodeId().getDatacenterName();
        Set<String> rackSet = rackSetByDatacenter.computeIfAbsent(datacenter, k -> new HashSet<>());

        String rackId = replica.getDataNodeId().getRackId();
        if (rackId != null) {
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
        Integer replicasInDatacenter = numReplicasMap.getOrDefault(datacenter, 0);
        numReplicasMap.put(datacenter, replicasInDatacenter + 1);
      }
      for (int replicasInDatacenter : numReplicasMap.values()) {
        assertEquals("Datacenter does not have expected number of replicas", numReplicas, replicasInDatacenter);
      }
    }
  }
}

