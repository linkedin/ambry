package com.github.ambry.clustermap;

import org.json.JSONException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ClusterMapManager} class.
 */
public class ClusterMapManagerTest {
  // TODO: Add separate ClusterMap test that just exercises clustermap API. Consider doing same for DataNodeId,
  // PartitionId, and ReplicaId?

  @Rule
  public org.junit.rules.TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void clusterMapInterface() throws JSONException {
    // Exercise entire clusterMap interface

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    TestUtils.TestPartitionLayout testPartitionLayout = new TestUtils.TestPartitionLayout(testHardwareLayout);

    ClusterMapManager clusterMapManager = new ClusterMapManager(testPartitionLayout.getPartitionLayout());

    assertEquals(clusterMapManager.getWritablePartitionIdsCount(),testPartitionLayout.getPartitionCount());
    for(int i=0; i < clusterMapManager.getWritablePartitionIdsCount(); i++) {
      PartitionId partitionId = clusterMapManager.getWritablePartitionIdAt(i);
      assertEquals(partitionId.getReplicaIds().size(), testPartitionLayout.getReplicaCount());

      PartitionId fetchedPartitionId = clusterMapManager.getPartitionIdFromBytes(partitionId.getBytes());
      assertEquals(partitionId, fetchedPartitionId);
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
  public void addNewPartition() throws JSONException {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    ClusterMapManager clusterMapManager = new ClusterMapManager(partitionLayout);

    assertEquals(clusterMapManager.getWritablePartitionIdsCount(),0);
    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(6), 100);
    assertEquals(clusterMapManager.getWritablePartitionIdsCount(), 1);
    PartitionId partitionId = clusterMapManager.getWritablePartitionIdAt(0);
    assertEquals(partitionId.getReplicaIds().size(), 6);
  }

  @Test
  public void bestEffortAllocation() throws JSONException  {
    int replicaCountPerDataCenter = 2;
    long replicaCapacityGB = 100;

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    ClusterMapManager clusterMapManager = new ClusterMapManager(partitionLayout);
    List<PartitionId> allocatedPartitions;

    // Allocate a five partitions that fit within cluster's capacity
    allocatedPartitions = clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getWritablePartitionIds().size(), 5);

    // Allocate "too many" partitions (1M) to exhaust capacity.
    allocatedPartitions = clusterMapManager.allocatePartitions(1000*1000, replicaCountPerDataCenter, replicaCapacityGB);
    assertEquals(allocatedPartitions.size() + 5, clusterMapManager.getWritablePartitionIds().size());
    assertEquals(clusterMapManager.getFreeCapacityGB(), 0);

    // Capacity is already exhausted...
    allocatedPartitions = clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 0);
  }

  @Test
  public void capacities() throws JSONException {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    ClusterMapManager clusterMapManager = new ClusterMapManager(partitionLayout);

    // Confirm initial capacity is available for use
    long raw = clusterMapManager.getRawCapacityGB();
    long allocated = clusterMapManager.getAllocatedCapacityGB();
    long free = clusterMapManager.getFreeCapacityGB();

    assertEquals(free, raw);
    assertEquals(allocated, 0);

    for(Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      for(DataNode dataNode : datacenter.getDataNodes()) {
        long dataNodeFree = clusterMapManager.getFreeCapacityGB(dataNode);
        assertEquals(dataNodeFree, testHardwareLayout.getDiskCapacityGB() * testHardwareLayout.getDiskCount());
        for(Disk disk : dataNode.getDisks()) {
          long diskFree = clusterMapManager.getFreeCapacityGB(disk);
          assertEquals(diskFree, testHardwareLayout.getDiskCapacityGB());
        }
      }
    }

    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(6), 100);

    // Confirm 100GB has been used on 6 distinct DataNodes / Disks.
    assertEquals(clusterMapManager.getRawCapacityGB(), raw);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 6 * 100);
    assertEquals(clusterMapManager.getFreeCapacityGB(), free - (6*100));

    for(Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      for(DataNode dataNode : datacenter.getDataNodes()) {
        long dataNodeFree = clusterMapManager.getFreeCapacityGB(dataNode);
        assertTrue(dataNodeFree <= testHardwareLayout.getDiskCapacityGB() * testHardwareLayout.getDiskCount());
        assertTrue(dataNodeFree >= testHardwareLayout.getDiskCapacityGB() * testHardwareLayout.getDiskCount() - 100);
        for(Disk disk : dataNode.getDisks()) {
          long diskFree = clusterMapManager.getFreeCapacityGB(disk);
          assertTrue(diskFree <= testHardwareLayout.getDiskCapacityGB());
          assertTrue(diskFree >= testHardwareLayout.getDiskCapacityGB() - 100);
        }
      }
    }
  }

  @Test
  public void persistAndReadBack() throws JSONException, IOException{
    String tmpDir = folder.getRoot().getPath();
    // TODO: Confirm that TemporaryFolder is doing what we want for all devs.
    System.err.println("TODO: CONFIRM THIS DIRECTORY IS DELETED = " + tmpDir);

    String hardwareLayoutSer = tmpDir + "/hardwareLayoutSer.json";
    String partitionLayoutSer = tmpDir + "/partitionLayoutSer.json";
    String hardwareLayoutDe = tmpDir + "/hardwareLayoutDe.json";
    String partitionLayoutDe = tmpDir + "/partitionLayoutDe.json";

    ClusterMapManager clusterMapManagerSer = TestUtils.getTestClusterMap();
    clusterMapManagerSer.persist(hardwareLayoutSer, partitionLayoutSer);

    ClusterMapManager clusterMapManagerDe = new ClusterMapManager(hardwareLayoutSer, partitionLayoutSer);
    assertEquals(clusterMapManagerSer, clusterMapManagerDe);

    clusterMapManagerDe.persist(hardwareLayoutDe, partitionLayoutDe);
    ClusterMapManager clusterMapManagerDeDe = new ClusterMapManager(hardwareLayoutDe, partitionLayoutDe);
    assertEquals(clusterMapManagerDe, clusterMapManagerDeDe);
  }
}

