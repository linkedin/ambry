package com.github.ambry.clustermap;

import com.github.ambry.utils.ByteBufferInputStream;
import org.json.JSONException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ClusterMapManager} class.
 */
public class ClusterMapManagerTest {
  @Rule
  public org.junit.rules.TemporaryFolder folder = new TemporaryFolder();

  // Useful for understanding partition layout affect on free capacity across all hardware.
  public String freeCapacityDump(ClusterMapManager clusterMapManager, HardwareLayout hardwareLayout) {
    StringBuilder sb = new StringBuilder();
    sb.append("Free space dump for cluster.").append(System.getProperty("line.separator"));
    sb.append(hardwareLayout.getClusterName()).append(" : ").append(clusterMapManager.getFreeCapacityInBytes())
            .append(System.getProperty("line.separator"));
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      sb.append("\t").append(datacenter).append(" : ").append(clusterMapManager.getFreeCapacityInBytes(datacenter)).append
              (System.getProperty("line.separator"));
      for (DataNode dataNode : datacenter.getDataNodes()) {
        sb.append("\t\t").append(dataNode).append(" : ").append(clusterMapManager.getFreeCapacityInBytes(dataNode)).append
                (System.getProperty("line.separator"));
        for (Disk disk : dataNode.getDisks()) {
          sb.append("\t\t\t").append(disk).append(" : ").append(clusterMapManager.getFreeCapacityInBytes(disk)).append
                  (System.getProperty("line.separator"));
        }
      }
    }
    return sb.toString();
  }

  @Test
  public void clusterMapInterface() throws JSONException {
    // Exercise entire clusterMap interface

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    TestUtils.TestPartitionLayout testPartitionLayout = new TestUtils.TestPartitionLayout(testHardwareLayout);

    ClusterMapManager clusterMapManager = new ClusterMapManager(testPartitionLayout.getPartitionLayout());

    assertEquals(clusterMapManager.getWritablePartitionIdsCount(), testPartitionLayout.getPartitionCount());
    for (int i = 0; i < clusterMapManager.getWritablePartitionIdsCount(); i++) {
      PartitionId partitionId = clusterMapManager.getWritablePartitionIdAt(i);
      assertEquals(partitionId.getReplicaIds().size(), testPartitionLayout.getReplicaCount());

      DataInputStream partitionStream =
              new DataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(partitionId.getBytes())));

      try {
        PartitionId fetchedPartitionId = clusterMapManager.getPartitionIdFromStream(partitionStream);
        assertEquals(partitionId, fetchedPartitionId);
      }
      catch (IOException e) {
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
  public void findDatacenter() throws JSONException {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    TestUtils.TestPartitionLayout testPartitionLayout = new TestUtils.TestPartitionLayout(testHardwareLayout);

    ClusterMapManager clusterMapManager = new ClusterMapManager(testPartitionLayout.getPartitionLayout());

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      assertTrue(clusterMapManager.hasDatacenter(datacenter.getName()));
      assertFalse(clusterMapManager.hasDatacenter(datacenter.getName() + datacenter.getName()));
    }
  }

  @Test
  public void addNewPartition() throws JSONException {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    ClusterMapManager clusterMapManager = new ClusterMapManager(partitionLayout);

    assertEquals(clusterMapManager.getWritablePartitionIdsCount(), 0);
    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(6), 100 * 1024 * 1024 * 1024L);
    assertEquals(clusterMapManager.getWritablePartitionIdsCount(), 1);
    PartitionId partitionId = clusterMapManager.getWritablePartitionIdAt(0);
    assertEquals(partitionId.getReplicaIds().size(), 6);
  }

  @Test
  public void bestEffortAllocation() throws JSONException, IOException {
    int replicaCountPerDataCenter = 2;
    long replicaCapacityInBytes = 100 * 1024 * 1024 * 1024L;

    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    ClusterMapManager clusterMapManager = new ClusterMapManager(partitionLayout);
    List<PartitionId> allocatedPartitions;

    // Allocate a five partitions that fit within cluster's capacity
    allocatedPartitions = clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getWritablePartitionIds().size(), 5);

    // Allocate "too many" partitions (1M) to exhaust capacity. Capacity is not exhausted evenly across nodes so some
    // "free" but unusable capacity may be left after trying to allocate these partitions.
    allocatedPartitions = clusterMapManager.allocatePartitions(1000 * 1000,
                                                               replicaCountPerDataCenter,
                                                               replicaCapacityInBytes);
    assertEquals(allocatedPartitions.size() + 5, clusterMapManager.getWritablePartitionIds().size());
    System.out.println(freeCapacityDump(clusterMapManager, testHardwareLayout.getHardwareLayout()));

    // Capacity is already exhausted...
    allocatedPartitions = clusterMapManager.allocatePartitions(5, replicaCountPerDataCenter, replicaCapacityInBytes);
    assertEquals(allocatedPartitions.size(), 0);
  }

  @Test
  public void capacities() throws JSONException {
    TestUtils.TestHardwareLayout testHardwareLayout = new TestUtils.TestHardwareLayout("Alpha");
    PartitionLayout partitionLayout = new PartitionLayout(testHardwareLayout.getHardwareLayout());

    ClusterMapManager clusterMapManager = new ClusterMapManager(partitionLayout);

    // Confirm initial capacity is available for use
    long raw = clusterMapManager.getRawCapacityInBytes();
    long allocated = clusterMapManager.getAllocatedCapacityInBytes();
    long free = clusterMapManager.getFreeCapacityInBytes();

    assertEquals(free, raw);
    assertEquals(allocated, 0);

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        long dataNodeFree = clusterMapManager.getFreeCapacityInBytes(dataNode);
        assertEquals(dataNodeFree, testHardwareLayout.getDiskCapacityInBytes() * testHardwareLayout.getDiskCount());
        for (Disk disk : dataNode.getDisks()) {
          long diskFree = clusterMapManager.getFreeCapacityInBytes(disk);
          assertEquals(diskFree, testHardwareLayout.getDiskCapacityInBytes());
        }
      }
    }

    clusterMapManager.addNewPartition(testHardwareLayout.getIndependentDisks(6), 100 * 1024 * 1024 * 1024L);

    // Confirm 100GB has been used on 6 distinct DataNodes / Disks.
    assertEquals(clusterMapManager.getRawCapacityInBytes(), raw);
    assertEquals(clusterMapManager.getAllocatedCapacityInBytes(), 6 * 100 * 1024 * 1024 * 1024L);
    assertEquals(clusterMapManager.getFreeCapacityInBytes(), free - (6 * 100 * 1024 * 1024 * 1024L));

    for (Datacenter datacenter : testHardwareLayout.getHardwareLayout().getDatacenters()) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        long dataNodeFree = clusterMapManager.getFreeCapacityInBytes(dataNode);
        assertTrue(dataNodeFree <= testHardwareLayout.getDiskCapacityInBytes() * testHardwareLayout.getDiskCount());
        assertTrue(dataNodeFree >= testHardwareLayout.getDiskCapacityInBytes() *
                                   testHardwareLayout.getDiskCount() - (100 * 1024 * 1024 * 1024L));
        for (Disk disk : dataNode.getDisks()) {
          long diskFree = clusterMapManager.getFreeCapacityInBytes(disk);
          assertTrue(diskFree <= testHardwareLayout.getDiskCapacityInBytes());
          assertTrue(diskFree >= testHardwareLayout.getDiskCapacityInBytes() - (100 * 1024 * 1024 * 1024L));
        }
      }
    }
  }

  @Test
  public void persistAndReadBack() throws JSONException, IOException {
    String tmpDir = folder.getRoot().getPath();

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

  @Test
  public void validateSimpleConfig() throws JSONException, IOException {
    String configDir = System.getProperty("user.dir");
    // intelliJ and gradle return different values for user.dir: gradle includes the sub-project directory. To handle
    // this, we check the string suffix for the sub-project directory and append ".." to correctly set configDir.
    if (configDir.endsWith("ambry-clustermap")) {
      configDir += "/..";
    }
    configDir += "/config";
    String hardwareLayoutSer = configDir + "/HardwareLayout.json";
    String partitionLayoutSer = configDir + "/PartitionLayout.json";
    ClusterMapManager clusterMapManager = new ClusterMapManager(hardwareLayoutSer, partitionLayoutSer);
    assertEquals(clusterMapManager.getWritablePartitionIdsCount(), 1);
    assertEquals(clusterMapManager.getFreeCapacityInBytes(), 10737418240L);
    assertNotNull(clusterMapManager.getDataNodeId("localhost", 6667));
  }
}

