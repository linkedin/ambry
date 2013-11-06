package com.github.ambry;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ClusterMapManagerTest {

  public void basicTests(ClusterMapManager clusterMapManager) {

    // Coordinator API
    List<Partition> partitions = clusterMapManager.getReadWritePartitions();
    for(Partition partition : partitions) {
      assertTrue(partition.getReplicaCapacityGB() > 0);
      assertTrue(partition.getCapacityGB() > 0);
      assertEquals(partition.getState(), Partition.State.READ_WRITE);
      assertEquals(partition, clusterMapManager.getPartition(partition.getPartitionId()));

      for(Replica replica : partition.getReplicas()) {
        assertTrue(replica.getCapacityGB() > 0);
        ReplicaId replicaId = replica.getReplicaId();
        assertEquals(replicaId.getDiskId(), replica.getDisk().getDiskId());
        assertEquals(replicaId.getPartitionId(), replica.getPartition().getPartitionId());

        for(Replica peerReplica : clusterMapManager.getPeerReplicas(replica)) {
          assertEquals(peerReplica.getCapacityGB(), replica.getCapacityGB());
          assertEquals(peerReplica.getPartition(), replica.getPartition());
          assertNotSame(peerReplica.getDisk(), replica.getDisk());
        }
      }
    }


    // Administrative API

    assertTrue(clusterMapManager.getRawCapacityGB() > 0);
    assertTrue(clusterMapManager.getAllocatedCapacityGB() >= 0);
    assertTrue(clusterMapManager.getFreeCapacityGB() > 0);
  }

  @Test
  public void basicTestsWithNoPartitions() {
    basicTests(TestUtils.buildClusterMapManagerWithoutPartitions("Alpha"));
  }

  @Test
  public void basicTestsWithPartitions() {
    basicTests(TestUtils.buildClusterMapManager("Beta"));
  }

  @Test
  public void basicTestsWithAllocatedPartitions() {
    ClusterMapManager clusterMapManager = TestUtils.buildClusterMapManagerWithoutPartitions("Gamma");
    clusterMapManager.allocatePartitions(10, TestUtils.replicaCapacityGB);
    basicTests(clusterMapManager);
  }

  @Test
  public void bestEffortAllocation() {
    // Invariant used in other assertions below and baked into test utils
    assertEquals(TestUtils.diskCapacityGB, 10*TestUtils.replicaCapacityGB);

    ClusterMapManager clusterMapManager = TestUtils.buildClusterMapManagerWithoutPartitions("Delta");
    // "8" is number of disks hard coded into helper test method.
    assertEquals(clusterMapManager.getRawCapacityGB(), 8*TestUtils.diskCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 0);

    List<Partition> allocatedPartitions;

    allocatedPartitions = clusterMapManager.allocatePartitions(5, TestUtils.replicaCapacityGB);
    // "5" is number partitions above, "4" is replication factor hard coded into helper test method.
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 5*4*TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 5);

    allocatedPartitions = clusterMapManager.allocatePartitions(10, TestUtils.replicaCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 15*4*TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 10);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 15);

    allocatedPartitions = clusterMapManager.allocatePartitions(6, TestUtils.replicaCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 20*4*TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 20);

    allocatedPartitions = clusterMapManager.allocatePartitions(1, TestUtils.replicaCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 20*4*TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 0);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 20);
  }

}
