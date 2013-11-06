package com.github.ambry;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ClusterMapManagerTest {
  static final long replicaCapacityGB = 100;
  static final long diskCapacityGB = 1000;

  public static ClusterMapManager getTestClusterMap() {
    Cluster cluster = ClusterTest.getTestCluster();
    Layout layout = LayoutTest.getTestLayout(cluster);

    return new ClusterMapManager(cluster, layout);
  }

  public static ClusterMapManager buildTestClusterMap() {

    Cluster cluster = new Cluster("Beta");
    Layout layout = new Layout(cluster);

    ClusterMapManager clusterMapManager = new ClusterMapManager(cluster, layout);

    ArrayList<Disk> disksA = new ArrayList<Disk>();
    ArrayList<Disk> disksB = new ArrayList<Disk>();

    clusterMapManager.addNewDataCenter("ELA4");
    clusterMapManager.addNewDataNode("ELA4", "ela4-app999.prod");
    disksA.add(clusterMapManager.addNewDisk("ela4-app999.prod", diskCapacityGB));
    disksB.add(clusterMapManager.addNewDisk("ela4-app999.prod", diskCapacityGB));
    clusterMapManager.addNewDataNode("ELA4", "ela4-app007.prod");
    disksA.add(clusterMapManager.addNewDisk("ela4-app007.prod", diskCapacityGB));
    disksB.add(clusterMapManager.addNewDisk("ela4-app007.prod", diskCapacityGB));

    clusterMapManager.addNewDataCenter("LVA1");
    clusterMapManager.addNewDataNode("LVA1", "lva1-app999.prod");
    disksA.add(clusterMapManager.addNewDisk("lva1-app999.prod", diskCapacityGB));
    disksB.add(clusterMapManager.addNewDisk("lva1-app999.prod", diskCapacityGB));
    clusterMapManager.addNewDataNode("LVA1", "lva1-app007.prod");
    disksA.add(clusterMapManager.addNewDisk("lva1-app007.prod", diskCapacityGB));
    disksB.add(clusterMapManager.addNewDisk("lva1-app007.prod", diskCapacityGB));

    clusterMapManager.addNewPartition(disksA, replicaCapacityGB);
    clusterMapManager.addNewPartition(disksB, replicaCapacityGB);

    return clusterMapManager;
  }

  public static ClusterMapManager allocateTestClusterMap() {
    final long replicaCapacityGB = 100;
    final long diskCapacityGB = 1000;

    Cluster cluster = new Cluster("Gamma");
    Layout layout = new Layout(cluster);

    ClusterMapManager clusterMapManager = new ClusterMapManager(cluster, layout);

    clusterMapManager.addNewDataCenter("ELA4");
    clusterMapManager.addNewDataNode("ELA4", "ela4-app999.prod");
    clusterMapManager.addNewDisk("ela4-app999.prod", diskCapacityGB);
    clusterMapManager.addNewDisk("ela4-app999.prod", diskCapacityGB);
    clusterMapManager.addNewDataNode("ELA4", "ela4-app007.prod");
    clusterMapManager.addNewDisk("ela4-app007.prod", diskCapacityGB);
    clusterMapManager.addNewDisk("ela4-app007.prod", diskCapacityGB);

    clusterMapManager.addNewDataCenter("LVA1");
    clusterMapManager.addNewDataNode("LVA1", "lva1-app999.prod");
    clusterMapManager.addNewDisk("lva1-app999.prod", diskCapacityGB);
    clusterMapManager.addNewDisk("lva1-app999.prod", diskCapacityGB);
    clusterMapManager.addNewDataNode("LVA1", "lva1-app007.prod");
    clusterMapManager.addNewDisk("lva1-app007.prod", diskCapacityGB);
    clusterMapManager.addNewDisk("lva1-app007.prod", diskCapacityGB);


    clusterMapManager.allocatePartitions(10, replicaCapacityGB);
    return clusterMapManager;
  }


  public void basicClusterMapTests(ClusterMapManager clusterMapManager) {

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
    assertTrue(clusterMapManager.getAllocatedCapacityGB() > 0);
    assertTrue(clusterMapManager.getFreeCapacityGB() > 0);
  }

  @Test
  public void clusterMapTests() {
    basicClusterMapTests(getTestClusterMap());
  }

  @Test
  public void clusterMapTestWithBuildClusterMap() {
    basicClusterMapTests(buildTestClusterMap());
  }

  @Test
  public void clusterMapTestWithAllocateClusterMap() {
    basicClusterMapTests(allocateTestClusterMap());
  }

  @Test
  public void bestEffortAllocation() {
    ClusterMapManager clusterMapManager = allocateTestClusterMap();

    assertEquals(clusterMapManager.getReadWritePartitions().size(), 10);

    List<Partition> morePartitions = clusterMapManager.allocatePartitions(10, replicaCapacityGB);
    assertEquals(morePartitions.size(), 0);
  }

}
