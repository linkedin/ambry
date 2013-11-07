package com.github.ambry;

import org.json.JSONException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 *
 */
public class ClusterMapManagerTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  public void basicTests(ClusterMapManager clusterMapManager) {

    // Coordinator API
    List<Partition> partitions = clusterMapManager.getReadWritePartitions();
    for (Partition partition : partitions) {
      assertTrue(partition.getReplicaCapacityGB() > 0);
      assertTrue(partition.getCapacityGB() > 0);
      assertEquals(partition.getState(), Partition.State.READ_WRITE);
      assertEquals(partition, clusterMapManager.getPartition(partition.getPartitionId()));

      for (Replica replica : partition.getReplicas()) {
        assertTrue(replica.getCapacityGB() > 0);
        ReplicaId replicaId = replica.getReplicaId();
        assertEquals(replicaId.getDiskId(), replica.getDisk().getDiskId());
        assertEquals(replicaId.getPartitionId(), replica.getPartition().getPartitionId());

        for (Replica peerReplica : clusterMapManager.getPeerReplicas(replica)) {
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
  public void basicTestsWithPartitions() {
    basicTests(TestUtils.buildClusterMapManagerWithoutPartitions("Alpha"));
    basicTests(TestUtils.buildClusterMapManagerIndirectly("Beta"));

    ClusterMapManager clusterMapManager = TestUtils.buildClusterMapManagerWithoutPartitions("Gamma");
    clusterMapManager.allocatePartitions(10, TestUtils.replicaCapacityGB);
    basicTests(clusterMapManager);

    basicTests(TestUtils.buildClusterMapManagerDirectly("zeta"));

  }

  @Test
  public void bestEffortAllocation() {
    // Invariant used in other assertions below and baked into test utils
    assertEquals(TestUtils.diskCapacityGB, 10 * TestUtils.replicaCapacityGB);

    ClusterMapManager clusterMapManager = TestUtils.buildClusterMapManagerWithoutPartitions("Delta");
    // "8" is number of disks hard coded into helper test method.
    assertEquals(clusterMapManager.getRawCapacityGB(), 8 * TestUtils.diskCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 0);

    List<Partition> allocatedPartitions;

    allocatedPartitions = clusterMapManager.allocatePartitions(5, TestUtils.replicaCapacityGB);
    // "5" is number partitions above, "4" is replication factor hard coded into helper test method.
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 5 * 4 * TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 5);

    allocatedPartitions = clusterMapManager.allocatePartitions(10, TestUtils.replicaCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 15 * 4 * TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 10);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 15);

    allocatedPartitions = clusterMapManager.allocatePartitions(6, TestUtils.replicaCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 20 * 4 * TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 5);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 20);

    allocatedPartitions = clusterMapManager.allocatePartitions(1, TestUtils.replicaCapacityGB);
    assertEquals(clusterMapManager.getAllocatedCapacityGB(), 20 * 4 * TestUtils.replicaCapacityGB);
    assertEquals(allocatedPartitions.size(), 0);
    assertEquals(clusterMapManager.getReadWritePartitions().size(), 20);
  }


  @Test
  public void persistAndReadBack() {
    ClusterMapManager clusterMapManagerSer = TestUtils.buildClusterMapManagerDirectly("Epsilon");

    // String tmpDir = "/tmp";
    String tmpDir = folder.getRoot().getPath();
    String clusterJson1 = tmpDir + "/cluster1.json";
    String layoutJson1 = tmpDir + "/layout1.json";
    String clusterJson2 = tmpDir + "/cluster2.json";
    String layoutJson2 = tmpDir + "/layout2.json";

    try {
      clusterMapManagerSer.persist(clusterJson1, layoutJson1);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

    ClusterMapManager clusterMapManagerDe = null;
    try {
      clusterMapManagerDe = ClusterMapManager.buildFromFiles(clusterJson1, layoutJson1);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

    assertEquals(clusterMapManagerSer, clusterMapManagerDe);

    try {
      clusterMapManagerDe.persist(clusterJson2, layoutJson2);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

    ClusterMapManager clusterMapManagerDeDe = null;
    try {
      clusterMapManagerDeDe = ClusterMapManager.buildFromFiles(clusterJson2, layoutJson2);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

    assertEquals(clusterMapManagerDe, clusterMapManagerDeDe);
  }
}
