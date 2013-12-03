package com.github.ambry.clustermap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests {@link Partition} class.
 */
public class PartitionTest {

  @Test
  public void basics() throws JSONException {
    int replicaCount = 6;
    int replicaCapacityGB = 100;

    TestUtils.TestHardwareLayout thl = new TestUtils.TestHardwareLayout("Alpha");
    JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(thl.getIndependentDisks(replicaCount));
    JSONObject jsonObject = TestUtils.getJsonPartition(99, PartitionState.READ_WRITE, replicaCapacityGB, jsonReplicas);

    Partition partition = new Partition(thl.getHardwareLayout(), jsonObject);

    assertEquals(partition.getReplicaIds().size(), replicaCount);
    assertEquals(partition.getReplicaCapacityGB(), replicaCapacityGB);
    assertEquals(partition.getPartitionState(), PartitionState.READ_WRITE);
  }

  public void failValidation(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    try {
      new Partition(hardwareLayout, jsonObject);
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void validation() throws JSONException {
    int replicaCount = 6;
    int replicaCapacityGB = 100;

    TestUtils.TestHardwareLayout thl = new TestUtils.TestHardwareLayout("Alpha");
    JSONArray jsonReplicas = TestUtils.getJsonArrayReplicas(thl.getIndependentDisks(replicaCount));
    JSONObject jsonObject;

    // Bad replica capacity GB (too small)
    jsonObject = TestUtils.getJsonPartition(99, PartitionState.READ_WRITE, -1, jsonReplicas);
    failValidation(thl.getHardwareLayout(), jsonObject);

    // Bad replica capacity GB (too big)
    jsonObject = TestUtils.getJsonPartition(99, PartitionState.READ_WRITE, 1024*1024*1024, jsonReplicas);
    failValidation(thl.getHardwareLayout(), jsonObject);

    // Multiple Replica on same Disk.
    List<Disk> disks = new ArrayList<Disk>(replicaCount);
    Disk randomDisk = thl.getRandomDisk();
    for(int i=0; i<replicaCount; i++) {
      disks.add(randomDisk);
    }
    JSONArray jsonReplicasSameDisk = TestUtils.getJsonArrayReplicas(disks);
    jsonObject = TestUtils.getJsonPartition(99, PartitionState.READ_WRITE, replicaCapacityGB, jsonReplicasSameDisk);
    failValidation(thl.getHardwareLayout(), jsonObject);

    // Multiple Replica on same DataNode.
    JSONArray jsonReplicasSameDataNode = TestUtils.getJsonArrayReplicas(thl.getDependentDisks(replicaCount));
    jsonObject = TestUtils.getJsonPartition(99, PartitionState.READ_WRITE, replicaCapacityGB, jsonReplicasSameDataNode);
    failValidation(thl.getHardwareLayout(), jsonObject);
  }


}
