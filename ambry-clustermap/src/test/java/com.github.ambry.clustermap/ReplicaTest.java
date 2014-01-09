package com.github.ambry.clustermap;


import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// Permits Replica to be constructed with a null Partition
class TestReplica extends Replica {
  public TestReplica(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    super(hardwareLayout, null, jsonObject);
  }

  public TestReplica(Disk disk) throws JSONException {
    super(null, disk);
  }

  @Override
  public void validatePartition() {
    // Null OK
  }
}

/**
 * Tests {@link Replica} class.
 */
public class ReplicaTest {

  @Test
  public void basics() throws JSONException {
    // Much of Replica depends on Partition. With a null Partition, only nominal testing can be done.
    TestUtils.TestHardwareLayout thl = new TestUtils.TestHardwareLayout("Alpha");
    Disk disk = thl.getRandomDisk();

    TestReplica replicaA = new TestReplica(disk);
    assertEquals((Disk)replicaA.getDiskId(), disk);

    TestReplica replicaB = new TestReplica(thl.getHardwareLayout(), TestUtils.getJsonReplica(disk));
    assertEquals((Disk)replicaB.getDiskId(), disk);
  }

  @Test
  public void validation() throws JSONException {
    try {
      TestUtils.TestHardwareLayout thl = new TestUtils.TestHardwareLayout("Alpha");
      // Null Partition
      new Replica(null, thl.getRandomDisk());
      fail("Should have failed validation.");
    } catch (IllegalStateException e) {
      // Expected.
    }
  }
}
