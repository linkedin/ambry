package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class ReplicaTest {

  public class TestReplica extends Replica {

    public TestReplica(TestPartition partition, Disk disk) {
      super(partition, disk);
    }

    // Not ideal sinced not testing the Replica(Partitoin, JSONObject) constructor. PartitionTest will end up testing
    // that constructor.
    public TestReplica(TestPartition partition, JSONObject jsonObject) throws JSONException {
      super(partition, new TestDisk(new ReplicaId(new JSONObject(jsonObject.getString("replicaId"))).getDiskId(), 100));
    }

    @Override
    protected void validatePartitionId() {
      return;
    }
  }

  @Test
  public void jsonSerDeTest() {
    TestPartition testPartition = new TestPartition(new PartitionId(7));

    Replica replicaSer = new TestReplica(testPartition, new TestDisk(new DiskId(8), 100));
    // System.out.println(replicaSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(replicaSer.toString());

      Replica replicaDe = new TestReplica(testPartition, jsonObject);

      assertEquals(replicaSer, replicaDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
