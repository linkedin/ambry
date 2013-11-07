package com.github.ambry;

import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class ReplicaTest {

  @Test
  public void jsonSerDeTest() {
    Partition testPartition = TestUtils.getNewTestPartition();
    Disk testDisk = TestUtils.getNewTestDisk();

    Replica replicaSer = new Replica(testPartition, testDisk);
    // System.out.println(replicaSer.toString());

    try {
      Replica replicaDe = new Replica(testPartition, replicaSer.toJSONObject());

      assertEquals(replicaSer, replicaDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }
  }
}
