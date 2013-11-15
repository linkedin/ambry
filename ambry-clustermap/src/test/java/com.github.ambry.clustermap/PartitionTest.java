package com.github.ambry.clustermap;


import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class PartitionTest {

  @Test
  public void jsonSerDeTest() {
    Partition partitionSer = TestUtils.getNewTestPartition();
    // System.out.println(partitionSer.toString());

    try {
      Partition partitionDe = new TestUtils.TestPartition(partitionSer.toJSONObject());

      assertEquals(partitionSer, partitionDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }
  }
}
