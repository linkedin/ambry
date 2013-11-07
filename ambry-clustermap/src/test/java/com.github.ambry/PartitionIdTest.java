package com.github.ambry;

import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class PartitionIdTest {

  @Test
  public void jsonSerDeTest() {
    PartitionId partitionIdSer = TestUtils.getNewPartitionId();
    // System.out.println(partitionIdSer.toString());

    try {
      PartitionId partitionIdDe = new PartitionId(partitionIdSer.toJSONObject());

      assertEquals(partitionIdSer, partitionIdDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
