package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class PartitionTest {

  @Test
  public void jsonSerDeTest() {

    Partition partitionSer = new TestPartition(new PartitionId(7));
    // System.out.println(partitionSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(partitionSer.toString());

      Partition partitionDe = new TestPartition(jsonObject);

      assertEquals(partitionSer, partitionDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }  
}
