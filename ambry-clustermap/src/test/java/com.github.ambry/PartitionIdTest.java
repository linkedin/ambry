package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class PartitionIdTest {

  @Test
  public void jsonSerDeTest() {
    PartitionId partitionIdSer = new PartitionId(7);
    // System.out.println(partitionIdSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(partitionIdSer.toString());

      PartitionId partitionIdDe = new PartitionId(jsonObject);

      assertEquals(partitionIdSer, partitionIdDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }
  
}
