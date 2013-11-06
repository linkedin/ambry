package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DataNodeIdTest {

  @Test
  public void jsonSerDeTest() {
    DataNodeId dataNodeIdSer = new DataNodeId("localhost", 6666);
    // System.out.println(dataNodeIdSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(dataNodeIdSer.toString());

      DataNodeId dataNodeIdDe = new DataNodeId(jsonObject);

      assertEquals(dataNodeIdSer, dataNodeIdDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }


}
