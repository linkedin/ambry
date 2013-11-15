package com.github.ambry.clustermap;


import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DataNodeIdTest {

  @Test
  public void jsonSerDeTest() {
    DataNodeId dataNodeIdSer = new DataNodeId("localhost", TestUtils.getNewPort());
    // System.out.println(dataNodeIdSer.toString());

    try {
      DataNodeId dataNodeIdDe = new DataNodeId(dataNodeIdSer.toJSONObject());

      assertEquals(dataNodeIdSer, dataNodeIdDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }


}
