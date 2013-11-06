package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DataNodeTest {

  @Test
  public void jsonSerDeTest() {
    DataNode dataNodeSer = TestUtils.getNewTestDataNode();

    dataNodeSer.validate();
    // System.out.println(dataNode1.toString());

    try {
      JSONObject jsonObject = new JSONObject(dataNodeSer.toString());
      DataNode dataNodeDe = new TestUtils.TestDataNode(jsonObject);

      assertEquals(dataNodeSer, dataNodeDe);
      assertEquals(2, dataNodeDe.getDisks().size());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
