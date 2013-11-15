package com.github.ambry.clustermap;


import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DatacenterTest {

  @Test
  public void jsonSerDeTest() {
    Datacenter datacenterSer = TestUtils.getNewTestDatacenter("ELA4");

    datacenterSer.validate();
    // System.out.println(datacenterSer.toString());

    try {
      Datacenter datacenterDe = new TestUtils.TestDatacenter(datacenterSer.toJSONObject());

      assertEquals(datacenterSer, datacenterDe);
      // "2" comes from test helpers that popoulate datacenters with two nodes each having two disks.
      assertEquals(2, datacenterDe.getDataNodes().size());
      for (DataNode dataNodeDe : datacenterDe.getDataNodes()) {
        assertEquals(2, dataNodeDe.getDisks().size());
      }
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }
  }

}
