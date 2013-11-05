package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DatacenterTest {

  // Permit Datacenter to be constructed with a null Cluster
  public class TestDatacenter extends Datacenter {

    public TestDatacenter(JSONObject jsonObject) throws JSONException {
      super(null, jsonObject);
    }

    public TestDatacenter(String hostname) {
      super(null, hostname);
    }

    @Override
    public void validateCluster() {
      return;
    }
  }


  @Test
  public void jsonSerDeTest() {
    Datacenter datacenterSer = new TestDatacenter("ELA4");

    DataNode dataNode = new DataNode(datacenterSer, "ela4-app999.prod");
    dataNode.addDisk(new Disk(dataNode, new DiskId(0), 100));
    dataNode.addDisk(new Disk(dataNode, new DiskId(1), 100));
    datacenterSer.addDataNode(dataNode);

    dataNode = new DataNode(datacenterSer, "ela4-app007.prod");
    dataNode.addDisk(new Disk(dataNode, new DiskId(2), 100));
    dataNode.addDisk(new Disk(dataNode, new DiskId(3), 100));
    datacenterSer.addDataNode(dataNode);

    datacenterSer.validate();
    // System.out.println(datacenter1.toString());

    try {
      JSONObject jsonObject = new JSONObject(datacenterSer.toString());
      Datacenter datacenterDe = new TestDatacenter(jsonObject);

      assertEquals(datacenterSer, datacenterDe);
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
