package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DataNodeTest {

  // Permit DataNode to be constructed with a null Datacenter
  public class TestDataNode extends DataNode {

    public TestDataNode(JSONObject jsonObject) throws JSONException {
      super(null, jsonObject);
    }

    public TestDataNode(String hostname) {
      super(null, hostname);
    }

    @Override
    public void validateDatacenter() {
      return;
    }
  }


  // TODO: Hacky test to figure out how to get fully qualified name of host.
  @Test
  public void jsonFullyQualifiedNameTest() {
    try {
      System.out.println(InetAddress.getByName("ela4-app1970.prod").getCanonicalHostName());
    } catch (UnknownHostException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Test
  public void jsonSerDeTest() {
    DataNode dataNodeSer = new TestDataNode("ela4-app999.prod");
    dataNodeSer.addDisk(new Disk(dataNodeSer, new DiskId(0), 100));
    dataNodeSer.addDisk(new Disk(dataNodeSer, new DiskId(1), 100));

    dataNodeSer.validate();
    // System.out.println(dataNode1.toString());


    try {
      JSONObject jsonObject = new JSONObject(dataNodeSer.toString());
      DataNode dataNodeDe = new TestDataNode(jsonObject);

      assertEquals(dataNodeSer, dataNodeDe);
      assertEquals(2, dataNodeDe.getDisks().size());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
