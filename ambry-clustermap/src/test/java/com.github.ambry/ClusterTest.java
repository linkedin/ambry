package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class ClusterTest {

  // Not the way a cluster should be built. But useful for some testing.
  public static Cluster getClusterBuiltObjectbyObject() {
    Cluster cluster = new Cluster("Alpha");
    Datacenter datacenter = new Datacenter(cluster, "ELA4");
    cluster.addDatacenter(datacenter);

    DataNode dataNode = new DataNode(datacenter, "ela4-app999.prod");
    dataNode.addDisk(new Disk(dataNode, new DiskId(0), 1000));
    dataNode.addDisk(new Disk(dataNode, new DiskId(1), 1000));
    datacenter.addDataNode(dataNode);

    dataNode = new DataNode(datacenter, "ela4-app007.prod");
    dataNode.addDisk(new Disk(dataNode, new DiskId(2), 1000));
    dataNode.addDisk(new Disk(dataNode, new DiskId(3), 1000));
    datacenter.addDataNode(dataNode);

    datacenter = new Datacenter(cluster, "LVA1");
    cluster.addDatacenter(datacenter);

    dataNode = new DataNode(datacenter, "lva1-app999.prod");
    dataNode.addDisk(new Disk(dataNode, new DiskId(4), 1000));
    dataNode.addDisk(new Disk(dataNode, new DiskId(5), 1000));
    datacenter.addDataNode(dataNode);

    dataNode = new DataNode(datacenter, "lva1-app007.prod");
    dataNode.addDisk(new Disk(dataNode, new DiskId(6), 1000));
    dataNode.addDisk(new Disk(dataNode, new DiskId(7), 1000));
    datacenter.addDataNode(dataNode);

    return cluster;
  }

  public static Cluster getTestCluster() {
    Cluster cluster = new Cluster("Alpha");

    cluster.addNewDataCenter("ELA4");
    cluster.addNewDataNode("ELA4", "ela4-app999.prod");
    cluster.addNewDisk("ela4-app999.prod", 1000);
    cluster.addNewDisk("ela4-app999.prod", 1000);
    cluster.addNewDataNode("ELA4", "ela4-app007.prod");
    cluster.addNewDisk("ela4-app007.prod", 1000);
    cluster.addNewDisk("ela4-app007.prod", 1000);

    cluster.addNewDataCenter("LVA1");
    cluster.addNewDataNode("LVA1", "lva1-app999.prod");
    cluster.addNewDisk("lva1-app999.prod", 1000);
    cluster.addNewDisk("lva1-app999.prod", 1000);
    cluster.addNewDataNode("LVA1", "lva1-app007.prod");
    cluster.addNewDisk("lva1-app007.prod", 1000);
    cluster.addNewDisk("lva1-app007.prod", 1000);

    return cluster;
  }

  public void jsonSerDeTest(Cluster clusterSer) {
    clusterSer.validate();
    // System.out.println(clusterSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(clusterSer.toString());
      Cluster clusterDe = new Cluster(jsonObject);

      assertEquals(clusterSer, clusterDe);
      assertEquals(2, clusterDe.getDatacenters().size());
      for (Datacenter datacenterDe : clusterDe.getDatacenters()) {
        assertEquals(2, datacenterDe.getDataNodes().size());
        for (DataNode dataNodeDe : datacenterDe.getDataNodes()) {
          assertEquals(2, dataNodeDe.getDisks().size());
        }
      }
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }
  }

  @Test
  public void jsonSerDeTestClusterBuiltObjectByObject() {
    jsonSerDeTest(getClusterBuiltObjectbyObject());
  }

  @Test
  public void jsonSerDeTestCluster() {
    jsonSerDeTest(getTestCluster());
  }


}
