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
  public void jsonSerDeTestUtilsCluster() {
    jsonSerDeTest(TestUtils.getNewCluster("Alpha"));
  }

  @Test
  public void jsonSerDeBuildCluster() {
    jsonSerDeTest(TestUtils.buildCluster("Beta"));
  }


}
