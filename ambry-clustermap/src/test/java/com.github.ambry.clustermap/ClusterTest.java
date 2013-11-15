package com.github.ambry.clustermap;

import org.json.JSONException;
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
      Cluster clusterDe = new Cluster(clusterSer.toJSONObject());

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
