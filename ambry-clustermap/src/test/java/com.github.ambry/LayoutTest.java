package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class LayoutTest {

  public Layout getTestLayout(Cluster cluster) {
    Layout layout = new Layout(cluster);

    ArrayList<Disk> disks = new ArrayList<Disk>();
    disks.add(layout.getCluster().getDisk(new DiskId(1)));
    disks.add(layout.getCluster().getDisk(new DiskId(3)));
    disks.add(layout.getCluster().getDisk(new DiskId(5)));
    disks.add(layout.getCluster().getDisk(new DiskId(7)));
    layout.addNewPartition(disks);

    disks.clear();
    disks.add(layout.getCluster().getDisk(new DiskId(0)));
    disks.add(layout.getCluster().getDisk(new DiskId(2)));
    disks.add(layout.getCluster().getDisk(new DiskId(4)));
    disks.add(layout.getCluster().getDisk(new DiskId(6)));
    layout.addNewPartition(disks);

    return layout;
  }

  @Test
  public void jsonSerDeTest() {
    Cluster cluster = ClusterTest.getTestCluster();
    Layout layoutSer = getTestLayout(cluster);

    layoutSer.validate();
    // System.out.println(layoutSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(layoutSer.toString());
      Layout layoutDe = new Layout(cluster, jsonObject);

      assertEquals(layoutSer, layoutDe);
      assertEquals(2, layoutDe.getPartitions().size());
      for (Partition partitionDe : layoutDe.getPartitions()) {
        assertEquals(4, partitionDe.getReplicas().size());
      }
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }
  }
}
