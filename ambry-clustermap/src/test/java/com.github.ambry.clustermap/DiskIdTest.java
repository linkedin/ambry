package com.github.ambry.clustermap;


import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DiskIdTest {

  @Test
  public void jsonSerDeTest() {
    DiskId diskIdSer = TestUtils.getNewDiskId();
    // System.out.println(diskIdSer.toString());

    try {
      DiskId diskIdDe = new DiskId(diskIdSer.toJSONObject());

      assertEquals(diskIdSer, diskIdDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
