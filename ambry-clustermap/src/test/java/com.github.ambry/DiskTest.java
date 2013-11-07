package com.github.ambry;

import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 *
 */
public class DiskTest {

  @Test
  public void jsonSerDeTest() {
    Disk diskSer = TestUtils.getNewTestDisk();
    // System.out.println(diskSer.toString());

    try {
      Disk diskDe = new TestUtils.TestDisk(diskSer.toJSONObject());

      assertEquals(diskSer, diskDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
