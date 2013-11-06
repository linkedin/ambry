package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;



/**
 *
 */
public class DiskTest {




  @Test
  public void jsonSerDeTest() {
    Disk diskSer = new TestDisk(new DiskId(2), 1000);
    // System.out.println(diskSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(diskSer.toString());

      Disk diskDe = new TestDisk(jsonObject);

      assertEquals(diskSer, diskDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
