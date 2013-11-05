package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class DiskIdTest {


  @Test
  public void jsonSerDeTest() {
    DiskId diskIdSer = new DiskId(7);
    System.out.println(diskIdSer.toString());

    try {
      JSONObject jsonObject = new JSONObject(diskIdSer.toString());

      DiskId diskIdDe = new DiskId(jsonObject);

      assertEquals(diskIdSer, diskIdDe);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      fail();
    }

  }

}
