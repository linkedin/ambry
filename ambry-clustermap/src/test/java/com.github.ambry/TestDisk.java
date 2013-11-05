package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 */
public class TestDisk extends Disk {

  // Permit disk to be constructed without a DataNode.
  public TestDisk(JSONObject jsonObject)  throws JSONException {
    super(null, jsonObject);
  }

  public TestDisk(DiskId diskId, long capacityGB) {
    super(null, diskId, capacityGB);
  }

  @Override
  public void validateDataNode() {
    return;
  }
}