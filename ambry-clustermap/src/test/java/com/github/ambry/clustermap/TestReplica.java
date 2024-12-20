package com.github.ambry.clustermap;

import org.json.JSONException;
import org.json.JSONObject;


// Permits Replica to be constructed with a null Partition
public class TestReplica extends Replica {
  public TestReplica(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    super(hardwareLayout, null, jsonObject);
  }

  public TestReplica(TestUtils.TestHardwareLayout hardwareLayout, Disk disk) throws JSONException {
    super(null, disk, hardwareLayout.clusterMapConfig);
  }

  @Override
  public void validatePartition() {
    // Null OK
  }
}
