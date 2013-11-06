package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 */
public class TestPartition extends Partition {

  TestPartition(PartitionId partitionId, long replicaCapacityGB) {
    super(null, partitionId, replicaCapacityGB);
  }

  TestPartition(JSONObject jsonObject) throws JSONException {
    super(null, jsonObject);
  }

  @Override
  protected void validateLayout() {
    // Make null OK
    return;
  }
}