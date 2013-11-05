package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 */
public class TestPartition extends Partition {

  TestPartition(PartitionId partitionId) {
    super(null, partitionId);
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