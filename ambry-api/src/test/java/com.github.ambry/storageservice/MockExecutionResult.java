package com.github.ambry.storageservice;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class MockExecutionResult extends ExecutionResult {
  public MockExecutionResult(JSONObject result)
      throws JSONException {
    super(result);
  }
}
