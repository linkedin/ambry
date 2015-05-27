package com.github.ambry.admin;

import com.github.ambry.storageservice.ExecutionResult;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class AdminExecutionResult extends ExecutionResult {

  public AdminExecutionResult(JSONObject result)
      throws JSONException {
    super(result);
  }
}
