package com.github.ambry.storageservice;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public abstract class ExecutionResult {
  public static String OPERATION_RESULT_KEY = "operationResult";

  protected Logger logger = LoggerFactory.getLogger(getClass());
  protected JSONObject result = new JSONObject();

  public ExecutionResult(JSONObject result)
      throws JSONException {
    this.result.put(OPERATION_RESULT_KEY, result);
  }

  public JSONObject getOperationResult() {
    try {
      if(result.has(OPERATION_RESULT_KEY)) {
        return result.getJSONObject(OPERATION_RESULT_KEY);
      }
    } catch (JSONException e) {
      logger.error("JSONException when retrieving " + OPERATION_RESULT_KEY + " - " + e);
    }
    return null;
  }
}
