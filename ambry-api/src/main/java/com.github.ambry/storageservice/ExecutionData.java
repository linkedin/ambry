package com.github.ambry.storageservice;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public abstract class ExecutionData {
  public static String OPERATION_TYPE_KEY = "operationType";
  public static String OPERATION_DATA_KEY = "operationData";

  protected final JSONObject data;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public ExecutionData(JSONObject data) {
    verifyData(data);
    this.data = data;
  }

  private void verifyData(JSONObject data) {
    if (!data.has(OPERATION_TYPE_KEY) || !data.has(OPERATION_DATA_KEY)) {
      throw new IllegalArgumentException("Input JSON object cannot be converted to " + this.getClass().getSimpleName());
    }
  }

  public String getOperationType() {
    try {
      if (data.has(OPERATION_TYPE_KEY)) {
        return data.getString(OPERATION_TYPE_KEY);
      }
    } catch (JSONException e) {
      logger.error("JSONException when retrieving " + OPERATION_TYPE_KEY + " - " + e);
    }
    return null;
  }

  public JSONObject getOperationData() {
    try {
      if (data.has(OPERATION_DATA_KEY)) {
        return data.getJSONObject(OPERATION_DATA_KEY);
      }
    } catch (JSONException e) {
      logger.error("JSONException when retrieving " + OPERATION_DATA_KEY + " - " + e);
    }
    return null;
  }
}
