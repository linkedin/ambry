package com.github.ambry.admin;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A wrapper over the JSON object that is derived from the executionData header. Makes it easier and safer
 * to use executionData.
 */
public class AdminExecutionData {

  public static String OPERATION_TYPE_KEY = "operationType";
  public static String OPERATION_DATA_KEY = "operationData";

  protected final JSONObject data;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public AdminExecutionData(JSONObject data) {
    verifyData(data);
    this.data = data;
  }

  /**
   * Verifies that the input JSON can be converted to this object. Conditions for conversion is the presence
   * of certain necessary fields that anyone using this object would expect.
   * @param data
   */
  private void verifyData(JSONObject data) {
    if (!data.has(OPERATION_TYPE_KEY) || !data.has(OPERATION_DATA_KEY)) {
      throw new IllegalArgumentException("Input JSON object cannot be converted to " + this.getClass().getSimpleName());
    }
  }

  /**
   * Specifies the type of operation
   * @return
   */
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

  /**
   * Returns data that is required for this operation. Individual task executors are responsible
   * for checking presence of specific data inside this object.
   * @return
   */
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
