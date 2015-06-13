package com.github.ambry.admin;

import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executes the custom echo GET operation.
 */
public class EchoExecutor implements TaskExecutor {
  public static String TEXT_KEY = "text";

  private Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Refers to the text provided by the client in the operationData of the executionData header and echoes back
   * @param executionData
   * @return
   * @throws RestServiceException
   */
  public JSONObject execute(AdminExecutionData executionData)
      throws RestServiceException {
    JSONObject data = executionData.getOperationData();
    if (data != null && data.has(TEXT_KEY)) {
      try {
        String text = data.getString(TEXT_KEY);
        return packageResult(text);
      } catch (JSONException e) {
        throw new RestServiceException("Unable to construct result object - " + e,
            RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      throw new RestServiceException("Execution data does not have key - " + TEXT_KEY,
          RestServiceErrorCode.BadExecutionData);
    }
  }

  /**
   * packages the result into a JSON.
   * @param text
   * @return
   * @throws JSONException
   */
  private JSONObject packageResult(String text)
      throws JSONException {
    JSONObject result = new JSONObject();
    result.put(TEXT_KEY, text);
    return result;
  }
}
