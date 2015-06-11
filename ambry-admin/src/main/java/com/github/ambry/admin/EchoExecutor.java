package com.github.ambry.admin;

import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public class EchoExecutor implements TaskExecutor {
  public static String TEXT_KEY = "text";

  private Logger logger = LoggerFactory.getLogger(getClass());

  public AdminExecutionResult execute(AdminExecutionData executionData)
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

  private AdminExecutionResult packageResult(String text)
      throws JSONException {
    JSONObject result = new JSONObject();
    result.put(TEXT_KEY, text);
    return new AdminExecutionResult(result);
  }
}
