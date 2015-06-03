package com.github.ambry.admin;

import com.github.ambry.storageservice.BlobStorageServiceErrorCode;
import com.github.ambry.storageservice.BlobStorageServiceException;
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
      throws BlobStorageServiceException {
    JSONObject data = executionData.getOperationData();
    if (data != null && data.has(TEXT_KEY)) {
      try {
        String text = data.getString(TEXT_KEY);
        return packageResult(text);
      } catch (JSONException e) {
        throw new BlobStorageServiceException("Unable to construct result object - " + e,
            BlobStorageServiceErrorCode.ResponseBuildingError);
      }
    } else {
      throw new BlobStorageServiceException("Input AdminExecutionData does not have key - " + TEXT_KEY,
          BlobStorageServiceErrorCode.BadRequest);
    }
  }

  private AdminExecutionResult packageResult(String text)
      throws JSONException {
    JSONObject result = new JSONObject();
    result.put(TEXT_KEY, text);
    return new AdminExecutionResult(result);
  }
}
