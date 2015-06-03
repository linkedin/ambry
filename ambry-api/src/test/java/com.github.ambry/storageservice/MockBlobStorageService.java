package com.github.ambry.storageservice;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public class MockBlobStorageService implements BlobStorageService {
  //TODO: This class will probably change once we get in more components of blob storage service.

  public static String GET_HANDLED = "getHandled";
  public static String PUT_HANDLED = "putHandled";
  public static String EXECUTE_HANDLED = "executeHandled";

  public static String EXECUTION_RESULT_KEY = "executeResult";
  public static String DUMMY_EXECUTE_OPERATION = "dummyExecute";

  private Logger logger = LoggerFactory.getLogger(getClass());

  public void start()
      throws InstantiationException {
    logger.info("MockBlobStorageService started");
  }

  public void shutdown()
      throws Exception {
    logger.info("MockBlobStorageService shutdown");
  }

  public String putBlob()
      throws BlobStorageServiceException {
    return PUT_HANDLED;
  }

  public Object getBlob()
      throws BlobStorageServiceException {
    return GET_HANDLED;
  }

  public boolean deleteBlob()
      throws BlobStorageServiceException {
    return true;
  }

  public ExecutionResult execute(ExecutionData executionData)
      throws BlobStorageServiceException {
    try {
      if (executionData.getOperationType() == DUMMY_EXECUTE_OPERATION) {
        JSONObject result = new JSONObject();
        result.put(EXECUTION_RESULT_KEY, EXECUTE_HANDLED);
        return new MockExecutionResult(result);
      } else {
        throw new BlobStorageServiceException("Received unknown operation type - " + executionData.getOperationType(),
            BlobStorageServiceErrorCode.UnknownOperationType);
      }
    } catch (JSONException e) {
      throw new BlobStorageServiceException("Could not construct result",
          BlobStorageServiceErrorCode.ResponseBuildingError);
    }
  }
}
