package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class MockBlobStorageService implements BlobStorageService {
  //TODO: This class will probably change once we get in more components of blob storage service.

  public static String EXECUTION_DATA_HEADER_KEY = "executionData";
  public static String OPERATION_TYPE_KEY = "operationType";
  public static String OPERATION_DATA_KEY = "operationData";

  public static String OPERATION_THROW_HANDLING_REST_EXCEPTION = "throwHandlingRestException";
  public static String OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION = "throwHandlingRuntimeException";
  public static String OPERATION_THROW_PROCESSING_UNCHECKED_EXCEPTION = "throwProcessingUncheckedException";

  public MockBlobStorageService() {
  }

  public MockBlobStorageService(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      MetricRegistry metricRegistry) {
  }

  public void start()
      throws InstantiationException {
  }

  public void shutdown()
      throws Exception {
  }

  public void handleMessage(MessageInfo messageInfo)
      throws RestServiceException {
    // for testing
    RestObject restObject = messageInfo.getRestObject();
    if (restObject != null && restObject instanceof RestRequest && isCustomOperation((RestRequest) restObject)) {
      JSONObject executionData = getExecutionData((RestRequest) restObject);
      String operationType = getOperationType(executionData);
      if (OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION.equals(operationType)) {
        throw new RuntimeException("Requested handling exception");
      } else if (OPERATION_THROW_HANDLING_REST_EXCEPTION.equals(operationType)) {
        throw new RestServiceException("Requested handling rest exception", RestServiceErrorCode.InternalServerError);
      }
    }
    doHandleMessage(messageInfo);
  }

  private void doHandleMessage(MessageInfo messageInfo)
      throws RestServiceException {
    RestMethod restMethod = messageInfo.getRestRequest().getRestMethod();
    if (!isCustomOperation(messageInfo.getRestRequest())) {
      RestObject restObject = messageInfo.getRestObject();
      RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
      if (restObject instanceof RestRequest) {
        restResponseHandler.setContentType("text/plain");
        restResponseHandler.finalizeResponse();
        restResponseHandler.addToBodyAndFlush(restMethod.toString().getBytes(), true);
        messageInfo.getRestObject().release();
      } else if (restObject instanceof RestContent && ((RestContent) restObject).isLast()) {
        restResponseHandler.close();
        messageInfo.getRestObject().release();
      }
    } else {
      executeCustomOperation(messageInfo);
    }
  }

  private boolean isCustomOperation(RestRequest request) {
    return request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY) != null;
  }

  private JSONObject getExecutionData(RestRequest restRequest)
      throws RestServiceException {
    Object executionData = restRequest.getValueOfHeader(EXECUTION_DATA_HEADER_KEY);
    if (executionData != null) {
      try {
        if (!(executionData instanceof JSONObject)) {
          return new JSONObject(executionData.toString());
        } else {
          return (JSONObject) executionData;
        }
      } catch (JSONException e) {
        throw new RestServiceException(EXECUTION_DATA_HEADER_KEY + " header is not JSON",
            RestServiceErrorCode.BadRequest);
      }
    }
    return null;
  }

  private String getOperationType(JSONObject data) {
    try {
      return data.getString(OPERATION_TYPE_KEY);
    } catch (JSONException e) {
      return null;
    }
  }

  private Object getOperationData(JSONObject data) {
    try {
      return data.get(OPERATION_DATA_KEY);
    } catch (JSONException e) {
      return null;
    }
  }

  private void executeCustomOperation(MessageInfo messageInfo)
      throws RestServiceException {
    /**
     *  Suggestion for extending functionality when required :
     *  Check if restRequest is an instance of MockRestRequest. After that you can support
     *  any kind of custom function as long as it can be reached through MockRestRequest.
     */

    RestObject restObject = messageInfo.getRestObject();
    JSONObject executionData = getExecutionData((RestRequest) restObject);
    if (executionData != null) {
      String customOperation = getOperationType(executionData);
      if (OPERATION_THROW_PROCESSING_UNCHECKED_EXCEPTION.equals(customOperation)) {
        throw new RuntimeException("Requested processing exception");
      } else {
        throw new RestServiceException("Unknown " + OPERATION_TYPE_KEY + " - " + customOperation,
            RestServiceErrorCode.BadRequest);
      }
    } else {
      throw new IllegalStateException(
          "executeCustomOperation() was called without checking for presence of " + EXECUTION_DATA_HEADER_KEY
              + " header");
    }
  }
}
