package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of the BlobStorageService for testing purposes.
 */
public class MockBlobStorageService implements BlobStorageService {
  public static String EXECUTION_DATA_HEADER_KEY = "executionData";
  public static String OPERATION_TYPE_KEY = "operationType";
  public static String OPERATION_DATA_KEY = "operationData";

  public static String OPERATION_THROW_HANDLING_ERROR = "throwHandlingError";
  public static String OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION = "throwHandlingRuntimeException";
  public static String OPERATION_THROW_HANDLING_REST_EXCEPTION = "throwHandlingRestException";

  public MockBlobStorageService(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      MetricRegistry metricRegistry) {
    // this constructor is around so that this can be instantiated from the NioServerFactory.
    // we might have uses for the arguments in the future.
  }

  public void start()
      throws InstantiationException {
  }

  public void shutdown() {
  }

  public void handleGet(MessageInfo messageInfo)
      throws RestServiceException {
    doHandleMessage(messageInfo);
  }

  public void handlePost(MessageInfo messageInfo)
      throws RestServiceException {
    doHandleMessage(messageInfo);
  }

  public void handleDelete(MessageInfo messageInfo)
      throws RestServiceException {
    doHandleMessage(messageInfo);
  }

  public void handleHead(MessageInfo messageInfo)
      throws RestServiceException {
    doHandleMessage(messageInfo);
  }

  /**
   * Delegates custom operations. All other messages are handled by echoing the rest method back to the client.
   * @param messageInfo
   * @throws RestServiceException
   */
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

  /**
   * Check for the presence of executionData to determine whether this is a custom operation.
   * @param request
   * @return
   */
  private boolean isCustomOperation(RestRequest request) {
    return request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY) != null;
  }

  /**
   * Extract the data in the executionData header.
   * @param restRequest
   * @return
   * @throws RestServiceException
   */
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

  /**
   * Retrieve operation type from the executionData.
   * @param data
   * @return
   */
  private String getOperationType(JSONObject data) {
    try {
      return data.getString(OPERATION_TYPE_KEY);
    } catch (JSONException e) {
      return null;
    }
  }

  /**
   * Retrieve operationData from the executionData.
   * @param data
   * @return
   */
  private Object getOperationData(JSONObject data) {
    try {
      return data.get(OPERATION_DATA_KEY);
    } catch (JSONException e) {
      return null;
    }
  }

  /**
   * Execute any requested custom operations. (for e.g. throw exceptions to check behaviour of RestMessageHandler).
   * @param messageInfo
   * @throws RestServiceException
   */
  private void executeCustomOperation(MessageInfo messageInfo)
      throws RestServiceException {
    /**
     *  Suggestion for extending functionality when required :
     *  Check if restRequest is an instance of MockRestRequest. If it is, you can support
     *  any kind of custom function as long as it can be reached through MockRestRequest as a callback.
     */

    RestObject restObject = messageInfo.getRestObject();
    JSONObject executionData = getExecutionData((RestRequest) restObject);
    if (executionData != null) {
      String operationType = getOperationType(executionData);
      if (OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION.equals(operationType)) {
        // message is operationType so that it can be verified by the test
        throw new RuntimeException(operationType);
      } else if (OPERATION_THROW_HANDLING_REST_EXCEPTION.equals(operationType)) {
        throw new RestServiceException(operationType, RestServiceErrorCode.InternalServerError);
      } else if (OPERATION_THROW_HANDLING_ERROR.equals(operationType)) {
        throw new Error(operationType);
      } else {
        throw new RestServiceException("Unknown " + OPERATION_TYPE_KEY + " - " + operationType,
            RestServiceErrorCode.BadRequest);
      }
    } else {
      throw new IllegalStateException(
          "executeCustomOperation() was called without checking for presence of " + EXECUTION_DATA_HEADER_KEY
              + " header");
    }
  }
}
