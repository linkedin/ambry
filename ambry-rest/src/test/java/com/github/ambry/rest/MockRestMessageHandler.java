package com.github.ambry.rest;

import com.github.ambry.storageservice.ExecutionData;
import com.github.ambry.storageservice.MockExecutionData;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class MockRestMessageHandler extends RestMessageHandler {
  // custom operations
  public static String OPERATION_THROW_UNCHECKED_EXCEPTION = "throwUncheckedException";

  public MockRestMessageHandler(MockExternalServerMetrics serverMetrics) {
    super(serverMetrics);
  }

  protected void handleGet(MessageInfo messageInfo)
      throws RestException {
    if(!isCustomOperation(messageInfo.getRestRequest())) {
      RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
      restResponseHandler.addToBodyAndFlush(RestMethod.GET.toString().getBytes(), true);
      restResponseHandler.close();
    } else {
      executeCustomFunction(messageInfo.getRestRequest());
    }
  }

  protected void handlePost(MessageInfo messageInfo)
      throws RestException {
    if(!isCustomOperation(messageInfo.getRestRequest())) {
      RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
      restResponseHandler.addToBodyAndFlush(RestMethod.POST.toString().getBytes(), true);
      restResponseHandler.close();
      messageInfo.releaseAll();
    } else {
      executeCustomFunction(messageInfo.getRestRequest());
    }
  }


  protected void handleDelete(MessageInfo messageInfo)
      throws RestException {
    if(!isCustomOperation(messageInfo.getRestRequest())) {
      RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
      restResponseHandler.addToBodyAndFlush(RestMethod.DELETE.toString().getBytes(), true);
      restResponseHandler.close();
      messageInfo.releaseAll();
    } else {
      executeCustomFunction(messageInfo.getRestRequest());
    }
  }

  protected void handleHead(MessageInfo messageInfo)
      throws RestException {
    if(!isCustomOperation(messageInfo.getRestRequest())) {
      RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
      restResponseHandler.addToBodyAndFlush(RestMethod.HEAD.toString().getBytes(), true);
      restResponseHandler.close();
      messageInfo.releaseAll();
    } else {
      executeCustomFunction(messageInfo.getRestRequest());
    }
  }

  protected void onError(MessageInfo messageInfo, Exception e) {
    if(messageInfo != null && messageInfo.getResponseHandler() != null) {
      messageInfo.getResponseHandler().onError(e);
    }
  }

  protected void onRequestComplete(RestRequest request)
      throws Exception {
    // nothing to do
  }

  private boolean isCustomOperation(RestRequest request) {
    return request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY) != null;
  }

  private void executeCustomFunction(RestRequest restRequest) throws RestException {
    /**
     *  Suggestion for extending functionality when required :
     *  Check if restRequest is an instance of MockRestRequest. After that you can support
     *  any kind of custom function as long as it can be reached through MockRestRequest.
     */
    Object data = restRequest.getValueOfHeader(EXECUTION_DATA_HEADER_KEY);
    if(data instanceof JSONObject) {
      MockExecutionData executionData = new MockExecutionData((JSONObject) data);
      String customOperation = executionData.getOperationType();
      if(customOperation == OPERATION_THROW_UNCHECKED_EXCEPTION) {
        throw new RuntimeException("Requested exception");
      } else {
        throw new RestException("Unknown " + ExecutionData.OPERATION_TYPE_KEY + " - " + customOperation,
            RestErrorCode.BadRequest);
      }
    } else {
      throw new RestException(EXECUTION_DATA_HEADER_KEY + " should be JSONObject", RestErrorCode.BadRequest);
    }
  }
}
