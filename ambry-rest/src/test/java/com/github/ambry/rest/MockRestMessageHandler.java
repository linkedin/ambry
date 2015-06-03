package com.github.ambry.rest;

import com.github.ambry.storageservice.BlobStorageService;
import com.github.ambry.storageservice.BlobStorageServiceException;
import com.github.ambry.storageservice.ExecutionData;
import com.github.ambry.storageservice.MockBlobStorageService;
import com.github.ambry.storageservice.MockExecutionData;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class MockRestMessageHandler extends RestMessageHandler {
  // custom operations
  public static String OPERATION_THROW_UNCHECKED_EXCEPTION = "throwUncheckedException";

  private BlobStorageService blobStorageService;

  // for processMessage() tests
  private Map<String, String> translatedAnswers = new HashMap<String, String>();

  public MockRestMessageHandler(BlobStorageService blobStorageService, ServerMetrics serverMetrics) {
    super(serverMetrics);
    this.blobStorageService = blobStorageService;
    populateTranslatedAnswers();
  }

  protected void handleGet(MessageInfo messageInfo)
      throws RestException {
    try {
      if (!isCustomOperation(messageInfo.getRestRequest())) {
        RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
        Object getResponse = blobStorageService.getBlob();
        restResponseHandler.addToBodyAndFlush(translateAnswerIfPossible(getResponse.toString()).getBytes(), true);
        restResponseHandler.close();
      } else {
        executeCustomFunction(messageInfo.getRestRequest());
      }
    } catch (BlobStorageServiceException e) {
      throw new RestException(e, RestErrorCode.getRestErrorCode(e.getErrorCode()));
    }
  }

  protected void handlePost(MessageInfo messageInfo)
      throws RestException {
    try {
      if (!isCustomOperation(messageInfo.getRestRequest())) {
        RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
        String postResponse = blobStorageService.putBlob();
        restResponseHandler.addToBodyAndFlush(translateAnswerIfPossible(postResponse.toString()).getBytes(), true);
        restResponseHandler.close();
        messageInfo.releaseAll();
      } else {
        executeCustomFunction(messageInfo.getRestRequest());
      }
    } catch (BlobStorageServiceException e) {
      throw new RestException(e, RestErrorCode.getRestErrorCode(e.getErrorCode()));
    }
  }

  protected void handleDelete(MessageInfo messageInfo)
      throws RestException {
    try {
      if (!isCustomOperation(messageInfo.getRestRequest())) {
        RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
        if (blobStorageService.deleteBlob()) {
          restResponseHandler.addToBodyAndFlush(RestMethod.DELETE.toString().getBytes(), true);
        }
        restResponseHandler.close();
        messageInfo.releaseAll();
      } else {
        executeCustomFunction(messageInfo.getRestRequest());
      }
    } catch (BlobStorageServiceException e) {
      throw new RestException(e, RestErrorCode.getRestErrorCode(e.getErrorCode()));
    }
  }

  protected void handleHead(MessageInfo messageInfo)
      throws RestException {
    if (!isCustomOperation(messageInfo.getRestRequest())) {
      RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
      // TODO: nothing in BlobStorageService yet. Add when that is upgraded
      restResponseHandler.addToBodyAndFlush(RestMethod.HEAD.toString().getBytes(), true);
      restResponseHandler.close();
      messageInfo.releaseAll();
    } else {
      executeCustomFunction(messageInfo.getRestRequest());
    }
  }

  protected void onError(MessageInfo messageInfo, Exception e) {
    if (messageInfo != null && messageInfo.getResponseHandler() != null) {
      messageInfo.getResponseHandler().onError(e);
    }
  }

  protected void onRequestComplete(RestRequest request)
      throws Exception {
    // nothing to do
  }

  private void populateTranslatedAnswers() {
    translatedAnswers.put(MockBlobStorageService.GET_HANDLED, RestMethod.GET.toString());
    translatedAnswers.put(MockBlobStorageService.PUT_HANDLED, RestMethod.POST.toString());
  }

  private String translateAnswerIfPossible(String originalAnswer) {
    String translatedAnswer = originalAnswer;
    if (translatedAnswers.containsKey(originalAnswer)) {
      translatedAnswer = translatedAnswers.get(originalAnswer);
    }
    return translatedAnswer;
  }

  private boolean isCustomOperation(RestRequest request) {
    return request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY) != null;
  }

  private void executeCustomFunction(RestRequest restRequest)
      throws RestException {
    /**
     *  Suggestion for extending functionality when required :
     *  Check if restRequest is an instance of MockRestRequest. After that you can support
     *  any kind of custom function as long as it can be reached through MockRestRequest.
     */
    Object data = restRequest.getValueOfHeader(EXECUTION_DATA_HEADER_KEY);
    if (data instanceof JSONObject) {
      MockExecutionData executionData = new MockExecutionData((JSONObject) data);
      String customOperation = executionData.getOperationType();
      if (customOperation == OPERATION_THROW_UNCHECKED_EXCEPTION) {
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
