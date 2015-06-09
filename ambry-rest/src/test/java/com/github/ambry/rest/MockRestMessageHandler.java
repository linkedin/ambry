package com.github.ambry.rest;

import com.github.ambry.storageservice.BlobStorageService;
import com.github.ambry.storageservice.BlobStorageServiceException;
import com.github.ambry.storageservice.ExecutionData;
import com.github.ambry.storageservice.MockBlobStorageService;
import com.github.ambry.storageservice.MockExecutionData;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * TODO: write description
 */
public class MockRestMessageHandler extends RestMessageHandler {
  // custom operations
  public static String OPERATION_THROW_HANDLING_REST_EXCEPTION = "throwHandlingRestException";
  public static String OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION = "throwHandlingRuntimeException";
  public static String OPERATION_THROW_PROCESSING_UNCHECKED_EXCEPTION = "throwProcessingUncheckedException";
  public static String OPERATION_CREATE_FILE = "createFile";

  // createFile Data
  public static String CREATE_FILE_PATH = "path";

  private BlobStorageService blobStorageService;

  // for processMessage() tests
  private Map<String, String> translatedAnswers = new HashMap<String, String>();

  public MockRestMessageHandler(BlobStorageService blobStorageService, ServerMetrics serverMetrics) {
    super(serverMetrics);
    this.blobStorageService = blobStorageService;
    populateTranslatedAnswers();
  }

  @Override
  public void handleMessage(MessageInfo messageInfo)
      throws RestException {
    // for testing
    if(messageInfo.getRestObject() instanceof RestRequest) {
      RestRequest restRequest = messageInfo.getRestRequest();
      if(restRequest != null && isCustomOperation(restRequest)) {
        ExecutionData executionData = extractExecutionData(restRequest);
        if(executionData.getOperationType().equals(OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION)) {
          throw new RuntimeException("Requested handling exception");
        } else if(executionData.getOperationType().equals(OPERATION_THROW_HANDLING_REST_EXCEPTION)) {
          throw new RestException("Requested handling rest exception", RestErrorCode.InternalServerError);
        }
      }
    }
    super.handleMessage(messageInfo);
  }

  protected void handleGet(MessageInfo messageInfo)
      throws RestException {
    try {
      if (!isCustomOperation(messageInfo.getRestRequest())) {
        RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
        if(messageInfo.getRestObject() instanceof RestRequest) {
          Object getResponse = blobStorageService.getBlob();
          restResponseHandler.setContentType("text/plain");
          restResponseHandler.finalizeResponse();
          restResponseHandler.addToBodyAndFlush(translateAnswerIfPossible(getResponse.toString()).getBytes(), true);
        } else {
          restResponseHandler.close();
        }
      } else {
        executeCustomOperation(messageInfo);
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
        restResponseHandler.setContentType("text/plain");
        restResponseHandler.finalizeResponse();
        restResponseHandler.addToBodyAndFlush(translateAnswerIfPossible(postResponse.toString()).getBytes(), true);
        restResponseHandler.close();
        messageInfo.releaseAll();
      } else {
        executeCustomOperation(messageInfo);
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
          restResponseHandler.setContentType("text/plain");
          restResponseHandler.finalizeResponse();
          restResponseHandler.addToBodyAndFlush(RestMethod.DELETE.toString().getBytes(), true);
        }
        restResponseHandler.close();
        messageInfo.releaseAll();
      } else {
        executeCustomOperation(messageInfo);
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
      restResponseHandler.setContentType("text/plain");
      restResponseHandler.finalizeResponse();
      restResponseHandler.addToBodyAndFlush(RestMethod.HEAD.toString().getBytes(), true);
      restResponseHandler.close();
      messageInfo.releaseAll();
    } else {
      executeCustomOperation(messageInfo);
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

  private ExecutionData extractExecutionData(RestRequest restRequest) throws RestException {
    Object inputData = restRequest.getValueOfHeader(EXECUTION_DATA_HEADER_KEY);
    ExecutionData executionData = null;

    if(inputData != null) {
      try {
        if (!(inputData instanceof JSONObject)) {
          executionData = new MockExecutionData(new JSONObject(inputData.toString()));
        } else {
          executionData = new MockExecutionData((JSONObject)inputData);
        }
      } catch (JSONException e) {
        throw new RestException(RestMessageHandler.EXECUTION_DATA_HEADER_KEY + " header is not JSON",
            RestErrorCode.BadRequest);
      }
    }

    return executionData;
  }

  private boolean isCustomOperation(RestRequest request) {
    return request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY) != null;
  }

  private void executeCustomOperation(MessageInfo messageInfo)
      throws RestException {
    /**
     *  Suggestion for extending functionality when required :
     *  Check if restRequest is an instance of MockRestRequest. After that you can support
     *  any kind of custom function as long as it can be reached through MockRestRequest.
     */

    ExecutionData executionData = extractExecutionData(messageInfo.getRestRequest());
    if(executionData != null) {
      String customOperation = executionData.getOperationType();
      if (customOperation .equals(OPERATION_THROW_PROCESSING_UNCHECKED_EXCEPTION)) {
        throw new RuntimeException("Requested processing exception");
      } else if (customOperation.equals(OPERATION_CREATE_FILE)) {
        handleCreateFile(messageInfo, executionData.getOperationData());
      } else {
        throw new RestException("Unknown " + ExecutionData.OPERATION_TYPE_KEY + " - " + customOperation,
            RestErrorCode.BadRequest);
      }
    } else {
      throw new IllegalStateException("executeCustomOperation() was called without checking for presence of "
          + EXECUTION_DATA_HEADER_KEY + " header");
    }
  }

  private void handleCreateFile(MessageInfo messageInfo, JSONObject operationData) throws RestException {
    if(messageInfo.getRestObject() instanceof RestRequest) {
      try {
        createFile(operationData);
        messageInfo.getResponseHandler().finalizeResponse();
        messageInfo.getResponseHandler().addToBodyAndFlush(RestMethod.GET.toString().getBytes(), true);
      } catch (IllegalArgumentException e) {
        throw new RestException(e, RestErrorCode.BadRequest);
      } catch (Exception e) {
        throw new RestException("Unable to create file requested - " + e, RestErrorCode.InternalServerError);
      }
    } else {
      messageInfo.getResponseHandler().close();
    }
  }

  private void createFile(JSONObject operationData) throws Exception {
    if (!operationData.has(CREATE_FILE_PATH)) {
      throw new IllegalArgumentException("Invalid data for " + OPERATION_CREATE_FILE + ". Missing " + CREATE_FILE_PATH);
    }
    String path = operationData.getString(CREATE_FILE_PATH);
    File file = new File(path);
    if(!file.createNewFile()) {
      throw new Exception("Unable to create file at path " + path);
    }
  }


}
