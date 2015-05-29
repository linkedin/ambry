package com.github.ambry.admin;

import com.github.ambry.rest.MessageInfo;
import com.github.ambry.rest.RestErrorCode;
import com.github.ambry.rest.RestException;
import com.github.ambry.rest.RestMessageHandler;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.storageservice.BlobStorageServiceException;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Admin specific implementation of RestMessageHandler
 * <p/>
 * Supposed to handle GET, POST, DELETE and HEAD. Also is notified on error or when a request is complete
 */
public class AdminMessageHandler extends RestMessageHandler {
  private final AdminMetrics adminMetrics;
  private final AdminBlobStorageService adminBlobStorageService;

  public AdminMessageHandler(AdminMetrics adminMetrics, AdminBlobStorageService adminBlobStorageService) {
    super(adminMetrics);
    this.adminMetrics = adminMetrics;
    this.adminBlobStorageService = adminBlobStorageService;
  }

  public void onRequestComplete(RestRequest request) {
    /**
     * NOTE: This is where any cleanup code has to go. This is (has to be) called regardless of
     * the request being concluded successfully or unsuccessfully (i.e. connection interruption).
     * Any state that is maintained per request has to be destroyed here.
     */
    if (request != null) {
      request.release();
    }
  }

  public void onError(MessageInfo messageInfo, Exception e) {
    try {
      if (messageInfo != null && messageInfo.getResponseHandler() != null) {
        messageInfo.getResponseHandler().onError(e);
      } else {
        logger.error("No error response was sent because there is no response handler."
            + " The connection to the client might still be open");
      }
    } catch (Exception ee) {
      //TODO: metric
      logger.error("Caught exception while trying to handle an error - " + ee);
    }
  }

  protected void handleGet(MessageInfo messageInfo)
      throws RestException {
    RestRequest request = messageInfo.getRestRequest();
    logger.trace("Handling get request - " + request.getUri());
    if (!isSpecialOperation(request)) {
      // TODO: this is a traditional get
    } else {
      handleSpecialGetOperation(messageInfo);
    }
  }

  protected void handlePost(MessageInfo messageInfo)
      throws RestException {
    logger.trace("Handling post");
  }

  protected void handleDelete(MessageInfo messageInfo)
      throws RestException {
    logger.trace("Handling delete");
  }

  protected void handleHead(MessageInfo messageInfo)
      throws RestException {
    logger.trace("Handling head");
  }

  private boolean isSpecialOperation(RestRequest request) {
    return request.getPathPart(0) == null || request.getPathPart(0).isEmpty();
  }

  private void handleSpecialGetOperation(MessageInfo messageInfo)
      throws RestException {
    AdminExecutionData executionData = extractExecutionData(messageInfo.getRestRequest());
    AdminOperationType operationType = AdminOperationType.convert(executionData.getOperationType());
    switch (operationType) {
      case Echo:
        handleEcho(messageInfo, executionData);
        break;
      case GetReplicasForBlobId:
        handleGetReplicasForBlobId(messageInfo, executionData);
        break;
      default:
        throw new RestException("Unknown operation type - " + executionData.getOperationType(),
            RestErrorCode.UnknownOperationType);
    }
  }

  private AdminExecutionData extractExecutionData(RestRequest request)
      throws RestException {
    try {
      JSONObject data = new JSONObject(request.getValueOfHeader(EXECUTION_DATA_HEADER_KEY).toString());
      return new AdminExecutionData(data);
    } catch (JSONException e) {
      throw new RestException(EXECUTION_DATA_HEADER_KEY + " header missing or not valid JSON - " + e,
          RestErrorCode.BadRequest);
    } catch (IllegalArgumentException e) {
      throw new RestException(EXECUTION_DATA_HEADER_KEY + " header does not contain required data - " + e,
          RestErrorCode.BadRequest);
    }
  }

  private void handleEcho(MessageInfo messageInfo, AdminExecutionData executionData)
      throws RestException {
    logger.trace("Handling echo");
    try {
      RestResponseHandler responseHandler = messageInfo.getResponseHandler();
      if (messageInfo.getRestObject() instanceof RestRequest) {
        String echoStr = adminBlobStorageService.execute(executionData).getOperationResult().toString();
        if (echoStr != null) {
          responseHandler.setContentType("text/plain");
          responseHandler.finalizeResponse();
          responseHandler.addToBodyAndFlush(echoStr.getBytes(), true);
        } else {
          throw new RestException("Did not get a result for the echo operation", RestErrorCode.ResponseBuildingFailure);
        }
      } else {
        responseHandler.close();
      }
    } catch (BlobStorageServiceException e) {
      throw new RestException(e, RestErrorCode.getRestErrorCode(e.getErrorCode()));
    } finally {
      messageInfo.releaseRestObject();
    }
  }

  private void handleGetReplicasForBlobId(MessageInfo messageInfo, AdminExecutionData executionData)
      throws RestException {
    logger.trace("Handling getReplicas");
    try {
      RestResponseHandler responseHandler = messageInfo.getResponseHandler();
      if (messageInfo.getRestObject() instanceof RestRequest) {
        String replicaStr = adminBlobStorageService.execute(executionData).getOperationResult().toString();
        if (replicaStr != null) {
          responseHandler.setContentType("application/json");
          responseHandler.finalizeResponse();
          responseHandler.addToBodyAndFlush(replicaStr.getBytes(), true);
        } else {
          throw new RestException("Did not get a result for the GetReplicasForBlobId operation",
              RestErrorCode.ResponseBuildingFailure);
        }
      } else {
        responseHandler.close();
      }
    } catch (BlobStorageServiceException e) {
      throw new RestException(e, RestErrorCode.getRestErrorCode(e.getErrorCode()));
    } finally {
      messageInfo.releaseRestObject();
    }
  }
}
