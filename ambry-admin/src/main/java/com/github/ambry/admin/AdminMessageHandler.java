package com.github.ambry.admin;

import com.github.ambry.rest.MessageInfo;
import com.github.ambry.rest.RestErrorCode;
import com.github.ambry.rest.RestException;
import com.github.ambry.rest.RestMessageHandler;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseHandler;


/**
 * Admin specific implementation of RestMessageHandler
 *
 * Supposed to handle GET, POST, DELETE and HEAD. Also is notified on error or when a request is complete
 */
public class AdminMessageHandler extends RestMessageHandler {
  private final AdminMetrics adminMetrics;

  public AdminMessageHandler(AdminMetrics adminMetrics) {
    super(adminMetrics);
    this.adminMetrics = adminMetrics;
  }

  public void onRequestComplete(RestRequest request) {
    request.release();
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
      logger.error("Caught exception while trying to handle an error - " + ee);
    }
  }

  protected void handleGet(MessageInfo messageInfo)
      throws Exception {
    RestRequest request = messageInfo.getRestRequest();
    logger.trace("Handling get request - " + request.getUri());
    if (request.getValuesOfParameterInURI("action") != null) {
      String action = request.getValuesOfParameterInURI("action").get(0);
      if (action.startsWith("echo")) {
        handleEcho(messageInfo);
      } else {
        adminMetrics.unknownActionErrorCount.inc();
        throw new RestException("Unrecognized action to perform " + action, RestErrorCode.UnknownAction);
      }
    }
  }

  protected void handlePost(MessageInfo messageInfo)
      throws Exception {
    logger.trace("Handling post");
  }

  protected void handleDelete(MessageInfo messageInfo)
      throws Exception {
    logger.trace("Handling delete");
  }

  protected void handleHead(MessageInfo messageInfo)
      throws Exception {
    logger.trace("Handling head");
  }

  private void handleEcho(MessageInfo messageInfo)
      throws Exception {
    logger.trace("Handling echo");
    RestResponseHandler responseHandler = messageInfo.getResponseHandler();
    if (messageInfo.getRestObject() instanceof RestRequest
        && messageInfo.getRestRequest().getValuesOfParameterInURI("text") != null) {
      String in = messageInfo.getRestRequest().getValuesOfParameterInURI("text").get(0);
      logger.trace("Client says: " + in);
      responseHandler.setContentType("text/plain");
      responseHandler.finalizeResponse();
      responseHandler.addToBodyAndFlush(in.getBytes(), true);
    } else {
      responseHandler.close();
    }
    messageInfo.releaseRestObject();
  }
}
