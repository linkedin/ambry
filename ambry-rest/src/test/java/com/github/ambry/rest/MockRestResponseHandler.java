package com.github.ambry.rest;

import java.io.ByteArrayOutputStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public class MockRestResponseHandler implements RestResponseHandler {
  public static String RESPONSE_STATUS_KEY = "responseStatus";
  public static String RESPONSE_HEADERS_KEY = "responseHeaders";
  public static String CONTENT_TYPE_HEADER_KEY = "contentType";
  public static String ERROR_MESSAGE_KEY = "errorMessage";

  public static String STATUS_OK = "OK";
  public static String STATUS_ERROR = "Error";

  public static String BODY_STRING_KEY = "bodyString";

  private boolean channelClosed = false;
  private boolean errorSent = false;
  private boolean responseFinalized = false;
  private boolean responseFlushed = false;

  private JSONObject response = new JSONObject();

  private StringBuilder bodyStringBuilder = new StringBuilder();
  private ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();

  private Logger logger = LoggerFactory.getLogger(getClass());

  public void addToBody(byte[] data, boolean isLast) {
    addToBody(data, isLast, false);
  }

  public void addToBodyAndFlush(byte[] data, boolean isLast) {
    addToBody(data, isLast, true);
  }

  private void addToBody(byte[] data, boolean isLast, boolean flush) {
    verifyChannelOpen();
    bodyBytes.write(data, 0, data.length);
    if (flush) {
      flush();
    }
  }

  public void finalizeResponse()
      throws RestException {
    finalizeResponse(false);
  }

  public void finalizeResponseAndFlush()
      throws RestException {
    finalizeResponse(true);
  }

  private void finalizeResponse(boolean flush)
      throws RestException {
    verifyChannelOpen();
    verifyResponseAlive();
    try {
      response.put(RESPONSE_STATUS_KEY, STATUS_OK);
      responseFinalized = true;
    } catch (JSONException e) {
      throw new RestException("Failed to build response", RestErrorCode.ResponseBuildingFailure);
    }

    if (flush) {
      flush();
    }
  }

  public void flush() {
    responseFlushed = true;
    bodyStringBuilder.append(bodyBytes.toString());
    bodyBytes.reset();
  }

  public void close() {
    verifyChannelOpen();
    channelClosed = true;
  }

  public void onError(Throwable cause) {
    if (!errorSent) {
      try {
        setContentType("text/plain");
        response.put(RESPONSE_STATUS_KEY, STATUS_ERROR);
        response.put(ERROR_MESSAGE_KEY, cause.toString());
        flush();
        errorSent = true;
        close();
      } catch (JSONException e) {
        logger.error("Error while trying to record exception. Original exception - " + cause + ". New error - " + e);
      } catch (RestException e) {
        logger.error("Error while trying to record exception. Original exception - " + cause + ". New error - " + e);
      }
    }
  }

  public void onRequestComplete() {
    // nothing to do
  }

  public void setContentType(String type)
      throws RestException {
    try {
      if (!response.has(RESPONSE_HEADERS_KEY)) {
        response.put(RESPONSE_HEADERS_KEY, new JSONObject());
      }
      response.getJSONObject(RESPONSE_HEADERS_KEY).put(CONTENT_TYPE_HEADER_KEY, type);
    } catch (JSONException e) {
      throw new RestException("Unable to set content type", RestErrorCode.ResponseBuildingFailure);
    }
  }

  private void verifyResponseAlive() {
    if (responseFinalized) {
      throw new IllegalStateException("Cannot re-finalize response");
    }
  }

  private void verifyChannelOpen() {
    if (channelClosed) {
      throw new IllegalStateException("Channel has already been closed before write");
    }
  }

  // mock response handler specific functions (for testing)
  public JSONObject getResponse() {
    return response;
  }

  public JSONObject getFlushedResponse() {
    if (responseFlushed) {
      return getResponse();
    }
    return null;
  }

  public JSONObject getBody()
      throws RestException {
    try {
      JSONObject body = getFlushedBody();
      String fullBody = body.getString(BODY_STRING_KEY) + bodyBytes.toString();
      body.put(BODY_STRING_KEY, fullBody);
      return body;
    } catch (JSONException e) {
      throw new RestException("Failed to build body", RestErrorCode.ResponseBuildingFailure);
    }
  }

  public JSONObject getFlushedBody()
      throws RestException {
    try {
      JSONObject body = new JSONObject();
      body.put(BODY_STRING_KEY, bodyStringBuilder.toString());
      return body;
    } catch (JSONException e) {
      throw new RestException("Failed to build body", RestErrorCode.ResponseBuildingFailure);
    }
  }

  public boolean isChannelClosed() {
    return channelClosed;
  }

  public boolean isErrorSent() {
    return errorSent;
  }

  public boolean isResponseFinalized() {
    return responseFinalized;
  }

  public boolean isResponseFlushed() {
    return responseFlushed;
  }
}
