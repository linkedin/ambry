package com.github.ambry.restservice;

import java.io.ByteArrayOutputStream;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of RestResponseHandler that can be used by tests.
 */
public class MockRestResponseHandler implements RestResponseHandler {
  public static String RESPONSE_STATUS_KEY = "responseStatus";
  public static String RESPONSE_HEADERS_KEY = "responseHeaders";
  public static String CONTENT_TYPE_HEADER_KEY = "contentType";
  public static String ERROR_MESSAGE_KEY = "errorMessage";

  public static String STATUS_OK = "OK";
  public static String STATUS_ERROR = "Error";

  private boolean channelClosed = false;
  private boolean errorSent = false;
  private boolean responseFinalized = false;
  private boolean responseFlushed = false;

  /**
   * The response in constructed as a json object
   * Contains:
   * 1. responseStatus - the reponse status.
   * 2. responseHeaders - the response headers as a json object.
   *
   * In case of error it contains an additional field
   * 3. errorMessage
   *
   * Headers
   * 1. contentType
   */
  private JSONObject response = new JSONObject();
  /**
   * When addToBody is called, the bytes are added to end of this ByteArrayOutputStream.
   * On flush, the bytes are emptied into bodyStringBuilder and reset.
   * This essentially represents the un-flushed body only.
   */
  private ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
  /**
   * String builder for the body. Whenever a flush is called, the raw bytes in bodyBytes are appended to this.
   * This essentially represents the flushed body only.
   */
  private StringBuilder bodyStringBuilder = new StringBuilder();

  public void addToBody(byte[] data, boolean isLast) {
    verifyChannelOpen();
    bodyBytes.write(data, 0, data.length);
  }

  public void addToBodyAndFlush(byte[] data, boolean isLast) {
    addToBody(data, isLast);
    flush();
  }

  public void finalizeResponse()
      throws RestServiceException {
    verifyChannelOpen();
    verifyResponseAlive();
    try {
      response.put(RESPONSE_STATUS_KEY, STATUS_OK);
      responseFinalized = true;
    } catch (JSONException e) {
      throw new RestServiceException("Failed to build response", RestServiceErrorCode.ResponseBuildingFailure);
    }
  }

  public void finalizeResponseAndFlush()
      throws RestServiceException {
    finalizeResponse();
    flush();
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
        // nothing to do
      } catch (RestServiceException e) {
        // nothing to do
      }
    }
  }

  public void onRequestComplete() {
    // nothing to do
  }

  public void setContentType(String type)
      throws RestServiceException {
    try {
      if (!response.has(RESPONSE_HEADERS_KEY)) {
        response.put(RESPONSE_HEADERS_KEY, new JSONObject());
      }
      response.getJSONObject(RESPONSE_HEADERS_KEY).put(CONTENT_TYPE_HEADER_KEY, type);
    } catch (JSONException e) {
      throw new RestServiceException("Unable to set content type", RestServiceErrorCode.ResponseBuildingFailure);
    }
  }

  /**
   * Verify state of response so that we do not try to finalize response more than once.
   */
  private void verifyResponseAlive() {
    if (responseFinalized) {
      throw new IllegalStateException("Cannot re-finalize response");
    }
  }

  /**
   * Verify that channel is still open.
   */
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

  public String getBody()
      throws RestServiceException {
    return getFlushedBody() + bodyBytes.toString(); // the full body contains both flushed and un-flushed data.
  }

  public String getFlushedBody()
      throws RestServiceException {
    return bodyStringBuilder.toString();
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
