package com.github.ambry.rest;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of {@link RestResponseHandler} that can be used by tests.
 * <p/>
 * The responseMetadata is stored in-memory and data is "flushed" by moving it to a different data structure. The
 * responseMetadata and responseBody (flushed or non-flushed) can be obtained through APIs to check correctness.
 * <p/>
 * The responseMetadata in constructed as a {@link JSONObject} that contains the following fields: -
 * 1. "responseStatus" - String - the response status, "OK" or "Error".
 * 2. "responseHeaders" - {@link JSONObject} - the response headers as key value pairs.
 * In case of error it contains an additional field
 * 3. "errorMessage" - String - description of the error.
 * <p/>
 * List of possible responseHeaders: -
 * 1. "contentType" - String - the content type of the data in the response.
 * <p/>
 * When {@link MockRestResponseHandler#addToResponseBody(byte[], boolean)} is called, the input bytes are added to a
 * {@link ByteArrayOutputStream}. This represents the unflushed responseBody. On
 * {@link MockRestResponseHandler#flush()}, the {@link ByteArrayOutputStream} is emptied into a {@link StringBuilder}
 * and reset. The {@link StringBuilder} represents the flushed responseBody.
 * <p/>
 * All functions are synchronized because this is expected to be thread safe (very coarse grained but this is not
 * expected to be performant, just .
 */
public class MockRestResponseHandler implements RestResponseHandler {
  public static String RESPONSE_STATUS_KEY = "responseStatus";
  public static String RESPONSE_HEADERS_KEY = "responseHeaders";
  public static String CONTENT_TYPE_HEADER_KEY = "contentType";
  public static String ERROR_MESSAGE_KEY = "errorMessage";
  public static String STATUS_OK = "OK";
  public static String STATUS_ERROR = "Error";

  private AtomicBoolean channelActive = new AtomicBoolean(true);
  private AtomicBoolean requestComplete = new AtomicBoolean(false);
  private AtomicBoolean responseMetadataFinalized = new AtomicBoolean(false);
  private AtomicBoolean responseMetadataFlushed = new AtomicBoolean(false);
  private final JSONObject responseMetadata = new JSONObject();
  private final ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
  private final StringBuilder bodyStringBuilder = new StringBuilder();

  public MockRestResponseHandler()
      throws JSONException {
    responseMetadata.put(RESPONSE_STATUS_KEY, STATUS_OK);
  }

  @Override
  public synchronized void addToResponseBody(byte[] data, boolean isLast)
      throws RestServiceException {
    verifyChannelActive();
    responseMetadataFinalized.set(true);
    bodyBytes.write(data, 0, data.length);
  }

  @Override
  public synchronized void flush()
      throws RestServiceException {
    verifyChannelActive();
    responseMetadataFinalized.set(true);
    responseMetadataFlushed.set(true);
    bodyStringBuilder.append(bodyBytes.toString());
    bodyBytes.reset();
  }

  @Override
  public synchronized void onRequestComplete(Throwable cause, boolean forceClose) {
    if (requestComplete.compareAndSet(false, true)) {
      try {
        if (!responseMetadataFinalized.get() && cause != null) {
          setContentType("text/plain; charset=UTF-8");
          responseMetadata.put(RESPONSE_STATUS_KEY, STATUS_ERROR);
          responseMetadata.put(ERROR_MESSAGE_KEY, cause.toString());
          responseMetadataFinalized.set(true);
        } else if (cause != null) {
          throw new IllegalStateException("Discovered that a responseMetadata had already been sent to the client when"
              + " attempting to send an error responseMetadata. The original cause of error follows: ", cause);
        }
        flush();
        close();
      } catch (JSONException e) {
        // nothing to do
      } catch (RestServiceException e) {
        // nothing to do
      }
    }
  }

  @Override
  public boolean isRequestComplete() {
    return requestComplete.get();
  }

  @Override
  public synchronized void setContentType(String type)
      throws RestServiceException {
    if (!responseMetadataFinalized.get()) {
      try {
        if (!responseMetadata.has(RESPONSE_HEADERS_KEY)) {
          responseMetadata.put(RESPONSE_HEADERS_KEY, new JSONObject());
        }
        responseMetadata.getJSONObject(RESPONSE_HEADERS_KEY).put(CONTENT_TYPE_HEADER_KEY, type);
      } catch (JSONException e) {
        throw new RestServiceException("Unable to set content type", RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      throw new RestServiceException("Trying to change responseMetadata after it has been written to channel",
          RestServiceErrorCode.IllegalResponseMetadataStateTransition);
    }
  }

  private void close()
      throws RestServiceException {
    channelActive.set(false);
  }

  /**
   * Verify that channel is still active.
   */
  private void verifyChannelActive()
      throws RestServiceException {
    if (!channelActive.get()) {
      throw new RestServiceException("Channel has already been closed before write",
          RestServiceErrorCode.ChannelAlreadyClosed);
    }
  }

  // MockRestResponseHandler specific functions (for testing)

  /**
   * Gets the current responseMetadata whether it has been flushed or not (Might not be the final response metadata).
   * @return - get response metadata.
   */
  public synchronized JSONObject getResponseMetadata() {
    return responseMetadata;
  }

  /**
   * Gets the responseMetadata if it has been flushed.
   * @return  - get response metadata if flushed.
   */
  public synchronized JSONObject getFlushedResponseMetadata() {
    if (responseMetadataFlushed.get()) {
      return getResponseMetadata();
    }
    return null;
  }

  /**
   * Gets the responseMetadata body including both flushed and un-flushed data.
   * @return - flushed and un-flushed response body.
   * @throws RestServiceException
   */
  public synchronized String getResponseBody()
      throws RestServiceException {
    return getFlushedResponseBody() + bodyBytes.toString();
  }

  /**
   * Gets the responseMetadata body that has already been flushed.
   * @return - flushed response body.
   * @throws RestServiceException
   */
  public synchronized String getFlushedResponseBody()
      throws RestServiceException {
    return bodyStringBuilder.toString();
  }

  /**
   * Gets the active state of the channel.
   * @return - true if active, false if not
   */
  public synchronized boolean getChannelActive() {
    return channelActive.get();
  }
}
