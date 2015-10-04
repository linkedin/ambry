package com.github.ambry.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of {@link RestResponseChannel} that can be used by tests.
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
 * When {@link #write(ByteBuffer)} is called, the input bytes are added to a {@link ByteArrayOutputStream}. This
 * represents the unflushed responseBody. On {@link #flush()}, the {@link ByteArrayOutputStream} is emptied into a
 * {@link StringBuilder} and reset. The {@link StringBuilder} represents the flushed responseBody.
 * <p/>
 * All functions are synchronized because this is expected to be thread safe (very coarse grained but this is not
 * expected to be performant, just usable).
 */
public class MockRestResponseChannel implements RestResponseChannel {
  public static String RESPONSE_STATUS_KEY = "responseStatus";
  public static String RESPONSE_HEADERS_KEY = "responseHeaders";
  public static String CONTENT_TYPE_HEADER_KEY = "contentType";
  public static String ERROR_MESSAGE_KEY = "errorMessage";
  public static String STATUS_OK = "OK";
  public static String STATUS_ERROR = "Error";

  private AtomicBoolean channelOpen = new AtomicBoolean(true);
  private AtomicBoolean requestComplete = new AtomicBoolean(false);
  private AtomicBoolean responseMetadataFinalized = new AtomicBoolean(false);
  private AtomicBoolean responseMetadataFlushed = new AtomicBoolean(false);
  private final JSONObject responseMetadata = new JSONObject();
  private final ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
  private final StringBuilder bodyStringBuilder = new StringBuilder();

  public MockRestResponseChannel()
      throws JSONException {
    responseMetadata.put(RESPONSE_STATUS_KEY, STATUS_OK);
  }

  @Override
  public synchronized int write(ByteBuffer src)
      throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    responseMetadataFinalized.set(true);
    int bytesWritten = src.remaining();
    for (int i = 0; i < bytesWritten; i++) {
      bodyBytes.write(src.get());
    }
    return bytesWritten;
  }

  @Override
  public synchronized void flush()
      throws RestServiceException {
    if (!isOpen()) {
      throw new IllegalStateException("Flush did not succeed because channel is already closed");
    }
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
          // TODO: Write proper status.
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

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() {
    channelOpen.set(false);
  }

  // MockRestResponseChannel specific functions (for testing)

  /**
   * Gets the current responseMetadata whether it has been flushed or not (Might not be the final response metadata).
   * @return the response metadata as a {@link JSONObject}.
   */
  public synchronized JSONObject getResponseMetadata() {
    return responseMetadata;
  }

  /**
   * Gets the responseMetadata if it has been flushed.
   * @return the response metadata as a {@link JSONObject} if it has been flushed.
   */
  public synchronized JSONObject getFlushedResponseMetadata() {
    if (responseMetadataFlushed.get()) {
      return getResponseMetadata();
    }
    return null;
  }

  /**
   * Gets the responseMetadata body including both flushed and un-flushed data.
   * @return both the flushed and un-flushed response body as a {@link String}.
   */
  public synchronized String getResponseBody() {
    return getFlushedResponseBody() + bodyBytes.toString();
  }

  /**
   * Gets the responseMetadata body that has already been flushed.
   * @return flushed response body as a {@link String}.
   */
  public synchronized String getFlushedResponseBody() {
    return bodyStringBuilder.toString();
  }
}
