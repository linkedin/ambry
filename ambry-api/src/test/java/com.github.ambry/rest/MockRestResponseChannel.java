package com.github.ambry.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
 * 1. "responseStatus" - {@link ResponseStatus} as String - the response status.
 * 2. "responseHeaders" - {@link JSONObject} - the response headers as key value pairs.
 * <p/>
 * List of possible responseHeaders: -
 * 1. "Content-Type" - String - the type of the content in the response.
 * 2. "Content-Length" - Long - the length of content in the response.
 * 3. "Location" - String - The location of a newly created resource.
 * 4. "Last-Modified" - Date - The last modified time of the resource.
 * 5. "Expires" - Date - The expire time for the resource.
 * 6. "Cache-Control" - String - The cache control of the response.
 * 7. "Pragma" - String - The pragma of the response.
 * 8. "Date" - Date - The date of the response.
 * <p/>
 * When {@link #write(ByteBuffer)} is called, the input bytes are added to a {@link ByteArrayOutputStream}. This
 * represents the unflushed responseBody. On {@link #flush()}, the {@link ByteArrayOutputStream} is emptied into another
 * {@link ByteArrayOutputStream} and reset. The other {@link ByteArrayOutputStream} represents the flushed responseBody.
 * <p/>
 * All functions are synchronized because this is expected to be thread safe (very coarse grained but this is not
 * expected to be performant, just usable).
 */
public class MockRestResponseChannel implements RestResponseChannel {
  /**
   * List of "events" (function calls) that can occur inside MockRestResponseChannel.
   */
  public static enum Event {
    Write,
    Flush,
    OnRequestComplete,
    SetStatus,
    SetContentType,
    SetContentLength,
    SetLocation,
    SetLastModified,
    SetExpires,
    SetCacheControl,
    SetPragma,
    SetDate,
    SetHeader,
    IsOpen,
    Close
  }

  /**
   * Callback that can be used to listen to events that happen inside MockRestResponseChannel.
   * <p/>
   * Please *do not* write tests that check for events *not* arriving. Events will not arrive if there was an exception
   * in the function that triggers the event or inside the function that notifies listeners.
   */
  public interface EventListener {

    /**
     * Called when an event (function call) finishes successfully in MockRestResponseChannel. Does *not* trigger if the
     * event (function) fails.
     * @param mockRestResponseChannel the {@link MockRestResponseChannel} where the event occurred.
     * @param event the {@link Event} that occurred.
     */
    public void onEventComplete(MockRestResponseChannel mockRestResponseChannel, Event event);
  }

  // main fields
  public static final String RESPONSE_STATUS_KEY = "responseStatus";
  public static final String RESPONSE_HEADERS_KEY = "responseHeaders";

  // headers
  public static final String CONTENT_TYPE_HEADER_KEY = "Content-Type";
  public static final String CONTENT_LENGTH_HEADER_KEY = "Content-Length";
  public static final String LOCATION_HEADER_KEY = "Location";
  public static final String LAST_MODIFIED_KEY = "Last-Modified";
  public static final String EXPIRES_KEY = "Expires";
  public static final String CACHE_CONTROL_KEY = "Cache-Control";
  public static final String PRAGMA_KEY = "Pragma";
  public static final String DATE_KEY = "Date";

  private AtomicBoolean channelOpen = new AtomicBoolean(true);
  private AtomicBoolean requestComplete = new AtomicBoolean(false);
  private AtomicBoolean responseMetadataFinalized = new AtomicBoolean(false);
  private AtomicBoolean responseMetadataFlushed = new AtomicBoolean(false);
  private final JSONObject responseMetadata = new JSONObject();
  private final ByteArrayOutputStream unflushedBodyBytes = new ByteArrayOutputStream();
  private final ByteArrayOutputStream flushedBodyBytes = new ByteArrayOutputStream();
  private final List<EventListener> listeners = new ArrayList<EventListener>();

  private volatile Throwable cause = null;

  public MockRestResponseChannel()
      throws JSONException {
    responseMetadata.put(RESPONSE_STATUS_KEY, ResponseStatus.Ok);
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
      unflushedBodyBytes.write(src.get());
    }
    onEventComplete(Event.Write);
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
    try {
      unflushedBodyBytes.writeTo(flushedBodyBytes);
    } catch (IOException e) {
      throw new RestServiceException("Flush failed", RestServiceErrorCode.ChannelWriteError);
    }
    unflushedBodyBytes.reset();
    onEventComplete(Event.Flush);
  }

  @Override
  public synchronized void onResponseComplete(Throwable cause) {
    if (requestComplete.compareAndSet(false, true)) {
      this.cause = cause;
      try {
        if (!responseMetadataFinalized.get() && cause != null) {
          // clear headers
          responseMetadata.put(RESPONSE_HEADERS_KEY, new JSONObject());
          setContentType("text/plain; charset=UTF-8");
          ResponseStatus status = ResponseStatus.InternalServerError;
          if (cause instanceof RestServiceException) {
            status = ResponseStatus.getResponseStatus(((RestServiceException) cause).getErrorCode());
          }
          responseMetadata.put(RESPONSE_STATUS_KEY, status);
          unflushedBodyBytes.write(cause.toString().getBytes());
          responseMetadataFinalized.set(true);
        } else if (cause != null) {
          throw new IllegalStateException("Discovered that a responseMetadata had already been sent to the client when"
              + " attempting to send an error responseMetadata. The original cause of error follows: ", cause);
        }
        flush();
        close();
        onEventComplete(Event.OnRequestComplete);
      } catch (Exception e) {
        // nothing to do
      }
    }
  }

  @Override
  public synchronized void setStatus(ResponseStatus status)
      throws RestServiceException {
    if (isOpen() && !responseMetadataFinalized.get()) {
      try {
        responseMetadata.put(RESPONSE_STATUS_KEY, status);
        onEventComplete(Event.SetStatus);
      } catch (JSONException e) {
        throw new RestServiceException("Unable to set Status", RestServiceErrorCode.ResponseBuildingFailure);
      }
    } else {
      throw new RestServiceException("Cannot change response metadata",
          RestServiceErrorCode.IllegalResponseMetadataStateTransition);
    }
  }

  @Override
  public synchronized void setContentType(String type)
      throws RestServiceException {
    setHeader(CONTENT_TYPE_HEADER_KEY, type, Event.SetContentType);
  }

  @Override
  public synchronized void setContentLength(long length)
      throws RestServiceException {
    setHeader(CONTENT_LENGTH_HEADER_KEY, length, Event.SetContentLength);
  }

  @Override
  public synchronized void setLocation(String location)
      throws RestServiceException {
    setHeader(LOCATION_HEADER_KEY, location, Event.SetLocation);
  }

  @Override
  public synchronized void setLastModified(Date lastModified)
      throws RestServiceException {
    setHeader(LAST_MODIFIED_KEY, lastModified, Event.SetLastModified);
  }

  @Override
  public synchronized void setExpires(Date expireTime)
      throws RestServiceException {
    setHeader(EXPIRES_KEY, expireTime, Event.SetExpires);
  }

  @Override
  public synchronized void setCacheControl(String cacheControl)
      throws RestServiceException {
    setHeader(CACHE_CONTROL_KEY, cacheControl, Event.SetCacheControl);
  }

  @Override
  public synchronized void setPragma(String pragma)
      throws RestServiceException {
    setHeader(PRAGMA_KEY, pragma, Event.SetPragma);
  }

  @Override
  public synchronized void setDate(Date date)
      throws RestServiceException {
    setHeader(DATE_KEY, date, Event.SetDate);
  }

  @Override
  public synchronized void setHeader(String headerName, Object headerValue)
      throws RestServiceException {
    setHeader(headerName, headerValue, Event.SetHeader);
  }

  @Override
  public boolean isOpen() {
    boolean isOpen = channelOpen.get();
    onEventComplete(Event.IsOpen);
    return isOpen;
  }

  @Override
  public void close() {
    channelOpen.set(false);
    onEventComplete(Event.Close);
  }

  /**
   * Sets {@code headerName} to {@code headerValue} and fires the event {@code eventToFire}.
   * @param headerName the header to set to {@code headerValue}.
   * @param headerValue the value to set {@code headerName} to.
   * @param eventToFire the event to fire once header is set successfully.
   * @throws IllegalArgumentException if either of {@code headerName} or {@code headerValue} is null.
   * @throws RestServiceException if there is an error building or setting the header in the response.
   */
  private void setHeader(String headerName, Object headerValue, Event eventToFire)
      throws RestServiceException {
    if (headerName != null && headerValue != null) {
      if (isOpen() && !responseMetadataFinalized.get()) {
        try {
          if (!responseMetadata.has(RESPONSE_HEADERS_KEY)) {
            responseMetadata.put(RESPONSE_HEADERS_KEY, new JSONObject());
          }
          responseMetadata.getJSONObject(RESPONSE_HEADERS_KEY).put(headerName, headerValue);
          onEventComplete(eventToFire);
        } catch (JSONException e) {
          throw new RestServiceException("Unable to set " + headerName + " to " + headerValue,
              RestServiceErrorCode.ResponseBuildingFailure);
        }
      } else {
        throw new RestServiceException("Cannot change response metadata",
            RestServiceErrorCode.IllegalResponseMetadataStateTransition);
      }
    } else {
      throw new IllegalArgumentException("Header name [" + headerName + "] or header value [" + headerValue + "] null");
    }
  }

  // MockRestResponseChannel specific functions (for testing)

  /**
   * Getters for response inside the MockRestResponseChannel can specify whether they want flushed or unflushed data.
   */
  public enum DataStatus {
    Flushed,
    Unflushed
  }

  /**
   * Gets the response metadata based on {@code status}. Unflushed response metadata can change.
   * @param status the {@link DataStatus} of the data being returned.
   * @return the response metadata.
   */
  public synchronized JSONObject getResponseMetadata(DataStatus status) {
    JSONObject metadata = null;
    if (DataStatus.Unflushed.equals(status) || (DataStatus.Flushed.equals(status) && responseMetadataFlushed.get())) {
      metadata = responseMetadata;
    }
    return metadata;
  }

  /**
   * Gets the response body based on {@code dataStatus}. Unflushed response body can change.
   * @param status the {@link DataStatus} of the data being returned.
   * @return the response body.
   */
  public synchronized byte[] getResponseBody(DataStatus status) {
    byte[] content;
    if (DataStatus.Flushed.equals(status)) {
      content = flushedBodyBytes.toByteArray();
    } else {
      int size = flushedBodyBytes.size() + unflushedBodyBytes.size();
      content = new byte[size];
      System.arraycopy(flushedBodyBytes.toByteArray(), 0, content, 0, flushedBodyBytes.size());
      System
          .arraycopy(unflushedBodyBytes.toByteArray(), 0, content, flushedBodyBytes.size(), unflushedBodyBytes.size());
    }
    return content;
  }

  /**
   * Gets the value of the header with {@code headerName} if it exists.
   * @param headerName the name of the header.
   * @param status the status of the response metadata to use.
   * @return the value of the header if it exists, null otherwise.
   */
  public String getHeader(String headerName, DataStatus status) {
    JSONObject metadata = getResponseMetadata(status);
    String headerValue = null;
    try {
      if (metadata != null && metadata.has(RESPONSE_HEADERS_KEY) && metadata.getJSONObject(RESPONSE_HEADERS_KEY)
          .has(headerName)) {
        headerValue = metadata.getJSONObject(RESPONSE_HEADERS_KEY).getString(headerName);
      }
    } catch (JSONException e) {
      // too bad.
    }
    return headerValue;
  }

  /**
   * Gets the response status.
   * @param dataStatus the status of the response metadata to use.
   * @return the response status.
   */
  public ResponseStatus getResponseStatus(DataStatus dataStatus) {
    JSONObject metadata = getResponseMetadata(dataStatus);
    ResponseStatus status = null;
    try {
      if (metadata != null && metadata.has(RESPONSE_STATUS_KEY)) {
        status = ResponseStatus.valueOf(metadata.getString(RESPONSE_STATUS_KEY));
      }
    } catch (Exception e) {
      // too bad.
    }
    return status;
  }

  /**
   * Gets the Throwable that was passed to {@link #onResponseComplete(Throwable)}, if any.
   * @return the {@link Throwable} passed to {@link #onResponseComplete(Throwable)}.
   */
  public Throwable getCause() {
    return cause;
  }

  /**
   * Register to be notified about events that occur in this MockRestResponseChannel.
   * @param listener the listener that needs to be notified of events.
   */
  public MockRestResponseChannel addListener(EventListener listener) {
    if (listener != null) {
      synchronized (listeners) {
        listeners.add(listener);
      }
    }
    return this;
  }

  /**
   * Notify listeners of events.
   * <p/>
   * Please *do not* write tests that check for events *not* arriving. Events will not arrive if there was an exception
   * in the function that triggers the event or inside this function.
   * @param event the {@link Event} that just occurred.
   */
  private void onEventComplete(Event event) {
    synchronized (listeners) {
      for (EventListener listener : listeners) {
        try {
          listener.onEventComplete(this, event);
        } catch (Exception ee) {
          // too bad.
        }
      }
    }
  }
}
