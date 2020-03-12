/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.rest;

import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of {@link RestResponseChannel} that can be used by tests.
 * <p/>
 * The responseMetadata and response body are both stored in-memory. The responseMetadata and responseBody can be
 * obtained through APIs to check correctness.
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
 * All functions are synchronized because this is expected to be thread safe (very coarse grained but this is not
 * expected to be performant, just usable).
 */
public class MockRestResponseChannel implements RestResponseChannel {
  /**
   * List of "events" (function calls) that can occur inside MockRestResponseChannel.
   */
  public enum Event {
    Write, OnRequestComplete, SetStatus, SetHeader, IsOpen, Close
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

  private final RestRequest restRequest;
  private AtomicBoolean channelOpen = new AtomicBoolean(true);
  private AtomicBoolean requestComplete = new AtomicBoolean(false);
  private AtomicBoolean responseMetadataFinalized = new AtomicBoolean(false);
  private final JSONObject responseMetadata = new JSONObject();
  private final ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
  private final List<EventListener> listeners = new ArrayList<EventListener>();

  private volatile Exception exception = null;

  public MockRestResponseChannel() throws JSONException {
    this(null);
  }

  public MockRestResponseChannel(RestRequest restRequest) throws JSONException {
    responseMetadata.put(RESPONSE_STATUS_KEY, ResponseStatus.Ok.name());
    this.restRequest = restRequest;
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if (src == null) {
      throw new IllegalArgumentException("Source buffer cannot be null");
    }
    FutureResult<Long> futureResult = new FutureResult<Long>();
    long bytesWritten = 0;
    Exception exception = null;
    if (!isOpen()) {
      exception = new ClosedChannelException();
    } else {
      responseMetadataFinalized.set(true);
      bytesWritten = src.remaining();
      for (int i = 0; i < bytesWritten; i++) {
        bodyBytes.write(src.get());
      }
    }
    futureResult.done(bytesWritten, exception);
    if (callback != null) {
      callback.onCompletion(bytesWritten, exception);
    }
    onEventComplete(Event.Write);
    return futureResult;
  }

  @Override
  public synchronized void onResponseComplete(Exception exception) {
    if (requestComplete.compareAndSet(false, true)) {
      this.exception = exception;
      try {
        if (!responseMetadataFinalized.get() && exception != null) {
          // clear headers except for the value of Allow
          String allow = getHeader(RestUtils.Headers.ALLOW);
          responseMetadata.put(RESPONSE_HEADERS_KEY, new JSONObject());
          if (!Utils.isNullOrEmpty(allow)) {
            setHeader(RestUtils.Headers.ALLOW, allow);
          }
          setHeader(RestUtils.Headers.CONTENT_TYPE, "text/plain; charset=UTF-8");
          ResponseStatus status = ResponseStatus.InternalServerError;
          if (exception instanceof RestServiceException) {
            status = ResponseStatus.getResponseStatus(((RestServiceException) exception).getErrorCode());
          }
          responseMetadata.put(RESPONSE_STATUS_KEY, status.name());
          bodyBytes.write(exception.toString().getBytes());
          responseMetadataFinalized.set(true);
        }
        close();
        if (restRequest != null) {
          restRequest.getMetricsTracker().nioMetricsTracker.markRequestCompleted();
          restRequest.close();
        }
        onEventComplete(Event.OnRequestComplete);
      } catch (Exception e) {
        // nothing to do
      }
    }
  }

  @Override
  public synchronized void setStatus(ResponseStatus status) throws RestServiceException {
    if (isOpen() && !responseMetadataFinalized.get()) {
      try {
        responseMetadata.put(RESPONSE_STATUS_KEY, status.name());
        onEventComplete(Event.SetStatus);
      } catch (JSONException e) {
        throw new RestServiceException("Unable to set Status", RestServiceErrorCode.InternalServerError);
      }
    } else {
      throw new IllegalStateException("Cannot change response metadata after it has been finalized");
    }
  }

  @Override
  public ResponseStatus getStatus() {
    ResponseStatus status = null;
    try {
      if (responseMetadata.has(RESPONSE_STATUS_KEY)) {
        status = ResponseStatus.valueOf(responseMetadata.getString(RESPONSE_STATUS_KEY));
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return status;
  }

  @Override
  public synchronized void setHeader(String headerName, Object headerValue) {
    setHeader(headerName, headerValue, Event.SetHeader);
  }

  @Override
  public String getHeader(String headerName) {
    String headerValue = null;
    try {
      if (responseMetadata.has(RESPONSE_HEADERS_KEY) && responseMetadata.getJSONObject(RESPONSE_HEADERS_KEY)
          .has(headerName)) {
        headerValue = responseMetadata.getJSONObject(RESPONSE_HEADERS_KEY).get(headerName).toString();
      }
    } catch (JSONException e) {
      throw new IllegalStateException(e);
    }
    return headerValue;
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
   * @throws IllegalStateException if the response metadata has already been finalized.
   * @throws JSONException if RESPONSE_HEADERS_KEY not in responaeMetadata or its value is not a {@link JSONObject}
   */
  private void setHeader(String headerName, Object headerValue, Event eventToFire) {
    if (headerName != null && headerValue != null) {
      if (isOpen() && !responseMetadataFinalized.get()) {
        if (!responseMetadata.has(RESPONSE_HEADERS_KEY)) {
          responseMetadata.put(RESPONSE_HEADERS_KEY, new JSONObject());
        }
        if (headerValue instanceof Date) {
          SimpleDateFormat dateFormatter = new SimpleDateFormat(RestUtils.HTTP_DATE_FORMAT, Locale.US);
          dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
          headerValue = dateFormatter.format((Date) headerValue);
        }
        responseMetadata.getJSONObject(RESPONSE_HEADERS_KEY).put(headerName, headerValue);
        onEventComplete(eventToFire);
      } else {
        throw new IllegalStateException("Cannot change response metadata after it has been finalized");
      }
    } else {
      throw new IllegalArgumentException("Header name [" + headerName + "] or header value [" + headerValue + "] null");
    }
  }

  // MockRestResponseChannel specific functions (for testing)

  /**
   * Gets the response body. If the channel isn't closed, response body can change.
   * @return the response body.
   */
  public synchronized byte[] getResponseBody() {
    return bodyBytes.toByteArray();
  }

  /**
   * Gets the Throwable that was passed to {@link #onResponseComplete(Exception)}, if any.
   * @return the {@link Throwable} passed to {@link #onResponseComplete(Exception)}.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * @return the response headers associated with this response channel
   */
  public Map<String, Object> getResponseHeaders() {
    return responseMetadata.has(RESPONSE_HEADERS_KEY) ? responseMetadata.getJSONObject(RESPONSE_HEADERS_KEY).toMap()
        : Collections.emptyMap();
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
