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

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLSession;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Implementation of {@link RestRequest} that can be used in tests.
 * <p/>
 * The underlying request metadata is in the form of a {@link JSONObject} that contains the following fields: -
 * 1. "restMethod" - {@link RestMethod} - the rest method required.
 * 2. "uri" - String - the uri.
 * 3. "headers" - {@link JSONObject} - all the headers as key value pairs.
 * <p/>
 * Headers:
 * 1. "contentLength" - the length of content accompanying this request. Defaults to 0.
 * <p/>
 * This also contains the content of the request. This content can be streamed out through the read operations.
 */
public class MockRestRequest implements RestRequest {
  /**
   * List of "events" (function calls) that can occur inside MockRestRequest.
   */
  public enum Event {
    GetRestMethod, GetPath, GetUri, GetArgs, SetArgs, GetSize, ReadInto, IsOpen, Close, GetMetricsTracker
  }

  /**
   * Callback that can be used to listen to events that happen inside MockRestRequest.
   * <p/>
   * Please *do not* write tests that check for events *not* arriving. Events will not arrive if there was an exception
   * in the function that triggers the event or inside the function that notifies listeners.
   */
  public interface EventListener {

    /**
     * Called when an event (function call) finishes successfully in MockRestRequest. Does *not* trigger if the event
     * (function) fails.
     * @param mockRestRequest the {@link MockRestRequest} where the event occurred.
     * @param event the {@link Event} that occurred.
     */
    public void onEventComplete(MockRestRequest mockRestRequest, Event event);
  }

  public static final JSONObject DUMMY_DATA = new JSONObject();

  // main fields
  public static String REST_METHOD_KEY = "restMethod";
  public static String URI_KEY = "uri";
  public static String HEADERS_KEY = "headers";

  // header fields
  public static String CONTENT_LENGTH_HEADER_KEY = "Content-Length";

  private final RestMethod restMethod;
  private final URI uri;
  private final Map<String, Object> args = new HashMap<String, Object>();
  private final ReentrantLock contentLock = new ReentrantLock();
  private final List<ByteBuffer> requestContents;
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final List<EventListener> listeners = new ArrayList<EventListener>();
  private final RestRequestMetricsTracker restRequestMetricsTracker = new RestRequestMetricsTracker();
  private final AtomicLong bytesReceived = new AtomicLong(0);

  private MessageDigest digest = null;
  private byte[] digestBytes = null;

  private volatile AsyncWritableChannel writeChannel = null;
  private volatile ReadIntoCallbackWrapper callbackWrapper = null;
  private volatile boolean allContentReceived = false;

  private static String MULTIPLE_HEADER_VALUE_DELIMITER = ", ";

  static {
    try {
      DUMMY_DATA.put(REST_METHOD_KEY, RestMethod.GET.name());
      DUMMY_DATA.put(URI_KEY, "/");
    } catch (JSONException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Create a MockRestRequest.
   * @param data the request metadata with the fields required.
   * @param requestContents contents of the request, if any. Can be null and can be added later via
   *                          {@link #addContent(ByteBuffer)}. Add a null {@link ByteBuffer} at the end to signify end
   *                          of content.
   * @throws IllegalArgumentException if the {@link RestMethod} required is not recognized.
   * @throws JSONException if there is an exception retrieving required fields.
   * @throws UnsupportedEncodingException if some parts of the URI are not in a format that can be decoded.
   * @throws URISyntaxException if there is a syntax error in the URI.
   */
  public MockRestRequest(JSONObject data, List<ByteBuffer> requestContents)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    restRequestMetricsTracker.nioMetricsTracker.markRequestReceived();
    this.restMethod = RestMethod.valueOf(data.getString(REST_METHOD_KEY));
    this.uri = new URI(data.getString(URI_KEY));
    JSONObject headers = data.has(HEADERS_KEY) ? data.getJSONObject(HEADERS_KEY) : null;
    populateArgs(headers);
    if (requestContents != null) {
      requestContents.forEach(buf -> bytesReceived.getAndAdd(buf != null ? buf.remaining() : 0));
      this.requestContents = requestContents;
    } else {
      this.requestContents = new LinkedList<ByteBuffer>();
    }
  }

  @Override
  public RestMethod getRestMethod() {
    onEventComplete(Event.GetRestMethod);
    return restMethod;
  }

  @Override
  public String getPath() {
    onEventComplete(Event.GetPath);
    return uri.getPath();
  }

  @Override
  public String getUri() {
    onEventComplete(Event.GetUri);
    return uri.toString();
  }

  @Override
  public Map<String, Object> getArgs() {
    onEventComplete(Event.GetArgs);
    return args;
  }

  @Override
  public Object setArg(String key, Object value) {
    onEventComplete(Event.SetArgs);
    return args.put(key, value);
  }

  @Override
  public SSLSession getSSLSession() {
    return null;
  }

  @Override
  public void prepare() {
    // no op.
  }

  /**
   * Returns the value of the ambry specific content length header ({@link RestUtils.Headers#BLOB_SIZE}. If there is
   * no such header, returns length in the "Content-Length" header. If there is no such header, returns 0.
   * <p/>
   * This function does not individually count the bytes in the content (it is not possible) so the bytes received may
   * actually be different if the stream is buggy or the client made a mistake. Do *not* treat this as fully accurate.
   * @return the size of content as defined in headers. Might not be actual length of content if the stream is buggy.
   */
  @Override
  public long getSize() {
    long contentLength;
    if (args.get(RestUtils.Headers.BLOB_SIZE) != null) {
      contentLength = Long.parseLong(args.get(RestUtils.Headers.BLOB_SIZE).toString());
    } else {
      contentLength =
          args.get(CONTENT_LENGTH_HEADER_KEY) != null ? Long.parseLong(args.get(CONTENT_LENGTH_HEADER_KEY).toString())
              : -1;
    }
    onEventComplete(Event.GetSize);
    return contentLength;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    ReadIntoCallbackWrapper tempWrapper = new ReadIntoCallbackWrapper(callback);
    contentLock.lock();
    try {
      if (!channelOpen.get()) {
        tempWrapper.invokeCallback(new ClosedChannelException());
      } else if (writeChannel != null) {
        throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
      }
      Iterator<ByteBuffer> bufferIterator = requestContents.iterator();
      while (bufferIterator.hasNext()) {
        ByteBuffer buffer = bufferIterator.next();
        writeContent(asyncWritableChannel, tempWrapper, buffer);
        bufferIterator.remove();
      }
      callbackWrapper = tempWrapper;
      writeChannel = asyncWritableChannel;
    } finally {
      contentLock.unlock();
    }
    onEventComplete(Event.ReadInto);
    return tempWrapper.futureResult;
  }

  @Override
  public void setDigestAlgorithm(String digestAlgorithm) throws NoSuchAlgorithmException {
    if (callbackWrapper != null) {
      throw new IllegalStateException("Cannot create a digest because some content has already been discarded");
    }
    digest = MessageDigest.getInstance(digestAlgorithm);
  }

  @Override
  public byte[] getDigest() {
    if (digest == null) {
      return null;
    } else if (!allContentReceived) {
      throw new IllegalStateException("Cannot calculate digest yet because all the content has not been processed");
    }
    if (digestBytes == null) {
      digestBytes = digest.digest();
    }
    return digestBytes;
  }

  @Override
  public long getBytesReceived() {
    return bytesReceived.get();
  }

  @Override
  public long getBlobBytesReceived() {
    return bytesReceived.get();
  }

  @Override
  public boolean isOpen() {
    onEventComplete(Event.IsOpen);
    return channelOpen.get();
  }

  @Override
  public void close() throws IOException {
    channelOpen.set(false);
    onEventComplete(Event.Close);
  }

  @Override
  public RestRequestMetricsTracker getMetricsTracker() {
    onEventComplete(Event.GetMetricsTracker);
    return restRequestMetricsTracker;
  }

  /**
   * Register to be notified about events that occur in this MockRestRequest.
   * @param listener the listener that needs to be notified of events.
   */
  public MockRestRequest addListener(EventListener listener) {
    if (listener != null) {
      synchronized (listeners) {
        listeners.add(listener);
      }
    }
    return this;
  }

  /**
   * Adds some content in the form of {@link ByteBuffer} to this RestRequest. This content will be available to read
   * through the read operations. To indicate end of content, add a null ByteBuffer.
   * @throws ClosedChannelException if request channel has been closed.
   */
  public void addContent(ByteBuffer content) throws IOException {
    if (!RestMethod.POST.equals(getRestMethod()) && content != null) {
      throw new IllegalStateException("There is no content expected for " + getRestMethod());
    } else if (!isOpen()) {
      throw new ClosedChannelException();
    } else {
      contentLock.lock();
      try {
        if (!isOpen()) {
          throw new ClosedChannelException();
        }
        bytesReceived.getAndAdd(content != null ? content.remaining() : 0);
        if (writeChannel != null) {
          writeContent(writeChannel, callbackWrapper, content);
        } else {
          requestContents.add(content);
        }
      } finally {
        contentLock.unlock();
      }
    }
  }

  /**
   * Writes the provided {@code content} to the given {@code writeChannel}.
   * @param writeChannel the {@link AsyncWritableChannel} to write the {@code content} to.
   * @param callbackWrapper the {@link ReadIntoCallbackWrapper} for the read operation.
   * @param content the piece of {@link ByteBuffer} that needs to be written to the {@code writeChannel}.
   */
  private void writeContent(AsyncWritableChannel writeChannel, ReadIntoCallbackWrapper callbackWrapper,
      ByteBuffer content) {
    ContentWriteCallback writeCallback;
    if (content == null) {
      allContentReceived = true;
      writeCallback = new ContentWriteCallback(true, callbackWrapper);
      content = ByteBuffer.allocate(0);
    } else {
      writeCallback = new ContentWriteCallback(false, callbackWrapper);
      if (digest != null) {
        int savedPosition = content.position();
        digest.update(content);
        content.position(savedPosition);
      }
    }
    writeChannel.write(content, writeCallback);
  }

  /**
   * Adds all headers and parameters in the URL as arguments.
   * @param headers headers sent with the request.
   * @throws UnsupportedEncodingException if an argument key or value cannot be URL decoded.
   */
  private void populateArgs(JSONObject headers) throws JSONException, UnsupportedEncodingException {
    if (headers != null) {
      // add headers. Handles headers with multiple values.
      Iterator<String> headerKeys = headers.keys();
      while (headerKeys.hasNext()) {
        String headerKey = headerKeys.next();
        Object headerValue = JSONObject.NULL.equals(headers.get(headerKey)) ? null : headers.get(headerKey);
        addOrUpdateArg(headerKey, headerValue);
      }
    }

    // decode parameters in the URI. Handles parameters without values and multiple values for the same parameter.
    if (uri.getQuery() != null) {
      for (String parameterValue : uri.getQuery().split("&")) {
        int idx = parameterValue.indexOf("=");
        String key = idx > 0 ? parameterValue.substring(0, idx) : parameterValue;
        String value = idx > 0 ? parameterValue.substring(idx + 1) : null;
        addOrUpdateArg(key, value);
      }
    }

    // convert all StringBuilders to String
    for (Map.Entry<String, Object> e : args.entrySet()) {
      Object value = e.getValue();
      if (value != null && value instanceof StringBuilder) {
        args.put(e.getKey(), (e.getValue()).toString());
      }
    }
  }

  /**
   * Adds a {@code key}, {@code value} pair to args after URL decoding them. If {@code key} already exists,
   * {@code value} is added to a list of values.
   * @param key the key of the argument.
   * @param value the value of the argument.
   * @throws UnsupportedEncodingException if {@code key} or {@code value} cannot be URL decoded.
   */
  private void addOrUpdateArg(String key, Object value) throws UnsupportedEncodingException {
    key = URLDecoder.decode(key, "UTF-8");
    if (value != null && value instanceof String) {
      String valueStr = URLDecoder.decode((String) value, "UTF-8");
      StringBuilder sb;
      if (args.get(key) == null) {
        sb = new StringBuilder(valueStr);
        args.put(key, sb);
      } else {
        sb = (StringBuilder) args.get(key);
        sb.append(MULTIPLE_HEADER_VALUE_DELIMITER).append(value);
      }
    } else if (value != null && args.containsKey(key)) {
      throw new IllegalStateException("Value of key [" + key + "] is not a string and it already exists in the args");
    } else {
      args.put(key, value);
    }
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

  /**
   * Callback for each write into the given {@link AsyncWritableChannel}.
   */
  private class ContentWriteCallback implements Callback<Long> {
    private final boolean isLast;
    private final ReadIntoCallbackWrapper callbackWrapper;

    /**
     * Creates a new instance of ContentWriteCallback.
     * @param isLast if this is the last piece of content for this request.
     * @param callbackWrapper the {@link ReadIntoCallbackWrapper} that will receive updates of bytes read and one that
     *                        should be invoked in {@link #onCompletion(Long, Exception)} if {@code isLast} is
     *                        {@code true} or exception passed is not null.
     */
    public ContentWriteCallback(boolean isLast, ReadIntoCallbackWrapper callbackWrapper) {
      this.isLast = isLast;
      this.callbackWrapper = callbackWrapper;
    }

    /**
     * Updates the number of bytes read and invokes {@link ReadIntoCallbackWrapper#invokeCallback(Exception)} if
     * {@code exception} is not {@code null} or if this is the last piece of content in the request.
     * @param result The result of the request. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(Long result, Exception exception) {
      callbackWrapper.updateBytesRead(result);
      if (exception != null || isLast) {
        callbackWrapper.invokeCallback(exception);
      }
    }
  }

  /**
   * Wrapper for callbacks provided to {@link MockRestRequest#readInto(AsyncWritableChannel, Callback)}.
   */
  private class ReadIntoCallbackWrapper {
    /**
     * The {@link Future} where the result of {@link MockRestRequest#readInto(AsyncWritableChannel, Callback)} will
     * eventually be updated.
     */
    public final FutureResult<Long> futureResult = new FutureResult<Long>();

    private final Callback<Long> callback;
    private final AtomicLong totalBytesRead = new AtomicLong(0);
    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    /**
     * Creates an instance of ReadIntoCallbackWrapper with the given {@code callback}.
     * @param callback the {@link Callback} to invoke on operation completion.
     */
    public ReadIntoCallbackWrapper(Callback<Long> callback) {
      this.callback = callback;
    }

    /**
     * Updates the number of bytes that have been successfully read into the given {@link AsyncWritableChannel}.
     * @param delta the number of bytes read in the current invocation.
     * @return the total number of bytes read until now.
     */
    public long updateBytesRead(long delta) {
      return totalBytesRead.addAndGet(delta);
    }

    /**
     * Invokes the callback and updates the future once this is called. This function ensures that the callback is invoked
     * just once.
     * @param exception the {@link Exception}, if any, to pass to the callback.
     */
    public void invokeCallback(Exception exception) {
      if (callbackInvoked.compareAndSet(false, true)) {
        futureResult.done(totalBytesRead.get(), exception);
        if (callback != null) {
          callback.onCompletion(totalBytesRead.get(), exception);
        }
      }
    }
  }
}
