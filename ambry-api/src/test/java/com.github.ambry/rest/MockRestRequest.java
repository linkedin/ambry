package com.github.ambry.rest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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
  public static enum Event {
    GetRestMethod,
    GetPath,
    GetUri,
    GetArgs,
    GetSize,
    Read,
    IsOpen,
    Close
  }

  /**
   * Callback that can be used to listen to events that happen inside MockRestRequest.
   * <p/>
   * Please *do not* write tests that check for events *not* arriving. Events will not arrive if there was an exception
   * in the function that triggers the event or inside the function that notifies listeners.
   */
  public interface EventListener {

    /**
     * Called when an event (function call) finishes successfully in {@link MockRestRequest}. Does *not* trigger
     * if the event (function) fails.
     * @param mockRestRequest the {@link MockRestRequest} where the event occurred.
     * @param event the {@link MockRestRequest.Event} that occurred.
     */
    public void onEventComplete(MockRestRequest mockRestRequest, MockRestRequest.Event event);
  }

  // main fields
  public static String REST_METHOD_KEY = "restMethod";
  public static String URI_KEY = "uri";
  public static String HEADERS_KEY = "headers";

  // header fields
  public static String CONTENT_LENGTH_HEADER_KEY = "Content-Length";

  private final RestMethod restMethod;
  private final URI uri;
  private final Map<String, List<String>> args = new HashMap<String, List<String>>();
  private final ReentrantLock contentLock = new ReentrantLock();
  private final List<ByteBuffer> requestContents;
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean streamEnded = new AtomicBoolean(false);
  private final List<EventListener> listeners = new ArrayList<EventListener>();

  /**
   * Create a MockRestRequest.
   * @param data the request metadata with the fields required.
   * @param requestContents contents of the request, if any. Can be null and can be added later via
   *                          {@link #addContent(ByteBuffer)}. Add a null {@link ByteBuffer} at the end to signify end
   *                          of content.
   * @throws JSONException if there is an exception retrieving required fields.
   * @throws UnsupportedEncodingException if some parts of the URI are not in a format that can be decoded.
   * @throws URISyntaxException if there is a syntax error in the URI.
   */
  public MockRestRequest(JSONObject data, List<ByteBuffer> requestContents)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    this.restMethod = RestMethod.valueOf(data.getString(REST_METHOD_KEY));
    this.uri = new URI(data.getString(URI_KEY));
    JSONObject headers = data.has(HEADERS_KEY) ? data.getJSONObject(HEADERS_KEY) : null;
    populateArgs(headers);
    if (requestContents != null) {
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
  public Map<String, List<String>> getArgs() {
    onEventComplete(Event.GetArgs);
    return args;
  }

  /**
   * Returns length in the "content-length" header. If there is no such header, returns 0.
   * <p/>
   * This function does not individually count the bytes in the content (it is not possible) so the bytes received may
   * actually be different if the stream is buggy or the client made a mistake. Do *not* treat this as fully accurate.
   * @return the size of content as defined in the "content-length" header. Might not be actual length of content if
   *          the stream is buggy.
   */
  @Override
  public long getSize() {
    long contentLength;
    if (args.get(RestConstants.Headers.Blob_Size) != null) {
      contentLength = Long.parseLong(args.get(RestConstants.Headers.Blob_Size).get(0));
    } else {
      contentLength =
          args.get(CONTENT_LENGTH_HEADER_KEY) != null ? Long.parseLong(args.get(CONTENT_LENGTH_HEADER_KEY).get(0)) : 0;
    }
    onEventComplete(Event.GetSize);
    return contentLength;
  }

  @Override
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten = streamEnded.get() ? -1 : 0;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else if (!streamEnded.get()) {
      try {
        contentLock.lock();
        // We read from the ByteBuffer at the head of the list until :-
        // 1. The writable channel can hold no more data or there is no more data immediately available - while loop
        //      ends.
        // 2. The ByteBuffer runs out of content - remove it from the head of the list and start reading from new head
        //      if it is available.
        // Content may be added at any time and it is not necessary that the list have any elements at the time of
        // reading. Read returns -1 only when a null is encountered in the content list. If stream has not ended and
        // there is no content in the list, we return 0.
        // Cases to consider:
        // 1. Writable channel can consume no content (nothing to do. Should return 0).
        // 2. Writable channel can consume data limited to one ByteBuffer (might rollover).
        //      a. There is content available in the ByteBuffer at the head of the list (don't rollover).
        //      b. Request content stream ended when we tried to read from the ByteBuffer at the head of the list (end
        //          of stream).
        //      b. There is no content available right now in the ByteBuffer at the head of the list but it has not
        //          finished its content (don't rollover).
        //      c. There is no content available in the ByteBuffer at the head of the list because it just finished its
        //          content (rollover).
        //            i. More ByteBuffer available in the list (continue read).
        //            ii. No more ByteBuffer in the list currently (cannot continue read).
        // 3. Writable channel can consume data across ByteBuffers (will rollover).
        //      a. More ByteBuffer is available in the list (continue read).
        //      b. Request content stream has not ended but more ByteBuffer is not available in the list (cannot
        //          continue read).
        //      c. Request content stream has ended (end of stream).
        int currentBytesWritten = requestContents.size() > 0 ? channel.write(requestContents.get(0)) : 0;
        while (currentBytesWritten != 0) {
          if (currentBytesWritten == -1) {
            requestContents.remove(0);
            if (requestContents.get(0) == null) {
              streamEnded.set(true);
              requestContents.remove(0);
            }
          } else {
            bytesWritten += currentBytesWritten;
          }

          currentBytesWritten = 0;
          if (!streamEnded.get() && requestContents.size() > 0) {
            currentBytesWritten = channel.write(requestContents.get(0));
          }
        }
      } finally {
        contentLock.unlock();
      }
    }
    onEventComplete(Event.Read);
    return bytesWritten;
  }

  @Override
  public boolean isOpen() {
    onEventComplete(Event.IsOpen);
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    channelOpen.set(false);
    onEventComplete(Event.Close);
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
  public void addContent(ByteBuffer content)
      throws IOException {
    try {
      contentLock.lock();
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
      requestContents.add(content);
    } finally {
      contentLock.unlock();
    }
  }

  /**
   * Adds all headers and parameters in the URL as arguments.
   * @param headers headers sent with the request.
   * @throws UnsupportedEncodingException if an argument key or value cannot be URL decoded.
   */
  private void populateArgs(JSONObject headers)
      throws JSONException, UnsupportedEncodingException {
    if (headers != null) {
      // add headers.
      Iterator<String> headerKeys = headers.keys();
      while (headerKeys.hasNext()) {
        String headerKey = headerKeys.next();
        String headerValue = headers.getString(headerKey);
        addOrUpdateArg(headerKey, headerValue);
      }
    }

    // decode parameters in the URI. Handle parameters without values and multiple values for the same parameter.
    if (uri.getQuery() != null) {
      for (String parameterValue : uri.getQuery().split("&")) {
        int idx = parameterValue.indexOf("=");
        String key = idx > 0 ? parameterValue.substring(0, idx) : parameterValue;
        String value = idx > 0 ? parameterValue.substring(idx + 1) : null;
        addOrUpdateArg(key, value);
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
  private void addOrUpdateArg(String key, String value)
      throws UnsupportedEncodingException {
    key = URLDecoder.decode(key, "UTF-8");
    value = URLDecoder.decode(value, "UTF-8");
    if (!args.containsKey(key)) {
      args.put(key, new LinkedList<String>());
    }
    args.get(key).add(value);
  }

  /**
   * Notify listeners of events.
   * <p/>
   * Please *do not* write tests that check for events *not* arriving. Events will not arrive if there was an exception
   * in the function that triggers the event or inside this function.
   * @param event the {@link MockRestRequest.Event} that just occurred.
   */
  private void onEventComplete(MockRestRequest.Event event) {
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
