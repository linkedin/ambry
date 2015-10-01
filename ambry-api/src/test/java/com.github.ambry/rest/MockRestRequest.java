package com.github.ambry.rest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Also contains request content in the form of a list of {@link RestRequestContent}.
 */
public class MockRestRequest implements RestRequest {
  // main fields
  public static String REST_METHOD_KEY = "restMethod";
  public static String URI_KEY = "uri";
  public static String HEADERS_KEY = "headers";

  // header fields
  public static String CONTENT_LENGTH = "contentLength";

  private final RestMethod restMethod;
  private final URI uri;
  private final Map<String, List<String>> args = new HashMap<String, List<String>>();
  private final ReentrantLock contentLock = new ReentrantLock();
  private final List<RestRequestContent> requestContents = new LinkedList<RestRequestContent>();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ReentrantLock referenceCountLock = new ReentrantLock();
  private final AtomicInteger referenceCount = new AtomicInteger(0);
  private final AtomicBoolean streamEnded = new AtomicBoolean(false);

  /**
   * Create a MockRestRequest.
   * @param data the request metadata with the fields required.
   * @throws JSONException if there is an exception retrieving required fields.
   * @throws UnsupportedEncodingException if some parts of the URI are not in a format that can be decoded.
   * @throws URISyntaxException if there is a syntax error in the URI.
   */
  public MockRestRequest(JSONObject data)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    this.restMethod = RestMethod.valueOf(data.getString(REST_METHOD_KEY));
    this.uri = new URI(data.getString(URI_KEY));
    JSONObject headers = data.has(HEADERS_KEY) ? data.getJSONObject(HEADERS_KEY) : null;
    populateArgs(headers);
  }

  @Override
  public RestMethod getRestMethod() {
    return restMethod;
  }

  @Override
  public String getPath() {
    return uri.getPath();
  }

  @Override
  public String getUri() {
    return uri.toString();
  }

  @Override
  public Map<String, List<String>> getArgs() {
    return args;
  }

  @Override
  public void addContent(RestRequestContent restRequestContent)
      throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    try {
      contentLock.lock();
      requestContents.add(restRequestContent);
      restRequestContent.retain();
    } finally {
      contentLock.unlock();
    }
  }

  @Override
  public void retain() {
    try {
      referenceCountLock.lock();
      if (isOpen()) {
        referenceCount.incrementAndGet();
      } else {
        // this is specifically in place so that we know when a piece of code retains after close. Real implementations
        // might (or might not) quietly handle this but in tests we want to know when we retain after close.
        throw new IllegalStateException("Trying to retain request metadata after closing channel");
      }
    } finally {
      referenceCountLock.unlock();
    }
  }

  @Override
  public void release() {
    try {
      referenceCountLock.lock();
      if (referenceCount.get() <= 0) {
        // this is specifically in place so that we know when a piece of code releases too much. Real implementations
        // might (or might not) quietly handle this but in tests we want to know when we release too much.
        throw new IllegalStateException("Request metadata has been released more times than it has been retained");
      } else {
        referenceCount.decrementAndGet();
      }
    } finally {
      referenceCountLock.unlock();
    }
  }

  @Override
  public long getSize() {
    return args.get(CONTENT_LENGTH) != null ? Long.parseLong(args.get(CONTENT_LENGTH).get(0)) : 0;
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
        // We read from the RestRequestContent at the head of the list until :-
        // 1. The writable channel can hold no more data or there is no more data immediately available - while loop
        //      ends.
        // 2. The RestRequestContent runs out of content - remove it from the head of the list and start reading from
        //      new head if it is available.
        // Content may be added at any time and it is not necessary that the list have any elements at the time of
        // reading. Read returns -1 only when the a RestRequestContent with isLast() true is read. If stream has not
        // ended and there is no content in the list, we return 0.
        // Cases to consider:
        // 1. Writable channel can consume no content (nothing to do. Should return 0).
        // 2. Writable channel can consume data limited to one RestRequestContent (might rollover).
        //      a. There is content available in the RestRequestContent at the head of the list (don't rollover).
        //      b. Request content stream ended when we tried to read from the RestRequestContent at the head of the
        //          list (end of stream).
        //      b. There is no content available right now in the RestRequestContent at the head of the list but it has
        //          not finished its content (don't rollover).
        //      c. There is no content available in the RestRequestContent at the head of the list because it
        //          just finished its content (rollover).
        //            i. More RestRequestContent available in the list (continue read).
        //            ii. No more RestRequestContent in the list currently (cannot continue read).
        // 3. Writable channel can consume data across RestRequestContents (will rollover).
        //      a. More RestRequestContent is available in the list (continue read).
        //      b. Request content stream has not ended but more RestRequestContent is not available in the list (cannot
        //          continue read).
        //      c. Request content stream has ended (end of stream).
        int currentBytesWritten = requestContents.size() > 0 ? requestContents.get(0).read(channel) : 0;
        while (currentBytesWritten != 0) {
          if (currentBytesWritten == -1) {
            RestRequestContent restRequestContent = requestContents.remove(0);
            restRequestContent.close();
            streamEnded.set(restRequestContent.isLast());
          } else {
            bytesWritten += currentBytesWritten;
          }

          currentBytesWritten = 0;
          if (!streamEnded.get() && requestContents.size() > 0) {
            currentBytesWritten = requestContents.get(0).read(channel);
          }
        }
      } finally {
        contentLock.unlock();
      }
    }
    return bytesWritten;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    if (channelOpen.compareAndSet(true, false)) {
      try {
        contentLock.lock();
        Iterator<RestRequestContent> requestContentIterator = requestContents.iterator();
        while (requestContentIterator.hasNext()) {
          requestContentIterator.next().close();
          requestContentIterator.remove();
        }
        while (referenceCount.get() > 0) {
          release();
        }
      } finally {
        contentLock.unlock();
      }
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
}
