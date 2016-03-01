package com.github.ambry.rest;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link RestRequest}.
 * <p/>
 * A wrapper over {@link HttpRequest} and all the {@link HttpContent} associated with the request.
 */
class NettyRequest implements RestRequest {
  private final NettyMetrics nettyMetrics;
  private final QueryStringDecoder query;
  private final HttpRequest request;
  private final RestMethod restMethod;
  private final Map<String, Object> args;

  private final ReentrantLock contentLock = new ReentrantLock();
  private final Queue<HttpContent> requestContents = new LinkedBlockingQueue<HttpContent>();
  private final RestRequestMetricsTracker restRequestMetricsTracker = new RestRequestMetricsTracker();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private volatile AsyncWritableChannel writeChannel = null;
  private volatile ReadIntoCallbackWrapper callbackWrapper = null;

  protected static String MULTIPLE_HEADER_VALUE_DELIMITER = ", ";

  /**
   * Wraps the {@code request} in an implementation of {@link RestRequest} so that other layers can understand the
   * request.
   * @param request the {@link HttpRequest} that needs to be wrapped.
   * @param nettyMetrics the {@link NettyMetrics} instance to use.
   * @throws IllegalArgumentException if {@code request} is null.
   * @throws RestServiceException if the HTTP method defined in {@code request} is not recognized as a
   *                                {@link RestMethod}.
   */
  public NettyRequest(HttpRequest request, NettyMetrics nettyMetrics)
      throws RestServiceException {
    if (request == null) {
      throw new IllegalArgumentException("Received null HttpRequest");
    }
    restRequestMetricsTracker.nioMetricsTracker.markRequestReceived();
    HttpMethod httpMethod = request.getMethod();
    if (httpMethod == HttpMethod.GET) {
      restMethod = RestMethod.GET;
    } else if (httpMethod == HttpMethod.POST) {
      restMethod = RestMethod.POST;
    } else if (httpMethod == HttpMethod.DELETE) {
      restMethod = RestMethod.DELETE;
    } else if (httpMethod == HttpMethod.HEAD) {
      restMethod = RestMethod.HEAD;
    } else {
      nettyMetrics.unsupportedHttpMethodError.inc();
      throw new RestServiceException("http method not supported: " + httpMethod,
          RestServiceErrorCode.UnsupportedHttpMethod);
    }
    this.request = request;
    this.query = new QueryStringDecoder(request.getUri());
    this.nettyMetrics = nettyMetrics;

    Map<String, Object> allArgs = new HashMap<String, Object>();
    // query params.
    for (Map.Entry<String, List<String>> e : query.parameters().entrySet()) {
      StringBuilder value = null;
      if (e.getValue() != null) {
        StringBuilder combinedValues = combineVals(new StringBuilder(), e.getValue());
        if (combinedValues.length() > 0) {
          value = combinedValues;
        }
      }
      allArgs.put(e.getKey(), value);
    }
    // headers.
    for (Map.Entry<String, String> e : request.headers()) {
      StringBuilder sb;
      boolean valueNull = request.headers().get(e.getKey()) == null;
      if (!valueNull && allArgs.get(e.getKey()) == null) {
        sb = new StringBuilder(e.getValue());
        allArgs.put(e.getKey(), sb);
      } else if (!valueNull) {
        sb = (StringBuilder) allArgs.get(e.getKey());
        sb.append(MULTIPLE_HEADER_VALUE_DELIMITER).append(e.getValue());
      } else if (!allArgs.containsKey(e.getKey())) {
        allArgs.put(e.getKey(), null);
      }
    }
    // turn all StringBuilders into String
    for (Map.Entry<String, Object> e : allArgs.entrySet()) {
      if (allArgs.get(e.getKey()) != null) {
        allArgs.put(e.getKey(), (e.getValue()).toString());
      }
    }
    args = Collections.unmodifiableMap(allArgs);
  }

  private StringBuilder combineVals(StringBuilder currValue, List<String> values) {
    for (String value : values) {
      if (currValue.length() > 0) {
        currValue.append(MULTIPLE_HEADER_VALUE_DELIMITER);
      }
      currValue.append(value);
    }
    return currValue;
  }

  @Override
  public String getUri() {
    return request.getUri();
  }

  @Override
  public String getPath() {
    return query.path();
  }

  @Override
  public RestMethod getRestMethod() {
    return restMethod;
  }

  @Override
  public Map<String, Object> getArgs() {
    return args;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    if (channelOpen.compareAndSet(true, false)) {
      contentLock.lock();
      try {
        logger.trace("Closing NettyRequest {} with {} content chunks unread", getUri(), requestContents.size());
        // For non-POST we usually have one content chunk unread - this the LastHttpContent chunk. This is OK.
        while (requestContents.peek() != null) {
          ReferenceCountUtil.release(requestContents.poll());
        }
      } finally {
        contentLock.unlock();
        restRequestMetricsTracker.recordMetrics();
        if (callbackWrapper != null) {
          callbackWrapper.invokeCallback(new ClosedChannelException());
        }
      }
    }
  }

  @Override
  public RestRequestMetricsTracker getMetricsTracker() {
    return restRequestMetricsTracker;
  }

  /**
   * Only prints the request metadata (URI, path, HTTP method etc) as a string. Does not print the content.
   * @return the request metadata (URI, path, HTTP method etc) as a String. Content is *not* included.
   */
  @Override
  public String toString() {
    return request.toString();
  }

  /**
   * Returns the value of the ambry specific content length header ({@link RestUtils.Headers#BLOB_SIZE}. If there is
   * no such header, returns length in the "Content-Length" header. If there is no such header, tries to infer content
   * size. If that cannot be done, returns 0.
   * <p/>
   * This function does not individually count the bytes in the content (it is not possible) so the bytes received may
   * actually be different if the stream is buggy or the client made a mistake. Do *not* treat this as fully accurate.
   * @return the size of content as defined in headers. Might not be actual length of content if the stream is buggy.
   */
  @Override
  public long getSize() {
    long contentLength;
    if (HttpHeaders.getHeader(request, RestUtils.Headers.BLOB_SIZE, null) != null) {
      contentLength = Long.parseLong(HttpHeaders.getHeader(request, RestUtils.Headers.BLOB_SIZE));
    } else {
      contentLength = HttpHeaders.getContentLength(request, 0);
    }
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
      while (requestContents.peek() != null) {
        writeContent(asyncWritableChannel, tempWrapper, requestContents.peek());
        ReferenceCountUtil.release(requestContents.poll());
      }
      callbackWrapper = tempWrapper;
      writeChannel = asyncWritableChannel;
    } finally {
      contentLock.unlock();
    }
    return tempWrapper.futureResult;
  }

  /**
   * Adds some content in the form of {@link HttpContent} to this RestRequest. This content will be available to read
   * through the read operations.
   * @throws IllegalStateException if content is being added when it is not expected (GET, DELETE, HEAD).
   * @throws ClosedChannelException if request channel has been closed.
   * @throws IllegalArgumentException if {@code httpContent} is null.
   */
  public void addContent(HttpContent httpContent)
      throws ClosedChannelException {
    if (!getRestMethod().equals(RestMethod.POST) && (!(httpContent instanceof LastHttpContent)
        || httpContent.content().readableBytes() > 0)) {
      throw new IllegalStateException("There is no content expected for " + getRestMethod());
    } else if (!isOpen()) {
      throw new ClosedChannelException();
    } else {
      contentLock.lock();
      try {
        if (!isOpen()) {
          throw new ClosedChannelException();
        } else if (writeChannel != null) {
          writeContent(writeChannel, callbackWrapper, httpContent);
        } else {
          ReferenceCountUtil.retain(httpContent);
          requestContents.add(httpContent);
        }
      } finally {
        contentLock.unlock();
      }
    }
  }

  /**
   * Provides info on whether this request desires keep-alive or not.
   * @return {@code true} if keep-alive. {@code false} otherwise.
   */
  protected boolean isKeepAlive() {
    return HttpHeaders.isKeepAlive(request);
  }

  /**
   * Writes the data in the provided {@code httpContent} to the given {@code writeChannel}.
   * @param writeChannel the {@link AsyncWritableChannel} to write the data of {@code httpContent} to.
   * @param callbackWrapper the {@link ReadIntoCallbackWrapper} for the read operation.
   * @param httpContent the piece of {@link HttpContent} that needs to be written to the {@code writeChannel}.
   */
  private void writeContent(AsyncWritableChannel writeChannel, ReadIntoCallbackWrapper callbackWrapper,
      HttpContent httpContent) {
    boolean retained = false;
    ByteBuffer contentBuffer;
    Callback<Long> writeCallback;
    // LastHttpContent in the end marker in netty http world.
    boolean isLast = httpContent instanceof LastHttpContent;
    if (httpContent.content().nioBufferCount() > 0) {
      // not a copy.
      ReferenceCountUtil.retain(httpContent);
      retained = true;
      contentBuffer = httpContent.content().nioBuffer();
      writeCallback = new ContentWriteCallback(httpContent, isLast, callbackWrapper);
    } else {
      // this will not happen (looking at current implementations of ByteBuf in Netty), but if it does, we cannot avoid
      // a copy (or we can introduce a read(GatheringByteChannel) method in ReadableStreamChannel if required).
      nettyMetrics.contentCopyCount.inc();
      logger.warn("HttpContent had to be copied because ByteBuf did not have a backing ByteBuffer");
      contentBuffer = ByteBuffer.allocate(httpContent.content().capacity());
      httpContent.content().readBytes(contentBuffer);
      // no need to retain httpContent since we have a copy.
      writeCallback = new ContentWriteCallback(null, isLast, callbackWrapper);
    }
    boolean asyncWriteCalled = false;
    try {
      writeChannel.write(contentBuffer, writeCallback);
      asyncWriteCalled = true;
    } finally {
      if (retained && !asyncWriteCalled) {
        ReferenceCountUtil.release(httpContent);
      }
    }
  }
}

/**
 * Callback for each write into the given {@link AsyncWritableChannel}.
 */
class ContentWriteCallback implements Callback<Long> {
  private final HttpContent httpContent;
  private final boolean isLast;
  private final ReadIntoCallbackWrapper callbackWrapper;

  /**
   * Creates a new instance of ContentWriteCallback.
   * @param httpContent the {@link HttpContent} whose bytes were just written. Should be null if the data from the
   *                    original {@link HttpContent} was copied and not "retained".
   * @param isLast if this is the last piece of {@link HttpContent} for this request.
   * @param callbackWrapper the {@link ReadIntoCallbackWrapper} that will receive updates of bytes read and one that
   *                        should be invoked in {@link #onCompletion(Long, Exception)} if {@code isLast} is
   *                        {@code true} or exception passed is not null.
   */
  public ContentWriteCallback(HttpContent httpContent, boolean isLast, ReadIntoCallbackWrapper callbackWrapper) {
    this.httpContent = httpContent;
    this.isLast = isLast;
    this.callbackWrapper = callbackWrapper;
  }

  /**
   * Decreases reference counts of content if required, updates the number of bytes read and invokes
   * {@link ReadIntoCallbackWrapper#invokeCallback(Exception)} if {@code exception} is not {@code null} or if this is
   * the last piece of content in the request.
   * @param result The result of the request. This would be non null when the request executed successfully
   * @param exception The exception that was reported on execution of the request
   */
  @Override
  public void onCompletion(Long result, Exception exception) {
    if (httpContent != null) {
      ReferenceCountUtil.release(httpContent);
    }
    callbackWrapper.updateBytesRead(result);
    if (exception != null || isLast) {
      callbackWrapper.invokeCallback(exception);
    }
  }
}

/**
 * Wrapper for callbacks provided to {@link NettyRequest#readInto(AsyncWritableChannel, Callback)}.
 */
class ReadIntoCallbackWrapper {
  /**
   * The {@link Future} where the result of {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} will
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
