/**
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
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.DefaultMaxBytesRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLSession;
import javax.servlet.http.Cookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link RestRequest}.
 * <p/>
 * A wrapper over {@link HttpRequest} and all the {@link HttpContent} associated with the request.
 */
class NettyRequest implements RestRequest {
  // If the write of at least {@code bufferWatermark} amount of data is unacknowledged, reading from the channel will be
  // temporarily suspended. It will be resumed when the amount of data unacknowledged drops below this number. If this
  // is <=0, it is assumed that there is no limit on the size of unacknowledged data.
  static int bufferWatermark = -1;
  private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  protected final HttpRequest request;
  protected final Channel channel;
  protected final NettyMetrics nettyMetrics;
  protected final Map<String, Object> allArgs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  protected final Queue<HttpContent> requestContents = new LinkedBlockingQueue<>();
  protected final ReentrantLock contentLock = new ReentrantLock();

  protected final Map<String, Object> allArgsReadOnly = Collections.unmodifiableMap(allArgs);
  protected final RecvByteBufAllocator savedAllocator;
  protected volatile ReadIntoCallbackWrapper callbackWrapper = null;

  private final long size;
  private final QueryStringDecoder query;
  private final RestMethod restMethod;
  private final RestRequestMetricsTracker restRequestMetricsTracker = new RestRequestMetricsTracker();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  protected final AtomicLong bytesReceived = new AtomicLong(0);
  private final AtomicLong bytesBuffered = new AtomicLong(0);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RecvByteBufAllocator recvByteBufAllocator = new DefaultMaxBytesRecvByteBufAllocator();
  private final SSLSession sslSession;

  private MessageDigest digest;
  private byte[] digestBytes;
  private long digestCalculationTimeInMs = -1;

  private volatile AsyncWritableChannel writeChannel = null;
  private volatile Exception channelException = CLOSED_CHANNEL_EXCEPTION;
  private volatile boolean allContentReceived = false;

  protected static String MULTIPLE_HEADER_VALUE_DELIMITER = ", ";

  /**
   * Wraps the {@code request} in an implementation of {@link RestRequest} so that other layers can understand the
   * request.
   * <p/>
   * Note on content size: The content size is deduced in the following order:-
   * 1. From the {@link RestUtils.Headers#BLOB_SIZE} header.
   * 2. If 1 fails, from the {@link HttpHeaderNames#CONTENT_LENGTH} header.
   * 3. If 2 fails, it is set to -1 which means that the content size is unknown.
   * If content size is set in the header (i.e. not -1), the actual content size should match that value. Otherwise, an
   * exception will be thrown.
   * @param request the {@link HttpRequest} that needs to be wrapped.
   * @param channel the {@link Channel} over which the {@code request} has been received.
   * @param nettyMetrics the {@link NettyMetrics} instance to use.
   * @param blacklistedQueryParams the set of query params that should not be exposed via {@link #getArgs()}.
   * @throws IllegalArgumentException if {@code request} is null.
   * @throws RestServiceException if the {@link HttpMethod} defined in {@code request} is not recognized as a
   *                                {@link RestMethod} or if the {@link RestUtils.Headers#BLOB_SIZE} header is invalid.
   */
  public NettyRequest(HttpRequest request, Channel channel, NettyMetrics nettyMetrics,
      Set<String> blacklistedQueryParams) throws RestServiceException {
    if (request == null || channel == null) {
      throw new IllegalArgumentException("Received null argument(s)");
    }
    restRequestMetricsTracker.nioMetricsTracker.markRequestReceived();
    this.request = request;
    query = new QueryStringDecoder(request.uri());
    this.channel = channel;
    savedAllocator = channel.config().getRecvByteBufAllocator();
    this.nettyMetrics = nettyMetrics;

    SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
    sslSession = sslHandler != null ? sslHandler.engine().getSession() : null;

    HttpMethod httpMethod = request.method();
    if (HttpMethod.GET.equals(httpMethod)) {
      restMethod = RestMethod.GET;
    } else if (HttpMethod.POST.equals(httpMethod)) {
      restMethod = RestMethod.POST;
      if (bufferWatermark > 0) {
        setAutoRead(false);
        continueReadIfPossible(0);
      }
    } else if (HttpMethod.PUT.equals(httpMethod)) {
      restMethod = RestMethod.PUT;
      if (bufferWatermark > 0) {
        setAutoRead(false);
        continueReadIfPossible(0);
      }
    } else if (HttpMethod.DELETE.equals(httpMethod)) {
      restMethod = RestMethod.DELETE;
    } else if (HttpMethod.HEAD.equals(httpMethod)) {
      restMethod = RestMethod.HEAD;
    } else if (HttpMethod.OPTIONS.equals(httpMethod)) {
      restMethod = RestMethod.OPTIONS;
    } else {
      nettyMetrics.unsupportedHttpMethodError.inc();
      throw new RestServiceException("http method not supported: " + httpMethod,
          RestServiceErrorCode.UnsupportedHttpMethod);
    }

    String blobSizeStr = request.headers().get(RestUtils.Headers.BLOB_SIZE, null);
    if (blobSizeStr != null) {
      try {
        size = Long.parseLong(blobSizeStr);
        if (size < 0) {
          throw new RestServiceException(RestUtils.Headers.BLOB_SIZE + " [" + size + "] is less than 0",
              RestServiceErrorCode.InvalidArgs);
        }
      } catch (NumberFormatException e) {
        throw new RestServiceException(
            RestUtils.Headers.BLOB_SIZE + " [" + blobSizeStr + "] could not parsed into a number",
            RestServiceErrorCode.InvalidArgs);
      }
    } else {
      size = HttpUtil.getContentLength(request, -1L);
    }

    // query params.
    for (Map.Entry<String, List<String>> e : query.parameters().entrySet()) {
      if (!blacklistedQueryParams.contains(e.getKey())) {
        StringBuilder value = null;
        if (e.getValue() != null) {
          StringBuilder combinedValues = combineVals(new StringBuilder(), e.getValue());
          if (combinedValues.length() > 0) {
            value = combinedValues;
          }
        }
        allArgs.put(e.getKey(), value);
      } else {
        logger.debug("Encountered blacklisted query parameter {} in request {}", e, request);
      }
    }

    Set<io.netty.handler.codec.http.cookie.Cookie> nettyCookies = null;
    // headers.
    for (Map.Entry<String, String> e : request.headers()) {
      StringBuilder sb;
      if (e.getKey().equalsIgnoreCase(HttpHeaderNames.COOKIE.toString())) {
        String value = e.getValue();
        if (value != null) {
          nettyCookies = ServerCookieDecoder.STRICT.decode(value);
        }
      } else {
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
    }

    // turn all StringBuilders into String
    for (Map.Entry<String, Object> e : allArgs.entrySet()) {
      if (allArgs.get(e.getKey()) != null) {
        allArgs.put(e.getKey(), (e.getValue()).toString());
      }
    }
    // add cookies to the args as java cookies
    if (nettyCookies != null) {
      Set<javax.servlet.http.Cookie> cookies = convertHttpToJavaCookies(nettyCookies);
      allArgs.put(RestUtils.Headers.COOKIE, cookies);
    }
  }

  @Override
  public String getUri() {
    return request.uri();
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
    return allArgsReadOnly;
  }

  @Override
  public Object setArg(String key, Object value) {
    return allArgs.put(key, value);
  }

  @Override
  public SSLSession getSSLSession() {
    return sslSession;
  }

  @Override
  public void prepare() throws RestServiceException {
    // no op.
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() {
    if (channelOpen.compareAndSet(true, false)) {
      setAutoRead(true);
      contentLock.lock();
      try {
        logger.trace("Closing NettyRequest {} with {} content chunks unread", getUri(), requestContents.size());
        // For non-POST we usually have one content chunk unread - this the LastHttpContent chunk. This is OK.
        HttpContent content = requestContents.poll();
        while (content != null) {
          ReferenceCountUtil.release(content);
          content = requestContents.poll();
        }
      } finally {
        contentLock.unlock();
        restRequestMetricsTracker.nioMetricsTracker.markRequestCompleted();
        if (digestCalculationTimeInMs >= 0) {
          nettyMetrics.digestCalculationTimeInMs.update(digestCalculationTimeInMs);
        }
        if (callbackWrapper != null) {
          callbackWrapper.invokeCallback(channelException);
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
   * size. If that cannot be done, returns -1.
   * <p/>
   * This function does not individually count the bytes in the content (it is not possible) so the bytes received may
   * actually be different if the stream is buggy or the client made a mistake. Do *not* treat this as fully accurate.
   * @return the size of content as defined in headers. Might not be actual length of content if the stream is buggy.
   */
  @Override
  public long getSize() {
    return size;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    ReadIntoCallbackWrapper tempWrapper = new ReadIntoCallbackWrapper(callback);
    contentLock.lock();
    try {
      if (!isOpen()) {
        nettyMetrics.requestAlreadyClosedError.inc();
        tempWrapper.invokeCallback(new ClosedChannelException());
      } else if (writeChannel != null) {
        throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
      }
      HttpContent content = requestContents.poll();
      while (content != null) {
        try {
          writeContent(asyncWritableChannel, tempWrapper, content);
        } finally {
          ReferenceCountUtil.release(content);
        }
        content = requestContents.poll();
      }
      callbackWrapper = tempWrapper;
      writeChannel = asyncWritableChannel;
    } finally {
      contentLock.unlock();
    }
    return tempWrapper.futureResult;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * This function can only be called before {@link #readInto(AsyncWritableChannel, Callback)}.
   * @param digestAlgorithm the digest algorithm to use.
   * @throws NoSuchAlgorithmException if the {@code digestAlgorithm} does not exist or is not supported.
   * @throws IllegalStateException if {@link #readInto(AsyncWritableChannel, Callback)} has already been called.
   */
  @Override
  public void setDigestAlgorithm(String digestAlgorithm) throws NoSuchAlgorithmException {
    if (callbackWrapper != null) {
      throw new IllegalStateException("Cannot create a digest because some content may have been consumed");
    }
    digest = MessageDigest.getInstance(digestAlgorithm);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * This function can only be called once the channel has been emptied.
   * @return the digest as computed by the digest algorithm set through {@link #setDigestAlgorithm(String)}. If none
   * was set, {@code null}.
   * @throws IllegalStateException if called before the channel has been emptied.
   */
  @Override
  public byte[] getDigest() {
    if (digest == null) {
      return null;
    } else if (!allContentReceived) {
      throw new IllegalStateException("Cannot calculate digest yet because all the content has not been processed");
    }
    if (digestBytes == null) {
      long startTime = System.currentTimeMillis();
      digestBytes = digest.digest();
      digestCalculationTimeInMs += (System.currentTimeMillis() - startTime);
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

  /**
   * Adds some content in the form of {@link HttpContent} to this RestRequest. This content will be available to read
   * through the read operations.
   * @param httpContent the {@link HttpContent} that needs to be added.
   * @throws IllegalStateException if content is being added when it is not expected (GET, DELETE, HEAD).
   * @throws RestServiceException if request channel has been closed.
   */
  protected void addContent(HttpContent httpContent) throws RestServiceException {
    if (!getRestMethod().equals(RestMethod.POST) && !getRestMethod().equals(RestMethod.PUT) && (
        !(httpContent instanceof LastHttpContent) || httpContent.content().readableBytes() > 0)) {
      throw new IllegalStateException("There is no content expected for " + getRestMethod());
    } else {
      validateState(httpContent);
      contentLock.lock();
      try {
        int size = httpContent.content().readableBytes();
        if (!isOpen()) {
          nettyMetrics.requestAlreadyClosedError.inc();
          throw new RestServiceException("The request has been closed and is not accepting content",
              RestServiceErrorCode.RequestChannelClosed);
        } else if (writeChannel != null) {
          writeContent(writeChannel, callbackWrapper, httpContent);
          continueReadIfPossible(size);
        } else {
          requestContents.add(httpContent.retain());
          continueReadIfPossible(size);
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
    return HttpUtil.isKeepAlive(request);
  }

  /**
   * Provides info on whether this request is multipart or not.
   * @return {@code true} if multipart. {@code false} otherwise.
   */
  protected boolean isMultipart() {
    return HttpPostRequestDecoder.isMultipart(request);
  }

  /**
   * Writes the data in the provided {@code httpContent} to the given {@code writeChannel}.
   * @param writeChannel the {@link AsyncWritableChannel} to write the data of {@code httpContent} to.
   * @param callbackWrapper the {@link ReadIntoCallbackWrapper} for the read operation.
   * @param httpContent the piece of {@link HttpContent} that needs to be written to the {@code writeChannel}.
   */
  protected void writeContent(AsyncWritableChannel writeChannel, ReadIntoCallbackWrapper callbackWrapper,
      HttpContent httpContent) {
    ByteBuffer[] contentBuffers;
    // LastHttpContent in the end marker in netty http world.
    boolean isLast = httpContent instanceof LastHttpContent;
    if (isLast) {
      setAutoRead(true);
    }
    if (httpContent.content().nioBufferCount() > 0) {
      contentBuffers = httpContent.content().nioBuffers();
    } else {
      // this will not happen (looking at current implementations of ByteBuf in Netty), but if it does, we cannot avoid
      // a copy (or we can introduce a read(GatheringByteChannel) method in ReadableStreamChannel if required).
      nettyMetrics.contentCopyCount.inc();
      ByteBuf content = httpContent.content();
      logger.warn("HttpContent had to be copied because ByteBuf did not have a backing ByteBuffer");
      ByteBuffer contentBuffer = ByteBuffer.allocate(content.readableBytes());
      // don't change the readerIndex
      content.getBytes(content.readerIndex(), contentBuffer);
      contentBuffer.flip();
      contentBuffers = new ByteBuffer[]{contentBuffer};
    }
    if (digest != null) {
      for (int i = 0; i < contentBuffers.length; i++) {
        long startTime = System.currentTimeMillis();
        digest.update(contentBuffers[i]);
        digestCalculationTimeInMs += (System.currentTimeMillis() - startTime);
      }
    }
    // Retain this httpContent so it won't be garbage collected right away. Release it in the callback.
    httpContent.retain();
    writeChannel.write(httpContent.content(), new ContentWriteCallback(httpContent, isLast, callbackWrapper));
    allContentReceived = isLast;
  }

  /**
   * Switches auto reading from the channel on and off.
   * @param autoRead {@code true} if auto read has to be switched on. {@code false} otherwise.
   */
  protected void setAutoRead(boolean autoRead) {
    channel.config().setAutoRead(autoRead);
    channel.config().setRecvByteBufAllocator(autoRead ? savedAllocator : recvByteBufAllocator);
    logger.trace("Setting auto-read to {} on channel {}", channel.config().isAutoRead(), channel);
  }

  /**
   * Converts the Set of {@link io.netty.handler.codec.http.cookie.Cookie}s to equivalent
   * {@link javax.servlet.http.Cookie}s
   * @param httpCookies Set of {@link io.netty.handler.codec.http.cookie.Cookie}s that needs to be converted
   * @return Set of {@link javax.servlet.http.Cookie}s equivalent to the passed in
   * {@link io.netty.handler.codec.http.cookie.Cookie}s
   */
  private Set<Cookie> convertHttpToJavaCookies(Set<io.netty.handler.codec.http.cookie.Cookie> httpCookies) {
    Set<javax.servlet.http.Cookie> cookies = new HashSet<Cookie>();
    for (io.netty.handler.codec.http.cookie.Cookie cookie : httpCookies) {
      try {
        javax.servlet.http.Cookie javaCookie = new javax.servlet.http.Cookie(cookie.name(), cookie.value());
        cookies.add(javaCookie);
      } catch (IllegalArgumentException e) {
        logger.debug("Could not create cookie with name [{}]", cookie.name(), e);
      }
    }
    return cookies;
  }

  /**
   * Combines {@code values} into {@code currValue} by creating a comma separated string.
   * @param currValue the value to which {@code values} have to be appended to.
   * @param values the values that need to be appended to @code currValue}.
   * @return the updated @code currValue}.
   */
  private StringBuilder combineVals(StringBuilder currValue, List<String> values) {
    for (String value : values) {
      if (currValue.length() > 0) {
        currValue.append(MULTIPLE_HEADER_VALUE_DELIMITER);
      }
      currValue.append(value);
    }
    return currValue;
  }

  /**
   * Validates the stream by checking that the size in the headers matches the size of the actual data.
   * @param httpContent the {@link HttpContent} that was just received.
   * @throws RestServiceException if {@code httpContent} is the last piece of content and the size of data does
   *                              not match the size in the header.
   */
  private void validateState(HttpContent httpContent) throws RestServiceException {
    long bytesReceivedTillNow = bytesReceived.addAndGet(httpContent.content().readableBytes());
    if (size > 0) {
      if (bytesReceivedTillNow > size) {
        channelException = new RestServiceException("Size of content is more than the size provided in headers",
            RestServiceErrorCode.BadRequest);
        throw (RestServiceException) channelException;
      } else if (httpContent instanceof LastHttpContent && bytesReceivedTillNow != size) {
        channelException = new RestServiceException("Size of content is less than the size provided in headers",
            RestServiceErrorCode.BadRequest);
        throw (RestServiceException) channelException;
      }
    }
  }

  /**
   * Invokes a read from the read channel if the number of bytes buffered is below the buffer watermark. No effect if
   * auto-read is on.
   * @param delta number of bytes read from the read channel in the current read (positive) or number of bytes written
   *              to the write channel in the current write (negative).
   */
  private void continueReadIfPossible(long delta) {
    if (!channel.config().isAutoRead()) {
      if (bytesBuffered.addAndGet(delta) < bufferWatermark) {
        channel.read();
      } else {
        nettyMetrics.watermarkOverflowCount.inc();
      }
    }
  }

  /**
   * Callback for each write into the given {@link AsyncWritableChannel}.
   */
  protected class ContentWriteCallback implements Callback<Long> {
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
        httpContent.release();
      }
      callbackWrapper.updateBytesRead(result);
      continueReadIfPossible(-result);
      if (exception != null || isLast) {
        callbackWrapper.invokeCallback(exception);
      }
    }
  }

  /**
   * Wrapper for callbacks provided to {@link NettyRequest#readInto(AsyncWritableChannel, Callback)}.
   */
  protected class ReadIntoCallbackWrapper {
    /**
     * The {@link Future} where the result of {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} will
     * eventually be updated.
     */
    public final FutureResult<Long> futureResult = new FutureResult<Long>();

    private Callback<Long> callback;
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
          // the callback may hold a reference to a large buffer. Setting the callback to null allows for memory to be
          // freed before the next request comes over this connection.
          callback = null;
        }
      }
    }
  }
}
