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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.DefaultMaxBytesRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyRequest}.
 */
public class NettyRequestTest {
  private static final int GENERATED_CONTENT_SIZE = 10240;
  private static final int GENERATED_CONTENT_PART_COUNT = 10;
  private static String BLACKLISTED_QUERY_PARAM = "paramBlacklisted";
  private static final Set<String> BLACKLISTED_QUERY_PARAM_SET = Collections.singleton(BLACKLISTED_QUERY_PARAM);
  private static final int DEFAULT_WATERMARK;

  static {
    DEFAULT_WATERMARK = new NettyConfig(new VerifiableProperties(new Properties())).nettyServerRequestBufferWatermark;
  }

  public NettyRequestTest() {
    NettyRequest.bufferWatermark = DEFAULT_WATERMARK;
  }

  /**
   * Tests conversion of {@link HttpRequest} to {@link NettyRequest} given good input.
   * @throws RestServiceException
   */
  @Test
  public void conversionWithGoodInputTest() throws RestServiceException, CertificateException, SSLException {
    // headers
    HttpHeaders headers = new DefaultHttpHeaders(false);
    headers.add(HttpHeaderNames.CONTENT_LENGTH, new Random().nextInt(Integer.MAX_VALUE));
    headers.add("headerKey", "headerValue1");
    headers.add("headerKey", "headerValue2");
    headers.add("overLoadedKey", "headerOverloadedValue");
    headers.add("paramNoValueInUriButValueInHeader", "paramValueInHeader");

    // params
    Map<String, List<String>> params = new HashMap<String, List<String>>();
    List<String> values = new ArrayList<String>(2);
    values.add("paramValue1");
    values.add("paramValue2");
    params.put("paramKey", values);
    values = new ArrayList<String>(1);
    values.add("paramOverloadedValue");
    params.put("overLoadedKey", values);
    params.put("paramNoValue", null);
    params.put("paramNoValueInUriButValueInHeader", null);
    params.put(BLACKLISTED_QUERY_PARAM, values);

    StringBuilder uriAttachmentBuilder = new StringBuilder("?");
    for (Map.Entry<String, List<String>> param : params.entrySet()) {
      if (param.getValue() != null) {
        for (String value : param.getValue()) {
          uriAttachmentBuilder.append(param.getKey()).append("=").append(value).append("&");
        }
      } else {
        uriAttachmentBuilder.append(param.getKey()).append("&");
      }
    }
    uriAttachmentBuilder.deleteCharAt(uriAttachmentBuilder.length() - 1);
    String uriAttachment = uriAttachmentBuilder.toString();

    NettyRequest nettyRequest;
    String uri;
    Set<Cookie> cookies = new HashSet<>();
    Cookie httpCookie = new DefaultCookie("CookieKey1", "CookieValue1");
    cookies.add(httpCookie);
    httpCookie = new DefaultCookie("CookieKey2", "CookieValue2");
    cookies.add(httpCookie);
    headers.add(RestUtils.Headers.COOKIE, getCookiesHeaderValue(cookies));

    for (MockChannel channel : Arrays.asList(new MockChannel(), new MockChannel().addSslHandlerToPipeline())) {
      uri = "/GET" + uriAttachment;
      nettyRequest = createNettyRequest(HttpMethod.GET, uri, headers, channel);
      validateRequest(nettyRequest, RestMethod.GET, uri, headers, params, cookies, channel);
      closeRequestAndValidate(nettyRequest, channel);

      RecvByteBufAllocator savedAllocator = channel.config().getRecvByteBufAllocator();
      int[] bufferWatermarks = {-1, 0, 1, DEFAULT_WATERMARK};
      for (int bufferWatermark : bufferWatermarks) {
        NettyRequest.bufferWatermark = bufferWatermark;
        uri = "/POST" + uriAttachment;
        nettyRequest = createNettyRequest(HttpMethod.POST, uri, headers, channel);
        validateRequest(nettyRequest, RestMethod.POST, uri, headers, params, cookies, channel);
        if (bufferWatermark > 0) {
          assertTrue("RecvAllocator should have changed",
              channel.config().getRecvByteBufAllocator() instanceof DefaultMaxBytesRecvByteBufAllocator);
        } else {
          assertEquals("RecvAllocator not as expected", savedAllocator, channel.config().getRecvByteBufAllocator());
        }
        closeRequestAndValidate(nettyRequest, channel);
        assertEquals("Allocator not as expected", savedAllocator, channel.config().getRecvByteBufAllocator());
      }

      for (int bufferWatermark : bufferWatermarks) {
        NettyRequest.bufferWatermark = bufferWatermark;
        uri = "/PUT" + uriAttachment;
        nettyRequest = createNettyRequest(HttpMethod.PUT, uri, headers, channel);
        validateRequest(nettyRequest, RestMethod.PUT, uri, headers, params, cookies, channel);
        if (bufferWatermark > 0) {
          assertTrue("RecvAllocator should have changed",
              channel.config().getRecvByteBufAllocator() instanceof DefaultMaxBytesRecvByteBufAllocator);
        } else {
          assertEquals("RecvAllocator not as expected", savedAllocator, channel.config().getRecvByteBufAllocator());
        }
        closeRequestAndValidate(nettyRequest, channel);
        assertEquals("Allocator not as expected", savedAllocator, channel.config().getRecvByteBufAllocator());
      }

      NettyRequest.bufferWatermark = DEFAULT_WATERMARK;

      uri = "/DELETE" + uriAttachment;
      nettyRequest = createNettyRequest(HttpMethod.DELETE, uri, headers, channel);
      validateRequest(nettyRequest, RestMethod.DELETE, uri, headers, params, cookies, channel);
      closeRequestAndValidate(nettyRequest, channel);

      uri = "/HEAD" + uriAttachment;
      nettyRequest = createNettyRequest(HttpMethod.HEAD, uri, headers, channel);
      validateRequest(nettyRequest, RestMethod.HEAD, uri, headers, params, cookies, channel);
      closeRequestAndValidate(nettyRequest, channel);

      uri = "/OPTIONS" + uriAttachment;
      nettyRequest = createNettyRequest(HttpMethod.OPTIONS, uri, headers, channel);
      validateRequest(nettyRequest, RestMethod.OPTIONS, uri, headers, params, cookies, channel);
      closeRequestAndValidate(nettyRequest, channel);
    }
  }

  /**
   * Tests conversion of {@link HttpRequest} to {@link NettyRequest} given bad input (i.e. checks for the correct
   * exception and {@link RestServiceErrorCode} if any).
   * @throws RestServiceException
   */
  @Test
  public void conversionWithBadInputTest() throws RestServiceException {
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "");
    // HttpRequest null.
    try {
      new NettyRequest(null, new MockChannel(), new NettyMetrics(new MetricRegistry()), BLACKLISTED_QUERY_PARAM_SET);
      fail("Provided null HttpRequest to NettyRequest, yet it did not fail");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // Channel null.
    try {
      new NettyRequest(httpRequest, null, new NettyMetrics(new MetricRegistry()), BLACKLISTED_QUERY_PARAM_SET);
      fail("Provided null Channel to NettyRequest, yet it did not fail");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // unknown http method
    try {
      createNettyRequest(HttpMethod.TRACE, "/", null, new MockChannel());
      fail("Unknown http method was supplied to NettyRequest. It should have failed to construct");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.UnsupportedHttpMethod, e.getErrorCode());
    }

    String[] invalidBlobSizeStrs = {"aba123", "12ab", "-1", "ddsdd", "999999999999999999999999999", "1.234"};
    for (String blobSizeStr : invalidBlobSizeStrs) {
      // bad blob size
      try {
        createNettyRequest(HttpMethod.GET, "/", new DefaultHttpHeaders().add(RestUtils.Headers.BLOB_SIZE, blobSizeStr),
            new MockChannel());
        fail("Bad blob size header was supplied to NettyRequest. It should have failed to construct");
      } catch (RestServiceException e) {
        assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
      }
    }
  }

  /**
   * Tests for behavior of multiple operations after {@link NettyRequest#close()} has been called. Some should be ok to
   * do and some should throw exceptions.
   * @throws Exception
   */
  @Test
  public void operationsAfterCloseTest() throws Exception {
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    closeRequestAndValidate(nettyRequest, channel);

    // operations that should be ok to do (does not include all operations).
    nettyRequest.close();

    // operations that will throw exceptions.
    AsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    try {
      nettyRequest.readInto(writeChannel, callback).get();
      fail("Request channel has been closed, so read should have thrown ClosedChannelException");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertTrue("Exception is not ClosedChannelException", exception instanceof ClosedChannelException);
      callback.awaitCallback();
      assertEquals("Exceptions of callback and future differ", exception.getMessage(), callback.exception.getMessage());
    }

    try {
      byte[] content = TestUtils.getRandomBytes(1024);
      nettyRequest.addContent(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content)));
      fail("Request channel has been closed, so addContent() should have thrown ClosedChannelException");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.RequestChannelClosed, e.getErrorCode());
    }
  }

  /**
   * Tests {@link NettyRequest#addContent(HttpContent)} and
   * {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} with different digest algorithms (including a test
   * with no digest algorithm).
   * @throws Exception
   */
  @Test
  public void contentAddAndReadTest() throws Exception {
    String[] digestAlgorithms = {"", "MD5", "SHA-1", "SHA-256"};
    HttpMethod[] methods = {HttpMethod.POST, HttpMethod.PUT};
    for (HttpMethod method : methods) {
      for (String digestAlgorithm : digestAlgorithms) {
        contentAddAndReadTest(digestAlgorithm, true, method);
        contentAddAndReadTest(digestAlgorithm, false, method);
      }
    }
  }

  /**
   * Tests {@link NettyRequest#addContent(HttpContent)} and
   * {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} with different digest algorithms (including a test
   * with no digest algorithm) and checks that back pressure is applied correctly.
   * @throws Exception
   */
  @Test
  public void backPressureTest() throws Exception {
    String[] digestAlgorithms = {"", "MD5", "SHA-1", "SHA-256"};

    HttpMethod[] methods = {HttpMethod.POST, HttpMethod.PUT};
    for (HttpMethod method : methods) {
      for (String digestAlgorithm : digestAlgorithms) {
        backPressureTest(digestAlgorithm, true, method);
        backPressureTest(digestAlgorithm, false, method);
      }
    }
  }

  /**
   * Tests exception scenarios of {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} and behavior of
   * {@link NettyRequest} when {@link AsyncWritableChannel} instances fail.
   * @throws Exception
   */
  @Test
  public void readIntoExceptionsTest() throws Exception {
    Channel channel = new MockChannel();
    // try to call readInto twice.
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    AsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    nettyRequest.readInto(writeChannel, null);

    try {
      nettyRequest.readInto(writeChannel, null);
      fail("Calling readInto twice should have failed");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
    closeRequestAndValidate(nettyRequest, channel);

    // write into a channel that throws exceptions
    // non RuntimeException
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    assertTrue("Not enough content has been generated", httpContents.size() > 2);
    String expectedMsg = "@@expectedMsg@@";
    Exception exception = new Exception(expectedMsg);
    writeChannel = new BadAsyncWritableChannel(exception);
    ReadIntoCallback callback = new ReadIntoCallback();

    // add content initially
    int addedCount = 0;
    for (; addedCount < httpContents.size() / 2; addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    Future<Long> future = nettyRequest.readInto(writeChannel, callback);

    // add some more content
    for (; addedCount < httpContents.size(); addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      nettyRequest.addContent(httpContent);
    }

    writeChannel.close();
    verifyRefCnts(httpContents);
    callback.awaitCallback();
    assertNotNull("Exception was not piped correctly", callback.exception);
    assertEquals("Exception message mismatch (callback)", expectedMsg, callback.exception.getMessage());
    try {
      future.get();
      fail("Future should have thrown exception");
    } catch (ExecutionException e) {
      assertEquals("Exception message mismatch (future)", expectedMsg, Utils.getRootCause(e).getMessage());
    }
    closeRequestAndValidate(nettyRequest, channel);

    // RuntimeException
    // during readInto
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    exception = new IllegalStateException(expectedMsg);
    writeChannel = new BadAsyncWritableChannel(exception);
    callback = new ReadIntoCallback();

    for (HttpContent httpContent : httpContents) {
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    try {
      nettyRequest.readInto(writeChannel, callback);
      fail("readInto did not throw expected exception");
    } catch (Exception e) {
      assertEquals("Exception caught does not match expected exception", expectedMsg, e.getMessage());
    }
    writeChannel.close();
    closeRequestAndValidate(nettyRequest, channel);
    verifyRefCnts(httpContents);

    // after readInto
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    exception = new IllegalStateException(expectedMsg);
    writeChannel = new BadAsyncWritableChannel(exception);
    callback = new ReadIntoCallback();

    nettyRequest.readInto(writeChannel, callback);
    // add content
    HttpContent httpContent = httpContents.get(1);
    try {
      nettyRequest.addContent(httpContent);
      fail("addContent did not throw expected exception");
    } catch (Exception e) {
      assertEquals("Exception caught does not match expected exception", expectedMsg, e.getMessage());
    }
    writeChannel.close();
    closeRequestAndValidate(nettyRequest, channel);
    verifyRefCnts(httpContents);
  }

  /**
   * Tests that {@link NettyRequest#close()} leaves any added {@link HttpContent} the way it was before it was added.
   * (i.e no reference count changes).
   * @throws RestServiceException
   */
  @Test
  public void closeTest() throws RestServiceException {
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    Queue<HttpContent> httpContents = new LinkedBlockingQueue<HttpContent>();
    for (int i = 0; i < 5; i++) {
      ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(1024));
      HttpContent httpContent = new DefaultHttpContent(Unpooled.wrappedBuffer(content));
      nettyRequest.addContent(httpContent);
      httpContents.add(httpContent);
    }
    closeRequestAndValidate(nettyRequest, channel);
    while (httpContents.peek() != null) {
      assertEquals("Reference count of http content has changed", 1, httpContents.poll().refCnt());
    }
  }

  /**
   * Tests different state transitions that can happen with {@link NettyRequest#addContent(HttpContent)} for GET
   * requests. Some transitions are valid and some should necessarily throw exceptions.
   * @throws RestServiceException
   */
  @Test
  public void addContentForGetTest() throws RestServiceException {
    byte[] content = TestUtils.getRandomBytes(16);
    // adding non LastHttpContent to nettyRequest
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null, new MockChannel());
    try {
      nettyRequest.addContent(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
      fail("GET requests should not accept non-LastHTTPContent");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    // adding LastHttpContent with some content to nettyRequest
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", null, new MockChannel());
    try {
      nettyRequest.addContent(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content)));
      fail("GET requests should not accept actual content in LastHTTPContent");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    // should accept LastHttpContent just fine.
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", null, new MockChannel());
    nettyRequest.addContent(new DefaultLastHttpContent());

    // should not accept LastHttpContent after close
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", null, new MockChannel());
    nettyRequest.close();
    try {
      nettyRequest.addContent(new DefaultLastHttpContent());
      fail("Request channel has been closed, so addContent() should have thrown ClosedChannelException");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.RequestChannelClosed, e.getErrorCode());
    }
  }

  @Test
  public void keepAliveTest() throws RestServiceException {
    NettyRequest request = createNettyRequest(HttpMethod.GET, "/", null, new MockChannel());
    // by default, keep-alive is true for HTTP 1.1
    assertTrue("Keep-alive not as expected", request.isKeepAlive());

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    request = createNettyRequest(HttpMethod.GET, "/", headers, new MockChannel());
    assertTrue("Keep-alive not as expected", request.isKeepAlive());

    headers = new DefaultHttpHeaders();
    headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    request = createNettyRequest(HttpMethod.GET, "/", headers, new MockChannel());
    assertFalse("Keep-alive not as expected", request.isKeepAlive());
  }

  /**
   * Tests the {@link NettyRequest#getSize()} function to see that it respects priorities.
   * @throws RestServiceException
   */
  @Test
  public void sizeTest() throws RestServiceException {
    // no length headers provided.
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null, new MockChannel());
    assertEquals("Size not as expected", -1, nettyRequest.getSize());

    // deliberate mismatch to check priorities.
    int xAmbryBlobSize = 20;
    int contentLength = 10;

    // Content-Length header set
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.CONTENT_LENGTH, contentLength);
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", headers, new MockChannel());
    assertEquals("Size not as expected", contentLength, nettyRequest.getSize());

    // xAmbryBlobSize set
    headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.BLOB_SIZE, xAmbryBlobSize);
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", headers, new MockChannel());
    assertEquals("Size not as expected", xAmbryBlobSize, nettyRequest.getSize());

    // both set
    headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.BLOB_SIZE, xAmbryBlobSize);
    headers.add(HttpHeaderNames.CONTENT_LENGTH, contentLength);
    nettyRequest = createNettyRequest(HttpMethod.GET, "/", headers, new MockChannel());
    assertEquals("Size not as expected", xAmbryBlobSize, nettyRequest.getSize());
  }

  /**
   * Tests for POST request that has no content.
   * @throws Exception
   */
  @Test
  public void zeroSizeContentTest() throws Exception {
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    HttpContent httpContent = new DefaultLastHttpContent();

    nettyRequest.addContent(httpContent);
    assertEquals("Reference count is not as expected", 2, httpContent.refCnt());

    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    Future<Long> future = nettyRequest.readInto(writeChannel, callback);
    assertEquals("There should be no content", 0, writeChannel.getNextChunk().remaining());
    writeChannel.resolveOldestChunk(null);
    closeRequestAndValidate(nettyRequest, channel);
    writeChannel.close();
    assertEquals("Reference count of http content has changed", 1, httpContent.refCnt());
    callback.awaitCallback();
    if (callback.exception != null) {
      throw callback.exception;
    }
    long futureBytesRead = future.get();
    assertEquals("Total bytes read does not match (callback)", 0, callback.bytesRead);
    assertEquals("Total bytes read does not match (future)", 0, futureBytesRead);
  }

  /**
   * Tests reaction of NettyRequest when content size is different from the size specified in the headers.
   * @throws Exception
   */
  @Test
  public void headerAndContentSizeMismatchTest() throws Exception {
    sizeInHeaderMoreThanContentTest();
    sizeInHeaderLessThanContentTest();
  }

  /**
   * Does any left over tests for {@link NettyRequest.ContentWriteCallback}
   */
  @Test
  public void contentWriteCallbackTests() throws RestServiceException {
    ReadIntoCallback readIntoCallback = new ReadIntoCallback();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.GET, "/", null, new MockChannel());
    NettyRequest.ReadIntoCallbackWrapper wrapper = nettyRequest.new ReadIntoCallbackWrapper(readIntoCallback);
    NettyRequest.ContentWriteCallback callback = nettyRequest.new ContentWriteCallback(null, true, wrapper);
    long bytesRead = new Random().nextInt(Integer.MAX_VALUE);
    // there should be no problem even though httpContent is null.
    callback.onCompletion(bytesRead, null);
    assertEquals("Bytes read does not match", bytesRead, readIntoCallback.bytesRead);
  }

  /**
   * Tests for incorrect usage of {@link NettyRequest#setDigestAlgorithm(String)} and {@link NettyRequest#getDigest()}.
   * @throws NoSuchAlgorithmException
   * @throws RestServiceException
   */
  @Test
  public void digestIncorrectUsageTest() throws NoSuchAlgorithmException, RestServiceException {
    setDigestAfterReadTest();
    setBadAlgorithmTest();
    getDigestWithoutSettingAlgorithmTest();
    getDigestBeforeAllContentProcessedTest();
  }

  // helpers
  // general

  /**
   * Creates a {@link NettyRequest} with the given parameters.
   * @param httpMethod the {@link HttpMethod} desired.
   * @param uri the URI desired.
   * @param headers {@link HttpHeaders} that need to be a part of the request.
   * @param channel the {@link Channel} that the request arrived over.
   * @return {@link NettyRequest} encapsulating a {@link HttpRequest} with the given parameters.
   * @throws RestServiceException if the {@code httpMethod} is not recognized by {@link NettyRequest}.
   */
  private NettyRequest createNettyRequest(HttpMethod httpMethod, String uri, HttpHeaders headers, Channel channel)
      throws RestServiceException {
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri, false);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    NettyRequest nettyRequest =
        new NettyRequest(httpRequest, channel, new NettyMetrics(metricRegistry), BLACKLISTED_QUERY_PARAM_SET);
    assertEquals("Auto-read is in an invalid state",
        (!httpMethod.equals(HttpMethod.POST) && !httpMethod.equals(HttpMethod.PUT))
            || NettyRequest.bufferWatermark <= 0, channel.config().isAutoRead());
    return nettyRequest;
  }

  /**
   * Closes the provided {@code nettyRequest} and validates that it is actually closed.
   * @param nettyRequest the {@link NettyRequest} that needs to be closed and validated.
   * @param channel the {@link Channel} over which the request was received.
   */
  private void closeRequestAndValidate(NettyRequest nettyRequest, Channel channel) {
    nettyRequest.close();
    assertFalse("Request channel is not closed", nettyRequest.isOpen());
    assertTrue("Auto-read is not as expected", channel.config().isAutoRead());
  }

  /**
   * Convert a set of {@link Cookie} to a string that could be used as header value in http request
   * @param cookies that needs conversion
   * @return string representation of the set of cookies
   */
  private String getCookiesHeaderValue(Set<Cookie> cookies) {
    StringBuilder cookieStr = new StringBuilder();
    for (Cookie cookie : cookies) {
      if (cookieStr.length() != 0) {
        cookieStr.append("; ");
      }
      cookieStr.append(cookie.name()).append("=").append(cookie.value());
    }
    return cookieStr.toString();
  }

  // conversionWithGoodInputTest() helpers

  /**
   * Validates the various expected properties of the provided {@code nettyRequest}.
   * @param nettyRequest the {@link NettyRequest} that needs to be validated.
   * @param restMethod the expected {@link RestMethod} in {@code nettyRequest}.
   * @param uri the expected URI in {@code nettyRequest}.
   * @param headers the {@link HttpHeaders} passed with the request that need to be in {@link NettyRequest#getArgs()}.
   * @param params the parameters passed with the request that need to be in {@link NettyRequest#getArgs()}.
   * @param httpCookies Set of {@link Cookie} set in the request
   * @param channel the {@link MockChannel} over which the request was received.
   */
  private void validateRequest(NettyRequest nettyRequest, RestMethod restMethod, String uri, HttpHeaders headers,
      Map<String, List<String>> params, Set<Cookie> httpCookies, MockChannel channel) {
    long contentLength =
        headers.contains(HttpHeaderNames.CONTENT_LENGTH) ? Long.parseLong(headers.get(HttpHeaderNames.CONTENT_LENGTH))
            : 0;
    assertTrue("Request channel is not open", nettyRequest.isOpen());
    assertEquals("Mismatch in content length", contentLength, nettyRequest.getSize());
    assertEquals("Mismatch in rest method", restMethod, nettyRequest.getRestMethod());
    assertEquals("Mismatch in path", uri.substring(0, uri.indexOf("?")), nettyRequest.getPath());
    assertEquals("Mismatch in uri", uri, nettyRequest.getUri());
    assertNotNull("There should have been a RestRequestMetricsTracker", nettyRequest.getMetricsTracker());
    assertFalse("Should not have been a multipart request", nettyRequest.isMultipart());
    SSLSession sslSession = nettyRequest.getSSLSession();
    if (channel.pipeline().get(SslHandler.class) == null) {
      assertNull("Non-null SSLSession when pipeline does not contain an SslHandler", sslSession);
    } else {
      assertEquals("SSLSession does not match one from MockChannel", channel.getSSLEngine().getSession(), sslSession);
    }

    Set<javax.servlet.http.Cookie> actualCookies =
        (Set<javax.servlet.http.Cookie>) nettyRequest.getArgs().get(RestUtils.Headers.COOKIE);
    compareCookies(httpCookies, actualCookies);

    Map<String, List<String>> receivedArgs = new HashMap<String, List<String>>();
    for (Map.Entry<String, Object> e : nettyRequest.getArgs().entrySet()) {
      if (!e.getKey().equalsIgnoreCase(HttpHeaderNames.COOKIE.toString())) {
        if (!receivedArgs.containsKey(e.getKey())) {
          receivedArgs.put(e.getKey(), new LinkedList<String>());
        }

        if (e.getValue() != null) {
          List<String> values =
              Arrays.asList(e.getValue().toString().split(NettyRequest.MULTIPLE_HEADER_VALUE_DELIMITER));
          receivedArgs.get(e.getKey()).addAll(values);
        }
      }
    }
    Map<String, Integer> keyValueCount = new HashMap<String, Integer>();
    for (Map.Entry<String, List<String>> param : params.entrySet()) {
      boolean containsKey = receivedArgs.containsKey(param.getKey());
      if (BLACKLISTED_QUERY_PARAM_SET.contains(param.getKey())) {
        assertFalse("Should not contain blacklisted key: " + param.getKey(), containsKey);
      } else {
        assertTrue("Did not find key: " + param.getKey(), containsKey);
        if (!keyValueCount.containsKey(param.getKey())) {
          keyValueCount.put(param.getKey(), 0);
        }

        if (param.getValue() != null) {
          boolean containsAllValues = receivedArgs.get(param.getKey()).containsAll(param.getValue());
          assertTrue("Did not find all values expected for key: " + param.getKey(), containsAllValues);
          keyValueCount.put(param.getKey(), keyValueCount.get(param.getKey()) + param.getValue().size());
        }
      }
    }

    for (Map.Entry<String, String> e : headers) {
      if (!e.getKey().equalsIgnoreCase(HttpHeaderNames.COOKIE.toString())) {
        assertTrue("Did not find key: " + e.getKey(), receivedArgs.containsKey(e.getKey()));
        if (!keyValueCount.containsKey(e.getKey())) {
          keyValueCount.put(e.getKey(), 0);
        }
        if (headers.get(e.getKey()) != null) {
          assertTrue("Did not find value '" + e.getValue() + "' expected for key: '" + e.getKey() + "'",
              receivedArgs.get(e.getKey()).contains(e.getValue()));
          keyValueCount.put(e.getKey(), keyValueCount.get(e.getKey()) + 1);
        }
      }
    }

    assertEquals("Number of args does not match", keyValueCount.size(), receivedArgs.size());
    for (Map.Entry<String, Integer> e : keyValueCount.entrySet()) {
      assertEquals("Value count for key " + e.getKey() + " does not match", e.getValue().intValue(),
          receivedArgs.get(e.getKey()).size());
    }

    assertEquals("Auto-read is in an invalid state",
        (!restMethod.equals(RestMethod.POST) && !restMethod.equals(RestMethod.PUT))
            || NettyRequest.bufferWatermark <= 0, channel.config().isAutoRead());
  }

  /**
   * Compares a set of HttpCookies {@link Cookie} with a set of Java Cookies {@link javax.servlet.http.Cookie} for
   * equality in values
   * @param expected Set of {@link Cookie}s to be compared with the {@code actual}
   * @param actual Set of {@link javax.servlet.http.Cookie}s to be compared with those of {@code expected}
   */
  private void compareCookies(Set<Cookie> expected, Set<javax.servlet.http.Cookie> actual) {
    Assert.assertEquals("Size didn't match", expected.size(), actual.size());
    HashMap<String, Cookie> expectedHashMap = new HashMap<String, Cookie>();
    for (Cookie cookie : expected) {
      expectedHashMap.put(cookie.name(), cookie);
    }
    for (javax.servlet.http.Cookie cookie : actual) {
      Assert.assertEquals("Value field didn't match ", expectedHashMap.get(cookie.getName()).value(),
          cookie.getValue());
    }
  }

  // contentAddAndReadTest(), readIntoExceptionsTest() and backPressureTest() helpers

  /**
   * Tests {@link NettyRequest#addContent(HttpContent)} and
   * {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} by creating a {@link NettyRequest}, adding a few
   * pieces of content to it and then reading from it to match the stream with the added content.
   * <p/>
   * The read happens at different points of time w.r.t content addition (before, during, after).
   * @param digestAlgorithm the digest algorithm to use. Can be empty or {@code null} if digest checking is not
   *                        required.
   * @param useCopyForcingByteBuf if {@code true}, uses {@link CopyForcingByteBuf} instead of the default
   *                              {@link ByteBuf}.
   * @param method Http method
   * @throws Exception
   */
  private void contentAddAndReadTest(String digestAlgorithm, boolean useCopyForcingByteBuf, HttpMethod method)
      throws Exception {
    // non composite content
    // start reading before addition of content
    List<HttpContent> httpContents = new ArrayList<>();
    ByteBuffer content = generateContent(httpContents, useCopyForcingByteBuf);
    doContentAddAndReadTest(digestAlgorithm, content, httpContents, 0, method);

    // start reading in the middle of content add
    httpContents.clear();
    content = generateContent(httpContents, useCopyForcingByteBuf);
    doContentAddAndReadTest(digestAlgorithm, content, httpContents, httpContents.size() / 2, method);

    // start reading after all content added
    httpContents.clear();
    content = generateContent(httpContents, useCopyForcingByteBuf);
    doContentAddAndReadTest(digestAlgorithm, content, httpContents, httpContents.size(), method);

    // composite content
    httpContents.clear();
    content = generateCompositeContent(httpContents);
    doContentAddAndReadTest(digestAlgorithm, content, httpContents, 0, method);
  }

  /**
   * Does the content addition and read verification based on the arguments provided.
   * @param digestAlgorithm the digest algorithm to use. Can be empty or {@code null} if digest checking is not
   *                        required.
   * @param content the complete content.
   * @param httpContents {@code content} in parts and as {@link HttpContent}. Should contain all the data in
   * {@code content}.
   * @param numChunksToAddBeforeRead the number of {@link HttpContent} to add before making the
   * {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} call.
   * @param method Http method
   * @throws Exception
   */
  private void doContentAddAndReadTest(String digestAlgorithm, ByteBuffer content, List<HttpContent> httpContents,
      int numChunksToAddBeforeRead, HttpMethod method) throws Exception {
    if (numChunksToAddBeforeRead < 0 || numChunksToAddBeforeRead > httpContents.size()) {
      throw new IllegalArgumentException("Illegal value of numChunksToAddBeforeRead");
    }

    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(method, "/", null, channel);
    byte[] wholeDigest = null;
    if (digestAlgorithm != null && !digestAlgorithm.isEmpty()) {
      MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
      digest.update(content);
      wholeDigest = digest.digest();
      content.rewind();
      nettyRequest.setDigestAlgorithm(digestAlgorithm);
    }

    int bytesToVerify = 0;
    int addedCount = 0;
    for (; addedCount < numChunksToAddBeforeRead; addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      bytesToVerify += httpContent.content().readableBytes();
      nettyRequest.addContent(httpContent);
      // ref count always 2 when added before calling readInto()
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    Future<Long> future = nettyRequest.readInto(writeChannel, callback);
    readAndVerify(bytesToVerify, writeChannel, content);

    bytesToVerify = 0;
    for (; addedCount < httpContents.size(); addedCount++) {
      HttpContent httpContent = httpContents.get(addedCount);
      bytesToVerify += httpContent.content().readableBytes();
      nettyRequest.addContent(httpContent);
      assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
    }
    readAndVerify(bytesToVerify, writeChannel, content);
    verifyRefCnts(httpContents);
    writeChannel.close();
    callback.awaitCallback();
    if (callback.exception != null) {
      throw callback.exception;
    }
    long futureBytesRead = future.get();
    assertEquals("Total bytes read does not match (callback)", content.limit(), callback.bytesRead);
    assertEquals("Total bytes read does not match (future)", content.limit(), futureBytesRead);
    assertEquals("nettyRequest.getBytesReceived() does not match expected", content.limit(),
        nettyRequest.getBytesReceived());

    // check twice to make sure the same digest is returned every time
    for (int i = 0; i < 2; i++) {
      assertArrayEquals("Part by part digest should match digest of whole", wholeDigest, nettyRequest.getDigest());
    }
    closeRequestAndValidate(nettyRequest, channel);
  }

  /**
   * Tests backpressure support in {@link NettyRequest} for different values of {@link NettyRequest#bufferWatermark}.
   * @param digestAlgorithm the digest algorithm to use. Can be empty or {@code null} if digest checking is not
   *                        required.
   * @param useCopyForcingByteBuf if {@code true}, uses {@link CopyForcingByteBuf} instead of the default
   *                              {@link ByteBuf}.
   * @param method Http method
   * @throws Exception
   */
  private void backPressureTest(String digestAlgorithm, boolean useCopyForcingByteBuf, HttpMethod method)
      throws Exception {
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    byte[] contentBytes = TestUtils.getRandomBytes(GENERATED_CONTENT_SIZE);
    ByteBuffer content = ByteBuffer.wrap(contentBytes);
    splitContent(contentBytes, GENERATED_CONTENT_PART_COUNT, httpContents, useCopyForcingByteBuf);
    int chunkSize = httpContents.get(0).content().readableBytes();
    int[] bufferWatermarks =
        {1, chunkSize - 1, chunkSize, chunkSize + 1, chunkSize * httpContents.size() / 2, content.limit() - 1,
            content.limit(), content.limit() + 1};
    for (int bufferWatermark : bufferWatermarks) {
      NettyRequest.bufferWatermark = bufferWatermark;
      // start reading before addition of content
      httpContents.clear();
      content.rewind();
      splitContent(contentBytes, GENERATED_CONTENT_PART_COUNT, httpContents, useCopyForcingByteBuf);
      doBackPressureTest(digestAlgorithm, content, httpContents, 0, method);
      // start reading in the middle of content add
      httpContents.clear();
      content.rewind();
      splitContent(contentBytes, GENERATED_CONTENT_PART_COUNT, httpContents, useCopyForcingByteBuf);
      doBackPressureTest(digestAlgorithm, content, httpContents, httpContents.size() / 2, method);
      // start reading after all content added
      httpContents.clear();
      content.rewind();
      splitContent(contentBytes, GENERATED_CONTENT_PART_COUNT, httpContents, useCopyForcingByteBuf);
      doBackPressureTest(digestAlgorithm, content, httpContents, httpContents.size(), method);
    }
  }

  /**
   * Does the backpressure test by ensuring that {@link Channel#read()} isn't called when the number of bytes buffered
   * is above the {@link NettyRequest#bufferWatermark}. Also ensures that {@link Channel#read()} is called correctly
   * when the number of buffered bytes falls below the {@link NettyRequest#bufferWatermark}.
   * @param digestAlgorithm the digest algorithm to use. Can be empty or {@code null} if digest checking is not
   *                        required.
   * @param content the complete content.
   * @param httpContents {@code content} in parts and as {@link HttpContent}. Should contain all the data in
   * {@code content}.
   * @param numChunksToAddBeforeRead the number of {@link HttpContent} to add before making the
   * {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} call.
   * @param method Http Method
   * @throws Exception
   */
  private void doBackPressureTest(String digestAlgorithm, ByteBuffer content, List<HttpContent> httpContents,
      int numChunksToAddBeforeRead, HttpMethod method) throws Exception {
    if (numChunksToAddBeforeRead < 0 || numChunksToAddBeforeRead > httpContents.size()) {
      throw new IllegalArgumentException("Illegal value of numChunksToAddBeforeRead");
    }

    MockChannel channel = new MockChannel();
    final NettyRequest nettyRequest = createNettyRequest(method, "/", null, channel);
    byte[] wholeDigest = null;
    if (digestAlgorithm != null && !digestAlgorithm.isEmpty()) {
      MessageDigest digest = MessageDigest.getInstance(digestAlgorithm);
      digest.update(content);
      wholeDigest = digest.digest();
      content.rewind();
      nettyRequest.setDigestAlgorithm(digestAlgorithm);
    }

    final AtomicInteger queuedReads = new AtomicInteger(0);
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();

    channel.setChannelReadCallback(new MockChannel.ChannelReadCallback() {
      @Override
      public void onRead() {
        queuedReads.incrementAndGet();
      }
    });

    int addedCount = 0;
    Future<Long> future = null;
    boolean suspended = false;
    int bytesToVerify = 0;
    while (addedCount < httpContents.size()) {
      if (suspended) {
        assertEquals("There should have been no reads queued when over buffer watermark", 0, queuedReads.get());
        if (future == null) {
          future = nettyRequest.readInto(writeChannel, callback);
        }
        int chunksRead = readAndVerify(bytesToVerify, writeChannel, content);
        assertEquals("Number of reads triggered is not as expected", chunksRead, queuedReads.get());
        // collapse many reads into one
        queuedReads.set(1);
        bytesToVerify = 0;
        suspended = false;
      } else {
        assertEquals("There should have been only one read queued", 1, queuedReads.get());
        queuedReads.set(0);
        if (future == null && addedCount == numChunksToAddBeforeRead) {
          future = nettyRequest.readInto(writeChannel, callback);
        }
        final HttpContent httpContent = httpContents.get(addedCount);
        bytesToVerify += (httpContent.content().readableBytes());
        suspended = bytesToVerify >= NettyRequest.bufferWatermark;
        addedCount++;
        nettyRequest.addContent(httpContent);
        assertEquals("Reference count is not as expected", 2, httpContent.refCnt());
      }
    }

    if (future == null) {
      future = nettyRequest.readInto(writeChannel, callback);
    }
    readAndVerify(bytesToVerify, writeChannel, content);
    verifyRefCnts(httpContents);
    writeChannel.close();
    callback.awaitCallback();
    if (callback.exception != null) {
      throw callback.exception;
    }
    long futureBytesRead = future.get(1, TimeUnit.SECONDS);
    assertEquals("Total bytes read does not match (callback)", content.limit(), callback.bytesRead);
    assertEquals("Total bytes read does not match (future)", content.limit(), futureBytesRead);

    // check twice to make sure the same digest is returned every time
    for (int i = 0; i < 2; i++) {
      assertArrayEquals("Part by part digest should match digest of whole", wholeDigest, nettyRequest.getDigest());
    }
    closeRequestAndValidate(nettyRequest, channel);
  }

  /**
   * Generates random content and fills it up (in parts) in {@code httpContents}.
   * @param httpContents the {@link List<HttpContent>} that will contain all the content in parts.
   * @return the whole content as a {@link ByteBuffer} - serves as a source of truth.
   */
  private ByteBuffer generateContent(List<HttpContent> httpContents) {
    return generateContent(httpContents, false);
  }

  /**
   * Generates random content and fills it up (in parts) in {@code httpContents}.
   * @param httpContents the {@link List<HttpContent>} that will contain all the content in parts.
   * @param useCopyForcingByteBuf if {@code true}, uses {@link CopyForcingByteBuf} instead of the default
   *                              {@link ByteBuf}.
   * @return the whole content as a {@link ByteBuffer} - serves as a source of truth.
   */
  private ByteBuffer generateContent(List<HttpContent> httpContents, boolean useCopyForcingByteBuf) {
    byte[] contentBytes = TestUtils.getRandomBytes(GENERATED_CONTENT_SIZE);
    splitContent(contentBytes, GENERATED_CONTENT_PART_COUNT, httpContents, useCopyForcingByteBuf);
    return ByteBuffer.wrap(contentBytes);
  }

  /**
   * Splits the given {@code contentBytes} into {@code numChunks} chunks and stores them in {@code httpContents}.
   * @param contentBytes the content that needs to be split.
   * @param numChunks the number of chunks to split {@code contentBytes} into.
   * @param httpContents the {@link List<HttpContent>} that will contain all the content in parts.
   * @param useCopyForcingByteBuf if {@code true}, uses {@link CopyForcingByteBuf} instead of the default
   *                              {@link ByteBuf}.
   */
  private void splitContent(byte[] contentBytes, int numChunks, List<HttpContent> httpContents,
      boolean useCopyForcingByteBuf) {
    int individualPartSize = contentBytes.length / numChunks;
    ByteBuf content;
    for (int addedContentCount = 0; addedContentCount < numChunks - 1; addedContentCount++) {
      if (useCopyForcingByteBuf) {
        content =
            CopyForcingByteBuf.wrappedBuffer(contentBytes, addedContentCount * individualPartSize, individualPartSize);
      } else {
        content = Unpooled.wrappedBuffer(contentBytes, addedContentCount * individualPartSize, individualPartSize);
      }
      httpContents.add(new DefaultHttpContent(content));
    }
    if (useCopyForcingByteBuf) {
      content =
          CopyForcingByteBuf.wrappedBuffer(contentBytes, (numChunks - 1) * individualPartSize, individualPartSize);
    } else {
      content = Unpooled.wrappedBuffer(contentBytes, (numChunks - 1) * individualPartSize, individualPartSize);
    }
    httpContents.add(new DefaultLastHttpContent(content));
  }

  /**
   * Generates random content and fills it up in {@code httpContents} with a backing {@link CompositeByteBuf}.
   * @param httpContents the {@link List<HttpContent>} that will contain all the content.
   * @return the whole content as a {@link ByteBuffer} - serves as a source of truth.
   */
  private ByteBuffer generateCompositeContent(List<HttpContent> httpContents) {
    int individualPartSize = GENERATED_CONTENT_SIZE / GENERATED_CONTENT_PART_COUNT;
    byte[] contentBytes = TestUtils.getRandomBytes(GENERATED_CONTENT_SIZE);
    ArrayList<ByteBuf> byteBufs = new ArrayList<>(GENERATED_CONTENT_PART_COUNT);
    for (int addedContentCount = 0; addedContentCount < GENERATED_CONTENT_PART_COUNT; addedContentCount++) {
      byteBufs.add(Unpooled.wrappedBuffer(contentBytes, addedContentCount * individualPartSize, individualPartSize));
    }
    httpContents.add(new DefaultLastHttpContent(new CompositeByteBuf(ByteBufAllocator.DEFAULT, false, 20, byteBufs)));
    return ByteBuffer.wrap(contentBytes);
  }

  /**
   * Verifies that the reference counts of {@code httpContents} is undisturbed after all operations.
   * @param httpContents the {@link List<HttpContent>} of contents whose reference counts need to checked.
   */
  private void verifyRefCnts(List<HttpContent> httpContents) {
    for (HttpContent httpContent : httpContents) {
      assertEquals("Reference count of http content has changed", 1, httpContent.refCnt());
    }
  }

  /**
   * Reads from the provided {@code writeChannel} and verifies that the bytes received match the original content
   * provided through {@code content}.
   * @param readLengthDesired desired length of bytes to read.
   * @param writeChannel the {@link ByteBufferAsyncWritableChannel} to read from.
   * @param content the original content that serves as the source of truth.
   * @return the number of chunks read.
   * @throws InterruptedException
   */
  private int readAndVerify(int readLengthDesired, ByteBufferAsyncWritableChannel writeChannel, ByteBuffer content)
      throws InterruptedException {
    int bytesRead = 0;
    int chunksRead = 0;
    while (bytesRead < readLengthDesired) {
      ByteBuffer recvdContent = writeChannel.getNextChunk();
      while (recvdContent.hasRemaining()) {
        assertEquals("Unexpected byte", content.get(), recvdContent.get());
        bytesRead++;
      }
      writeChannel.resolveOldestChunk(null);
      chunksRead++;
    }
    return chunksRead;
  }

  // headerAndContentSizeMismatchTest() helpers

  /**
   * Tests reaction of NettyRequest when content size is less than the size specified in the headers.
   * @throws Exception
   */
  private void sizeInHeaderMoreThanContentTest() throws Exception {
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    ByteBuffer content = generateContent(httpContents);
    HttpHeaders httpHeaders = new DefaultHttpHeaders();
    httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, content.limit() + 1);
    doHeaderAndContentSizeMismatchTest(httpHeaders, httpContents);
  }

  /**
   * Tests reaction of NettyRequest when content size is more than the size specified in the headers.
   * @throws Exception
   */
  private void sizeInHeaderLessThanContentTest() throws Exception {
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    ByteBuffer content = generateContent(httpContents);
    HttpHeaders httpHeaders = new DefaultHttpHeaders();
    int lastHttpContentSize = httpContents.get(httpContents.size() - 1).content().readableBytes();
    httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, content.limit() - lastHttpContentSize - 1);
    doHeaderAndContentSizeMismatchTest(httpHeaders, httpContents);
  }

  /**
   * Tests reaction of NettyRequest when content size is different from the size specified in the headers.
   * @param httpHeaders {@link HttpHeaders} that need to be a part of the request.
   * @param httpContents the {@link List<HttpContent>} that needs to be added to {@code nettyRequest}.
   * @throws Exception
   */
  private void doHeaderAndContentSizeMismatchTest(HttpHeaders httpHeaders, List<HttpContent> httpContents)
      throws Exception {
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", httpHeaders, channel);
    AsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    Future<Long> future = nettyRequest.readInto(writeChannel, callback);

    int bytesAdded = 0;
    HttpContent httpContentToAdd = null;
    for (HttpContent httpContent : httpContents) {
      httpContentToAdd = httpContent;
      int contentBytes = httpContentToAdd.content().readableBytes();
      if (!(httpContentToAdd instanceof LastHttpContent) && (bytesAdded + contentBytes <= nettyRequest.getSize())) {
        nettyRequest.addContent(httpContentToAdd);
        assertEquals("Reference count is not as expected", 2, httpContentToAdd.refCnt());
        bytesAdded += contentBytes;
      } else {
        break;
      }
    }

    // the addition of the next content should throw an exception.
    try {
      nettyRequest.addContent(httpContentToAdd);
      fail("Adding content should have failed because there was a mismatch in size");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadRequest, e.getErrorCode());
    }
    closeRequestAndValidate(nettyRequest, channel);
    writeChannel.close();
    verifyRefCnts(httpContents);
    callback.awaitCallback();
    assertNotNull("There should be a RestServiceException in the callback", callback.exception);
    assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadRequest,
        ((RestServiceException) callback.exception).getErrorCode());
    try {
      future.get();
      fail("Should have thrown exception because the future is expected to have been given one");
    } catch (ExecutionException e) {
      RestServiceException restServiceException = (RestServiceException) Utils.getRootCause(e);
      assertNotNull("There should be a RestServiceException in the future", restServiceException);
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadRequest,
          restServiceException.getErrorCode());
    }
  }

  // digestIncorrectUsageTest() helpers.

  /**
   * Tests for failure when {@link NettyRequest#setDigestAlgorithm(String)} after
   * {@link NettyRequest#readInto(AsyncWritableChannel, Callback)} is called.
   * @throws NoSuchAlgorithmException
   * @throws RestServiceException
   */
  private void setDigestAfterReadTest() throws NoSuchAlgorithmException, RestServiceException {
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    nettyRequest.readInto(writeChannel, callback);

    try {
      nettyRequest.setDigestAlgorithm("MD5");
      fail("Setting a digest algorithm should have failed because readInto() has already been called");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    writeChannel.close();
    closeRequestAndValidate(nettyRequest, channel);
  }

  /**
   * Tests for failure when {@link NettyRequest#setDigestAlgorithm(String)} is called with an unrecognized algorithm.
   * @throws RestServiceException
   */
  private void setBadAlgorithmTest() throws RestServiceException {
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    try {
      nettyRequest.setDigestAlgorithm("NonExistentAlgorithm");
      fail("Setting a digest algorithm should have failed because the algorithm isn't valid");
    } catch (NoSuchAlgorithmException e) {
      // expected. Nothing to do.
    }
    closeRequestAndValidate(nettyRequest, channel);
  }

  /**
   * Tests for failure when {@link NettyRequest#getDigest()} is called without a call to
   * {@link NettyRequest#setDigestAlgorithm(String)}.
   * @throws RestServiceException
   */
  private void getDigestWithoutSettingAlgorithmTest() throws RestServiceException {
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    nettyRequest.readInto(writeChannel, callback);

    for (HttpContent httpContent : httpContents) {
      nettyRequest.addContent(httpContent);
    }
    assertNull("Digest should be null because no digest algorithm was set", nettyRequest.getDigest());
    closeRequestAndValidate(nettyRequest, channel);
  }

  /**
   * Tests for failure when {@link NettyRequest#getDigest()} is called before
   * 1. All content is added.
   * 2. All content is processed (i.e. before a call to {@link NettyRequest#readInto(AsyncWritableChannel, Callback)}).
   * @throws NoSuchAlgorithmException
   * @throws RestServiceException
   */
  private void getDigestBeforeAllContentProcessedTest() throws NoSuchAlgorithmException, RestServiceException {
    // all content not added test.
    List<HttpContent> httpContents = new ArrayList<HttpContent>();
    generateContent(httpContents);
    Channel channel = new MockChannel();
    NettyRequest nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    nettyRequest.setDigestAlgorithm("MD5");

    // add all except the LastHttpContent
    for (int i = 0; i < httpContents.size() - 1; i++) {
      nettyRequest.addContent(httpContents.get(i));
    }

    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    nettyRequest.readInto(writeChannel, callback);
    try {
      nettyRequest.getDigest();
      fail("Getting a digest should have failed because all the content has not been added");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
    closeRequestAndValidate(nettyRequest, channel);

    // content not processed test.
    httpContents.clear();
    generateContent(httpContents);
    nettyRequest = createNettyRequest(HttpMethod.POST, "/", null, channel);
    nettyRequest.setDigestAlgorithm("MD5");

    for (HttpContent httpContent : httpContents) {
      nettyRequest.addContent(httpContent);
    }
    try {
      nettyRequest.getDigest();
      fail("Getting a digest should have failed because the content has not been processed (readInto() not called)");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
    closeRequestAndValidate(nettyRequest, channel);
  }
}

/**
 * Callback for all read operations on {@link NettyRequest}.
 */
class ReadIntoCallback implements Callback<Long> {
  public volatile long bytesRead;
  public volatile Exception exception;
  private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
  private final CountDownLatch latch = new CountDownLatch(1);

  @Override
  public void onCompletion(Long result, Exception exception) {
    if (callbackInvoked.compareAndSet(false, true)) {
      bytesRead = result;
      this.exception = exception;
      latch.countDown();
    } else {
      this.exception = new IllegalStateException("Callback invoked more than once");
    }
  }

  /**
   * Waits for the callback to be received.
   * @throws InterruptedException if there was any intteruption while waiting for the callback.
   * @throws TimeoutException if the callback did not arrive within a particular timeout.
   */
  public void awaitCallback() throws InterruptedException, TimeoutException {
    if (!latch.await(1, TimeUnit.SECONDS)) {
      throw new TimeoutException("Timed out waiting for callback to trigger");
    }
  }
}

/**
 * Used to test for {@link NettyRequest} behavior when a {@link AsyncWritableChannel} throws exceptions.
 */
class BadAsyncWritableChannel implements AsyncWritableChannel {
  private final Exception exceptionToThrow;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);
  private Callback<Long> runtimeExceptionCallback = null;

  /**
   * Creates an instance of BadAsyncWritableChannel that throws {@code exceptionToThrow} on write.
   * @param exceptionToThrow the {@link Exception} to throw on write.
   */
  public BadAsyncWritableChannel(Exception exceptionToThrow) {
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if (exceptionToThrow instanceof RuntimeException) {
      runtimeExceptionCallback = callback;
      throw (RuntimeException) exceptionToThrow;
    } else {
      return markFutureInvokeCallback(callback, 0, exceptionToThrow);
    }
  }

  @Override
  public boolean isOpen() {
    return isOpen.get();
  }

  @Override
  public void close() throws IOException {
    isOpen.set(false);
    if (runtimeExceptionCallback != null) {
      runtimeExceptionCallback.onCompletion((long) 0, exceptionToThrow);
    }
  }

  /**
   * Creates and marks a future as done and invoked the callback with paramaters {@code totalBytesWritten} and
   * {@code Exception}.
   * @param callback the {@link Callback} to invoke.
   * @param totalBytesWritten the number of bytes successfully written.
   * @param exception the {@link Exception} that occurred if any.
   * @return the {@link Future} that will contain the result of the operation.
   */
  private Future<Long> markFutureInvokeCallback(Callback<Long> callback, long totalBytesWritten, Exception exception) {
    FutureResult<Long> futureResult = new FutureResult<Long>();
    futureResult.done(totalBytesWritten, exception);
    if (callback != null) {
      callback.onCompletion(totalBytesWritten, exception);
    }
    return futureResult;
  }
}

/**
 * A mock channel that can be configured to run custom code on {@link Channel#read()}.
 */
class MockChannel extends EmbeddedChannel {
  /**
   * Interface to provide code that is to be executed on {@link Channel#read()}.
   */
  public interface ChannelReadCallback {
    /**
     * This is called when {@link Channel#read()} is called. Should contain logic that needs to be executed on read.
     */
    void onRead();
  }

  private final ChannelConfig config = new DefaultChannelConfig(this);
  private SSLEngine sslEngine = null;
  private ChannelReadCallback channelReadCallback = null;
  private int queuedOnReads = 0;

  MockChannel() {
    // sending a placeholder handler to avoid NPE. This is of no consequence and saves us from having to implement
    // needless functions.
    super(new ConnectionStatsHandler(new NettyMetrics(new MetricRegistry())));
  }

  /**
   * Add an {@link SslHandler} to the pipeline (for testing {@link NettyRequest#getSSLSession()}.
   * @throws SSLException
   * @throws CertificateException
   */
  MockChannel addSslHandlerToPipeline() throws SSLException, CertificateException {
    if (pipeline().get(SslHandler.class) == null) {
      SelfSignedCertificate ssc = new SelfSignedCertificate();
      SslContext sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
      sslEngine = sslCtx.newEngine(alloc());
      pipeline().addFirst(new SslHandler(sslEngine));
    }
    return this;
  }

  /**
   * @return the {@link SSLEngine} associated with this channel, or {@code null} if no {@link SslHandler} is on this
   *         pipeline.
   */
  SSLEngine getSSLEngine() {
    return sslEngine;
  }

  /**
   * Sets the {@link ChannelReadCallback}.
   * @param channelReadCallback the {@link ChannelReadCallback} that will executed on {@link #read()}.
   */
  void setChannelReadCallback(ChannelReadCallback channelReadCallback) {
    this.channelReadCallback = channelReadCallback;
    for (; queuedOnReads > 0; queuedOnReads--) {
      channelReadCallback.onRead();
    }
  }

  @Override
  public ChannelConfig config() {
    return config;
  }

  @Override
  public Channel read() {
    if (channelReadCallback != null) {
      channelReadCallback.onRead();
    } else {
      queuedOnReads++;
    }
    return this;
  }
}

/**
 * An implementation of {@link ByteBuf} that forces {@link NettyRequest} to make a copy of the data.
 */
class CopyForcingByteBuf extends UnpooledHeapByteBuf {
  private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;

  /**
   * Returns a {@link ByteBuf} that will not expose the underlying buffer through {@link #nioBuffer()} if {@code length}
   * is greater than 0.
   * @param array the backing byte array.
   * @param offset the offset in the array from which the data is valid.
   * @param length the length of data in the array from the {@code offset} which is valid.
   * @return a {@link ByteBuf} that will not expose the underlying buffer through {@link #nioBuffer()} if {@code length}
   * is greater than 0.
   */
  public static ByteBuf wrappedBuffer(byte[] array, int offset, int length) {
    if (length == 0) {
      return Unpooled.EMPTY_BUFFER;
    }
    return new CopyForcingByteBuf(ALLOC, array, array.length).slice(offset, length);
  }

  protected CopyForcingByteBuf(ByteBufAllocator alloc, byte[] initialArray, int maxCapacity) {
    super(alloc, initialArray, maxCapacity);
  }

  @Override
  public int nioBufferCount() {
    return -1;
  }
}
