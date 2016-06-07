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
import com.github.ambry.router.Callback;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyResponseChannel}.
 * <p/>
 * To understand what each {@link TestingUri} is doing, refer to
 * {@link MockNettyMessageProcessor#handleRequest(HttpRequest)} and
 * {@link MockNettyMessageProcessor#handleContent(HttpContent)}
 */
public class NettyResponseChannelTest {
  private static final Map<RestServiceErrorCode, HttpResponseStatus> REST_ERROR_CODE_TO_HTTP_STATUS = new HashMap<>();

  static {
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.BadRequest, HttpResponseStatus.BAD_REQUEST);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.Unauthorized, HttpResponseStatus.UNAUTHORIZED);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.Deleted, HttpResponseStatus.GONE);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.NotFound, HttpResponseStatus.NOT_FOUND);
    REST_ERROR_CODE_TO_HTTP_STATUS
        .put(RestServiceErrorCode.ResourceScanInProgress, HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.ResourceDirty, HttpResponseStatus.FORBIDDEN);
    REST_ERROR_CODE_TO_HTTP_STATUS
        .put(RestServiceErrorCode.InternalServerError, HttpResponseStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * Tests the common workflow of the {@link NettyResponseChannel} i.e., add some content to response body via
   * {@link NettyResponseChannel#write(ByteBuffer, Callback)} and then completes the response.
   * <p/>
   * For a description of what different URIs do, check {@link TestingUri}. For the actual functionality, check
   * {@link MockNettyMessageProcessor}).
   * @throws IOException
   */
  @Test
  public void commonCaseTest()
      throws IOException {
    String content = "@@randomContent@@@";
    String lastContent = "@@randomLastContent@@@";
    EmbeddedChannel channel = createEmbeddedChannel();
    AtomicLong requestIdGenerator = new AtomicLong(0);

    final int ITERATIONS = 5;
    for (int i = 0; i < 5; i++) {
      boolean isKeepAlive = i != (ITERATIONS - 1);
      String contentToSend = content + requestIdGenerator.getAndIncrement();
      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", null);
      HttpHeaders.setKeepAlive(httpRequest, isKeepAlive);
      channel.writeInbound(httpRequest);
      channel.writeInbound(createContent(contentToSend, false));
      channel.writeInbound(createContent(lastContent, true));
      // first outbound has to be response.
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
      // content echoed back.
      String returnedContent = RestTestUtils.getContentString((HttpContent) channel.readOutbound());
      assertEquals("Content does not match with expected content", contentToSend, returnedContent);
      // last content echoed back.
      returnedContent = RestTestUtils.getContentString((HttpContent) channel.readOutbound());
      assertEquals("Content does not match with expected content", lastContent, returnedContent);
      assertTrue("Did not receive end marker", channel.readOutbound() instanceof LastHttpContent);
      assertEquals("Unexpected channel state on the server", isKeepAlive, channel.isActive());
    }
  }

  /**
   * Checks the case where no body needs to be returned but just a
   * {@link RestResponseChannel#onResponseComplete(Exception)} is called on the server. This should return just
   * response metadata.
   */
  @Test
  public void noResponseBodyTest() {
    EmbeddedChannel channel = createEmbeddedChannel();
    HttpRequest httpRequest =
        RestTestUtils.createRequest(HttpMethod.GET, TestingUri.ImmediateResponseComplete.toString(), null);
    HttpHeaders.setKeepAlive(httpRequest, false);
    channel.writeInbound(httpRequest);
    // There should be a response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    // Channel should be closed.
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Performs bad state transitions and verifies that they throw the right exceptions.
   * @throws Exception
   */
  @Test
  public void badStateTransitionsTest()
      throws Exception {
    // write after close.
    doBadStateTransitionTest(TestingUri.WriteAfterClose, ClosedChannelException.class);

    // modify response data after it has been written to the channel
    doBadStateTransitionTest(TestingUri.ModifyResponseMetadataAfterWrite, IllegalStateException.class);
  }

  /**
   * Tests that no exceptions are thrown on repeating idempotent operations. Does <b><i>not</i></b> currently test that
   * state changes are idempotent.
   */
  @Test
  public void idempotentOperationsTest() {
    doIdempotentOperationsTest(TestingUri.MultipleClose);
    doIdempotentOperationsTest(TestingUri.MultipleOnResponseComplete);
  }

  /**
   * Tests behaviour of various functions of {@link NettyResponseChannel} under write failures.
   * @throws Exception
   */
  @Test
  public void behaviourUnderWriteFailuresTest()
      throws Exception {
    onResponseCompleteUnderWriteFailureTest(TestingUri.ImmediateResponseComplete);
    onResponseCompleteUnderWriteFailureTest(TestingUri.OnResponseCompleteWithNonRestException);

    // writing to channel with a outbound handler that generates an Exception
    try {
      String content = "@@randomContent@@@";
      MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
      ChannelOutboundHandler badOutboundHandler = new ExceptionOutboundHandler();
      EmbeddedChannel channel = new EmbeddedChannel(badOutboundHandler, processor);
      channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, "/", null));
      // channel gets closed because of write failure
      channel.writeInbound(createContent(content, true));
      fail("Callback for write would have thrown an Exception");
    } catch (Exception e) {
      assertEquals("Exception not as expected", ExceptionOutboundHandler.EXCEPTION_MESSAGE, e.getMessage());
    }

    // writing to channel with a outbound handler that generates an Error
    EmbeddedChannel channel = new EmbeddedChannel(new ErrorOutboundHandler(), new MockNettyMessageProcessor());
    try {
      channel.writeInbound(
          RestTestUtils.createRequest(HttpMethod.GET, TestingUri.WriteFailureWithThrowable.toString(), null));
    } catch (Error e) {
      assertEquals("Unexpected error", ErrorOutboundHandler.ERROR_MESSAGE, e.getMessage());
    }

    channel = createEmbeddedChannel();
    channel
        .writeInbound(RestTestUtils.createRequest(HttpMethod.GET, TestingUri.ResponseFailureMidway.toString(), null));
    assertFalse("Channel is not closed at the remote end", channel.isActive());
  }

  /**
   * Tests handling of content that is larger than write buffer size. In this test case, the write buffer low and high
   * watermarks are requested to be set to 1 and 2 respectively so the content will be written byte by byte into the
   * {@link NettyResponseChannel}. This does <b><i>not</i></b> test for the same situation in a async scenario since
   * {@link EmbeddedChannel} only provides blocking semantics.
   * @throws IOException
   */
  @Test
  public void fillWriteBufferTest()
      throws IOException {
    String content = "@@randomContent@@@";
    String lastContent = "@@randomLastContent@@@";
    EmbeddedChannel channel = createEmbeddedChannel();
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.GET, TestingUri.FillWriteBuffer.toString(), null);
    HttpHeaders.setKeepAlive(httpRequest, false);
    channel.writeInbound(httpRequest);
    channel.writeInbound(createContent(content, false));
    channel.writeInbound(createContent(lastContent, true));

    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    // content echoed back.
    StringBuilder returnedContent = new StringBuilder();
    while (returnedContent.length() < content.length()) {
      returnedContent.append(RestTestUtils.getContentString((HttpContent) channel.readOutbound()));
    }
    assertEquals("Content does not match with expected content", content, returnedContent.toString());
    // last content echoed back.
    StringBuilder returnedLastContent = new StringBuilder();
    while (returnedLastContent.length() < lastContent.length()) {
      returnedLastContent.append(RestTestUtils.getContentString((HttpContent) channel.readOutbound()));
    }
    assertEquals("Content does not match with expected content", lastContent, returnedLastContent.toString());
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Sends a request with certain headers that will copied into the response. Checks the response for those headers to
   * see that values match.
   * @throws ParseException
   */
  @Test
  public void headersPresenceTest()
      throws ParseException {
    HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.CopyHeaders.toString());
    HttpHeaders.setKeepAlive(request, false);
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(request);

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertFalse("Channel not closed on the server", channel.isActive());

    checkHeaders(request, response);
  }

  /**
   * Sends null input to {@link NettyResponseChannel#setHeader(String, Object)} (through
   * {@link MockNettyMessageProcessor}) and tests for reaction.
   */
  @Test
  public void nullHeadersSetTest() {
    HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.SetNullHeader.toString());
    HttpHeaders.setKeepAlive(request, false);
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(request);

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.getStatus());
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Tries different exception scenarios for {@link NettyResponseChannel#setRequest(NettyRequest)}.
   */
  @Test
  public void setRequestTest() {
    HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.SetRequest.toString());
    HttpHeaders.setKeepAlive(request, false);
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(request);

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.getStatus());
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Tests setting of different available {@link ResponseStatus} codes and sees that they are recognized and converted
   * in {@link NettyResponseChannel}.
   * <p/>
   * If this test fails, a case for conversion probably needs to be added in {@link NettyResponseChannel}.
   */
  @Test
  public void setStatusTest() {
    // ask for every status to be set
    for (ResponseStatus expectedResponseStatus : ResponseStatus.values()) {
      HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.SetStatus.toString());
      HttpHeaders.setHeader(request, MockNettyMessageProcessor.STATUS_HEADER_NAME, expectedResponseStatus);
      HttpHeaders.setKeepAlive(request, false);
      EmbeddedChannel channel = createEmbeddedChannel();
      channel.writeInbound(request);

      // pull but discard response
      channel.readOutbound();
      assertFalse("Channel not closed on the server", channel.isActive());
    }
    // check if all the ResponseStatus codes were recognized.
    String metricName = MetricRegistry.name(NettyResponseChannel.class, "UnknownResponseStatusCount");
    long metricCount = MockNettyMessageProcessor.METRIC_REGISTRY.getCounters().get(metricName).getCount();
    assertEquals("Some of the ResponseStatus codes were not recognized", 0, metricCount);
  }

  /**
   * Tests that HEAD returns no body in error responses.
   */
  @Test
  public void noBodyForHeadTest() {
    EmbeddedChannel channel = createEmbeddedChannel();
    for (Map.Entry<RestServiceErrorCode, HttpResponseStatus> entry : REST_ERROR_CODE_TO_HTTP_STATUS.entrySet()) {
      HttpHeaders httpHeaders = new DefaultHttpHeaders();
      httpHeaders.set(MockNettyMessageProcessor.REST_SERVICE_ERROR_CODE_HEADER_NAME, entry.getKey());
      channel.writeInbound(RestTestUtils
          .createRequest(HttpMethod.HEAD, TestingUri.OnResponseCompleteWithRestException.toString(), httpHeaders));
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", entry.getValue(), response.getStatus());
      if (response instanceof FullHttpResponse) {
        // assert that there is no content
        assertEquals("The response should not contain content", 0,
            ((FullHttpResponse) response).content().readableBytes());
      } else {
        HttpContent content = (HttpContent) channel.readOutbound();
        assertTrue("End marker should be received", content instanceof LastHttpContent);
      }
      assertNull("There should be no more data in the channel", channel.readOutbound());
      boolean shouldBeAlive = !NettyResponseChannel.CLOSE_CONNECTION_ERROR_STATUSES.contains(entry.getValue());
      assertEquals("Channel state (open/close) not as expected", shouldBeAlive, channel.isActive());
      assertEquals("Connection header should be consistent with channel state", shouldBeAlive,
          HttpHeaders.isKeepAlive(response));
      if (!shouldBeAlive) {
        channel = createEmbeddedChannel();
      }
    }
    channel.close();
  }

  /**
   * Tests keep-alive for different HTTP methods and error statuses.
   */
  @Test
  public void keepAliveTest() {
    HttpMethod[] HTTP_METHODS = {HttpMethod.POST, HttpMethod.GET, HttpMethod.HEAD, HttpMethod.DELETE};
    EmbeddedChannel channel = createEmbeddedChannel();
    for (HttpMethod httpMethod : HTTP_METHODS) {
      for (Map.Entry<RestServiceErrorCode, HttpResponseStatus> entry : REST_ERROR_CODE_TO_HTTP_STATUS.entrySet()) {
        channel = doKeepAliveTest(channel, httpMethod, entry.getKey(), entry.getValue());
      }
      channel = doKeepAliveTest(channel, httpMethod, null, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
    channel.close();
  }

  /**
   * Tests that the underlying network channel is closed when {@link NettyResponseChannel#close()} is called.
   */
  @Test
  public void closeTest() {
    // request is keep-alive by default.
    HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.Close.toString());
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(request);

    // drain the channel of content.
    while (channel.readOutbound() != null) {
    }
    assertFalse("Channel should be closed", channel.isOpen());
  }

  // helpers
  // general

  /**
   * Creates {@link HttpContent} wrapping the {@code content}.
   * @param content the content to wrap.
   * @param isLast {@code true} if this is the last piece of content. {@code false} otherwise.
   * @return a {@link HttpContent} wrapping the {@code content}.
   */
  private HttpContent createContent(String content, boolean isLast) {
    ByteBuf buf = Unpooled.copiedBuffer(content.getBytes());
    if (isLast) {
      return new DefaultLastHttpContent(buf);
    } else {
      return new DefaultHttpContent(buf);
    }
  }

  /**
   * Creates a new {@link EmbeddedChannel} with a {@link ChunkedWriteHandler} and {@link MockNettyMessageProcessor} in
   * the pipeline.
   * @return the created {@link EmbeddedChannel}.
   */
  private EmbeddedChannel createEmbeddedChannel() {
    ChunkedWriteHandler chunkedWriteHandler = new ChunkedWriteHandler();
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    return new EmbeddedChannel(chunkedWriteHandler, processor);
  }

  // badStateTransitionsTest() helpers

  /**
   * Creates a channel and sends the request to the {@link EmbeddedChannel}. Checks for an exception and verifies the
   * exception class matches. If {@code exceptionClass} is {@link RestServiceException}, then checks the provided
   * {@link RestServiceErrorCode}.
   * @param uri the uri to hit.
   * @param exceptionClass the class of the exception expected.
   * @throws Exception
   */
  private void doBadStateTransitionTest(TestingUri uri, Class exceptionClass)
      throws Exception {
    EmbeddedChannel channel = createEmbeddedChannel();
    try {
      channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, uri.toString(), null));
      fail("This test was expecting the handler in the channel to throw an exception");
    } catch (Exception e) {
      if (!exceptionClass.isInstance(e)) {
        throw e;
      }
    }
  }

  // idempotentOperationsTest() helpers

  /**
   * Checks that idempotent operations do not throw exceptions when called multiple times. Does <b><i>not</i></b>
   * currently test that state changes are idempotent.
   * @param uri the uri to be hit.
   */
  private void doIdempotentOperationsTest(TestingUri uri) {
    EmbeddedChannel channel = createEmbeddedChannel();
    // no exceptions.
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, uri.toString(), null));
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
  }

  /**
   * Checks that no exceptions are thrown by {@link RestResponseChannel#onResponseComplete(Exception)} when
   * there are write failures.
   * @param uri the uri to hit.
   */
  private void onResponseCompleteUnderWriteFailureTest(TestingUri uri) {
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    ExceptionOutboundHandler exceptionOutboundHandler = new ExceptionOutboundHandler();
    EmbeddedChannel channel = new EmbeddedChannel(exceptionOutboundHandler, processor);
    // no exception because onResponseComplete() swallows it.
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, uri.toString(), null));
    assertFalse("Channel is not closed at the remote end", channel.isActive());
  }

  // headersPresenceTest() helpers

  /**
   * Creates a {@link HttpRequest} with some headers set that will be checked on response.
   * @param httpMethod the {@link HttpMethod} desired.
   * @param uri the URI to hit.
   * @return a link {@link HttpRequest} with some headers set.
   */
  private HttpRequest createRequestWithHeaders(HttpMethod httpMethod, String uri) {
    long currentTime = System.currentTimeMillis();
    HttpRequest request = RestTestUtils.createRequest(httpMethod, uri, null);
    HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_TYPE, "dummy/content-type");
    HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_LENGTH, 100);
    HttpHeaders.setHeader(request, HttpHeaders.Names.LOCATION, "dummyLocation");
    HttpHeaders.setDateHeader(request, HttpHeaders.Names.LAST_MODIFIED, new Date(currentTime));
    HttpHeaders.setDateHeader(request, HttpHeaders.Names.EXPIRES, new Date(currentTime + 1));
    HttpHeaders.setHeader(request, HttpHeaders.Names.CACHE_CONTROL, "dummyCacheControl");
    HttpHeaders.setHeader(request, HttpHeaders.Names.PRAGMA, "dummyPragma");
    HttpHeaders.setDateHeader(request, HttpHeaders.Names.DATE, new Date(currentTime + 2));
    HttpHeaders.setHeader(request, MockNettyMessageProcessor.CUSTOM_HEADER_NAME, "customHeaderValue");
    return request;
  }

  /**
   * Checks the headers in the response match those in the request.
   * @param request the {@link HttpRequest} with the original value of the headers.
   * @param response the {@link HttpResponse} that should have the same value for some headers in {@code request}.
   * @throws ParseException
   */
  private void checkHeaders(HttpRequest request, HttpResponse response)
      throws ParseException {
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.getStatus());
    assertEquals(HttpHeaders.Names.CONTENT_TYPE + " does not match",
        HttpHeaders.getHeader(request, HttpHeaders.Names.CONTENT_TYPE),
        HttpHeaders.getHeader(response, HttpHeaders.Names.CONTENT_TYPE));
    assertEquals(HttpHeaders.Names.CONTENT_LENGTH + " does not match",
        HttpHeaders.getHeader(request, HttpHeaders.Names.CONTENT_LENGTH),
        HttpHeaders.getHeader(response, HttpHeaders.Names.CONTENT_LENGTH));
    assertEquals(HttpHeaders.Names.LOCATION + " does not match",
        HttpHeaders.getHeader(request, HttpHeaders.Names.LOCATION),
        HttpHeaders.getHeader(response, HttpHeaders.Names.LOCATION));
    assertEquals(HttpHeaders.Names.LAST_MODIFIED + " does not match",
        HttpHeaders.getDateHeader(request, HttpHeaders.Names.LAST_MODIFIED),
        HttpHeaders.getDateHeader(response, HttpHeaders.Names.LAST_MODIFIED));
    assertEquals(HttpHeaders.Names.EXPIRES + " does not match",
        HttpHeaders.getDateHeader(request, HttpHeaders.Names.EXPIRES),
        HttpHeaders.getDateHeader(response, HttpHeaders.Names.EXPIRES));
    assertEquals(HttpHeaders.Names.CACHE_CONTROL + " does not match",
        HttpHeaders.getHeader(request, HttpHeaders.Names.CACHE_CONTROL),
        HttpHeaders.getHeader(response, HttpHeaders.Names.CACHE_CONTROL));
    assertEquals(HttpHeaders.Names.PRAGMA + " does not match", HttpHeaders.getHeader(request, HttpHeaders.Names.PRAGMA),
        HttpHeaders.getHeader(response, HttpHeaders.Names.PRAGMA));
    assertEquals(HttpHeaders.Names.DATE + " does not match", HttpHeaders.getDateHeader(request, HttpHeaders.Names.DATE),
        HttpHeaders.getDateHeader(response, HttpHeaders.Names.DATE));
    assertEquals(MockNettyMessageProcessor.CUSTOM_HEADER_NAME + " does not match",
        HttpHeaders.getHeader(request, MockNettyMessageProcessor.CUSTOM_HEADER_NAME),
        HttpHeaders.getHeader(response, MockNettyMessageProcessor.CUSTOM_HEADER_NAME));
  }

  // keepAliveTest() helpers.

  /**
   * Does the keep-alive test by setting the {@link HttpHeaders.Names#CONNECTION} to its two possible values and tests
   * that the response has the appropriate value for the {@link HttpHeaders.Names#CONNECTION}.
   * @param channel the {@link EmbeddedChannel} to send the request over.
   * @param httpMethod the {@link HttpMethod} of the request.
   * @param errorCode the {@link RestServiceErrorCode} to induce at {@link MockNettyMessageProcessor}. {@code null} if
   *                  {@link TestingUri#OnResponseCompleteWithNonRestException} is desired.
   * @param expectedResponseStatus the expected {@link HttpResponseStatus} from remote.
   * @return the {@link EmbeddedChannel} to use once this function is complete. If the channel did not close, this
   * function will return the {@code channel} instance that was passed, otherwise it returns a new channel.
   */
  private EmbeddedChannel doKeepAliveTest(EmbeddedChannel channel, HttpMethod httpMethod,
      RestServiceErrorCode errorCode, HttpResponseStatus expectedResponseStatus) {
    boolean keepAlive = true;
    for (int i = 0; i < 2; i++) {
      HttpHeaders httpHeaders = new DefaultHttpHeaders();
      TestingUri uri = TestingUri.OnResponseCompleteWithNonRestException;
      if (errorCode != null) {
        uri = TestingUri.OnResponseCompleteWithRestException;
        httpHeaders.set(MockNettyMessageProcessor.REST_SERVICE_ERROR_CODE_HEADER_NAME, errorCode);
      }
      if (!keepAlive) {
        httpHeaders.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
      }
      channel.writeInbound(RestTestUtils.createRequest(httpMethod, uri.toString(), httpHeaders));
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", expectedResponseStatus, response.getStatus());
      if (!(response instanceof FullHttpResponse)) {
        // empty the channel
        while (channel.readOutbound() != null) {
        }
      }
      boolean shouldBeAlive = keepAlive &&
          !httpMethod.equals(HttpMethod.POST) && !NettyResponseChannel.CLOSE_CONNECTION_ERROR_STATUSES
          .contains(expectedResponseStatus);
      assertEquals("Channel state (open/close) not as expected", shouldBeAlive, channel.isActive());
      assertEquals("Connection header should be consistent with channel state", shouldBeAlive,
          HttpHeaders.isKeepAlive(response));
      if (!shouldBeAlive) {
        channel.close();
        channel = createEmbeddedChannel();
      }
      keepAlive = !keepAlive;
    }
    return channel;
  }
}

/**
 * List of all the testing URIs.
 */
enum TestingUri {
  /**
   * When this request is received, {@link NettyResponseChannel#close()} is called immediately.
   */
  Close,
  /**
   * When this request is received, headers from the request are copied into the response channel.
   */
  CopyHeaders,
  /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with null {@code cause}.
   */
  ImmediateResponseComplete,
  /**
   * Reduces the write buffer low and high watermarks to 1 and 2 bytes respectively in
   * {@link io.netty.channel.ChannelConfig} so that data is written to the channel byte by byte. This simulates filling
   * up of write buffer (but does not test async writing and flushing since {@link EmbeddedChannel} is blocking).
   */
  FillWriteBuffer,
  /**
   * When this request is received, some data is initially written to the channel via
   * {@link NettyResponseChannel#write(ByteBuffer, Callback)} . An attempt to modify response headers (metadata) is made
   * after this.
   */
  ModifyResponseMetadataAfterWrite,
  /**
   * When this request is received, {@link NettyResponseChannel#close()} is called multiple times.
   */
  MultipleClose,
  /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * multiple times.
   */
  MultipleOnResponseComplete,
  /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with a {@link RestServiceException} as {@code cause}. The exception message and error code is the
   * {@link RestServiceErrorCode} passed in as the value of the header
   * {@link MockNettyMessageProcessor#REST_SERVICE_ERROR_CODE_HEADER_NAME}.
   */
  OnResponseCompleteWithRestException,
  /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with a {@link RuntimeException} as {@code cause}. The exception message is the URI string.
   */
  OnResponseCompleteWithNonRestException,
  /**
   * Response sending fails midway through a write.
   */
  ResponseFailureMidway,
  /**
   * When this request is received, {@link NettyResponseChannel#setHeader(String, Object)} is attempted with null
   * arguments. If these calls don't fail, we report an error.
   */
  SetNullHeader,
  /**
   * Tests setting of a {@link NettyRequest} in {@link NettyResponseChannel}.
   */
  SetRequest,
  /**
   * Requests a certain status to be set.
   */
  SetStatus,
  /**
   * When this request is received, the {@link NettyResponseChannel} is closed and then a write operation is attempted.
   */
  WriteAfterClose,
  /**
   * Fail a write with a {@link Throwable} to test reactions.
   */
  WriteFailureWithThrowable,
  /**
   * Catch all TestingUri.
   */
  Unknown;

  /**
   * Converts the uri specified by the input string into a {@link TestingUri}.
   * @param uri the TestingUri as a string.
   * @return the uri requested as a valid {@link TestingUri} if uri is known, otherwise returns {@link #Unknown}
   */
  public static TestingUri getTestingURI(String uri) {
    try {
      return TestingUri.valueOf(uri);
    } catch (IllegalArgumentException e) {
      return TestingUri.Unknown;
    }
  }
}

/**
 * A test handler that forms the pipeline of the {@link EmbeddedChannel} used in tests.
 * <p/>
 * Exposes some URI strings through which a predefined flow can be executed and verified.
 */
class MockNettyMessageProcessor extends SimpleChannelInboundHandler<HttpObject> {
  static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
  static final String CUSTOM_HEADER_NAME = "customHeader";
  static final String STATUS_HEADER_NAME = "status";
  static final String REST_SERVICE_ERROR_CODE_HEADER_NAME = "restServiceErrorCode";

  private ChannelHandlerContext ctx;
  private NettyRequest request;
  private NettyResponseChannel restResponseChannel;
  private NettyMetrics nettyMetrics;

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    this.ctx = ctx;
    nettyMetrics = new NettyMetrics(METRIC_REGISTRY);
    RestRequestMetricsTracker.setDefaults(METRIC_REGISTRY);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    request = null;
    restResponseChannel = null;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws Exception {
    if (obj != null && obj instanceof HttpRequest) {
      if (obj.getDecoderResult().isSuccess()) {
        handleRequest((HttpRequest) obj);
      } else {
        throw new RestServiceException("Malformed request received - " + obj, RestServiceErrorCode.MalformedRequest);
      }
    } else if (obj != null && obj instanceof HttpContent) {
      handleContent((HttpContent) obj);
    } else {
      throw new RestServiceException("HttpObject received is null or not of a known type",
          RestServiceErrorCode.UnknownHttpObject);
    }
  }

  /**
   * Handles a {@link HttpRequest}. If content is awaited, handles some state maintenance. Else handles the request
   * according to a predefined flow based on the uri.
   * @param httpRequest the {@link HttpRequest} that needs to be handled.
   * @throws Exception
   */
  private void handleRequest(HttpRequest httpRequest)
      throws Exception {
    request = new NettyRequest(httpRequest, nettyMetrics);
    restResponseChannel = new NettyResponseChannel(ctx, nettyMetrics);
    restResponseChannel.setRequest(request);
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "text/plain; charset=UTF-8");
    TestingUri uri = TestingUri.getTestingURI(request.getUri());
    switch (uri) {
      case Close:
        restResponseChannel.close();
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case CopyHeaders:
        copyHeaders(httpRequest);
        restResponseChannel.onResponseComplete(null);
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case ImmediateResponseComplete:
        restResponseChannel.onResponseComplete(null);
        assertEquals("ResponseStatus differs from default", ResponseStatus.Ok, restResponseChannel.getStatus());
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case FillWriteBuffer:
        ctx.channel().config().setWriteBufferLowWaterMark(1);
        ctx.channel().config().setWriteBufferHighWaterMark(2);
        break;
      case ModifyResponseMetadataAfterWrite:
        restResponseChannel.write(ByteBuffer.wrap(new byte[0]), null);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "text/plain; charset=UTF-8");
        break;
      case MultipleClose:
        restResponseChannel.onResponseComplete(null);
        assertFalse("Request channel is not closed", request.isOpen());
        restResponseChannel.close();
        restResponseChannel.close();
        break;
      case MultipleOnResponseComplete:
        restResponseChannel.onResponseComplete(null);
        assertFalse("Request channel is not closed", request.isOpen());
        restResponseChannel.onResponseComplete(null);
        break;
      case OnResponseCompleteWithRestException:
        String errorCodeStr = (String) request.getArgs().get(REST_SERVICE_ERROR_CODE_HEADER_NAME);
        RestServiceErrorCode errorCode = RestServiceErrorCode.valueOf(errorCodeStr);
        restResponseChannel.onResponseComplete(new RestServiceException(errorCodeStr, errorCode));
        assertEquals("ResponseStatus does not reflect error", ResponseStatus.getResponseStatus(errorCode),
            restResponseChannel.getStatus());
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case OnResponseCompleteWithNonRestException:
        restResponseChannel
            .onResponseComplete(new RuntimeException(TestingUri.OnResponseCompleteWithNonRestException.toString()));
        assertEquals("ResponseStatus does not reflect error", ResponseStatus.InternalServerError,
            restResponseChannel.getStatus());
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case ResponseFailureMidway:
        ChannelWriteCallback callback = new ChannelWriteCallback();
        callback.compareWithFuture(restResponseChannel
            .write(ByteBuffer.wrap(TestingUri.ResponseFailureMidway.toString().getBytes()), callback));
        assertNull("There shouldn't have been any exceptions on the first write", callback.exception);
        restResponseChannel.onResponseComplete(new Exception());
        // this should close the channel and the test will check for that.
        break;
      case SetNullHeader:
        setNullHeaders();
        break;
      case SetRequest:
        setRequestTest();
        break;
      case SetStatus:
        restResponseChannel.setStatus(ResponseStatus.valueOf(HttpHeaders.getHeader(httpRequest, STATUS_HEADER_NAME)));
        restResponseChannel.onResponseComplete(null);
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case WriteAfterClose:
        restResponseChannel.close();
        assertFalse("Request channel is not closed", request.isOpen());
        callback = new ChannelWriteCallback();
        callback.compareWithFuture(
            restResponseChannel.write(ByteBuffer.wrap(TestingUri.WriteAfterClose.toString().getBytes()), callback));
        if (callback.exception != null) {
          throw callback.exception;
        }
        break;
      case WriteFailureWithThrowable:
        callback = new ChannelWriteCallback();
        callback.compareWithFuture(restResponseChannel
            .write(ByteBuffer.wrap(TestingUri.WriteFailureWithThrowable.toString().getBytes()), callback));
        break;
    }
  }

  /**
   * Handles a {@link HttpContent}. Checks state and echoes back the content.
   * @param httpContent the {@link HttpContent} that needs to be handled.
   * @throws Exception
   */
  private void handleContent(HttpContent httpContent)
      throws Exception {
    if (request != null) {
      boolean isLast = httpContent instanceof LastHttpContent;
      ByteBuffer content = ByteBuffer.wrap(httpContent.content().array());
      int bytesWritten = 0;
      while (content.hasRemaining()) {
        ChannelWriteCallback callback = new ChannelWriteCallback();
        callback.compareWithFuture(restResponseChannel.write(content, callback));
        if (callback.exception == null) {
          bytesWritten += callback.result;
        } else {
          throw callback.exception;
        }
      }
      assertEquals("Bytes written not equal to content size", httpContent.content().array().length, bytesWritten);
      if (isLast) {
        restResponseChannel.onResponseComplete(null);
        assertFalse("Request channel is not closed", request.isOpen());
      }
    } else {
      throw new RestServiceException("Received data without a request", RestServiceErrorCode.InvalidRequestState);
    }
  }

  /**
   * Copies headers from request to response.
   * @param httpRequest the {@link HttpRequest} to copy headers from.
   * @throws ParseException
   * @throws RestServiceException
   */
  private void copyHeaders(HttpRequest httpRequest)
      throws ParseException, RestServiceException {
    restResponseChannel.setStatus(ResponseStatus.Accepted);
    assertEquals("ResponseStatus differs from what was set", ResponseStatus.Accepted, restResponseChannel.getStatus());

    restResponseChannel
        .setHeader(RestUtils.Headers.CONTENT_TYPE, HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CONTENT_TYPE));
    assertEquals("Value of [" + RestUtils.Headers.CONTENT_TYPE + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CONTENT_TYPE),
        restResponseChannel.getHeader(HttpHeaders.Names.CONTENT_TYPE));

    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH,
        Long.parseLong(HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CONTENT_LENGTH)));
    assertEquals("Value of [" + RestUtils.Headers.CONTENT_LENGTH + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CONTENT_LENGTH),
        restResponseChannel.getHeader(HttpHeaders.Names.CONTENT_LENGTH));

    restResponseChannel
        .setHeader(RestUtils.Headers.LOCATION, HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.LOCATION));
    assertEquals("Value of [" + RestUtils.Headers.LOCATION + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.LOCATION),
        restResponseChannel.getHeader(HttpHeaders.Names.LOCATION));

    restResponseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED,
        HttpHeaders.getDateHeader(httpRequest, HttpHeaders.Names.LAST_MODIFIED));
    assertEquals("Value of [" + RestUtils.Headers.LAST_MODIFIED + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.LAST_MODIFIED),
        restResponseChannel.getHeader(HttpHeaders.Names.LAST_MODIFIED));

    restResponseChannel
        .setHeader(RestUtils.Headers.EXPIRES, HttpHeaders.getDateHeader(httpRequest, HttpHeaders.Names.EXPIRES));
    assertEquals("Value of [" + RestUtils.Headers.EXPIRES + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.EXPIRES),
        restResponseChannel.getHeader(HttpHeaders.Names.EXPIRES));

    restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL,
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CACHE_CONTROL));
    assertEquals("Value of [" + RestUtils.Headers.CACHE_CONTROL + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CACHE_CONTROL),
        restResponseChannel.getHeader(HttpHeaders.Names.CACHE_CONTROL));

    restResponseChannel
        .setHeader(RestUtils.Headers.PRAGMA, HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.PRAGMA));
    assertEquals("Value of [" + RestUtils.Headers.PRAGMA + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.PRAGMA),
        restResponseChannel.getHeader(HttpHeaders.Names.PRAGMA));

    restResponseChannel
        .setHeader(RestUtils.Headers.DATE, HttpHeaders.getDateHeader(httpRequest, HttpHeaders.Names.DATE));
    assertEquals("Value of [" + RestUtils.Headers.DATE + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.DATE),
        restResponseChannel.getHeader(HttpHeaders.Names.DATE));

    restResponseChannel.setHeader(CUSTOM_HEADER_NAME, HttpHeaders.getHeader(httpRequest, CUSTOM_HEADER_NAME));
    assertEquals("Value of [" + CUSTOM_HEADER_NAME + "] differs from what was set",
        HttpHeaders.getHeader(httpRequest, CUSTOM_HEADER_NAME), restResponseChannel.getHeader(CUSTOM_HEADER_NAME));
  }

  /**
   * Tries to set null headers in the {@link NettyResponseChannel}. If the operation does not fail, reports an error.
   * @throws RestServiceException
   */
  private void setNullHeaders()
      throws RestServiceException {
    ResponseStatus status = ResponseStatus.Accepted;
    try {
      // headerName null.
      try {
        restResponseChannel.setHeader(null, "dummyHeaderValue");
        status = ResponseStatus.InternalServerError;
        fail("Call to setHeader with null values succeeded. It should have not");
      } catch (IllegalArgumentException e) {
        // expected. nothing to do.
      }

      // headerValue null.
      try {
        restResponseChannel.setHeader("dummyHeaderName", null);
        status = ResponseStatus.InternalServerError;
        fail("Call to setHeader with null values succeeded. It should have not");
      } catch (IllegalArgumentException e) {
        // expected. nothing to do.
      }

      // headerName and headerValue null.
      try {
        restResponseChannel.setHeader(null, null);
        status = ResponseStatus.InternalServerError;
        fail("Call to setHeader with null values succeeded. It should have not");
      } catch (IllegalArgumentException e) {
        // expected. nothing to do.
      }
    } finally {
      restResponseChannel.setStatus(status);
      restResponseChannel.onResponseComplete(null);
      assertFalse("Request channel is not closed", request.isOpen());
    }
  }

  /**
   * Tries different exception scenarios for {@link NettyResponseChannel#setRequest(NettyRequest)}.
   * @throws RestServiceException
   */
  private void setRequestTest()
      throws RestServiceException {
    ResponseStatus status = ResponseStatus.Accepted;
    restResponseChannel = new NettyResponseChannel(ctx, new NettyMetrics(new MetricRegistry()));
    try {
      try {
        restResponseChannel.setRequest(null);
        status = ResponseStatus.InternalServerError;
        fail("Tried to set null request yet no exception was thrown");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }

      restResponseChannel.setRequest(request);
      try {
        restResponseChannel.setRequest(request);
        status = ResponseStatus.InternalServerError;
        fail("Tried to reset request and no exception was thrown");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
    } finally {
      restResponseChannel.setStatus(status);
      restResponseChannel.onResponseComplete(null);
      assertFalse("Request channel is not closed", request.isOpen());
    }
  }
}

/**
 * A {@link ChannelOutboundHandler} that throws exceptions on write.
 */
class ExceptionOutboundHandler extends ChannelOutboundHandlerAdapter {
  protected static String EXCEPTION_MESSAGE = "@@randomExceptionMessage@@";

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    throw new Exception(EXCEPTION_MESSAGE);
  }
}

/**
 * A {@link ChannelOutboundHandler} that throws errors on write.
 */
class ErrorOutboundHandler extends ChannelOutboundHandlerAdapter {
  protected static String ERROR_MESSAGE = "@@randomErrorMessage@@";

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    throw new Error(ERROR_MESSAGE);
  }
}

/**
 * Class that can be used to receive callbacks from {@link NettyResponseChannel}.
 * <p/>
 * On callback, stores the result and exception to be retrieved for later use.
 */
class ChannelWriteCallback implements Callback<Long> {
  /**
   * Contains the result of the operation for which this was set as callback.
   * If there was no result or if this was called before callback is received, it will be null
   */
  public Long result = null;
  /**
   * Stores any exception thrown by the operation for which this was set as callback.
   * If there was no exception or if this was called before callback is received, it will be null.
   */
  public Exception exception = null;
  private CountDownLatch callbackReceived = new CountDownLatch(1);

  @Override
  public void onCompletion(Long result, Exception exception) {
    this.result = result;
    this.exception = exception;
    callbackReceived.countDown();
  }

  /**
   * Compares the data obtained from the callback with the data obtained from {@code future}.
   * @param future the {@link Future} that represents the result of the same operation that this callback is meant for.
   * @throws InterruptedException
   * @throws TimeoutException
   */
  public void compareWithFuture(Future<Long> future)
      throws InterruptedException, TimeoutException {
    Long futureOutput;
    try {
      futureOutput = future.get(1, TimeUnit.MILLISECONDS);
      assertEquals("Future and callback results don't match", futureOutput, result);
    } catch (ExecutionException e) {
      if (!callbackReceived.await(1, TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException("Callback has not been invoked even though future.get() has returned");
      } else {
        assertEquals("Future and callback exceptions don't match", e.getCause().getMessage(), exception.getMessage());
      }
    }
  }
}
