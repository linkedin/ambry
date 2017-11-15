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
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
 * {@link MockNettyMessageProcessor#handleContent(HttpContent)}.
 */
public class NettyResponseChannelTest {
  private static final Map<RestServiceErrorCode, HttpResponseStatus> REST_ERROR_CODE_TO_HTTP_STATUS = new HashMap<>();

  static {
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.BadRequest, HttpResponseStatus.BAD_REQUEST);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.Unauthorized, HttpResponseStatus.UNAUTHORIZED);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.Deleted, HttpResponseStatus.GONE);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.NotFound, HttpResponseStatus.NOT_FOUND);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.ResourceScanInProgress,
        HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.ResourceDirty, HttpResponseStatus.FORBIDDEN);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.AccessDenied, HttpResponseStatus.FORBIDDEN);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.InternalServerError,
        HttpResponseStatus.INTERNAL_SERVER_ERROR);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.RangeNotSatisfiable,
        HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
    REST_ERROR_CODE_TO_HTTP_STATUS.put(RestServiceErrorCode.RequestTooLarge,
        HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
  }

  /**
   * Tests the common workflow of the {@link NettyResponseChannel} i.e., add some content to response body via
   * {@link NettyResponseChannel#write(ByteBuffer, Callback)} and then complete the response.
   * <p/>
   * These responses have the header Transfer-Encoding set to chunked.
   * @throws Exception
   */
  @Test
  public void responsesWithTransferEncodingChunkedTest() throws Exception {
    String content = "@@randomContent@@@";
    String lastContent = "@@randomLastContent@@@";
    EmbeddedChannel channel = createEmbeddedChannel();
    MockNettyMessageProcessor processor = channel.pipeline().get(MockNettyMessageProcessor.class);
    AtomicLong contentIdGenerator = new AtomicLong(0);

    final int ITERATIONS = 10;
    for (int i = 0; i < ITERATIONS; i++) {
      boolean isKeepAlive = i != (ITERATIONS - 1);
      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", null);
      HttpUtil.setKeepAlive(httpRequest, isKeepAlive);
      channel.writeInbound(httpRequest);
      ArrayList<String> contents = new ArrayList<>();
      for (int j = 0; j <= i; j++) {
        String contentToSend = content + contentIdGenerator.getAndIncrement();
        channel.writeInbound(createContent(contentToSend, false));
        contents.add(contentToSend);
      }
      channel.writeInbound(createContent(lastContent, true));
      verifyCallbacks(processor);
      // first outbound has to be response.
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
      assertTrue("Response must say 'Transfer-Encoding : chunked'", HttpUtil.isTransferEncodingChunked(response));
      // content echoed back.
      for (String srcOfTruth : contents) {
        String returnedContent = RestTestUtils.getContentString((HttpContent) channel.readOutbound());
        assertEquals("Content does not match with expected content", srcOfTruth, returnedContent);
      }
      // last content echoed back.
      String returnedContent = RestTestUtils.getContentString((HttpContent) channel.readOutbound());
      assertEquals("Content does not match with expected content", lastContent, returnedContent);
      assertTrue("Did not receive end marker", channel.readOutbound() instanceof LastHttpContent);
      assertEquals("Unexpected channel state on the server", isKeepAlive, channel.isActive());
    }
  }

  /**
   * Tests the common workflow of the {@link NettyResponseChannel} i.e., add some content to response body via
   * {@link NettyResponseChannel#write(ByteBuffer, Callback)} and then complete the response.
   * <p/>
   * These responses have the header Content-Length set.
   * @throws Exception
   */
  @Test
  public void responsesWithContentLengthTest() throws Exception {
    EmbeddedChannel channel = createEmbeddedChannel();
    MockNettyMessageProcessor processor = channel.pipeline().get(MockNettyMessageProcessor.class);
    final int ITERATIONS = 10;
    for (int i = 0; i < ITERATIONS; i++) {
      boolean isKeepAlive = i != (ITERATIONS - 1);
      HttpHeaders httpHeaders = new DefaultHttpHeaders();
      httpHeaders.set(MockNettyMessageProcessor.CHUNK_COUNT_HEADER_NAME, i);
      HttpRequest httpRequest =
          RestTestUtils.createRequest(HttpMethod.POST, TestingUri.ResponseWithContentLength.toString(), httpHeaders);
      HttpUtil.setKeepAlive(httpRequest, isKeepAlive);
      channel.writeInbound(httpRequest);
      verifyCallbacks(processor);

      // first outbound has to be response.
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
      long contentLength = HttpUtil.getContentLength(response, -1);
      assertEquals("Unexpected Content-Length", MockNettyMessageProcessor.CHUNK.length * i, contentLength);
      if (contentLength == 0) {
        // special case. Since Content-Length is set, the response should be an instance of FullHttpResponse.
        assertTrue("Response not instance of FullHttpResponse", response instanceof FullHttpResponse);
      } else {
        HttpContent httpContent = null;
        for (int j = 0; j < i; j++) {
          httpContent = (HttpContent) channel.readOutbound();
          byte[] returnedContent = httpContent.content().array();
          assertArrayEquals("Content does not match with expected content", MockNettyMessageProcessor.CHUNK,
              returnedContent);
        }
        // the last HttpContent should also be an instance of LastHttpContent
        assertTrue("The last part of the content is not LastHttpContent", httpContent instanceof LastHttpContent);
      }
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

    // with Transfer-Encoding:Chunked
    HttpRequest httpRequest =
        RestTestUtils.createRequest(HttpMethod.GET, TestingUri.ImmediateResponseComplete.toString(), null);
    channel.writeInbound(httpRequest);
    // There should be a response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    assertTrue("Response must say 'Transfer-Encoding : chunked'", HttpUtil.isTransferEncodingChunked(response));
    // since this is Transfer-Encoding:chunked, there should be a LastHttpContent
    assertTrue("Did not receive end marker", channel.readOutbound() instanceof LastHttpContent);
    assertTrue("Channel should be alive", channel.isActive());

    // with Content-Length set
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(MockNettyMessageProcessor.CHUNK_COUNT_HEADER_NAME, 0);
    httpRequest = RestTestUtils.createRequest(HttpMethod.GET, TestingUri.ImmediateResponseComplete.toString(), headers);
    HttpUtil.setKeepAlive(httpRequest, false);
    channel.writeInbound(httpRequest);
    // There should be a response.
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Response must have Content-Length set to 0", 0, HttpUtil.getContentLength(response, -1));
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    // since Content-Length is set, the response should be an instance of FullHttpResponse.
    assertTrue("Response not instance of FullHttpResponse", response instanceof FullHttpResponse);
    assertFalse("Channel should not be alive", channel.isActive());
  }

  /**
   * Performs bad state transitions and verifies that they throw the right exceptions.
   * @throws Exception
   */
  @Test
  public void badStateTransitionsTest() throws Exception {
    // write after close.
    doBadStateTransitionTest(TestingUri.WriteAfterClose, IOException.class);

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
  public void behaviourUnderWriteFailuresTest() throws Exception {
    onResponseCompleteUnderWriteFailureTest(TestingUri.ImmediateResponseComplete);
    onResponseCompleteUnderWriteFailureTest(TestingUri.OnResponseCompleteWithNonRestException);

    // writing to channel with a outbound handler that generates an Exception
    String message = UtilsTest.getRandomString(10);
    try {
      String content = "@@randomContent@@@";
      MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
      ChannelOutboundHandler badOutboundHandler = new ExceptionOutboundHandler(new Exception(message));
      EmbeddedChannel channel = new EmbeddedChannel(badOutboundHandler, processor);
      channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, "/", null));
      // channel has been closed because of write failure
      channel.writeInbound(createContent(content, true));
      verifyCallbacks(processor);
      fail("Callback for write would have thrown an Exception");
    } catch (Exception e) {
      assertEquals("Exception not as expected", message, e.getMessage());
    }

    // writing to channel with a outbound handler that encounters a ClosedChannelException
    try {
      String content = "@@randomContent@@@";
      MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
      ChannelOutboundHandler badOutboundHandler = new ExceptionOutboundHandler(new ClosedChannelException());
      EmbeddedChannel channel = new EmbeddedChannel(badOutboundHandler, processor);
      channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, "/", null));
      // channel has been closed because of write failure
      channel.writeInbound(createContent(content, true));
      verifyCallbacks(processor);
      fail("Callback for write would have thrown an Exception");
    } catch (IOException e) {
      assertTrue("Should be recognized as a client termination", Utils.isPossibleClientTermination(e));
    }

    // writing to channel with a outbound handler that generates an Error
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    EmbeddedChannel channel = new EmbeddedChannel(new ErrorOutboundHandler(), processor);
    try {
      channel.writeInbound(
          RestTestUtils.createRequest(HttpMethod.GET, TestingUri.WriteFailureWithThrowable.toString(), null));
      verifyCallbacks(processor);
    } catch (Error e) {
      assertEquals("Unexpected error", ErrorOutboundHandler.ERROR_MESSAGE, e.getMessage());
    }

    channel = createEmbeddedChannel();
    processor = channel.pipeline().get(MockNettyMessageProcessor.class);
    channel.writeInbound(
        RestTestUtils.createRequest(HttpMethod.GET, TestingUri.ResponseFailureMidway.toString(), null));
    verifyCallbacks(processor);
    assertFalse("Channel is not closed at the remote end", channel.isActive());
  }

  /**
   * Asks the server to write more data than the set Content-Length and checks behavior.
   * @throws Exception
   */
  @Test
  public void writeMoreThanContentLengthTest() throws Exception {
    doWriteMoreThanContentLengthTest(0);
    doWriteMoreThanContentLengthTest(5);
  }

  /**
   * Tests handling of content that is larger than write buffer size. In this test case, the write buffer low and high
   * watermarks are requested to be set to 1 and 2 respectively so the content will be written byte by byte into the
   * {@link NettyResponseChannel}. This does <b><i>not</i></b> test for the same situation in a async scenario since
   * {@link EmbeddedChannel} only provides blocking semantics.
   * @throws IOException
   */
  @Test
  public void fillWriteBufferTest() throws IOException {
    String content = "@@randomContent@@@";
    String lastContent = "@@randomLastContent@@@";
    EmbeddedChannel channel = createEmbeddedChannel();
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.GET, TestingUri.FillWriteBuffer.toString(), null);
    HttpUtil.setKeepAlive(httpRequest, false);
    channel.writeInbound(httpRequest);
    channel.writeInbound(createContent(content, false));
    channel.writeInbound(createContent(lastContent, true));

    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
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
  public void headersPresenceTest() throws ParseException {
    HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.CopyHeaders.toString());
    HttpUtil.setKeepAlive(request, false);
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
    HttpUtil.setKeepAlive(request, false);
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(request);

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.status());
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Tries different exception scenarios for {@link NettyResponseChannel#setRequest(NettyRequest)}.
   */
  @Test
  public void setRequestTest() {
    HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.SetRequest.toString());
    HttpUtil.setKeepAlive(request, false);
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(request);

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.status());
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
      request.headers().set(MockNettyMessageProcessor.STATUS_HEADER_NAME, expectedResponseStatus);
      HttpUtil.setKeepAlive(request, false);
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
   * Tests that error responses are correctly formed.
   */
  @Test
  public void errorResponseTest() {
    EmbeddedChannel channel = createEmbeddedChannel();
    for (Map.Entry<RestServiceErrorCode, HttpResponseStatus> entry : REST_ERROR_CODE_TO_HTTP_STATUS.entrySet()) {
      HttpHeaders httpHeaders = new DefaultHttpHeaders();
      httpHeaders.set(MockNettyMessageProcessor.REST_SERVICE_ERROR_CODE_HEADER_NAME, entry.getKey());
      channel.writeInbound(
          RestTestUtils.createRequest(HttpMethod.HEAD, TestingUri.OnResponseCompleteWithRestException.toString(),
              httpHeaders));
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", entry.getValue(), response.status());
      boolean containsFailureReasonHeader = response.headers().contains(NettyResponseChannel.FAILURE_REASON_HEADER);
      if (entry.getValue() == HttpResponseStatus.BAD_REQUEST) {
        assertTrue("Could not find failure reason header.", containsFailureReasonHeader);
      } else {
        assertFalse("Should not have found failure reason header.", containsFailureReasonHeader);
      }
      if (HttpStatusClass.CLIENT_ERROR.contains(response.status().code())) {
        assertEquals("Wrong error code", entry.getKey(),
            RestServiceErrorCode.valueOf(response.headers().get(NettyResponseChannel.ERROR_CODE_HEADER)));
      } else {
        assertFalse("Should not have found error code header",
            response.headers().contains(NettyResponseChannel.ERROR_CODE_HEADER));
      }
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
          HttpUtil.isKeepAlive(response));
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
    HttpMethod[] HTTP_METHODS = {HttpMethod.POST, HttpMethod.PUT, HttpMethod.GET, HttpMethod.HEAD, HttpMethod.DELETE};
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
   * Tests that client initiated terminations don't count towards {@link HttpResponseStatus#INTERNAL_SERVER_ERROR}.
   */
  @Test
  public void clientEarlyTerminationTest() throws Exception {
    EmbeddedChannel channel = createEmbeddedChannel();
    TestingUri uri = TestingUri.OnResponseCompleteWithEarlyClientTermination;
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, uri.toString(), null);
    HttpUtil.setKeepAlive(httpRequest, false);

    String iseMetricName = MetricRegistry.name(NettyResponseChannel.class, "InternalServerErrorCount");
    long iseBeforeCount = MockNettyMessageProcessor.METRIC_REGISTRY.getCounters().get(iseMetricName).getCount();
    String cetMetricName = MetricRegistry.name(NettyResponseChannel.class, "ClientEarlyTerminationCount");
    long cetBeforeCount = MockNettyMessageProcessor.METRIC_REGISTRY.getCounters().get(cetMetricName).getCount();

    channel.writeInbound(httpRequest);
    // first outbound has to be response.
    HttpResponse response = channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
    if (!(response instanceof FullHttpResponse)) {
      // empty the channel
      while (channel.readOutbound() != null) {
      }
    }

    assertEquals("Client terminations should not count towards InternalServerError count", iseBeforeCount,
        MockNettyMessageProcessor.METRIC_REGISTRY.getCounters().get(iseMetricName).getCount());
    assertEquals("Client terminations should have been tracked", cetBeforeCount + 1,
        MockNettyMessageProcessor.METRIC_REGISTRY.getCounters().get(cetMetricName).getCount());
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

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
    assertFalse("Inconsistent value for Connection header", HttpUtil.isKeepAlive(response));
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

  /**
   * Verifies any callbacks queued in the {@code processor}.
   * @param processor the {@link MockNettyMessageProcessor} that contains the callbacks that need to be verified.
   * @throws Exception
   */
  private void verifyCallbacks(MockNettyMessageProcessor processor) throws Exception {
    assertNotNull("There is no MockNettyMessageProcessor in the channel", processor);
    for (ChannelWriteCallback callback : processor.writeCallbacksToVerify) {
      callback.compareWithFuture();
      if (callback.exception != null) {
        throw callback.exception;
      }
    }
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
  private void doBadStateTransitionTest(TestingUri uri, Class exceptionClass) throws Exception {
    EmbeddedChannel channel = createEmbeddedChannel();
    MockNettyMessageProcessor processor = channel.pipeline().get(MockNettyMessageProcessor.class);
    try {
      channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, uri.toString(), null));
      verifyCallbacks(processor);
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
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
  }

  /**
   * Checks that no exceptions are thrown by {@link RestResponseChannel#onResponseComplete(Exception)} when
   * there are write failures.
   * @param uri the uri to hit.
   */
  private void onResponseCompleteUnderWriteFailureTest(TestingUri uri) {
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    ExceptionOutboundHandler exceptionOutboundHandler = new ExceptionOutboundHandler(new Exception());
    EmbeddedChannel channel = new EmbeddedChannel(exceptionOutboundHandler, processor);
    // no exception because onResponseComplete() swallows it.
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, uri.toString(), null));
    assertFalse("Channel is not closed at the remote end", channel.isActive());
  }

  // writeMoreThanContentLengthTest() helpers.

  /**
   * Asks the server to write more data than the set Content-Length and checks behavior.
   * @param chunkCount the number of chunks of {@link MockNettyMessageProcessor#CHUNK} to use to set Content-Length.
   * @throws Exception
   */
  private void doWriteMoreThanContentLengthTest(int chunkCount) throws Exception {
    EmbeddedChannel channel = createEmbeddedChannel();
    MockNettyMessageProcessor processor = channel.pipeline().get(MockNettyMessageProcessor.class);
    HttpHeaders httpHeaders = new DefaultHttpHeaders();
    httpHeaders.set(MockNettyMessageProcessor.CHUNK_COUNT_HEADER_NAME, chunkCount);
    HttpRequest httpRequest =
        RestTestUtils.createRequest(HttpMethod.POST, TestingUri.WriteMoreThanContentLength.toString(), httpHeaders);
    HttpUtil.setKeepAlive(httpRequest, true);
    channel.writeInbound(httpRequest);

    try {
      verifyCallbacks(processor);
      fail("One of the callbacks should have failed because the data written was more than Content-Length");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    // It doesn't matter what the response is - because it may either fail or succeed depending on certain race
    // conditions. What matters is that the programming error is caught appropriately by NettyResponseChannel and it
    // makes a callback with the right exception.
    while (channel.readOutbound() != null) {
    }
    channel.close();
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
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "dummy/content-type");
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, 100);
    request.headers().set(HttpHeaderNames.LOCATION, "dummyLocation");
    request.headers().set(HttpHeaderNames.LAST_MODIFIED, new Date(currentTime));
    request.headers().set(HttpHeaderNames.EXPIRES, new Date(currentTime + 1));
    request.headers().set(HttpHeaderNames.CACHE_CONTROL, "dummyCacheControl");
    request.headers().set(HttpHeaderNames.PRAGMA, "dummyPragma");
    request.headers().set(HttpHeaderNames.DATE, new Date(currentTime + 2));
    request.headers().set(MockNettyMessageProcessor.CUSTOM_HEADER_NAME, "customHeaderValue");
    return request;
  }

  /**
   * Checks the headers in the response match those in the request.
   * @param request the {@link HttpRequest} with the original value of the headers.
   * @param response the {@link HttpResponse} that should have the same value for some headers in {@code request}.
   * @throws ParseException
   */
  private void checkHeaders(HttpRequest request, HttpResponse response) throws ParseException {
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.status());
    assertEquals(HttpHeaderNames.CONTENT_TYPE + " does not match", request.headers().get(HttpHeaderNames.CONTENT_TYPE),
        response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    assertEquals(HttpHeaderNames.CONTENT_LENGTH + " does not match",
        request.headers().get(HttpHeaderNames.CONTENT_LENGTH), response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
    assertEquals(HttpHeaderNames.LOCATION + " does not match", request.headers().get(HttpHeaderNames.LOCATION),
        response.headers().get(HttpHeaderNames.LOCATION));
    assertEquals(HttpHeaderNames.LAST_MODIFIED + " does not match",
        request.headers().getTimeMillis(HttpHeaderNames.LAST_MODIFIED),
        response.headers().getTimeMillis(HttpHeaderNames.LAST_MODIFIED));
    assertEquals(HttpHeaderNames.EXPIRES + " does not match", request.headers().getTimeMillis(HttpHeaderNames.EXPIRES),
        response.headers().getTimeMillis(HttpHeaderNames.EXPIRES));
    assertEquals(HttpHeaderNames.CACHE_CONTROL + " does not match",
        request.headers().get(HttpHeaderNames.CACHE_CONTROL), response.headers().get(HttpHeaderNames.CACHE_CONTROL));
    assertEquals(HttpHeaderNames.PRAGMA + " does not match", request.headers().get(HttpHeaderNames.PRAGMA),
        response.headers().get(HttpHeaderNames.PRAGMA));
    assertEquals(HttpHeaderNames.DATE + " does not match", request.headers().getTimeMillis(HttpHeaderNames.DATE),
        response.headers().getTimeMillis(HttpHeaderNames.DATE));
    assertEquals(MockNettyMessageProcessor.CUSTOM_HEADER_NAME + " does not match",
        request.headers().get(MockNettyMessageProcessor.CUSTOM_HEADER_NAME),
        response.headers().get(MockNettyMessageProcessor.CUSTOM_HEADER_NAME));
  }

  // keepAliveTest() helpers.

  /**
   * Does the keep-alive test by setting the {@link HttpHeaderNames#CONNECTION} to its two possible values and tests
   * that the response has the appropriate value for the {@link HttpHeaderNames#CONNECTION}.
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
        httpHeaders.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
      }
      channel.writeInbound(RestTestUtils.createRequest(httpMethod, uri.toString(), httpHeaders));
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", expectedResponseStatus, response.status());
      if (!(response instanceof FullHttpResponse)) {
        // empty the channel
        while (channel.readOutbound() != null) {
        }
      }
      boolean shouldBeAlive = keepAlive && !httpMethod.equals(HttpMethod.POST) && !httpMethod.equals(HttpMethod.PUT)
          && !NettyResponseChannel.CLOSE_CONNECTION_ERROR_STATUSES.contains(expectedResponseStatus);
      assertEquals("Channel state (open/close) not as expected", shouldBeAlive, channel.isActive());
      assertEquals("Connection header should be consistent with channel state", shouldBeAlive,
          HttpUtil.isKeepAlive(response));
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
  Close, /**
   * When this request is received, headers from the request are copied into the response channel.
   */
  CopyHeaders, /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with null {@code cause}.
   */
  ImmediateResponseComplete, /**
   * Reduces the write buffer low and high watermarks to 1 and 2 bytes respectively in
   * {@link io.netty.channel.ChannelConfig} so that data is written to the channel byte by byte. This simulates filling
   * up of write buffer (but does not test async writing and flushing since {@link EmbeddedChannel} is blocking).
   */
  FillWriteBuffer, /**
   * When this request is received, some data is initially written to the channel via
   * {@link NettyResponseChannel#write(ByteBuffer, Callback)} . An attempt to modify response headers (metadata) is made
   * after this.
   */
  ModifyResponseMetadataAfterWrite, /**
   * When this request is received, {@link NettyResponseChannel#close()} is called multiple times.
   */
  MultipleClose, /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * multiple times.
   */
  MultipleOnResponseComplete, /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with a {@link RestServiceException} as {@code cause}. The exception message and error code is the
   * {@link RestServiceErrorCode} passed in as the value of the header
   * {@link MockNettyMessageProcessor#REST_SERVICE_ERROR_CODE_HEADER_NAME}.
   */
  OnResponseCompleteWithRestException, /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with a {@link RuntimeException} as {@code cause}. The exception message is the URI string.
   */
  OnResponseCompleteWithNonRestException, /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with an {@link IOException} with message {@link Utils#CLIENT_RESET_EXCEPTION_MSG}.
   */
  OnResponseCompleteWithEarlyClientTermination, /**
   * Response sending fails midway through a write.
   */
  ResponseFailureMidway, /**
   * When this request is received, a response with {@link RestUtils.Headers#CONTENT_LENGTH} set is returned.
   * The value of the header {@link MockNettyMessageProcessor#CHUNK_COUNT_HEADER_NAME} is used to determine the number
   * of chunks (each equal to {@link MockNettyMessageProcessor#CHUNK}) to return.
   * <p/>
   * The {@link RestUtils.Headers#CONTENT_LENGTH} is equal to the value in
   * {@link MockNettyMessageProcessor#CHUNK_COUNT_HEADER_NAME} times the length of
   * {@link MockNettyMessageProcessor#CHUNK}
   */
  ResponseWithContentLength, /**
   * When this request is received, {@link NettyResponseChannel#setHeader(String, Object)} is attempted with null
   * arguments. If these calls don't fail, we report an error.
   */
  SetNullHeader, /**
   * Tests setting of a {@link NettyRequest} in {@link NettyResponseChannel}.
   */
  SetRequest, /**
   * Requests a certain status to be set.
   */
  SetStatus, /**
   * When this request is received, the {@link NettyResponseChannel} is closed and then a write operation is attempted.
   */
  WriteAfterClose, /**
   * Fail a write with a {@link Throwable} to test reactions.
   */
  WriteFailureWithThrowable, /**
   * When this request is received, a response with {@link RestUtils.Headers#CONTENT_LENGTH} set is returned.
   * The value of the header {@link MockNettyMessageProcessor#CHUNK_COUNT_HEADER_NAME} is used to determine the number
   * of chunks (each equal to {@link MockNettyMessageProcessor#CHUNK}) to add to the response channel. The chunks added
   * is one more than the value of {@link MockNettyMessageProcessor#CHUNK_COUNT_HEADER_NAME}. The last chunk write is
   * checked for error.
   * <p/>
   * The {@link RestUtils.Headers#CONTENT_LENGTH} is equal to the value in
   * {@link MockNettyMessageProcessor#CHUNK_COUNT_HEADER_NAME} times the length of
   * {@link MockNettyMessageProcessor#CHUNK}
   */
  WriteMoreThanContentLength, /**
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

  // CHUNK and CHUNK_COUNT HEADER_NAME together help in Content-Length tests.
  // If a test sets CHUNK_COUNT_HEADER_NAME to 3,
  // 1. The Content-Length is set to 3 * CHUNK.length
  // 2. The content is sent in two chunks both of which contain the same data.
  // 3. The last chunk will be sent as LastHttpContent.
  static final byte[] CHUNK = TestUtils.getRandomBytes(1024);
  static final String CHUNK_COUNT_HEADER_NAME = "chunkCount";

  // the write callbacks to verify if any. This is reset at the beginning of every request.
  final List<ChannelWriteCallback> writeCallbacksToVerify = new ArrayList<>();

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
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj) throws Exception {
    if (obj != null && obj instanceof HttpRequest) {
      if (obj.decoderResult().isSuccess()) {
        handleRequest((HttpRequest) obj);
      } else {
        throw new RestServiceException("Malformed request received - " + obj, RestServiceErrorCode.MalformedRequest);
      }
    } else if (obj != null && obj instanceof HttpContent) {
      handleContent((HttpContent) obj);
    } else {
      throw new RestServiceException("HttpObject received is null or not of a known type",
          RestServiceErrorCode.MalformedRequest);
    }
  }

  /**
   * Handles a {@link HttpRequest}. If content is awaited, handles some state maintenance. Else handles the request
   * according to a predefined flow based on the uri.
   * @param httpRequest the {@link HttpRequest} that needs to be handled.
   * @throws Exception
   */
  private void handleRequest(HttpRequest httpRequest) throws Exception {
    writeCallbacksToVerify.clear();
    request = new NettyRequest(httpRequest, ctx.channel(), nettyMetrics, Collections.emptySet());
    restResponseChannel = new NettyResponseChannel(ctx, nettyMetrics);
    restResponseChannel.setRequest(request);
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
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
        int chunkCount = httpRequest.headers().getInt(CHUNK_COUNT_HEADER_NAME, -1);
        if (chunkCount > 0) {
          restResponseChannel.onResponseComplete(new RestServiceException(
              "Invalid value for header : [" + CHUNK_COUNT_HEADER_NAME + "]. Can only be 0 for [/" + uri + "]",
              RestServiceErrorCode.BadRequest));
        } else if (chunkCount == 0) {
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        }
        restResponseChannel.onResponseComplete(null);
        assertEquals("ResponseStatus differs from default", ResponseStatus.Ok, restResponseChannel.getStatus());
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case FillWriteBuffer:
        WriteBufferWaterMark writeBufferWaterMark = new WriteBufferWaterMark(1, 2);
        ctx.channel().config().setWriteBufferWaterMark(writeBufferWaterMark);
        break;
      case ModifyResponseMetadataAfterWrite:
        restResponseChannel.write(ByteBuffer.wrap(new byte[0]), null);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
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
        restResponseChannel.onResponseComplete(
            new RuntimeException(TestingUri.OnResponseCompleteWithNonRestException.toString()));
        assertEquals("ResponseStatus does not reflect error", ResponseStatus.InternalServerError,
            restResponseChannel.getStatus());
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case OnResponseCompleteWithEarlyClientTermination:
        restResponseChannel.onResponseComplete(Utils.convertToClientTerminationException(new Exception()));
        assertEquals("ResponseStatus does not reflect error", ResponseStatus.InternalServerError,
            restResponseChannel.getStatus());
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case ResponseFailureMidway:
        ChannelWriteCallback callback = new ChannelWriteCallback();
        callback.setFuture(
            restResponseChannel.write(ByteBuffer.wrap(TestingUri.ResponseFailureMidway.toString().getBytes()),
                callback));
        writeCallbacksToVerify.add(callback);
        restResponseChannel.onResponseComplete(new Exception());
        // this should close the channel and the test will check for that.
        break;
      case ResponseWithContentLength:
        chunkCount = httpRequest.headers().getInt(CHUNK_COUNT_HEADER_NAME, -1);
        if (chunkCount == -1) {
          restResponseChannel.onResponseComplete(
              new RestServiceException("Request should contain header : [" + CHUNK_COUNT_HEADER_NAME + "]",
                  RestServiceErrorCode.BadRequest));
        } else {
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, chunkCount * CHUNK.length);
          if (chunkCount == 0) {
            // special case check
            callback = new ChannelWriteCallback();
            callback.setFuture(restResponseChannel.write(ByteBuffer.allocate(0), callback));
            writeCallbacksToVerify.add(callback);
          } else {
            for (int i = 0; i < chunkCount; i++) {
              callback = new ChannelWriteCallback();
              callback.setFuture(restResponseChannel.write(ByteBuffer.wrap(CHUNK), callback));
              writeCallbacksToVerify.add(callback);
            }
          }
          restResponseChannel.onResponseComplete(null);
        }
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case SetNullHeader:
        setNullHeaders();
        break;
      case SetRequest:
        setRequestTest();
        break;
      case SetStatus:
        restResponseChannel.setStatus(ResponseStatus.valueOf(httpRequest.headers().get(STATUS_HEADER_NAME)));
        restResponseChannel.onResponseComplete(null);
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case WriteAfterClose:
        restResponseChannel.close();
        assertFalse("Request channel is not closed", request.isOpen());
        callback = new ChannelWriteCallback();
        callback.setFuture(
            restResponseChannel.write(ByteBuffer.wrap(TestingUri.WriteAfterClose.toString().getBytes()), callback));
        writeCallbacksToVerify.add(callback);
        break;
      case WriteFailureWithThrowable:
        callback = new ChannelWriteCallback();
        callback.setFuture(
            restResponseChannel.write(ByteBuffer.wrap(TestingUri.WriteFailureWithThrowable.toString().getBytes()),
                callback));
        writeCallbacksToVerify.add(callback);
        break;
      case WriteMoreThanContentLength:
        chunkCount = httpRequest.headers().getInt(CHUNK_COUNT_HEADER_NAME, -1);
        if (chunkCount == -1) {
          restResponseChannel.onResponseComplete(
              new RestServiceException("Request should contain header : [" + CHUNK_COUNT_HEADER_NAME + "]",
                  RestServiceErrorCode.BadRequest));
        } else {
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, chunkCount * CHUNK.length);
          // write one more chunk than required.
          for (int i = 0; i <= chunkCount; i++) {
            callback = new ChannelWriteCallback();
            callback.setFuture(restResponseChannel.write(ByteBuffer.wrap(CHUNK), callback));
            writeCallbacksToVerify.add(callback);
          }
          restResponseChannel.onResponseComplete(null);
        }
        break;
    }
  }

  /**
   * Handles a {@link HttpContent}. Checks state and echoes back the content.
   * @param httpContent the {@link HttpContent} that needs to be handled.
   * @throws Exception
   */
  private void handleContent(HttpContent httpContent) throws Exception {
    if (request != null) {
      boolean isLast = httpContent instanceof LastHttpContent;
      ByteBuffer content = ByteBuffer.wrap(httpContent.content().array());
      ChannelWriteCallback callback = new ChannelWriteCallback();
      callback.setFuture(restResponseChannel.write(content, callback));
      writeCallbacksToVerify.add(callback);
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
  private void copyHeaders(HttpRequest httpRequest) throws ParseException, RestServiceException {
    restResponseChannel.setStatus(ResponseStatus.Accepted);
    assertEquals("ResponseStatus differs from what was set", ResponseStatus.Accepted, restResponseChannel.getStatus());

    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE,
        httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE));
    assertEquals("Value of [" + RestUtils.Headers.CONTENT_TYPE + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE),
        restResponseChannel.getHeader(HttpHeaderNames.CONTENT_TYPE.toString()));

    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH,
        Long.parseLong(httpRequest.headers().get(HttpHeaderNames.CONTENT_LENGTH)));
    assertEquals("Value of [" + RestUtils.Headers.CONTENT_LENGTH + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.CONTENT_LENGTH),
        restResponseChannel.getHeader(HttpHeaderNames.CONTENT_LENGTH.toString()));

    restResponseChannel.setHeader(RestUtils.Headers.LOCATION, httpRequest.headers().get(HttpHeaderNames.LOCATION));
    assertEquals("Value of [" + RestUtils.Headers.LOCATION + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.LOCATION),
        restResponseChannel.getHeader(HttpHeaderNames.LOCATION.toString()));

    restResponseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED,
        new Date(httpRequest.headers().getTimeMillis(HttpHeaderNames.LAST_MODIFIED)));
    assertEquals("Value of [" + RestUtils.Headers.LAST_MODIFIED + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.LAST_MODIFIED),
        restResponseChannel.getHeader(HttpHeaderNames.LAST_MODIFIED.toString()));

    restResponseChannel.setHeader(RestUtils.Headers.EXPIRES,
        new Date(httpRequest.headers().getTimeMillis(HttpHeaderNames.EXPIRES)));
    assertEquals("Value of [" + RestUtils.Headers.EXPIRES + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.EXPIRES),
        restResponseChannel.getHeader(HttpHeaderNames.EXPIRES.toString()));

    restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL,
        httpRequest.headers().get(HttpHeaderNames.CACHE_CONTROL));
    assertEquals("Value of [" + RestUtils.Headers.CACHE_CONTROL + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.CACHE_CONTROL),
        restResponseChannel.getHeader(HttpHeaderNames.CACHE_CONTROL.toString()));

    restResponseChannel.setHeader(RestUtils.Headers.PRAGMA, httpRequest.headers().get(HttpHeaderNames.PRAGMA));
    assertEquals("Value of [" + RestUtils.Headers.PRAGMA + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.PRAGMA),
        restResponseChannel.getHeader(HttpHeaderNames.PRAGMA.toString()));

    restResponseChannel.setHeader(RestUtils.Headers.DATE,
        new Date(httpRequest.headers().getTimeMillis(HttpHeaderNames.DATE)));
    assertEquals("Value of [" + RestUtils.Headers.DATE + "] differs from what was set",
        httpRequest.headers().get(HttpHeaderNames.DATE),
        restResponseChannel.getHeader(HttpHeaderNames.DATE.toString()));

    restResponseChannel.setHeader(CUSTOM_HEADER_NAME, httpRequest.headers().get(CUSTOM_HEADER_NAME));
    assertEquals("Value of [" + CUSTOM_HEADER_NAME + "] differs from what was set",
        httpRequest.headers().get(CUSTOM_HEADER_NAME), restResponseChannel.getHeader(CUSTOM_HEADER_NAME));
  }

  /**
   * Tries to set null headers in the {@link NettyResponseChannel}. If the operation does not fail, reports an error.
   * @throws RestServiceException
   */
  private void setNullHeaders() throws RestServiceException {
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
  private void setRequestTest() throws RestServiceException {
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
  private final Exception exceptionToThrow;

  public ExceptionOutboundHandler(Exception exceptionToThrow) {
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    throw exceptionToThrow;
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
  private Future<Long> future;

  @Override
  public void onCompletion(Long result, Exception exception) {
    this.result = result;
    this.exception = exception;
    callbackReceived.countDown();
  }

  /**
   * Set the {@link Future} associated with the write for which this object is a callback.
   * @param future the {@link Future} associated with the write for which this object is a callback.
   */
  void setFuture(Future<Long> future) {
    this.future = future;
  }

  /**
   * Compares the data obtained from the callback with the data obtained from future.
   * @throws InterruptedException
   * @throws TimeoutException
   */
  void compareWithFuture() throws InterruptedException, TimeoutException {
    Long futureResult = null;
    Exception futureException = null;
    try {
      futureResult = future.get(1, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      futureException = e;
    }

    if (!callbackReceived.await(1, TimeUnit.MILLISECONDS)) {
      throw new IllegalStateException("Callback has not been invoked even though future.get() has returned");
    } else {
      if (futureException == null) {
        assertEquals("Future and callback results don't match", futureResult, result);
        assertNull("There should have been no exception in the callback", exception);
      } else {
        assertEquals("Future and callback exceptions don't match", Utils.getRootCause(exception).getMessage(),
            Utils.getRootCause(futureException).getMessage());
      }
    }
  }
}
