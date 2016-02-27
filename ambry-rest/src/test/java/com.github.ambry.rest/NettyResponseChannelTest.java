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
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.text.ParseException;
import java.util.Date;
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
      HttpRequest httpRequest = createRequest(HttpMethod.POST, "/");
      HttpHeaders.setKeepAlive(httpRequest, isKeepAlive);
      channel.writeInbound(httpRequest);
      channel.writeInbound(createContent(contentToSend, false));
      channel.writeInbound(createContent(lastContent, true));
      // first outbound has to be response.
      HttpResponse response = (HttpResponse) channel.readOutbound();
      assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
      // content echoed back.
      String returnedContent = getContentString((HttpContent) channel.readOutbound());
      assertEquals("Content does not match with expected content", contentToSend, returnedContent);
      // last content echoed back.
      returnedContent = getContentString((HttpContent) channel.readOutbound());
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
    channel.writeInbound(createRequest(HttpMethod.GET, TestingUri.ImmediateResponseComplete.toString()));
    // There should be a response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    // Channel should be closed.
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Checks behaviour of {@link RestResponseChannel#onResponseComplete(Exception)} with valid
   * {@link RestServiceException}s and a {@link RuntimeException}.
   */
  @Test
  public void onResponseCompleteWithExceptionTest() {
    // Throws BadRequest RestServiceException. There should be a BAD_REQUEST HTTP response.
    doOnResponseCompleteWithExceptionTest(TestingUri.OnResponseCompleteWithBadRequest, HttpResponseStatus.BAD_REQUEST);

    // Throws InternalServerError RestServiceException. There should be a INTERNAL_SERVER_ERROR HTTP response.
    doOnResponseCompleteWithExceptionTest(TestingUri.OnResponseCompleteWithInternalServerError,
        HttpResponseStatus.INTERNAL_SERVER_ERROR);

    // Throws RuntimeException. There should be a INTERNAL_SERVER_ERROR HTTP response.
    doOnResponseCompleteWithExceptionTest(TestingUri.OnResponseCompleteWithNonRestException,
        HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
    onResponseCompleteUnderWriteFailureTest(TestingUri.OnResponseCompleteWithBadRequest);

    // writing to channel with a outbound handler that generates an Exception
    try {
      String content = "@@randomContent@@@";
      MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
      ChannelOutboundHandler badOutboundHandler = new ExceptionOutboundHandler();
      EmbeddedChannel channel = new EmbeddedChannel(badOutboundHandler, processor);
      channel.writeInbound(createRequest(HttpMethod.GET, "/"));
      // channel gets closed because of write failure
      channel.writeInbound(createContent(content, true));
      fail("Callback for write would have thrown an Exception");
    } catch (Exception e) {
      assertEquals("Exception not as expected", ExceptionOutboundHandler.EXCEPTION_MESSAGE, e.getMessage());
    }

    // writing to channel with a outbound handler that generates an Error
    EmbeddedChannel channel = new EmbeddedChannel(new ErrorOutboundHandler(), new MockNettyMessageProcessor());
    try {
      channel.writeInbound(createRequest(HttpMethod.GET, TestingUri.WriteFailureWithThrowable.toString()));
    } catch (Error e) {
      assertEquals("Unexpected error", ErrorOutboundHandler.ERROR_MESSAGE, e.getMessage());
    }

    channel = createEmbeddedChannel();
    channel.writeInbound(createRequest(HttpMethod.GET, TestingUri.ResponseFailureMidway.toString()));
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
    channel.writeInbound(createRequest(HttpMethod.GET, TestingUri.FillWriteBuffer.toString()));
    channel.writeInbound(createContent(content, false));
    channel.writeInbound(createContent(lastContent, true));

    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    // content echoed back.
    StringBuilder returnedContent = new StringBuilder();
    while (returnedContent.length() < content.length()) {
      returnedContent.append(getContentString((HttpContent) channel.readOutbound()));
    }
    assertEquals("Content does not match with expected content", content, returnedContent.toString());
    // last content echoed back.
    StringBuilder returnedLastContent = new StringBuilder();
    while (returnedLastContent.length() < lastContent.length()) {
      returnedLastContent.append(getContentString((HttpContent) channel.readOutbound()));
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
    HttpRequest request = createRequestWithHeaders(HttpMethod.GET, TestingUri.SetRequestTest.toString());
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(request);

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.getStatus());
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  // helpers
  // general

  /**
   * Creates a {@link HttpRequest} with the specified {@code httpMethod} and {@code uri}.
   * @param httpMethod the {@link HttpMethod} required.
   * @param uri the URI to hit.
   * @return a {@link HttpRequest} with the specified {@code httpMethod} and {@code uri}.
   */
  private HttpRequest createRequest(HttpMethod httpMethod, String uri) {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
    HttpHeaders.setKeepAlive(request, false);
    return request;
  }

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
   * Converts the content in {@link HttpContent} to a human readable string.
   * @param httpContent the content that needs to be converted to a human readable string.
   * @return {@code httpContent} as a human readable string.
   * @throws IOException
   */
  private String getContentString(HttpContent httpContent)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    httpContent.content().readBytes(out, httpContent.content().readableBytes());
    return out.toString("UTF-8");
  }

  // onResponseCompleteWithExceptionTest() helpers

  /**
   * Creates a channel and sends the request to the {@link EmbeddedChannel}. Checks the response for the expected
   * status code.
   * @param uri the uri to hit.
   * @param expectedResponseStatus the response status that is expected.
   */
  private void doOnResponseCompleteWithExceptionTest(TestingUri uri, HttpResponseStatus expectedResponseStatus) {
    EmbeddedChannel channel = createEmbeddedChannel();
    channel.writeInbound(createRequest(HttpMethod.GET, uri.toString()));

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", expectedResponseStatus, response.getStatus());
    // Channel should be closed.
    assertFalse("Channel not closed on the server", channel.isActive());
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
      channel.writeInbound(createRequest(HttpMethod.GET, uri.toString()));
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
    channel.writeInbound(createRequest(HttpMethod.GET, uri.toString()));
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
    channel.writeInbound(createRequest(HttpMethod.GET, uri.toString()));
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
    HttpRequest request = createRequest(httpMethod, uri);
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
}

/**
 * List of all the testing URIs.
 */
enum TestingUri {
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
   * immediately with a {@link RestServiceException} as {@code cause}. The exception message is the URI string and the
   * error code is {@link RestServiceErrorCode#BadRequest}.
   */
  OnResponseCompleteWithBadRequest,
  /**
   * When this request is received, {@link RestResponseChannel#onResponseComplete(Exception)} is called
   * immediately with a {@link RestServiceException} as {@code cause}. The exception message is the URI string and the
   * error code is {@link RestServiceErrorCode#InternalServerError}.
   */
  OnResponseCompleteWithInternalServerError,
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
  SetRequestTest,
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
  public static final String CUSTOM_HEADER_NAME = "customHeader";

  private ChannelHandlerContext ctx;
  private NettyRequest request;
  private NettyResponseChannel restResponseChannel;
  private NettyMetrics nettyMetrics;

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    this.ctx = ctx;
    MetricRegistry metricRegistry = new MetricRegistry();
    nettyMetrics = new NettyMetrics(metricRegistry);
    RestRequestMetricsTracker.setDefaults(metricRegistry);
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
      case CopyHeaders:
        copyHeaders(httpRequest);
        restResponseChannel.onResponseComplete(null);
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case ImmediateResponseComplete:
        restResponseChannel.onResponseComplete(null);
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
      case OnResponseCompleteWithBadRequest:
        restResponseChannel.onResponseComplete(
            new RestServiceException(TestingUri.OnResponseCompleteWithBadRequest.toString(),
                RestServiceErrorCode.BadRequest));
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case OnResponseCompleteWithInternalServerError:
        restResponseChannel.onResponseComplete(
            new RestServiceException(TestingUri.OnResponseCompleteWithInternalServerError.toString(),
                RestServiceErrorCode.InternalServerError));
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case OnResponseCompleteWithNonRestException:
        restResponseChannel
            .onResponseComplete(new RuntimeException(TestingUri.OnResponseCompleteWithNonRestException.toString()));
        assertFalse("Request channel is not closed", request.isOpen());
        break;
      case ResponseFailureMidway:
        ChannelWriteCallback callback = new ChannelWriteCallback();
        callback.compareWithFuture(
            restResponseChannel.write(ByteBuffer.wrap(TestingUri.WriteAfterClose.toString().getBytes()), callback));
        assertNull("There shouldn't have been any exceptions on the first write", callback.exception);
        restResponseChannel.onResponseComplete(new Exception());
        // this should close the channel and the test will check for that.
        break;
      case SetNullHeader:
        setNullHeaders();
        break;
      case SetRequestTest:
        setRequestTest();
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
        callback.compareWithFuture(
            restResponseChannel.write(ByteBuffer.wrap(TestingUri.WriteAfterClose.toString().getBytes()), callback));
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
    restResponseChannel
        .setHeader(RestUtils.Headers.CONTENT_TYPE, HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CONTENT_TYPE));
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH,
        Long.parseLong(HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CONTENT_LENGTH)));
    restResponseChannel
        .setHeader(RestUtils.Headers.LOCATION, HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.LOCATION));
    restResponseChannel.setHeader(RestUtils.Headers.LAST_MODIFIED,
        HttpHeaders.getDateHeader(httpRequest, HttpHeaders.Names.LAST_MODIFIED));
    restResponseChannel
        .setHeader(RestUtils.Headers.EXPIRES, HttpHeaders.getDateHeader(httpRequest, HttpHeaders.Names.EXPIRES));
    restResponseChannel.setHeader(RestUtils.Headers.CACHE_CONTROL,
        HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.CACHE_CONTROL));
    restResponseChannel
        .setHeader(RestUtils.Headers.PRAGMA, HttpHeaders.getHeader(httpRequest, HttpHeaders.Names.PRAGMA));
    restResponseChannel
        .setHeader(RestUtils.Headers.DATE, HttpHeaders.getDateHeader(httpRequest, HttpHeaders.Names.DATE));
    restResponseChannel.setHeader(CUSTOM_HEADER_NAME, HttpHeaders.getHeader(httpRequest, CUSTOM_HEADER_NAME));
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
