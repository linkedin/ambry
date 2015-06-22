package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.restservice.RestResponseHandler;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.json.JSONException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * Tests functionality of {@link NettyResponseHandler}.
 */
public class NettyResponseHandlerTest {
  // TODO: More tests will be added.

  /**
   * Tests the common workflow of the {@link NettyResponseHandler} i.e., add some content to response body via
   * {@link NettyResponseHandler#addToResponseBody(byte[], boolean)} and then
   * {@link NettyResponseHandler#flush()} (For the actual functionality check {@link MockNettyMessageProcessor}).
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void responseHandlerCommonCaseTest()
      throws Throwable {
    String content = "@@randomContent@@@";
    String lastContent = "@@randomLastContent@@@";

    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createRequest(HttpMethod.GET, "/"));
    channel.writeInbound(createContent(content, false));
    channel.writeInbound(createContent(lastContent, true));

    if (processor.getCause() != null) {
      throw processor.getCause();
    }

    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Response status in not ok", HttpResponseStatus.OK, response.getStatus());
    // content echoed back.
    String returnedContent = getContentString((HttpContent) channel.readOutbound());
    assertEquals("Content does not match with expected content", content, returnedContent);
    // last content echoed back.
    returnedContent = getContentString((HttpContent) channel.readOutbound());
    assertEquals("Content does not match with expected content", lastContent, returnedContent);

    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Checks the case where no body needs to be returned but just a {@link NettyResponseHandler#flush()} is called on the
   * server. This should return just response metadata
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void responseHandlerNoBodyTest()
      throws Throwable {
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createRequest(HttpMethod.GET, MockNettyMessageProcessor.IMMEDIATE_FLUSH_AND_CLOSE_URI));

    if (processor.getCause() != null) {
      throw processor.getCause();
    }

    // There should be a response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Response status in not ok", HttpResponseStatus.OK, response.getStatus());
    // Channel should be closed.
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  // helpers
  // general
  private HttpRequest createRequest(HttpMethod httpMethod, String uri)
      throws JSONException {
    return new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
  }

  private HttpContent createContent(String content, boolean isLast) {
    ByteBuf buf = Unpooled.copiedBuffer(content.getBytes());
    if (isLast) {
      return new DefaultLastHttpContent(buf);
    } else {
      return new DefaultHttpContent(buf);
    }
  }

  /**
   * Converts the content in {@link HttpContent} to a human readable string.
   * @param httpContent
   * @return
   * @throws java.io.IOException
   */
  private String getContentString(HttpContent httpContent)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    httpContent.content().readBytes(out, httpContent.content().readableBytes());
    return out.toString("UTF-8");
  }
}

/**
 * A test handler that forms the pipeline of the {@link EmbeddedChannel} used in tests.
 * Exposes some URI strings through which a predefined flow can be executed and verified.
 */
class MockNettyMessageProcessor extends SimpleChannelInboundHandler<HttpObject> {
  protected static String IMMEDIATE_FLUSH_AND_CLOSE_URI = "immediateFlushAndClose";

  private Throwable cause = null;
  private HttpRequest request;
  private RestResponseHandler restResponseHandler;

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    restResponseHandler = new NettyResponseHandler(ctx, new NettyMetrics(new MetricRegistry()));
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    request = null;
    restResponseHandler = null;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    this.cause = cause;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws RestServiceException {
    if (obj != null && obj instanceof HttpRequest) {
      HttpRequest httpRequest = (HttpRequest) obj;
      if (httpRequest.getDecoderResult().isSuccess()) {
        handleRequest(httpRequest);
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
   * Returns the cause of error if there was one. Otherwise null.
   * @return - the cause of the error if there was one. Otherwise null.
   */
  public Throwable getCause() {
    return cause;
  }

  /**
   * Handles a HttpRequest. Does some state maintenance before moving forward.
   * @param httpRequest
   * @throws RestServiceException
   */
  private void handleRequest(HttpRequest httpRequest)
      throws RestServiceException {
    if (request == null) {
      request = httpRequest;
      restResponseHandler.setContentType("text/plain");
      if (IMMEDIATE_FLUSH_AND_CLOSE_URI.equals(request.getUri())) {
        restResponseHandler.flush();
        restResponseHandler.close();
      }
    } else {
      throw new RestServiceException(
          "Received duplicate request. Old request - " + request + ". New request - " + httpRequest,
          RestServiceErrorCode.DuplicateRequest);
    }
  }

  /**
   * Handles a HttpContent. Checks state before moving forward
   * @param httpContent
   * @throws RestServiceException
   */
  private void handleContent(HttpContent httpContent)
      throws RestServiceException {
    if (request != null) {
      byte[] content = httpContent.content().array();
      boolean isLast = httpContent instanceof LastHttpContent;

      restResponseHandler.addToResponseBody(content, isLast);
      if (isLast) {
        restResponseHandler.flush();
        restResponseHandler.close();
      }
    } else {
      throw new RestServiceException("Received data without a request", RestServiceErrorCode.NoRequest);
    }
  }
}
