package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.restservice.RestRequestMetadata;
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
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link NettyResponseHandler}.
 * <p/>
 * To examine functionality of each URI, refer to {@link MockNettyMessageProcessor#handleRequest(HttpRequest)} and
 * {@link MockNettyMessageProcessor#handleContent(HttpContent)}
 */
public class NettyResponseHandlerTest {
  /**
   * Tests the common workflow of the {@link NettyResponseHandler} i.e., add some content to response body via
   * {@link NettyResponseHandler#addToResponseBody(byte[], boolean)} and then
   * {@link NettyResponseHandler#flush()} (For the actual functionality check {@link MockNettyMessageProcessor}).
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void responseHandlerCommonCaseTest()
      throws IOException, JSONException {
    String content = "@@randomContent@@@";
    String lastContent = "@@randomLastContent@@@";
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createRequest(HttpMethod.GET, "/"));
    channel.writeInbound(createContent(content, false));
    channel.writeInbound(createContent(lastContent, true));

    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
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
   * server. This should return just response metadata.
   * @throws JSONException
   */
  @Test
  public void responseHandlerNoBodyTest()
      throws JSONException {
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createRequest(HttpMethod.GET, MockNettyMessageProcessor.IMMEDIATE_FLUSH_AND_CLOSE_URI));
    // There should be a response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    // Channel should be closed.
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  /**
   * Checks {@link com.github.ambry.restservice.RestResponseHandler#onRequestComplete(Throwable, boolean)}
   * with a valid {@link RestServiceException} and with a null exception.
   * @throws JSONException
   */
  @Test
  public void onRequestCompleteTest()
      throws JSONException {
    // Throws RestException. There should be a response which is BAD_REQUEST. This is the expected response.
    doOnRequestCompleteTest(MockNettyMessageProcessor.ON_REQUEST_COMPLETE_WITH_REST_EXCEPTION,
        HttpResponseStatus.BAD_REQUEST);

    // Exception is null.
    doOnRequestCompleteTest(MockNettyMessageProcessor.ON_REQUEST_COMPLETE_WTH_NULL_EXCEPTION, HttpResponseStatus.OK);
  }

  /**
   * Performs bad state transitions and verifies that they throw the right exceptions.
   * @throws JSONException
   */
  @Test
  public void badStateTransitionsTest()
      throws JSONException {
    // write after close.
    doBadStateTransitionTest(MockNettyMessageProcessor.WRITE_AFTER_CLOSE_URI,
        RestServiceErrorCode.ChannelAlreadyClosed);

    // modify response data after it has been written to the channel
    doBadStateTransitionTest(MockNettyMessageProcessor.MODIFY_RESPONSE_METADATA_AFTER_WRITE_URI,
        RestServiceErrorCode.IllegalResponseMetadataStateTransition);
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
   * @throws IOException
   */
  private String getContentString(HttpContent httpContent)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    httpContent.content().readBytes(out, httpContent.content().readableBytes());
    return out.toString("UTF-8");
  }

  // onRequestCompleteTest() helpers

  /**
   * Creates a channel and send the request to the {@link EmbeddedChannel}. Checks the response for the expected
   * status code.
   * @param uri - the uri to hit.
   * @param expectedResponseStatus
   * @throws JSONException
   */
  private void doOnRequestCompleteTest(String uri, HttpResponseStatus expectedResponseStatus)
      throws JSONException {
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createRequest(HttpMethod.GET, uri));

    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", expectedResponseStatus, response.getStatus());
    // Channel should be closed.
    assertFalse("Channel not closed on the server", channel.isActive());
  }

  // badStateTransitionsTest() helpers

  /**
   * Creates a channel and sends the request to the {@link EmbeddedChannel}. Checks for an exception and verifies the
   * {@link RestServiceErrorCode}.
   * @param uri
   * @param expectedCode
   * @throws JSONException
   */
  private void doBadStateTransitionTest(String uri, RestServiceErrorCode expectedCode)
      throws JSONException {
    MockNettyMessageProcessor processor = new MockNettyMessageProcessor();
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    try {
      channel.writeInbound(createRequest(HttpMethod.GET, uri));
      fail("This test was expecting the handler in the channel to throw a RestServiceException with error code "
          + expectedCode);
    } catch (Exception e) {
      RestServiceException re = (RestServiceException) e;
      assertEquals("Unexpected RestServiceErrorCode", expectedCode, re.getErrorCode());
    }
  }
}

/**
 * A test handler that forms the pipeline of the {@link EmbeddedChannel} used in tests.
 * Exposes some URI strings through which a predefined flow can be executed and verified.
 */
class MockNettyMessageProcessor extends SimpleChannelInboundHandler<HttpObject> {
  protected static String IMMEDIATE_FLUSH_AND_CLOSE_URI = "immediateFlushAndClose";
  protected static String ON_REQUEST_COMPLETE_WITH_REST_EXCEPTION = "onRequestCompleteWithRestException";
  protected static String ON_REQUEST_COMPLETE_WTH_NULL_EXCEPTION = "onRequestCompleteWithNullException";
  protected static String WRITE_AFTER_CLOSE_URI = "writeAfterClose";
  protected static String MODIFY_RESPONSE_METADATA_AFTER_WRITE_URI = "modifyResponseMetadataAfterWrite";

  private RestRequestMetadata request;
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
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws RestServiceException {
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
   * Handles a {@link HttpRequest}. Does some state maintenance before moving forward or handles the request
   * immediately.
   * @param httpRequest - the {@link HttpRequest} that needs to be handled.
   * @throws RestServiceException
   */
  private void handleRequest(HttpRequest httpRequest)
      throws RestServiceException {
    if (request == null) {
      request = new NettyRequestMetadata(httpRequest);
      restResponseHandler.setContentType("text/plain; charset=UTF-8");
      if (IMMEDIATE_FLUSH_AND_CLOSE_URI.equals(request.getUri())) {
        restResponseHandler.flush();
        restResponseHandler.onRequestComplete(null, false);
      } else if (ON_REQUEST_COMPLETE_WITH_REST_EXCEPTION.equals(request.getUri())) {
        restResponseHandler.onRequestComplete(
            new RestServiceException(ON_REQUEST_COMPLETE_WITH_REST_EXCEPTION, RestServiceErrorCode.BadRequest), false);
      } else if (ON_REQUEST_COMPLETE_WTH_NULL_EXCEPTION.equals(request.getUri())) {
        restResponseHandler.onRequestComplete(null, false);
      } else if (WRITE_AFTER_CLOSE_URI.equals(request.getUri())) {
        restResponseHandler.onRequestComplete(null, false);
        // write something. It should fail.
        restResponseHandler.addToResponseBody(WRITE_AFTER_CLOSE_URI.getBytes(), true);
      } else if (MODIFY_RESPONSE_METADATA_AFTER_WRITE_URI.equals(request.getUri())) {
        restResponseHandler.flush();
        restResponseHandler.setContentType("text/plain; charset=UTF-8");
      }
    } else {
      restResponseHandler.onRequestComplete(null, false);
    }
  }

  /**
   * Handles a {@link HttpContent}. Checks state and echoes back the content.
   * @param httpContent - the {@link HttpContent} that needs to be handled.
   * @throws RestServiceException
   */
  private void handleContent(HttpContent httpContent)
      throws RestServiceException {
    if (request != null) {
      boolean isLast = httpContent instanceof LastHttpContent;
      restResponseHandler.addToResponseBody(httpContent.content().array(), isLast);
      if (isLast) {
        restResponseHandler.flush();
        restResponseHandler.onRequestComplete(null, false);
      }
    } else {
      throw new RestServiceException("Received data without a request", RestServiceErrorCode.NoRequest);
    }
  }
}
