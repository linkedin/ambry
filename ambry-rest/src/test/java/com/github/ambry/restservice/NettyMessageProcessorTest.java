package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Unit tests for {@link NettyMessageProcessor}.
 */
public class NettyMessageProcessorTest {
  private static MockRestRequestHandlerController requestHandlerController;

  /**
   * Sets up the {@link MockRestRequestHandlerController} that {@link NettyMessageProcessor} can use.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void startRequestHandlerController()
      throws InstantiationException, IOException {
    BlobStorageService blobStorageService = new MockBlobStorageService(new MockClusterMap());
    requestHandlerController = new MockRestRequestHandlerController(1, blobStorageService);
    requestHandlerController.start();
  }

  /**
   * Shuts down the {@link MockRestRequestHandlerController}.
   */
  @AfterClass
  public static void shutdownRequestHandlerController() {
    requestHandlerController.shutdown();
  }

  /**
   * Tests for the common case request handling flow.
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void requestHandleWithGoodInputTest()
      throws IOException, JSONException {
    String content = "@@randomContent@@@";
    String lastContent = "@@randomLastContent@@@";

    NettyMessageProcessor processor =
        new NettyMessageProcessor(new NettyMetrics(new MetricRegistry()), requestHandlerController);
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createRequest(HttpMethod.GET, "/"));
    channel.writeInbound(createContent(content, false));
    channel.writeInbound(createContent(lastContent, true));

    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    assertEquals("Unexpected content", RestMethod.GET.toString(),
        getContentString((HttpContent) channel.readOutbound()));
    // content echoed back.
    String returnedContent = getContentString((HttpContent) channel.readOutbound());
    assertEquals("Content does not match with expected content", content, returnedContent);
    // last content echoed back.
    returnedContent = getContentString((HttpContent) channel.readOutbound());
    assertEquals("Content does not match with expected content", lastContent, returnedContent);
  }

  /**
   * Tests the exceptions thrown on failure of tasks performed by
   * {@link NettyMessageProcessor#channelActive(io.netty.channel.ChannelHandlerContext)}.
   * @throws JSONException
   */
  @Test
  public void channelActiveTasksFailureTest()
      throws JSONException {
    Properties properties = new Properties();
    properties.setProperty(MockRestRequestHandlerController.RETURN_NULL_ON_GET_REQUEST_HANDLER, "true");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    // RestRequestHandler returned is null.
    requestHandlerController.breakdown(verifiableProperties);
    doChannelActiveTasksFailureTest();
    properties.setProperty(MockRestRequestHandlerController.RETURN_NULL_ON_GET_REQUEST_HANDLER, "false");
    verifiableProperties = new VerifiableProperties(properties);
    // RestException is thrown when a call is made to getRequestHandler().
    requestHandlerController.breakdown(verifiableProperties);
    doChannelActiveTasksFailureTest();
    requestHandlerController.fix();
  }

  /**
   * Tests for error handling flow when bad input streams are provided to the {@link NettyMessageProcessor}.
   */
  @Test
  public void requestHandleWithBadInputTest() {
    // content without request.
    String content = "@@randomContent@@@";
    NettyMessageProcessor processor =
        new NettyMessageProcessor(new NettyMetrics(new MetricRegistry()), requestHandlerController);
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createContent(content, true));
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.getStatus());

    // wrong HTTPObject.
    processor = new NettyMessageProcessor(new NettyMetrics(new MetricRegistry()), requestHandlerController);
    channel = new EmbeddedChannel(processor);
    channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.getStatus());
  }

  /**
   * Tests for error handling flow when the {@link com.github.ambry.restservice.RestRequestHandler} throws exceptions.
   * @throws JSONException
   */
  @Test
  public void requestHandlerExceptionTest()
      throws JSONException {
    doRequestHandlerExceptionTest(MockBlobStorageService.OPERATION_THROW_HANDLING_REST_EXCEPTION,
        HttpResponseStatus.INTERNAL_SERVER_ERROR);
    doRequestHandlerExceptionTest(MockBlobStorageService.OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION,
        HttpResponseStatus.INTERNAL_SERVER_ERROR);
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

  // channelActiveTasksFailureTest() helpers.
  private void doChannelActiveTasksFailureTest() {
    NettyMessageProcessor processor =
        new NettyMessageProcessor(new NettyMetrics(new MetricRegistry()), requestHandlerController);
    // throws when channelActive() is called.
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.INTERNAL_SERVER_ERROR, response.getStatus());
  }

  // requestHandlerExceptionTest() helpers.
  private void doRequestHandlerExceptionTest(String uri, HttpResponseStatus expectedStatus)
      throws JSONException {
    NettyMessageProcessor processor =
        new NettyMessageProcessor(new NettyMetrics(new MetricRegistry()), requestHandlerController);
    EmbeddedChannel channel = new EmbeddedChannel(processor);
    channel.writeInbound(createRequest(HttpMethod.GET, uri));
    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", expectedStatus, response.getStatus());
  }
}
