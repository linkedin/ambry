package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.Router;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * Unit tests for {@link NettyMessageProcessor}.
 */
public class NettyMessageProcessorTest {
  private final Router router;
  private final BlobStorageService blobStorageService;
  private final MockRestRequestResponseHandler requestHandler;

  /**
   * Sets up the mock services that {@link NettyMessageProcessor} can use.
   * @throws InstantiationException
   * @throws IOException
   */
  public NettyMessageProcessorTest()
      throws InstantiationException, IOException {
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    RestRequestMetricsTracker.setDefaults(new MetricRegistry());
    router = new InMemoryRouter(verifiableProperties);
    requestHandler = new MockRestRequestResponseHandler();
    blobStorageService = new MockBlobStorageService(verifiableProperties, requestHandler, router);
    requestHandler.setBlobStorageService(blobStorageService);
    blobStorageService.start();
    requestHandler.start();
  }

  /**
   * Clean up task.
   */
  @After
  public void cleanUp()
      throws IOException {
    blobStorageService.shutdown();
    router.close();
  }

  /**
   * Tests for the common case request handling flow.
   * @throws IOException
   */
  @Test
  public void requestHandleWithGoodInputTest()
      throws IOException {
    doRequestHandleWithGoodInputTest(HttpMethod.GET, RestMethod.GET);
    doRequestHandleWithGoodInputTest(HttpMethod.POST, RestMethod.POST);
    doRequestHandleWithGoodInputTest(HttpMethod.DELETE, RestMethod.DELETE);
    doRequestHandleWithGoodInputTest(HttpMethod.HEAD, RestMethod.HEAD);
  }

  /**
   * Tests for error handling flow when bad input streams are provided to the {@link NettyMessageProcessor}.
   */
  @Test
  public void requestHandleWithBadInputTest() {
    // content without request.
    String content = "@@randomContent@@@";
    EmbeddedChannel channel = createChannel();
    channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content.getBytes())));
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.getStatus());

    // content when no content is expected.
    content = "@@randomContent@@@";
    channel = createChannel();
    channel.writeInbound(createRequest(HttpMethod.GET, "/"));
    channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content.getBytes())));
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.getStatus());

    // wrong HTTPObject.
    channel = createChannel();
    channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.getStatus());
  }

  /**
   * Tests for error handling flow when the {@link RestRequestHandler} throws exceptions.
   */
  @Test
  public void requestHandlerExceptionTest() {
    try {
      // RuntimeException
      Properties properties = new Properties();
      properties.setProperty(MockRestRequestResponseHandler.RUNTIME_EXCEPTION_ON_HANDLE, "true");
      requestHandler.breakdown(new VerifiableProperties(properties));
      doRequestHandlerExceptionTest(HttpMethod.GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);

      // RestServiceException
      properties.clear();
      properties.setProperty(MockRestRequestResponseHandler.REST_EXCEPTION_ON_HANDLE,
          RestServiceErrorCode.InternalServerError.toString());
      requestHandler.breakdown(new VerifiableProperties(properties));
      doRequestHandlerExceptionTest(HttpMethod.GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);

      // ClosedChannelException
      properties.clear();
      properties.setProperty(MockRestRequestResponseHandler.CLOSE_REQUEST_ON_HANDLE, "true");
      requestHandler.breakdown(new VerifiableProperties(properties));
      doRequestHandlerExceptionTest(HttpMethod.POST, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } finally {
      requestHandler.fix();
    }
  }

  // helpers
  // general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link NettyMessageProcessor}.
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link NettyMessageProcessor}.
   */
  private EmbeddedChannel createChannel() {
    NettyMetrics nettyMetrics = new NettyMetrics(new MetricRegistry());
    NettyConfig nettyConfig = new NettyConfig(new VerifiableProperties(new Properties()));
    NettyMessageProcessor processor = new NettyMessageProcessor(nettyMetrics, nettyConfig, requestHandler);
    return new EmbeddedChannel(new ChunkedWriteHandler(), processor);
  }

  /**
   * Creates a {@link HttpRequest} with the given parameters.
   * @param httpMethod the {@link HttpMethod} required.
   * @param uri the URI to hit.
   * @return a {@link HttpRequest} with the given parameters.
   */
  private HttpRequest createRequest(HttpMethod httpMethod, String uri) {
    return new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
  }

  /**
   * Converts the content in {@code httpContent} to a human readable string.
   * @param httpContent the {@link HttpContent} whose content needs to be converted to a human readable string.
   * @return content that is inside {@code httpContent} as a human readable string.
   * @throws IOException
   */
  private String getContentString(HttpContent httpContent)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    httpContent.content().readBytes(out, httpContent.content().readableBytes());
    return out.toString("UTF-8");
  }

  // requestHandleWithGoodInputTest() helpers

  /**
   * Does a test to see that request handling with good input succeeds.
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param restMethod the equivalent {@link RestMethod} for {@code httpMethod}. Used to check for correctness of
   *                   response.
   * @throws IOException
   */
  private void doRequestHandleWithGoodInputTest(HttpMethod httpMethod, RestMethod restMethod)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    channel.writeInbound(createRequest(httpMethod, MockBlobStorageService.ECHO_REST_METHOD));
    if (httpMethod != HttpMethod.POST) {
      // For POST, adding LastHttpContent will throw an exception simply because of the way ECHO_REST_METHOD works in
      // MockBlobStorageService (doesn't wait for content and closes the RestRequest once response is written). Except
      // POST, no one has this problem because NettyMessageProcessor waits for LastHttpContent.
      channel.writeInbound(new DefaultLastHttpContent());
    }
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    // MockBlobStorageService echoes the RestMethod.
    assertEquals("Unexpected content", restMethod.toString(), getContentString((HttpContent) channel.readOutbound()));
    assertFalse("Channel not closed", channel.isOpen());
  }

  // requestHandlerExceptionTest() helpers.

  /**
   * Does a test where the request handler inside {@link NettyMessageProcessor} fails. Checks for the right error code
   * in the response.
   * @param httpMethod the {@link HttpMethod} to use for the request.
   * @param expectedStatus the excepted {@link HttpResponseStatus} in the response.
   */
  private void doRequestHandlerExceptionTest(HttpMethod httpMethod, HttpResponseStatus expectedStatus) {
    EmbeddedChannel channel = createChannel();
    channel.writeInbound(createRequest(httpMethod, "/"));
    channel.writeInbound(new DefaultLastHttpContent());
    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", expectedStatus, response.getStatus());
  }
}
