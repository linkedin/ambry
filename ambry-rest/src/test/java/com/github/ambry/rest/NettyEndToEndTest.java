package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.NioServer;
import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestRequestHandlerController;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Some end to end tests that exercise {@link NettyServer}, {@link NettyMessageProcessor} and
 * {@link NettyResponseHandler}.
 */
public class NettyEndToEndTest {
  private static String NETTY_SERVER_PORT = "8088";
  private static String NETTY_SERVER_ALTERNATE_PORT = "8089";
  // magic number.
  private static int RESPONSE_QUEUE_POLL_TIMEOUT_SECS = 30;

  private static RestRequestHandlerController requestHandlerController;
  private static NioServer nioServer;

  /**
   * Preps up for the tests by creating and starting a {@link NettyServer}. Called just once before all the tests.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void startNettyServer()
      throws InstantiationException, IOException {
    requestHandlerController = createRestRequestHandlerController();
    nioServer = createNettyServer(requestHandlerController, null);
    requestHandlerController.start();
    nioServer.start();
  }

  /**
   * Shuts down the {@link NettyServer} and {@link RestRequestHandlerController}. Called just once after all the tests.
   */
  @AfterClass
  public static void shutdownNettyServer() {
    nioServer.shutdown();
    requestHandlerController.shutdown();
  }

  /**
   * Test the handling of requests that are well formed.
   * <p/>
   * {@link MockBlobStorageService} just echoes back the {@link HttpMethod} equivalent {@link RestMethod}.
   * These are just tests to see that different methods are acknowledged correctly and not for actual functionality
   * (that goes into an integration test).
   * @throws Exception
   */
  @Test
  public void handleRequestSuccessTest()
      throws Exception {
    FullHttpRequest request;

    request = createRequest(HttpMethod.GET, "/");
    doHandleRequestSuccessTest(request, RestMethod.GET.toString(), NETTY_SERVER_PORT);

    request = createRequest(HttpMethod.POST, "/");
    doHandleRequestSuccessTest(request, RestMethod.POST.toString(), NETTY_SERVER_PORT);

    request = createRequest(HttpMethod.DELETE, "/");
    doHandleRequestSuccessTest(request, RestMethod.DELETE.toString(), NETTY_SERVER_PORT);

    request = createRequest(HttpMethod.HEAD, "/");
    doHandleRequestSuccessTest(request, RestMethod.HEAD.toString(), NETTY_SERVER_PORT);
  }

  /**
   * Test the error handling of {@link NettyMessageProcessor} given bad input.
   * @throws Exception
   */
  @Test
  public void badInputStreamTest()
      throws Exception {
    // TODO: the duplicate request handling needs rethinking.
    // duplicate request test
    // doDuplicateRequestTest(NETTY_SERVER_PORT);
  }

  /**
   * Exercises some internals (mostly error handling) of {@link NettyMessageProcessor} and {@link NettyResponseHandler}
   * by wilfully introducing exceptions through {@link MockBlobStorageService}.
   * @throws Exception
   */
  @Test
  public void handleRequestFailureTest()
      throws Exception {
    FullHttpRequest request;

    request = createRequest(HttpMethod.GET, MockBlobStorageService.OPERATION_THROW_HANDLING_REST_EXCEPTION);
    doHandleRequestFailureTest(request, HttpResponseStatus.INTERNAL_SERVER_ERROR, NETTY_SERVER_PORT);

    request = createRequest(HttpMethod.GET, MockBlobStorageService.OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION);
    doHandleRequestFailureTest(request, HttpResponseStatus.INTERNAL_SERVER_ERROR, NETTY_SERVER_PORT);

    // unknown http method
    request = createRequest(HttpMethod.TRACE, "/");
    doHandleRequestFailureTest(request, HttpResponseStatus.BAD_REQUEST, NETTY_SERVER_PORT);

    // do success test at the end to make sure that server is alive
    request = createRequest(HttpMethod.GET, "/");
    doHandleRequestSuccessTest(request, RestMethod.GET.toString(), NETTY_SERVER_PORT);
  }

  /**
   * Tests the scenario where the {@link AsyncRequestHandler} fails because it has not been started (because the
   * {@link RequestHandlerController} has not been started).
   * @throws Exception
   */
  @Test
  public void requestHandlerControllerFailureTest()
      throws Exception {
    // Start a new NettyServer (on a different port) with a new RequestHandlerController.
    Properties properties = new Properties();
    properties.setProperty(NettyConfig.PORT_KEY, NETTY_SERVER_ALTERNATE_PORT);
    RestRequestHandlerController handlerController = createRestRequestHandlerController();
    NioServer server = createNettyServer(handlerController, properties);
    server.start();

    // We haven't started the RequestHandlerController (in turn the AsyncRequestHandler, so any request we send should result
    // in a failure.
    FullHttpRequest request = createRequest(HttpMethod.GET, "/");
    doHandleRequestFailureTest(request, HttpResponseStatus.INTERNAL_SERVER_ERROR, NETTY_SERVER_ALTERNATE_PORT);

    // Now start the handlerController and make sure everything is ok.
    handlerController.start();
    doHandleRequestSuccessTest(request, RestMethod.GET.toString(), NETTY_SERVER_ALTERNATE_PORT);
    server.shutdown();
    handlerController.shutdown();
  }

  // helpers
  // general
  private FullHttpRequest createRequest(HttpMethod httpMethod, String uri)
      throws JSONException {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri);
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

  // Before/After class helpers
  private static NioServer createNettyServer(RestRequestHandlerController requestHandlerController,
      Properties properties)
      throws InstantiationException {
    if (properties == null) {
      // dud properties. should pick up defaults
      properties = new Properties();
    }
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    NettyConfig nettyConfig = new NettyConfig(verifiableProperties);
    NettyMetrics nettyMetrics = new NettyMetrics(new MetricRegistry());
    return new NettyServer(nettyConfig, nettyMetrics, requestHandlerController);
  }

  private static RestRequestHandlerController createRestRequestHandlerController()
      throws InstantiationException, IOException {
    RestServerMetrics restServerMetrics = new RestServerMetrics(new MetricRegistry());
    BlobStorageService blobStorageService = new MockBlobStorageService(new MockClusterMap());
    return new RequestHandlerController(1, restServerMetrics, blobStorageService);
  }

  // handleRequestSuccessTest() helpers

  /**
   * Sends a request and expects a certain response. If response doesn't match, declares test failure.
   * @param httpRequest
   * @param expectedResponse
   * @param serverPort
   * @throws Exception
   */
  public void doHandleRequestSuccessTest(FullHttpRequest httpRequest, String expectedResponse, String serverPort)
      throws Exception {
    LinkedBlockingQueue<HttpObject> contentQueue = new LinkedBlockingQueue<HttpObject>();
    LinkedBlockingQueue<HttpObject> responseQueue = new LinkedBlockingQueue<HttpObject>();
    NettyClient nettyClient = new NettyClient(Integer.parseInt(serverPort), contentQueue, responseQueue);
    contentQueue.offer(httpRequest);

    nettyClient.start();
    // request is being sent.
    try {
      boolean responseReceived = false;
      while (true) {
        HttpObject httpObject = responseQueue.poll(RESPONSE_QUEUE_POLL_TIMEOUT_SECS, TimeUnit.SECONDS);
        if (httpObject != null) {
          if (httpObject instanceof HttpResponse) {
            responseReceived = true;
            HttpResponse response = (HttpResponse) httpObject;
            assertTrue("Received a bad response", response.getDecoderResult().isSuccess());
            assertEquals("Response status is not OK", HttpResponseStatus.OK, response.getStatus());
            assertEquals("Unexpected content type", "text/plain",
                response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
          } else if (httpObject instanceof HttpContent) {
            if (httpObject instanceof LastHttpContent) {
              ReferenceCountUtil.release(httpObject);
              break;
            }
            try {
              assertTrue("Received HttpContent without receiving a response first", responseReceived);
              String content = getContentString((HttpContent) httpObject);
              assertEquals("Did not get expected reply from server", expectedResponse, content);
            } finally {
              ReferenceCountUtil.release(httpObject);
            }
          } else {
            fail("Unknown HttpObject - " + httpObject.getClass());
          }
        } else {
          fail("Did not receive any content in 30 seconds. There is an error or the timeout needs to increase");
        }
      }
    } finally {
      nettyClient.shutdown();
    }
  }

  // badInputStreamTest() helpers

  /**
   * Sends two requests in one stream and expects an error to be returned.
   * @param serverPort
   * @throws Exception
   */
  private void doDuplicateRequestTest(String serverPort)
      throws Exception {
    LinkedBlockingQueue<HttpObject> contentQueue = new LinkedBlockingQueue<HttpObject>();
    LinkedBlockingQueue<HttpObject> responseQueue = new LinkedBlockingQueue<HttpObject>();
    NettyClient nettyClient = new NettyClient(Integer.parseInt(serverPort), contentQueue, responseQueue);
    contentQueue.offer(createRequest(HttpMethod.GET, "/"));
    contentQueue.offer(createRequest(HttpMethod.GET, "/"));

    nettyClient.start();
    // request is being sent.
    try {
      HttpObject httpObject = responseQueue.poll(RESPONSE_QUEUE_POLL_TIMEOUT_SECS, TimeUnit.SECONDS);
      if (httpObject != null && httpObject instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) httpObject;
        assertTrue("Received a bad response", response.getDecoderResult().isSuccess());
        assertEquals("Response status is not Bad Request", HttpResponseStatus.BAD_REQUEST, response.getStatus());
      } else {
        fail("Did not receive a response");
      }
    } finally {
      nettyClient.shutdown();
    }
  }

  // handleRequestFailureTest() helpers

  /**
   * Sends a request that is expected to trigger an error and expects the error in the response. If no error or
   * different from the one being expected, it fails the test.
   * @param httpRequest
   * @param expectedStatus
   * @param serverPort
   * @throws Exception
   */
  private void doHandleRequestFailureTest(FullHttpRequest httpRequest, HttpResponseStatus expectedStatus,
      String serverPort)
      throws Exception {
    LinkedBlockingQueue<HttpObject> contentQueue = new LinkedBlockingQueue<HttpObject>();
    LinkedBlockingQueue<HttpObject> responseQueue = new LinkedBlockingQueue<HttpObject>();
    NettyClient nettyClient = new NettyClient(Integer.parseInt(serverPort), contentQueue, responseQueue);
    contentQueue.offer(httpRequest);

    nettyClient.start();
    // request is being sent.
    try {
      HttpObject httpObject = responseQueue.poll(RESPONSE_QUEUE_POLL_TIMEOUT_SECS, TimeUnit.SECONDS);
      if (httpObject != null && httpObject instanceof HttpResponse) {
        HttpResponse response = (HttpResponse) httpObject;
        assertTrue("Received a bad response", response.getDecoderResult().isSuccess());
        assertEquals("Response status differs from expectation", expectedStatus, response.getStatus());
      } else {
        fail("Did not receive a response");
      }
    } finally {
      nettyClient.shutdown();
    }
  }
}
