package com.github.ambry.rest;

import com.github.ambry.utils.UtilsTest;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class HealthCheckHandlerTest {
  private RestServerState restServerState;
  private final String healthCheckUri = "/healthCheckUri";
  private final byte[] goodBytes = "GOOD".getBytes();
  private final byte[] badBytes = "BAD".getBytes();

  public HealthCheckHandlerTest() {
    this.restServerState = new RestServerState(healthCheckUri);
  }

  /**
   * Tests for the common case request handling flow for VIP requests.
   * @throws java.io.IOException
   */
  @Test
  public void requestHandleWithVIPRequestTest()
      throws IOException {
    testVIPRequest(HttpMethod.POST, healthCheckUri, true);
    testVIPRequest(HttpMethod.POST, healthCheckUri, false);
  }

  /**
   * Tests non VIP requests handling
   * @throws java.io.IOException
   */
  @Test
  public void requestHandleWithNonVIPRequestTest()
      throws IOException {
    testNonVIPRequest(HttpMethod.POST, "POST");
    testNonVIPRequest(HttpMethod.GET, "GET");
    testNonVIPRequest(HttpMethod.GET, MockBlobStorageService.ECHO_REST_METHOD);
    testNonVIPRequest(HttpMethod.DELETE, "DELETE");
  }

  /**
   * Does a test to see that request handling with good input succeeds.
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @throws IOException
   */
  private void testVIPRequest(HttpMethod httpMethod, String uri, boolean isServiceUp)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    Random random = new Random();
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    restServerState.markServiceUp();
    String location = UtilsTest.getRandomString(60);
    int size = random.nextInt(10000);
    // for POST, the EchoMethodHandler sets location and blob size(in response) based on the values from the request headers
    if (httpMethod == HttpMethod.POST) {
      headers.add(RestUtils.Headers.LOCATION, location);
      headers.add(RestUtils.Headers.BLOB_SIZE, size);
    }
    if (!isServiceUp) {
      restServerState.markServiceDown();
    }
    channel.writeInbound(RestTestUtils.createRequest(httpMethod, uri, headers));
    FullHttpResponse response = (FullHttpResponse) channel.readOutbound();
    HttpResponseStatus httpResponseStatus =
        (isServiceUp) ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE;
    assertEquals("Unexpected response status", httpResponseStatus, response.getStatus());
    byte[] expectedBytes = (isServiceUp) ? goodBytes : badBytes;
    assertEquals("Unexpected content", RestTestUtils.getContentString(expectedBytes),
        RestTestUtils.getContentString(response.content().array()));
    assertTrue("Channel is closed", channel.isOpen());
    restServerState.markServiceDown();
    channel.close();
  }

  /**
   * Does a test to see that request handling with good input succeeds.
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @throws IOException
   */
  private void testNonVIPRequest(HttpMethod httpMethod, String uri)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    Random random = new Random();
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaders.Names.CONTENT_LENGTH, new Random().nextLong());
    String location = UtilsTest.getRandomString(60);
    int size = random.nextInt(10000);
    // for POST, the EchoMethodHandler sets location and blob size(in response) based on the values from the request headers
    if (httpMethod == HttpMethod.POST) {
      headers.add(RestUtils.Headers.LOCATION, location);
      headers.add(RestUtils.Headers.BLOB_SIZE, size);
    }
    channel.writeInbound(RestTestUtils.createRequest(httpMethod, uri, headers));
    channel.writeInbound(new DefaultLastHttpContent());

    FullHttpResponse response = (FullHttpResponse) channel.readOutbound();
    HttpResponseStatus responseStatus = null;
    if (httpMethod == HttpMethod.POST) {
      responseStatus = HttpResponseStatus.CREATED;
    } else if (httpMethod == HttpMethod.GET) {
      responseStatus = HttpResponseStatus.OK;
    } else if (httpMethod == HttpMethod.DELETE) {
      responseStatus = HttpResponseStatus.ACCEPTED;
    }
    assertEquals("Unexpected response status", responseStatus, response.getStatus());
    assertEquals("Unexpected content", httpMethod.toString(),
        RestTestUtils.getContentString(response.content().array()));
    assertFalse("Channel not closed", channel.isOpen());
    channel.close();
  }

  // helpers
  // general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link HealthCheckHandler}
   * and {@link EchoMethodHandler}.
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link HealthCheckHandler}
   * and {@link EchoMethodHandler}.
   */
  private EmbeddedChannel createChannel() {
    return new EmbeddedChannel(new HealthCheckHandler(restServerState), new EchoMethodHandler());
  }
}
