package com.github.ambry.rest;

import com.github.ambry.utils.UtilsTest;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
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
  private final RestServerState restServerState;
  private final String healthCheckUri = "/healthCheck";
  private final String goodStr = "GOOD";
  private final String badStr = "BAD";

  public HealthCheckHandlerTest() {
    this.restServerState = new RestServerState(healthCheckUri);
  }

  /**
   * Tests for the common case request handling flow for health check requests.
   * @throws java.io.IOException
   */
  @Test
  public void requestHandleWithHealthCheckRequestTest()
      throws IOException {
    testHealthCheckRequest(HttpMethod.GET, true);
    testHealthCheckRequest(HttpMethod.GET, false);
  }

  /**
   * Tests non health check requests handling
   * @throws java.io.IOException
   */
  @Test
  public void requestHandleWithNonHealthCheckRequestTest()
      throws IOException {
    testNonHealthCheckRequest(HttpMethod.POST, "POST");
    testNonHealthCheckRequest(HttpMethod.GET, "GET");
    testNonHealthCheckRequest(HttpMethod.DELETE, "DELETE");
  }

  /**
   * Does a test to see that a health check request results in expected response from the health check handler
   * @param httpMethod the {@link HttpMethod} for the request.
   * @throws IOException
   */
  private void testHealthCheckRequest(HttpMethod httpMethod, boolean isServiceUp)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    if (isServiceUp) {
      restServerState.markServiceUp();
    }
    channel.writeInbound(RestTestUtils.createRequest(httpMethod, healthCheckUri, null));
    channel.writeInbound(new DefaultLastHttpContent());
    FullHttpResponse response = (FullHttpResponse) channel.readOutbound();
    HttpResponseStatus httpResponseStatus =
        (isServiceUp) ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE;
    assertEquals("Unexpected response status", httpResponseStatus, response.getStatus());
    String expectedStr = (isServiceUp) ? goodStr : badStr;
    assertEquals("Unexpected content", expectedStr, RestTestUtils.getContentString(response));
    assertTrue("Channel is closed", channel.isOpen());
    restServerState.markServiceDown();
    channel.close();
  }

  /**
   * Does a test to see that a non health check request results in expected responses
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @throws IOException
   */
  private void testNonHealthCheckRequest(HttpMethod httpMethod, String uri)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    channel.writeInbound(RestTestUtils.createRequest(httpMethod, uri, null));
    channel.writeInbound(new DefaultLastHttpContent());
    FullHttpResponse response = (FullHttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    assertEquals("Unexpected content", httpMethod.toString(), RestTestUtils.getContentString(response));
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
