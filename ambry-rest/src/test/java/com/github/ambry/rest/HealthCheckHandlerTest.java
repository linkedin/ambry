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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import java.io.IOException;
import junit.framework.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


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
  public void requestHandleWithHealthCheckRequestTest() throws IOException {
    // test with keep alive
    testHealthCheckRequest(HttpMethod.GET, true, true);
    testHealthCheckRequest(HttpMethod.GET, false, true);

    // test without keep alive
    testHealthCheckRequest(HttpMethod.GET, true, false);
    testHealthCheckRequest(HttpMethod.GET, false, false);
  }

  /**
   * Tests non health check requests handling
   * @throws IOException
   */
  @Test
  public void requestHandleWithNonHealthCheckRequestTest() throws IOException {
    testNonHealthCheckRequest(HttpMethod.POST, "POST");
    testNonHealthCheckRequest(HttpMethod.GET, "GET");
    testNonHealthCheckRequest(HttpMethod.DELETE, "DELETE");
  }

  /**
   * Does a test to see that a health check request results in expected response from the health check handler
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param keepAlive true if keep alive has to be set in the request, false otherwise
   * @throws IOException
   */
  private void testHealthCheckRequest(HttpMethod httpMethod, boolean isServiceUp, boolean keepAlive)
      throws IOException {
    EmbeddedChannel channel = createChannel();
    for (int i = 0; i < 2; i++) {
      if (isServiceUp) {
        restServerState.markServiceUp();
      }
      HttpRequest request = RestTestUtils.createRequest(httpMethod, healthCheckUri, null);
      HttpUtil.setKeepAlive(request, keepAlive);
      FullHttpResponse response = sendRequestAndGetResponse(channel, request);
      HttpResponseStatus httpResponseStatus =
          (isServiceUp) ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE;
      assertEquals("Unexpected response status", httpResponseStatus, response.status());
      String expectedStr = (isServiceUp) ? goodStr : badStr;
      assertEquals("Unexpected content", expectedStr, RestTestUtils.getContentString(response));
      restServerState.markServiceDown();
      if (keepAlive && isServiceUp) {
        Assert.assertTrue("Channel should not be closed ", channel.isOpen());
      } else {
        Assert.assertFalse("Channel should have been closed by now ", channel.isOpen());
        channel = createChannel();
      }
    }
    channel.close();
  }

  /**
   * Does a test to see that a non health check request results in expected responses
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param uri Uri to be used during the request
   * @throws IOException
   */
  private void testNonHealthCheckRequest(HttpMethod httpMethod, String uri) throws IOException {
    EmbeddedChannel channel = createChannel();
    HttpRequest request = RestTestUtils.createRequest(httpMethod, uri, null);
    FullHttpResponse response = sendRequestAndGetResponse(channel, request);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    assertEquals("Unexpected content", httpMethod.toString(), RestTestUtils.getContentString(response));
    channel.close();
  }

  /**
   * Sends request to the passed in channel and returns the response
   * @param channel to which the request has to be sent
   * @param request {@link HttpRequest} which has to be sent to the passed in channel
   * @return the HttpResponse in response to the request sent to the channel
   */
  private FullHttpResponse sendRequestAndGetResponse(EmbeddedChannel channel, HttpRequest request) {
    channel.writeInbound(request);
    channel.writeInbound(new DefaultLastHttpContent());
    return channel.readOutbound();
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
    return new EmbeddedChannel(new HealthCheckHandler(restServerState, new NettyMetrics(new MetricRegistry())),
        new EchoMethodHandler());
  }
}
