/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network.http2;

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class RequestsStatsHandlerTest {

  RequestsStatsHandler requestsStatsHandler;

  public RequestsStatsHandlerTest() {
    setup();
  }

  @Before
  public void setup() {
    requestsStatsHandler = new RequestsStatsHandler(new Http2ClientMetrics(new MetricRegistry()));
  }

  @Test
  public void inFlightRequestCountTest() {

    EmbeddedChannel channel = new EmbeddedChannel(requestsStatsHandler);
    int inFlightRequestCount = 0;

    channel.writeOutbound(new Object());
    Assert.assertEquals("Mismatch in in flight request count", ++inFlightRequestCount,
        requestsStatsHandler.getInFlightRequestCount());

    channel.writeInbound(new Object());
    Assert.assertEquals("Mismatch in in flight request count", --inFlightRequestCount,
        requestsStatsHandler.getInFlightRequestCount());
  }

  @Test
  public void inFlightRequestCountTestDuringException() {
    EmbeddedChannel channel = new EmbeddedChannel(new TestHandler(), requestsStatsHandler);
    int inFlightRequestCount = 0;

    channel.writeOutbound(new Object());
    Assert.assertEquals("Mismatch in in flight request count", ++inFlightRequestCount,
        requestsStatsHandler.getInFlightRequestCount());

    try {
      channel.writeInbound(new Object());
      Assert.assertEquals("Mismatch in in flight request count", --inFlightRequestCount,
          requestsStatsHandler.getInFlightRequestCount());
      verify(requestsStatsHandler, times(1)).exceptionCaught(any(), any());
    } catch (Exception ignored) {

    }
  }

  static class TestHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      throw new RuntimeException("Network error");
    }
  }
}
