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
package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * Unit test for {@link NettyServerRequestResponseChannel}.
 */
public class NettyServerRequestResponseChannelTest {
  private final static int TIMEOUT = 500;
  private final static int CAPACITY = 2;

  @Test
  public void testNoRejectRequests() throws InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(NetworkConfig.REQUEST_QUEUE_CAPACITY, "2");
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            null));
    // Fill up channel
    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(createNettyServerRequest(1));
    // Verify rejectRequest() is not invoked
    verify(channel, never()).rejectRequest(any(), anyBoolean());
  }

  @Test
  public void testRejectRequests() throws InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(NetworkConfig.REQUEST_QUEUE_CAPACITY, "2");
    ServerRequestResponseHelper requestResponseHelper = mock(ServerRequestResponseHelper.class);
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            requestResponseHelper));
    doNothing().when(channel).rejectRequest(any(), anyBoolean());
    // Fill up channel
    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(createNettyServerRequest(1));
    // Add overflow
    channel.sendRequest(createNettyServerRequest(1));
    // Verify rejectRequest() is invoked
    verify(channel, atLeastOnce()).rejectRequest(any(), anyBoolean());
  }

  @Test
  public void testRejectRequestsWithException() throws IOException, InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(NetworkConfig.REQUEST_QUEUE_CAPACITY, "2");
    ServerRequestResponseHelper requestResponseHelper = mock(ServerRequestResponseHelper.class);
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            requestResponseHelper));
    when(requestResponseHelper.getDecodedRequest(any())).thenThrow(
        new UnsupportedOperationException("Request type not supported"));
    // Fill up channel
    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(createNettyServerRequest(1));
    // Verify rejectRequest() is invoked for last request
    verify(channel, times(1)).rejectRequest(any(), anyBoolean());
    // Verify close connection is invoked due to exception
    verify(channel, times(1)).closeConnection(any());
  }

  @Test
  public void testRejectRequestsOnExpiry() throws InterruptedException {
    ServerRequestResponseHelper requestResponseHelper = mock(ServerRequestResponseHelper.class);
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(TIMEOUT, mockTime, CAPACITY);
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new Http2ServerMetrics(new MetricRegistry()),
            new ServerMetrics(new MetricRegistry(), this.getClass()), requestResponseHelper, requestQueue));
    doNothing().when(channel).rejectRequest(any(), anyBoolean());
    // 1. Queue 2 requests with a sleep between them so that 1st request expires
    NettyServerRequest expiredRequest = createNettyServerRequest(1, mockTime.milliseconds());
    channel.sendRequest(expiredRequest);
    mockTime.sleep(TIMEOUT + 1);
    channel.sendRequest(createNettyServerRequest(1, mockTime.milliseconds()));
    channel.receiveRequest();
    // 2. Verify rejectRequest() is invoked inside receiveRequest() since request is expired
    verify(channel, times(1)).rejectRequest(eq(expiredRequest), eq(true));
  }

  /**
   * Test send and receive requests
   * @throws Exception
   */
  @Test
  public void testSendAndReceiveRequest() throws Exception {

    Properties properties = new Properties();
    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            null);

    channel.sendRequest(createNettyServerRequest(13));
    NetworkRequest request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(13, ((NettyServerRequest) request).content().readableBytes());

    channel.sendRequest(createEmptyNettyServerRequest());
    channel.sendRequest(createNettyServerRequest(19));
    channel.sendRequest(createNettyServerRequest(23));
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(0, ((NettyServerRequest) request).content().readableBytes());
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(19, ((NettyServerRequest) request).content().readableBytes());
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(23, ((NettyServerRequest) request).content().readableBytes());
  }

  private NettyServerRequest createNettyServerRequest(int len) {
    byte[] array = new byte[len];
    TestUtils.RANDOM.nextBytes(array);
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content);
  }

  private NettyServerRequest createNettyServerRequest(int len, long creationTime) {
    byte[] array = new byte[len];
    TestUtils.RANDOM.nextBytes(array);
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content, creationTime);
  }

  private NettyServerRequest createEmptyNettyServerRequest() {
    byte[] array = new byte[0];
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content);
  }
}
