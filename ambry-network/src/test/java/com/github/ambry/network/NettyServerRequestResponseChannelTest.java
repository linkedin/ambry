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
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link NettyServerRequestResponseChannel}.
 */
public class NettyServerRequestResponseChannelTest {
  private final static int QUEUE_SIZE = 10;
  private final static int REQUEST_SIZE = 10;

  /**
   * Test send and receive requests
   * @throws Exception
   */
  @Test
  public void testSendAndReceiveRequest() throws Exception {

    Properties properties = new Properties();
    properties.put("queued.max.requests", String.valueOf(QUEUE_SIZE));
    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()));

    channel.sendRequest(createNettyServerRequest(13));
    NetworkRequest request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(13, ((NettyServerRequest) request).content().readableBytes());

    channel.sendRequest(createEmptyNettyServerRequest());
    channel.sendRequest(createEmptyNettyServerRequest());
    channel.sendRequest(createNettyServerRequest(17));

    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(17, ((NettyServerRequest) request).content().readableBytes());

    channel.sendRequest(createNettyServerRequest(19));
    channel.sendRequest(createNettyServerRequest(23));
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(19, ((NettyServerRequest) request).content().readableBytes());
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    Assert.assertEquals(23, ((NettyServerRequest) request).content().readableBytes());
  }

  @Test
  public void testGetDroppedRequests() throws InterruptedException {
    Properties properties = new Properties();
    properties.put("queued.max.requests", String.valueOf(QUEUE_SIZE));
    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()));

    int overflowSize = 10;
    List<NetworkRequest> validRequests = new ArrayList<>();
    List<NetworkRequest> droppedRequests = new ArrayList<>();
    // Add requests within queue capacity
    for (int i = 0; i < QUEUE_SIZE; i++) {
      NetworkRequest request = createNettyServerRequest(REQUEST_SIZE);
      validRequests.add(request);
      channel.sendRequest(request);
    }
    // Add requests overflowing queue capacity
    for (int i = 0; i < overflowSize; i++) {
      NetworkRequest request = createNettyServerRequest(REQUEST_SIZE);
      droppedRequests.add(request);
      channel.sendRequest(request);
    }
    // Add an additional invalid request which would be dropped since its stream cannot be consumed
    channel.sendRequest(createEmptyNettyServerRequest());

    // Receive the requests
    for (int i = 0; i < QUEUE_SIZE; i++) {
      NetworkRequest request = channel.receiveRequest();
      Assert.assertTrue(request instanceof NettyServerRequest);
      Assert.assertEquals(((NettyServerRequest) validRequests.get(i)).content(),
          ((NettyServerRequest) request).content());
    }

    List<NetworkRequest> receivedDroppedRequests = channel.getDroppedRequests();
    Assert.assertEquals(overflowSize, receivedDroppedRequests.size());
    for (int i = 0; i < overflowSize; i++) {
      NetworkRequest droppedRequest = droppedRequests.get(i);
      Assert.assertTrue(droppedRequest instanceof NettyServerRequest);
      Assert.assertEquals(((NettyServerRequest) droppedRequests.get(i)).content(),
          ((NettyServerRequest) droppedRequest).content());
    }
  }

  private NettyServerRequest createNettyServerRequest(int len) {
    byte[] array = new byte[len + 8];
    TestUtils.RANDOM.nextBytes(array);
    ByteBuffer byteBuffer = ByteBuffer.wrap(array);
    byteBuffer.putLong(len);
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content);
  }

  private NettyServerRequest createEmptyNettyServerRequest() {
    byte[] array = new byte[0];
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content);
  }
}
