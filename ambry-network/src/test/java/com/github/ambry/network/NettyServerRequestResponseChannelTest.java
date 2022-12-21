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
  private final static int QUEUE_TIMEOUT_MS = 500;
  private final static int REQUEST_SIZE = 10;

  /**
   * Test send and receive requests
   * @throws Exception
   */
  @Test
  public void testSendAndReceiveRequest() throws Exception {

    Properties properties = new Properties();
    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()));

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
    properties.put(NetworkConfig.REQUEST_QUEUE_TIMEOUT_MS, String.valueOf(QUEUE_TIMEOUT_MS));
    ServerMetrics serverMetrics = new ServerMetrics(new MetricRegistry(), this.getClass());
    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), serverMetrics);

    List<NetworkRequest> validRequests = new ArrayList<>();
    List<NetworkRequest> droppedRequests = new ArrayList<>();
    int numActiveRequests = 5;
    int numDroppedRequests = 5;

    // Add 5 requests to channel that would eventually timeout
    for (int i = 0; i < numDroppedRequests; i++) {
      NetworkRequest request = createNettyServerRequest(REQUEST_SIZE);
      channel.sendRequest(request);
      droppedRequests.add(request);
    }
    // Sleep so that requests timeout
    Thread.sleep(QUEUE_TIMEOUT_MS + 10);
    // Add 5 more requests to the channel which remain active
    for (int i = 0; i < numActiveRequests; i++) {
      NetworkRequest request = createNettyServerRequest(REQUEST_SIZE);
      channel.sendRequest(request);
      validRequests.add(request);
    }

    // Verify metrics to track queue sizes are updated
    // Since we didn't dequeue any request, the timed out requests are not moved to dropped request queue.
    Assert.assertEquals("Mismatch in number of active requests", numActiveRequests + numDroppedRequests,
        serverMetrics.activeRequestsQueueSize.getValue().intValue());
    Assert.assertEquals("Mismatch in number of dropped requests", 0,
        serverMetrics.droppedRequestsQueueSize.getValue().intValue());

    // Verify that when we receive requests, we only receive active requests.
    for (int i = 0; i < 5; i++) {
      NetworkRequest request = channel.receiveRequest();
      Assert.assertTrue(request instanceof NettyServerRequest);
      Assert.assertEquals(((NettyServerRequest) validRequests.get(i)).content(),
          ((NettyServerRequest) request).content());
    }

    // Since we dequeued active requests, count should be 0. Also, the timed out requests would have been moved to
    // dropped request queue.
    Assert.assertEquals("Mismatch in number of active requests", 0,
        serverMetrics.activeRequestsQueueSize.getValue().intValue());
    Assert.assertEquals("Mismatch in number of dropped requests", numDroppedRequests,
        serverMetrics.droppedRequestsQueueSize.getValue().intValue());

    List<NetworkRequest> droppedRequestsInChannel = channel.getDroppedRequests();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(droppedRequestsInChannel.get(i) instanceof NettyServerRequest);
      Assert.assertEquals(((NettyServerRequest) droppedRequests.get(i)).content(),
          ((NettyServerRequest) droppedRequestsInChannel.get(i)).content());
    }

    Assert.assertEquals("Mismatch in number of active requests", 0,
        serverMetrics.activeRequestsQueueSize.getValue().intValue());
    Assert.assertEquals("Mismatch in number of dropped requests", 0,
        serverMetrics.droppedRequestsQueueSize.getValue().intValue());
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
