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
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link NettyServerRequestResponseChannel}.
 */
public class NettyServerRequestResponseChannelTest {
  private final static int QUEUE_SIZE = 10;

  /**
   * Test send and receive requests
   * @throws Exception
   */
  @Test
  public void testSendAndReceiveRequest() throws Exception {

    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(QUEUE_SIZE, new Http2ServerMetrics(new MetricRegistry()));

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
