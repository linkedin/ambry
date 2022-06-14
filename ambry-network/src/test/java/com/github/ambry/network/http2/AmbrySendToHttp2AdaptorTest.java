/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.network.Send;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Test suite for {@link AmbrySendToHttp2Adaptor}.
 */
public class AmbrySendToHttp2AdaptorTest {
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Test when write {@link Send} from server to client. There are two cases tested in this method
   * 1. {@link Send} returns a non-null value for {@link Send#content()} method.
   * 2. {@link Send} returns a null value for {@link Send#content()} method.
   * Make sure both cases don't leak memory.
   * @throws Exception
   */
  @Test
  public void testServerWrite() {
    int maxFrameSize = 2001;
    EmbeddedChannel channel = new EmbeddedChannel(new AmbrySendToHttp2Adaptor(true, maxFrameSize));
    int contentSize = 7000;
    byte[] byteArray = new byte[contentSize];
    new Random().nextBytes(byteArray);
    ByteBuf content = PooledByteBufAllocator.DEFAULT.heapBuffer(contentSize).writeBytes(byteArray);
    // Retain the ByteBuf for data comparison
    content.retain();
    Send send = new MockSend(content);
    channel.writeOutbound(send);

    DefaultHttp2HeadersFrame header = channel.readOutbound();
    Assert.assertNotNull(header.headers());
    Assert.assertEquals(header.headers().status().toString(), "200");

    byte[] resultArray = new byte[contentSize];
    int i;
    for (i = 0; i < contentSize / maxFrameSize; i++) {
      DefaultHttp2DataFrame data = channel.readOutbound();
      data.content().readBytes(resultArray, i * maxFrameSize, maxFrameSize);
      data.content().release();
    }
    // The last frame
    DefaultHttp2DataFrame data = channel.readOutbound();
    data.content().readBytes(resultArray, i * maxFrameSize, data.content().readableBytes());
    data.content().release();
    Assert.assertArrayEquals(byteArray, resultArray);
    content.release();
  }

  /**
   * Test writing when the channel is closed.
   */
  @Test
  public void testChannelCloseBeforeWriting() {
    AmbrySendToHttp2Adaptor adaptor = new AmbrySendToHttp2Adaptor(true, 100);
    EmbeddedChannel channel = new EmbeddedChannel(adaptor);
    byte[] byteArray = new byte[100];
    new Random().nextBytes(byteArray);
    ByteBuf content = PooledByteBufAllocator.DEFAULT.heapBuffer(100).writeBytes(byteArray);

    // Get the context before closing the channel, since closing channel would remove all the context.
    ChannelHandlerContext ctx = channel.pipeline().firstContext();
    channel.close().getNow();
    adaptor.write(ctx, content, channel.newPromise());
  }
}
