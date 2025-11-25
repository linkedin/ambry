/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.commons.Callback;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.VerifiableProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests bytebuf memory management in NettyResponseChannel.
 * These tests verify that NettyResponseChannel correctly manages bytebuf reference counts
 * according to Netty's reference counting contract: whoever creates or retains a bytebuf
 * is responsible for releasing it.
 * 1. write(ByteBuffer): NettyResponseChannel wraps the bytebuffer in a bytebuf internally.
 *    Since NettyResponseChannel creates this wrapper, it must release it.
 * 2. write(ByteBuf): The caller provides their own bytebuf and retains ownership.
 *    NettyResponseChannel must not release it.
 */
public class NettyResponseChannelMemoryManagementTest {
  private EmbeddedChannel channel;
  private NettyMetrics nettyMetrics;
  private NettyConfig nettyConfig;
  private PerformanceConfig performanceConfig;
  private TestContextCapture contextCapture;

  @Before
  public void setUp() {
    nettyMetrics = new NettyMetrics(new MetricRegistry());

    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    nettyConfig = new NettyConfig(verifiableProperties);
    performanceConfig = new PerformanceConfig(verifiableProperties);

    // Create channel with handler that activates the channel.
    contextCapture = new TestContextCapture();
    ChunkedWriteHandler chunkedWriteHandler = new ChunkedWriteHandler();
    channel = new EmbeddedChannel(chunkedWriteHandler, contextCapture);
  }

  @After
  public void tearDown() {
    if (channel != null && channel.isOpen()) {
      channel.close();
    }
  }

  /**
   * Verifies that write(ByteBuffer) releases the bytebuf wrapper it creates internally.
   * When write(ByteBuffer) is called, NettyResponseChannel creates a bytebuf from the bytebuffer.
   * Since NettyResponseChannel creates this wrapper and the caller has no reference to it, it owns it
   * and must release it.
   */
  @Test
  public void testWriteByteBufferReleasesWrapper() throws Exception {
    runTestWithinChannel((responseChannel) -> {
      // Call write(ByteBuffer) creating an internal bytebuf wrapper that NettyResponseChannel owns
      String testData = "test data";
      ByteBuffer byteBuffer = ByteBuffer.wrap(testData.getBytes(StandardCharsets.UTF_8));
      responseChannel.write(byteBuffer, new ReleasingCallback());
    });
  }

  /**
   * Verifies that write(ByteBuf) does not release the caller's bytebuf.
   * When write(ByteBuf) is called, the caller passes their own bytebuf and retains ownership.
   * NettyResponseChannel must not call release() on it. The caller is responsible for releasing
   * their own bytebuf in their callback.
   */
  @Test
  public void testWriteByteBufDoesNotReleaseCaller() throws Exception {
    runTestWithinChannel((responseChannel) -> {
      // Caller creates their own bytebuf. The caller owns this and is responsible for releasing it
      String testData = "test data";
      ByteBuf callerByteBuf = Unpooled.wrappedBuffer(testData.getBytes(StandardCharsets.UTF_8));
      assertEquals("Caller's bytebuf should start with refCnt=1", 1, callerByteBuf.refCnt());
      responseChannel.write(callerByteBuf, new ReleasingCallback(callerByteBuf));
    });
  }

  private void runTestWithinChannel(Consumer<NettyResponseChannel> channelWrite) throws Exception {
    try (NettyResponseChannel responseChannel = new NettyResponseChannel(
        contextCapture.ctx, nettyMetrics, performanceConfig, nettyConfig)) {

      responseChannel.setStatus(ResponseStatus.Ok);
      responseChannel.setHeader(HttpHeaderNames.TRANSFER_ENCODING.toString(), "chunked");

      channelWrite.accept(responseChannel);

      HttpResponse response = channel.readOutbound();
      assertNotNull("HttpResponse should be written to channel", response);
      HttpContent content = channel.readOutbound();
      assertNotNull("HttpContent should be written to channel", content);
      ByteBuf contentByteBuf = content.content();
      assertNotNull("HttpContent should contain bytebuf", contentByteBuf);
      assertEquals("The content bytebuf should have refCnt=1. If this fails with refCnt=2, it proves "
          + "NettyResponseChannel leaked the memory. refCnt=0 and there will be a double-free.",
          1, contentByteBuf.refCnt());
    }
  }

  private static class ReleasingCallback implements Callback<Long> {
    private final ByteBuf _byteBuf;

    public ReleasingCallback() {
      _byteBuf = null;
    }

    public ReleasingCallback(ByteBuf byteBuf) {
      _byteBuf = byteBuf;
    }

    @Override
    public void onCompletion(Long result, Exception exception) {
      if (_byteBuf != null) {
        _byteBuf.release();
      }
    }
  }

  /**
   * Handler that captures the ChannelHandlerContext when channelActive is called.
   * This ensures we have a context from an actually active channel for testing.
   */
  private static class TestContextCapture extends ChannelInboundHandlerAdapter {
    ChannelHandlerContext ctx;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      this.ctx = ctx;
      ctx.fireChannelActive();
    }
  }
}
