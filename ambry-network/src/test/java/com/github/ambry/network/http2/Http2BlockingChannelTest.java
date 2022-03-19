/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Unit tests for {@link Http2BlockingChannel}.
 */
public class Http2BlockingChannelTest {
  private final ScheduledExecutorService service;
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  public Http2BlockingChannelTest() {
    service = Utils.newScheduler(2, "", false);
  }

  @Before
  public void beforeTest() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void afterTest() {
    nettyByteBufLeakHelper.afterTest();
    service.shutdown();
  }

  /**
   * Test for the basic functions for Http2BlockingChannel, sendAndReceive
   * @throws Exception
   */
  @Test
  @Ignore
  public void testSendAndReceive() throws Exception {
    EmbeddedChannelPool channelPool = new EmbeddedChannelPool();
    InetSocketAddress address = new InetSocketAddress("localhost", 2021);
    Http2BlockingChannel blockingChannel = new Http2BlockingChannel(channelPool, address, createHttp2ClientConfig());

    int sendSize = 1234;
    byte[] byteArray = new byte[sendSize];
    new Random().nextBytes(byteArray);
    ByteBuf content = PooledByteBufAllocator.DEFAULT.heapBuffer(sendSize).writeBytes(byteArray);
    MockSend send = new MockSend(content);
    ScheduledFuture<ChannelOutput> future =
        service.schedule(() -> blockingChannel.sendAndReceive(send), 0, TimeUnit.MILLISECONDS);

    EmbeddedChannel channel = channelPool.getCurrentChannel();
    int responseSize = 100;
    byteArray = new byte[responseSize];
    new Random().nextBytes(byteArray);
    ByteBuf response =
        PooledByteBufAllocator.DEFAULT.heapBuffer(responseSize + 8).writeLong(responseSize + 8).writeBytes(byteArray);
    channel.writeInbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, response));

    ChannelOutput output = future.get();
    int streamSize = (int) output.getStreamSize();
    Assert.assertEquals(responseSize, streamSize);
    byte[] obtainedResponse = new byte[streamSize];
    output.getInputStream().read(obtainedResponse);
    Assert.assertArrayEquals(byteArray, obtainedResponse);

    // Make sure the channel is released
    Assert.assertNull(channelPool.getCurrentChannelNow());
    content.release();
    response.release();
  }

  /**
   * Test when acquiring channel times out.
   * @throws Exception
   */
  @Test
  public void testAcquireTimeout() throws Exception {
    EmbeddedChannelPool channelPool = new EmbeddedChannelPool(new TimeoutException("Timeout"));
    InetSocketAddress address = new InetSocketAddress("localhost", 2021);
    Http2BlockingChannel blockingChannel = new Http2BlockingChannel(channelPool, address, createHttp2ClientConfig());

    byte[] byteArray = new byte[1234];
    new Random().nextBytes(byteArray);
    ByteBuf content = PooledByteBufAllocator.DEFAULT.heapBuffer(1234).writeBytes(byteArray);
    MockSend send = new MockSend(content);

    try {
      blockingChannel.sendAndReceive(send);
      Assert.fail("Should fail in acquire");
    } catch (IOException e) {
    }
    content.release();
  }

  /**
   * Test when receiving response times out
   * @throws Exception
   */
  @Test
  public void testReceiveTimeout() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Http2BlockingChannelResponseHandler handler = new Http2BlockingChannelResponseHandler() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        latch.await();
        super.channelRead0(ctx, msg);
      }
    };

    long receiveTimeoutMs = 1000;
    EmbeddedChannelPool channelPool = new EmbeddedChannelPool(handler);
    InetSocketAddress address = new InetSocketAddress("localhost", 2021);
    Http2BlockingChannel blockingChannel =
        new Http2BlockingChannel(channelPool, address, createHttp2ClientConfig(1000, 1000, receiveTimeoutMs));

    int sendSize = 1234;
    byte[] byteArray = new byte[sendSize];
    new Random().nextBytes(byteArray);
    ByteBuf content = PooledByteBufAllocator.DEFAULT.heapBuffer(sendSize).writeBytes(byteArray);
    MockSend send = new MockSend(content);
    ScheduledFuture<ChannelOutput> future =
        service.schedule(() -> blockingChannel.sendAndReceive(send), 0, TimeUnit.MILLISECONDS);

    EmbeddedChannel channel = channelPool.getCurrentChannel();
    int responseSize = 100;
    byteArray = new byte[responseSize];
    new Random().nextBytes(byteArray);
    ByteBuf response =
        PooledByteBufAllocator.DEFAULT.heapBuffer(responseSize + 8).writeLong(responseSize + 8).writeBytes(byteArray);
    service.submit(() -> {
      channel.writeInbound(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, response));
    });

    // Blocking channel is waiting for response. Response is blocked by countdown latch.
    Thread.sleep(2 * receiveTimeoutMs);
    latch.countDown();

    try {
      future.get();
      Assert.fail("Should fail on receiving response");
    } catch (Exception e) {
    }

    content.release();
    // response.release();
  }

  /**
   * Create a {@link Http2ClientConfig}.
   * @return
   */
  private Http2ClientConfig createHttp2ClientConfig() {
    return createHttp2ClientConfig(1000, 5000, 10000);
  }

  /**
   * Create a {@link Http2ClientConfig} with given timeout values.
   * @param acquireTimeoutMs The timeout in ms for acquiring a channel.
   * @param sendTimeoutMs The timeout for sending request out.
   * @param receiveTimeoutMs The timeout for receiving response.
   * @return
   */
  private Http2ClientConfig createHttp2ClientConfig(long acquireTimeoutMs, long sendTimeoutMs, long receiveTimeoutMs) {
    Properties properties = new Properties();
    properties.setProperty(Http2ClientConfig.HTTP2_BLOCKING_CHANNEL_ACQUIRE_TIMEOUT_MS,
        String.valueOf(acquireTimeoutMs));
    properties.setProperty(Http2ClientConfig.HTTP2_BLOCKING_CHANNEL_SEND_TIMEOUT_MS, String.valueOf(sendTimeoutMs));
    properties.setProperty(Http2ClientConfig.HTTP2_BLOCKING_CHANNEL_RECEIVE_TIMEOUT_MS,
        String.valueOf(receiveTimeoutMs));
    return new Http2ClientConfig(new VerifiableProperties(properties));
  }
}

/**
 * A mock implementation of {@link ChannelPool}.
 */
class EmbeddedChannelPool implements ChannelPool {
  private volatile EmbeddedChannel currentChannel;
  private final EventLoopGroup eventLoopGroup = new DefaultEventLoopGroup(2);
  private final Throwable acquireException;
  private final ChannelHandler handler;

  EmbeddedChannelPool() {
    this(null, new Http2BlockingChannelResponseHandler());
  }

  EmbeddedChannelPool(ChannelHandler handler) {
    this(null, handler);
  }

  EmbeddedChannelPool(Throwable acquireException) {
    this(acquireException, new Http2BlockingChannelResponseHandler());
  }

  EmbeddedChannelPool(Throwable acquireException, ChannelHandler handler) {
    this.acquireException = acquireException;
    this.handler = handler;
  }

  synchronized EmbeddedChannel getCurrentChannel() throws Exception {
    while (currentChannel == null) {
      wait();
    }
    return currentChannel;
  }

  synchronized EmbeddedChannel getCurrentChannelNow() {
    return currentChannel;
  }

  @Override
  public synchronized Future<Channel> acquire() {
    return acquire(eventLoopGroup.next().newPromise());
  }

  @Override
  public synchronized Future<Channel> acquire(Promise<Channel> promise) {
    if (acquireException == null) {
      EmbeddedChannel channel = new EmbeddedChannel(this.handler);
      promise.setSuccess(channel);
      currentChannel = channel;
      notifyAll();
      return promise;
    } else {
      promise.setFailure(acquireException);
      return promise;
    }
  }

  @Override
  public synchronized Future<Void> release(Channel channel) {
    currentChannel = null;
    return channel.close();
  }

  @Override
  public synchronized Future<Void> release(Channel channel, Promise<Void> promise) {
    currentChannel = null;
    promise.setSuccess(null);
    return channel.close();
  }

  @Override
  public void close() {

  }
}