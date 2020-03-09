/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Modifications copyright (C) 2020 <Linkedin/zzmao>
 */


package com.github.ambry.network.http2;

import static com.github.ambry.network.http2.MultiplexedChannelRecordTest.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.FailedFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import static org.junit.Assert.*;


/**
 * Tests for {@link Http2MultiplexedChannelPool}.
 */
public class Http2MultiplexedChannelPoolTest {
  private static EventLoopGroup loopGroup;
  private static int maxConcurrentStreamsPerConnection = 256;
  private static int maxContentLength = 1024 * 1024;

  @BeforeClass
  public static void setup() {
    loopGroup = new NioEventLoopGroup(4);
  }

  @AfterClass
  public static void teardown() {
    loopGroup.shutdownGracefully().awaitUninterruptibly();
  }

  /**
   * Channel acquire and release test.
   */
  @Test
  public void streamChannelAcquireReleaseTest() throws Exception {
    Channel channel = newHttp2Channel();

    try {
      ChannelPool connectionPool = Mockito.mock(ChannelPool.class);

      loopGroup.register(channel).awaitUninterruptibly();
      Promise<Channel> channelPromise = new DefaultPromise<>(loopGroup.next());
      channelPromise.setSuccess(channel);

      Mockito.when(connectionPool.acquire()).thenReturn(channelPromise);

      Http2MultiplexedChannelPool h2Pool =
          new Http2MultiplexedChannelPool(connectionPool, loopGroup, new HashSet<>(), null, 1,
              maxConcurrentStreamsPerConnection, maxContentLength, new Http2ClientMetrics(new MetricRegistry()));

      Channel streamChannel1 = h2Pool.acquire().awaitUninterruptibly().getNow();
      assertTrue(streamChannel1 instanceof Http2StreamChannel);
      Mockito.verify(connectionPool, Mockito.times(1)).acquire();

      Channel streamChannel2 = h2Pool.acquire().awaitUninterruptibly().getNow();
      assertTrue(streamChannel2 instanceof Http2StreamChannel);
      Mockito.verify(connectionPool, Mockito.times(1)).acquire();

      // Verify number of numOfAvailableStreams
      MultiplexedChannelRecord multiplexedChannelRecord =
          streamChannel2.parent().attr(Http2MultiplexedChannelPool.MULTIPLEXED_CHANNEL).get();
      assertEquals(maxConcurrentStreamsPerConnection - 2, multiplexedChannelRecord.getNumOfAvailableStreams().get());
      h2Pool.release(streamChannel1).getNow();
      h2Pool.release(streamChannel2).getNow();
      assertEquals(maxConcurrentStreamsPerConnection, multiplexedChannelRecord.getNumOfAvailableStreams().get());
    } finally {
      channel.close();
    }
  }

  /**
   * Minimum Connection is required.
   */
  @Test
  public void minConnectionTest() throws Exception {
    int minConnections = 2;
    int totalStreamToAcquire = 4;
    List<Channel> channels = new ArrayList<>();

    try {

      ChannelPool connectionPool = Mockito.mock(ChannelPool.class);

      OngoingStubbing<Future<Channel>> mockito = Mockito.when(connectionPool.acquire());
      for (int i = 0; i < minConnections; i++) {
        Channel channel = newHttp2Channel();
        channels.add(channel);

        loopGroup.register(channel).awaitUninterruptibly();
        Promise<Channel> channelPromise = new DefaultPromise<>(loopGroup.next());
        channelPromise.setSuccess(channel);
        mockito = mockito.thenReturn(channelPromise);
      }

      Http2MultiplexedChannelPool h2Pool =
          new Http2MultiplexedChannelPool(connectionPool, loopGroup, new HashSet<>(), null, minConnections,
              maxConcurrentStreamsPerConnection, maxContentLength, new Http2ClientMetrics(new MetricRegistry()));

      List<Channel> toRelease = new ArrayList<>();

      for (int i = 0; i < minConnections; i++) {
        Channel streamChannel = h2Pool.acquire().awaitUninterruptibly().getNow();
        toRelease.add(streamChannel);
        assertTrue(streamChannel instanceof Http2StreamChannel);
        Mockito.verify(connectionPool, Mockito.times(i + 1)).acquire();
      }

      for (int i = minConnections; i < totalStreamToAcquire; i++) {
        Channel streamChannel = h2Pool.acquire().awaitUninterruptibly().getNow();
        toRelease.add(streamChannel);
        assertTrue(streamChannel instanceof Http2StreamChannel);
        // No more parent channel acquisition
        Mockito.verify(connectionPool, Mockito.times(minConnections)).acquire();
      }

      for (Channel streamChannel : toRelease) {
        h2Pool.release(streamChannel).getNow();
      }
    } finally {
      for (Channel channel : channels) {
        channel.close();
      }
    }
  }

  /**
   * Channel acquire should fail if parent channel pool acquire fails.
   */
  @Test
  public void failedConnectionAcquireNotifiesPromise() throws InterruptedException {
    IOException exception = new IOException();
    ChannelPool connectionPool = mock(ChannelPool.class);
    when(connectionPool.acquire()).thenReturn(new FailedFuture<>(loopGroup.next(), exception));

    ChannelPool pool = new Http2MultiplexedChannelPool(connectionPool, loopGroup.next(), new HashSet<>(), null, 1,
        maxConcurrentStreamsPerConnection, maxContentLength, new Http2ClientMetrics(new MetricRegistry()));

    Future<Channel> acquirePromise = pool.acquire().await();
    assertFalse(acquirePromise.isSuccess());
    assertEquals(acquirePromise.cause(), exception);
  }

  /**
   * Channel acquire should fail if pool is closed.
   */
  @Test
  public void releaseParentChannelsIfPoolIsClosed() {
    SocketChannel channel = new NioSocketChannel();
    try {
      loopGroup.register(channel).awaitUninterruptibly();

      ChannelPool connectionPool = mock(ChannelPool.class);
      ArgumentCaptor<Promise> releasePromise = ArgumentCaptor.forClass(Promise.class);
      when(connectionPool.release(eq(channel), releasePromise.capture())).thenAnswer(invocation -> {
        Promise<?> promise = releasePromise.getValue();
        promise.setSuccess(null);
        return promise;
      });

      MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 8, null, maxContentLength);
      Http2MultiplexedChannelPool h2Pool =
          new Http2MultiplexedChannelPool(connectionPool, loopGroup, Collections.singleton(record), null, 1,
              maxConcurrentStreamsPerConnection, maxContentLength, new Http2ClientMetrics(new MetricRegistry()));

      h2Pool.close();

      InOrder inOrder = Mockito.inOrder(connectionPool);
      inOrder.verify(connectionPool).release(eq(channel), isA(Promise.class));
      inOrder.verify(connectionPool).close();
    } finally {
      channel.close().awaitUninterruptibly();
    }
  }

  /**
   * Acquire should fail if pool is closed.
   */
  @Test
  public void acquireAfterCloseFails() throws InterruptedException {
    ChannelPool connectionPool = mock(ChannelPool.class);
    Http2MultiplexedChannelPool h2Pool =
        new Http2MultiplexedChannelPool(connectionPool, loopGroup.next(), new HashSet<>(), null, 1,
            maxConcurrentStreamsPerConnection, maxContentLength, new Http2ClientMetrics(new MetricRegistry()));

    h2Pool.close();

    Future<Channel> acquireResult = h2Pool.acquire().await();
    assertFalse(acquireResult.isSuccess());
    assertTrue(acquireResult.cause() instanceof IOException);
  }

  /**
   * Connection pool is released first and then close upon h2 pool close.
   */
  @Test
  public void closeWaitsForConnectionToBeReleasedBeforeClosingConnectionPool() {
    SocketChannel channel = new NioSocketChannel();
    try {
      loopGroup.register(channel).awaitUninterruptibly();

      ChannelPool connectionPool = mock(ChannelPool.class);
      ArgumentCaptor<Promise> releasePromise = ArgumentCaptor.forClass(Promise.class);
      when(connectionPool.release(eq(channel), releasePromise.capture())).thenAnswer(invocation -> {
        Promise<?> promise = releasePromise.getValue();
        promise.setSuccess(null);
        return promise;
      });

      MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 8, null, maxContentLength);
      Http2MultiplexedChannelPool h2Pool =
          new Http2MultiplexedChannelPool(connectionPool, loopGroup, Collections.singleton(record), null, 1,
              maxConcurrentStreamsPerConnection, maxContentLength, new Http2ClientMetrics(new MetricRegistry()));

      h2Pool.close();

      InOrder inOrder = Mockito.inOrder(connectionPool);
      inOrder.verify(connectionPool).release(eq(channel), isA(Promise.class));
      inOrder.verify(connectionPool).close();
    } finally {
      channel.close().awaitUninterruptibly();
    }
  }

  /**
   * Interrupt flag is preserved if pool close is interrupted.
   */
  @Test(timeout = 5_000)
  public void interruptDuringClosePreservesFlag() throws InterruptedException {
    SocketChannel channel = new NioSocketChannel();
    try {
      loopGroup.register(channel).awaitUninterruptibly();
      Promise<Channel> channelPromise = new DefaultPromise<>(loopGroup.next());
      channelPromise.setSuccess(channel);

      ChannelPool connectionPool = mock(ChannelPool.class);
      Promise<Void> releasePromise = Mockito.spy(new DefaultPromise<>(loopGroup.next()));

      when(connectionPool.release(eq(channel))).thenReturn(releasePromise);

      MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 8, null, maxContentLength);
      Http2MultiplexedChannelPool h2Pool =
          new Http2MultiplexedChannelPool(connectionPool, loopGroup, Collections.singleton(record), null, 1,
              maxConcurrentStreamsPerConnection, maxContentLength, new Http2ClientMetrics(new MetricRegistry()));

      CompletableFuture<Boolean> interrupteFlagPreserved = new CompletableFuture<>();

      Thread t = new Thread(() -> {
        try {
          h2Pool.close();
        } catch (Exception e) {
          if (e.getCause() instanceof InterruptedException && Thread.currentThread().isInterrupted()) {
            interrupteFlagPreserved.complete(true);
          }
        }
      });

      t.start();
      t.interrupt();
      t.join();
      assertTrue(interrupteFlagPreserved.join());
    } finally {
      channel.close().awaitUninterruptibly();
    }
  }
}
