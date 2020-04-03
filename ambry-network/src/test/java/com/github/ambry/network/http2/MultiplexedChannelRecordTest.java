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

import static org.junit.Assert.*;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class MultiplexedChannelRecordTest {
  private EventLoopGroup loopGroup;
  private MockChannel channel;
  private Long idleTimeoutMillis;
  private int maxContentLength;

  @Before
  public void setup() throws Exception {
    loopGroup = new NioEventLoopGroup(4);
    channel = new MockChannel();
    idleTimeoutMillis = 500L;
    maxContentLength = 1024 * 1024;
  }

  @After
  public void teardown() {
    loopGroup.shutdownGracefully().awaitUninterruptibly();
    channel.close();
  }

  /**
   * Regular stream acquire and release test.
   */
  @Test
  public void streamAcquireReleaseTest() {
    EmbeddedChannel channel = newHttp2Channel();
    int maxConcurrentStreams = 100;
    int testStreams = 10;
    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, maxConcurrentStreams, null,
        maxContentLength);

    List<Promise<Channel>> streamPromises = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Promise<Channel> streamPromise = channel.eventLoop().newPromise();
      record.acquireStream(streamPromise);
      streamPromises.add(streamPromise);
    }

    channel.runPendingTasks();
    for (Promise<Channel> streamPromise : streamPromises) {
      assertTrue(streamPromise.isSuccess());
      assertTrue(channel.isOpen());
    }

    Assert.assertEquals(record.getNumOfAvailableStreams().get(), maxConcurrentStreams - testStreams);

    for (Promise<Channel> streamPromise : streamPromises) {
      record.closeAndReleaseChild(streamPromise.getNow());
    }

    assertTrue(channel.isOpen());

    channel.runPendingTasks();
    Assert.assertEquals(record.getNumOfAvailableStreams().get(), maxConcurrentStreams);

    assertTrue(channel.isOpen());
  }

  /**
   * False from acquireStream() is expected if no stream available.
   */
  @Test
  public void availableStream0ShouldBeFalse() {
    loopGroup.register(channel).awaitUninterruptibly();
    Promise<Channel> channelPromise = new DefaultPromise<>(loopGroup.next());
    channelPromise.setSuccess(channel);

    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 0, null, maxContentLength);

    assertFalse(record.acquireStream(null));
  }

  /**
   * False from acquireStream() is expected if run out of streams.
   */
  @Test
  public void enforceMaxNumberOfStreamTest() {
    EmbeddedChannel channel = newHttp2Channel();
    int maxConcurrentStreams = 10;
    int totalStreams = 20;
    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, maxConcurrentStreams, null,
        maxContentLength);

    List<Promise<Channel>> streamPromises = new ArrayList<>();

    for (int i = 0; i < maxConcurrentStreams; i++) {
      Promise<Channel> streamPromise = channel.eventLoop().newPromise();
      assertTrue(record.acquireStream(streamPromise));
      streamPromises.add(streamPromise);
    }

    for (long i = maxConcurrentStreams; i < totalStreams; i++) {
      Promise<Channel> streamPromise = channel.eventLoop().newPromise();
      assertFalse(record.acquireStream(streamPromise));
    }

    // Release streams
    for (Promise<Channel> streamPromise : streamPromises) {
      record.closeAndReleaseChild(streamPromise.getNow());
    }

    assertTrue(channel.isOpen());

    channel.runPendingTasks();
    Assert.assertEquals(record.getNumOfAvailableStreams().get(), maxConcurrentStreams);
  }

  /**
   * Idle timeout doesn't apply before first stream is created.
   */
  @Test
  public void idleTimerDoesNotApplyBeforeFirstStreamIsCreated() throws InterruptedException {
    EmbeddedChannel channel = newHttp2Channel();
    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 2, idleTimeoutMillis, maxContentLength);

    Thread.sleep(idleTimeoutMillis * 2);
    channel.runPendingTasks();

    assertTrue(channel.isOpen());
  }

  /**
   * Idle timeout test.
   */
  @Test
  public void recordsWithoutInFlightStreamsAreClosedAfterTimeout() throws InterruptedException {
    EmbeddedChannel channel = newHttp2Channel();
    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 1, idleTimeoutMillis, maxContentLength);

    Promise<Channel> streamPromise = channel.eventLoop().newPromise();
    record.acquireStream(streamPromise);

    channel.runPendingTasks();

    assertTrue(streamPromise.isSuccess());
    assertTrue(channel.isOpen());

    record.closeAndReleaseChild(streamPromise.getNow());

    assertTrue(channel.isOpen());

    Thread.sleep(idleTimeoutMillis * 2);
    channel.runPendingTasks();

    assertFalse(channel.isOpen());
  }

  /**
   * Idle timeout is not applied if there is active stream.
   */
  @Test
  public void recordsWithReservedStreamsAreNotClosedAfterTimeout() throws InterruptedException {
    EmbeddedChannel channel = newHttp2Channel();
    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 2, idleTimeoutMillis, maxContentLength);

    Promise<Channel> streamPromise = channel.eventLoop().newPromise();
    Promise<Channel> streamPromise2 = channel.eventLoop().newPromise();
    record.acquireStream(streamPromise);
    record.acquireStream(streamPromise2);

    channel.runPendingTasks();

    assertTrue(streamPromise.isSuccess());
    assertTrue(streamPromise2.isSuccess());
    assertTrue(channel.isOpen());

    record.closeAndReleaseChild(streamPromise.getNow());

    assertTrue(channel.isOpen());

    Thread.sleep(idleTimeoutMillis * 2);
    channel.runPendingTasks();

    assertTrue(channel.isOpen());
  }

  /**
   * Stream acquisition resets timer.
   */
  @Test
  public void acquireRequestResetsCloseTimer() throws InterruptedException {
    EmbeddedChannel channel = newHttp2Channel();
    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 2, idleTimeoutMillis, maxContentLength);

    for (int i = 0; i < 20; ++i) {
      Thread.sleep(idleTimeoutMillis / 10);
      channel.runPendingTasks();

      Promise<Channel> streamPromise = channel.eventLoop().newPromise();
      assertTrue(record.acquireStream(streamPromise));
      channel.runPendingTasks();

      assertTrue(streamPromise.isSuccess());
      assertTrue(channel.isOpen());

      record.closeAndReleaseChild(streamPromise.getNow());
      channel.runPendingTasks();
    }

    assertTrue(channel.isOpen());

    Thread.sleep(idleTimeoutMillis * 2);
    channel.runPendingTasks();

    assertFalse(channel.isOpen());
  }

  /**
   * IOException is expected if acquire stream from closed channel.
   */
  @Test
  public void acquireClaimedConnectionOnClosedChannelShouldThrowIOException() {
    loopGroup.register(channel).awaitUninterruptibly();
    Promise<Channel> channelPromise = new DefaultPromise<>(loopGroup.next());

    MultiplexedChannelRecord record = new MultiplexedChannelRecord(channel, 1, 10000L, maxContentLength);

    record.closeChildChannels();

    record.acquireClaimedStream(channelPromise);

    try {
      channelPromise.get();
    } catch (InterruptedException | ExecutionException e) {
      assertTrue(e.getCause() instanceof IOException);
    }
  }

  static EmbeddedChannel newHttp2Channel() {
    EmbeddedChannel channel =
        new EmbeddedChannel(Http2FrameCodecBuilder.forClient().build(), new Http2MultiplexHandler(new NoOpHandler()));
    return channel;
  }

  private static class NoOpHandler extends ChannelInitializer<Channel> {
    @Override
    protected void initChannel(Channel ch) {
    }
  }

  private class MockChannel extends EmbeddedChannel {
    public MockChannel() throws Exception {
      super.doRegister();
    }
  }
}
