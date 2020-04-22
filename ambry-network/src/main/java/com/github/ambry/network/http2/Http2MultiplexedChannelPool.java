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

import com.github.ambry.commons.NettyUtils;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseCombiner;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import io.netty.bootstrap.Bootstrap;
import org.slf4j.LoggerFactory;


public class Http2MultiplexedChannelPool implements ChannelPool {
  private static final Logger log = LoggerFactory.getLogger(Http2MultiplexedChannelPool.class);

  /**
   * Reference to the {@link MultiplexedChannelRecord} on a channel.
   * Attribute of parent channel.
   */
  static final AttributeKey<MultiplexedChannelRecord> MULTIPLEXED_CHANNEL =
      AttributeKey.newInstance("MULTIPLEXED_CHANNEL");

  /**
   * Reference to {@link Http2MultiplexedChannelPool} where stream channel is acquired.
   * Attribute of parent channel.
   */
  static final AttributeKey<Http2MultiplexedChannelPool> HTTP2_MULTIPLEXED_CHANNEL_POOL =
      AttributeKey.newInstance("HTTP2_MULTIPLEXED_CHANNEL_POOL");

  /**
   * Whether a parent channel has been released yet. This guards against double-releasing to the connection pool.
   */
  private static final AttributeKey<Boolean> PARENT_CHANNEL_RELEASED =
      AttributeKey.newInstance("PARENT_CHANNEL_RELEASED");

  // HTTP/2 physical connection pool.
  private final ChannelPool parentConnectionPool;
  private final EventLoopGroup eventLoopGroup;
  private final Set<MultiplexedChannelRecord> parentConnections;
  private final Long idleConnectionTimeoutMs;
  private final int minParentConnections;
  private final int maxConcurrentStreamsAllowed;
  private final int maxContentLength;
  private final Http2ClientMetrics http2ClientMetrics;

  private AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * @param inetSocketAddress IP Socket Address (IP address + port number).
   * @param sslFactory {@link SSLFactory} used for SSL connection.
   * @param eventLoopGroup The event loop group.
   * @param http2ClientConfig http2 client configs.
   * @param http2ClientMetrics http2 client metrics.
   */
  Http2MultiplexedChannelPool(InetSocketAddress inetSocketAddress, SSLFactory sslFactory, EventLoopGroup eventLoopGroup,
      Http2ClientConfig http2ClientConfig, Http2ClientMetrics http2ClientMetrics) {
    this(new SimpleChannelPool(new Bootstrap().group(eventLoopGroup)
            .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_RCVBUF, http2ClientConfig.nettyReceiveBufferSize)
            .option(ChannelOption.SO_SNDBUF, http2ClientConfig.nettySendBufferSize)
            // To honor http2 window size, WriteBufferWaterMark.high() should be greater or equal to http2 window size.
            // Also see: https://github.com/netty/netty/issues/10193
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(http2ClientConfig.http2InitialWindowSize,
                    2 * http2ClientConfig.http2InitialWindowSize))
            .remoteAddress(inetSocketAddress),
            new Http2ChannelPoolHandler(sslFactory, inetSocketAddress.getHostName(), inetSocketAddress.getPort(),
                http2ClientConfig)), eventLoopGroup, ConcurrentHashMap.newKeySet(),
        http2ClientConfig.idleConnectionTimeoutMs, http2ClientConfig.http2MinConnectionPerPort,
        http2ClientConfig.http2MaxConcurrentStreamsPerConnection, http2ClientConfig.http2MaxContentLength,
        http2ClientMetrics);
  }

  /**
   * @param connectionPool The {@link ChannelPool} to acquire parent channel.
   * @param eventLoopGroup The event loop group.
   * @param idleConnectionTimeoutMs the idle time before a channel is closed.
   * @param minParentConnections Minimum number of parent channel will be created before reuse.
   * @param maxConcurrentStreamsAllowed The maximum streams allowed per parent channel.
   * @param maxContentLength Maximum content length for a full HTTP/2 content. Used in HttpObjectAggregator.
   * @param http2ClientMetrics Http2 client metrics.
   */
  Http2MultiplexedChannelPool(ChannelPool connectionPool, EventLoopGroup eventLoopGroup,
      Set<MultiplexedChannelRecord> connections, Long idleConnectionTimeoutMs, int minParentConnections,
      int maxConcurrentStreamsAllowed, int maxContentLength, Http2ClientMetrics http2ClientMetrics) {
    this.parentConnectionPool = connectionPool;
    this.eventLoopGroup = eventLoopGroup;
    this.parentConnections = connections;
    this.idleConnectionTimeoutMs = idleConnectionTimeoutMs;
    this.minParentConnections = minParentConnections;
    this.maxConcurrentStreamsAllowed = maxConcurrentStreamsAllowed;
    this.maxContentLength = maxContentLength;
    this.http2ClientMetrics = http2ClientMetrics;
  }

  @Override
  public Future<Channel> acquire() {
    return acquire(eventLoopGroup.next().newPromise());
  }

  @Override
  public Future<Channel> acquire(Promise<Channel> promise) {
    http2ClientMetrics.http2NewStreamCount.inc();
    if (closed.get()) {
      return promise.setFailure(new IOException("Channel pool is closed!"));
    }

    if (parentConnections.size() >= minParentConnections) {
      // TODO: if warmup is not 100%, new connections are still required.
      // This is a passive load balance depends on compareAndSet in claimStream().
      for (MultiplexedChannelRecord multiplexedChannel : parentConnections) {
        if (acquireStreamOnInitializedConnection(multiplexedChannel, promise)) {
          return promise;
        }
        http2ClientMetrics.http2StreamSlipAcquireCount.inc();
      }
    }

    // No connection or No available streams on existing connections, establish new connection and add it to set.
    acquireStreamOnNewConnection(promise);
    return promise;
  }

  private void acquireStreamOnNewConnection(Promise<Channel> promise) {
    log.trace("Creating new connection, number of connections: {}", parentConnections.size());
    http2ClientMetrics.http2NewConnectionCount.inc();
    long startTime = System.currentTimeMillis();
    Future<Channel> newConnectionAcquire = parentConnectionPool.acquire();

    newConnectionAcquire.addListener(f -> {
      if (!newConnectionAcquire.isSuccess()) {
        http2ClientMetrics.http2NewConnectionFailureCount.inc();
        promise.setFailure(newConnectionAcquire.cause());
        return;
      }
      http2ClientMetrics.http2ConnectionAcquireTime.update(System.currentTimeMillis() - startTime);
      Channel parentChannel = newConnectionAcquire.getNow();
      try {
        parentChannel.attr(HTTP2_MULTIPLEXED_CHANNEL_POOL).set(this);

        // When the protocol future is completed on the new connection, we're ready for new streams to be added to it.
        acquireStreamOnFreshConnection(promise, parentChannel);
      } catch (Throwable e) {
        failAndCloseParent(promise, parentChannel, e);
      }
    });
  }

  private void acquireStreamOnFreshConnection(Promise<Channel> promise, Channel parentChannel) {
    long startTime = System.currentTimeMillis();
    try {
      MultiplexedChannelRecord multiplexedChannel =
          new MultiplexedChannelRecord(parentChannel, maxConcurrentStreamsAllowed, idleConnectionTimeoutMs,
              maxContentLength);
      parentChannel.attr(MULTIPLEXED_CHANNEL).set(multiplexedChannel);

      Promise<Channel> streamPromise = parentChannel.eventLoop().newPromise();

      if (!acquireStreamOnInitializedConnection(multiplexedChannel, streamPromise)) {
        failAndCloseParent(promise, parentChannel,
            new IOException("Connection was closed while creating a new stream."));
        return;
      }

      streamPromise.addListener(f -> {
        if (!streamPromise.isSuccess()) {
          promise.setFailure(streamPromise.cause());
          return;
        }

        Channel stream = streamPromise.getNow();
        cacheConnectionForFutureStreams(stream, multiplexedChannel, promise);
        http2ClientMetrics.http2FirstStreamAcquireTime.update(System.currentTimeMillis() - startTime);
      });
    } catch (Throwable e) {
      failAndCloseParent(promise, parentChannel, e);
    }
  }

  private void cacheConnectionForFutureStreams(Channel stream, MultiplexedChannelRecord multiplexedChannel,
      Promise<Channel> promise) {
    Channel parentChannel = stream.parent();

    // Before we cache the connection, make sure that exceptions on the connection will remove it from the cache.
    parentChannel.pipeline().addLast(ReleaseOnExceptionHandler.INSTANCE);
    parentConnections.add(multiplexedChannel);

    if (closed.get()) {
      // Whoops, we were closed while we were setting up. Make sure everything here is cleaned up properly.
      failAndCloseParent(promise, parentChannel,
          new IOException("Connection pool was closed while creating a new stream."));
      return;
    }

    promise.setSuccess(stream);
  }

  private Void failAndCloseParent(Promise<Channel> promise, Channel parentChannel, Throwable exception) {
    promise.setFailure(exception);
    closeAndReleaseParent(parentChannel);
    return null;
  }

  /**
   * Acquire a stream on a connection that has already been initialized. This will return false if the connection cannot have
   * any more streams allocated, and true if the stream can be allocated.
   *
   * This will NEVER complete the provided future when the return value is false. This will ALWAYS complete the provided
   * future when the return value is true.
   */
  private boolean acquireStreamOnInitializedConnection(MultiplexedChannelRecord channelRecord,
      Promise<Channel> promise) {
    long startTime = System.currentTimeMillis();
    Promise<Channel> acquirePromise = channelRecord.getParentChannel().eventLoop().newPromise();

    if (!channelRecord.acquireStream(acquirePromise)) {
      return false;
    }

    acquirePromise.addListener(f -> {
      try {
        if (!acquirePromise.isSuccess()) {
          promise.setFailure(acquirePromise.cause());
          return;
        }

        Channel channel = acquirePromise.getNow();
        channel.parent().attr(HTTP2_MULTIPLEXED_CHANNEL_POOL).set(this);
        channel.parent().attr(MULTIPLEXED_CHANNEL).set(channelRecord);
        promise.setSuccess(channel);
        http2ClientMetrics.http2RegularStreamAcquireTime.update(System.currentTimeMillis() - startTime);
      } catch (Exception e) {
        promise.setFailure(e);
      }
    });

    return true;
  }

  @Override
  public Future<Void> release(Channel childChannel) {
    return release(childChannel, childChannel.eventLoop().newPromise());
  }

  @Override
  public Future<Void> release(Channel childChannel, Promise<Void> promise) {
    if (childChannel.parent() == null) {
      // This isn't a child channel. Oddly enough, this is "expected" and is handled properly by the
      // BetterFixedChannelPool AS LONG AS we return an IllegalArgumentException via the promise.
      closeAndReleaseParent(childChannel);
      return promise.setFailure(new IllegalArgumentException("Channel (" + childChannel + ") is not a child channel."));
    }

    Channel parentChannel = childChannel.parent();
    MultiplexedChannelRecord multiplexedChannel = parentChannel.attr(MULTIPLEXED_CHANNEL).get();
    if (multiplexedChannel == null) {
      // This is a child channel, but there is no attached multiplexed channel, which there should be if it was from
      // this pool. Close it and log an error.
      Exception exception = new IOException(
          "Channel (" + childChannel + ") is not associated with any channel records. "
              + "It will be closed, but cannot be released within this pool.");
      log.error(exception.getMessage());
      childChannel.close();
      return promise.setFailure(exception);
    }

    multiplexedChannel.closeAndReleaseChild(childChannel);

    if (multiplexedChannel.canBeClosedAndReleased()) {
      log.debug("Parent channel closed: " + parentChannel.remoteAddress());
      // We just closed the last stream in a connection that has reached the end of its life.
      return closeAndReleaseParent(parentChannel, null, promise);
    }

    return promise.setSuccess(null);
  }

  private Future<Void> closeAndReleaseParent(Channel parentChannel) {
    return closeAndReleaseParent(parentChannel, null, parentChannel.eventLoop().newPromise());
  }

  private Future<Void> closeAndReleaseParent(Channel parentChannel, Throwable cause) {
    return closeAndReleaseParent(parentChannel, cause, parentChannel.eventLoop().newPromise());
  }

  private Future<Void> closeAndReleaseParent(Channel parentChannel, Throwable cause, Promise<Void> resultPromise) {
    if (parentChannel.parent() != null) {
      // This isn't a parent channel. Notify it that something is wrong.
      Exception exception = new IOException(
          "Channel (" + parentChannel + ") is not a parent channel. It will be closed, "
              + "but cannot be released within this pool.");
      log.error(exception.getMessage());
      parentChannel.close();
      return resultPromise.setFailure(exception);
    }

    MultiplexedChannelRecord multiplexedChannel = parentChannel.attr(MULTIPLEXED_CHANNEL).get();

    // We may not have a multiplexed channel if the parent channel hasn't been fully initialized.
    if (multiplexedChannel != null) {
      if (cause == null) {
        multiplexedChannel.closeChildChannels();
      } else {
        multiplexedChannel.closeChildChannels(cause);
      }
      parentConnections.remove(multiplexedChannel);
    }

    parentChannel.close();
    if (parentChannel.attr(PARENT_CHANNEL_RELEASED).getAndSet(Boolean.TRUE) == null) {
      return parentConnectionPool.release(parentChannel, resultPromise);
    }

    return resultPromise.setSuccess(null);
  }

  public void handleGoAway(Channel parentChannel, int lastStreamId, GoAwayException exception) {
    log.debug("Received GOAWAY on " + parentChannel + " with lastStreamId of " + lastStreamId);
    try {
      MultiplexedChannelRecord multiplexedChannel = parentChannel.attr(MULTIPLEXED_CHANNEL).get();

      if (multiplexedChannel != null) {
        multiplexedChannel.handleGoAway(lastStreamId, exception);
      } else {
        // If we don't have a multiplexed channel, the parent channel hasn't been fully initialized. Close it now.
        closeAndReleaseParent(parentChannel);
      }
    } catch (Exception e) {
      log.error("Failed to handle GOAWAY frame on channel " + parentChannel, e);
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      Future<?> closeCompleteFuture = doClose();

      try {
        if (!closeCompleteFuture.await(10, TimeUnit.SECONDS)) {
          throw new RuntimeException("Event loop didn't close after 10 seconds.");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      Throwable exception = closeCompleteFuture.cause();
      if (exception != null) {
        throw new RuntimeException("Failed to close channel pool.", exception);
      }
    }
  }

  private Future<?> doClose() {
    EventLoop closeEventLoop = eventLoopGroup.next();
    Promise<?> closeFinishedPromise = closeEventLoop.newPromise();

    NettyUtils.doInEventLoop(closeEventLoop, () -> {
      Promise<Void> releaseAllChannelsPromise = closeEventLoop.newPromise();
      PromiseCombiner promiseCombiner = new PromiseCombiner(closeEventLoop);

      // Create a copy of the connections to remove while we close them, in case closing updates the original list.
      List<MultiplexedChannelRecord> channelsToRemove = new ArrayList<>(parentConnections);
      for (MultiplexedChannelRecord channel : channelsToRemove) {
        promiseCombiner.add(closeAndReleaseParent(channel.getParentChannel()));
      }
      promiseCombiner.finish(releaseAllChannelsPromise);

      releaseAllChannelsPromise.addListener(f -> {
        parentConnectionPool.close();
        closeFinishedPromise.setSuccess(null);
      });
    });

    return closeFinishedPromise;
  }

  @ChannelHandler.Sharable
  private static final class ReleaseOnExceptionHandler extends ChannelDuplexHandler {
    private static final ReleaseOnExceptionHandler INSTANCE = new ReleaseOnExceptionHandler();

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      closeAndReleaseParent(ctx, new ClosedChannelException());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      closeAndReleaseParent(ctx, cause);
    }

    private void closeAndReleaseParent(ChannelHandlerContext ctx, Throwable cause) {
      Http2MultiplexedChannelPool pool = ctx.channel().attr(HTTP2_MULTIPLEXED_CHANNEL_POOL).get();
      pool.closeAndReleaseParent(ctx.channel(), cause);
    }
  }
}


