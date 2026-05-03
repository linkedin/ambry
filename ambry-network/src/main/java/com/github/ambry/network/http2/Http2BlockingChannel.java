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

import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Send;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A HTTP2 implementation of {@link ConnectedChannel} used in replication and test.
 * This implementation reuses connections maintained by {@link Http2MultiplexedChannelPool}(a pool for a single host:port) and creates a stream channel for each send.
 */
public class Http2BlockingChannel implements ConnectedChannel {
  private static final Logger logger = LoggerFactory.getLogger(Http2BlockingChannel.class);
  private final ChannelPool channelPool;
  private final Http2ClientConfig http2ClientConfig;
  private final InetSocketAddress inetSocketAddress;
  // Non-null only when this instance allocated its own pool/event-loop (test constructor).
  // disconnect() must release them; production callers share these resources externally.
  private final EventLoopGroup ownedEventLoopGroup;
  private final boolean ownsChannelPool;
  final static AttributeKey<CompletableFuture<ByteBuf>> RESPONSE_PROMISE = AttributeKey.newInstance("ResponsePromise");
  final static AttributeKey<ChannelPool> CHANNEL_POOL_ATTRIBUTE_KEY = AttributeKey.newInstance("ChannelPool");

  public Http2BlockingChannel(ChannelPool channelPool, InetSocketAddress inetSocketAddress,
      Http2ClientConfig http2ClientConfig) {
    this.channelPool = channelPool;
    this.inetSocketAddress = inetSocketAddress;
    this.http2ClientConfig = http2ClientConfig;
    this.ownedEventLoopGroup = null;
    this.ownsChannelPool = false;
  }

  /**
   * Constructor for test purpose.
   */
  public Http2BlockingChannel(String hostName, int port, SSLConfig sslConfig, Http2ClientConfig http2ClientConfig,
      Http2ClientMetrics http2ClientMetrics) {
    NettySslHttp2Factory nettySslHttp2Factory;
    try {
      nettySslHttp2Factory = new NettySslHttp2Factory(sslConfig);
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("Can't create NettySslHttp2Factory: ", e);
    }
    this.http2ClientConfig = http2ClientConfig;
    this.inetSocketAddress = new InetSocketAddress(hostName, port);
    this.ownedEventLoopGroup = Epoll.isAvailable() ? new EpollEventLoopGroup() : new NioEventLoopGroup();
    this.channelPool = new Http2MultiplexedChannelPool(this.inetSocketAddress, nettySslHttp2Factory,
        this.ownedEventLoopGroup, http2ClientConfig, http2ClientMetrics,
        new Http2BlockingChannelStreamChannelInitializer(http2ClientConfig));
    this.ownsChannelPool = true;
  }

  @Override
  public void connect() throws IOException {

  }

  @Override
  public void disconnect() throws IOException {
    // No-op: existing test code calls connect/disconnect/connect/disconnect on a single
    // channel instance and depends on the channel remaining usable after disconnect. The
    // test-constructor's owned EventLoopGroup is a small leak in tests but not test-fatal
    // once the bigger Http2NetworkClientFactory leak is fixed. To explicitly release this
    // channel's resources, call close().
  }

  /**
   * Release the channel pool and event-loop group this instance owns (test-constructor only).
   * Safe to call multiple times; subsequent calls are no-ops. Production callers that pass an
   * external pool need not call this.
   */
  public void close() {
    if (ownsChannelPool && channelPool instanceof Http2MultiplexedChannelPool) {
      try {
        ((Http2MultiplexedChannelPool) channelPool).close();
      } catch (Exception e) {
        logger.warn("Error closing owned Http2MultiplexedChannelPool for {}", inetSocketAddress, e);
      }
    }
    if (ownedEventLoopGroup != null) {
      try {
        ownedEventLoopGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).await(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * A new stream channel is created for each send.
   */
  @Override
  public void send(Send request) throws IOException {
    logger.error("Http2 doesn't support send() receive(). Please use sendAndReceive()");
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelOutput receive() throws IOException {
    logger.error("Http2 doesn't support send() receive(). Please use sendAndReceive()");
    throw new UnsupportedOperationException();
  }

  public ChannelOutput sendAndReceive(Send request) throws IOException {
    Channel streamChannel;
    try {
      streamChannel =
          channelPool.acquire().get(http2ClientConfig.http2BlockingChannelAcquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IOException("Can't acquire stream channel from " + getRemoteHost() + ":" + getRemotePort(), e);
    }

    CompletableFuture<ByteBuf> responsePromise = new CompletableFuture<ByteBuf>();
    streamChannel.attr(RESPONSE_PROMISE).set(responsePromise);
    streamChannel.attr(CHANNEL_POOL_ATTRIBUTE_KEY).set(channelPool);
    boolean success = streamChannel.writeAndFlush(request)
        .awaitUninterruptibly(http2ClientConfig.http2BlockingChannelSendTimeoutMs, TimeUnit.MILLISECONDS);
    if (!success) {
      if (streamChannel.attr(RESPONSE_PROMISE).getAndSet(null) != null) {
        channelPool.release(streamChannel);
      }
      throw new IOException(
          "Failed to write and flush request on time, from " + getRemoteHost() + ":" + getRemotePort());
    }

    ByteBuf responseByteBuf;
    try {
      responseByteBuf =
          responsePromise.get(http2ClientConfig.http2BlockingChannelReceiveTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      if (streamChannel.attr(RESPONSE_PROMISE).getAndSet(null) != null) {
        channelPool.release(streamChannel);
      }
      throw new IOException("Failed to receive response from " + getRemoteHost() + ":" + getRemotePort(), e);
    }
    NettyByteBufDataInputStream dataInputStream = new NettyByteBufDataInputStream(responseByteBuf);
    // Because readLong() is called to get entire stream size(long),
    // the size of remaining data should be dataInputStream.readLong() - 8
    return new ChannelOutput(dataInputStream, dataInputStream.readLong() - Long.BYTES);
  }

  @Override
  public String getRemoteHost() {
    return inetSocketAddress.getHostName();
  }

  @Override
  public int getRemotePort() {
    return inetSocketAddress.getPort();
  }
}
