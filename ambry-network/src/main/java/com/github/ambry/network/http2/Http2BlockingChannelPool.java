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

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A connection pool that uses {@link Http2MultiplexedChannelPool} as the underlying channel for communication.
 * {@link Http2MultiplexedChannelPool} maintains physical connections to a host:port.
 * A stream channel represented by {@link Http2BlockingChannel} is checked out when checkOutConnection() is invoked.
 */
public class Http2BlockingChannelPool implements ConnectionPool {
  private static final Logger logger = LoggerFactory.getLogger(Http2BlockingChannelPool.class);
  private Http2ChannelPoolMap http2ChannelPoolMap;
  private EventLoopGroup eventLoopGroup;
  private Http2ClientConfig http2ClientConfig;

  public Http2BlockingChannelPool(SSLFactory sslFactory, Http2ClientConfig http2ClientConfig,
      Http2ClientMetrics http2ClientMetrics) {
    if (Epoll.isAvailable()) {
      logger.info("Using EpollEventLoopGroup in Http2BlockingChannelPool.");
      this.eventLoopGroup = new EpollEventLoopGroup(http2ClientConfig.http2NettyEventLoopGroupThreads);
    } else {
      this.eventLoopGroup = new NioEventLoopGroup(http2ClientConfig.http2NettyEventLoopGroupThreads);
    }
    http2ChannelPoolMap = new Http2ChannelPoolMap(sslFactory, eventLoopGroup, http2ClientConfig, http2ClientMetrics,
        new Http2BlockingChannelStreamChannelInitializer(http2ClientConfig.http2MaxContentLength));
    this.http2ClientConfig = http2ClientConfig;
  }

  @Override
  public void start() {
    logger.info("Http2BlockingChannelPool started");
  }

  @Override
  public void shutdown() {
    logger.info("Shutting down Http2BlockingChannelPool.");
    try {
      eventLoopGroup.shutdownGracefully().await(http2ClientConfig.http2BlockingChannelPoolShutdownTimeoutMs);
    } catch (InterruptedException e) {
      logger.info("EventLoopGroup shutdown timeout: {} ms",
          http2ClientConfig.http2BlockingChannelPoolShutdownTimeoutMs);
    }
  }

  @Override
  public ConnectedChannel checkOutConnection(String host, Port port, long timeout)
      throws IOException, InterruptedException, ConnectionPoolTimeoutException {
    InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port.getPort());
    return new Http2BlockingChannel((Http2MultiplexedChannelPool) (http2ChannelPoolMap.get(inetSocketAddress)));
  }

  @Override
  public void checkInConnection(ConnectedChannel connectedChannel) {

  }

  @Override
  public void destroyConnection(ConnectedChannel connectedChannel) {

  }
}
