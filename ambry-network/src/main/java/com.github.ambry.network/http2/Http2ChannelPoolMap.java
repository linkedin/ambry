/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;


/**
 * Implementation of {@link AbstractChannelPoolMap}. Each Host:Port has a ChannelPool.
 * The ChannelPool is {@link Http2MultiplexedChannelPool}, which leverages http2 multiplexing.
 */
public class Http2ChannelPoolMap extends AbstractChannelPoolMap<InetSocketAddress, ChannelPool> {
  private final EventLoopGroup eventLoopGroup;
  private final SSLFactory sslFactory;
  private final Http2ClientConfig http2ClientConfig;

  public Http2ChannelPoolMap(SSLFactory sslFactory, EventLoopGroup eventLoopGroup,
      Http2ClientConfig http2ClientConfig) {
    this.sslFactory = sslFactory;
    this.eventLoopGroup = eventLoopGroup;
    this.http2ClientConfig = http2ClientConfig;
  }

  @Override
  protected ChannelPool newPool(InetSocketAddress inetSocketAddress) {
    Bootstrap bootstrap = new Bootstrap().group(eventLoopGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .remoteAddress(inetSocketAddress);

    Http2ChannelPoolHandler http2ChannelPoolHandler =
        new Http2ChannelPoolHandler(sslFactory, inetSocketAddress.getHostName(), inetSocketAddress.getPort());
    return new Http2MultiplexedChannelPool(bootstrap, http2ChannelPoolHandler, eventLoopGroup,
        http2ClientConfig.idleConnectionTimeoutMs, http2ClientConfig.http2MinConnectionPerPort,
        http2ClientConfig.http2MaxConcurrentStreamsPerConnection);
  }
}
