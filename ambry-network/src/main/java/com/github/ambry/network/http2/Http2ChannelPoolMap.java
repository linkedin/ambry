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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPool;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link AbstractChannelPoolMap}. Each Host:Port has a ChannelPool.
 * The ChannelPool is {@link Http2MultiplexedChannelPool}, which leverages http2 multiplexing.
 */
public class Http2ChannelPoolMap extends AbstractChannelPoolMap<InetSocketAddress, ChannelPool> {
  private static final Logger log = LoggerFactory.getLogger(Http2MultiplexedChannelPool.class);
  private final EventLoopGroup eventLoopGroup;
  private final SSLFactory sslFactory;
  private final Http2ClientConfig http2ClientConfig;
  private final Http2ClientMetrics http2ClientMetrics;

  public Http2ChannelPoolMap(SSLFactory sslFactory, EventLoopGroup eventLoopGroup, Http2ClientConfig http2ClientConfig,
      Http2ClientMetrics http2ClientMetrics) {
    this.sslFactory = sslFactory;
    this.eventLoopGroup = eventLoopGroup;
    this.http2ClientConfig = http2ClientConfig;
    this.http2ClientMetrics = http2ClientMetrics;
  }

  @Override
  protected ChannelPool newPool(InetSocketAddress inetSocketAddress) {
    log.trace("New pool created for " + inetSocketAddress);
    http2ClientMetrics.http2NewPoolCount.inc();
    return new Http2MultiplexedChannelPool(inetSocketAddress, sslFactory, eventLoopGroup, http2ClientConfig,
        http2ClientMetrics);
  }
}
