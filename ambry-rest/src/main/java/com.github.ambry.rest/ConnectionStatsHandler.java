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
package com.github.ambry.rest;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler that tracks connection establishment statistics.
 */
@ChannelHandler.Sharable
public class ConnectionStatsHandler extends ChannelInboundHandlerAdapter {
  private final NettyMetrics metrics;
  private final AtomicLong openConnections;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public ConnectionStatsHandler(NettyMetrics metrics) {
    this.metrics = metrics;
    openConnections = new AtomicLong(0);
    metrics.registerConnectionsStatsHandler(openConnections);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    logger.trace("Channel Active " + ctx.channel().remoteAddress());
    metrics.connectionsConnectedCount.inc();
    openConnections.incrementAndGet();
    logHandshakeStatus(ctx);
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.trace("Channel Inactive " + ctx.channel().remoteAddress());
    metrics.connectionsDisconnectedCount.inc();
    openConnections.decrementAndGet();
    super.channelInactive(ctx);
  }

  /**
   * Listen to the handshake future if this is an SSL connection. Log and update metrics if the handshake failed.
   * @param ctx the {@link ChannelHandlerContext}.
   */
  private void logHandshakeStatus(ChannelHandlerContext ctx) {
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    if (sslHandler != null) {
      sslHandler.handshakeFuture().addListener(future -> {
        if (!future.isSuccess()) {
          logger.debug("SSL handshake failed for channel: {}", ctx.channel(), future.cause());
          metrics.handshakeFailureCount.inc();
        }
      });
    }
  }
}
