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

import com.github.ambry.network.NettyServerRequest;
import com.github.ambry.network.RequestResponseChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Retain content of {@link FullHttpRequest} and send to {@link RequestResponseChannel}.
 */
@ChannelHandler.Sharable
public class AmbryNetworkRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = LoggerFactory.getLogger(AmbryNetworkRequestHandler.class);
  private final RequestResponseChannel requestResponseChannel;
  private final Http2ServerMetrics http2ServerMetrics;

  public AmbryNetworkRequestHandler(RequestResponseChannel requestResponseChannel,
      Http2ServerMetrics http2ServerMetrics) {
    this.requestResponseChannel = requestResponseChannel;
    this.http2ServerMetrics = http2ServerMetrics;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {

    ByteBuf dup = msg.content().retainedDuplicate();
    try {
      requestResponseChannel.sendRequest(new NettyServerRequest(ctx, dup));
    } catch (InterruptedException e) {
      dup.release();
      http2ServerMetrics.requestResponseChannelErrorCount.inc();
      logger.warn("Can't send NettyServerRequest to requestResponseChannel. Cause: ", e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    http2ServerMetrics.http2StreamExceptionCount.inc();
    logger.warn("Exception caught in AmbryNetworkRequestHandler, cause: ", cause);
    ctx.channel().close();
  }
}
