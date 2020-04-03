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
package com.github.ambry.rest;

import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.stream.ChunkedWriteHandler;


/*
 * HTTP2 stream handler for each stream.
 */
@ChannelHandler.Sharable
public class Http2StreamHandler extends ChannelInboundHandlerAdapter {

  private NettyMetrics nettyMetrics;
  private NettyConfig nettyConfig;
  private PerformanceConfig performanceConfig;
  private Http2ClientConfig http2ClientConfig;
  private RestRequestHandler requestHandler;

  public Http2StreamHandler(NettyMetrics nettyMetrics, NettyConfig nettyConfig, PerformanceConfig performanceConfig,
      Http2ClientConfig http2ClientConfig, RestRequestHandler requestHandler) {
    this.nettyMetrics = nettyMetrics;
    this.nettyConfig = nettyConfig;
    this.performanceConfig = performanceConfig;
    this.http2ClientConfig = http2ClientConfig;
    this.requestHandler = requestHandler;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    ctx.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(true));
    // TODO: Remove HttpObjectAggregator. Currently, without HttpObjectAggregator, last httpContent can't be recognized as LastHttpContent.
    ctx.pipeline().addLast(new HttpObjectAggregator(http2ClientConfig.http2MaxContentLength));
    // NettyMessageProcessor depends on ChunkedWriteHandler.
    // TODO: Add deployment health check handler.
    ctx.pipeline().addLast(new ChunkedWriteHandler());
    ctx.pipeline().addLast(new NettyMessageProcessor(nettyMetrics, nettyConfig, performanceConfig, requestHandler));
  }
}
