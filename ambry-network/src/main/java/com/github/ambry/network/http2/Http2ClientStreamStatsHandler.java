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

import com.github.ambry.network.RequestInfo;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A handler to stats response streams.
 */

@ChannelHandler.Sharable
class Http2ClientStreamStatsHandler extends SimpleChannelInboundHandler<Http2Frame> {
  private static final Logger logger = LoggerFactory.getLogger(Http2ClientStreamStatsHandler.class);
  private final Http2ClientMetrics http2ClientMetrics;

  public Http2ClientStreamStatsHandler(Http2ClientMetrics http2ClientMetrics) {
    this.http2ClientMetrics = http2ClientMetrics;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2Frame frame) throws Exception {
    ReferenceCountUtil.retain(frame);
    RequestInfo requestInfo = ctx.channel().attr(Http2NetworkClient.REQUEST_INFO).get();
    requestInfo.responseFramesCount++;
    long time = System.currentTimeMillis() - requestInfo.getStreamSendTime();
    if (frame instanceof Http2HeadersFrame) {
      http2ClientMetrics.http2StreamRoundTripTime.update(time);
      requestInfo.setStreamHeaderFrameReceiveTime(System.currentTimeMillis());
      logger.debug("Header Frame received. Time from send: {}ms. Request: {}", time, requestInfo);
    } else if (frame instanceof Http2DataFrame) {
      logger.debug("Data Frame size: {}. Time from send: {}ms. Request: {}",
          ((Http2DataFrame) frame).content().readableBytes(), time, requestInfo);
    }

    if (frame instanceof Http2DataFrame && ((Http2DataFrame) frame).isEndStream()) {
      http2ClientMetrics.http2StreamFirstToLastFrameTime.update(time);
      http2ClientMetrics.http2ResponseFrameCount.update(requestInfo.responseFramesCount);
      logger.debug("All Frame received. Time from send: {}ms. Request: {}", time, requestInfo);
    }
    ctx.fireChannelRead(frame);
  }
}

