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
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private Http2NetworkClient http2NetworkClient;

  public Http2ClientStreamStatsHandler(Http2NetworkClient http2NetworkClient) {
    this.http2NetworkClient = http2NetworkClient;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2Frame frame) throws Exception {
    ReferenceCountUtil.retain(frame);
    RequestInfo requestInfo = ctx.channel().attr(Http2NetworkClient.REQUEST_INFO).get();
    requestInfo.responseFramesCount++;
    if (frame instanceof Http2HeadersFrame) {
      requestInfo.setStreamHeaderFrameReceiveTime(System.currentTimeMillis());
      http2NetworkClient.getHttp2ClientMetrics().http2StreamRoundTripTime.update(
          System.currentTimeMillis() - requestInfo.getStreamSendTime());
      logger.debug("Header Frame received. Request: {}", requestInfo);
    } else if (frame instanceof Http2DataFrame) {
      logger.debug("Data Frame size: {}. Request: {}", ((Http2DataFrame) frame).content().readableBytes(), requestInfo);
    }

    if (frame instanceof Http2DataFrame && ((Http2DataFrame) frame).isEndStream()) {
      long time = System.currentTimeMillis() - requestInfo.getStreamHeaderFrameReceiveTime();
      http2NetworkClient.getHttp2ClientMetrics().http2StreamFirstToLastFrameTime.update(time);
      http2NetworkClient.getHttp2ClientMetrics().http2ResponseFrameCount.update(requestInfo.responseFramesCount);
      logger.debug("All Frame received. Time: {}ms. Request: {}", time, requestInfo);
    }
    ctx.fireChannelRead(frame);
  }
}

