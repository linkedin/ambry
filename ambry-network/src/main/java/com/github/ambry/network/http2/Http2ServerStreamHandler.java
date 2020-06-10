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

import com.github.ambry.config.Http2ClientConfig;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;


/**
 * Handler initializer for http2 stream channel.
 */
@ChannelHandler.Sharable
public class Http2ServerStreamHandler extends ChannelInboundHandlerAdapter {
  private final Http2StreamFrameToHttpObjectCodec http2StreamFrameToHttpObjectCodec;
  private final AmbryNetworkRequestHandler ambryNetworkRequestHandler;
  private final AmbrySendToHttp2Adaptor ambrySendToHttp2Adaptor;
  private final Http2ClientConfig http2ClientConfig;

  public Http2ServerStreamHandler(AmbryNetworkRequestHandler ambryNetworkRequestHandler,
      Http2StreamFrameToHttpObjectCodec http2StreamFrameToHttpObjectCodec,
      AmbrySendToHttp2Adaptor ambrySendToHttp2Adaptor, Http2ClientConfig http2ClientConfig) {
    this.http2StreamFrameToHttpObjectCodec = http2StreamFrameToHttpObjectCodec;
    this.ambryNetworkRequestHandler = ambryNetworkRequestHandler;
    this.ambrySendToHttp2Adaptor = ambrySendToHttp2Adaptor;
    this.http2ClientConfig = http2ClientConfig;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    ctx.pipeline().addLast(http2StreamFrameToHttpObjectCodec);
    ctx.pipeline().addLast(new HttpObjectAggregator(http2ClientConfig.http2MaxContentLength));
    ctx.pipeline().addLast(ambryNetworkRequestHandler);
    ctx.pipeline().addLast(ambrySendToHttp2Adaptor);
  }
}
