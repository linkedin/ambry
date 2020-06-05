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
 *
 */
package com.github.ambry.network.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.concurrent.Promise;


/**
 * A ChannelInitializer used to setup stream channel pipeline once stream is created in {@link MultiplexedChannelRecord}.
 */
public class Http2BlockingChannelStreamChannelInitializer extends ChannelInitializer {

  private final int http2MaxContentLength;

  Http2BlockingChannelStreamChannelInitializer(int http2MaxContentLength) {
    this.http2MaxContentLength = http2MaxContentLength;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();
    p.addLast(new Http2StreamFrameToHttpObjectCodec(false));
    p.addLast(new HttpObjectAggregator(http2MaxContentLength));
    p.addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        Promise<ByteBuf> promise = ctx.channel().attr(Http2BlockingChannel.RESPONSE_PROMISE).get();
        if (promise != null) {
          promise.setSuccess(msg.content().retainedDuplicate());
          // Stream channel can't be reused. Release it here.
          releaseStreamChannel(ctx);
        }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Promise<ByteBuf> promise = ctx.channel().attr(Http2BlockingChannel.RESPONSE_PROMISE).getAndSet(null);
        if (promise != null) {
          promise.setFailure(cause);
          releaseStreamChannel(ctx);
        }
      }

      private void releaseStreamChannel(ChannelHandlerContext ctx) {
        ctx.channel()
            .parent()
            .attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL)
            .get()
            .release(ctx.channel());
      }
    });
    p.addLast(new AmbrySendToHttp2Adaptor());
  }
}
