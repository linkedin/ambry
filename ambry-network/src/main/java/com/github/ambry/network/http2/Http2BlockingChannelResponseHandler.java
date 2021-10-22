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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.FullHttpResponse;
import java.util.concurrent.CompletableFuture;


/**
 * Netty handler for {@link Http2BlockingChannel} to handle {@link FullHttpResponse}.
 */
@ChannelHandler.Sharable
public class Http2BlockingChannelResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
    CompletableFuture<ByteBuf> promise = ctx.channel().attr(Http2BlockingChannel.RESPONSE_PROMISE).getAndSet(null);
    if (promise != null) {
      promise.complete(msg.content().retainedDuplicate());
      // Stream channel can't be reused. Release it here.
      releaseStreamChannel(ctx);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    CompletableFuture<ByteBuf> promise = ctx.channel().attr(Http2BlockingChannel.RESPONSE_PROMISE).getAndSet(null);
    if (promise != null) {
      promise.completeExceptionally(cause);
      releaseStreamChannel(ctx);
    }
  }

  private void releaseStreamChannel(ChannelHandlerContext ctx) {
    ChannelPool pool = ctx.channel().attr(Http2BlockingChannel.CHANNEL_POOL_ATTRIBUTE_KEY).get();
    if (pool != null) {
      pool.release(ctx.channel());
    } else {
      // We don't have a pool in the attribute, just release it by using pool in parent channel.
      ctx.channel()
          .parent()
          .attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL)
          .get()
          .release(ctx.channel());
    }
  }
}
