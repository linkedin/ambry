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

import com.github.ambry.utils.BatchBlockingQueue;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Process {@link io.netty.handler.codec.http.FullHttpResponse} translated from HTTP/2 frames
 */
@ChannelHandler.Sharable
class Http2ClientResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
  private static final Logger logger = LoggerFactory.getLogger(Http2ClientResponseHandler.class);
  private static final ResponseInfo WAKEUP_MARKER = new ResponseInfo(null, null, null);
  private BatchBlockingQueue<ResponseInfo> responseInfoQueue = new BatchBlockingQueue<>(WAKEUP_MARKER);
  private Http2ClientMetrics http2ClientMetrics;

  public Http2ClientResponseHandler(Http2ClientMetrics http2ClientMetrics) {
    this.http2ClientMetrics = http2ClientMetrics;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {

    ByteBuf dup = msg.content().retainedDuplicate();
    // Consume length
    dup.readLong();
    RequestInfo requestInfo = ctx.channel().attr(Http2NetworkClient.REQUEST_INFO).get();
    http2ClientMetrics.http2StreamFirstToAllFrameReadyTime.update(
        System.currentTimeMillis() - requestInfo.getStreamHeaderFrameReceiveTime());
    ResponseInfo responseInfo = new ResponseInfo(requestInfo, null, dup);
    responseInfoQueue.put(responseInfo);
    releaseAndCloseStreamChannel(ctx);
  }

  /**
   * Get the queue of responses received by {@link Http2ClientResponseHandler#channelRead0}.
   * {@link Http2NetworkClient} consumes {@link ResponseInfo} from this queue.
   * from this queue.
   * @return the queue of responses received by this handler
   */
  public BatchBlockingQueue<ResponseInfo> getResponseInfoQueue() {
    return responseInfoQueue;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    http2ClientMetrics.http2StreamExceptionCount.inc();
    logger.info("Exception Caught from inbound.", cause.getMessage());
    releaseAndCloseStreamChannel(ctx);
  }

  private void releaseAndCloseStreamChannel(ChannelHandlerContext ctx) {
    logger.debug("Stream channel is being closed. Stream: {}, Parent: {}", ctx.channel(), ctx.channel().parent());
    ctx.channel()
        .parent()
        .attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL)
        .get()
        .release(ctx.channel());
  }
}
