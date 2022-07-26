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

import com.github.ambry.network.NetworkClientErrorCode;
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

import static com.github.ambry.network.http2.Http2Utils.*;


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
    RequestInfo requestInfo = releaseAndCloseStreamChannel(ctx.channel());
    if (requestInfo != null) {
      // A request maybe just dropped by Http2NetworkClient.
      http2ClientMetrics.http2StreamFirstToAllFrameReadyTime.update(
          System.currentTimeMillis() - requestInfo.getResponseHeaderReceiveTime());
      ResponseInfo responseInfo = new ResponseInfo(requestInfo, null, dup);
      responseInfoQueue.put(responseInfo);
    } else {
      logger.info("Failed to get request from attribute map on channel {}, request maybe dropped", ctx.channel());
      dup.release();
    }
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
    logger.info("Exception caught in Http2ClientResponseHandler {} {}. Closing stream channel. Cause: ",
        ctx.channel().hashCode(), ctx.channel(), cause);
    RequestInfo requestInfo = releaseAndCloseStreamChannel(ctx.channel());
    if (requestInfo != null) {
      responseInfoQueue.put(new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null));
      // Will be removed from
    }
  }
}
