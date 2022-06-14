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

import com.github.ambry.network.Send;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Translates Ambry {@link Send} to the HTTP/2 frame objects.
 */
@ChannelHandler.Sharable
public class AmbrySendToHttp2Adaptor extends ChannelOutboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(AmbrySendToHttp2Adaptor.class);
  private final boolean forServer;
  private final int maxFrameSize;

  /**
   * @param forServer if true, the handler is used as server side outbound handler. Otherwise, it's use as client side
   *                  outbound handler.
   * @param maxFrameSize the max size of a http2 frame.
   */
  public AmbrySendToHttp2Adaptor(boolean forServer, int maxFrameSize) {
    this.forServer = forServer;
    this.maxFrameSize = maxFrameSize;
  }

  /**
   * Handles conversion of {@link Send} to HTTP/2 frames.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (!ctx.channel().isOpen()) {
      logger.debug("Channel closed when write. Channel: {}", ctx.channel());
      promise.setFailure(new ChannelException("Channel has been closed when write."));
      ReferenceCountUtil.release(msg);
      return;
    }
    if (!(msg instanceof Send)) {
      ctx.write(msg, promise);
      return;
    }
    Send send = (Send) msg;

    Http2Headers http2Headers;
    if (forServer) {
      logger.trace("Write content to channel as server {}", ctx.channel());
      http2Headers = new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText());
    } else {
      logger.trace("Write content to channel as client {}", ctx.channel());
      http2Headers = new DefaultHttp2Headers().method(HttpMethod.POST.asciiName()).scheme("https").path("/");
    }
    DefaultHttp2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(http2Headers, false);
    ctx.write(headersFrame);

    // Referencing counting for derived {@link ByteBuf}: https://netty.io/wiki/reference-counted-objects.html#derived-buffers
    try {
      while (send.content().isReadable(maxFrameSize)) {
        ByteBuf slice = send.content().readSlice(maxFrameSize);
        slice.retain();
        DefaultHttp2DataFrame dataFrame = new DefaultHttp2DataFrame(slice, false);
        ctx.write(dataFrame);
      }
      // The last slice
      ByteBuf slice = send.content().readSlice(send.content().readableBytes());
      slice.retain();
      DefaultHttp2DataFrame dataFrame = new DefaultHttp2DataFrame(slice, true);
      ctx.write(dataFrame, promise);
    } catch (Exception e) {
      logger.error("Error while processing frames. Channel: {}", ctx.channel(), e);
    } finally {
      send.content().release();
    }
  }
}
