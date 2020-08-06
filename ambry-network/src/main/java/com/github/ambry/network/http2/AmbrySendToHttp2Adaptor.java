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
import com.github.ambry.utils.ByteBufChannel;
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
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Translates Ambry {@link Send} to the HTTP/2 frame objects.
 */
@ChannelHandler.Sharable
public class AmbrySendToHttp2Adaptor extends ChannelOutboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(AmbrySendToHttp2Adaptor.class);
  private final boolean forServer;

  /**
   * @param forServer if true, the handler is used as server side outbound handler. Otherwise, it's use as client side
   *                  outbound handler.
   */
  public AmbrySendToHttp2Adaptor(boolean forServer) {
    this.forServer = forServer;
  }

  /**
   * Handles conversion of {@link Send} to HTTP/2 frames.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (!ctx.channel().isOpen()) {
      logger.debug("Channel closed when write. Channel: {}", ctx.channel());
      promise.setFailure(new ChannelException("Channel has been closed when write."));
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
    ByteBuf dataContent = send.content();
    if (dataContent == null) {
      ByteBufChannel byteBufChannel = new ByteBufChannel();
      try {
        send.writeTo(byteBufChannel);
        dataContent = byteBufChannel.getBuf();
      } catch (IOException e) {
        promise.setFailure(e);
        return;
      } finally {
        send.release();
      }
    }
    DefaultHttp2DataFrame dataFrame = new DefaultHttp2DataFrame(dataContent, true);
    // Caller should call writeAndFlush().
    ctx.write(dataFrame, promise);
  }
}
