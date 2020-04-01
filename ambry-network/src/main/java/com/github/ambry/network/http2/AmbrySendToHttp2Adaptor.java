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

import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.network.Send;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.ByteBufChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpMethod;
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
public class AmbrySendToHttp2Adaptor extends ChannelOutboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(AmbrySendToHttp2Adaptor.class);

  public AmbrySendToHttp2Adaptor() {

  }

  /**
   * Handles conversion of {@link Send} to HTTP/2 frames.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (!(msg instanceof Send)) {
      ctx.write(msg, promise);
      return;
    }
    Send send = (Send) msg;
    Http2Headers http2Headers = new DefaultHttp2Headers().method(HttpMethod.POST.asciiName()).scheme("https").path("/");
    DefaultHttp2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(http2Headers, false);
    ctx.write(headersFrame);
    // TODO: Use {@link RetainingAsyncWritableChannel} after writeTo(AsyncWritableChannel channel, Callback<Long> callback) is fully implemented.
    ByteBufChannel byteBufChannel = new ByteBufChannel();
    try {
      send.writeTo(byteBufChannel);
    } catch (IOException e) {
      promise.setFailure(e);
      return;
    }
    DefaultHttp2DataFrame dataFrame = new DefaultHttp2DataFrame(byteBufChannel.getBuf(), true);
    ctx.write(dataFrame, promise);
  }
}
