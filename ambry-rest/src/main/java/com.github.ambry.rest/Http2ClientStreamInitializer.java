/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.rest;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;


/**
 * A {@link ChannelInitializer} to be used for http2 stream.
 */
public class Http2ClientStreamInitializer extends ChannelInitializer<Channel> {
  private Http2ResponseHandler http2ResponseHandler;

  public Http2ClientStreamInitializer(Http2ResponseHandler http2ResponseHandler) {
    this.http2ResponseHandler = http2ResponseHandler;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();
    p.addLast(new Http2StreamFrameToHttpObjectCodec(false));
    p.addLast(new HttpObjectAggregator(1024 * 1024));
    p.addLast(http2ResponseHandler);
  }

  public Http2ResponseHandler getHttp2ResponseHandler() {
    return http2ResponseHandler;
  }
}

