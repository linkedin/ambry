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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;


/**
 * A ChannelInitializer used to setup stream channel pipeline once stream is created in {@link MultiplexedChannelRecord}.
 */
public class Http2BlockingChannelStreamChannelInitializer extends ChannelInitializer {
  private final int http2MaxContentLength;
  private static final Http2StreamFrameToHttpObjectCodec http2StreamFrameToHttpObjectCodec =
      new Http2StreamFrameToHttpObjectCodec(false);
  private static final Http2BlockingChannelResponseHandler http2BlockingChannelResponseHandler =
      new Http2BlockingChannelResponseHandler();
  private static final AmbrySendToHttp2Adaptor ambrySendToHttp2Adaptor = new AmbrySendToHttp2Adaptor(false);

  Http2BlockingChannelStreamChannelInitializer(int http2MaxContentLength) {
    this.http2MaxContentLength = http2MaxContentLength;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();
    p.addLast(http2StreamFrameToHttpObjectCodec);
    p.addLast(new HttpObjectAggregator(http2MaxContentLength));
    p.addLast(http2BlockingChannelResponseHandler);
    p.addLast(ambrySendToHttp2Adaptor);
  }
}
