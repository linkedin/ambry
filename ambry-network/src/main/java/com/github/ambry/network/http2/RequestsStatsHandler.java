/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A handler to track requests stats. For now, it only tracks in-flight request count.
 */

@ChannelHandler.Sharable
class RequestsStatsHandler extends ChannelDuplexHandler {
  private final AtomicLong inFlightRequests = new AtomicLong(0);

  public RequestsStatsHandler(Http2ClientMetrics http2Metrics) {
    http2Metrics.registerInFlightRequestCount(inFlightRequests);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
    inFlightRequests.decrementAndGet();
    super.channelRead(ctx, obj);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    inFlightRequests.incrementAndGet();
    super.write(ctx, msg, promise);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    inFlightRequests.decrementAndGet();
    super.exceptionCaught(ctx, cause);
  }

  /**
   * @return the number of in-flight requests.
   */
  public long getInFlightRequestCount() {
    return inFlightRequests.get();
  }
}

