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

import com.github.ambry.network.NettyServerRequest;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.protocol.RequestAPI;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.DataInputStream;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Retain content of {@link FullHttpRequest} and send to {@link RequestResponseChannel}.
 */
@ChannelHandler.Sharable
public class AmbryNetworkRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger logger = LoggerFactory.getLogger(AmbryNetworkRequestHandler.class);
  private final RequestResponseChannel requestResponseChannel;
  private final Http2ServerMetrics http2ServerMetrics;
  private final RequestAPI requestAPI;

  public AmbryNetworkRequestHandler(RequestResponseChannel requestResponseChannel,
      Http2ServerMetrics http2ServerMetrics, RequestAPI requestAPI) {
    this.requestResponseChannel = requestResponseChannel;
    this.http2ServerMetrics = http2ServerMetrics;
    this.requestAPI = requestAPI;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
    ByteBuf dup = msg.content().retainedDuplicate();
    try {
      NettyServerRequest networkRequest = new NettyServerRequest(ctx, dup);
      boolean isRequestQueued = requestResponseChannel.sendRequest(networkRequest);
      // Reject the request if input queue is full.
      if (!isRequestQueued) {
        try {
          // 1. Consume the first 8 bytes of the input stream which represents the size of the request. This is added to the
          // request since Ambry's SocketServer stack needs to know the number of the bytes to read for a request
          // (see BoundedNettyByteBufReceive#readFrom(ReadableByteChannel) method). This is not needed when using
          // Netty HTTP2 server stack. We can remove this once SocketServer stack is retired. For now, we are
          // consuming the bytes here so that rest of request handling in server is same.
          DataInputStream stream = new DataInputStream(networkRequest.getInputStream());
          stream.readLong();
          // 2. Use RequestAPI class to reject the request. This method will update the metrics and send response immediately.
          requestAPI.dropRequest(networkRequest);
        } catch (IOException e) {
          logger.error("Encountered an error while reading length out of request {}, close the connection",
              networkRequest, e);
          ctx.channel().close();
        } finally {
          // 3. Release the memory held by this request.
          networkRequest.release();
        }
      }
    } catch (InterruptedException e) {
      dup.release();
      http2ServerMetrics.requestResponseChannelErrorCount.inc();
      logger.warn("Can't send NettyServerRequest to requestResponseChannel. Cause: ", e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    http2ServerMetrics.http2StreamExceptionCount.inc();
    logger.warn("Exception caught in AmbryNetworkRequestHandler, cause: ", cause);
    ctx.channel().close();
  }
}
