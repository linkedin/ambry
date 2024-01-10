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

import com.github.ambry.network.BoundedNettyByteBufReceive;
import com.github.ambry.network.NettyServerRequest;
import com.github.ambry.network.NetworkRequest;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.network.SocketServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
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

  public AmbryNetworkRequestHandler(RequestResponseChannel requestResponseChannel,
      Http2ServerMetrics http2ServerMetrics) {
    this.requestResponseChannel = requestResponseChannel;
    this.http2ServerMetrics = http2ServerMetrics;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
    ByteBuf dup = msg.content().retainedDuplicate();
    NettyServerRequest networkRequest = new NettyServerRequest(ctx, dup);
    try {
      discardHeaderBytes(networkRequest);
      requestResponseChannel.sendRequest(networkRequest);
    } catch (InterruptedException e) {
      dup.release();
      http2ServerMetrics.requestResponseChannelErrorCount.inc();
      logger.warn("Can't send NettyServerRequest to requestResponseChannel. Cause: ", e);
    } catch (IOException e) {
      logger.error("Encountered an error while reading length out of request {}, close the connection {}",
          networkRequest, ctx.channel(), e);
      ctx.channel().close();
      networkRequest.release();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    http2ServerMetrics.http2StreamExceptionCount.inc();
    logger.warn("Exception caught in AmbryNetworkRequestHandler, cause: ", cause);
    ctx.fireExceptionCaught(cause);
  }

  /**
   * Consume the first 8 bytes of the input stream which represents the size of the request. This is added to the
   * request since legacy Ambry's {@link SocketServer} stack needs to know the number of the bytes to read for a request
   * (see {@link BoundedNettyByteBufReceive#readFrom(ReadableByteChannel)} method). This is not needed when using
   * Netty HTTP2 server stack. We can remove this once {@link SocketServer} stack is retired. For now, we are
   * discarding the bytes here so that rest of request handling in server is same.
   * @param request incoming request
   */
  private void discardHeaderBytes(NetworkRequest request) throws IOException {
    DataInputStream stream = new DataInputStream(request.getInputStream());
    stream.readLong();
  }
}
