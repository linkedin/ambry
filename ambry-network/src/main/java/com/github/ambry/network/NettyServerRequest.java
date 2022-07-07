/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network;

import com.github.ambry.utils.AbstractByteBufHolder;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.InputStream;


/**
 * A wrapper class at the network layer for NettyRequest based RestRequest.
 */
public class NettyServerRequest extends AbstractByteBufHolder<NettyServerRequest> implements NetworkRequest {
  private final ChannelHandlerContext ctx;
  private final InputStream inputStream;
  private final long startTimeInMs;
  private final ByteBuf content;
  private final FullHttpRequest request;

  public NettyServerRequest(ChannelHandlerContext ctx, ByteBuf content) {
    this.ctx = ctx;
    this.content = content;
    this.inputStream = new NettyByteBufDataInputStream(content);
    this.request = null;
    this.startTimeInMs = System.currentTimeMillis();
  }

  public NettyServerRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
    this.ctx = ctx;
    this.content = request.content();
    this.inputStream = null;
    this.request = request;
    this.startTimeInMs = System.currentTimeMillis();
  }

  ChannelHandlerContext getCtx() {
    return ctx;
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  @Override
  public FullHttpRequest getHttpRequest() {
    return request;
  }

  @Override
  public boolean isRegularHttp() {
    return request != null;
  }

  @Override
  public ByteBuf content() {
    return content;
  }

  @Override
  public NettyServerRequest replace(ByteBuf content) {
    return new NettyServerRequest(ctx, content);
  }
}

