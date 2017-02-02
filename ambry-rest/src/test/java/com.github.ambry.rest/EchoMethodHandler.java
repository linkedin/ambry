/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler to echo the Method type of the request.
 * Used purely for testing purposes
 */
public class EchoMethodHandler extends SimpleChannelInboundHandler<HttpObject> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  public static final String IS_CHUNKED = "is_chunked_header";
  public static final String DISCONNECT_URI = "disconnect";
  public static final String CLOSE_URI = "close";
  private FullHttpResponse response;
  private HttpResponse httpResponse;
  private List<HttpContent> httpContentList;
  private String requestUri;
  public static final String RESPONSE_HEADER_KEY_PREFIX = "response_Header_Key";
  public static final String RESPONSE_HEADER_KEY_1 = RESPONSE_HEADER_KEY_PREFIX + "_1";
  public static final String RESPONSE_HEADER_KEY_2 = RESPONSE_HEADER_KEY_PREFIX + "_2";

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj) throws Exception {
    logger.trace("Reading on channel {}", ctx.channel());
    if (obj instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) obj;
      logger.trace("Handling incoming request " + request);
      requestUri = request.uri();
      byte[] methodBytes = request.method().toString().getBytes();
      if (request.headers().get(IS_CHUNKED) == null || !request.headers().get(IS_CHUNKED).equals("true")) {
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
            Unpooled.wrappedBuffer(methodBytes));
        updateHeaders(response, request, methodBytes.length);
      } else {
        httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpUtil.setTransferEncodingChunked(httpResponse, true);
        httpContentList = new ArrayList<>();
        ByteBuf content = Unpooled.wrappedBuffer(methodBytes);
        HttpContent httpContent = new DefaultHttpContent(content);
        httpContentList.add(httpContent);
        httpContentList.add(httpContent);
        updateHeaders(httpResponse, request, methodBytes.length);
      }
    } else if (obj instanceof LastHttpContent) {
      if (requestUri.equals(DISCONNECT_URI)) {
        ctx.disconnect();
      } else if (requestUri.equals(CLOSE_URI)) {
        ctx.close();
      } else {
        if (response != null) {
          ctx.writeAndFlush(response);
        } else if (httpResponse != null) {
          ctx.writeAndFlush(httpResponse);
          for (HttpContent httpContent : httpContentList) {
            ctx.writeAndFlush(httpContent);
          }
          ctx.writeAndFlush(new DefaultLastHttpContent());
        }
      }
    }
  }

  private void updateHeaders(HttpResponse response, HttpRequest request, int contentLength) {
    HttpUtil.setContentLength(response, contentLength);
    if (request.headers().get(RESPONSE_HEADER_KEY_1) != null) {
      response.headers().set(RESPONSE_HEADER_KEY_1, request.headers().get(RESPONSE_HEADER_KEY_1));
    }
    if (request.headers().get(RESPONSE_HEADER_KEY_2) != null) {
      response.headers().set(RESPONSE_HEADER_KEY_2, request.headers().get(RESPONSE_HEADER_KEY_2));
    }
  }
}
