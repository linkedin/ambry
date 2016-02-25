package com.github.ambry.rest;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EchoMethodHandler extends SimpleChannelInboundHandler<HttpObject> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private FullHttpResponse response;

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws Exception {
    logger.trace("Reading on channel {}", ctx.channel());
    if (obj instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) obj;
      logger.trace("Handling incoming request " + request);
      HttpResponseStatus httpResponseStatus = HttpResponseStatus.OK;
      if (request.getMethod() == HttpMethod.POST) {
        httpResponseStatus = HttpResponseStatus.CREATED;
      } else if (request.getMethod() == HttpMethod.DELETE) {
        httpResponseStatus = HttpResponseStatus.ACCEPTED;
      }
      byte[] methodBytes = request.getMethod().toString().getBytes();
      response =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpResponseStatus, Unpooled.wrappedBuffer(methodBytes));
      HttpHeaders.setContentLength(response, methodBytes.length);
      if (request.getMethod() == HttpMethod.POST) {
        HttpHeaders
            .setHeader(response, RestUtils.Headers.LOCATION, request.headers().get(RestUtils.Headers.LOCATION));
        HttpHeaders
            .setHeader(response, RestUtils.Headers.BLOB_SIZE, request.headers().get(RestUtils.Headers.BLOB_SIZE));
      }
    } else if (obj instanceof LastHttpContent) {
      ctx.writeAndFlush(response);
    }
  }
}
