package com.github.ambry.rest;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler to echo the Method type of the request.
 * Used purely for testing purposes
 */
public class EchoMethodHandler extends SimpleChannelInboundHandler<HttpObject> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final static String DISCONNECT_URI = "disconnect";
  private final static String CLOSE_URI = "close";
  private FullHttpResponse response;
  private String requestUri;
  public static String responseHeaderKey1 = "responseHeaderKey1";
  public static String responseHeaderKey2 = "responseHeaderKey2";

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject obj)
      throws Exception {
    logger.trace("Reading on channel {}", ctx.channel());
    if (obj instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) obj;
      logger.trace("Handling incoming request " + request);
      requestUri = request.getUri();
      byte[] methodBytes = request.getMethod().toString().getBytes();
      response =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(methodBytes));
      HttpHeaders.setContentLength(response, methodBytes.length);
      if(HttpHeaders.getHeader(request, responseHeaderKey1) != null){
        HttpHeaders.setHeader(response, responseHeaderKey1, HttpHeaders.getHeader(request, responseHeaderKey1));
      }
      if(HttpHeaders.getHeader(request, responseHeaderKey2) != null){
        HttpHeaders.setHeader(response, responseHeaderKey2, HttpHeaders.getHeader(request, responseHeaderKey2));
      }
    } else if (obj instanceof LastHttpContent) {
      if (requestUri.equals(DISCONNECT_URI)) {
        ctx.disconnect();
      } else if (requestUri.equals(CLOSE_URI)) {
        ctx.close();
      } else {
        ctx.writeAndFlush(response);
      }
    }
  }
}
