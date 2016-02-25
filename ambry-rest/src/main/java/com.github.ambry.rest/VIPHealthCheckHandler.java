package com.github.ambry.rest;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsbile responding to VIP health check requests
 * {@link VIPHealthCheckService} assists in knowing the state of the system at any point in time
 */
public class VIPHealthCheckHandler extends ChannelDuplexHandler {
  private final String healthCheckUri;
  private final VIPHealthCheckService healthCheckService;
  private final byte[] goodBytes = "GOOD".getBytes();
  private final byte[] badBytes = "BAD".getBytes();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public VIPHealthCheckHandler(VIPHealthCheckService healthCheckService) {
    this.healthCheckService = healthCheckService;
    this.healthCheckUri = healthCheckService.getHealthCheckUri();
    logger.info("Created VIPRequestHandler for healthCheckUri=" + healthCheckUri);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object obj)
      throws Exception {
    logger.trace("Reading on channel {}", ctx.channel());
    if (obj instanceof HttpRequest && ((HttpRequest) obj).getUri().equals(healthCheckUri)) {
      logger.trace("Handling VIP health check request while in state " + healthCheckService.isServiceUp());
      // Build the GOOD / BAD response
      HttpRequest request = (HttpRequest) obj;
      FullHttpResponse response;
      if (healthCheckService.isServiceUp()) {
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
            Unpooled.wrappedBuffer(goodBytes));
        HttpHeaders.setKeepAlive(response, true);
        HttpHeaders.setContentLength(response, goodBytes.length);
      } else {
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE,
            Unpooled.wrappedBuffer(badBytes));
        HttpHeaders.setKeepAlive(response, true);
        HttpHeaders.setContentLength(response, badBytes.length);
      }
      ctx.writeAndFlush(response);
    } else {
      super.channelRead(ctx, obj);
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (!healthCheckService.isServiceUp()) {
      if (msg instanceof LastHttpContent) {
        // Start closing client channels after we've completed writing to them (even if they are keep-alive)
        logger.info("VIP request handler closing connection " + ctx.channel() + " since in shutdown mode.");
        promise.addListener(ChannelFutureListener.CLOSE);
      }
    }
    super.write(ctx, msg, promise);
  }
}