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

import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.server.ServerSecurityService;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import java.util.Objects;
import javax.net.ssl.SSLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler that performs any security validation on the new HTTP2 connection.
 */
@ChannelHandler.Sharable
public class ServerSecurityHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(ServerSecurityHandler.class);
  private final ServerSecurityService serverSecurityService;
  private final ServerMetrics serverMetrics;

  public ServerSecurityHandler(ServerSecurityService serverSecurityService, ServerMetrics serverMetrics) {
    this.serverSecurityService =
        Objects.requireNonNull(serverSecurityService, "server security service can not be null");
    this.serverMetrics = Objects.requireNonNull(serverMetrics, "serverMetrics can not be null");
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    logger.trace("Channel Active " + ctx.channel().remoteAddress());
    validateSslConnection(ctx);
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.trace("Channel Inactive " + ctx.channel().remoteAddress());
    super.channelInactive(ctx);
  }

  /**
   * Listen to the handshake future for the SSL termination, then apply the security policies, potentialy closing the
   * connection in case security check fails.
   * @param ctx the {@link ChannelHandlerContext}.
   */
  private void validateSslConnection(ChannelHandlerContext ctx) throws Exception {
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    if (sslHandler != null) {
      sslHandler.handshakeFuture().addListener(future -> {
        if (!future.isSuccess()) {
          logger.error("SSL handshake failed for channel: {}", ctx.channel(), future.cause());
        } else {
          logger.debug("SSL handshake succedded for channel: {}", ctx.channel(), future.cause());
          long startTimeMs = System.currentTimeMillis();
          try {
            SSLSession sslSession = sslHandler.engine().getSession();
            serverSecurityService.validateConnection(sslSession, (r, e) -> {
              if (e != null) {
                logger.error("security validation failed for channel: {}", ctx.channel(), e);
                serverMetrics.serverValidateConnectionFailure.inc();
                ctx.channel().close();
              } else {
                logger.debug("security validation succeeded for channel: {}", ctx.channel());
                serverMetrics.serverValidateConnectionSuccess.inc();
              }
            });
          } catch (Exception e) {
            logger.error("security validation failed for channel: {}", ctx.channel(), e);
            serverMetrics.serverValidateConnectionFailure.inc();
            ctx.channel().close();
          } finally {
            serverMetrics.securityServiceValidateConnectionTimeInMs.update(System.currentTimeMillis() - startTimeMs);
          }
        }
      });
    }
  }
}
