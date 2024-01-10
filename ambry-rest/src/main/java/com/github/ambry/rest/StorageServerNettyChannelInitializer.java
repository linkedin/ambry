/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.network.http2.Http2ServerStreamHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link ChannelInitializer} to be used with StorageServerNettyFactory. Calling {@link #initChannel(SocketChannel)}
 * adds the necessary handlers to a channel's pipeline so that it may handle requests.
 */
public class StorageServerNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger logger = LoggerFactory.getLogger(StorageServerNettyChannelInitializer.class);
  private final Http2ClientConfig http2ClientConfig;
  private final Http2ServerMetrics http2ServerMetrics;
  private final SSLFactory sslFactory;
  private final ConnectionStatsHandler connectionStatsHandler;
  private final ServerSecurityHandler serverSecurityHandler;
  private final Http2ServerStreamHandler http2ServerStreamHandler;

  /**
   * Construct a {@link StorageServerNettyChannelInitializer}.
   * @param http2ClientConfig configs that http2 client used.
   * @param http2ServerMetrics http2ServerMetrics
   * @param sslFactory the {@link SSLFactory} to use for generating {@link javax.net.ssl.SSLEngine} instances.
   * @param connectionStatsHandler handler to stats connections.
   * @param http2ServerStreamHandler handler initializer for http2 stream channel.
   * @param serverSecurityHandler security validation handler for new HTTP2 connection.
   */
  public StorageServerNettyChannelInitializer(Http2ClientConfig http2ClientConfig,
      Http2ServerMetrics http2ServerMetrics, SSLFactory sslFactory, ConnectionStatsHandler connectionStatsHandler,
      Http2ServerStreamHandler http2ServerStreamHandler, ServerSecurityHandler serverSecurityHandler) {
    this.http2ClientConfig = http2ClientConfig;
    this.http2ServerMetrics = http2ServerMetrics;
    // For http2, SSL encrypted is required. sslFactory should not be null.
    Objects.requireNonNull(sslFactory);
    this.sslFactory = sslFactory;
    this.connectionStatsHandler = connectionStatsHandler;
    this.http2ServerStreamHandler = http2ServerStreamHandler;
    this.serverSecurityHandler = serverSecurityHandler;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    // To honor http2 window size, WriteBufferWaterMark.high() should be greater or equal to http2 window size.
    // Also see: https://github.com/netty/netty/issues/10193
    // https://stackoverflow.com/questions/25281124/netty-4-high-and-low-write-watermarks
    ch.config()
        .setSendBufferSize(http2ClientConfig.nettySendBufferSize)
        .setReceiveBufferSize(http2ClientConfig.nettyReceiveBufferSize)
        .setWriteBufferWaterMark(new WriteBufferWaterMark(http2ClientConfig.http2InitialWindowSize / 2,
            http2ClientConfig.http2InitialWindowSize));
    // If channel handler implementations are not annotated with @Sharable, Netty creates a new instance of every class
    // in the pipeline for every connection.
    // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
    ChannelPipeline pipeline = ch.pipeline();
    // connection stats handler to track connection related metrics
    pipeline.addLast("ConnectionStatsHandler", connectionStatsHandler);
    InetSocketAddress peerAddress = ch.remoteAddress();
    String peerHost = peerAddress.getHostName();
    int peerPort = peerAddress.getPort();
    SslHandler sslHandler = new SslHandler(sslFactory.createSSLEngine(peerHost, peerPort, SSLFactory.Mode.SERVER));
    pipeline.addLast("SslHandler", sslHandler);
    pipeline.addLast("SecurityChecker", serverSecurityHandler);
    pipeline.addLast("Http2FrameCodec", Http2FrameCodecBuilder.forServer()
        .initialSettings(Http2Settings.defaultSettings()
            .maxFrameSize(http2ClientConfig.http2FrameMaxSize)
            .initialWindowSize(http2ClientConfig.http2InitialWindowSize))
        .frameLogger(new Http2FrameLogger(LogLevel.DEBUG, "server"))
        .build());
    pipeline.addLast("Http2MultiplexHandler", new Http2MultiplexHandler(http2ServerStreamHandler));
    pipeline.addLast("CloseOnExceptionHandler", new CloseOnExceptionHandler());
  }

  private final class CloseOnExceptionHandler extends ChannelInboundHandlerAdapter {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /**
     * Netty calls this function when channel becomes inactive. The channel becomes inactive AFTER it is closed (either by
     * the local or the remote end). One case is when server shutdown.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      logger.trace("Parent channel {} become inactive.", ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      // Close the channel would force netty to flush the remaining buffer back to client. If the flush operation
      // fails due to exceptions, when this method would be invoked again. And we would fall into an infinite loop
      // of stack trace. So use a boolean value to make sure we only do close once.
      if (closed.compareAndSet(false, true)) {
        http2ServerMetrics.http2ParentExceptionCount.inc();
        logger.warn("Parent channel {} exception: ", ctx.channel(), cause);
        ctx.channel().close();
      }
    }
  }
}

