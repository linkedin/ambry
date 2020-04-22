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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
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


/**
 * A {@link ChannelInitializer} to be used with {@link StorageServerNettyFactory}. Calling {@link #initChannel(SocketChannel)}
 * adds the necessary handlers to a channel's pipeline so that it may handle requests.
 */
public class StorageServerNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final NettyConfig nettyConfig;
  private final PerformanceConfig performanceConfig;
  private final Http2ClientConfig http2ClientConfig;
  private final NettyMetrics nettyMetrics;
  private final ConnectionStatsHandler connectionStatsHandler;
  private final RestRequestHandler requestHandler;
  private final SSLFactory sslFactory;

  /**
   * Construct a {@link StorageServerNettyChannelInitializer}.
   * @param nettyConfig the config to use when instantiating certain handlers on this pipeline.
   * @param http2ClientConfig configs that http2 client used.
   * @param performanceConfig the config to use when evaluating ambry service level objectives that include latency.
   * @param nettyMetrics the {@link NettyMetrics} object to use.
   * @param connectionStatsHandler the {@link ConnectionStatsHandler} to use.
   * @param requestHandler the {@link RestRequestHandler} to handle requests on this pipeline.
   * @param sslFactory the {@link SSLFactory} to use for generating {@link javax.net.ssl.SSLEngine} instances,
   *                   or {@code null} if SSL is not enabled in this pipeline.
   */
  public StorageServerNettyChannelInitializer(NettyConfig nettyConfig, Http2ClientConfig http2ClientConfig,
      PerformanceConfig performanceConfig, NettyMetrics nettyMetrics, ConnectionStatsHandler connectionStatsHandler,
      RestRequestHandler requestHandler, SSLFactory sslFactory, MetricRegistry metricRegistry) {
    this.nettyConfig = nettyConfig;
    this.performanceConfig = performanceConfig;
    this.http2ClientConfig = http2ClientConfig;
    this.nettyMetrics = nettyMetrics;
    // For http2, SSL encrypted is required. sslFactory should not be null.
    Objects.requireNonNull(sslFactory);
    this.sslFactory = sslFactory;
    this.connectionStatsHandler = connectionStatsHandler;
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    this.requestHandler = requestHandler;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    // To honor http2 window size, WriteBufferWaterMark.high() should be greater or equal to http2 window size.
    // Also see: https://github.com/netty/netty/issues/10193
    // https://stackoverflow.com/questions/25281124/netty-4-high-and-low-write-watermarks
    ch.config()
        .setSendBufferSize(http2ClientConfig.nettySendBufferSize)
        .setReceiveBufferSize(http2ClientConfig.nettyReceiveBufferSize)
        .setWriteBufferWaterMark(new WriteBufferWaterMark(http2ClientConfig.http2InitialWindowSize,
            2 * http2ClientConfig.http2InitialWindowSize));
    // If channel handler implementations are not annotated with @Sharable, Netty creates a new instance of every class
    // in the pipeline for every connection.
    // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
    ChannelPipeline pipeline = ch.pipeline();
    // connection stats handler to track connection related metrics
    pipeline.addLast("connectionStatsHandler", connectionStatsHandler);
    InetSocketAddress peerAddress = ch.remoteAddress();
    String peerHost = peerAddress.getHostName();
    int peerPort = peerAddress.getPort();
    SslHandler sslHandler = new SslHandler(sslFactory.createSSLEngine(peerHost, peerPort, SSLFactory.Mode.SERVER));
    pipeline.addLast("SslHandler", sslHandler);
    pipeline.addLast(Http2FrameCodecBuilder.forServer()
        .initialSettings(Http2Settings.defaultSettings()
            .maxFrameSize(http2ClientConfig.http2FrameMaxSize)
            .initialWindowSize(http2ClientConfig.http2InitialWindowSize))
        .frameLogger(new Http2FrameLogger(LogLevel.DEBUG, "server"))
        .build())
        .addLast("Http2MultiplexHandler", new Http2MultiplexHandler(
            new Http2StreamHandler(nettyMetrics, nettyConfig, performanceConfig, http2ClientConfig, requestHandler)));
  }
}

