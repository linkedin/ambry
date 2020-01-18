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
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.InetSocketAddress;


/**
 * A {@link ChannelInitializer} to be used with {@link StorageServerNettyFactory}. Calling {@link #initChannel(SocketChannel)}
 * adds the necessary handlers to a channel's pipeline so that it may handle requests.
 */
public class StorageServerNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final NettyConfig nettyConfig;
  private final PerformanceConfig performanceConfig;
  private final NettyMetrics nettyMetrics;
  private final ConnectionStatsHandler connectionStatsHandler;
  private final RestRequestHandler requestHandler;
  private final RestServerState restServerState;
  private final SSLFactory sslFactory;

  /**
   * Construct a {@link StorageServerNettyChannelInitializer}.
   * @param nettyConfig the config to use when instantiating certain handlers on this pipeline.
   * @param performanceConfig the config to use when evaluating ambry service level objectives that include latency.
   * @param nettyMetrics the {@link NettyMetrics} object to use.
   * @param connectionStatsHandler the {@link ConnectionStatsHandler} to use.
   * @param requestHandler the {@link RestRequestHandler} to handle requests on this pipeline.
   * @param restServerState the {@link RestServerState} object to use.
   * @param sslFactory the {@link SSLFactory} to use for generating {@link javax.net.ssl.SSLEngine} instances,
   *                   or {@code null} if SSL is not enabled in this pipeline.
   */
  public StorageServerNettyChannelInitializer(NettyConfig nettyConfig, PerformanceConfig performanceConfig,
      NettyMetrics nettyMetrics, ConnectionStatsHandler connectionStatsHandler, RestRequestHandler requestHandler,
      RestServerState restServerState, SSLFactory sslFactory, MetricRegistry metricRegistry) {
    this.nettyConfig = nettyConfig;
    this.performanceConfig = performanceConfig;
    this.nettyMetrics = nettyMetrics;
    this.connectionStatsHandler = connectionStatsHandler;
    this.restServerState = restServerState;
    this.sslFactory = sslFactory;
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    this.requestHandler = requestHandler;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    // If channel handler implementations are not annotated with @Sharable, Netty creates a new instance of every class
    // in the pipeline for every connection.
    // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
    ChannelPipeline pipeline = ch.pipeline();
    // connection stats handler to track connection related metrics
    // if SSL is enabled, add an SslHandler before the HTTP codec
    if (sslFactory != null) {
      InetSocketAddress peerAddress = ch.remoteAddress();
      String peerHost = peerAddress.getHostName();
      int peerPort = peerAddress.getPort();
      SslHandler sslHandler = new SslHandler(sslFactory.createSSLEngine(peerHost, peerPort, SSLFactory.Mode.SERVER));
      pipeline.addLast("SslHandler", sslHandler);
    }
    pipeline.addLast(Http2FrameCodecBuilder.forServer().build())
        .addLast("IdleStateHandler", new IdleStateHandler(0, 0, nettyConfig.nettyServerIdleTimeSeconds))
        .addLast("Http2MultiplexHandler", new Http2MultiplexHandler(
            new Http2StreamHandler(nettyMetrics, nettyConfig, performanceConfig, requestHandler)));
  }
}

