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
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.NettyConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.InetSocketAddress;


/**
 * A {@link ChannelInitializer} to be used with {@link NettyServer}. Calling {@link #initChannel(SocketChannel)} adds
 * the necessary handlers to a channel's pipeline so that it may handle requests.
 */
public class FrontendNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final NettyConfig nettyConfig;
  private final PerformanceConfig performanceConfig;
  private final NettyMetrics nettyMetrics;
  private final ConnectionStatsHandler connectionStatsHandler;
  private final RestRequestHandler requestHandler;
  private final PublicAccessLogger publicAccessLogger;
  private final RestServerState restServerState;
  private final SSLFactory sslFactory;

  /**
   * Construct a {@link FrontendNettyChannelInitializer}.
   * @param nettyConfig the config to use when instantiating certain handlers on this pipeline.
   * @param performanceConfig the config to use when evaluating ambry service level objectives that include latency.
   * @param nettyMetrics the {@link NettyMetrics} object to use.
   * @param connectionStatsHandler the {@link ConnectionStatsHandler} to use.
   * @param requestHandler the {@link RestRequestHandler} to handle requests on this pipeline.
   * @param publicAccessLogger the {@link PublicAccessLogger} to use.
   * @param restServerState the {@link RestServerState} object to use.
   * @param sslFactory the {@link SSLFactory} to use for generating {@link javax.net.ssl.SSLEngine} instances,
   *                   or {@code null} if SSL is not enabled in this pipeline.
   */
  public FrontendNettyChannelInitializer(NettyConfig nettyConfig, PerformanceConfig performanceConfig, NettyMetrics nettyMetrics,
      ConnectionStatsHandler connectionStatsHandler, RestRequestHandler requestHandler,
      PublicAccessLogger publicAccessLogger, RestServerState restServerState, SSLFactory sslFactory) {
    this.nettyConfig = nettyConfig;
    this.performanceConfig = performanceConfig;
    this.nettyMetrics = nettyMetrics;
    this.connectionStatsHandler = connectionStatsHandler;
    this.requestHandler = requestHandler;
    this.publicAccessLogger = publicAccessLogger;
    this.restServerState = restServerState;
    this.sslFactory = sslFactory;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    // If channel handler implementations are not annotated with @Sharable, Netty creates a new instance of every class
    // in the pipeline for every connection.
    // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
    ChannelPipeline pipeline = ch.pipeline();
    // connection stats handler to track connection related metrics
    pipeline.addLast("connectionStatsHandler", connectionStatsHandler);
    // if SSL is enabled, add an SslHandler before the HTTP codec
    if (sslFactory != null) {
      InetSocketAddress peerAddress = ch.remoteAddress();
      String peerHost = peerAddress.getHostName();
      int peerPort = peerAddress.getPort();
      SslHandler sslHandler = new SslHandler(sslFactory.createSSLEngine(peerHost, peerPort, SSLFactory.Mode.SERVER));
      pipeline.addLast("sslHandler", sslHandler);
    }
    pipeline
        // for http encoding/decoding.
        .addLast("codec",
            new HttpServerCodec(nettyConfig.nettyServerMaxInitialLineLength, nettyConfig.nettyServerMaxHeaderSize,
                nettyConfig.nettyServerMaxChunkSize))
        // for health check request handling
        .addLast("healthCheckHandler", new HealthCheckHandler(restServerState, nettyMetrics))
        // for public access logging
        .addLast("publicAccessLogHandler", new PublicAccessLogHandler(publicAccessLogger, nettyMetrics))
        // for detecting connections that have been idle too long - probably because of an error.
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, nettyConfig.nettyServerIdleTimeSeconds))
        // for safe writing of chunks for responses
        .addLast("chunker", new ChunkedWriteHandler())
        // custom processing class that interfaces with a RestRequestService.
        .addLast("processor", new NettyMessageProcessor(nettyMetrics, nettyConfig, performanceConfig, requestHandler));
  }
}
