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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.VerifiableProperties;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;


/**
 * Netty specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NettyServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class NettyServerFactory implements NioServerFactory {

  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  private final ChannelInitializer<SocketChannel> channelInitializer;

  /**
   * Creates a new instance of NettyServerFactory.
   * @param verifiableProperties the in-memory {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param requestHandler the {@link RestRequestHandler} to hand off the requests to.
   * @param publicAccessLogger the {@link PublicAccessLogger} that can be used for public access logging
   * @param restServerState the {@link RestServerState} that can be used to check the health of the system
   *                              to respond to health check requests
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public NettyServerFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      final RestRequestHandler requestHandler, final PublicAccessLogger publicAccessLogger,
      final RestServerState restServerState) {
    if (verifiableProperties == null || metricRegistry == null || requestHandler == null || publicAccessLogger == null
        || restServerState == null) {
      throw new IllegalArgumentException("Null arg(s) received during instantiation of NettyServerFactory");
    } else {
      nettyConfig = new NettyConfig(verifiableProperties);
      nettyMetrics = new NettyMetrics(metricRegistry);
      final ConnectionStatsHandler connectionStatsHandler = new ConnectionStatsHandler(nettyMetrics);
      channelInitializer = new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          ch.pipeline()
              // connection stats handler to track connection related metrics
              .addLast("connectionStatsHandler", connectionStatsHandler)
              // for http encoding/decoding. Note that we get content in 8KB chunks and a change to that number has
              // to go here.
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
              // custom processing class that interfaces with a BlobStorageService.
              .addLast("processor", new NettyMessageProcessor(nettyMetrics, nettyConfig, requestHandler));
        }
      };
    }
  }

  /**
   * Returns a new instance of {@link NettyServer}.
   * @return a new instance of {@link NettyServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new NettyServer(nettyConfig, nettyMetrics, channelInitializer);
  }
}
