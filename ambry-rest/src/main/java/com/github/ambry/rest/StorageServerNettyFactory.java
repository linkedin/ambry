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
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.VerifiableProperties;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NettyServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class StorageServerNettyFactory implements NioServerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageServerNettyFactory.class);

  private final NettyConfig nettyConfig;
  private final PerformanceConfig performanceConfig;
  private final NettyMetrics nettyMetrics;
  final Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers;

  /**
   * Creates a new instance of StorageServerNettyFactory.
   * @param http2Port the port for HTTP2 request.
   * @param verifiableProperties the in-memory {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param requestHandler the {@link RestRequestHandler} to hand off the requests to.
   * @param sslFactory the {@link SSLFactory} used to construct the {@link javax.net.ssl.SSLEngine} used for
   *                          handling http2 requests.
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public StorageServerNettyFactory(int http2Port, VerifiableProperties verifiableProperties,
      MetricRegistry metricRegistry, final RestRequestHandler requestHandler, SSLFactory sslFactory) {
    if (verifiableProperties == null || metricRegistry == null || requestHandler == null || sslFactory == null) {
      throw new IllegalArgumentException("Null arg(s) received during instantiation of StorageServerNettyFactory");
    }
    nettyConfig = new NettyConfig(verifiableProperties);
    performanceConfig = new PerformanceConfig(verifiableProperties);
    nettyMetrics = new NettyMetrics(metricRegistry);
    Http2ClientConfig http2ClientConfig = new Http2ClientConfig(verifiableProperties);
    ConnectionStatsHandler connectionStatsHandler = new ConnectionStatsHandler(nettyMetrics);

    Map<Integer, ChannelInitializer<SocketChannel>> initializers = Collections.singletonMap(http2Port,
        new StorageServerNettyChannelInitializer(nettyConfig, http2ClientConfig, performanceConfig, nettyMetrics, connectionStatsHandler,
            requestHandler, sslFactory, metricRegistry));
    channelInitializers = Collections.unmodifiableMap(initializers);
  }

  /**
   * Returns a new instance of {@link NettyServer}.
   * @return a new instance of {@link NettyServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new NettyServer(nettyConfig, nettyMetrics, channelInitializers);
  }
}
