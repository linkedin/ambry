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
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NettyServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class FrontendNettyFactory implements NioServerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrontendNettyFactory.class);

  private final NettyConfig nettyConfig;
  private final PerformanceConfig performanceConfig;
  private final NettyMetrics nettyMetrics;
  final Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers;

  /**
   * Creates a new instance of FrontendNettyFactory.
   * @param verifiableProperties the in-memory {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param requestHandler the {@link RestRequestHandler} to hand off the requests to.
   * @param publicAccessLogger the {@link PublicAccessLogger} that can be used for public access logging
   * @param restServerState the {@link RestServerState} that can be used to check the health of the system
   *                              to respond to health check requests
   * @param defaultSslFactory the {@link SSLFactory} used to construct the {@link javax.net.ssl.SSLEngine} used for
   *                          handling SSL requests (unless {@link NettyConfig#SSL_FACTORY_KEY} is set, in which case
   *                          it will be overridden).
   * @throws IllegalArgumentException if any of the arguments are null.
   * @throws ReflectiveOperationException if a netty-specific {@link SSLFactory} cannot be instantiated via reflection.
   */
  public FrontendNettyFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      final RestRequestHandler requestHandler, final PublicAccessLogger publicAccessLogger,
      final RestServerState restServerState, SSLFactory defaultSslFactory) throws ReflectiveOperationException {
    if (verifiableProperties == null || metricRegistry == null || requestHandler == null || publicAccessLogger == null
        || restServerState == null) {
      throw new IllegalArgumentException("Null arg(s) received during instantiation of FrontendNettyFactory");
    }
    nettyConfig = new NettyConfig(verifiableProperties);
    performanceConfig = new PerformanceConfig(verifiableProperties);
    nettyMetrics = new NettyMetrics(metricRegistry);
    ConnectionStatsHandler connectionStatsHandler = new ConnectionStatsHandler(nettyMetrics);

    Map<Integer, ChannelInitializer<SocketChannel>> initializers = new HashMap<>();
    initializers.put(nettyConfig.nettyServerPort,
        new FrontendNettyChannelInitializer(nettyConfig, performanceConfig, nettyMetrics, connectionStatsHandler,
            requestHandler, publicAccessLogger, restServerState, null));
    if (nettyConfig.nettyServerEnableSSL) {
      SSLFactory sslFactoryToUse;
      if (nettyConfig.nettyServerSslFactory.isEmpty()) {
        sslFactoryToUse = defaultSslFactory;
      } else {
        LOGGER.info("Using " + nettyConfig.nettyServerSslFactory + " for Netty SSL instead of the shared instance.");
        sslFactoryToUse = Utils.getObj(nettyConfig.nettyServerSslFactory, new SSLConfig(verifiableProperties));
      }
      if (sslFactoryToUse == null) {
        throw new IllegalArgumentException("NettyServer requires SSL, but sslFactory is null");
      }
      initializers.put(nettyConfig.nettyServerSSLPort,
          new FrontendNettyChannelInitializer(nettyConfig, performanceConfig, nettyMetrics, connectionStatsHandler,
              requestHandler, publicAccessLogger, restServerState, sslFactoryToUse));
    }
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
