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
import com.github.ambry.account.MockRouter;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.VerifiableProperties;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests basic functionality of {@link NettyServer}.
 */
public class NettyServerTest {
  private static final NettyMetrics NETTY_METRICS = new NettyMetrics(new MetricRegistry());
  private static final RestRequestService REST_REQUEST_SERVICE =
      new MockRestRequestService(new VerifiableProperties(new Properties()), new MockRouter());
  private static final RestRequestHandler REQUEST_HANDLER = new MockRestRequestResponseHandler(REST_REQUEST_SERVICE);
  private static final PublicAccessLogger PUBLIC_ACCESS_LOGGER = new PublicAccessLogger(new String[]{}, new String[]{});
  private static final RestServerState REST_SERVER_STATE = new RestServerState("/healthCheck");
  private static final ConnectionStatsHandler CONNECTION_STATS_HANDLER = new ConnectionStatsHandler(NETTY_METRICS);
  private static final SSLFactory SSL_FACTORY = RestTestUtils.getTestSSLFactory();

  /**
   * Tests {@link NettyServer#start()} and {@link NettyServer#shutdown()} given good input.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutdownTest() throws InstantiationException, IOException {
    NioServer nioServer = getNettyServer(null);
    nioServer.start();
    nioServer.shutdown();
  }

  /**
   * Tests for {@link NettyServer#shutdown()} when {@link NettyServer#start()} has not been called previously.
   * This test is for cases where {@link NettyServer#start()} has failed and {@link NettyServer#shutdown()} needs to be
   * run.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest() throws InstantiationException, IOException {
    NioServer nioServer = getNettyServer(null);
    nioServer.shutdown();
  }

  /**
   * Tests for correct exceptions are thrown on {@link NettyServer} instantiation/{@link NettyServer#start()} with bad
   * input.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startWithBadInputTest() throws InstantiationException, IOException {
    Properties properties = new Properties();
    // Should be > 0. So will throw at start().
    properties.setProperty("netty.server.port", "-1");
    doStartFailureTest(properties);

    properties = new Properties();
    // Should be > 0. So will throw at start().
    properties.setProperty("netty.server.ssl.port", "-1");
    doStartFailureTest(properties);
  }

  // helpers
  // general

  /**
   * Gets an instance of {@link NettyServer}.
   * @param properties the in-memory {@link Properties} to use.
   * @return an instance of {@link NettyServer}.
   * @throws InstantiationException
   * @throws IOException
   */
  private NettyServer getNettyServer(Properties properties) throws InstantiationException, IOException {
    if (properties == null) {
      // dud properties. should pick up defaults
      properties = new Properties();
    }
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    final NettyConfig nettyConfig = new NettyConfig(verifiableProperties);
    final PerformanceConfig performanceConfig = new PerformanceConfig(verifiableProperties);

    Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers = new HashMap<>();
    channelInitializers.put(nettyConfig.nettyServerPort,
        new FrontendNettyChannelInitializer(nettyConfig, performanceConfig, NETTY_METRICS, CONNECTION_STATS_HANDLER,
            REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER, REST_SERVER_STATE, null));
    channelInitializers.put(nettyConfig.nettyServerSSLPort,
        new FrontendNettyChannelInitializer(nettyConfig, performanceConfig, NETTY_METRICS, CONNECTION_STATS_HANDLER,
            REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER, REST_SERVER_STATE, SSL_FACTORY));
    return new NettyServer(nettyConfig, NETTY_METRICS, channelInitializers);
  }

  /**
   * Test that the {@link NettyServer} fails to start up with the given properties.
   * @param properties
   */
  private void doStartFailureTest(Properties properties) throws IOException, InstantiationException {
    NioServer nioServer = getNettyServer(properties);
    try {
      nioServer.start();
      fail("NettyServer start() should have failed because of bad port number value");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    } finally {
      if (nioServer != null) {
        nioServer.shutdown();
      }
    }
  }
}
