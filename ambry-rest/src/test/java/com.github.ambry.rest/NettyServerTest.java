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
import com.github.ambry.network.MockSSLFactory;
import com.github.ambry.network.SSLFactory;
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
    for (int i = 0; i < 2; i++) {
      Properties properties = new Properties();
      // Should be > 0. So will throw at start().
      properties.setProperty(i == 0 ? "netty.server.port" : "netty.server.ssl.port", "-1");
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
    final NettyMetrics nettyMetrics = new NettyMetrics(new MetricRegistry());
    final RestRequestHandler requestHandler = new MockRestRequestResponseHandler();
    final PublicAccessLogger publicAccessLogger = new PublicAccessLogger(new String[]{}, new String[]{});
    final RestServerState restServerState = new RestServerState("/healthCheck");
    final ConnectionStatsHandler connectionStatsHandler = new ConnectionStatsHandler(nettyMetrics);
    final SSLFactory sslFactory = new MockSSLFactory(verifiableProperties);

    Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers = new HashMap<>();
    channelInitializers.put(nettyConfig.nettyServerPort,
        new NettyServerChannelInitializer(nettyConfig, nettyMetrics, connectionStatsHandler, requestHandler,
            publicAccessLogger, restServerState, null));
    channelInitializers.put(nettyConfig.nettyServerSSLPort,
        new NettyServerChannelInitializer(nettyConfig, nettyMetrics, connectionStatsHandler, requestHandler,
            publicAccessLogger, restServerState, sslFactory));
    return new NettyServer(nettyConfig, nettyMetrics, channelInitializers);
  }
}
