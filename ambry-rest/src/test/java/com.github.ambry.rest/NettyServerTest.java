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
import java.io.IOException;
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
    Properties properties = new Properties();
    // Should be int. So will throw at instantiation.
    properties.setProperty("netty.server.port", "abcd");
    NioServer nioServer = null;
    try {
      nioServer = getNettyServer(properties);
      fail("NettyServer instantiation should have failed because of bad nettyServerPort value");
    } catch (NumberFormatException e) {
      // nothing to do. expected.
    } finally {
      if (nioServer != null) {
        nioServer.shutdown();
      }
    }

    // Should be > 0. So will throw at start().
    properties.setProperty("netty.server.port", "-1");
    nioServer = getNettyServer(properties);
    try {
      nioServer.start();
      fail("NettyServer start() should have failed because of bad nettyServerPort value");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    } finally {
      if (nioServer != null) {
        nioServer.shutdown();
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
    return new NettyServer(nettyConfig, nettyMetrics, new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        ch.pipeline()
            // connection stats handler to track connection related metrics
            .addLast("ConnectionStatsHandler", connectionStatsHandler)
            // for http encoding/decoding. Note that we get content in 8KB chunks and a change to that number has
            // to go here.
            .addLast("codec", new HttpServerCodec())
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
    });
  }
}
