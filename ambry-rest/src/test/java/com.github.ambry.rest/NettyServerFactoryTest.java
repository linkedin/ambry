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
import com.github.ambry.config.VerifiableProperties;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyServerFactory}.
 */
public class NettyServerFactoryTest {
  // dud properties. server should pick up defaults
  private static final RestRequestHandler REST_REQUEST_HANDLER = new MockRestRequestResponseHandler();
  private static final PublicAccessLogger PUBLIC_ACCESS_LOGGER = new PublicAccessLogger(new String[]{}, new String[]{});
  private static final RestServerState REST_SERVER_STATE = new RestServerState("/healthCheck");
  private static final SSLFactory SSL_FACTORY = RestTestUtils.getTestSSLFactory();

  /**
   * Checks to see that getting the default {@link NioServer} (currently {@link NettyServer}) works.
   */
  @Test
  public void getNettyServerTest() throws Exception {
    Properties properties = new Properties();
    doGetNettyServerTest(properties, SSL_FACTORY);
    doGetNettyServerTest(properties, null);
    properties.setProperty("netty.server.enable.ssl", "true");
    doGetNettyServerTest(properties, SSL_FACTORY);
  }

  /**
   * Test that a {@link NettyServer} can be constructed by the factory.
   * @param properties the {@link Properties} to use.
   * @param sslFactory the {@link SSLFactory} to use.
   */
  private void doGetNettyServerTest(Properties properties, SSLFactory sslFactory) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    NettyConfig nettyConfig = new NettyConfig(verifiableProperties);
    NettyServerFactory nettyServerFactory =
        new NettyServerFactory(verifiableProperties, new MetricRegistry(), REST_REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER,
            REST_SERVER_STATE, sslFactory);
    NioServer nioServer = nettyServerFactory.getNioServer();
    assertNotNull("No NioServer returned", nioServer);
    assertEquals("Did not receive a NettyServer instance", NettyServer.class.getCanonicalName(),
        nioServer.getClass().getCanonicalName());
    Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers = nettyServerFactory.channelInitializers;
    if (nettyConfig.nettyServerEnableSSL && sslFactory != null) {
      assertEquals("Expected two ChannelInitializers when SSLFactory is not null", 2, channelInitializers.size());
      assertNotNull("No ChannelInitializer for SSL port", channelInitializers.get(nettyConfig.nettyServerSSLPort));
    } else {
      assertEquals("Expected one ChannelInitializer when SSLFactory is null", 1, channelInitializers.size());
    }
    assertNotNull("No ChannelInitializer for plaintext port", channelInitializers.get(nettyConfig.nettyServerPort));
  }

  /**
   * Tests instantiation of {@link NettyServerFactory} with bad input.
   */
  @Test
  public void getNettyServerFactoryWithBadInputTest() {
    Properties properties = new Properties();
    properties.setProperty("netty.server.enable.ssl", "true");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    doConstructionFailureTest(null, metricRegistry, REST_REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER, REST_SERVER_STATE,
        SSL_FACTORY);
    doConstructionFailureTest(verifiableProperties, null, REST_REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER, REST_SERVER_STATE,
        SSL_FACTORY);
    doConstructionFailureTest(verifiableProperties, metricRegistry, null, PUBLIC_ACCESS_LOGGER, REST_SERVER_STATE,
        SSL_FACTORY);
    doConstructionFailureTest(verifiableProperties, metricRegistry, REST_REQUEST_HANDLER, null, REST_SERVER_STATE,
        SSL_FACTORY);
    doConstructionFailureTest(verifiableProperties, metricRegistry, REST_REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER, null,
        SSL_FACTORY);
    doConstructionFailureTest(verifiableProperties, metricRegistry, REST_REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER,
        REST_SERVER_STATE, null);
  }

  /**
   * Test that {@link NettyServerFactory} construction fails with the given constructor inputs.
   * @param verifiableProperties the {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param restRequestHandler the {@link RestRequestHandler} to use.
   * @param publicAccessLogger the {@link PublicAccessLogger} to use.
   * @param restServerState the {@link RestServerState} to use.
   * @param sslFactory the {@link SSLFactory} to use.
   */
  private void doConstructionFailureTest(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestHandler restRequestHandler, PublicAccessLogger publicAccessLogger, RestServerState restServerState,
      SSLFactory sslFactory) {
    try {
      new NettyServerFactory(verifiableProperties, metricRegistry, restRequestHandler, publicAccessLogger,
          restServerState, sslFactory);
      fail("Instantiation should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }
}
