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
import com.github.ambry.commons.NettySslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.VerifiableProperties;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link FrontendNettyFactory}.
 */
public class FrontendNettyFactoryTest {
  // dud properties. server should pick up defaults
  private static final RestRequestHandler REST_REQUEST_HANDLER =
      new AsyncRequestResponseHandler(new RequestResponseHandlerMetrics(new MetricRegistry()), 1,
          new MockRestRequestService(new VerifiableProperties(new Properties()), new MockRouter()));
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
    // test with ssl
    properties.setProperty("netty.server.enable.ssl", "true");
    doGetNettyServerTest(properties, SSL_FACTORY);
    // test overriding ssl factory
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    trustStoreFile.deleteOnExit();
    TestSSLUtils.addSSLProperties(properties, "", SSLFactory.Mode.SERVER, trustStoreFile, "frontend");
    properties.setProperty(NettyConfig.SSL_FACTORY_KEY, NettySslFactory.class.getName());
    doGetNettyServerTest(properties, SSL_FACTORY);
  }

  /**
   * Test that a {@link NettyServer} can be constructed by the factory.
   * @param properties the {@link Properties} to use.
   * @param defaultSslFactory the default {@link SSLFactory} to pass into the constructor.
   */
  private void doGetNettyServerTest(Properties properties, SSLFactory defaultSslFactory) throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    NettyConfig nettyConfig = new NettyConfig(verifiableProperties);
    FrontendNettyFactory nettyServerFactory =
        new FrontendNettyFactory(verifiableProperties, new MetricRegistry(), REST_REQUEST_HANDLER, PUBLIC_ACCESS_LOGGER,
            REST_SERVER_STATE, defaultSslFactory);
    NioServer nioServer = nettyServerFactory.getNioServer();
    assertNotNull("No NioServer returned", nioServer);
    assertEquals("Did not receive a NettyServer instance", NettyServer.class.getCanonicalName(),
        nioServer.getClass().getCanonicalName());
    Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers = nettyServerFactory.channelInitializers;
    if (nettyConfig.nettyServerEnableSSL && defaultSslFactory != null) {
      assertEquals("Expected two ChannelInitializers when SSLFactory is not null", 2, channelInitializers.size());
      assertNotNull("No ChannelInitializer for SSL port", channelInitializers.get(nettyConfig.nettyServerSSLPort));
    } else {
      assertEquals("Expected one ChannelInitializer when SSLFactory is null", 1, channelInitializers.size());
    }
    assertNotNull("No ChannelInitializer for plaintext port", channelInitializers.get(nettyConfig.nettyServerPort));
  }

  /**
   * Tests instantiation of {@link FrontendNettyFactory} with bad input.
   */
  @Test
  public void getNettyServerFactoryWithBadInputTest() throws Exception {
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
   * Test that {@link FrontendNettyFactory} construction fails with the given constructor inputs.
   * @param verifiableProperties the {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param restRequestHandler the {@link RestRequestHandler} to use.
   * @param publicAccessLogger the {@link PublicAccessLogger} to use.
   * @param restServerState the {@link RestServerState} to use.
   * @param sslFactory the {@link SSLFactory} to use.
   */
  private void doConstructionFailureTest(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestHandler restRequestHandler, PublicAccessLogger publicAccessLogger, RestServerState restServerState,
      SSLFactory sslFactory) throws Exception {
    try {
      new FrontendNettyFactory(verifiableProperties, metricRegistry, restRequestHandler, publicAccessLogger,
          restServerState, sslFactory);
      fail("Instantiation should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }
}
