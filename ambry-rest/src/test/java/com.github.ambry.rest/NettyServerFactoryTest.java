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
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link NettyServerFactory}.
 */
public class NettyServerFactoryTest {

  /**
   * Checks to see that getting the default {@link NioServer} (currently {@link NettyServer}) works.
   */
  @Test
  public void getNettyServerTest() throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    NettyConfig nettyConfig = new NettyConfig(verifiableProperties);
    RestRequestHandler restRequestHandler = new MockRestRequestResponseHandler();
    PublicAccessLogger publicAccessLogger = new PublicAccessLogger(new String[]{}, new String[]{});
    RestServerState restServerState = new RestServerState("/healthCheck");
    SSLFactory sslFactory = new MockSSLFactory(verifiableProperties);

    for (int i = 0; i < 2; i++) {
      NettyServerFactory nettyServerFactory =
          new NettyServerFactory(verifiableProperties, new MetricRegistry(), restRequestHandler, publicAccessLogger,
              restServerState, i == 0 ? sslFactory : null);
      NioServer nioServer = nettyServerFactory.getNioServer();
      assertNotNull("No NioServer returned", nioServer);
      assertEquals("Did not receive a NettyServer instance", NettyServer.class.getCanonicalName(),
          nioServer.getClass().getCanonicalName());
      Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers = nettyServerFactory.getChannelInitializers();
      if (i == 0) {
        assertEquals("Expected two ChannelInitializers when SSLFactoryImpl is not null", 2, channelInitializers.size());
        assertNotNull("No ChannelInitializer for SSL port", channelInitializers.get(nettyConfig.nettyServerSSLPort));
      } else {
        assertEquals("Expected one ChannelInitializer when SSLFactoryImpl is null", 1, channelInitializers.size());
      }
      assertNotNull("No ChannelInitializer for plaintext port", channelInitializers.get(nettyConfig.nettyServerPort));
    }
  }

  /**
   * Tests instantiation of {@link NettyServerFactory} with bad input.
   */
  @Test
  public void getNettyServerFactoryWithBadInputTest() throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestHandler restRequestHandler = new MockRestRequestResponseHandler();
    PublicAccessLogger publicAccessLogger = new PublicAccessLogger(new String[]{}, new String[]{});
    RestServerState restServerState = new RestServerState("/healthCheck");
    SSLFactory sslFactory = new MockSSLFactory(verifiableProperties);

    for (int i = 0; i < 5; i++) {
      try {
        new NettyServerFactory(i == 0 ? verifiableProperties : null, i == 1 ? metricRegistry : null,
            i == 2 ? restRequestHandler : null, i == 3 ? publicAccessLogger : null, i == 4 ? restServerState : null,
            sslFactory);
        fail("Instantiation should have failed because argument " + i + " was null");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }

    // Should be int. So will throw at instantiation.
    properties.setProperty("netty.server.ssl.port", "abcd");
    try {
      new NettyServerFactory(verifiableProperties, metricRegistry, restRequestHandler, publicAccessLogger,
          restServerState, sslFactory);
      fail("NettyServerFactory instantiation should have failed because of bad port number value");
    } catch (NumberFormatException e) {
      // nothing to do. expected.
    }
  }
}
