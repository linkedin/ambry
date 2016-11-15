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
import com.github.ambry.config.VerifiableProperties;
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
  public void getNettyServerTest() {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    RestRequestHandler restRequestHandler = new MockRestRequestResponseHandler();
    PublicAccessLogger publicAccessLogger = new PublicAccessLogger(new String[]{}, new String[]{});
    RestServerState restServerState = new RestServerState("/healthCheck");

    NettyServerFactory nettyServerFactory =
        new NettyServerFactory(verifiableProperties, new MetricRegistry(), restRequestHandler, publicAccessLogger,
            restServerState);
    NioServer nioServer = nettyServerFactory.getNioServer();
    assertNotNull("No NioServer returned", nioServer);
    assertEquals("Did not receive a NettyServer instance", NettyServer.class.getCanonicalName(),
        nioServer.getClass().getCanonicalName());
  }

  /**
   * Tests instantiation of {@link NettyServerFactory} with bad input.
   */
  @Test
  public void getNettyServerFactoryWithBadInputTest() {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestHandler restRequestHandler = new MockRestRequestResponseHandler();
    PublicAccessLogger publicAccessLogger = new PublicAccessLogger(new String[]{}, new String[]{});
    RestServerState restServerState = new RestServerState("/healthCheck");

    // VerifiableProperties null.
    try {
      new NettyServerFactory(null, metricRegistry, restRequestHandler, publicAccessLogger, restServerState);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // MetricRegistry null.
    try {
      new NettyServerFactory(verifiableProperties, null, restRequestHandler, publicAccessLogger, restServerState);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // RestRequestHandler null.
    try {
      new NettyServerFactory(verifiableProperties, metricRegistry, null, publicAccessLogger, restServerState);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // PublicAccessLogger null.
    try {
      new NettyServerFactory(verifiableProperties, metricRegistry, restRequestHandler, null, restServerState);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // RestServerState null.
    try {
      new NettyServerFactory(verifiableProperties, metricRegistry, restRequestHandler, publicAccessLogger, null);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }
}
