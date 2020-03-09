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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.router.InMemoryRouterFactory;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test functionality of {@link RestServer}.
 */
public class RestServerTest {
  private static final SSLFactory SSL_FACTORY = RestTestUtils.getTestSSLFactory();

  /**
   * Tests {@link RestServer#start()} and {@link RestServer#shutdown()}.
   * @throws Exception
   */
  @Test
  public void startShutdownTest() throws Exception {
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = getVProps(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();

    RestServer server = new RestServer(verifiableProperties, clusterMap, notificationSystem, SSL_FACTORY);
    server.start();
    server.shutdown();
    server.awaitShutdown();
  }

  /**
   * Tests for {@link RestServer#shutdown()} when {@link RestServer#start()} had not been called previously. This test
   * is for cases where {@link RestServer#start()} has failed and {@link RestServer#shutdown()} needs to be run.
   * @throws Exception
   */
  @Test
  public void shutdownWithoutStartTest() throws Exception {
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = getVProps(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();

    RestServer server = new RestServer(verifiableProperties, clusterMap, notificationSystem, SSL_FACTORY);
    server.shutdown();
    server.awaitShutdown();
  }

  /**
   * Tests for correct exceptions thrown on {@link RestServer} instantiation/{@link RestServer#start()} with bad input.
   * @throws Exception
   * @throws IOException
   */
  @Test
  public void serverCreationWithBadInputTest() throws Exception {
    badArgumentsTest();
    badFactoriesTest();
  }

  /**
   * Tests for correct exceptions thrown on {@link RestServer#start()}/{@link RestServer#shutdown()} with bad
   * components.
   * @throws Exception
   */
  @Test
  public void startShutdownTestWithBadComponent() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("rest.server.nio.server.factory", MockNioServerFactory.class.getCanonicalName());
    // makes MockNioServer throw exceptions.
    properties.setProperty(MockNioServerFactory.IS_FAULTY_KEY, "true");
    VerifiableProperties verifiableProperties = getVProps(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();
    RestServer server = new RestServer(verifiableProperties, clusterMap, notificationSystem, SSL_FACTORY);
    try {
      server.start();
      fail("start() should not be successful. MockNioServer::start() would have thrown InstantiationException");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    } finally {
      try {
        server.shutdown();
        fail("RestServer shutdown should have failed.");
      } catch (RuntimeException e) {
        // nothing to do. expected.
      }
    }
  }

  // helpers
  // general

  /**
   * Gets {@link VerifiableProperties} with some mandatory values set.
   * @param properties the {@link Properties} file to use to build the {@link VerifiableProperties}.
   * @return an instance of {@link VerifiableProperties}.
   */
  private VerifiableProperties getVProps(Properties properties) {
    setMandatoryValues(properties);
    return new VerifiableProperties(properties);
  }

  /**
   * Set some mandatory values in the {@link Properties} passed in.
   * @param properties The {@link Properties} to set these values in.
   */
  private void setMandatoryValues(Properties properties) {
    properties.setProperty("rest.server.router.factory", InMemoryRouterFactory.class.getCanonicalName());
    properties.setProperty("rest.server.response.handler.factory",
        MockRestRequestResponseHandlerFactory.class.getCanonicalName());
    properties.setProperty("rest.server.rest.request.service.factory",
        MockRestRequestServiceFactory.class.getCanonicalName());
    properties.setProperty("rest.server.request.response.handler.factory",
        MockRestRequestResponseHandlerFactory.class.getCanonicalName());
    properties.setProperty("rest.server.nio.server.factory", MockNioServerFactory.class.getCanonicalName());
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "dc1");
  }

  // serverCreationWithBadInputTest() helpers

  /**
   * Tests {@link RestServer} instantiation attempts with bad input.
   * @throws Exception
   * @throws IOException
   */
  private void badArgumentsTest() throws Exception {
    // dud properties. server should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();

    try {
      // no props.
      new RestServer(null, clusterMap, notificationSystem, SSL_FACTORY);
      fail("Properties missing, yet no exception was thrown");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    }

    try {
      // no ClusterMap.
      new RestServer(verifiableProperties, null, notificationSystem, SSL_FACTORY);
      fail("ClusterMap missing, yet no exception was thrown");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    }

    try {
      // no NotificationSystem.
      new RestServer(verifiableProperties, clusterMap, null, SSL_FACTORY);
      fail("NotificationSystem missing, yet no exception was thrown");
    } catch (IllegalArgumentException e) {
      // nothing to do. expected.
    }
  }

  /**
   * Tests for bad factory class names in {@link RestServer}.
   * @throws Exception
   */
  private void badFactoriesTest() throws Exception {
    doBadFactoryClassTest("rest.server.nio.server.factory");
    doBadFactoryClassTest("rest.server.rest.request.service.factory");
    doBadFactoryClassTest("rest.server.router.factory");
    doBadFactoryClassTest("rest.server.request.response.handler.factory");
    doBadFactoryClassTest("rest.server.account.service.factory");
  }

  /**
   * Tests for bad factory class name for {@code configKey} in {@link RestServer}.
   * @param configKey the property whose value is the bad factory class
   * @throws Exception
   */
  private void doBadFactoryClassTest(String configKey) throws Exception {
    Properties properties = new Properties();
    setMandatoryValues(properties);

    // Non existent class.
    properties.setProperty(configKey, "non.existent.factory");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem(), SSL_FACTORY);
      fail("Properties file contained non existent " + configKey + ", yet no exception was thrown");
    } catch (ClassNotFoundException e) {
      // nothing to do. expected.
    }

    // invalid factory class.
    properties.setProperty(configKey, RestServerTest.class.getCanonicalName());
    verifiableProperties = new VerifiableProperties(properties);
    try {
      new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem(), SSL_FACTORY);
      fail("Properties file contained invalid " + configKey + " class, yet no exception was thrown");
    } catch (NoSuchMethodException e) {
      // nothing to do. expected.
    }

    // faulty factory class
    properties.setProperty(configKey, FaultyFactory.class.getCanonicalName());
    verifiableProperties = new VerifiableProperties(properties);
    try {
      RestServer restServer =
          new RestServer(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem(), SSL_FACTORY);
      restServer.start();
      fail("Properties file contained faulty " + configKey + " class, yet no exception was thrown");
    } catch (InstantiationException e) {
      // nothing to do. expected.
    }
  }
}
