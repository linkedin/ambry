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
package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link CoordinatorBackedRouterFactory}.
 */
public class CoordinatorBackedRouterFactoryTest {

  /**
   * Tests the instantiation of an {@link CoordinatorBackedRouter} instance through the
   * {@link CoordinatorBackedRouterFactory}.
   * @throws IOException
   */
  @Test
  public void getCoordinatorBackedRouterTest()
      throws IOException {
    VerifiableProperties verifiableProperties = getVprops();

    CoordinatorBackedRouterFactory routerFactory =
        new CoordinatorBackedRouterFactory(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
    Router router = routerFactory.getRouter();
    assertNotNull("No RouterFactory returned", routerFactory);
    assertEquals("Did not receive an CoordinatorBackedRouter instance",
        CoordinatorBackedRouter.class.getCanonicalName(), router.getClass().getCanonicalName());
  }

  /**
   * Tests instantiation of {@link CoordinatorBackedRouterFactory} with bad input.
   * @throws IOException
   */
  @Test
  public void getCoordinatorBackedRouterFactoryWithBadInputTest()
      throws IOException {
    VerifiableProperties verifiableProperties = getVprops();
    ClusterMap clusterMap = new MockClusterMap();
    NotificationSystem notificationSystem = new LoggingNotificationSystem();

    // VerifiableProperties null.
    try {
      new CoordinatorBackedRouterFactory(null, clusterMap, notificationSystem);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // ClusterMap null.
    try {
      new CoordinatorBackedRouterFactory(verifiableProperties, null, notificationSystem);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // NotificationSystem null.
    try {
      new CoordinatorBackedRouterFactory(verifiableProperties, clusterMap, null);
      fail("Instantiation should have failed because one of the arguments was null");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  public VerifiableProperties getVprops() {
    Properties properties = new Properties();
    properties.setProperty("coordinator.hostname", "localhost");
    properties.setProperty("coordinator.datacenter.name", "DC1");
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    return new VerifiableProperties(properties);
  }
}
