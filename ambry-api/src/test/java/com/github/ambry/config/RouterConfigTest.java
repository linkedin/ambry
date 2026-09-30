/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.config;

import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link RouterConfig}.
 */
public class RouterConfigTest {

  @Test
  public void testRouterBlobIdCurrentVersionAllowsSeven() {
    Properties properties = getRequiredRouterProperties();
    properties.setProperty(RouterConfig.ROUTER_BLOBID_CURRENT_VERSION, "7");
    RouterConfig config = new RouterConfig(new VerifiableProperties(properties));
    Assert.assertEquals("router.blobid.current.version should accept value 7", 7, config.routerBlobidCurrentVersion);
  }

  @Test
  public void testRouterBlobIdCurrentVersionRejectsOutOfRange() {
    Properties properties = getRequiredRouterProperties();
    properties.setProperty(RouterConfig.ROUTER_BLOBID_CURRENT_VERSION, "8");
    try {
      new RouterConfig(new VerifiableProperties(properties));
      Assert.fail("router.blobid.current.version outside allowed set should fail");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void testRouterBlobIdCurrentVersionRejectsBelowRange() {
    Properties properties = getRequiredRouterProperties();
    properties.setProperty(RouterConfig.ROUTER_BLOBID_CURRENT_VERSION, "0");
    try {
      new RouterConfig(new VerifiableProperties(properties));
      Assert.fail("router.blobid.current.version below allowed set should fail");
    } catch (IllegalArgumentException ignored) {
    }
  }

  private Properties getRequiredRouterProperties() {
    Properties properties = new Properties();
    properties.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    properties.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, "DEV");
    return properties;
  }
}
