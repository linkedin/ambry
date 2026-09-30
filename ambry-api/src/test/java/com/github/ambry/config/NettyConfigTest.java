/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link NettyConfig}.
 */
public class NettyConfigTest {

  @Test
  public void testOfflineServiceIdsDefaultIsEmpty() {
    NettyConfig config = new NettyConfig(new VerifiableProperties(new Properties()));
    Assert.assertEquals("Unconfigured netty.server.offline.service.ids should yield an empty set, not {\"\"}",
        Collections.emptySet(), config.nettyServerOfflineServiceIds);
  }

  @Test
  public void testOfflineServiceIdsTrimsWhitespaceAndDropsEmptyEntries() {
    Properties properties = new Properties();
    properties.setProperty(NettyConfig.NETTY_SERVER_OFFLINE_SERVICE_IDS, " service-a, service-b ,,service-c");
    NettyConfig config = new NettyConfig(new VerifiableProperties(properties));
    Assert.assertEquals("Ids should be trimmed and empty entries dropped",
        new HashSet<>(Arrays.asList("service-a", "service-b", "service-c")), config.nettyServerOfflineServiceIds);
  }
}
