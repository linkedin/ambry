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
package com.github.ambry.metrics;

import com.github.ambry.config.MetricsConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


public class JvmMetricsTest {

  class MockMetricsRegistryMap extends MetricsRegistryMap {

    public boolean hasMetric(String name) {
      Metric metric = metrics.get("com.github.ambry.metrics.JvmMetrics").get(name);
      return metric != null;
    }
  }

  @Test
  public void testJvmMetrics()
      throws IOException {
    MockMetricsRegistryMap registry = new MockMetricsRegistryMap();
    JvmMetrics metrics = new JvmMetrics(registry);
    metrics.start();

    // verify that all metrics have been registered
    Assert.assertEquals(true, registry.hasMetric("mem-non-heap-used-mb"));
    Assert.assertEquals(true, registry.hasMetric("mem-non-heap-committed-mb"));
    Assert.assertEquals(true, registry.hasMetric("mem-heap-used-mb"));
    Assert.assertEquals(true, registry.hasMetric("mem-heap-committed-mb"));
    Assert.assertEquals(true, registry.hasMetric("threads-new"));
    Assert.assertEquals(true, registry.hasMetric("threads-runnable"));
    Assert.assertEquals(true, registry.hasMetric("threads-blocked"));
    Assert.assertEquals(true, registry.hasMetric("threads-waiting"));
    Assert.assertEquals(true, registry.hasMetric("threads-timed-waiting"));
    Assert.assertEquals(true, registry.hasMetric("threads-terminated"));
    Assert.assertEquals(true, registry.hasMetric("gc-count"));
    Assert.assertEquals(true, registry.hasMetric("gc-time-millis"));

    metrics.stop();
  }
}
