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
