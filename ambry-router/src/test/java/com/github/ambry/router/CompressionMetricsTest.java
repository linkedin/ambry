package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import org.junit.Assert;
import org.junit.Test;

public class CompressionMetricsTest {

  @Test
  public void testConstructor() {
    CompressionMetrics metrics = new CompressionMetrics(new MetricRegistry());
    metrics.decompressSuccessRate.mark();
    Assert.assertEquals(1, metrics.decompressSuccessRate.getCount());

    CompressionMetrics.AlgorithmMetrics algorithmMetrics = metrics.getAlgorithmMetrics("ABC");
    algorithmMetrics.compressRate.mark();
    Assert.assertEquals(1, algorithmMetrics.compressRate.getCount());
  }
}