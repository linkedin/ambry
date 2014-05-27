package com.github.ambry.metrics;

import com.github.ambry.config.MetricsConfig;


public interface MetricsReporterFactory {
  MetricsReporter getMetricsReporter(String name, String containerName, MetricsConfig config);
}

