package com.github.ambry.metrics;

public interface MetricsReporter {
  void start();

  void register(String source, ReadableMetricsRegistry registry);

  void stop();
}

