package com.github.ambry.metrics;

public interface ReadableMetricsRegistryListener {
  void onCounter(String group, Counter counter);

  void onGauge(String group, Gauge<?> gauge);
}
