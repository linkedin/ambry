package com.github.ambry.metrics;

public class NoOpMetricsRegistry implements MetricsRegistry {
  @Override
  public Counter newCounter(String group, String name) {
    return new Counter(name);
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    return counter;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    return new Gauge<T>(name, value);
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> gauge) {
    return gauge;
  }
}

