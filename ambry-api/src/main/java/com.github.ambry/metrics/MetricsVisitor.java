package com.github.ambry.metrics;

/**
 * A metric visitor visits a metric, before metrics are flushed to a metrics stream.
 */
public abstract class MetricsVisitor {
  public abstract void counter(Counter counter);

  public abstract <T> void gauge(Gauge<T> gauge);

  public void visit(Metric metric) {
    if (metric instanceof Counter) {
      counter((Counter) metric);
    } else if (metric instanceof Gauge<?>) {
      gauge((Gauge<?>) metric);
    }
  }
}
