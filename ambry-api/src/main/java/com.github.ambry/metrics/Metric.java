package com.github.ambry.metrics;

/**
 * Metric class that allows metric visitors to visit it to get its information.
 * The metrics information is required by the visitor to register the metric.
 */
public interface Metric {
  void visit(MetricsVisitor visitor);
}

