package com.github.ambry.metrics;


/**
 * Metric class that allows metric visitors to visit it to get its information.
 */
public interface Metric {
  void visit(MetricsVisitor visitor);
}

