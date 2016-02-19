package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;


/**
 * {@link NonBlockingRouter} specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link NonBlockingRouter} to the provided {@link MetricRegistry}
 */
public class NonBlockingRouterMetrics {
  //@todo add metrics.
  MetricRegistry registry;
  public NonBlockingRouterMetrics(MetricRegistry registry) {
    this.registry = registry;
  }
}
