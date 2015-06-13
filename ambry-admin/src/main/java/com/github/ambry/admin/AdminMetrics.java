package com.github.ambry.admin;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;


/**
 * Admin specific metrics tracking
 */
public class AdminMetrics {
  //errors
  /**
   * tracks requests with http methods that the server does not know how to execute.
   */
  public final Counter unknownActionErrorCount;

  public AdminMetrics(MetricRegistry metricRegistry) {
    //errors
    unknownActionErrorCount =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "unknownActionErrorCount"));
  }
}
