package com.github.ambry.admin;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.ServerMetrics;


/**
 * Admin specific metrics tracking
 */
public class AdminMetrics extends ServerMetrics {
  //errors
  public final Counter handlerSelectionErrorCount;
  public final Counter noMessageHandlersErrorCount;
  public final Counter unknownActionErrorCount;

  public AdminMetrics(MetricRegistry metricRegistry) {
    super(metricRegistry);

    //errors
    handlerSelectionErrorCount =
        metricRegistry.counter(MetricRegistry.name(AdminRequestDelegator.class, "handlerSelectionErrorCount"));
    noMessageHandlersErrorCount =
        metricRegistry.counter(MetricRegistry.name(AdminRequestDelegator.class, "noMessageHandlersErrorCount"));
    unknownActionErrorCount =
        metricRegistry.counter(MetricRegistry.name(AdminMessageHandler.class, "unknownActionErrorCount"));
  }
}
