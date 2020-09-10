package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.Throttler;
import com.github.ambry.commons.ThrottlerFactory;
import com.github.ambry.commons.ThrottlingMode;


public class ServerHealthThrottlerFactory implements ThrottlerFactory {
  private final MetricRegistry metricRegistry;
  private final ThrottlingMode throttlingMode;
  private final boolean isHttp2;

  public ServerHealthThrottlerFactory(MetricRegistry metricRegistry, ThrottlingMode throttlingMode, boolean isHttp2) {
    this.metricRegistry = metricRegistry;
    this.throttlingMode = throttlingMode;
    this.isHttp2 = isHttp2;
  }

  @Override
  public Throttler getThrottler() {
    return new ServerHealthThrottler(metricRegistry, throttlingMode, isHttp2);
  }
}
