/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.server;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.Throttler;
import com.github.ambry.commons.ThrottlingMode;
import com.github.ambry.network.Selector;
import com.github.ambry.rest.ConnectionStatsHandler;


/**
 * Class to throttle requests made to Ambry server based on the system health metrics.
 */
public class ServerHealthThrottler extends Throttler {
  private final MetricRegistry metricRegistry;
  private final boolean isHttp2;

  /**
   * Constructor for {@link ServerHealthThrottler}
   * @param metricRegistry {@link MetricRegistry} object.
   */
  public ServerHealthThrottler(MetricRegistry metricRegistry, ThrottlingMode throttlingMode, boolean isHttp2) {
    super(throttlingMode);
    this.metricRegistry = metricRegistry;
    this.isHttp2 = isHttp2;
  }

  private long getOpenConnections() {
    if (isHttp2)
      return getHttp2OpenConnections();
    else
      return getHttpOpenConnections();
  }

  private long getHttp2OpenConnections() {
    Gauge<Long> openConnections = (Gauge<Long>) metricRegistry.getMetrics()
        .get(MetricRegistry.name(ConnectionStatsHandler.class, "OpenConnections"));
    if (openConnections != null) {
      return openConnections.getValue();
    }
    return 0;
  }

  private long getHttpOpenConnections() {
    Gauge<Long> openConnections = (Gauge<Long>) metricRegistry.getMetrics()
        .get(MetricRegistry.name(Selector.class, "SelectorActiveConnectionsCount"));
    if(openConnections != null) {
      return openConnections.getValue();
    } else {
      return 0;
    }
  }

  private int getHeapUsage() {
    Runtime runtime = Runtime.getRuntime();
    return (int) (((runtime.totalMemory() - runtime.freeMemory()) * 100) / runtime.maxMemory());
  }

  public boolean throttle() {
    return false;
  }
}
