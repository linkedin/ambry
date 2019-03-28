/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


public class VcrMetrics {

  public final Histogram vcrStartTimeInMs;
  public final Histogram vcrShutdownTimeInMs;

  public VcrMetrics(MetricRegistry registry) {
    // TODO: add metrics to registry
    vcrStartTimeInMs = registry.histogram(MetricRegistry.name(VcrServer.class, "VcrStartTimeInMs"));
    vcrShutdownTimeInMs = registry.histogram(MetricRegistry.name(VcrServer.class, "VcrShutdownTimeInMs"));
  }
}
