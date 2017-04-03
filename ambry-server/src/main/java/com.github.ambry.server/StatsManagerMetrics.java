/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics for {@link StatsManager} and related components.
 */
class StatsManagerMetrics {
  final Counter statsAggregationFailureCount;
  final Histogram totalFetchAndAggregateTimeMs;
  final Histogram fetchAndAggregateTimePerStoreMs;

  StatsManagerMetrics(MetricRegistry registry) {
    statsAggregationFailureCount =
        registry.counter(MetricRegistry.name(StatsManager.class, "StatsAggregationFailureCount"));
    totalFetchAndAggregateTimeMs =
        registry.histogram(MetricRegistry.name(StatsManager.class, "TotalFetchAndAggregateTimeMs"));
    fetchAndAggregateTimePerStoreMs =
        registry.histogram(MetricRegistry.name(StatsManager.class, "FetchAndAggregateTimePerStoreMs"));
  }
}
