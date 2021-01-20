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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Metrics for {@link StatsManager} and related components.
 */
class StatsManagerMetrics {
  final Counter statsAggregationFailureCount;
  final Histogram totalFetchAndAggregateTimeMs;
  final Histogram fetchAndAggregateTimePerStoreMs;
  private final AtomicBoolean deleteStatsGaugeInitialized = new AtomicBoolean(false);
  private final AtomicReference<StatsManager.AggregatedDeleteTombstoneStats> aggregatedDeleteTombstoneStats;
  private final MetricRegistry registry;

  StatsManagerMetrics(MetricRegistry registry,
      AtomicReference<StatsManager.AggregatedDeleteTombstoneStats> aggregatedDeleteTombstoneStats) {
    this.registry = registry;
    this.aggregatedDeleteTombstoneStats = aggregatedDeleteTombstoneStats;
    statsAggregationFailureCount =
        registry.counter(MetricRegistry.name(StatsManager.class, "StatsAggregationFailureCount"));
    totalFetchAndAggregateTimeMs =
        registry.histogram(MetricRegistry.name(StatsManager.class, "TotalFetchAndAggregateTimeMs"));
    fetchAndAggregateTimePerStoreMs =
        registry.histogram(MetricRegistry.name(StatsManager.class, "FetchAndAggregateTimePerStoreMs"));
  }

  void initDeleteStatsGaugesIfNeeded() {
    if (deleteStatsGaugeInitialized.compareAndSet(false, true)) {
      Gauge<Long> deleteTombstoneWithTtlCount =
          () -> aggregatedDeleteTombstoneStats.get().getDeleteTombstoneWithTtlCount();
      registry.register(MetricRegistry.name(StatsManager.class, "DeleteTombstoneWithTtlCount"),
          deleteTombstoneWithTtlCount);
      Gauge<Long> deleteTombstoneWithTtlSize =
          () -> aggregatedDeleteTombstoneStats.get().getDeleteTombstoneWithTtlSize();
      registry.register(MetricRegistry.name(StatsManager.class, "DeleteTombstoneWithTtlSize"),
          deleteTombstoneWithTtlSize);
      Gauge<Long> deleteTombstoneWithoutTtlCount =
          () -> aggregatedDeleteTombstoneStats.get().getDeleteTombstoneWithoutTtlCount();
      registry.register(MetricRegistry.name(StatsManager.class, "DeleteTombstoneWithoutTtlCount"),
          deleteTombstoneWithoutTtlCount);
      Gauge<Long> deleteTombstoneWithoutTtlSize =
          () -> aggregatedDeleteTombstoneStats.get().getDeleteTombstoneWithoutTtlSize();
      registry.register(MetricRegistry.name(StatsManager.class, "DeleteTombstoneWithoutTtlSize"),
          deleteTombstoneWithoutTtlSize);
    }
  }
}
