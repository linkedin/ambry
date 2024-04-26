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
import com.github.ambry.store.DeleteTombstoneStats;
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
  private final AtomicReference<DeleteTombstoneStats> aggregatedDeleteTombstoneStats;
  private final MetricRegistry registry;

  StatsManagerMetrics(MetricRegistry registry, AtomicReference<DeleteTombstoneStats> aggregatedDeleteTombstoneStats) {
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
      Gauge<Long> expiredDeleteTombstoneCount = () -> aggregatedDeleteTombstoneStats.get().expiredCount;
      registry.gauge(MetricRegistry.name(StatsManager.class, "ExpiredDeleteTombstoneCount"),
          () -> expiredDeleteTombstoneCount);
      Gauge<Long> expiredDeleteTombstoneSize = () -> aggregatedDeleteTombstoneStats.get().expiredSize;
      registry.gauge(MetricRegistry.name(StatsManager.class, "ExpiredDeleteTombstoneSize"),
          () -> expiredDeleteTombstoneSize);
      Gauge<Long> permanentDeleteTombstoneCount = () -> aggregatedDeleteTombstoneStats.get().permanentCount;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneCount"),
          () -> permanentDeleteTombstoneCount);
      Gauge<Long> permanentDeleteTombstoneSize = () -> aggregatedDeleteTombstoneStats.get().permanentSize;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSize"),
          () -> permanentDeleteTombstoneSize);
      Gauge<Long> permanentDeleteTombstoneSizeOneDay = () -> aggregatedDeleteTombstoneStats.get().permanentSizeOneDay;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSizeOneDay"),
          () -> permanentDeleteTombstoneSizeOneDay);
      Gauge<Long> permanentDeleteTombstoneSizeTwoDays = () -> aggregatedDeleteTombstoneStats.get().permanentSizeTwoDays;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSizeTwoDays"),
          () -> permanentDeleteTombstoneSizeTwoDays);
      Gauge<Long> permanentDeleteTombstoneSizeThreeDays =
          () -> aggregatedDeleteTombstoneStats.get().permanentSizeThreeDays;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSizeThreeDays"),
          () -> permanentDeleteTombstoneSizeThreeDays);
      Gauge<Long> permanentDeleteTombstoneSizeFiveDays =
          () -> aggregatedDeleteTombstoneStats.get().permanentSizeFiveDays;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSizeFiveDays"),
          () -> permanentDeleteTombstoneSizeFiveDays);
      Gauge<Long> permanentDeleteTombstoneSizeSevenDays =
          () -> aggregatedDeleteTombstoneStats.get().permanentSizeSevenDays;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSizeSevenDays"),
          () -> permanentDeleteTombstoneSizeSevenDays);
      Gauge<Long> permanentDeleteTombstoneSizeTenDays = () -> aggregatedDeleteTombstoneStats.get().permanentSizeTenDays;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSizeTenDays"),
          () -> permanentDeleteTombstoneSizeTenDays);
      Gauge<Long> permanentDeleteTombstoneSizeFourteenDays =
          () -> aggregatedDeleteTombstoneStats.get().permanentSizeFourteenDays;
      registry.gauge(MetricRegistry.name(StatsManager.class, "PermanentDeleteTombstoneSizeFourteenDays"),
          () -> permanentDeleteTombstoneSizeFourteenDays);
    }
  }
}
