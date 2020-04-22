/*
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
package com.github.ambry.clustermap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Metrics for {@link HelixParticipant} to monitor partition state transitions.
 */
class HelixParticipantMetrics {
  final AtomicInteger bootstrapCount = new AtomicInteger();
  final AtomicInteger standbyCount = new AtomicInteger();
  final AtomicInteger leaderCount = new AtomicInteger();
  final AtomicInteger inactiveCount = new AtomicInteger();
  final AtomicInteger offlineCount = new AtomicInteger();
  final AtomicInteger errorStateCount = new AtomicInteger();
  // no need to record exact number of "dropped" partition, a counter to track partition-dropped events would suffice
  final Counter partitionDroppedCount;

  HelixParticipantMetrics(MetricRegistry metricRegistry, String zkConnectStr) {
    String zkSuffix = zkConnectStr == null ? "" : "-" + zkConnectStr;
    Gauge<Integer> bootstrapPartitionCount = bootstrapCount::get;
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "bootstrapPartitionCount" + zkSuffix),
        bootstrapPartitionCount);
    Gauge<Integer> standbyPartitionCount = standbyCount::get;
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "standbyPartitionCount" + zkSuffix),
        standbyPartitionCount);
    Gauge<Integer> leaderPartitionCount = leaderCount::get;
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "leaderPartitionCount" + zkSuffix),
        leaderPartitionCount);
    Gauge<Integer> inactivePartitionCount = inactiveCount::get;
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "inactivePartitionCount" + zkSuffix),
        inactivePartitionCount);
    Gauge<Integer> offlinePartitionCount = offlineCount::get;
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "offlinePartitionCount" + zkSuffix),
        offlinePartitionCount);
    Gauge<Integer> errorStatePartitionCount = errorStateCount::get;
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "errorStatePartitionCount" + zkSuffix),
        errorStatePartitionCount);
    partitionDroppedCount =
        metricRegistry.counter(MetricRegistry.name(HelixParticipant.class, "partitionDroppedCount" + zkSuffix));
  }

  /**
   * Set number of partitions on current node. This is invoked during startup.
   * @param partitionCount number of partitions on current node
   */
  void setLocalPartitionCount(int partitionCount) {
    // this method should be invoked before participation, so the initial value is expected to be 0.
    if (!offlineCount.compareAndSet(0, partitionCount)) {
      throw new IllegalStateException("Number of OFFLINE partitions has changed to " + offlineCount.get()
          + " before initializing participant metrics ");
    }
  }
}
