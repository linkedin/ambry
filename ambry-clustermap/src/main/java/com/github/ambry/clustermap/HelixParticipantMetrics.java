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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Metrics for {@link HelixParticipant} to monitor partition state transitions.
 */
class HelixParticipantMetrics {
  final AtomicInteger offlineCount = new AtomicInteger();
  // no need to record exact number of "dropped" partition, a counter to track partition-dropped events would suffice
  final Counter partitionDroppedCount;

  HelixParticipantMetrics(MetricRegistry metricRegistry, String zkConnectStr,
      Map<String, ReplicaState> localPartitionAndState) {
    String zkSuffix = zkConnectStr == null ? "" : "-" + zkConnectStr;
    Gauge<Integer> bootstrapPartitionCount =
        () -> Collections.frequency(localPartitionAndState.values(), ReplicaState.BOOTSTRAP);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "bootstrapPartitionCount" + zkSuffix),
        bootstrapPartitionCount);
    Gauge<Integer> standbyPartitionCount =
        () -> Collections.frequency(localPartitionAndState.values(), ReplicaState.STANDBY);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "standbyPartitionCount" + zkSuffix),
        standbyPartitionCount);
    Gauge<Integer> leaderPartitionCount =
        () -> Collections.frequency(localPartitionAndState.values(), ReplicaState.LEADER);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "leaderPartitionCount" + zkSuffix),
        leaderPartitionCount);
    Gauge<Integer> inactivePartitionCount =
        () -> Collections.frequency(localPartitionAndState.values(), ReplicaState.INACTIVE);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "inactivePartitionCount" + zkSuffix),
        inactivePartitionCount);
    Gauge<Integer> offlinePartitionCount =
        () -> Collections.frequency(localPartitionAndState.values(), ReplicaState.OFFLINE);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "offlinePartitionCount" + zkSuffix),
        offlinePartitionCount);
    Gauge<Integer> errorStatePartitionCount =
        () -> Collections.frequency(localPartitionAndState.values(), ReplicaState.ERROR);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "errorStatePartitionCount" + zkSuffix),
        errorStatePartitionCount);
    partitionDroppedCount =
        metricRegistry.counter(MetricRegistry.name(HelixParticipant.class, "partitionDroppedCount" + zkSuffix));
  }
}
