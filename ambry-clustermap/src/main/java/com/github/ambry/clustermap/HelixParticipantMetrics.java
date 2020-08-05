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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;


/**
 * Metrics for {@link HelixParticipant} to monitor partition state transitions.
 */
class HelixParticipantMetrics {
  private Map<ReplicaState, Integer> replicaCountByState = new HashMap<>();
  private final Map<String, ReplicaState> localPartitionAndState;
  // no need to record exact number of "dropped" partition, a counter to track partition-dropped events would suffice
  final Counter partitionDroppedCount;
  final Counter setReplicaDisabledStateErrorCount;

  HelixParticipantMetrics(MetricRegistry metricRegistry, String zkConnectStr,
      Map<String, ReplicaState> localPartitionAndState) {
    String zkSuffix = zkConnectStr == null ? "" : "-" + zkConnectStr;
    this.localPartitionAndState = localPartitionAndState;
    EnumSet.complementOf(EnumSet.of(ReplicaState.DROPPED)).forEach(state -> replicaCountByState.put(state, 0));
    Gauge<Integer> bootstrapPartitionCount = () -> getReplicaCountInState(ReplicaState.BOOTSTRAP);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "bootstrapPartitionCount" + zkSuffix),
        bootstrapPartitionCount);
    Gauge<Integer> standbyPartitionCount = () -> getReplicaCountInState(ReplicaState.STANDBY);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "standbyPartitionCount" + zkSuffix),
        standbyPartitionCount);
    Gauge<Integer> leaderPartitionCount = () -> getReplicaCountInState(ReplicaState.LEADER);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "leaderPartitionCount" + zkSuffix),
        leaderPartitionCount);
    Gauge<Integer> inactivePartitionCount = () -> getReplicaCountInState(ReplicaState.INACTIVE);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "inactivePartitionCount" + zkSuffix),
        inactivePartitionCount);
    Gauge<Integer> offlinePartitionCount = () -> getReplicaCountInState(ReplicaState.OFFLINE);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "offlinePartitionCount" + zkSuffix),
        offlinePartitionCount);
    Gauge<Integer> errorStatePartitionCount = () -> getReplicaCountInState(ReplicaState.ERROR);
    metricRegistry.register(MetricRegistry.name(HelixParticipant.class, "errorStatePartitionCount" + zkSuffix),
        errorStatePartitionCount);
    partitionDroppedCount =
        metricRegistry.counter(MetricRegistry.name(HelixParticipant.class, "partitionDroppedCount" + zkSuffix));
    setReplicaDisabledStateErrorCount = metricRegistry.counter(
        MetricRegistry.name(HelixParticipant.class, "setReplicaDisabledStateErrorCount" + zkSuffix));
  }

  /**
   * Get the number of replicas in given state.
   * @param state the {@link ReplicaState} associated with local replica.
   * @return number of replicas in given state
   */
  private int getReplicaCountInState(ReplicaState state) {
    // Scan the whole map only when it's OFFLINE state. Other gauges should be able to read cached result from
    // replicaCountByState map.
    if (state == ReplicaState.OFFLINE) {
      Map<ReplicaState, Integer> replicaStateAndCount = new HashMap<>();
      EnumSet.complementOf(EnumSet.of(ReplicaState.DROPPED))
          .forEach(replicaState -> replicaStateAndCount.put(replicaState, 0));
      for (ReplicaState replicaState : localPartitionAndState.values()) {
        replicaStateAndCount.put(replicaState, replicaStateAndCount.get(replicaState) + 1);
      }
      // reference switch should be atomic
      replicaCountByState = replicaStateAndCount;
    }
    return replicaCountByState.get(state);
  }
}
