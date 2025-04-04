/*
 * *
 *  * Copyright 2024 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package com.github.ambry.replication;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;


/**
 * Comparator that prioritizes replicas based on:
 * 1. Number of available replicas for the partition (fewer = higher priority)
 * 2. If equal, prioritize by partition size (larger = higher priority)
 * 3. If still equal, use partition ID for consistent ordering
 */
public class ReplicaAvailabilityComparator implements Comparator<ReplicaId> {
  @Override
  public int compare(ReplicaId r1, ReplicaId r2) {

    // First compare by available replicas
    int availabilityCompare = Integer.compare(getPartitionAvailabilityInfo(r1), getPartitionAvailabilityInfo(r2));
    if (availabilityCompare != 0) {
      // Lower integer = higher priority
      return -availabilityCompare;
    }

    // If equal, use partition ID for consistent ordering
    return r1.getPartitionId().toString().compareTo(r2.getPartitionId().toString());
  }

  /**
   * Get availability information for a partition
   * @param replica the replica to get availability information for
   * @return PartitionAvailabilityInfo containing availability metrics
   */
  private int getPartitionAvailabilityInfo(ReplicaId replica) {
    Set<ReplicaState> states = new HashSet<>(Arrays.asList(ReplicaState.LEADER, ReplicaState.STANDBY));
    Map<ReplicaState, List<ReplicaId>> replicas = (Map<ReplicaState, List<ReplicaId>>) replica.getPartitionId()
        .getReplicaIdsByStates(states, replica.getDataNodeId().getDatacenterName());

    return replicas.size();
  }
}
