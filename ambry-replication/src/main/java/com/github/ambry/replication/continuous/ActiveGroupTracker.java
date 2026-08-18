/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication.continuous;

import java.util.List;
import java.util.stream.Collectors;


/**
 * This class tracks for a current state for Active groups for a continuous replication cycle.
 * Active group tracker have preassigned replicas and from these replicas only we create active remote replica groups
 * Any remote replica group created for this tracker has replicas with ACTIVE state only.
 *
 * Each tracker carries a {@code weight} used to compute its share of the per-cycle fetch budget:
 * a chunk's metadata-request fetchSize is {@code weight * baseFetchSize}, where {@code baseFetchSize}
 * is the per-cycle scalar held on {@link com.github.ambry.replication.ReplicaThread}'s
 * {@code RemoteReplicaGroupPoller}. Priority chunks are segregated into singleton groups with
 * {@code weight = boost} and {@code isPriority = true}; normal chunks get {@code weight = 1}.
 */
public class ActiveGroupTracker extends GroupTracker {
  private final List<ReplicaTracker> preAssignedReplicas;
  private final boolean priority;
  private final int weight;

  public ActiveGroupTracker(int groupId, List<ReplicaTracker> preAssignedReplicas) {
    this(groupId, preAssignedReplicas, false, 1);
  }

  public ActiveGroupTracker(int groupId, List<ReplicaTracker> preAssignedReplicas, boolean priority, int weight) {
    super(groupId);
    this.preAssignedReplicas = preAssignedReplicas;
    this.priority = priority;
    this.weight = weight;
  }

  public List<ReplicaTracker> getPreAssignedReplicas() {
    return preAssignedReplicas;
  }

  public List<ReplicaTracker> getPreAssignedReplicas(List<ReplicaStatus> statuses) {
    return preAssignedReplicas.stream()
        .filter(replicaTracker -> statuses.contains(replicaTracker.getReplicaStatus()))
        .collect(Collectors.toList());
  }

  /**
   * @return number of replicas pre-assigned to this group; used by the per-cycle fetch-budget
   * denominator in {@code ReplicaThread.fillDataNodeTrackers}.
   */
  public int getReplicaCount() {
    return preAssignedReplicas.size();
  }

  /**
   * @return true if this group was segregated to a single replica because that replica's partition
   * is a current priority entry; false for normal chunks.
   */
  public boolean isPriority() {
    return priority;
  }

  /**
   * @return the multiplier applied to the cycle's {@code baseFetchSize} when computing this
   * group's per-iteration metadata fetchSize. Priority groups carry the operator-supplied boost;
   * normal groups carry 1.
   */
  public int getWeight() {
    return weight;
  }

  @Override
  public String toString() {
    return "ActiveGroupTracker :[" + super.toString() + " " + preAssignedReplicas.toString() + " priority="
        + priority + " weight=" + weight + "]";
  }
}
