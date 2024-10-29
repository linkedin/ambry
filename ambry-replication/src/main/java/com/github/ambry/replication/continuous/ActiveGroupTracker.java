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
 * Any remote replica group created for this tracker has replicas with ACTIVE state only
 */
public class ActiveGroupTracker extends GroupTracker {
  private final List<ReplicaTracker> preAssignedReplicas;

  public ActiveGroupTracker(int groupId, List<ReplicaTracker> preAssignedReplicas) {
    super(groupId);
    this.preAssignedReplicas = preAssignedReplicas;
  }

  public List<ReplicaTracker> getPreAssignedReplicas() {
    return preAssignedReplicas;
  }

  public List<ReplicaTracker> getPreAssignedReplicas(List<ReplicaStatus> statuses) {
    return preAssignedReplicas.stream()
        .filter(replicaTracker -> statuses.contains(replicaTracker.getReplicaStatus()))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "ActiveGroupTracker :[" + super.toString() + " " + preAssignedReplicas.toString() + "]";
  }
}
