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

import com.github.ambry.replication.ReplicaThread;
import java.util.ArrayList;
import java.util.List;


/**
 *   This class tracks for a current state for {@link com.github.ambry.replication.ReplicaThread.RemoteReplicaGroup}
 *   for continuous replication cycle.
 *
 *   {@link #groupId} will be group id assigned to remote replica group
 *   {@link #remoteReplicaGroup} will be group for which we have created this tracker
 *   {@link #inflightReplicas} will be replicas that are currently present in {@link #remoteReplicaGroup}
 *   {@link #iterations} will be total number of iterations for the group with {@link #groupId} group id
 */
public abstract class GroupTracker {
  private final int groupId;
  private ReplicaThread.RemoteReplicaGroup remoteReplicaGroup;
  private List<ReplicaTracker> inflightReplicas;
  private int iterations;

  GroupTracker(int groupId) {
    this.groupId = groupId;
    this.remoteReplicaGroup = null;
    this.inflightReplicas = new ArrayList<>();
    this.iterations = 0;
  }

  public int getGroupId() {
    return groupId;
  }

  public void startIteration(ReplicaThread.RemoteReplicaGroup remoteReplicaGroup,
      List<ReplicaTracker> inflightReplicas) {
    this.remoteReplicaGroup = remoteReplicaGroup;
    this.inflightReplicas = inflightReplicas;
    this.iterations++;
  }

  public void finishIteration() {
    remoteReplicaGroup = null;
    inflightReplicas.clear();
  }

  public boolean isAvailable() {
    return remoteReplicaGroup == null;
  }
}
