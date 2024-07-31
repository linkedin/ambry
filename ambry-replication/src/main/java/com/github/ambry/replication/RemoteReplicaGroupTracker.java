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
package com.github.ambry.replication;

import java.util.ArrayList;
import java.util.List;


abstract class RemoteReplicaGroupTracker {
  private ReplicaThread.RemoteReplicaGroup replicaGroup;
  private List<ReplicaTracker> inflightReplicas;
  private int iterationCount = 0;
  private int id;

  RemoteReplicaGroupTracker(int id) {
    replicaGroup = null;
    inflightReplicas = new ArrayList<>();
    this.id = id;
  }

  int getId() {
    return id;
  }

  ReplicaThread.RemoteReplicaGroup getRemoteReplicaGroup() {
    return replicaGroup;
  }

  RemoteReplicaGroupTracker setReplicaGroup(ReplicaThread.RemoteReplicaGroup replicaGroup,
      List<ReplicaTracker> inflightReplicas) {
    this.replicaGroup = replicaGroup;
    this.inflightReplicas = inflightReplicas;
    iterationCount++;
    return this;
  }

  boolean isFree() {
    return replicaGroup == null;
  }

  void free( ){
    replicaGroup = null;
  }

  boolean isPresentAndFinished() {
    return replicaGroup != null && replicaGroup.isDone();
  }

  List<ReplicaTracker> getInflightReplicas(){
    return inflightReplicas;
  }


}

class ActiveGroupTracker extends RemoteReplicaGroupTracker {
  private final List<ReplicaTracker> preAssignedReplicas;

  ActiveGroupTracker(int i, List<ReplicaTracker> preAssignedReplicas) {
    super(i);
    this.preAssignedReplicas = preAssignedReplicas;
  }

  List<ReplicaTracker> getPreAssignedReplicas(){
    return preAssignedReplicas;
  }

}

class StandByGroupTracker extends RemoteReplicaGroupTracker {
  StandByGroupTracker(int i) {
    super(i);
  }
}