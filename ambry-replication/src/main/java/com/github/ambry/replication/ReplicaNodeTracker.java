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

import com.github.ambry.clustermap.DataNodeId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class ReplicaNodeTracker {
  DataNodeId nodeId;
  HashSet<RemoteReplicaInfo> activePool; // Holds active replicas ready to replicate
  HashSet<RemoteReplicaInfo> inactivePool; // Holds standby and offline/backoff replicas
  ArrayList<ReplicaGroupTracker> inFlight; // Tracks inflight groups

  public ReplicaNodeTracker(DataNodeId nodeId) {}
  public DataNodeId getNodeId() {
    return nodeId;
  }
  public void addInFlightGroup(ReplicaGroupTracker tracker) {}
  public ArrayList<ReplicaGroupTracker> getInFlightGroups() { return inFlight; }
  public void moveToActivePool(List<RemoteReplicaInfo> active) { /* add to active; rm from inactive */ }
  public void moveToInactivePool(List<RemoteReplicaInfo> inactive) { /* add to inactive; rm from active */ }
  public HashSet<RemoteReplicaInfo> getActivePool() { return activePool; }
  public HashSet<RemoteReplicaInfo> getInactivePool() { return inactivePool; }
}
