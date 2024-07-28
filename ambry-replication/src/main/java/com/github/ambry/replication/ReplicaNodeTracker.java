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
  HashSet<RemoteReplicaInfo> activePool;
  HashSet<RemoteReplicaInfo> timedOutPool;
  ArrayList<ReplicaGroupTracker> inFlight;

  public ReplicaNodeTracker(DataNodeId nodeId) {}
  public DataNodeId getNodeId() {
    return nodeId;
  }
  public void addInFlightGroup(ReplicaGroupTracker tracker) {}
  public ArrayList<ReplicaGroupTracker> getInFlightGroups() {
    return inFlight;
  }
  public void addToActivePool(List<RemoteReplicaInfo> active) {}
  public void addToTimedOutPool(List<RemoteReplicaInfo> timeo) {}
  public HashSet<RemoteReplicaInfo> getActivePool() {
    return activePool;
  }
  public HashSet<RemoteReplicaInfo> getTimedOutPool() {
    return timedOutPool;
  }
}
