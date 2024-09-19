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
 *   {@link #groupId} group id assigned to remote replica group
 *   {@link #remoteReplicaGroup} group for which we have created this tracker
 *   {@link #inflightReplicaTrackers} replicas that are currently present in {@link #remoteReplicaGroup}
 *   {@link #iterations} total number of iterations for the remote replica group with {@link #groupId} group id
 */
public abstract class GroupTracker {
  private final int groupId;
  private ReplicaThread.RemoteReplicaGroup remoteReplicaGroup;
  private List<ReplicaTracker> inflightReplicaTrackers;
  private int iterations;

  GroupTracker(int groupId) {
    this.groupId = groupId;
    this.remoteReplicaGroup = null;
    this.inflightReplicaTrackers = new ArrayList<>();
    this.iterations = 0;
  }

  public int getGroupId() {
    return groupId;
  }

  public ReplicaThread.RemoteReplicaGroup getRemoteReplicaGroup() {
    return remoteReplicaGroup;
  }

  public List<ReplicaTracker> getInflightReplicaTrackers() {
    return inflightReplicaTrackers;
  }

  public int getIterations() {
    return iterations;
  }

  /**
   * This method should be called, when a new remote replica group
   * @param remoteReplicaGroup Remote replica group created for this tracker
   * @param inflightReplicas replica trackers for replicas included in the remoteReplicaGroup
   */
  public void startIteration(ReplicaThread.RemoteReplicaGroup remoteReplicaGroup,
      List<ReplicaTracker> inflightReplicas) {
    this.remoteReplicaGroup = remoteReplicaGroup;
    this.inflightReplicaTrackers = inflightReplicas;
    this.iterations++;
  }

  /**
   * removes remote replica group from tracking, finishes iterations for inflight replicas
   * and removes from tracking afterward
   */
  public void finishIteration() {
    remoteReplicaGroup = null;

    inflightReplicaTrackers.forEach(ReplicaTracker::finishIteration);
    inflightReplicaTrackers.clear();
  }

  /**
   * If remoteReplicaGroup is null, only then we consider group is not getting tracked
   * @return returns true if group is getting tracked, false otherwise
   */
  public boolean isInFlight() {
    return remoteReplicaGroup != null;
  }

  /**
   * @return checks and returns true if group is getting tracked and in done state, false otherwise
   */
  public boolean isGroupDone() {
    return isInFlight() && remoteReplicaGroup.isDone();
  }

  @Override
  public String toString() {
    return "GroupTracker: [" + groupId + " " + inflightReplicaTrackers.toString() + " " + iterations + "]";
  }
}
