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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


/**
 * This class tracks for a current state for a remote host for a continuous replication cycle.
 * Each DataNode has multiple active group trackers.
 * Each DataNode has only one Standby group for reducing cross colo calls.
 * {@link #activeGroupTrackers} has group trackers for groups with active replicas
 * {@link #standByGroupTracker} has group tracker for stand by group for this data node
 */
public class DataNodeTracker {
  private final DataNodeId dataNodeId;
  private final List<ActiveGroupTracker> activeGroupTrackers;
  private final StandByGroupTracker standByGroupTracker;

  /**
   * Creating active group trackers and standByGroupTracker.
   * All group trackers have consecutive group id.
   * @param dataNodeId remote host for which to create datanode tracker
   * @param remoteReplicas remote replicas for this data node
   * @param maxActiveGroupSize maximum count of replicas in active groups
   * @param startGroupId group id from which we can start and increment and generate unique group id for each group
   */
  public DataNodeTracker(DataNodeId dataNodeId, List<RemoteReplicaInfo> remoteReplicas, int maxActiveGroupSize,
      int startGroupId) {
    this.dataNodeId = dataNodeId;
    this.activeGroupTrackers = new ArrayList<>();

    int currentGroupId = startGroupId;

    // for this data node break a larger array of remote replicas to smaller multiple arrays of maxActiveGroupSize
    List<List<RemoteReplicaInfo>> remoteReplicaSegregatedList =
        maxActiveGroupSize > 0 ? Utils.partitionList(remoteReplicas, maxActiveGroupSize)
            : Collections.singletonList(remoteReplicas);

    // for each of smaller array of remote replicas create active group trackers with consecutive group ids
    for (List<RemoteReplicaInfo> remoteReplicaList : remoteReplicaSegregatedList) {
      ActiveGroupTracker activeGroupTracker = new ActiveGroupTracker(currentGroupId,
          remoteReplicaList.stream().map(ReplicaTracker::new).collect(Collectors.toList()));
      activeGroupTrackers.add(activeGroupTracker);
      currentGroupId++;
    }

    // standby group id has maximum group id
    standByGroupTracker = new StandByGroupTracker(currentGroupId);
  }

  /**
   * @return getting maximum group id for this data node
   */
  public int getMaxGroupId() {
    return standByGroupTracker.getGroupId();
  }

  public DataNodeId getDataNodeId() {
    return dataNodeId;
  }

  public List<ActiveGroupTracker> getActiveGroupTrackers() {
    return activeGroupTrackers;
  }

  public StandByGroupTracker getStandByGroupTracker() {
    return standByGroupTracker;
  }
}
