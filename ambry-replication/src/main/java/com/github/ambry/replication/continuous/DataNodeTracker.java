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
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;
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
   * @param startGroupId group id from which we can start and increment and generate unique group id for each group
   * @param time Ambry time
   * @param replicaThrottleDurationMs throttle duration for replicas. Lane-specific
   *                                  (inter vs intra DC); resolved once by the owning {@link ReplicaThread}
   *                                  and passed in as a primitive so lane logic doesn't leak into this class.
   * @param replicationConfig replication config. Reads
   *                          {@link ReplicationConfig#replicationMaxPartitionCountPerRequest} for the
   *                          per-chunk replica cap and
   *                          {@link ReplicationConfig#replicationSpreadLaggersAcrossChunks} for whether
   *                          the chunking pass sorts and round-robins instead of slicing sequentially.
   * @param lagExtractor function that returns a replica's remote-lag-in-bytes; used as the sort key when
   *                    {@link ReplicationConfig#replicationSpreadLaggersAcrossChunks} is true. Supplied by the
   *                    caller so the chunking helper does not need access to package-private state on
   *                    {@link RemoteReplicaInfo}.
   */
  public DataNodeTracker(DataNodeId dataNodeId, List<RemoteReplicaInfo> remoteReplicas, int startGroupId, Time time,
      long replicaThrottleDurationMs, ReplicationConfig replicationConfig,
      ToLongFunction<RemoteReplicaInfo> lagExtractor) {
    this.dataNodeId = dataNodeId;
    this.activeGroupTrackers = new ArrayList<>();

    int currentGroupId = startGroupId;

    // for this data node break a larger array of remote replicas to smaller multiple arrays
    List<List<RemoteReplicaInfo>> remoteReplicaSegregatedList =
        chunkReplicas(remoteReplicas, replicationConfig.replicationMaxPartitionCountPerRequest,
            replicationConfig.replicationSpreadLaggersAcrossChunks, lagExtractor);

    // for each of smaller array of remote replicas create active group trackers with consecutive group ids
    for (List<RemoteReplicaInfo> remoteReplicaList : remoteReplicaSegregatedList) {
      ActiveGroupTracker activeGroupTracker = new ActiveGroupTracker(currentGroupId, remoteReplicaList.stream()
          .map(remoteReplicaInfo -> new ReplicaTracker(remoteReplicaInfo, time, replicaThrottleDurationMs))
          .collect(Collectors.toList()));
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

  public List<GroupTracker> getGroupTrackers() {
    List<GroupTracker> groupTrackers = new ArrayList<>(getActiveGroupTrackers());
    groupTrackers.add(getStandByGroupTracker());
    return groupTrackers;
  }

  public List<ActiveGroupTracker> getActiveGroupTrackers() {
    return activeGroupTrackers;
  }

  public StandByGroupTracker getStandByGroupTracker() {
    return standByGroupTracker;
  }

  /**
   * @return returns remote replica groups that are getting tracked in group trackers of
   * this data node tracker
   */
  public List<ReplicaThread.RemoteReplicaGroup> getInflightRemoteReplicaGroups() {
    return getGroupTrackers().stream()
        .filter(GroupTracker::isInFlight)
        .map(GroupTracker::getRemoteReplicaGroup)
        .collect(Collectors.toList());
  }

  /**
   * Iterates over all group trackers and if remote replica group is getting tracked by group tracker is done,
   * removes it from group tracker
   * @return returns list of remote replica groups removed from tracking
   */
  public List<ReplicaThread.RemoteReplicaGroup> processFinishedGroups() {
    List<ReplicaThread.RemoteReplicaGroup> finishedGroups = new ArrayList<>();
    getGroupTrackers().forEach(groupTracker -> {
      if (!groupTracker.isGroupDone()) {
        return;
      }
      finishedGroups.add(groupTracker.getRemoteReplicaGroup());
      groupTracker.finishIteration();
    });
    return finishedGroups;
  }

  /**
   * @return Iterates over all group trackers and returns maximum iterations done by any group
   */
  public int getMaxIterationAcrossGroups() {
    return getGroupTrackers().stream().map(GroupTracker::getIterations).max(Comparator.naturalOrder()).orElse(0);
  }

  /**
   * @return iterates over all active group trackers and returns trackers which are not tracking
   * any remote replica group
   */
  public List<ActiveGroupTracker> getFinishedActiveGroupTrackers() {
    return getActiveGroupTrackers().stream()
        .filter(groupTracker -> !groupTracker.isInFlight())
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "DataNodeTracker :[" + dataNodeId.toString() + " " + activeGroupTrackers.toString() + " "
        + standByGroupTracker.toString() + "]";
  }

  /**
   * Splits {@code remoteReplicas} into chunks of at most {@code maxActiveGroupSize}. When {@code spread}
   * is false, replicas are sliced sequentially via {@link Utils#partitionList}. When true, replicas are
   * first sorted by {@code lagExtractor} descending, then round-robin distributed across chunks so that
   * the top laggers land in different chunks. When {@code maxActiveGroupSize <= 0}, returns a single
   * chunk containing all replicas.
   *
   * Package-private for test access; do not call from outside {@link DataNodeTracker}.
   */
  static List<List<RemoteReplicaInfo>> chunkReplicas(List<RemoteReplicaInfo> remoteReplicas, int maxActiveGroupSize,
      boolean spread, ToLongFunction<RemoteReplicaInfo> lagExtractor) {
    if (maxActiveGroupSize <= 0) {
      return Collections.singletonList(remoteReplicas);
    }
    if (!spread || remoteReplicas.isEmpty()) {
      return Utils.partitionList(remoteReplicas, maxActiveGroupSize);
    }
    List<RemoteReplicaInfo> sorted = new ArrayList<>(remoteReplicas);
    // Snapshot lag once per replica before sorting. Re-reading inside the comparator is unsafe:
    // the underlying field is mutated by other threads and can trip TimSort's contract check.
    // IdentityHashMap because RemoteReplicaInfo overrides equals() without hashCode().
    Map<RemoteReplicaInfo, Long> lagSnapshot = new IdentityHashMap<>(sorted.size());
    for (RemoteReplicaInfo r : sorted) {
      lagSnapshot.put(r, lagExtractor.applyAsLong(r));
    }
    sorted.sort((a, b) -> Long.compare(lagSnapshot.get(b), lagSnapshot.get(a)));
    int chunkCount = (sorted.size() + maxActiveGroupSize - 1) / maxActiveGroupSize;
    List<List<RemoteReplicaInfo>> chunks = new ArrayList<>(chunkCount);
    for (int chunk = 0; chunk < chunkCount; chunk++) {
      List<RemoteReplicaInfo> bucket = new ArrayList<>(maxActiveGroupSize);
      for (int j = chunk; j < sorted.size(); j += chunkCount) {
        bucket.add(sorted.get(j));
      }
      chunks.add(bucket);
    }
    return chunks;
  }
}
