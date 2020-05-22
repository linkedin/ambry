/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.server.StoreManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * Maintains the list of leader partitions on local node and their corresponding peer leaders in remote data centers
 */
public class PartitionLeaderInfo {

  private final Map<String, Set<ReplicaId>> peerLeaderReplicasByPartition = new ConcurrentHashMap<>();
  private final StoreManager storeManager;
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private static final Logger logger = LoggerFactory.getLogger(PartitionLeaderInfo.class);

  public PartitionLeaderInfo(StoreManager storeManager) {
    this.storeManager = storeManager;
  }

  /**
   * Get a map of partitions to their sets of peer leader replicas (this method is only by ReplicationTest for now)
   * @return an unmodifiable map of peer leader replicas stored by partition {@link PartitionLeaderInfo#peerLeaderReplicasByPartition}
   */
  public Map<String, Set<ReplicaId>> getPeerLeaderReplicasByPartition() {
    return Collections.unmodifiableMap(peerLeaderReplicasByPartition);
  }

  /**
   * Add a leader partition and its set of peer leader replicas. This method is thread safe.
   * @param partitionName name of the partition to be added
   */
  public void addPartition(String partitionName) {

    // 1. get local replica from store manager
    ReplicaId localReplica = storeManager.getReplica(partitionName);

    // 2. Get the peer leader replicas from all data centers for this partition
    List<? extends ReplicaId> leaderReplicas =
        localReplica.getPartitionId().getReplicaIdsByState(ReplicaState.LEADER, null);

    // 3. Log the list of leader replicas associated with this partition (will be used later for leadership based replication)
    List<ReplicaId> peerLeaderReplicas = new ArrayList<>();
    for (ReplicaId leaderReplica : leaderReplicas) {
      if (leaderReplica.getDataNodeId() != localReplica.getDataNodeId()) {
        peerLeaderReplicas.add(leaderReplica);
        logger.info("Partition {} on node instance {} is leader in remote dc {}", partitionName,
            getInstanceName(leaderReplica.getDataNodeId().getHostname(), leaderReplica.getDataNodeId().getPort()),
            leaderReplica.getDataNodeId().getDatacenterName());
      }
    }

    // Read-write lock avoids contention from threads removing old leader partitions (removePartition()) and threads updating existing leader partitions (refreshPeerLeadersForAllPartitions())
    rwLock.writeLock().lock();
    try {
      peerLeaderReplicasByPartition.put(partitionName, new HashSet<>(peerLeaderReplicas));
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Remove a partition from the map of leader partitions. This method is thread safe.
   * @param partitionName name of the partition to be removed
   */
  public void removePartition(String partitionName) {
    // Read-write lock avoids contention from threads adding new leaders (addPartition()) and threads updating existing leader partitions (refreshPeerLeadersForAllPartitions())
    rwLock.writeLock().lock();
    try {
      peerLeaderReplicasByPartition.remove(partitionName);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Refreshes the list of remote leaders for all leader partitions by querying the latest information from RoutingTableSnapshots of all data centers.
   * This method is thread safe.
   */
  public void refreshPeerLeadersForAllPartitions() {

    // Read-write lock usage: Avoids contention between threads doing the following activities:
    // 1. Adding new leaders (in addPeerLeadersByPartition())
    // 2. Removing old leaders (in removePartition())
    // 3. Refreshing remote leader set for existing leaders (current method).
    // Explanation for point 3: Multiple threads from different cluster change handlers (we have one cluster change handler for each DC) can trigger onRoutingTableUpdate() in parallel which calls this method to refresh leader partitions.
    // We need to make sure that the sequence of gathering remote leaders (from RoutingTableSnapshot of each DC) and updating the map is an atomic operation.

    rwLock.writeLock().lock();
    try {
      for (Map.Entry<String, Set<ReplicaId>> entry : peerLeaderReplicasByPartition.entrySet()) {
        String partitionName = entry.getKey();
        ReplicaId localLeaderReplica = storeManager.getReplica(partitionName);
        PartitionId partition = localLeaderReplica.getPartitionId();
        Set<ReplicaId> previousRemoteLeaderReplicas = entry.getValue();
        Set<ReplicaId> currentRemoteLeaderReplicas =
            new HashSet<>(partition.getReplicaIdsByState(ReplicaState.LEADER, null));
        currentRemoteLeaderReplicas.remove(localLeaderReplica);
        if (!previousRemoteLeaderReplicas.equals(currentRemoteLeaderReplicas)) {
          peerLeaderReplicasByPartition.put(partitionName, currentRemoteLeaderReplicas);
        }
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Checks if a remote replica is a leader for a partition (Pre-requisite: the partition itself should be a leader locally).
   * @param partitionName name of local leader partition
   * @param replicaId remote replica to be checked
   * @return true if remote replica is a leader for a partition (Pre-requisite: the partition itself should be a leader locally).
   */
  public boolean isPeerReplicaLeaderForPartition(String partitionName, ReplicaId replicaId) {
    rwLock.readLock().lock();
    try {
      return peerLeaderReplicasByPartition.containsKey(partitionName) && peerLeaderReplicasByPartition.get(
          partitionName).contains(replicaId);
    } finally {
      rwLock.readLock().unlock();
    }
  }
}
