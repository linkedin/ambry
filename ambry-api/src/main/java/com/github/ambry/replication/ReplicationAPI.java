/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.store.StoreException;
import java.util.Collection;
import java.util.List;


/**
 * This defines the API implemented by Ambry replication nodes.
 */
public interface ReplicationAPI {

  /**
   * Enables/disables replication of the given {@code ids} from {@code origins}. The disabling is in-memory and
   * therefore is not valid across restarts.
   * @param ids the {@link PartitionId}s to enable/disable it on.
   * @param origins the list of datacenters from which replication should be enabled/disabled. Having an empty list
   *                disables replication from all datacenters.
   * @param enable whether to enable ({@code true}) or disable.
   * @return {@code true} if disabling succeeded, {@code false} otherwise. Disabling fails if {@code origins} is empty
   * or contains unrecognized datacenters.
   */
  boolean controlReplicationForPartitions(Collection<PartitionId> ids, List<String> origins, boolean enable);

  /**
   * Updates the total bytes read by a remote replica from local store
   * @param partitionId PartitionId to which the replica belongs to
   * @param hostName HostName of the datanode where the replica belongs to
   * @param replicaPath Replica Path of the replica interested in
   * @param totalBytesRead Total bytes read by the replica
   * @throws StoreException
   */
  void updateTotalBytesReadByRemoteReplica(PartitionId partitionId, String hostName, String replicaPath,
      long totalBytesRead) throws StoreException;

  /**
   * Gets the replica lag of the remote replica with the local store
   * @param partitionId The partition to which the remote replica belongs to
   * @param hostName The hostname where the remote replica is present
   * @param replicaPath The path of the remote replica on the host
   * @return The lag in bytes that the remote replica is behind the local store
   */
  long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath);

  /**
   * Boost the per-cycle fetch budget for the listed partitions by {@code boost} on every replica
   * thread on this server. Implementations that do not support a priority API should leave the
   * default that throws {@link UnsupportedOperationException}.
   * @param partitions partitions to prioritize
   * @param boost positive boost factor
   */
  default void prioritizePartitions(List<PartitionId> partitions, int boost) {
    throw new UnsupportedOperationException(
        "Priority API not supported on " + getClass().getName());
  }

  /**
   * Remove priority entries for the listed partitions on every replica thread on this server.
   * An empty list wipes ALL priority entries — that branch is only reachable from a handler
   * that has explicitly validated the operator's wipe-all intent (otherwise we'd risk silent
   * wipe-all from an operator typo).
   * @param partitions partitions to unset; empty list ⇒ wipe all
   */
  default void unsetPriorityPartitions(List<PartitionId> partitions) {
    throw new UnsupportedOperationException(
        "Priority API not supported on " + getClass().getName());
  }

  /**
   * @param partition the partition to look up
   * @return true if this server is registered to replicate {@code partition}.
   */
  default boolean hostsPartition(PartitionId partition) {
    throw new UnsupportedOperationException(
        "Priority API not supported on " + getClass().getName());
  }

  /**
   * @return a deterministically-ordered snapshot of priority entries across every
   *         {@code ReplicaThread} on this server, one entry per (partition, thread) pair, tagged
   *         with the thread's {@code isInterColo} flag. Implementations without a priority API
   *         leave the default that throws {@link UnsupportedOperationException}.
   */
  default List<PriorityEntry> listAllPriorityPartitions() {
    throw new UnsupportedOperationException(
        "Priority API not supported on " + getClass().getName());
  }
}
