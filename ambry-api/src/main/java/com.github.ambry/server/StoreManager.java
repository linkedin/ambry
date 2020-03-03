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
package com.github.ambry.server;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.store.Store;
import java.util.Collection;
import java.util.List;


/**
 * High level interface for handling and managing blob stores.
 */
public interface StoreManager {

  /**
   * Add a new BlobStore with given {@link ReplicaId}.
   * @param replica the {@link ReplicaId} of the {@link Store} which would be added.
   * @return {@code true} if adding store was successful. {@code false} if not.
   */
  boolean addBlobStore(ReplicaId replica);

  /**
   * Remove store from storage manager.
   * @param id the {@link PartitionId} associated with store
   * @return {@code true} if removal succeeds. {@code false} otherwise.
   */
  boolean removeBlobStore(PartitionId id);

  /**
   * Start BlobStore with given {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link Store} which would be started.
   * @return true if successfully started, false otherwise.
   */
  boolean startBlobStore(PartitionId id);

  /**
   * Shutdown BlobStore with given {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link Store} which would be shutdown.
   * @return true if successfully shutdown, false otherwise.
   */
  boolean shutdownBlobStore(PartitionId id);

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @return the {@link Store} corresponding to the given {@link PartitionId}, or {@code null} if no store was found for
   *         that partition, or that store was not started.
   */
  Store getStore(PartitionId id);

  /**
   * Get replicaId on current node by partition name. (There should be at most one replica belonging to specific
   * partition on single node)
   * @param partitionName name of {@link PartitionId}
   * @return {@link ReplicaId} associated with given partition name. {@code null} if replica is not found in storage manager.
   */
  ReplicaId getReplica(String partitionName);

  /**
   * Get all partitions that are managed by {@link StoreManager} on local node.
   * @return a collection of {@link PartitionId} on local node.
   */
  Collection<PartitionId> getLocalPartitions();

  /**
   * Set BlobStore Stopped state with given {@link PartitionId} {@code id}.
   * @param partitionIds a list {@link PartitionId} of the {@link Store} whose stopped state should be set.
   * @param markStop whether to mark BlobStore as stopped ({@code true}) or started.
   * @return a list of {@link PartitionId} whose stopped state fails to be updated.
   */
  List<PartitionId> setBlobStoreStoppedState(List<PartitionId> partitionIds, boolean markStop);

  /**
   * Check if a certain partition is available locally.
   * @param partition the {@link PartitionId} to check.
   * @param localReplica {@link ReplicaId} of local replica of the partition {@code PartitionId}.
   * @return {@code true} if the partition is available. {@code false} if not.
   */
  ServerErrorCode checkLocalPartitionStatus(PartitionId partition, ReplicaId localReplica);

  /**
   * Schedules the {@link PartitionId} {@code id} for compaction next.
   * @param id the {@link PartitionId} of the {@link Store} to compact.
   * @return {@code true} if the scheduling was successful. {@code false} if not.
   */
  boolean scheduleNextForCompaction(PartitionId id);

  /**
   * Disable compaction on the {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link Store} on which compaction is disabled or enabled.
   * @param enabled whether to enable ({@code true}) or disable.
   * @return {@code true} if disabling was successful. {@code false} if not.
   */
  boolean controlCompactionForBlobStore(PartitionId id, boolean enabled);
}
