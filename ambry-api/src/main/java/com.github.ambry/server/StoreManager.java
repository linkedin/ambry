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

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.store.Store;
import java.util.List;


public interface StoreManager {

  /**
   * Add a new BlobStore with given {@link ReplicaId}.
   * @param replica the {@link ReplicaId} of the {@link Store} which would be added.
   * @return {@code true} if adding store was successful. {@code false} if not.
   */
  public boolean addBlobStore(ReplicaId replica);

  /**
   * Remove store from storage manager.
   * @param id the {@link PartitionId} associated with store
   * @return {@code true} if removal succeeds. {@code false} otherwise.
   */
  public boolean removeBlobStore(PartitionId id);

  /**
   * Start BlobStore with given {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link Store} which would be started.
   */
  public boolean startBlobStore(PartitionId id);

  /**
   * Shutdown BlobStore with given {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link Store} which would be shutdown.
   */
  public boolean shutdownBlobStore(PartitionId id);

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @return the {@link Store} corresponding to the given {@link PartitionId}, or {@code null} if no store was found for
   *         that partition, or that store was not started.
   */
  public Store getStore(PartitionId id);

  /**
   * Set BlobStore Stopped state with given {@link PartitionId} {@code id}.
   * @param partitionIds a list {@link PartitionId} of the {@link Store} whose stopped state should be set.
   * @param markStop whether to mark BlobStore as stopped ({@code true}) or started.
   * @return a list of {@link PartitionId} whose stopped state fails to be updated.
   */
  public List<PartitionId> setBlobStoreStoppedState(List<PartitionId> partitionIds, boolean markStop);

  /**
   * Check if a certain disk is available.
   * @param disk the {@link DiskId} to check.
   * @return {@code true} if the disk is available. {@code false} if not.
   */
  public boolean isDiskAvailable(DiskId disk);//todo fix this

  /**
   * Schedules the {@link PartitionId} {@code id} for compaction next.
   * @param id the {@link PartitionId} of the {@link Store} to compact.
   * @return {@code true} if the scheduling was successful. {@code false} if not.
   */
  public boolean scheduleNextForCompaction(PartitionId id);

  /**
   * Disable compaction on the {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link Store} on which compaction is disabled or enabled.
   * @param enabled whether to enable ({@code true}) or disable.
   * @return {@code true} if disabling was successful. {@code false} if not.
   */
  public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled);
}
