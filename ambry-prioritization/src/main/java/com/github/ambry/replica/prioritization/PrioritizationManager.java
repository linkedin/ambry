/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.Disk;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The PrioritizationManager is responsible for managing the prioritization of replicas for replication.
 */
public interface PrioritizationManager {
  /**
   * Start the PrioritizationManager.
   */
  void start();

  /**
   * Shutdown the PrioritizationManager.
   */
  void shutdown();

  /**
   * Checks status of Prioritization manager.
   * @return true if the PrioritizationManager is running, false otherwise.
   */
  boolean isRunning();

  /**
   * Get the list of partitions that should be replicated from the given disk.
   * @param diskId the {@link DiskId} for which the list of partitions should be replicated.
   * @param numberOfReplicasPerDisk the number of replicas that should be replicated from the given disk.
   * @return the list of {@link ReplicaId} that should be replicated from the given disk.
   */
  List<ReplicaId> getPartitionListForDisk(DiskId diskId, int numberOfReplicasPerDisk);

  /**
   * Get the list of partitions that are in progress for the given disk.
   * @param diskId the {@link DiskId} for which the list of partitions are in progress.
   * @return the list of {@link ReplicaId} that are in progress for the given disk.
   */
  List<ReplicaId> getInProgressReplicaIdsForDisk(DiskId diskId);


  /**
   * Add a replica to the prioritization manager.
   * @param replicaId the {@link ReplicaId} to add.
   */
  boolean addReplica(ReplicaId replicaId);

  /**
   * Remove the list of replicas that have finished replication.
   * @param diskId the {@link DiskId} that the replicas are on.
   * @param replicaId the {@link ReplicaId} to remove.
   * @return {@code true} if the replica was removed, {@code false} otherwise.
   */
  boolean removeReplica(DiskId diskId, ReplicaId replicaId);

  /**
   * Remove the list of replicas that are in progress.
   * @param diskId the {@link DiskId} that the replicas are on.
   * @param replicaId the {@link ReplicaId} to remove.
   * @return {@code true} if the replica was removed, {@code false} otherwise.
   */
  boolean removeInProgressReplica(DiskId diskId, ReplicaId replicaId);

}