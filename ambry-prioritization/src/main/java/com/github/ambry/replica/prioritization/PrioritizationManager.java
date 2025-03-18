/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
public interface PrioritizationManager {

  void start();

  void shutdown();

  boolean isRunning();

  List<ReplicaId> getPartitionListForDisk(DiskId diskId, int numberOfReplicasPerDisk);

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
   * Returns the number of disks that are currently in the prioritization manager.
   * @return the number of disks.
   */
  int getNumberOfDisks(int numberOfDisks);
}
