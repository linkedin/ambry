/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import java.util.Collection;
import java.util.List;


/**
 * A callback that needs to be implemented by different implementations of the cluster manager. External components may
 * get resources (i.e. {@link ReplicaId}, {@link PartitionId}, {@link DiskId}, {@link DataNodeId}) via this callback.
 */
interface ClusterManagerCallback<R extends ReplicaId, D extends DiskId, P extends PartitionId, N extends DataNodeId> {
  /**
   * Get all replica ids associated with the given {@link AmbryPartition}
   * @param partition the {@link PartitionId} for which to get the list of replicas.
   * @return the list of {@link ReplicaId}s associated with the given partition.
   */
  List<R> getReplicaIdsForPartition(P partition);

  /**
   * Get replicas of given partition from specified datacenter that are in required state
   * @param partition the {@link PartitionId} for which to get the list of replicas.
   * @param state {@link ReplicaState} associated with replica
   * @param dcName name of datacenter from which the replicas should come
   * @return the list of {@link ReplicaId}s satisfying requirements.
   */
  List<R> getReplicaIdsByState(P partition, ReplicaState state, String dcName);

  /**
   * Get the counter for the sealed state change for partitions.
   * @return the counter for the sealed state change for partitions.
   */
  long getSealedStateChangeCounter();

  /**
   * Get the list of {@link DiskId}s (all or associated with a particular {@link DataNodeId}.
   * @param dataNode if disks of a particular data node is required, {@code null} for all disks.
   * @return a collection of all the disks in this datacenter.
   */
  Collection<D> getDisks(N dataNode);

  /**
   * @return a collection of partitions in this cluster.
   */
  Collection<P> getPartitions();
}
