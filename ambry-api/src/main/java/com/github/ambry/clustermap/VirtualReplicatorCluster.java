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
package com.github.ambry.clustermap;

import java.util.Collection;
import java.util.List;


/**
 * The {@link VirtualReplicatorCluster} provides a high-level interface to Virtual Replicator Cluster.
 * In Virtual Replicator Cluster, {@link PartitionId}s are resources and they are assigned to participant node.
 */
public interface VirtualReplicatorCluster extends AutoCloseable {
  String Cloud_Replica_Keyword = "vcr";

  /**
   * Gets all nodes in the cluster.
   * @return list of PartitionId
   */
  List<? extends DataNodeId> getAllDataNodeIds();

  /**
   * Gets {@link DataNodeId} representation of current node.
   * @return DataNodeId.
   */
  DataNodeId getCurrentDataNodeId();

  /**
   * Join the cluster as a participant.
   */
  void participate() throws Exception;

  /**
   * Gets all {@link PartitionId}s assigned to current node.
   * @return {@link Collection} of PartitionId
   */
  Collection<? extends PartitionId> getAssignedPartitionIds();

  /**
   * Check is a partition is assigned to current node.
   * @param partitionPath partition id of the partition.
   * @return {@code true} if partition is assigned to current node. {@code false} otherwise.
   */
  boolean isPartitionAssigned(String partitionPath);

  /**
   * Add {@link VirtualReplicatorClusterListener} to listen for cluster change.
   * @param listener to add.
   */
  void addListener(VirtualReplicatorClusterListener listener);
}
