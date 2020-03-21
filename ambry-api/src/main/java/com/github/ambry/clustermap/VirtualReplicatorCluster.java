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

import java.util.List;
import org.apache.helix.InstanceType;


/**
 * The {@link VirtualReplicatorCluster} provides a high-level interface to Virtual Replicator Cluster.
 * In Virtual Replicator Cluster, {@link PartitionId}s are resources and they are assigned to participant node.
 */
public interface VirtualReplicatorCluster extends AutoCloseable {

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
   * Join the cluster as {@link InstanceType#PARTICIPANT} or {@link InstanceType#SPECTATOR} .
   * @param role the {@link InstanceType}.
   */
  void participate(InstanceType role) throws Exception;

  /**
   * Gets all {@link PartitionId}s assigned to current node.
   * @return list of PartitionId
   */
  List<? extends PartitionId> getAssignedPartitionIds();

  /**
   * Add {@link VirtualReplicatorClusterListener} to listen for cluster change.
   * @param listener to add.
   */
  void addListener(VirtualReplicatorClusterListener listener);
}
