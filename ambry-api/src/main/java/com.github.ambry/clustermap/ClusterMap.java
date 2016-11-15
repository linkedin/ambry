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
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;


/**
 * The ClusterMap provides a high-level interface to {@link DataNodeId}s, {@link PartitionId}s and {@link ReplicaId}s.
 */
public interface ClusterMap {

  /**
   * Gets PartitionId based on serialized bytes.
   * @param stream data input stream that contains the serialized partition bytes
   * @return deserialized PartitionId
   */
  public PartitionId getPartitionIdFromStream(DataInputStream stream) throws IOException;

  /**
   * Gets a list of partitions that are available for writes.
   */
  public List<PartitionId> getWritablePartitionIds();

  /**
   * Checks if datacenter name corresponds to some datacenter in this cluster map's hardware layout.
   *
   * @param datacenterName name of datacenter
   * @return true if datacenter with datacenterName is in the hardware layout of this cluster map.
   */
  public boolean hasDatacenter(String datacenterName);

  /**
   * Gets a specific DataNodeId by its hostname and port.
   *
   * @param hostname of the DataNodeId
   * @param port of the DataNodeId
   * @return DataNodeId for this hostname and port.
   */
  public DataNodeId getDataNodeId(String hostname, int port);

  /**
   * Gets the ReplicaIds stored on the specified DataNodeId.
   *
   * @param dataNodeId
   * @return list of ReplicaIds on the specified dataNodeId
   */
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId);

  /**
   * Gets the DataNodeIds for all nodes in the cluster.
   *
   * @return list of all DataNodeIds
   */
  public List<DataNodeId> getDataNodeIds();

  /**
   * Gets the MetricRegistry that other users of the ClusterMap ought to use for metrics.
   *
   * @return MetricRegistry
   */
  public MetricRegistry getMetricRegistry();

  /**
   * Performs the required action for a replica related event.
   */
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event);
}
