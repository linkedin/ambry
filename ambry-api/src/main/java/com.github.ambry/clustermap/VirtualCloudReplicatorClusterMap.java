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

import com.codahale.metrics.MetricRegistry;
import java.util.List;
import org.json.JSONObject;


/**
 * The {@link VirtualCloudReplicatorClusterMap} provides a high-level interface to VCR Cluster backed by Helix.
 * In VCR Cluster, {@link PartitionId}s are Helix resources and VCR hosts are Helix participants.
 */
public interface VirtualCloudReplicatorClusterMap extends AutoCloseable {

  /**
   * Gets the id of the local datacenter.
   * @return The id of the local datacenter.
   */
  byte getLocalDatacenterId();

  /**
   * Gets the name of the datacenter from the ID.
   *
   * @param id the ID of the datacenter
   * @return name of the datacenter from the ID or null if not found.
   */
  String getDatacenterName(byte id);

  /**
   * Gets a specific DataNodeId by its hostname and port.
   *
   * @param hostname of the DataNodeId
   * @param port of the DataNodeId
   * @return DataNodeId for this hostname and port.
   */
  DataNodeId getDataNodeId(String hostname, int port);

  /**
   * Gets the PartitionIds backing up by specified DataNodeId.
   *
   * @param dataNodeId the {@link DataNodeId} whose replicas are to be returned.
   * @return list of ReplicaIds on the specified dataNodeId
   */
  List<? extends PartitionId> getPartitionIds(DataNodeId dataNodeId);

  /**
   * Gets the DataNodeIds for all nodes in the cluster.
   *
   * @return list of all DataNodeIds
   */
  List<? extends DataNodeId> getDataNodeIds();

  /**
   * Gets the MetricRegistry that other users of the ClusterMap ought to use for metrics.
   *
   * @return MetricRegistry
   */
  MetricRegistry getMetricRegistry();

  /**
   * Performs the required action for a replica related event.
   */
  void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event);

  /**
   * @return a snapshot which includes information that the implementation considers relevant. Must necessarily contain
   * information about nodes, partitions and replicas from each datacenter
   */
  JSONObject getSnapshot();
}
