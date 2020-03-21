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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.json.JSONObject;


/**
 * The ClusterMap provides a high-level interface to {@link DataNodeId}s, {@link PartitionId}s and {@link ReplicaId}s.
 */
public interface ClusterMap extends AutoCloseable {

  /**
   * Gets PartitionId based on serialized bytes.
   * @param stream {@link InputStream} that contains the serialized partition bytes
   * @return deserialized PartitionId
   */
  PartitionId getPartitionIdFromStream(InputStream stream) throws IOException;

  /**
   * Gets a list of partitions that are available for writes. Gets a mutable shallow copy of the list of the partitions
   * that are available for writes
   * @param partitionClass the partition class whose writable partitions are required. Can be {@code null}
   * @return a list of all writable partitions belonging to the partition class (or all writable partitions if
   * {@code partitionClass} is {@code null})
   */
  List<? extends PartitionId> getWritablePartitionIds(String partitionClass);

  /**
   * Get a writable partition chosen at random that belongs to given partitionclass.
   * @param partitionClass the partition class whose writable partitions are required. Can be {@code null}
   * @param partitionsToExclude list of partitions that shouldnt be considered as a possible partition returned
   * @return chosen random partition. Can be {@code null}
   */
  PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude);

  /**
   * Gets a list of all partitions in the cluster.  Gets a mutable shallow copy of the list of all partitions.
   * @param partitionClass the partition class whose partitions are required. Can be {@code null}
   * @return a list of all partitions belonging to the partition class (or all partitions if {@code partitionClass} is
   * {@code null})
   */
  List<? extends PartitionId> getAllPartitionIds(String partitionClass);

  /**
   * Checks if datacenter name corresponds to some datacenter in this cluster map's hardware layout.
   *
   * @param datacenterName name of datacenter
   * @return true if datacenter with datacenterName is in the hardware layout of this cluster map.
   */
  boolean hasDatacenter(String datacenterName);

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
   * @return DataNodeId for this hostname and port, or {@code null} if the hostname and port does not match a node in
   *         the cluster.
   */
  DataNodeId getDataNodeId(String hostname, int port);

  /**
   * Gets the ReplicaIds stored on the specified DataNodeId.
   *
   * @param dataNodeId the {@link DataNodeId} whose replicas are to be returned.
   * @return list of ReplicaIds on the specified dataNodeId
   */
  List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId);

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

  /**
   * Attempt to get a bootstrap replica of certain partition that is supposed to be added onto specified data node.
   * This method is designed to fetch detailed infos about bootstrap replica and create an instance of this replica. The
   * purpose is to support dynamically adding new replica to specified data node.
   * @param partitionIdStr the partition id string
   * @param dataNodeId the {@link DataNodeId} on which bootstrap replica is placed
   * @return {@link ReplicaId} if there is a new replica satisfying given partition and data node. {@code null} otherwise.
   */
  ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId);

  /**
   * Register a listener of cluster map for any changes.
   * @param clusterMapChangeListener the {@link ClusterMapChangeListener} to add.
   */
  void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener);

  /**
   * Close the cluster map. Any cleanups should be done in this call.
   */
  @Override
  void close();
}
