/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A cluster manager that is a composed of a {@link StaticClusterManager} and a {@link HelixClusterManager}.
 * It provides a merged view of the static and helix clusters.
 */
public class RecoveryTestClusterManager implements ClusterMap {
  private final Logger logger = LoggerFactory.getLogger(RecoveryTestClusterManager.class);
  final StaticClusterManager staticClusterManager;
  final HelixClusterManager helixClusterManager;
  final Map<AmbryDisk, Disk> ambryDiskToDiskMap;
  final Map<AmbryDataNode, DataNode> ambryDataNodeToDataNodeMap;

  /**
   * Construct a RecoveryTestClusterManager instance.
   * @param staticClusterManager the {@link StaticClusterManager} object.
   * @param helixClusterManager the {@link HelixClusterManager} object.
   */
  RecoveryTestClusterManager(StaticClusterManager staticClusterManager, HelixClusterManager helixClusterManager) {
    this.staticClusterManager = Objects.requireNonNull(staticClusterManager);
    this.helixClusterManager = Objects.requireNonNull(helixClusterManager);
    ambryDataNodeToDataNodeMap = new HashMap<>();
    ambryDiskToDiskMap = new HashMap<>();
  }

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    DuplicatingInputStream duplicatingInputStream = new DuplicatingInputStream(stream);
    duplicatingInputStream.mark(0);
    PartitionId partitionIdStatic = staticClusterManager.getPartitionIdFromStream(duplicatingInputStream);
    return partitionIdStatic;
  }

  @Override
  public List<PartitionId> getWritablePartitionIds(String partitionClass) {
    throw new UnsupportedOperationException(String.format("getWritablePartitionIds method is not supported"));
  }

  @Override
  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    throw new UnsupportedOperationException(String.format("getRandomWritablePartition method is not supported"));
  }

  /**
   * {@inheritDoc}
   * Get all partition ids from both the underlying {@link StaticClusterManager}.
   * @param partitionClass the partition class whose partitions are required. Can be {@code null}
   * @return a list of partition ids from the underlying {@link StaticClusterManager}.
   */
  @Override
  public List<PartitionId> getAllPartitionIds(String partitionClass) {
    return staticClusterManager.getAllPartitionIds(partitionClass);
  }

  /**
   * Check for existence of the given datacenter from both the static and the helix based cluster managers and update
   * a metric if there is a mismatch.
   * @param datacenterName name of datacenter
   * @return true if the datacenter exists in the underlying {@link StaticClusterManager}; false otherwise.
   */
  @Override
  public boolean hasDatacenter(String datacenterName) {
    boolean staticHas = staticClusterManager.hasDatacenter(datacenterName);
    boolean helixHas = helixClusterManager.hasDatacenter(datacenterName);
    return staticHas || helixHas;
  }

  @Override
  public byte getLocalDatacenterId() {
    return staticClusterManager.getLocalDatacenterId();
  }

  @Override
  public String getDatacenterName(byte id) {
    String dcName = staticClusterManager.getDatacenterName(id);
    return (dcName != null) ? dcName : helixClusterManager.getDatacenterName(id);
  }

  /**
   * Return the {@link DataNodeId} associated with the given hostname and port in the underlying
   * {@link StaticClusterManager}. If not found in static cluster map, then get the same from helix cluster map.
   * @param hostname of the DataNodeId
   * @param port of the DataNodeId
   * @return the {@link DataNodeId} associated with the given hostname and port.
   */
  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    DataNodeId dataNode = staticClusterManager.getDataNodeId(hostname, port);
    if (dataNode == null) {
      dataNode = helixClusterManager.getDataNodeId(hostname, port);
    }
    return dataNode;
  }

  /**
   * Get the list of {@link ReplicaId}s associated with the given {@link DataNodeId} in the underlying
   * {@link StaticClusterManager}. The list of {@link ReplicaId}s is obtained by merging replica lists from
   * {@link HelixClusterManager} and {@link StaticClusterManager}.
   * @param dataNodeId the {@link DataNodeId} for which the replicas are to be listed.
   * @return merged list of {@link ReplicaId}s as present in the underlying {@link StaticClusterManager} and
   * {@link HelixClusterManager} for the given node.
   */
  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    List<ReplicaId> mergedReplicas = new ArrayList<>();
    for (ReplicaId staticReplica : staticClusterManager.getReplicaIds(dataNodeId)) {
      AmbryPartition partitionId = (AmbryPartition) getHelixPartition(staticReplica);
      if (partitionId == null) {
        throw new IllegalStateException(String.format("No partition %s not found in helix clustermap",
            staticReplica.getPartitionId().toPathString()));
      }
      Partition partition = (Partition) staticReplica.getPartitionId();
      Partition compositePartition =
          new Partition(Long.parseLong(partitionId.toPathString()), partition.getPartitionClass(),
              partition.getPartitionState(), partition.getReplicaCapacityInBytes());
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        compositePartition.addReplica(replicaId);
      }
      Replica compositeReplica = new Replica(compositePartition, (Disk) staticReplica.getDiskId(),
          ((Replica) staticReplica).getClusterMapConfig());
      compositePartition.addReplica(compositeReplica);
      mergedReplicas.add(compositeReplica);
    }
    return mergedReplicas;
  }

  /**
   * Search from all the partitions of {@link HelixClusterManager}, the {@link PartitionId} with same partition id as
   * one in the {@link ReplicaId} passed.
   * @param replicaId {@link ReplicaId} object.
   * @return {@link PartitionId} of {@link HelixClusterManager}.
   */
  PartitionId getHelixPartition(ReplicaId replicaId) {
    return helixClusterManager.getAllPartitionIds(null)
        .stream()
        .filter(partitionId -> replicaId.getPartitionId().toPathString().equals(partitionId.toPathString()))
        .findAny()
        .get();
  }

  /**
   * The list of {@link DataNodeId}s present in the underlying {@link HelixClusterManager}
   * @return the list of {@link DataNodeId}s present in the underlying {@link HelixClusterManager}
   */
  @Override
  public List<? extends DataNodeId> getDataNodeIds() {
    return helixClusterManager.getDataNodeIds();
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return staticClusterManager.getMetricRegistry();
  }

  /**
   * Relay the event to both the underlying {@link StaticClusterManager} and the underlying {@link HelixClusterManager}.
   * @param replicaId the {@link ReplicaId} for which this event has occurred.
   * @param event the {@link ReplicaEventType}.
   */
  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    helixClusterManager.onReplicaEvent(replicaId, event);
  }

  @Override
  public JSONObject getSnapshot() {
    return staticClusterManager.getSnapshot();
  }

  @Override
  public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
    return helixClusterManager.getBootstrapReplica(partitionIdStr, dataNodeId);
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    helixClusterManager.registerClusterMapListener(clusterMapChangeListener);
  }

  @Override
  public void close() {
    staticClusterManager.close();
    if (helixClusterManager != null) {
      helixClusterManager.close();
    }
  }
}
