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

import com.codahale.metrics.MetricRegistry;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * A cluster manager that is a wrapper over a {@link StaticClusterManager} instance and a {@link HelixClusterManager}
 * instance and uses the {@link StaticClusterManager} as the source-of-truth. It relays events to both and checks for
 * and reports inconsistencies in the views from the two underlying cluster managers.
 */
class CompositeClusterManager implements ClusterMap {
  private final StaticClusterManager staticClusterManager;
  private final HelixClusterManager helixClusterManager;
  private final HelixClusterManagerMetrics helixClusterManagerMetrics;

  /**
   * Construct a CompositeClusterManager instance.
   * @param staticClusterManager the {@link StaticClusterManager} instance to use as the source-of-truth.
   * @param helixClusterManager the {@link HelixClusterManager} instance to use for comparison of views.
   */
  CompositeClusterManager(StaticClusterManager staticClusterManager, HelixClusterManager helixClusterManager) {
    this.staticClusterManager = staticClusterManager;
    this.helixClusterManager = helixClusterManager;
    this.helixClusterManagerMetrics = helixClusterManager.helixClusterManagerMetrics;
  }

  @Override
  public PartitionId getPartitionIdFromStream(DataInputStream stream) throws IOException {
    return staticClusterManager.getPartitionIdFromStream(stream);
  }

  /**
   * Get writable partition ids from both the underlying {@link StaticClusterManager} and the underlying
   * {@link HelixClusterManager}. Compare the two and if there is a mismatch, update a metric.
   * @return a list of partition ids from the underlying {@link StaticClusterManager}.
   */
  @Override
  public List<PartitionId> getWritablePartitionIds() {
    List<PartitionId> staticWritablePartitionIds = staticClusterManager.getWritablePartitionIds();
    Set<String> staticPartitionIdStrings = new HashSet<>();
    for (PartitionId partitionId : staticWritablePartitionIds) {
      staticPartitionIdStrings.add(partitionId.toString());
    }

    Set<String> helixPartitionIdStrings = new HashSet<>();
    for (PartitionId partitionId : helixClusterManager.getWritablePartitionIds()) {
      helixPartitionIdStrings.add(partitionId.toString());
    }

    if (!staticPartitionIdStrings.equals(helixPartitionIdStrings)) {
      helixClusterManagerMetrics.getWritablePartitionIdsMismatchCount.inc();
    }
    return staticWritablePartitionIds;
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
    if (staticHas != helixHas) {
      helixClusterManagerMetrics.hasDatacenterMismatchCount.inc();
    }
    return staticHas;
  }

  /**
   * Return the {@link DataNodeId} associated with the given hostname and port in the underlying
   * {@link StaticClusterManager}.
   * Also get the associated data node from the underlying {@link HelixClusterManager} and if the two returned
   * DataNodeId
   * objects do not refer to the same actual instance, update a metric.
   * @param hostname of the DataNodeId
   * @param port of the DataNodeId
   * @return the {@link DataNodeId} associated with the given hostname and port in the underlying
   * {@link StaticClusterManager}.
   */
  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    DataNodeId staticDataNode = staticClusterManager.getDataNodeId(hostname, port);
    DataNodeId helixDataNode = helixClusterManager.getDataNodeId(hostname, port);
    if (!staticDataNode.toString().equals(helixDataNode.toString())) {
      helixClusterManagerMetrics.getDataNodeIdMismatchCount.inc();
    }
    return staticDataNode;
  }

  /**
   * Get the list of {@link ReplicaId}s associated with the given {@link DataNodeId} in the underlying
   * {@link StaticClusterManager}.
   * Also get the associated list of {@link ReplicaId}s from the underlying {@link HelixClusterManager} and verify
   * equality, updating a metric if there is a mismatch.
   * @param dataNodeId the {@link DataNodeId} for which the replicas are to be listed.
   * @return the list of {@link ReplicaId}s as present in the underlying {@link StaticClusterManager} for the given
   * node.
   */
  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    List<ReplicaId> staticReplicaIds = staticClusterManager.getReplicaIds(dataNodeId);
    Set<String> staticReplicaIdStrings = new HashSet<>();
    for (ReplicaId replicaId : staticReplicaIds) {
      staticReplicaIdStrings.add(replicaId.toString());
    }

    Set<String> helixReplicaIdStrings = new HashSet<>();
    DataNodeId ambryDataNode = helixClusterManager.getDataNodeId(dataNodeId.getHostname(), dataNodeId.getPort());
    for (ReplicaId replicaId : helixClusterManager.getReplicaIds(ambryDataNode)) {
      helixReplicaIdStrings.add(replicaId.toString());
    }

    if (!staticReplicaIdStrings.equals(helixReplicaIdStrings)) {
      helixClusterManagerMetrics.getReplicaIdsMismatchCount.inc();
    }
    return staticReplicaIds;
  }

  /**
   * The list of {@link DataNodeId}s present in the underlying {@link StaticClusterManager}
   * Also verify that the underlying {@link HelixClusterManager} has the same set of nodes, if not, update a metric.
   * @return the list of {@link DataNodeId}s present in the underlying {@link StaticClusterManager}
   */
  @Override
  public List<DataNodeId> getDataNodeIds() {
    List<DataNodeId> staticDataNodeIds = staticClusterManager.getDataNodeIds();
    Set<String> staticDataNodeIdStrings = new HashSet<>();
    for (DataNodeId dataNodeId : staticDataNodeIds) {
      staticDataNodeIdStrings.add(dataNodeId.toString());
    }

    Set<String> helixDataNodeIdStrings = new HashSet<>();
    for (DataNodeId dataNodeId : helixClusterManager.getDataNodeIds()) {
      helixDataNodeIdStrings.add(dataNodeId.toString());
    }

    if (!staticDataNodeIdStrings.equals(helixDataNodeIdStrings)) {
      helixClusterManagerMetrics.getDataNodeIdsMismatchCount.inc();
    }
    return staticDataNodeIds;
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
    staticClusterManager.onReplicaEvent(replicaId, event);
    AmbryReplica ambryReplica =
        helixClusterManager.getReplicaForPartitionOnNode(replicaId.getDataNodeId().getHostname(),
            replicaId.getDataNodeId().getPort(), replicaId.getPartitionId().toString());
    helixClusterManager.onReplicaEvent(ambryReplica, event);
  }
}
