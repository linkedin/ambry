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

import com.github.ambry.clustermap.HelixClusterManager.ClusterChangeHandlerCallback;
import com.github.ambry.config.ClusterMapConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Currently, this just adds a single {@link ReplicaType#CLOUD_BACKED} replica every time a partition is added to allow
 * for requests to be routed to cloud services. Currently it is only registered as a {@link ClusterMapChangeListener},
 * but once the clustermap natively supports VCR clusters, this class can implement {@link HelixClusterChangeHandler}
 * and listen for changes in the VCR cluster.
 */
class CloudServiceClusterChangeHandler implements ClusterMapChangeListener, ClusterChangeHandler {
  private static final Logger logger = LoggerFactory.getLogger(CloudServiceClusterChangeHandler.class);
  private final Set<String> knownPartitions = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<String, CloudServiceReplica> partitionToReplica = new ConcurrentHashMap<>();
  private final List<ClusterMapChangeListener> listeners = new CopyOnWriteArrayList<>();
  private final AtomicLong errorCount = new AtomicLong(0);
  private final String dcName;
  private final ClusterMapConfig clusterMapConfig;
  private final ClusterChangeHandlerCallback changeHandlerCallback;
  private final CloudServiceDataNode cloudServiceDataNode;

  public CloudServiceClusterChangeHandler(String dcName, ClusterMapConfig clusterMapConfig,
      ClusterChangeHandlerCallback changeHandlerCallback) throws Exception {
    this.dcName = dcName;
    this.clusterMapConfig = clusterMapConfig;
    this.changeHandlerCallback = changeHandlerCallback;
    cloudServiceDataNode = new CloudServiceDataNode(dcName, clusterMapConfig);
  }

  @Override
  public void onReplicaAddedOrRemoved(List<ReplicaId> addedReplicas, List<ReplicaId> removedReplicas) {
    // This impl assumes that no partition will ever be completely removed.
    List<ReplicaId> newReplicas = new ArrayList<>();
    for (ReplicaId replica : addedReplicas) {
      AmbryPartition partition = (AmbryPartition) replica.getPartitionId();
      String partitionName = partition.toPathString();
      // Only add one replica object for each partition
      if (knownPartitions.add(partitionName)) {
        try {
          logger.info("Adding cloud replica: dc={}, partition={}", dcName, partitionName);
          // Copy the capacity from an existing replica
          CloudServiceReplica cloudServiceReplica =
              new CloudServiceReplica(clusterMapConfig, cloudServiceDataNode, partition, replica.getCapacityInBytes());
          partitionToReplica.put(partitionName, cloudServiceReplica);
          changeHandlerCallback.addReplicasToPartition(partition, Collections.singletonList(cloudServiceReplica));
          newReplicas.add(cloudServiceReplica);
        } catch (Exception e) {
          errorCount.getAndIncrement();
          logger.error("Exception while adding cloud replica: dc={}, partition={}", dcName, partitionName, e);
        }
      }
    }
    listeners.forEach(listener -> listener.onReplicaAddedOrRemoved(newReplicas, Collections.emptyList()));
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    // Only register change listeners other than this instance so that CloudServiceClusterChangeHandler does not notify
    // itself twice.
    if (clusterMapChangeListener != this) {
      listeners.add(clusterMapChangeListener);
    }
  }

  @Override
  public Stream<AmbryReplica> getReplicaIdsByState(AmbryPartition partition, ReplicaState state) {
    AmbryReplica replica = partitionToReplica.get(partition.toPathString());
    return replica == null ? Stream.empty() : Stream.of(replica);
  }

  @Override
  public Map<AmbryDataNode, Set<AmbryDisk>> getDataNodeToDisksMap() {
    return Collections.emptyMap();
  }

  @Override
  public AmbryDataNode getDataNode(String instanceName) {
    // unsupported since a CloudServiceDataNode is not really an addressable instance.
    return null;
  }

  @Override
  public AmbryReplica getReplicaId(AmbryDataNode ambryDataNode, String partitionName) {
    return ambryDataNode == cloudServiceDataNode ? partitionToReplica.get(partitionName) : null;
  }

  @Override
  public List<AmbryReplica> getReplicaIds(AmbryDataNode ambryDataNode) {
    return ambryDataNode == cloudServiceDataNode ? new ArrayList<>(partitionToReplica.values())
        : Collections.emptyList();
  }

  @Override
  public List<AmbryDataNode> getAllDataNodes() {
    return Collections.singletonList(cloudServiceDataNode);
  }

  @Override
  public Set<AmbryDisk> getDisks(AmbryDataNode ambryDataNode) {
    return Collections.emptySet();
  }

  @Override
  public Map<String, String> getPartitionToResourceMap() {
    return Collections.emptyMap();
  }

  @Override
  public long getErrorCount() {
    return errorCount.get();
  }
}
