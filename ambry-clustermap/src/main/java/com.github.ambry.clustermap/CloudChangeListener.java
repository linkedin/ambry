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
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Currently, this just adds a single {@link ReplicaType#CLOUD_BACKED} replica every time a partition is added to allow
 * for requests to be routed to cloud services. Currently it is only registered as a {@link ClusterMapChangeListener},
 * but once the clustermap natively supports VCR clusters, this class can implement {@link ClusterChangeHandler} and
 * listen for changes in the VCR cluster.
 */
class CloudChangeListener implements ClusterMapChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(CloudChangeListener.class);
  private final Set<String> knownPartitions = ConcurrentHashMap.newKeySet();
  private final String dcName;
  private final ClusterMapConfig clusterMapConfig;
  private final ClusterChangeHandlerCallback changeHandlerCallback;

  public CloudChangeListener(String dcName, ClusterMapConfig clusterMapConfig,
      ClusterChangeHandlerCallback changeHandlerCallback) {
    this.dcName = dcName;
    this.clusterMapConfig = clusterMapConfig;
    this.changeHandlerCallback = changeHandlerCallback;
  }

  @Override
  public void onReplicaAddedOrRemoved(Collection<? extends ReplicaId> addedReplicas,
      Collection<? extends ReplicaId> removedReplicas) {
    // This impl assumes that no partition will ever be completely removed.
    for (ReplicaId replica : addedReplicas) {
      AmbryPartition partition = (AmbryPartition) replica.getPartitionId();
      String partitionName = replica.getPartitionId().toPathString();
      if (knownPartitions.add(partitionName)) {
        try {
          logger.debug("Adding cloud replica: dc={}, partition={}", dcName, partitionName);
          // Copy the capacity from an existing replica
          AmbryReplica cloudAmbryReplica =
              new CloudAmbryReplica(clusterMapConfig, partition, replica.getCapacityInBytes());
          changeHandlerCallback.addReplicasToPartition(partition, Collections.singletonList(cloudAmbryReplica));
        } catch (Exception e) {
          logger.error("Exception while adding cloud replica: dc={}, partition={}", dcName, partitionName, e);
        }
      }
    }
  }
}
