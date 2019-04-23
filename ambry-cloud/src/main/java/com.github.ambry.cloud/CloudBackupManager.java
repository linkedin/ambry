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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;


/**
 * {@link CloudBackupManager} is used to backup partitions to Cloud. Partitions assignment is handled by Helix.
 */
public class CloudBackupManager extends ReplicationEngine {

  public CloudBackupManager(CloudConfig cloudConfig, ReplicationConfig replicationConfig,
      ClusterMapConfig clusterMapConfig, StoreConfig storeConfig, StoreKeyFactory storeKeyFactory,
      ClusterMap clusterMap, VirtualReplicatorCluster virtualReplicatorCluster,
      CloudDestinationFactory cloudDestinationFactory, ScheduledExecutorService scheduler,
      ConnectionPool connectionPool, MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName) throws ReplicationException {

    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler,
        virtualReplicatorCluster.getCurrentDataNodeId(), Collections.emptyList(), connectionPool, metricRegistry,
        requestNotification, storeKeyConverterFactory, transformerClassName);
    CloudDestination cloudDestination = cloudDestinationFactory.getCloudDestination();
    List<? extends PartitionId> partitionIds = virtualReplicatorCluster.getAssignedPartitionIds();
    for (PartitionId partitionId : partitionIds) {
      ReplicaId cloudReplica =
          new CloudReplica(cloudConfig, partitionId, virtualReplicatorCluster.getCurrentDataNodeId());
      Store cloudStore = new CloudBlobStore(partitionId, cloudConfig, cloudDestination);
      try {
        cloudStore.start();
      } catch (StoreException e) {
        throw new ReplicationException("Can't start CloudStore " + cloudStore, e);
      }
      List<? extends ReplicaId> peerReplicas = cloudReplica.getPeerReplicaIds();
      if (peerReplicas != null) {
        List<RemoteReplicaInfo> remoteReplicas = new ArrayList<>();
        for (ReplicaId peerReplica : peerReplicas) {
          if (!peerReplica.getDataNodeId().getDatacenterName().equals(dataNodeId.getDatacenterName())) {
            continue;
          }
          // We need to ensure that a replica token gets persisted only after the corresponding data in the
          // store gets flushed to cloud. We use the store flush interval multiplied by a constant factor
          // to determine the token flush interval
          RemoteReplicaInfo remoteReplicaInfo =
              new RemoteReplicaInfo(peerReplica, cloudReplica, cloudStore, factory.getNewFindToken(),
                  storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
                  SystemTime.getInstance(), peerReplica.getDataNodeId().getPortToConnectTo());
          replicationMetrics.addRemoteReplicaToLagMetrics(remoteReplicaInfo);
          replicationMetrics.createRemoteReplicaErrorMetrics(remoteReplicaInfo);
          remoteReplicas.add(remoteReplicaInfo);
          updateReplicasToReplicate(peerReplica.getDataNodeId().getDatacenterName(), remoteReplicaInfo);
        }
        PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, partitionId, cloudStore, cloudReplica);
        partitionsToReplicate.put(partitionId, partitionInfo);
        partitionGroupedByMountPath.computeIfAbsent(cloudReplica.getMountPath(), key -> new ArrayList<>())
            .add(partitionInfo);
      } else {
        throw new ReplicationException(
            "Failed to back up partition " + partitionId + " because no peer replicas found.");
      }
    }
    replicationMetrics.populatePerColoMetrics(numberOfReplicaThreads.keySet());
    persistor =
        new CloudTokenPersistor(replicaTokenFileName, partitionGroupedByMountPath, replicationMetrics, clusterMap,
            factory, cloudDestination);
  }
}