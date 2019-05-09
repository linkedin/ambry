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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Set up replicas based on {@link ReplicationEngine} and do replication across all colos.
 */
public class ReplicationManager extends ReplicationEngine {

  public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StorageManager storageManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, ConnectionPool connectionPool,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler, dataNode,
        clusterMap.getReplicaIds(dataNode), connectionPool, metricRegistry, requestNotification,
        storeKeyConverterFactory, transformerClassName);
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNode);
    // initialize all partitions
    for (ReplicaId replicaId : replicaIds) {
      PartitionId partition = replicaId.getPartitionId();
      Store store = storageManager.getStore(partition);
      if (store != null) {
        List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        if (peerReplicas != null) {
          List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>(peerReplicas.size());
          for (ReplicaId remoteReplica : peerReplicas) {
            // We need to ensure that a replica token gets persisted only after the corresponding data in the
            // store gets flushed to disk. We use the store flush interval multiplied by a constant factor
            // to determine the token flush interval
            RemoteReplicaInfo remoteReplicaInfo =
                new RemoteReplicaInfo(remoteReplica, replicaId, store, factory.getNewFindToken(),
                    storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
                    SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());
            replicationMetrics.addRemoteReplicaToLagMetrics(remoteReplicaInfo);
            replicationMetrics.createRemoteReplicaErrorMetrics(remoteReplicaInfo);
            remoteReplicas.add(remoteReplicaInfo);
          }
          addRemoteReplicaInfoToReplicaThread(remoteReplicas, false);
          PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, partition, store, replicaId);
          partitionToPartitionInfo.put(partition, partitionInfo);
          mountPathToPartitionInfoList.computeIfAbsent(replicaId.getMountPath(), key -> new ArrayList<>())
              .add(partitionInfo);
        }
      } else {
        logger.error("Not replicating to partition " + partition + " because an initialized store could not be found");
      }
    }
    replicationMetrics.populatePerColoMetrics(replicaThreadPoolByDc.keySet());
    persistor =
        new DiskTokenPersistor(replicaTokenFileName, mountPathToPartitionInfoList, replicationMetrics, clusterMap,
            factory);
  }

  @Override
  public void start() throws ReplicationException {
    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : mountPathToPartitionInfoList.keySet()) {
        retrieveReplicaTokensAndPersistIfNecessary(mountPath);
      }
      if (replicaThreadPoolByDc.size() == 0) {
        logger.warn("Number of Datacenters to replicate from is 0, not starting any replica threads");
        return;
      }
      // valid for replication manager.
      replicationMetrics.trackReplicationDisabledPartitions(replicaThreadPoolByDc);

      // start all replica threads
      for (List<ReplicaThread> replicaThreads : replicaThreadPoolByDc.values()) {
        for (ReplicaThread thread : replicaThreads) {
          Thread replicaThread = Utils.newThread(thread.getName(), thread, false);
          logger.info("Starting replica thread " + thread.getName());
          replicaThread.start();
        }
      }

      // start background persistent thread
      // start scheduler thread to persist replica token in the background
      if (persistor != null) {
        this.scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
            replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
      }
    } catch (IOException e) {
      logger.error("IO error while starting replication", e);
    }
  }
}
