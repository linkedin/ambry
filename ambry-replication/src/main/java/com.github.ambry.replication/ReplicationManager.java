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
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Set up replicas based on {@link ReplicationEngine} and do replication across all data centers.
 */
public class ReplicationManager extends ReplicationEngine {
  private final StoreManager storeManager;
  private final StoreConfig storeConfig;

  public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, ConnectionPool connectionPool,
      MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler, dataNode,
        clusterMap.getReplicaIds(dataNode), connectionPool, metricRegistry, requestNotification,
        storeKeyConverterFactory, transformerClassName);
    this.storeManager = storeManager;
    this.storeConfig = storeConfig;
    List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNode);
    // initialize all partitions
    for (ReplicaId replicaId : replicaIds) {
      PartitionId partition = replicaId.getPartitionId();
      Store store = storeManager.getStore(partition);
      if (store != null) {
        List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
        if (peerReplicas != null) {
          List<RemoteReplicaInfo> remoteReplicas = createRemoteReplicaInfos(peerReplicas, replicaId);
          updatePartitionInfoMaps(remoteReplicas, replicaId);
          addRemoteReplicaInfoToReplicaThread(remoteReplicas, false);
        }
      } else {
        logger.error("Not replicating to partition " + partition + " because an initialized store could not be found");
      }
    }
    persistor = new DiskTokenPersistor(replicaTokenFileName, mountPathToPartitionInfos, replicationMetrics, clusterMap,
        tokenHelper, storeManager);
  }

  @Override
  public void start() throws ReplicationException {
    try {
      // read stored tokens
      // iterate through all mount paths and read replication info for the partitions it owns
      for (String mountPath : mountPathToPartitionInfos.keySet()) {
        retrieveReplicaTokensAndPersistIfNecessary(mountPath);
      }
      if (replicaThreadPoolByDc.size() == 0) {
        logger.warn("Number of data centers to replicate from is 0, not starting any replica threads");
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

  /**
   * Add given replica into replication manager.
   * @param replicaId the replica to add
   * @return {@code true} if addition succeeded, {@code false} failed to add replica because it already exists.
   */
  public boolean addReplica(ReplicaId replicaId) {
    if (partitionToPartitionInfo.containsKey(replicaId.getPartitionId())) {
      logger.error("{} already exists in replication manager, rejecting adding replica request.",
          replicaId.getPartitionId());
      return false;
    }
    List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
    List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
    if (!peerReplicas.isEmpty()) {
      remoteReplicaInfos = createRemoteReplicaInfos(peerReplicas, replicaId);
      updatePartitionInfoMaps(remoteReplicaInfos, replicaId);
    }
    logger.info("Assigning thread for {}", replicaId.getPartitionId());
    addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
    // No need to update persistor to explicitly persist tokens for new replica because background persistor will
    // periodically persist all tokens including new added replica's
    logger.info("{} is successfully added into replication manager", replicaId.getPartitionId());
    return true;
  }

  /**
   * Remove replica from replication manager
   * @param replicaId the replica to remove
   * @return {@code true} if replica is successfully removed. {@code false} otherwise
   */
  public boolean removeReplica(ReplicaId replicaId) {
    if (!partitionToPartitionInfo.containsKey(replicaId.getPartitionId())) {
      logger.error("{} doesn't exist in replication manager, skipping removing replica request.",
          replicaId.getPartitionId());
      return false;
    }
    PartitionInfo partitionInfo = partitionToPartitionInfo.get(replicaId.getPartitionId());
    List<RemoteReplicaInfo> remoteReplicaInfos = partitionInfo.getRemoteReplicaInfos();
    logger.info("Removing remote replicas of {} from replica threads", replicaId.getPartitionId());
    removeRemoteReplicaInfoFromReplicaThread(remoteReplicaInfos);
    mountPathToPartitionInfos.computeIfPresent(replicaId.getMountPath(), (k, v) -> {
      v.remove(partitionInfo);
      return v;
    });
    partitionToPartitionInfo.remove(replicaId.getPartitionId());
    logger.info("{} is successfully removed from replication manager", replicaId.getPartitionId());
    return true;
  }

  /**
   * Create {@link RemoteReplicaInfo}(s) that associates with given local replica.
   * @param peerReplicas the list peer replicas of given local replica
   * @param replicaId the local replica
   * @return list of {@link RemoteReplicaInfo} associated with local replica.
   */
  private List<RemoteReplicaInfo> createRemoteReplicaInfos(List<? extends ReplicaId> peerReplicas,
      ReplicaId replicaId) {
    List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
    PartitionId partition = replicaId.getPartitionId();
    Store store = storeManager.getStore(partition);
    for (ReplicaId remoteReplica : peerReplicas) {
      // We need to ensure that a replica token gets persisted only after the corresponding data in the
      // store gets flushed to disk. We use the store flush interval multiplied by a constant factor
      // to determine the token flush interval
      FindToken findToken =
          this.tokenHelper.getFindTokenFactoryFromReplicaType(remoteReplica.getReplicaType()).getNewFindToken();
      RemoteReplicaInfo remoteReplicaInfo = new RemoteReplicaInfo(remoteReplica, replicaId, store, findToken,
          TimeUnit.SECONDS.toMillis(storeConfig.storeDataFlushIntervalSeconds) * Replication_Delay_Multiplier,
          SystemTime.getInstance(), remoteReplica.getDataNodeId().getPortToConnectTo());
      replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo);
      remoteReplicaInfos.add(remoteReplicaInfo);
    }
    if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
      replicationMetrics.addLagMetricForPartition(partition);
    }
    return remoteReplicaInfos;
  }

  /**
   * Update {@link PartitionInfo} related maps including {@link ReplicationEngine#partitionToPartitionInfo} and
   * {@link ReplicationEngine#mountPathToPartitionInfos}
   * @param remoteReplicaInfos the {@link RemoteReplicaInfo}(s) of the local {@link ReplicaId}
   * @param replicaId the local replica
   */
  private void updatePartitionInfoMaps(List<RemoteReplicaInfo> remoteReplicaInfos, ReplicaId replicaId) {
    PartitionId partition = replicaId.getPartitionId();
    PartitionInfo partitionInfo =
        new PartitionInfo(remoteReplicaInfos, partition, storeManager.getStore(partition), replicaId);
    partitionToPartitionInfo.put(partition, partitionInfo);
    mountPathToPartitionInfos.computeIfAbsent(replicaId.getMountPath(), key -> ConcurrentHashMap.newKeySet())
        .add(partitionInfo);
  }
}
