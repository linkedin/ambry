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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.clustermap.VirtualReplicatorClusterListener;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.InstanceType;


/**
 * {@link CloudBackupManager} is used to backup partitions to Cloud. Partitions assignment is handled by Helix.
 */
public class CloudBackupManager extends ReplicationEngine {
  private final VerifiableProperties properties;
  private final CloudConfig cloudConfig;
  private final StoreConfig storeConfig;
  private final CloudDestination cloudDestination;
  private final VcrMetrics vcrMetrics;
  private final VirtualReplicatorCluster virtualReplicatorCluster;

  public CloudBackupManager(VerifiableProperties properties, CloudConfig cloudConfig,
      ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig, StoreConfig storeConfig,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, VirtualReplicatorCluster virtualReplicatorCluster,
      CloudDestinationFactory cloudDestinationFactory, ScheduledExecutorService scheduler,
      ConnectionPool connectionPool, MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler,
        virtualReplicatorCluster.getCurrentDataNodeId(), Collections.emptyList(), connectionPool, metricRegistry,
        requestNotification, storeKeyConverterFactory, transformerClassName);
    this.properties = properties;
    this.cloudConfig = cloudConfig;
    this.storeConfig = storeConfig;
    this.virtualReplicatorCluster = virtualReplicatorCluster;
    this.vcrMetrics = new VcrMetrics(metricRegistry);
    this.cloudDestination = cloudDestinationFactory.getCloudDestination();
    this.persistor =
        new CloudTokenPersistor(replicaTokenFileName, mountPathToPartitionInfoList, replicationMetrics, clusterMap,
            factory, cloudDestination);
  }

  @Override
  public void start() throws ReplicationException {
    // Add listener for new coming assigned partition
    virtualReplicatorCluster.addListener(new VirtualReplicatorClusterListener() {
      @Override
      public void onPartitionAdded(PartitionId partitionId) {
        try {
          addPartition(partitionId);
        } catch (ReplicationException e) {
          logger.error("Exception on adding partition{}: ", partitionId, e);
        }
      }

      @Override
      public void onPartitionRemoved(PartitionId partitionId) {
        PartitionInfo partitionInfo = partitionToPartitionInfo.get(partitionId);
        removeRemoteReplicaInfoFromReplicaThread(partitionInfo.getRemoteReplicaInfos());
        try {
          persistor.write(partitionInfo.getLocalReplicaId().getMountPath(), false);
        } catch (IOException | ReplicationException e) {
          logger.error("Exception on token write when remove partition{}: ", partitionId, e);
        }
      }
    });

    try {
      virtualReplicatorCluster.participate(InstanceType.PARTICIPANT);
    } catch (Exception e) {
      throw new ReplicationException("Cluster participate failed.", e);
    }

    // start background persistent thread
    // start scheduler thread to persist index in the background
    this.scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
        replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Add given {@link PartitionId} and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionId the {@link PartitionId} to add.
   */
  boolean addPartition(PartitionId partitionId) throws ReplicationException {
    if (partitionToPartitionInfo.get(partitionId) != null) {
      return false;
    }
    ReplicaId cloudReplica =
        new CloudReplica(cloudConfig, partitionId, virtualReplicatorCluster.getCurrentDataNodeId());
    Store cloudStore = new CloudBlobStore(properties, partitionId, cloudDestination, vcrMetrics);
    try {
      cloudStore.start();
    } catch (StoreException e) {
      throw new ReplicationException("Can't start CloudStore " + cloudStore, e);
    }
    List<? extends ReplicaId> peerReplicas = cloudReplica.getPeerReplicaIds();
    List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
    if (peerReplicas != null) {
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
        remoteReplicaInfos.add(remoteReplicaInfo);
      }
      PartitionInfo partitionInfo = new PartitionInfo(remoteReplicaInfos, partitionId, cloudStore, cloudReplica);
      partitionToPartitionInfo.put(partitionId, partitionInfo);
      mountPathToPartitionInfoList.compute(cloudReplica.getMountPath(), (key, value) -> {
        // For CloudBackUpManger, at most one PartitionInfo in the list.
        List<PartitionInfo> retList;
        retList = (value == null ? new ArrayList<>() : value);
        retList.add(partitionInfo);
        return retList;
      });
    } else {
      throw new ReplicationException("Failed to add partition " + partitionId + ", because no peer replicas found.");
    }
    // Reload replication token if exist.
    List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfos = persistor.retrieve(cloudReplica.getMountPath());
    if (tokenInfos.size() != 0) {
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
        boolean tokenReloaded = false;
        for (RemoteReplicaInfo.ReplicaTokenInfo tokenInfo : tokenInfos) {
          DataNodeId dataNodeId = remoteReplicaInfo.getReplicaId().getDataNodeId();
          if (dataNodeId.getHostname().equalsIgnoreCase(tokenInfo.getHostname())
              && dataNodeId.getPort() == tokenInfo.getPort() && remoteReplicaInfo.getReplicaId()
              .getReplicaPath()
              .equals(tokenInfo.getReplicaPath())) {
            logger.info("Read token for partition {} remote host {} port {} token {}", partitionId,
                tokenInfo.getHostname(), tokenInfo.getPort(), tokenInfo.getReplicaToken());
            tokenReloaded = true;
            remoteReplicaInfo.initializeTokens(tokenInfo.getReplicaToken());
            remoteReplicaInfo.setTotalBytesReadFromLocalStore(tokenInfo.getTotalBytesReadFromLocalStore());
            break;
          }
        }
        if (tokenReloaded == false) {
          logger.warn("Token reload failed. remoteReplicaInfo: {} tokenInfos: {}", remoteReplicaInfo, tokenInfos);
        }
      }
    }
    // Add remoteReplicaInfos to {@link ReplicaThread}.
    addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
    return true;
  }
}


