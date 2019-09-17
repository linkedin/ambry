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

import com.github.ambry.clustermap.ClusterMap;
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
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.InstanceType;


/**
 * {@link VcrReplicationManager} is used to backup partitions to Cloud. Partitions assignment is handled by Helix.
 */
public class VcrReplicationManager extends ReplicationEngine {
  private final VerifiableProperties properties;
  private final CloudConfig cloudConfig;
  private final StoreConfig storeConfig;
  private final CloudDestination cloudDestination;
  private final VcrMetrics vcrMetrics;
  private final VirtualReplicatorCluster virtualReplicatorCluster;
  private final CloudStorageCompactor cloudStorageCompactor;
  private final Map<String, Store> partitionStoreMap = new HashMap<>();
  private final StoreManager storeManager;

  public VcrReplicationManager(VerifiableProperties properties, CloudConfig cloudConfig,
      ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig, StoreConfig storeConfig,
      StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      VirtualReplicatorCluster virtualReplicatorCluster, CloudDestination cloudDestination,
      ScheduledExecutorService scheduler, ConnectionPool connectionPool, VcrMetrics vcrMetrics,
      NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory,
      String transformerClassName) throws ReplicationException {
    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler,
        virtualReplicatorCluster.getCurrentDataNodeId(), Collections.emptyList(), connectionPool,
        vcrMetrics.getMetricRegistry(), requestNotification, storeKeyConverterFactory, transformerClassName);
    this.properties = properties;
    this.cloudConfig = cloudConfig;
    this.storeConfig = storeConfig;
    this.storeManager = storeManager;
    this.virtualReplicatorCluster = virtualReplicatorCluster;
    this.vcrMetrics = vcrMetrics;
    this.cloudDestination = cloudDestination;
    this.persistor =
        new CloudTokenPersistor(replicaTokenFileName, mountPathToPartitionInfos, replicationMetrics, clusterMap,
            tokenHelper, cloudDestination);
    this.cloudStorageCompactor =
        new CloudStorageCompactor(cloudDestination, partitionToPartitionInfo.keySet(), vcrMetrics, false);
  }

  @Override
  public void start() throws ReplicationException {
    // Add listener for new coming assigned partition
    virtualReplicatorCluster.addListener(new VirtualReplicatorClusterListener() {
      @Override
      public void onPartitionAdded(PartitionId partitionId) {
        try {
          addPartition(partitionId);
          logger.info("Partition {} added to {}", partitionId, dataNodeId);
        } catch (ReplicationException e) {
          vcrMetrics.addPartitionErrorCount.inc();
          logger.error("Exception on adding Partition {} to {}: ", partitionId, dataNodeId, e);
        } catch (Exception e) {
          // Helix will run into error state if exception throws in Helix context.
          vcrMetrics.addPartitionErrorCount.inc();
          logger.error("Unknown Exception on adding Partition {} to {}: ", partitionId, dataNodeId, e);
        }
      }

      @Override
      public void onPartitionRemoved(PartitionId partitionId) {
        try {
          PartitionInfo partitionInfo = partitionToPartitionInfo.remove(partitionId);
          if (partitionInfo == null) {
            logger.error("Partition {} not exist when remove from {}. ", partitionId, dataNodeId);
            vcrMetrics.removePartitionErrorCount.inc();
            return;
          }
          removeRemoteReplicaInfoFromReplicaThread(partitionInfo.getRemoteReplicaInfos());
          if (replicationConfig.replicationPersistTokenOnShutdownOrReplicaRemove) {
            try {
              persistor.write(partitionInfo.getLocalReplicaId().getMountPath(), false);
            } catch (IOException | ReplicationException e) {
              logger.error("Exception on token write when remove Partition {} from {}: ", partitionId, dataNodeId, e);
              vcrMetrics.removePartitionErrorCount.inc();
            }
          }
          Store cloudStore = partitionStoreMap.get(partitionId.toPathString());
          if (cloudStore != null) {
            cloudStore.shutdown();
          } else {
            logger.warn("Store not found for partition {}", partitionId);
          }
          logger.info("Partition {} removed from {}", partitionId, dataNodeId);
          // We don't close cloudBlobStore because because replicate in ReplicaThread is using a copy of
          // remoteReplicaInfo which needs CloudBlobStore.
        } catch (Exception e) {
          // Helix will run into error state if exception throws in Helix context.
          vcrMetrics.removePartitionErrorCount.inc();
          logger.error("Unknown Exception on removing Partition {} from {}: ", partitionId, dataNodeId, e);
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
    scheduler.scheduleAtFixedRate(persistor, replicationConfig.replicationTokenFlushDelaySeconds,
        replicationConfig.replicationTokenFlushIntervalSeconds, TimeUnit.SECONDS);

    // Schedule thread to purge dead blobs for this VCR's partitions
    // Set random delay (between now and now+2h) to stagger the schedule across VCR's
    Random random = new Random(System.currentTimeMillis());
    long delaySec = random.nextInt((int) TimeUnit.HOURS.toSeconds(2));
    long intervalSec = TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours);
    scheduler.scheduleAtFixedRate(cloudStorageCompactor, delaySec, intervalSec, TimeUnit.SECONDS);
    logger.info("Scheduled compaction task to run every {} hours starting in {} seconds.",
        cloudConfig.cloudBlobCompactionIntervalHours, delaySec);
  }

  /**
   * Add given {@link PartitionId} and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionId the {@link PartitionId} to add.
   * @throws ReplicationException if replicas initialization failed.
   */
  void addPartition(PartitionId partitionId) throws ReplicationException {
    if (partitionToPartitionInfo.containsKey(partitionId)) {
      throw new ReplicationException("Partition " + partitionId + " already exists on " + dataNodeId);
    }
    ReplicaId cloudReplica = new CloudReplica(partitionId, virtualReplicatorCluster.getCurrentDataNodeId());
    if (!storeManager.addBlobStore(cloudReplica)) {
      logger.error("Can't start cloudstore for replica " + cloudReplica);
      throw new ReplicationException("Can't start cloudstore for replica " + cloudReplica);
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
        FindTokenFactory findTokenFactory =
            tokenHelper.getFindTokenFactoryFromReplicaType(peerReplica.getReplicaType());
        RemoteReplicaInfo remoteReplicaInfo =
            new RemoteReplicaInfo(peerReplica, cloudReplica, storeManager.getStore(partitionId),
                findTokenFactory.getNewFindToken(),
                storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
                SystemTime.getInstance(), peerReplica.getDataNodeId().getPortToConnectTo());
        replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo);
        remoteReplicaInfos.add(remoteReplicaInfo);
      }
      PartitionInfo partitionInfo =
          new PartitionInfo(remoteReplicaInfos, partitionId, storeManager.getStore(partitionId), cloudReplica);
      partitionToPartitionInfo.put(partitionId, partitionInfo);
      mountPathToPartitionInfos.compute(cloudReplica.getMountPath(), (key, value) -> {
        // For CloudBackupManager, at most one PartitionInfo in the list.
        List<PartitionInfo> retList = (value == null) ? new ArrayList<>() : value;
        retList.add(partitionInfo);
        return retList;
      });
      partitionStoreMap.put(partitionId.toPathString(), storeManager.getStore(partitionId));
    } else {
      try {
        storeManager.shutdownBlobStore(partitionId);
        storeManager.removeBlobStore(partitionId);
      } finally {
        throw new ReplicationException(
            "Failed to add Partition " + partitionId + " on " + dataNodeId + " , because no peer replicas found.");
      }
    }
    // Reload replication token if exist.
    List<RemoteReplicaInfo.ReplicaTokenInfo> tokenInfos = persistor.retrieve(cloudReplica.getMountPath());
    if (tokenInfos.size() != 0) {
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicaInfos) {
        boolean tokenReloaded = false;
        for (RemoteReplicaInfo.ReplicaTokenInfo tokenInfo : tokenInfos) {
          if (isTokenForRemoteReplicaInfo(remoteReplicaInfo, tokenInfo)) {
            logger.info("Read token for partition {} remote host {} port {} token {}", partitionId,
                tokenInfo.getHostname(), tokenInfo.getPort(), tokenInfo.getReplicaToken());
            tokenReloaded = true;
            remoteReplicaInfo.initializeTokens(tokenInfo.getReplicaToken());
            remoteReplicaInfo.setTotalBytesReadFromLocalStore(tokenInfo.getTotalBytesReadFromLocalStore());
            break;
          }
        }
        if (!tokenReloaded) {
          // This may happen on clusterMap update: replica removed or added.
          // Or error on token persist/retrieve.
          logger.warn("Token not found or reload failed. remoteReplicaInfo: {} tokenInfos: {}", remoteReplicaInfo,
              tokenInfos);
          vcrMetrics.tokenReloadWarnCount.inc();
        }
      }
    }
    // Add remoteReplicaInfos to {@link ReplicaThread}.
    addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
    if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
      replicationMetrics.addLagMetricForPartition(partitionId);
    }
  }

  public VcrMetrics getVcrMetrics() {
    return vcrMetrics;
  }
}
