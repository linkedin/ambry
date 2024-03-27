/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.AmbryDisk;
import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.AmbryServerReplica;
import com.github.ambry.clustermap.CompositeClusterManager;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.Disk;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaSealStatus;
import com.github.ambry.clustermap.StaticClusterManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.utils.Utils;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Checks the integrity of backup partitions in Azure
 */
public class BackupIntegrityMonitor implements Runnable {
  private final Logger logger = LoggerFactory.getLogger(RecoveryThread.class);
  private final ClusterMapConfig clusterMapConfig;
  private final HelixClusterManager helixClusterManager;
  private final StaticClusterManager staticClusterManager;
  private final DataNodeId nodeId;
  private final RecoveryManager azureReplicationManager;
  private final RecoveryThread azureReplicator;
  private final ReplicaThread serverReplicator;
  private final ReplicationManager serverReplicationManager;
  private final ScheduledExecutorService executor;
  private final StorageManager storageManager;

  public BackupIntegrityMonitor(RecoveryManager azure, ReplicationManager server,
      CompositeClusterManager cluster, StorageManager storage, DataNodeId node,
      VerifiableProperties properties) throws Exception {
    azureReplicationManager = azure;
    azureReplicator = azure.getRecoveryThread("ambry_backup_integrity_monitor");
    clusterMapConfig = new ClusterMapConfig(properties);
    helixClusterManager = cluster.getHelixClusterManager();
    staticClusterManager = cluster.getStaticClusterManager();
    executor = Utils.newScheduler(1, "ambry_backup_integrity_monitor_", true);
    nodeId = node;
    serverReplicationManager = server;
    serverReplicator = server.getBackupCheckerThread("ambry_backup_integrity_monitor");
    storageManager = storage;
    // log disk state
    staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds()
        .forEach(d -> logger.info("[BackupIntegrityMonitor] Disk = {} {} {} bytes", d.getMountPath(), d.getState(),
            d.getRawCapacityInBytes()));
    logger.info("Created BackupIntegrityMonitor");
  }

  /**
   * Starts and schedules monitor
   */
  public void start() {
    executor.scheduleWithFixedDelay(this::run, 0, 1, TimeUnit.HOURS);
    logger.info("[BackupIntegrityMonitor] Started BackupIntegrityMonitor");
  }

  /**
   * Shut down the monitor waiting for in progress operations to complete.
   */
  public void shutdown() {
    /*
      Shutdown executor.
      This arbitrary wait is merely an attempt to allow the worker threads to gracefully exit.
      We will force a shutdown later. All workers are daemons and JVM _will_ exit when only daemons remain.
      Any data inconsistencies must be resolved separately, but not by trying to predict the right shutdown timeout.
    */
    logger.info("[BackupIntegrityMonitor] Shutting down BackupIntegrityMonitor");
    Utils.shutDownExecutorService(executor, 10, TimeUnit.SECONDS);
    logger.info("[BackupIntegrityMonitor] Stopped BackupIntegrityMonitor");
  }

  /**
   * Select a local disk for given replica
   * @param size
   * @return
   */
  private DiskId getDisk(long size) {
    // TODO: Handle disk full or unavailable
    return staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds().stream()
        .filter(d -> d.getState() == HardwareState.AVAILABLE)
        .filter(d -> d.getAvailableSpaceInBytes() >= size)
        .collect(Collectors.toList())
        .get(0);
  }

  /**
   * This method randomly picks a partition in a cluster and locates its replica in the cluster and the cloud.
   * Then it downloads and compares data from both replicas.
   */
  @Override
  public void run() {
    Random random = new Random();
    PartitionId partition = null;
    try {
      // 1. Pick a partition P, replica R from helix cluster-map
      // 2. Pick a disk D using static cluster-map
      // 3. while(!done) { RecoveryThread::replicate(P) } - restore partition P from cloud to disk D
      // 4. while(!done) { BackupCheckerThread::replicate(R) } - copy metadata from R, compare with metadata in D

      /** Select partition P */
      List<PartitionId> partitions = helixClusterManager.getAllPartitionIds(null);
      partition = partitions.get(random.nextInt(partitions.size()));
      logger.info("[BackupIntegrityMonitor] Verifying backup partition-{}", partition.getId());

      /**
       * Create local replica L to store cloud data
       * It will be at least as big as the largest replica of the partition on a server.
       * Cloud replica will usually be smaller than server replica, unless server disk has a problem or is compacted.
       */
      long maxReplicaSize = partition.getReplicaIds().stream()
          .map(r -> r.getCapacityInBytes())
          .max(Long::compare)
          .get();
      // Convert Disk object to AmbryDisk object, static-map -> helix-map
      AmbryDisk disk = new AmbryDisk((Disk) getDisk(maxReplicaSize), clusterMapConfig);
      AmbryServerReplica localReplica = new AmbryServerReplica(clusterMapConfig, (AmbryPartition) partition, disk,
          true, maxReplicaSize, ReplicaSealStatus.NOT_SEALED);
      if (!(storageManager.addBlobStore(localReplica) && storageManager.startBlobStore(partition))) {
        throw new RuntimeException(String.format("Failed to add Store for %s", partition.getId()));
      }
      Store store = storageManager.getStore(partition, true);
      logger.info("[BackupIntegrityMonitor] Added Store {}", store.toString());

      /** Recover cloud backup C for partition P */
      RemoteReplicaInfo cloudReplica = azureReplicationManager.getCloudReplica(localReplica);
      azureReplicator.addRemoteReplicaInfo(cloudReplica);
      logger.info("[BackupIntegrityMonitor] Restoring backup partition-{} of approx. {} bytes to disk {}",
          partition.getId(), maxReplicaSize, disk);
      while (!((RecoveryToken) cloudReplica.getToken()).isEndOfPartition()) {
        azureReplicator.replicate();
      }
      azureReplicator.removeRemoteReplicaInfo(cloudReplica);
      logger.info("[BackupIntegrityMonitor] Restored backup partition-{} of {} bytes to disk {}",
          partition.getId(), store.getSizeInBytes(), disk);

      /** TODO: Replicate from server and compare */

      /** Clean up local disks */
      if (!(storageManager.shutdownBlobStore(partition) && storageManager.removeBlobStore(partition))) {
        throw new RuntimeException(String.format("Failed to remove Store %s", store));
      }
      logger.info("[BackupIntegrityMonitor] Removed Store {}", store);
    } catch (Throwable e) {
      // TODO: latency metrics, errors
      logger.error("Failed to run verification due to {}", e.getMessage());
    }
    logger.info("[BackupIntegrityMonitor] Verified backup partition-{}", partition.getId());
  }
}
