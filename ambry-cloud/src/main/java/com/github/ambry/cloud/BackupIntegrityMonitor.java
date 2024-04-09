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
import com.github.ambry.clustermap.AmbryReplica;
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
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.utils.Utils;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
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
  private final ReplicaThread serverScanner;
  private final ReplicationManager serverReplicationManager;
  private final ScheduledExecutorService executor;
  private final StorageManager storageManager;
  public final int RECOVERY_MILESTONE = 10; // num blobs
  public final long SCAN_STOP_RELTIME = TimeUnit.DAYS.toMillis(7);
  public final long SCAN_MILESTONE = TimeUnit.DAYS.toMillis(1);

  public BackupIntegrityMonitor(RecoveryManager azure, ReplicationManager server,
      CompositeClusterManager cluster, StorageManager storage, DataNodeId node,
      VerifiableProperties properties) {
    azureReplicationManager = azure;
    azureReplicator = azure.getRecoveryThread("ambry_backup_integrity_monitor");
    clusterMapConfig = new ClusterMapConfig(properties);
    helixClusterManager = cluster.getHelixClusterManager();
    staticClusterManager = cluster.getStaticClusterManager();
    executor = Utils.newScheduler(1, "ambry_backup_integrity_monitor_", true);
    nodeId = node;
    serverReplicationManager = server;
    serverScanner = server.getBackupCheckerThread("ambry_backup_integrity_monitor");
    storageManager = storage;
    // log disk state
    staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds()
        .forEach(d -> logger.info("[BackupIntegrityMonitor] Disk = {} {} {} bytes",
            d.getMountPath(), d.getState(), d.getRawCapacityInBytes()));
    logger.info("[BackupIntegrityMonitor] Created BackupIntegrityMonitor");
    this.run();
    logger.info("[BackupIntegrityMonitor] Started BackupIntegrityMonitor");
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
    logger.info("[BackupIntegrityMonitor] Shut down  BackupIntegrityMonitor");
  }

  private BlobStore startLocalStore(AmbryPartition partition) throws Exception {
    /**
     * Create local replica L to store cloud data
     * It will be at least as big as the largest replica of the partition on a server.
     * Cloud replica will usually be smaller than server replica, unless server disk has a problem or is compacted.
     */
    long maxReplicaSize = partition.getReplicaIds().stream()
        .map(r -> r.getCapacityInBytes())
        .max(Long::compare)
        .get();
    logger.info("[BackupIntegrityMonitor] maxReplicaSize = {}", maxReplicaSize);
    // TODO: Handle disk full or unavailable
    // TODO: Clear off other partitions on the disk
    List<DiskId> disks = staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds().stream()
        .filter(d -> d.getState() == HardwareState.AVAILABLE)
        .filter(d -> d.getAvailableSpaceInBytes() >= maxReplicaSize)
        .collect(Collectors.toList());
    logger.info("[BackupIntegrityMonitor] disks.len = {}", disks.size());
    // Pick disk randomly
    // DiskId disk = disks.get(new Random().nextInt(disks.size()));
    DiskId disk = disks.stream()
        .filter(d -> d.getMountPath().equals("/mnt/u005/ambrydata"))
        .collect(Collectors.toList())
        .get(0);
    logger.info("[BackupIntegrityMonitor] mountpath = {}", disk.getMountPath());
    // Convert Disk object to AmbryDisk object, static-map -> helix-map
    AmbryDisk ambryDisk = new AmbryDisk((Disk) disk, clusterMapConfig);
    AmbryServerReplica localReplica = new AmbryServerReplica(clusterMapConfig, partition, ambryDisk,
        true, maxReplicaSize, ReplicaSealStatus.NOT_SEALED);
    if (!storageManager.addBlobStore(localReplica)) {
      throw new RuntimeException(String.format("Failed to start Store for %s", partition.getId()));
    }
    Store store = storageManager.getStore(partition, true);
    logger.info("[BackupIntegrityMonitor] Started Store {}", store.toString());
    return (BlobStore) store;
  }

  private boolean stopLocalStore(AmbryPartition partition) {
    Store store = storageManager.getStore(partition, true);
    try {
      boolean ret = storageManager.shutdownBlobStore(partition) && storageManager.removeBlobStore(partition);
      logger.info("[BackupIntegrityMonitor] Stopped Store [{}]", store);
      return ret;
    } catch (Throwable e) {
      logger.error(String.format("Failed to stop Store [%s]", store));
      return false;
    }
  }

  /**
   * This method randomly picks a partition in a cluster and locates its replica in the cluster and the cloud.
   * Then it downloads and compares data from both replicas.
   */
  @Override
  public void run() {
    RemoteReplicaInfo cloudReplica = null;
    AmbryPartition partition = null;
    RemoteReplicaInfo serverReplica = null;
    try {
      // 1. Pick a partition P, replica R from helix cluster-map
      // 2. Pick a disk D using static cluster-map
      // 3. while(!done) { RecoveryThread::replicate(P) } - restore partition P from cloud to disk D
      // 4. while(!done) { BackupCheckerThread::replicate(R) } - copy metadata from R, compare with metadata in D
      Random random = new Random();

      /** Select partition P */
      // TODO: Don't pick the same partition twice
      List<PartitionId> partitions = helixClusterManager.getAllPartitionIds(null);
      // partition = (AmbryPartition) partitions.get(random.nextInt(partitions.size()));
      partition = (AmbryPartition) partitions.stream()
          .filter(p -> p.getId() == 57177)
          .collect(Collectors.toList())
          .get(0);
      logger.info("[BackupIntegrityMonitor] Verifying backup partition-{}", partition.getId());

      /** Create local Store S */
      BlobStore store = startLocalStore(partition);

      /** Restore cloud backup C */
      logger.info("[BackupIntegrityMonitor] Restoring backup partition-{} to disk [{}]", partition.getId(), store);
      cloudReplica = azureReplicationManager.getCloudReplica(store.getReplicaId());
      // TODO: Reload tokens
      azureReplicator.addRemoteReplicaInfo(cloudReplica);
      RecoveryToken token = (RecoveryToken) cloudReplica.getToken();
      while (!token.isEndOfPartition()) {
        azureReplicator.replicate();
        token = (RecoveryToken) cloudReplica.getToken();
        long numBlobs = token.getNumBlobs();
        if (numBlobs > 0 && (numBlobs % RECOVERY_MILESTONE == 0)) {
          // Print progress every N blobs
          logger.info("[BackupIntegrityMonitor] Recovered {} blobs {} bytes of partition-{} from Azure Storage",
              numBlobs, token.getBytesRead(), partition.getId());
          break;
        }
      }
      logger.info("[BackupIntegrityMonitor] Restored backup partition-{} to disk [{}]", partition.getId(), store);

      /** Replicate from server and compare */
      List<AmbryReplica> replicas = partition.getReplicaIds();
      AmbryReplica replica = replicas.get(random.nextInt(replicas.size()));
      logger.info("[BackupIntegrityMonitor] Selected replica [{}]", replica);
      serverReplica = serverReplicationManager.createRemoteReplicaInfos(
          Collections.singletonList(replica), store.getReplicaId()).get(0);
      logger.info("[BackupIntegrityMonitor] Created replica info [{}]", serverReplica);
      serverScanner.addRemoteReplicaInfo(serverReplica);
      serverReplica.getReplicatedUntilUTC();
      logger.info("[BackupIntegrityMonitor] Added replica info [{}]", serverReplica);
      long scanStartTime, scanEndTime = serverReplica.getReplicatedUntilUTC();
      long currentTime = System.currentTimeMillis();
      logger.info("[BackupIntegrityMonitor] Scanning partition-{} from replica [{}]", partition.getId(), replica);
      while (scanEndTime < (currentTime - SCAN_STOP_RELTIME)) {
        scanStartTime = serverReplica.getReplicatedUntilUTC();
        serverScanner.replicate();
        scanEndTime = serverReplica.getReplicatedUntilUTC();
        if (scanEndTime - scanStartTime > SCAN_MILESTONE) {
          // Print progress every N bytes
          DateFormat formatter = new SimpleDateFormat(VcrReplicationManager.DATE_FORMAT);
          logger.info("[BackupIntegrityMonitor] Scanned replica {} until {}, stop at {}",
              replica, formatter.format(scanEndTime), formatter.format(currentTime - SCAN_STOP_RELTIME));
        }
      }
      logger.info("[BackupIntegrityMonitor] Verified backup partition-{} against replica {}", partition.getId(), replica);
    } catch (Throwable e) {
      // TODO: latency metrics, errors
      logger.error("[BackupIntegrityMonitor] Failed to verify backup partition-{} due to {}", partition.getId(),
          e.getMessage());
      e.printStackTrace();
    } finally {
      if (cloudReplica != null) {
        azureReplicator.removeRemoteReplicaInfo(cloudReplica);
      }
      if (serverReplica != null) {
        serverScanner.removeRemoteReplicaInfo(serverReplica);
      }
      if (partition != null) {
        stopLocalStore(partition);
      }
    }
  }
}
