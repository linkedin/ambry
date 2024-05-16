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

import com.azure.data.tables.models.TableEntity;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
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
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.BackupCheckerThread;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * BackupIntegrityMonitor monitors the integrity of backup partitions in Azure by comparing them with server partitions.
 * It is NOT thread-safe because it clears entire disk before each run. This is the simplest thing to do rather than
 * being clever about how to manage disk space.
 *
 * We do not want multiple threads or processes of BackupIntegrityMonitor running around in data center as that will put
 * unnecessary load on Azure and on the servers. We want exactly _one_ thread of BackupIntegrityMonitor running per
 * ambry cluster in a data center.
 *
 * Here are the steps BackupIntegrityMonitor follows:
 * 0. Clear all local disks
 * 1. Randomly pick a partition P & its server-replica R from helix cluster-map
 * 2. Pick a local disk D using static cluster-map to store partition data
 * 3. Restore P from Azure to D => while(!done) { RecoveryThread::replicate(P) }
 * 4. Copy metadata from R & compare with metadata in D => while(!done) { BackupCheckerThread::replicate(R) }
 *
 * Because we clear disks in step 0, we must pick a partition randomly in step 1.
 * If we sort and pick partitions from either end of the list, we will never get to the other end of the list because
 * there will always at least be a restart due a deployment.
 */
public class BackupIntegrityMonitor implements Runnable {
  private final Logger logger = LoggerFactory.getLogger(RecoveryThread.class);
  private final ClusterMapConfig clusterMapConfig;
  private final ReplicationConfig replicationConfig;
  private final HelixClusterManager helixClusterManager;
  private final StaticClusterManager staticClusterManager;
  private final DataNodeId nodeId;
  private final RecoveryManager azureReplicationManager;
  private final RecoveryThread azureReplicator;
  private final CompositeClusterManager cluster;
  private final AzureCloudDestinationSync azureSyncClient;
  private BackupCheckerThread serverScanner;
  private final ReplicationManager serverReplicationManager;
  private final ScheduledExecutorService executor;
  private final StorageManager storageManager;
  private final AzureCloudConfig azureConfig;
  public final long SCAN_STOP_RELTIME = TimeUnit.HOURS.toMillis(1);
  public final long SCAN_MILESTONE = TimeUnit.DAYS.toMillis(1);
  private final RecoveryMetrics metrics;
  private final HashSet<Long> seen;

  public BackupIntegrityMonitor(RecoveryManager azure, ReplicationManager server,
      CompositeClusterManager cluster, StorageManager storage, DataNodeId node,
      VerifiableProperties properties) throws ReflectiveOperationException {
    azureReplicationManager = azure;
    azureReplicator = azure.getRecoveryThread("ambry_backup_integrity_monitor");
    azureConfig = new AzureCloudConfig(properties);
    this.cluster = cluster;
    clusterMapConfig = new ClusterMapConfig(properties);
    replicationConfig = new ReplicationConfig(properties);
    helixClusterManager = cluster.getHelixClusterManager();
    staticClusterManager = cluster.getStaticClusterManager();
    executor = Utils.newScheduler(1, "ambry_backup_integrity_monitor_", true);
    nodeId = node;
    metrics = new RecoveryMetrics(cluster.getMetricRegistry());
    serverReplicationManager = server;
    storageManager = storage;
    azureSyncClient = new AzureCloudDestinationSync(properties, helixClusterManager.getMetricRegistry(),
        helixClusterManager, null);
    seen = new HashSet<>();
    // log disk state
    staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds()
        .forEach(d -> logger.info("[BackupIntegrityMonitor] Disk = {} {} {} bytes",
            d.getMountPath(), d.getState(), d.getRawCapacityInBytes()));
    this.run();
    logger.info("[BackupIntegrityMonitor] Created BackupIntegrityMonitor");
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
    logger.info("[BackupIntegrityMonitor] Largest replica for partition {} is {} bytes",
        partition.getId(), maxReplicaSize);
    List<DiskId> disks = staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds().stream()
        .filter(d -> d.getState() == HardwareState.AVAILABLE)
        .filter(d -> d.getAvailableSpaceInBytes() >= maxReplicaSize)
        .filter(d -> d.getMountPath().equals("/mnt/u002/ambrydata"))
        .collect(Collectors.toList());
    logger.info("[BackupIntegrityMonitor] {} disks can accommodate partition-{}", disks.size(), partition.getId());
    // Pick disk randomly
    DiskId disk = disks.get(new Random().nextInt(disks.size()));
    logger.info("[BackupIntegrityMonitor] Selected disk at mount path {}", disk.getMountPath());
    // Convert Disk object to AmbryDisk object, static-map object -> helix-map object
    AmbryDisk ambryDisk = new AmbryDisk((Disk) disk, clusterMapConfig);
    AmbryServerReplica localReplica = new AmbryServerReplica(clusterMapConfig, partition, ambryDisk,
        true, maxReplicaSize, ReplicaSealStatus.NOT_SEALED);
    if (!storageManager.addBlobStore(localReplica)) {
      metrics.backupCheckerRuntimeError.inc();
      throw new RuntimeException(String.format("[BackupIntegrityMonitor] Failed to start Store for %s",
          partition.getId()));
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
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to stop Store [%s]", store));
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
    HashMap<String, MessageInfo> azureBlobs = new HashMap<>();
    serverScanner = serverReplicationManager.getBackupCheckerThread("ambry_backup_integrity_monitor");
    Random random = new Random();
    try {
      /** Select partition P */
      List<PartitionId> partitions = helixClusterManager.getAllPartitionIds(null);
      if (((double) seen.size())/partitions.size() >= 0.9) {
        // If we have seen 90% of the partitions, then just clear the seen-set
        seen.clear();
      }
      partitions = partitions.stream()
          .filter(p -> !seen.contains(p.getId()))
          .filter(p -> p.getId() == 586) // pick an older partition, likely backed up completely and is sealed
          .collect(Collectors.toList());
      partition = (AmbryPartition) partitions.get(0);
      seen.add(partition.getId());

      HashMap<String, TableEntity> tokenMap = new HashMap<>();
      for (AmbryReplica replica : partition.getReplicaIds()) {
        TableEntity entity = azureSyncClient.getTableEntity(azureConfig.azureTableNameReplicaTokens,
            String.valueOf(586), replica.getDataNodeId().getHostname());
        tokenMap.put(replica.getDataNodeId().getHostname(), entity);
      }

      logger.info("[BackupIntegrityMonitor] Verifying backup partition-{}", partition.getId());

      /** Create local Store S */
      BlobStore store = startLocalStore(partition);

      /** Create a temporary map of all keys recovered from cloud */
      StoreFindToken newDiskToken = new StoreFindToken(), oldDiskToken = null;
      while (!newDiskToken.equals(oldDiskToken)) {
        FindInfo finfo = store.findEntriesSince(newDiskToken, 1000 * (2 << 20),
            null, null);
        finfo.getMessageEntries().forEach(msg -> azureBlobs.put(msg.getStoreKey().getID(), msg));
        oldDiskToken = newDiskToken;
        newDiskToken = (StoreFindToken) finfo.getFindToken();
        logger.info("[BackupIntegrityMonitor] Disk-token = {}", newDiskToken.toString());
      }
      String blob = "AAYAAQQkAAgAAQAAAAAAAAJKgAIZk8nPQRmt92k8oIxEvQ";
      BlobId blobId = new BlobId(blob, cluster.getHelixClusterManager());
      try {
        store.findKey(blobId);
        logger.info("[BackupIntegrityMonitor] local store has {}", blob);
      } catch (StoreException e) {
        logger.error("[BackupIntegrityMonitor] {}", e.toString());
      }
      if (azureBlobs.containsKey(blob)) {
        logger.info("[BackupIntegrityMonitor] azure blobs has {}", blob);
      }
      serverScanner.setAzureBlobMap(azureBlobs);

      /** Replicate from server and compare */
      // If we filter for SEALED replicas, then we may return empty as there may be no sealed replicas
      List<AmbryReplica> replicas = partition.getReplicaIds().stream()
          .filter(r -> !r.isDown())
          .filter(r -> r.getDataNodeId().getHostname().equals("lva1-app5972.corp.linkedin.com")) // FIXME
          .filter(r -> tokenMap.get(r.getDataNodeId().getHostname()) != null)
          .collect(Collectors.toList());
      if (replicas.isEmpty()) {
        throw new RuntimeException(String.format("[BackupIntegrityMonitor] No server replicas available for partition-%s",
            partition.getId()));
      }
      AmbryReplica replica = replicas.get(random.nextInt(replicas.size()));
      logger.info("[BackupIntegrityMonitor] Selected peer server replica [{}]", replica);
      serverReplica = serverReplicationManager.createRemoteReplicaInfos(
          Collections.singletonList(replica), store.getReplicaId()).get(0);
      logger.info("[BackupIntegrityMonitor] Created peer server replica info [{}]", serverReplica);
      serverScanner.addRemoteReplicaInfo(serverReplica);
      logger.info("[BackupIntegrityMonitor] Queued peer server replica info [{}]", serverReplica);
      long scanStartTime, scanEndTime = serverReplica.getReplicatedUntilTime();
      long scanStopTime = System.currentTimeMillis() - SCAN_STOP_RELTIME;
      String replUntil = (String) tokenMap.get(serverReplica.getReplicaId().getDataNodeId().getHostname())
          .getProperties().get(VcrReplicationManager.REPLICATED_UNITL_UTC);
      if (!replUntil.equals(String.valueOf(Utils.Infinite_Time))) {
        DateFormat formatter = new SimpleDateFormat(VcrReplicationManager.DATE_FORMAT);
        scanStopTime = formatter.parse(replUntil).getTime();
      }
      logger.info("[BackupIntegrityMonitor] Scanning partition-{} from peer server replica [{}] until {} ({} ms)",
          partition.getId(), replica, replUntil, scanStopTime);
      while (scanEndTime < scanStopTime) {
        scanStartTime = serverReplica.getReplicatedUntilTime();
        serverScanner.replicate();
        scanEndTime = serverReplica.getReplicatedUntilTime();
        if (scanEndTime - scanStartTime > SCAN_MILESTONE) {
          // Print progress, if a SCAN_MILESTONE's worth of data has been received from server
          DateFormat formatter = new SimpleDateFormat(VcrReplicationManager.DATE_FORMAT);
          logger.info("[BackupIntegrityMonitor] Scanned {} num_server_blobs from peer server replica {} until {}, stop at {}",
              serverScanner.getNumBlobScanned(), replica, formatter.format(scanEndTime),
              formatter.format(scanStopTime));
        }
      }
      logger.info("[BackupIntegrityMonitor] Scanned {} num_server_blobs from peer server replica {}",
          serverScanner.getNumBlobScanned(), replica);

      serverScanner.printKeysAbsentInServer(serverReplica);

      ReplicationMetrics rmetrics = new ReplicationMetrics(cluster.getMetricRegistry(), Collections.emptyList());
      logger.info("[BackupIntegrityMonitor] Verified cloud backup partition-{} against peer server replica {}"
              + ", num_azure_blobs = {}, num_server_blobs = {}, num_mismatches = {}",
          partition.getId(), replica, -1, serverScanner.getNumBlobScanned(),
          rmetrics.backupIntegrityError.getCount());
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error("[BackupIntegrityMonitor] Failed to verify cloud backup partition-{} due to {}", partition.getId(),
          e.getMessage());
      // Swallow all exceptions and print a trace for inspection, but do not kill the job
      e.printStackTrace();
    }

    // Separate try-catch to swallow exceptions during shutdown
    try {
      azureBlobs.clear();
      if (cloudReplica != null) {
        azureReplicator.removeRemoteReplicaInfo(cloudReplica);
      }
      if (serverReplica != null) {
        serverScanner.removeRemoteReplicaInfo(serverReplica);
      }
      if (partition != null) {
        // stopLocalStore(partition); // FIXME
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error("[BackupIntegrityMonitor] Failed to stop due to {}", e.getMessage());
      e.printStackTrace();
    }
  }
}
