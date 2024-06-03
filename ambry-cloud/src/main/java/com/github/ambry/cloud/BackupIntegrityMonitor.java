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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.replication.BackupCheckerThread;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
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
  public final long SCAN_MILESTONE = TimeUnit.DAYS.toMillis(1);
  private final RecoveryMetrics metrics;
  private final HashSet<Long> seen;

  class AzureBlobInfo {
    public HashMap<String, MessageInfo> azureBlobs;
    public long partitionBackedUpUntil;
    public AzureBlobInfo(HashMap<String, MessageInfo> map, long t) {
      azureBlobs = map;
      partitionBackedUpUntil = t;
    }
  }

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
        .collect(Collectors.toList());
    logger.info("[BackupIntegrityMonitor] {} disks can accommodate partition-{}", disks.size(), partition.getId());
    // Pick disk randomly; any disk is ok as we will wipe it out after this
    DiskId disk = disks.get(new Random().nextInt(disks.size()));
    logger.info("[BackupIntegrityMonitor] Selected disk at mount path {}", disk.getMountPath());
    // Clear disk to make space, this is simpler instead of deciding which partition to delete.
    // This is why this is thread-unsafe.
    Arrays.stream(new File(disk.getMountPath()).listFiles()).forEach(f -> {
      try {
        Utils.deleteFileOrDirectory(f);
      } catch (Throwable e) {
        metrics.backupCheckerRuntimeError.inc();
        throw new RuntimeException(String.format("[BackupIntegrityMonitor] Failed to delete %s due to %s", f, e));
      }
    });
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
      // Don't remove the partition from disk, leave it behind for debugging
      boolean ret = storageManager.shutdownBlobStore(partition);
      logger.info("[BackupIntegrityMonitor] Stopped Store [{}]", store);
      return ret;
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to stop Store [%s]", store));
      return false;
    }
  }

  /**
   * Compares metadata received from servers with metadata received from Azure cloud
   * @param serverReplica
   * @param cloudReplica
   * @param partitionBackedUpUntil
   */
  void compareMetadata(RemoteReplicaInfo serverReplica, RemoteReplicaInfo cloudReplica, long partitionBackedUpUntil) {
    long partitionId = serverReplica.getReplicaId().getPartitionId().getId();
    long scanStartTime, scanEndTime = serverReplica.getReplicatedUntilTime();
    DateFormat formatter = new SimpleDateFormat(VcrReplicationManager.DATE_FORMAT);
    try {
      serverScanner.addRemoteReplicaInfo(serverReplica);
      logger.info("[BackupIntegrityMonitor] Queued peer server replica for scan [{}]", serverReplica);

      while (scanEndTime < partitionBackedUpUntil) {
        scanStartTime = serverReplica.getReplicatedUntilTime();
        serverScanner.replicate();
        scanEndTime = serverReplica.getReplicatedUntilTime();
        if (scanEndTime - scanStartTime > SCAN_MILESTONE) {
          // Print progress, if a SCAN_MILESTONE's worth of data has been received from server
          logger.info("[BackupIntegrityMonitor] Scanned {} num_server_blobs from peer server replica {} until {}, stop at {}",
              serverScanner.getNumBlobScanned(), serverReplica, formatter.format(scanEndTime),
              formatter.format(partitionBackedUpUntil));
        }
      }
      logger.info("[BackupIntegrityMonitor] Scanned {} num_server_blobs from peer server replica {}",
          serverScanner.getNumBlobScanned(), serverReplica);

      serverScanner.printKeysAbsentInServer(serverReplica);
      ReplicationMetrics rmetrics = new ReplicationMetrics(cluster.getMetricRegistry(), Collections.emptyList());
      logger.info("[BackupIntegrityMonitor] Verified cloud backup partition-{} against peer server replica {}"
              + ", num_azure_blobs = {}, num_server_blobs = {}, num_mismatches = {}",
          partitionId, serverReplica, ((RecoveryToken) cloudReplica.getToken()).getNumBlobs(),
          serverScanner.getNumBlobScanned(),
          rmetrics.backupIntegrityError.getCount());
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to verify server replica %s due to",
          serverReplica), e);
      // Swallow all exceptions and print a trace for inspection, but do not kill the job
    }

    try {
      if (serverReplica != null) {
        serverScanner.removeRemoteReplicaInfo(serverReplica);
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to dequeue server replica %s due to",
          serverReplica), e);
      // Swallow all exceptions and print a trace for inspection, but do not kill the job
    }
  }

  /**
   * Returns CRC of blob-content
   * @param msg
   * @param store
   * @return CRC
   */
  Long getBlobContentCRC(MessageInfo msg, BlobStore store) {
    EnumSet<StoreGetOptions> storeGetOptions = EnumSet.of(StoreGetOptions.Store_Include_Deleted,
        StoreGetOptions.Store_Include_Expired);
    MessageReadSet rdset = null;
    Long crc = null;
    try {
      StoreInfo stinfo = store.get(Collections.singletonList(msg.getStoreKey()), storeGetOptions);
      rdset = stinfo.getMessageReadSet();
      MessageInfo minfo = stinfo.getMessageReadSetInfo().get(0);
      rdset.doPrefetch(0, minfo.getSize() - MessageFormatRecord.Crc_Size,
          MessageFormatRecord.Crc_Size);
      crc = rdset.getPrefetchedData(0).getLong(0);
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      String err = String.format("[BackupIntegrityMonitor] Failed to get CRC for blob %s due to",
          msg.getStoreKey().getID());
      logger.error(err, e);
    } finally {
      if (rdset != null && rdset.count() > 0 && rdset.getPrefetchedData(0) != null) {
        rdset.getPrefetchedData(0).release();
      }
    }
    return crc;
  }


  /**
   * Create a temporary map of all keys recovered from cloud
   * @param store
   * @return
   * @throws StoreException
   */
  AzureBlobInfo getAzureBlobInfoFromLocalStore(BlobStore store) throws StoreException {
    HashMap<String, MessageInfo> azureBlobs = new HashMap<>();
    StoreFindToken newDiskToken = new StoreFindToken(), oldDiskToken = null;
    long partitionBackedUpUntil = Utils.Infinite_Time;
    while (!newDiskToken.equals(oldDiskToken)) {
      FindInfo finfo = store.findEntriesSince(newDiskToken, 1000 * (2 << 20),
          null, null);
      for (MessageInfo msg: finfo.getMessageEntries()) {
        azureBlobs.put(msg.getStoreKey().getID(), new MessageInfo(msg, getBlobContentCRC(msg, store)));
        partitionBackedUpUntil = Math.max(partitionBackedUpUntil, msg.getOperationTimeMs());
      }
      oldDiskToken = newDiskToken;
      newDiskToken = (StoreFindToken) finfo.getFindToken();
      logger.info("[BackupIntegrityMonitor] Disk-token = {}", newDiskToken.toString());
    }
    return new AzureBlobInfo(azureBlobs, partitionBackedUpUntil);
  }


  /**
   * This method randomly picks a partition in a cluster and locates its replica in the cluster and the cloud.
   * Then it downloads and compares data from both replicas.
   */
  @Override
  public void run() {
    RemoteReplicaInfo cloudReplica = null;
    AmbryPartition partition = null;
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
          .filter(p -> p.getId() <= 2000) // pick an older partition, likely backed up completely and is sealed
          .collect(Collectors.toList());
      partition = (AmbryPartition) partitions.get(random.nextInt(partitions.size()));
      seen.add(partition.getId());
      logger.info("[BackupIntegrityMonitor] Verifying backup partition-{}", partition.getId());

      /** Create local Store S */
      BlobStore store = startLocalStore(partition);

      /** Restore cloud backup C */
      logger.info("[BackupIntegrityMonitor] Restoring backup partition-{} to disk [{}]", partition.getId(), store);
      cloudReplica = azureReplicationManager.getCloudReplica(store.getReplicaId());
      // No need to reload tokens, since we clear off disks before each run
      azureReplicator.addRemoteReplicaInfo(cloudReplica);
      RecoveryToken azureToken = (RecoveryToken) cloudReplica.getToken();
      while (!azureToken.isEndOfPartition()) {
        azureReplicator.replicate();
        azureToken = (RecoveryToken) cloudReplica.getToken();
        long numBlobs = azureToken.getNumBlobs();
        if (numBlobs > 0 && (numBlobs % azureConfig.azureBlobStorageMaxResultsPerPage == 0)) {
          // Print progress, if N blobs have been restored from Azure
          logger.info("[BackupIntegrityMonitor] Recovered {} num_azure_blobs {} bytes of partition-{} from Azure Storage",
              numBlobs, azureToken.getBytesRead(), partition.getId());
        }
      }
      logger.info("[BackupIntegrityMonitor] Recovered {} num_azure_blobs {} bytes of partition-{} from Azure Storage",
          azureToken.getNumBlobs(), azureToken.getBytesRead(), partition.getId());

      /** Replicate from server and compare */
      // If we filter for SEALED replicas, then we may return empty as there may be no sealed replicas
      List<AmbryReplica> replicas = partition.getReplicaIds().stream()
          .filter(r -> r.getDataNodeId().getDatacenterName().equals(clusterMapConfig.clustermapVcrDatacenterName))
          .filter(r -> !r.isDown())
          .collect(Collectors.toList());
      if (replicas.isEmpty()) {
        throw new RuntimeException(String.format("[BackupIntegrityMonitor] No server replicas available for partition-%s",
            partition.getId()));
      }

      /** Compare metadata from server replicas with metadata from Azure */
      List<RemoteReplicaInfo> serverReplicas =
          serverReplicationManager.createRemoteReplicaInfos(replicas, store.getReplicaId());
      DateFormat formatter = new SimpleDateFormat(VcrReplicationManager.DATE_FORMAT);
      for (RemoteReplicaInfo serverReplica : serverReplicas) {
        /**
         * Find out how far each replica of the partition has been backed-up in Azure.
         * We will use this information to scan the replica until when it has been backed-up.
         * And then compare data in the replica until that point with the backup in Azure.
         */
        AzureBlobInfo azureBlobInfo = getAzureBlobInfoFromLocalStore(store);
        if (azureToken.getNumBlobs() != azureBlobInfo.azureBlobs.size()) {
          metrics.backupCheckerRuntimeError.inc();
          logger.error("[BackupIntegrityMonitor] Mismatch, num_azure_blobs = {}, num_azure_blobs on-disk = {}",
              azureToken.getNumBlobs(), azureBlobInfo.azureBlobs.size());
        }
        logger.info("[BackupIntegrityMonitor] Scanning partition-{} from peer server replica [{}] until {} ({} ms)",
            partition.getId(), serverReplica, formatter.format(azureBlobInfo.partitionBackedUpUntil),
            azureBlobInfo.partitionBackedUpUntil);
        serverScanner.setAzureBlobInfo(azureBlobInfo.azureBlobs, azureBlobInfo.partitionBackedUpUntil);
        compareMetadata(serverReplica, cloudReplica, azureBlobInfo.partitionBackedUpUntil);
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to verify cloud backup partition-%s due to",
          partition.getId()), e);
      // Swallow all exceptions and print a trace for inspection, but do not kill the job
    }

    try {
      if (cloudReplica != null) {
        azureReplicator.removeRemoteReplicaInfo(cloudReplica);
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error("[BackupIntegrityMonitor] Failed to stop due to", e);
    }

    try {
      if (partition != null) {
        stopLocalStore(partition);
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error("[BackupIntegrityMonitor] Failed to stop due to", e);
    }
  }
}
