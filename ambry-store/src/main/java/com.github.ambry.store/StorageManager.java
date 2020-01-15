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

package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


/**
 * The storage manager that handles all the stores on this node. The stores on each disk are handled by a
 * {@link DiskManager}
 */
public class StorageManager implements StoreManager {
  private final ConcurrentHashMap<PartitionId, DiskManager> partitionToDiskManager = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<DiskId, DiskManager> diskToDiskManager = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ReplicaId> partitionNameToReplicaId = new ConcurrentHashMap<>();
  private final StorageManagerMetrics metrics;
  private final Time time;
  private final StoreConfig storeConfig;
  private final DiskManagerConfig diskManagerConfig;
  private final ScheduledExecutorService scheduler;
  private final StoreMetrics storeMainMetrics;
  private final StoreMetrics storeUnderCompactionMetrics;
  private final StoreKeyFactory keyFactory;
  private final ClusterMap clusterMap;
  private final DataNodeId currentNode;
  private final MessageStoreRecovery recovery;
  private final MessageStoreHardDelete hardDelete;
  private final ReplicaStatusDelegate replicaStatusDelegate;
  private final List<String> stoppedReplicas;
  private final ClusterParticipant clusterParticipant;
  private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

  /**
   * Constructs a {@link StorageManager}
   * @param storeConfig the settings for store configuration.
   * @param diskManagerConfig the settings for disk manager configuration
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param registry the {@link MetricRegistry} used for store-related metrics.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param clusterMap the {@link ClusterMap} instance to use.
   * @param dataNodeId the {@link DataNodeId} of current node.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param clusterParticipant the {@link ClusterParticipant} that allows storage manager to interact with cluster
   *                           manager (i.e Helix)
   * @param time the {@link Time} instance to use.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   */
  public StorageManager(StoreConfig storeConfig, DiskManagerConfig diskManagerConfig,
      ScheduledExecutorService scheduler, MetricRegistry registry, StoreKeyFactory keyFactory, ClusterMap clusterMap,
      DataNodeId dataNodeId, MessageStoreHardDelete hardDelete, ClusterParticipant clusterParticipant, Time time,
      MessageStoreRecovery recovery) throws StoreException {
    verifyConfigs(storeConfig, diskManagerConfig);
    this.storeConfig = storeConfig;
    this.diskManagerConfig = diskManagerConfig;
    this.scheduler = scheduler;
    this.time = time;
    this.keyFactory = keyFactory;
    this.recovery = recovery;
    this.hardDelete = hardDelete;
    this.clusterMap = clusterMap;
    this.clusterParticipant = clusterParticipant;
    currentNode = dataNodeId;
    replicaStatusDelegate = clusterParticipant == null ? null : new ReplicaStatusDelegate(clusterParticipant);
    metrics = new StorageManagerMetrics(registry);
    storeMainMetrics = new StoreMetrics(registry);
    storeUnderCompactionMetrics = new StoreMetrics("UnderCompaction", registry);
    stoppedReplicas =
        replicaStatusDelegate == null ? Collections.emptyList() : replicaStatusDelegate.getStoppedReplicas();
    Map<DiskId, List<ReplicaId>> diskToReplicaMap = new HashMap<>();
    for (ReplicaId replica : clusterMap.getReplicaIds(dataNodeId)) {
      DiskId disk = replica.getDiskId();
      diskToReplicaMap.computeIfAbsent(disk, key -> new ArrayList<>()).add(replica);
      partitionNameToReplicaId.put(replica.getPartitionId().toPathString(), replica);
    }
    for (Map.Entry<DiskId, List<ReplicaId>> entry : diskToReplicaMap.entrySet()) {
      DiskId disk = entry.getKey();
      List<ReplicaId> replicasForDisk = entry.getValue();
      DiskManager diskManager =
          new DiskManager(disk, replicasForDisk, storeConfig, diskManagerConfig, scheduler, metrics, storeMainMetrics,
              storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, replicaStatusDelegate, stoppedReplicas,
              time);
      diskToDiskManager.put(disk, diskManager);
      for (ReplicaId replica : replicasForDisk) {
        partitionToDiskManager.put(replica.getPartitionId(), diskManager);
      }
    }
  }

  /**
   * Verify that the {@link StoreConfig} and {@link DiskManagerConfig} has valid settings.
   * @param storeConfig the {@link StoreConfig} to verify.
   * @param diskManagerConfig the {@link DiskManagerConfig} to verify
   * @throws StoreException if the {@link StoreConfig} or {@link DiskManagerConfig} is invalid.
   */
  private void verifyConfigs(StoreConfig storeConfig, DiskManagerConfig diskManagerConfig) throws StoreException {
    /* NOTE: We must ensure that the store never performs hard deletes on the part of the log that is not yet flushed.
       We do this by making sure that the retention period for deleted messages (which determines the end point for hard
       deletes) is always greater than the log flush period. */
    if (storeConfig.storeDeletedMessageRetentionDays
        < TimeUnit.SECONDS.toDays(storeConfig.storeDataFlushIntervalSeconds) + 1) {
      throw new StoreException("Message retention days must be greater than the store flush interval period",
          StoreErrorCodes.Initialization_Error);
    }
    if (diskManagerConfig.diskManagerReserveFileDirName.length() == 0) {
      throw new StoreException("Reserve file directory name is empty", StoreErrorCodes.Initialization_Error);
    }
  }

  /**
   * Start the {@link DiskManager}s for all disks on this node.
   * @throws InterruptedException
   */
  public void start() throws InterruptedException {
    long startTimeMs = time.milliseconds();
    try {
      logger.info("Starting storage manager");
      List<Thread> startupThreads = new ArrayList<>();
      for (final DiskManager diskManager : diskToDiskManager.values()) {
        Thread thread = Utils.newThread("disk-manager-startup-" + diskManager.getDisk(), () -> {
          try {
            diskManager.start();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Disk manager startup thread interrupted for disk " + diskManager.getDisk(), e);
          }
        }, false);
        thread.start();
        startupThreads.add(thread);
      }
      for (Thread startupThread : startupThreads) {
        startupThread.join();
      }
      metrics.initializeCompactionThreadsTracker(this, diskToDiskManager.size());
      if (clusterParticipant != null) {
        clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.StorageManagerListener,
            new PartitionStateChangeListenerImpl());
      }
      logger.info("Starting storage manager complete");
    } finally {
      metrics.storageManagerStartTimeMs.update(time.milliseconds() - startTimeMs);
    }
  }

  @Override
  public Store getStore(PartitionId id) {
    return getStore(id, false);
  }

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @param skipStateCheck whether to skip checking state of the store. if true, it also returns store that is not started yet.
   * @return the {@link Store} corresponding to the given {@link PartitionId}, or {@code null} if no store was found for
   *         that partition, or that store was not started.
   */
  public Store getStore(PartitionId id, boolean skipStateCheck) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null ? diskManager.getStore(id, skipStateCheck) : null;
  }

  @Override
  public ReplicaId getReplica(String partitionName) {
    return partitionNameToReplicaId.get(partitionName);
  }

  @Override
  public ServerErrorCode checkLocalPartitionStatus(PartitionId partition, ReplicaId localReplica) {
    if (getStore(partition) == null) {
      if (localReplica != null) {
        // check stores on the disk
        if (!isDiskAvailable(localReplica.getDiskId())) {
          return ServerErrorCode.Disk_Unavailable;
        } else {
          return ServerErrorCode.Replica_Unavailable;
        }
      } else {
        return ServerErrorCode.Partition_Unknown;
      }
    }
    return ServerErrorCode.No_Error;
  }

  /**
   * @param id the {@link PartitionId} to find the DiskManager for.
   * @return the {@link DiskManager} corresponding to the given {@link PartitionId}, or {@code null} if no DiskManager was found for
   *         that partition
   */
  DiskManager getDiskManager(PartitionId id) {
    return partitionToDiskManager.get(id);
  }

  /**
   * Check if a certain disk is available.
   * @param disk the {@link DiskId} to check.
   * @return {@code true} if the disk is available. {@code false} if not.
   */
  boolean isDiskAvailable(DiskId disk) {
    DiskManager diskManager = diskToDiskManager.get(disk);
    return diskManager != null && !diskManager.areAllStoresDown();
  }

  @Override
  public boolean scheduleNextForCompaction(PartitionId id) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null && diskManager.scheduleNextForCompaction(id);
  }

  @Override
  public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null && diskManager.controlCompactionForBlobStore(id, enabled);
  }

  /**
   * Shutdown the {@link DiskManager}s for the disks on this node.
   * @throws InterruptedException
   */
  public void shutdown() throws InterruptedException {
    long startTimeMs = time.milliseconds();
    try {
      logger.info("Shutting down storage manager");
      List<Thread> shutdownThreads = new ArrayList<>();
      for (final DiskManager diskManager : diskToDiskManager.values()) {
        Thread thread = Utils.newThread("disk-manager-shutdown-" + diskManager.getDisk(), () -> {
          try {
            diskManager.shutdown();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Disk manager shutdown thread interrupted for disk " + diskManager.getDisk(), e);
          }
        }, false);
        thread.start();
        shutdownThreads.add(thread);
      }
      for (Thread shutdownThread : shutdownThreads) {
        shutdownThread.join();
      }
      metrics.deregisterCompactionThreadsTracker();
      logger.info("Shutting down storage manager complete");
    } finally {
      metrics.storageManagerShutdownTimeMs.update(time.milliseconds() - startTimeMs);
    }
  }

  @Override
  public boolean addBlobStore(ReplicaId replica) {
    if (partitionToDiskManager.containsKey(replica.getPartitionId())) {
      logger.info("{} already exists in storage manager, rejecting adding store request", replica.getPartitionId());
      return false;
    }
    DiskManager diskManager = diskToDiskManager.computeIfAbsent(replica.getDiskId(), disk -> {
      DiskManager newDiskManager =
          new DiskManager(disk, Collections.emptyList(), storeConfig, diskManagerConfig, scheduler, metrics,
              storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, replicaStatusDelegate,
              stoppedReplicas, time);
      logger.info("Creating new DiskManager on {} for new added store", replica.getDiskId().getMountPath());
      try {
        newDiskManager.start();
      } catch (Exception e) {
        logger.error("Error while starting the new DiskManager for " + disk.getMountPath(), e);
        return null;
      }
      return newDiskManager;
    });
    if (diskManager == null || !diskManager.addBlobStore(replica)) {
      logger.error("Failed to add new store into DiskManager");
      return false;
    }
    partitionToDiskManager.put(replica.getPartitionId(), diskManager);
    partitionNameToReplicaId.put(replica.getPartitionId().toPathString(), replica);
    logger.info("New store is successfully added into StorageManager");
    return true;
  }

  @Override
  public boolean startBlobStore(PartitionId id) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null && diskManager.startBlobStore(id);
  }

  @Override
  public boolean shutdownBlobStore(PartitionId id) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null && diskManager.shutdownBlobStore(id);
  }

  @Override
  public boolean removeBlobStore(PartitionId id) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    if (diskManager == null) {
      logger.info("Store {} is not found in storage manager", id);
      return false;
    }
    if (!diskManager.removeBlobStore(id)) {
      logger.error("Fail to remove store {} from disk manager", id);
      return false;
    }
    partitionToDiskManager.remove(id);
    logger.info("Store {} is successfully removed from storage manager", id);
    return true;
  }

  @Override
  public List<PartitionId> setBlobStoreStoppedState(List<PartitionId> partitionIds, boolean markStop) {
    Map<DiskManager, List<PartitionId>> diskManagerToPartitionMap = new HashMap<>();
    List<PartitionId> failToUpdateStores = new ArrayList<>();
    for (PartitionId id : partitionIds) {
      DiskManager diskManager = partitionToDiskManager.get(id);
      if (diskManager != null) {
        diskManagerToPartitionMap.computeIfAbsent(diskManager, disk -> new ArrayList<>()).add(id);
      } else {
        failToUpdateStores.add(id);
      }
    }
    for (Map.Entry<DiskManager, List<PartitionId>> diskToPartitions : diskManagerToPartitionMap.entrySet()) {
      List<PartitionId> failList =
          diskToPartitions.getKey().setBlobStoreStoppedState(diskToPartitions.getValue(), markStop);
      failToUpdateStores.addAll(failList);
    }
    return failToUpdateStores;
  }

  /**
   * @return the number of compaction threads running.
   */
  int getCompactionThreadCount() {
    int count = 0;
    for (DiskManager diskManager : diskToDiskManager.values()) {
      if (diskManager.isCompactionExecutorRunning()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Implementation of {@link PartitionStateChangeListener} to capture state changes and take actions accordingly.
   */
  private class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {
    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      // check if partition exists on current node
      ReplicaId replica = partitionNameToReplicaId.get(partitionName);
      if (replica == null) {
        // there can be two scenarios:
        // 1. this is the first time to add new replica onto current node;
        // 2. last replica addition failed at some point before updating InstanceConfig in Helix
        // In either case, we should add replica to current node by calling "addBlobStore(ReplicaId replica)"
        ReplicaId replicaToAdd = clusterMap.getBootstrapReplica(partitionName, currentNode);
        if (replicaToAdd == null) {
          logger.error("No new replica found for partition {} in cluster map", partitionName);
          throw new StateTransitionException(
              "New replica " + partitionName + " is not found in clustermap for " + currentNode, ReplicaNotFound);
        }
        // Attempt to add store into storage manager. If store already exists, fail adding store request.
        if (!addBlobStore(replicaToAdd)) {
          logger.error("Failed to add store {} into storage manager", partitionName);
          throw new StateTransitionException("Failed to add store " + partitionName + " into storage manager",
              ReplicaOperationFailure);
        }
        if (clusterParticipant != null) {
          // update InstanceConfig in Helix
          try {
            if (!clusterParticipant.updateDataNodeInfoInCluster(replicaToAdd, true)) {
              logger.error("Failed to add partition {} into InstanceConfig for {}", partitionName,
                  currentNode.getHostname());
              throw new StateTransitionException("Failed to add partition " + partitionName + " into InstanceConfig",
                  StateTransitionException.TransitionErrorCode.HelixUpdateFailure);
            }
          } catch (IllegalStateException e) {
            throw new StateTransitionException(e.getMessage(),
                StateTransitionException.TransitionErrorCode.HelixUpdateFailure);
          }
        }
        // note that partitionNameToReplicaId should be updated if addBlobStore succeeds, so replicationManager should be
        // able to get new replica from storageManager without querying Helix
      } else {
        // if the replica is already on current node, there are 3 cases need to discuss:
        // 1. replica was initially present in clustermap;
        // 2. replica was dynamically added to this node but may fail during BOOTSTRAP -> STANDBY transition
        // 3. replica is on current node but its disk is offline. The replica is not able to start.
        // For case 1 and 2, OFFLINE -> BOOTSTRAP is complete, we leave remaining actions (if there any) to other transition.
        // For case 3, we should throw exception to make replica stay in ERROR state (thus, frontends won't pick this replica)
        if (getStore(replica.getPartitionId(), false) == null) {
          throw new StateTransitionException(
              "Store " + partitionName + " didn't start correctly, replica should be set to ERROR state",
              StoreNotStarted);
        }
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      // check if partition exists on current node
      ReplicaId replica = partitionNameToReplicaId.get(partitionName);
      // if replica is null that means partition is not on current node (this shouldn't happen unless we use server admin
      // tool to remove the store before initiating decommission on this partition). We throw exception in this case.
      if (replica != null) {
        // 0. as long as local replica exists, we create a decommission file in its dir
        File decommissionFile = new File(replica.getReplicaPath(), BlobStore.DECOMMISSION_FILE_NAME);
        try {
          if (!decommissionFile.exists()) {
            // if not present, create one.
            decommissionFile.createNewFile();
            logger.info("Decommission file is created for replica {}", replica.getReplicaPath());
          }
        } catch (IOException e) {
          logger.error("IOException occurs when creating decommission file for replica " + partitionName, e);
          throw new StateTransitionException(
              "Couldn't create decommission file for replica " + replica.getReplicaPath(), ReplicaOperationFailure);
        }
        Store localStore = getStore(replica.getPartitionId());
        if (localStore != null) {
          // 1. set state to INACTIVE
          localStore.setCurrentState(ReplicaState.INACTIVE);
          logger.info("Store {} is set to INACTIVE", partitionName);
          // 2. disable compaction on this store
          if (!controlCompactionForBlobStore(replica.getPartitionId(), false)) {
            logger.error("Failed to disable compaction on store {}", partitionName);
            // we set error code to ReplicaNotFound because that is the only reason why compaction may fail.
            throw new StateTransitionException("Couldn't disable compaction on replica " + replica.getReplicaPath(),
                ReplicaNotFound);
          }
          logger.info("Compaction is successfully disabled on store {}", partitionName);
        } else {
          // this may happen when the disk holding this store crashes (or store is stopped by server admin tool)
          throw new StateTransitionException("Store " + partitionName + " is not started", StoreNotStarted);
        }
      } else {
        throw new StateTransitionException("Replica " + partitionName + " is not found on current node",
            ReplicaNotFound);
      }
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      // if code arrives here, which means replica exists on current node. This is guaranteed by replication manager,
      // which checks existence of local replica (see onPartitionBecomeOfflineFromInactive method in ReplicationManager)
      ReplicaId replica = partitionNameToReplicaId.get(partitionName);
      if (!shutdownBlobStore(replica.getPartitionId())) {
        throw new StateTransitionException("Failed to shutdown store " + partitionName, ReplicaOperationFailure);
      }
      logger.info("Store {} is successfully shut down during Inactive-To-Offline transition", partitionName);
    }
  }
}
