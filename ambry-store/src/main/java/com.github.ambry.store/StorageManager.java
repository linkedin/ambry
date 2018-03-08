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
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.WriteStatusDelegate;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The storage manager that handles all the stores on this node. The stores on each disk are handled by a
 * {@link DiskManager}
 */
public class StorageManager {
  private final Map<PartitionId, DiskManager> partitionToDiskManager = new HashMap<>();
  private final List<DiskManager> diskManagers = new ArrayList<>();
  private final StorageManagerMetrics metrics;
  private final Time time;
  private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

  /**
   * Constructs a {@link StorageManager}
   * @param storeConfig the settings for store configuration.
   * @param diskManagerConfig the settings for disk manager configuration
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param registry the {@link MetricRegistry} used for store-related metrics.
   * @param replicas all the replicas on this disk.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  public StorageManager(StoreConfig storeConfig, DiskManagerConfig diskManagerConfig,
      ScheduledExecutorService scheduler, MetricRegistry registry, List<? extends ReplicaId> replicas,
      StoreKeyFactory keyFactory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
      WriteStatusDelegate writeStatusDelegate, Time time) throws StoreException {
    verifyConfigs(storeConfig, diskManagerConfig);
    metrics = new StorageManagerMetrics(registry);
    StoreMetrics storeMainMetrics = new StoreMetrics(registry);
    StoreMetrics storeUnderCompactionMetrics = new StoreMetrics("UnderCompaction", registry);
    this.time = time;
    Map<DiskId, List<ReplicaId>> diskToReplicaMap = new HashMap<>();
    for (ReplicaId replica : replicas) {
      DiskId disk = replica.getDiskId();
      diskToReplicaMap.computeIfAbsent(disk, key -> new ArrayList<>()).add(replica);
    }
    for (Map.Entry<DiskId, List<ReplicaId>> entry : diskToReplicaMap.entrySet()) {
      DiskId disk = entry.getKey();
      List<ReplicaId> replicasForDisk = entry.getValue();
      DiskManager diskManager =
          new DiskManager(disk, replicasForDisk, storeConfig, diskManagerConfig, scheduler, metrics, storeMainMetrics,
              storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, writeStatusDelegate, time);
      diskManagers.add(diskManager);
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
      for (final DiskManager diskManager : diskManagers) {
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
      metrics.initializeCompactionThreadsTracker(this, diskManagers.size());
      logger.info("Starting storage manager complete");
    } finally {
      metrics.storageManagerStartTimeMs.update(time.milliseconds() - startTimeMs);
    }
  }

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @return the {@link Store} corresponding to the given {@link PartitionId}, or {@code null} if no store was found for
   *         that partition, or that store was not started.
   */
  public Store getStore(PartitionId id) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null ? diskManager.getStore(id) : null;
  }

  /**
   * Schedules the {@link PartitionId} {@code id} for compaction next.
   * @param id the {@link PartitionId} of the {@link Store} to compact.
   * @return {@code true} if the scheduling was successful. {@code false} if not.
   */
  public boolean scheduleNextForCompaction(PartitionId id) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null && diskManager.scheduleNextForCompaction(id);
  }

  /**
   * Disable compaction on the {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link Store} on which compaction is disabled.
   * @return {@code true} if disabling was successful. {@code false} if not.
   */
  public boolean disableCompactionForBlobStore(PartitionId id) {
    DiskManager diskManager = partitionToDiskManager.get(id);
    return diskManager != null && diskManager.disableCompactionForBlobStore(id);
  }

  /**
   * Shutdown the {@link DiskManager}s for the disks on this node.
   * @throws StoreException
   * @throws InterruptedException
   */
  public void shutdown() throws InterruptedException {
    long startTimeMs = time.milliseconds();
    try {
      logger.info("Shutting down storage manager");
      List<Thread> shutdownThreads = new ArrayList<>();
      for (final DiskManager diskManager : diskManagers) {
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
      logger.info("Shutting down storage manager complete");
    } finally {
      metrics.storageManagerShutdownTimeMs.update(time.milliseconds() - startTimeMs);
    }
  }

  /**
   * @return the number of compaction threads running.
   */
  int getCompactionThreadCount() {
    int count = 0;
    for (DiskManager diskManager : diskManagers) {
      if (diskManager.isCompactionExecutorRunning()) {
        count++;
      }
    }
    return count;
  }
}
