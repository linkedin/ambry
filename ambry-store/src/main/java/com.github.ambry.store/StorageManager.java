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
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
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
  private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

  /**
   * Constructs a {@link StorageManager}
   * @param config the settings for store configuration.
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param registry the {@link MetricRegistry} used for store-related metrics.
   * @param replicas all the replicas on this disk.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  public StorageManager(StoreConfig config, ScheduledExecutorService scheduler, MetricRegistry registry,
      List<ReplicaId> replicas, StoreKeyFactory keyFactory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, Time time) throws StoreException {
    verifyConfigs(config);
    metrics = new StorageManagerMetrics(registry);

    Map<DiskId, List<ReplicaId>> diskToReplicaMap = new HashMap<>();
    for (ReplicaId replica : replicas) {
      DiskId disk = replica.getDiskId();
      List<ReplicaId> replicasForDisk = diskToReplicaMap.get(disk);
      if (replicasForDisk == null) {
        replicasForDisk = new ArrayList<>();
        diskToReplicaMap.put(disk, replicasForDisk);
      }
      replicasForDisk.add(replica);
    }
    for (Map.Entry<DiskId, List<ReplicaId>> entry : diskToReplicaMap.entrySet()) {
      DiskId disk = entry.getKey();
      List<ReplicaId> replicasForDisk = entry.getValue();
      DiskManager diskManager =
          new DiskManager(disk, replicasForDisk, config, scheduler, metrics, keyFactory, recovery, hardDelete, time);
      diskManagers.add(diskManager);
      for (ReplicaId replica : replicasForDisk) {
        partitionToDiskManager.put(replica.getPartitionId(), diskManager);
      }
    }
  }

  /**
   * Verify that the {@link StoreConfig} has valid settings.
   * @param config the {@link StoreConfig} to verify.
   * @throws StoreException if the {@link StoreConfig} is invalid.
   */
  private void verifyConfigs(StoreConfig config) throws StoreException {
    /* NOTE: We must ensure that the store never performs hard deletes on the part of the log that is not yet flushed.
       We do this by making sure that the retention period for deleted messages (which determines the end point for hard
       deletes) is always greater than the log flush period. */
    if (config.storeDeletedMessageRetentionDays < config.storeDataFlushIntervalSeconds / Time.SecsPerDay + 1) {
      throw new StoreException("Message retention days must be greater than the store flush interval period",
          StoreErrorCodes.Initialization_Error);
    }
  }

  /**
   * Start the {@link DiskManager}s for all disks on this node.
   */
  public void start() throws InterruptedException {
    logger.info("Starting storage manager");
    List<Thread> startupThreads = new ArrayList<>();
    for (final DiskManager diskManager : diskManagers) {
      Thread thread = Utils.newThread("disk-manager-startup-" + diskManager.getDisk(), new Runnable() {
        @Override
        public void run() {
          try {
            diskManager.start();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Disk manager startup thread interrupted for disk " + diskManager.getDisk(), e);
          }
        }
      }, false);
      thread.start();
      startupThreads.add(thread);
    }
    for (Thread startupThread : startupThreads) {
      startupThread.join();
    }
    logger.info("Starting storage manager complete");
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
   * Shutdown the {@link DiskManager}s for the disks on this node.
   * @throws StoreException
   */
  public void shutdown() throws StoreException {
    logger.info("Shutting down storage manager");
    for (DiskManager diskManager : diskManagers) {
      diskManager.shutdown();
    }
    logger.info("Shutting down storage manager complete");
  }
}
