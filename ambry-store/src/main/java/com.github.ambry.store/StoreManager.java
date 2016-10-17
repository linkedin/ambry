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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The store manager that handles all the stores on this node. The stores on each disk are handled by a
 * {@link DiskManager}
 */
public class StoreManager {

  private final StoreConfig config;
  private final ConcurrentMap<PartitionId, DiskManager> partitionToStoreManager = new ConcurrentHashMap<>();
  private final List<DiskManager> diskManagers = new ArrayList<>();
  private static final Logger logger = LoggerFactory.getLogger(StoreManager.class);

  /**
   * Constructs a {@link StoreManager}
   * @param config the settings for store configuration.
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param registry the {@link MetricRegistry} used for store-related metrics.
   * @param replicas all the replicas on this disk.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  public StoreManager(StoreConfig config, ScheduledExecutorService scheduler, MetricRegistry registry,
      List<ReplicaId> replicas, StoreKeyFactory keyFactory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, Time time)
      throws StoreException {
    this.config = config;
    verifyConfigs();

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
          new DiskManager(disk, replicasForDisk, config, scheduler, registry, keyFactory, recovery, hardDelete, time);
      diskManagers.add(diskManager);
      for (ReplicaId replica : replicasForDisk) {
        partitionToStoreManager.put(replica.getPartitionId(), diskManager);
      }
    }
  }

  /**
   * Verify that the {@link StoreConfig} has valid settings.
   * @throws StoreException if the {@link StoreConfig} is invalid.
   */
  private void verifyConfigs()
      throws StoreException {
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
  public void start() {
    logger.info("Starting store manager");
    ScheduledExecutorService diskStartupScheduler =
        Utils.newScheduler(Math.min(diskManagers.size(), config.storeNumDiskStartupThreads), "disk-startup-", false);
    try {
      Map<DiskManager, Future<Void>> futures = new HashMap<>();
      for (final DiskManager diskManager : diskManagers) {
        Future<Void> future = diskStartupScheduler.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws Exception {
            diskManager.start();
            return null;
          }
        });
        futures.put(diskManager, future);
      }
      int numFailed = 0;
      for (Map.Entry<DiskManager, Future<Void>> diskManagerAndFuture : futures.entrySet()) {
        DiskId disk = diskManagerAndFuture.getKey().getDisk();
        try {
          diskManagerAndFuture.getValue().get();
        } catch (ExecutionException e) {
          numFailed++;
          logger.error("Exception while starting disk manager for disk" + disk, e.getCause());
        } catch (InterruptedException e) {
          numFailed++;
          logger.error("Disk manager startup for disk " + disk + " was interrupted", e);
        }
      }
      if (numFailed > 0) {
        logger.error("Could not start " + numFailed + " out of " + diskManagers.size() + " disk managers",
            StoreErrorCodes.Initialization_Error);
      }
    } finally {
      diskStartupScheduler.shutdown();
    }
    logger.info("Starting store manager complete");
  }

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @return the {@link Store} corresponding to the given {@link PartitionId}, or {@code null} if no store was found for
   *         that partition.
   * @throws StoreException if the partition is not on this node, or the store for that partition has not been started.
   */
  public Store getStore(PartitionId id)
      throws StoreException {
    DiskManager diskManager = partitionToStoreManager.get(id);
    if (diskManager == null) {
      throw new StoreException("Could not find partition " + id + " on this node", StoreErrorCodes.Partition_Not_Found);
    }
    return diskManager.getStore(id);
  }

  /**
   * Shutdown the {@link DiskManager}s for the disks on this node.
   * @throws StoreException
   */
  public void shutdown()
      throws StoreException {
    logger.info("Shutting down store manager");
    for (DiskManager diskManager : diskManagers) {
      diskManager.shutdown();
    }
    logger.info("Shutting down store manager complete");
  }
}
