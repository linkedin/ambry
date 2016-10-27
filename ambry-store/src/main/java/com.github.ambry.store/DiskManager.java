/*
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

import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.store.StoreMetrics.DiskLevelMetrics;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages all the stores on a disk.
 */
class DiskManager {
  private final ConcurrentMap<PartitionId, BlobStore> stores = new ConcurrentHashMap<>();
  private final DiskId disk;
  private final StoreConfig config;
  private final DiskLevelMetrics metrics;

  private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

  /**
   * Constructs a {@link DiskManager}
   * @param disk representation of the disk.
   * @param replicas all the replicas on this disk.
   * @param config the settings for store configuration.
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param storeMetrics the {@link StoreMetrics} object used for store-related metrics.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  DiskManager(DiskId disk, List<ReplicaId> replicas, StoreConfig config, ScheduledExecutorService scheduler,
      StoreMetrics storeMetrics, StoreKeyFactory keyFactory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, Time time) {
    this.disk = disk;
    this.config = config;
    this.metrics = storeMetrics.createDiskLevelMetrics(disk.getMountPath());
    DiskIOScheduler ioScheduler = new DiskIOScheduler(null);
    for (ReplicaId replica : replicas) {
      if (disk.equals(replica.getDiskId())) {
        String storeId = replica.getPartitionId().toString();
        BlobStore store = new BlobStore(storeId, config, scheduler, ioScheduler, metrics, replica.getReplicaPath(),
            replica.getCapacityInBytes(), keyFactory, recovery, hardDelete, time);
        stores.put(replica.getPartitionId(), store);
      }
    }
  }

  /**
   * Starts all the stores on this disk.
   */
  void start()
      throws InterruptedException {
    Timer.Context context = metrics.diskStartTime.time();
    try {
      File mountPath = new File(disk.getMountPath());
      if (mountPath.exists()) {
        Map<StoreStarter, Thread> startupThreads = new HashMap<>();
        for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
          StoreStarter storeStarter = new StoreStarter(partitionAndStore.getKey(), partitionAndStore.getValue());
          Thread thread = Utils.newThread("store-startup-" + partitionAndStore.getKey(), storeStarter, false);
          thread.start();
          startupThreads.put(storeStarter, thread);
        }
        int numFailed = 0;
        for (Map.Entry<StoreStarter, Thread> starterAndThread : startupThreads.entrySet()) {
          starterAndThread.getValue().join();
          Exception exception = starterAndThread.getKey().exception;
          if (exception != null) {
            numFailed++;
            logger.error("Exception while starting store for the partition" + starterAndThread.getKey().partition,
                exception);
          }
        }
        if (numFailed > 0) {
          metrics.diskStartFailure.inc();
          logger.error("Could not start " + numFailed + " out of " + stores.size() + " stores on the disk " + disk);
        }
      } else {
        metrics.diskStartFailure.inc();
        logger.error("Mount path does not exist: " + mountPath + " ; cannot start stores on this disk");
      }
    } finally {
      context.stop();
    }
  }

  /**
   * Shuts down all the stores this disk.
   */
  void shutdown() {
    for (Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
      try {
        partitionAndStore.getValue().shutdown();
      } catch (Exception e) {
        logger.error(
            "Exception while shutting down store for partition " + partitionAndStore.getKey() + " on disk " + disk);
      }
    }
  }

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @return the associated {@link Store}, or {@code null} if the partition is not on this disk, or the store is not
   *         started.
   */
  Store getStore(PartitionId id) {
    BlobStore store = stores.get(id);
    return store.isStarted() ? store : null;
  }

  /**
   * @return the {@link DiskId} that is managed by this {@link DiskManager}.
   */
  DiskId getDisk() {
    return disk;
  }

  /**
   * A {@link Runnable} that starts a {@link Store} and saves a reference to an exception if starting failed.
   */
  private class StoreStarter implements Runnable {
    final PartitionId partition;
    final Store store;
    volatile Exception exception = null;

    public StoreStarter(PartitionId partition, Store store) {
      this.partition = partition;
      this.store = store;
    }

    @Override
    public void run() {
      try {
        store.start();
      } catch (Exception e) {
        exception = e;
      }
    }
  }
}
