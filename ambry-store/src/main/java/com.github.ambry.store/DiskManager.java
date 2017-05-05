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
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages all the stores on a disk.
 */
class DiskManager {
  private final Map<PartitionId, BlobStore> stores = new HashMap<>();
  private final DiskId disk;
  private final StorageManagerMetrics metrics;
  private final Time time;
  private final DiskIOScheduler diskIOScheduler;
  private final CompactionManager compactionManager;

  private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

  /**
   * Constructs a {@link DiskManager}
   * @param disk representation of the disk.
   * @param replicas all the replicas on this disk.
   * @param config the settings for store configuration.
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param metrics the {@link StorageManagerMetrics} object used for store-related metrics.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  DiskManager(DiskId disk, List<ReplicaId> replicas, StoreConfig config, ScheduledExecutorService scheduler,
      StorageManagerMetrics metrics, StoreKeyFactory keyFactory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, Time time) {
    this.disk = disk;
    this.metrics = metrics;
    this.time = time;
    diskIOScheduler = new DiskIOScheduler(getThrottlers(config, time));
    for (ReplicaId replica : replicas) {
      if (disk.equals(replica.getDiskId())) {
        String storeId = replica.getPartitionId().toString();
        BlobStore store = new BlobStore(storeId, config, scheduler, diskIOScheduler, metrics, replica.getReplicaPath(),
            replica.getCapacityInBytes(), keyFactory, recovery, hardDelete, time);
        stores.put(replica.getPartitionId(), store);
      }
    }
    compactionManager = new CompactionManager(disk.getMountPath(), config, stores.values(), metrics, time);
  }

  /**
   * Starts all the stores on this disk.
   * @throws InterruptedException
   */
  void start() throws InterruptedException {
    long startTimeMs = time.milliseconds();
    try {
      File mountPath = new File(disk.getMountPath());
      if (mountPath.exists()) {
        List<Thread> startupThreads = new ArrayList<>();
        final AtomicInteger numFailures = new AtomicInteger(0);
        for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
          Thread thread = Utils.newThread("store-startup-" + partitionAndStore.getKey(), new Runnable() {
            @Override
            public void run() {
              try {
                partitionAndStore.getValue().start();
              } catch (Exception e) {
                numFailures.incrementAndGet();
                metrics.totalStoreStartFailures.inc();
                logger.error("Exception while starting store for the partition" + partitionAndStore.getKey(), e);
              }
            }
          }, false);
          thread.start();
          startupThreads.add(thread);
        }
        for (Thread startupThread : startupThreads) {
          startupThread.join();
        }
        if (numFailures.get() > 0) {
          logger.error(
              "Could not start " + numFailures.get() + " out of " + stores.size() + " stores on the disk " + disk);
        }
        compactionManager.enable();
      } else {
        metrics.diskMountPathFailures.inc();
        metrics.totalStoreStartFailures.inc(stores.size());
        logger.error("Mount path does not exist: " + mountPath + " ; cannot start stores on this disk");
      }
    } finally {
     metrics.diskStartTimeMs.update(time.milliseconds() - startTimeMs);
    }
  }

  /**
   * Shuts down all the stores on this disk.
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException {
    long startTimeMs = time.milliseconds();
    try {
      compactionManager.disable();
      final AtomicInteger numFailures = new AtomicInteger(0);
      List<Thread> shutdownThreads = new ArrayList<>();
      for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
        Thread thread = Utils.newThread("store-shutdown-" + partitionAndStore.getKey(), new Runnable() {
          @Override
          public void run() {
            try {
              partitionAndStore.getValue().shutdown();
            } catch (Exception e) {
              numFailures.incrementAndGet();
              metrics.totalStoreShutdownFailures.inc();
              logger.error("Exception while shutting down store {} on disk {}", partitionAndStore.getKey(), disk, e);
            }
          }
        }, false);
        thread.start();
        shutdownThreads.add(thread);
      }
      for (Thread shutdownThread : shutdownThreads) {
        shutdownThread.join();
      }
      if (numFailures.get() > 0) {
        logger.error(
            "Could not shutdown " + numFailures.get() + " out of " + stores.size() + " stores on the disk " + disk);
      }
      compactionManager.awaitTermination();
      diskIOScheduler.close();
    } finally {
      metrics.diskShutdownTimeMs.update(time.milliseconds() - startTimeMs);
    }
  }

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @return the associated {@link Store}, or {@code null} if the partition is not on this disk, or the store is not
   *         started.
   */
  Store getStore(PartitionId id) {
    BlobStore store = stores.get(id);
    return (store != null && store.isStarted()) ? store : null;
  }

  /**
   * @return {@code true} if the compaction thread is running. {@code false} otherwise.
   */
  boolean isCompactionExecutorRunning() {
    return compactionManager.isCompactionExecutorRunning();
  }

  /**
   * @return the {@link DiskId} that is managed by this {@link DiskManager}.
   */
  DiskId getDisk() {
    return disk;
  }

  /**
   * Schedules the {@link PartitionId} {@code id} for compaction next.
   * @param id the {@link PartitionId} of the {@link BlobStore} to compact.
   * @return {@code true} if the scheduling was successful. {@code false} if not.
   */
  boolean scheduleNextForCompaction(PartitionId id) {
    BlobStore store = (BlobStore) getStore(id);
    return store != null && compactionManager.scheduleNextForCompaction(store);
  }

  /**
   * Gets all the throttlers that the {@link DiskIOScheduler} will be constructed with.
   * @param config the {@link StoreConfig} with configuration values.
   * @param time the {@link Time} instance to use in the throttlers
   * @return the throttlers that the {@link DiskIOScheduler} will be constructed with.
   */
  private Map<String, Throttler> getThrottlers(StoreConfig config, Time time) {
    Map<String, Throttler> throttlers = new HashMap<>();
    // compaction
    Throttler compactionCopyThrottler = new Throttler(config.storeCleanupOperationsBytesPerSec, 1000, true, time);
    throttlers.put(BlobStoreCompactor.LOG_SEGMENT_COPY_JOB_NAME, compactionCopyThrottler);
    return throttlers;
  }
}
