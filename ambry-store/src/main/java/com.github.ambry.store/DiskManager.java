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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
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
 * Manages all the stores on a disk.
 */
class DiskManager {
  private final ConcurrentMap<PartitionId, BlobStore> stores = new ConcurrentHashMap<>();
  private final DiskId disk;
  private final StoreConfig config;

  private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

  /**
   * Constructs a {@link DiskManager}
   * @param disk representation of the disk.
   * @param replicas all the replicas on this disk.
   * @param config the settings for store configuration.
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param registry the {@link MetricRegistry} used for store-related metrics.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  DiskManager(DiskId disk, List<ReplicaId> replicas, StoreConfig config, ScheduledExecutorService scheduler,
      MetricRegistry registry, StoreKeyFactory keyFactory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete, Time time) {
    this.disk = disk;
    this.config = config;
    DiskIOScheduler ioScheduler = new DiskIOScheduler(null);
    for (ReplicaId replica : replicas) {
      if (disk.equals(replica.getDiskId())) {
        String storeId = replica.getPartitionId().toString();
        BlobStore store = new BlobStore(storeId, config, scheduler, ioScheduler, registry, replica.getReplicaPath(),
            replica.getCapacityInBytes(), keyFactory, recovery, hardDelete, time);
        stores.put(replica.getPartitionId(), store);
      }
    }
  }

  /**
   * Starts all the stores on this disk.
   */
  void start()
      throws StoreException {
    File mountPath = new File(disk.getMountPath());
    if (mountPath.exists()) {
      ScheduledExecutorService storeStartupScheduler = Utils
          .newScheduler(Math.min(stores.size(), config.storeNumStoreStartupThreads), "store-startup-" + disk, false);
      try {
        Map<PartitionId, Future<Void>> futures = new HashMap<>();
        for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
          Future<Void> future = storeStartupScheduler.submit(new Callable<Void>() {
            @Override
            public Void call()
                throws Exception {
              partitionAndStore.getValue().start();
              return null;
            }
          });
          futures.put(partitionAndStore.getKey(), future);
        }
        int numFailed = 0;
        for (Map.Entry<PartitionId, Future<Void>> partitionAndFuture : futures.entrySet()) {
          try {
            partitionAndFuture.getValue().get();
          } catch (ExecutionException e) {
            numFailed++;
            logger.error("Exception while starting store for partition " + partitionAndFuture.getKey(), e.getCause());
          } catch (InterruptedException e) {
            numFailed++;
            logger.error("Store startup for partition " + partitionAndFuture.getKey() + " was interrupted", e);
          }
        }
        if (numFailed > 0) {
          throw new StoreException(
              "Could not start " + numFailed + " out of " + stores.size() + " stores on the disk " + disk,
              StoreErrorCodes.Initialization_Error);
        }
      } finally {
        storeStartupScheduler.shutdown();
      }
    } else {
      throw new StoreException("Mount path does not exist: " + mountPath + " ; cannot start stores on this disk",
          StoreErrorCodes.Initialization_Error);
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
   * @return the associated {@link Store}, or {@code null} if the partition is not on this disk.
   * @throws StoreException if the partition is not found on this disk, or the store for that partition has not been
   *                        started.
   */
  Store getStore(PartitionId id)
      throws StoreException {
    BlobStore store = stores.get(id);
    if (store == null) {
      throw new StoreException("Could not find partition " + id + " on disk " + disk,
          StoreErrorCodes.Partition_Not_Found);
    }
    if (!store.isStarted()) {
      throw new StoreException("Store for partition " + id + " on disk " + disk + " has not been started successfully",
          StoreErrorCodes.Store_Not_Started);
    }
    return store;
  }

  /**
   * @return the {@link DiskId} that is managed by this {@link DiskManager}.
   */
  DiskId getDisk() {
    return disk;
  }
}