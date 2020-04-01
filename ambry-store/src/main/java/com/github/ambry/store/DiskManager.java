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

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages all the stores on a disk.
 */
class DiskManager {

  private final ConcurrentHashMap<PartitionId, BlobStore> stores = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<PartitionId, ReplicaId> partitionToReplicaMap = new ConcurrentHashMap<>();
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final DiskId disk;
  private final StorageManagerMetrics metrics;
  private final Time time;
  private final DiskIOScheduler diskIOScheduler;
  private final ScheduledExecutorService longLivedTaskScheduler;
  private final DiskSpaceAllocator diskSpaceAllocator;
  private final CompactionManager compactionManager;
  private final List<String> stoppedReplicas;
  private final ReplicaStatusDelegate replicaStatusDelegate;
  private final Set<String> expectedDirs = new HashSet<>();
  private final StoreConfig storeConfig;
  private final ScheduledExecutorService scheduler;
  private final StoreMetrics storeMainMetrics;
  private final StoreMetrics storeUnderCompactionMetrics;
  private final StoreKeyFactory keyFactory;
  private final MessageStoreRecovery recovery;
  private final MessageStoreHardDelete hardDelete;
  private final List<String> unexpectedDirs = new ArrayList<>();
  private boolean running = false;

  private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

  /**
   * Constructs a {@link DiskManager}
   * @param disk representation of the disk.
   * @param replicas all the replicas on this disk.
   * @param storeConfig the settings for store configuration.
   * @param diskManagerConfig the settings for disk manager configuration.
   * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
   * @param metrics the {@link StorageManagerMetrics} instance to use.
   * @param storeMainMetrics the {@link StoreMetrics} object used for store-related metrics.
   * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
   * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
   * @param recovery the {@link MessageStoreRecovery} instance to use.
   * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
   * @param time the {@link Time} instance to use.
   */
  DiskManager(DiskId disk, List<ReplicaId> replicas, StoreConfig storeConfig, DiskManagerConfig diskManagerConfig,
      ScheduledExecutorService scheduler, StorageManagerMetrics metrics, StoreMetrics storeMainMetrics,
      StoreMetrics storeUnderCompactionMetrics, StoreKeyFactory keyFactory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, ReplicaStatusDelegate replicaStatusDelegate, List<String> stoppedReplicas,
      Time time) {
    this.disk = disk;
    this.storeConfig = storeConfig;
    this.scheduler = scheduler;
    this.metrics = metrics;
    this.storeMainMetrics = storeMainMetrics;
    this.storeUnderCompactionMetrics = storeUnderCompactionMetrics;
    this.keyFactory = keyFactory;
    this.recovery = recovery;
    this.hardDelete = hardDelete;
    this.time = time;
    diskIOScheduler = new DiskIOScheduler(getThrottlers(storeConfig, time));
    longLivedTaskScheduler = Utils.newScheduler(1, true);
    File reserveFileDir = new File(disk.getMountPath(), diskManagerConfig.diskManagerReserveFileDirName);
    diskSpaceAllocator = new DiskSpaceAllocator(diskManagerConfig.diskManagerEnableSegmentPooling, reserveFileDir,
        diskManagerConfig.diskManagerRequiredSwapSegmentsPerSize, metrics);
    this.replicaStatusDelegate = replicaStatusDelegate;
    this.stoppedReplicas = stoppedReplicas;
    expectedDirs.add(reserveFileDir.getAbsolutePath());
    for (ReplicaId replica : replicas) {
      if (disk.equals(replica.getDiskId())) {
        BlobStore store =
            new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, diskIOScheduler, diskSpaceAllocator,
                storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, replicaStatusDelegate,
                time);
        stores.put(replica.getPartitionId(), store);
        partitionToReplicaMap.put(replica.getPartitionId(), replica);
        expectedDirs.add(replica.getReplicaPath());
      }
    }
    compactionManager = new CompactionManager(disk.getMountPath(), storeConfig, stores.values(), metrics, time);
  }

  /**
   * Starts all the stores on this disk.
   * @throws InterruptedException
   */
  void start() throws InterruptedException {
    long startTimeMs = time.milliseconds();
    final AtomicInteger numStoreFailures = new AtomicInteger(0);
    rwLock.readLock().lock();
    try {
      checkMountPathAccessible();

      List<Thread> startupThreads = new ArrayList<>();
      for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
        if (stoppedReplicas.contains(partitionAndStore.getKey().toPathString())) {
          logger.info("Skip the store {} because it is on the stopped list", partitionAndStore.getKey());
          continue;
        }
        Thread thread = Utils.newThread("store-startup-" + partitionAndStore.getKey(), () -> {
          try {
            partitionAndStore.getValue().start();
          } catch (Exception e) {
            numStoreFailures.incrementAndGet();
            logger.error("Exception while starting store for the " + partitionAndStore.getKey(), e);
          }
        }, false);
        thread.start();
        startupThreads.add(thread);
      }
      for (Thread startupThread : startupThreads) {
        startupThread.join();
      }
      if (numStoreFailures.get() > 0) {
        logger.error(
            "Could not start " + numStoreFailures.get() + " out of " + stores.size() + " stores on the disk " + disk);
      }

      // DiskSpaceAllocator startup. This happens after BlobStore startup because it needs disk space requirements
      // from each store.
      List<DiskSpaceRequirements> requirementsList = new ArrayList<>();
      for (BlobStore blobStore : stores.values()) {
        if (blobStore.isStarted()) {
          DiskSpaceRequirements requirements = blobStore.getDiskSpaceRequirements();
          if (requirements != null) {
            requirementsList.add(requirements);
          }
        }
      }
      diskSpaceAllocator.initializePool(requirementsList);
      compactionManager.enable();
      reportUnrecognizedDirs();
      running = true;
    } catch (StoreException e) {
      logger.error("Error while starting the DiskManager for " + disk.getMountPath()
          + " ; no stores will be accessible on this disk.", e);
    } finally {
      if (!running) {
        metrics.totalStoreStartFailures.inc(stores.size());
        metrics.diskDownCount.inc();
      } else {
        metrics.totalStoreStartFailures.inc(numStoreFailures.get());
      }
      metrics.diskStartTimeMs.update(time.milliseconds() - startTimeMs);
      rwLock.readLock().unlock();
    }
  }

  /**
   * Shuts down all the stores on this disk.
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException {
    long startTimeMs = time.milliseconds();
    rwLock.readLock().lock();
    try {
      running = false;
      compactionManager.disable();
      diskIOScheduler.disable();
      final AtomicInteger numFailures = new AtomicInteger(0);
      List<Thread> shutdownThreads = new ArrayList<>();
      for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
        if (!partitionAndStore.getValue().isStarted()) {
          continue;
        }
        Thread thread = Utils.newThread("store-shutdown-" + partitionAndStore.getKey(), () -> {
          try {
            partitionAndStore.getValue().shutdown();
          } catch (Exception e) {
            numFailures.incrementAndGet();
            metrics.totalStoreShutdownFailures.inc();
            logger.error("Exception while shutting down store {} on disk {}", partitionAndStore.getKey(), disk, e);
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
      longLivedTaskScheduler.shutdown();
      if (!longLivedTaskScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.error("Could not terminate long live tasks after DiskManager shutdown");
      }
    } finally {
      rwLock.readLock().unlock();
      metrics.diskShutdownTimeMs.update(time.milliseconds() - startTimeMs);
    }
  }

  /**
   * @param id the {@link PartitionId} to find the store for.
   * @param skipStateCheck whether to skip checking state of store.
   * @return the associated {@link Store}, or {@code null} if the partition is not on this disk, or the store is not
   *         started.
   */
  Store getStore(PartitionId id, boolean skipStateCheck) {
    BlobStore storeToReturn;
    rwLock.readLock().lock();
    try {
      BlobStore store = stores.get(id);
      storeToReturn = (running && store != null && (store.isStarted() || skipStateCheck)) ? store : null;
    } finally {
      rwLock.readLock().unlock();
    }
    return storeToReturn;
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
    BlobStore store = (BlobStore) getStore(id, false);
    return store != null && compactionManager.scheduleNextForCompaction(store);
  }

  /**
   * Enable or disable compaction on the {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link BlobStore} on which compaction is disabled or enabled.
   * @param enabled whether to enable ({@code true}) or disable.
   * @return {@code true} if disabling was successful. {@code false} if not.
   */
  boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
    rwLock.readLock().lock();
    boolean succeed = false;
    try {
      BlobStore store = stores.get(id);
      if (store != null) {
        compactionManager.controlCompactionForBlobStore(store, enabled);
        succeed = true;
      }
    } finally {
      rwLock.readLock().unlock();
    }
    return succeed;
  }

  /**
   * Add a new BlobStore with given {@link ReplicaId}.
   * @param replica the {@link ReplicaId} of the {@link Store} which would be added.
   * @return {@code true} if adding store was successful. {@code false} if not.
   */
  boolean addBlobStore(ReplicaId replica) {
    rwLock.writeLock().lock();
    boolean succeed = false;
    try {
      if (!running) {
        logger.error("Failed to add {} because disk manager is not running", replica.getPartitionId());
      } else {
        // Clean up existing dir associated with this replica to add. Here we re-create a new store because we don't
        // know the state of files in old directory. (The old directory was created last time when adding this replica
        // but failed at some point before updating InstanceConfig)
        File storeDir = new File(replica.getReplicaPath());
        if (storeDir.exists()) {
          logger.info("Deleting previous store directory associated with {}", replica);
          try {
            Utils.deleteFileOrDirectory(storeDir);
          } catch (Exception e) {
            throw new IOException("Couldn't delete store directory " + replica.getReplicaPath(), e);
          }
          logger.info("Old store directory is deleted for {}", replica);
        }
        BlobStore store =
            new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, diskIOScheduler, diskSpaceAllocator,
                storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, replicaStatusDelegate,
                time);
        store.start();
        // collect store segment requirements and add into DiskSpaceAllocator
        List<DiskSpaceRequirements> storeRequirements = Collections.singletonList(store.getDiskSpaceRequirements());
        diskSpaceAllocator.addRequiredSegments(diskSpaceAllocator.getOverallRequirements(storeRequirements), false);
        // add store into CompactionManager
        compactionManager.addBlobStore(store);
        // add new created store into in-memory data structures.
        stores.put(replica.getPartitionId(), store);
        partitionToReplicaMap.put(replica.getPartitionId(), replica);
        // create a bootstrap-in-progress file to distinguish it from regular stores (the file will be checked during
        // BOOTSTRAP -> STANDBY transition)
        File bootstrapFile = new File(replica.getReplicaPath(), BlobStore.BOOTSTRAP_FILE_NAME);
        if (!bootstrapFile.exists()) {
          // if not present, create one. (it's possible the bootstrap file exists because node may crash immediately
          // after the file was created last time)
          bootstrapFile.createNewFile();
        }
        logger.info("New store is successfully added into DiskManager.");
        succeed = true;
      }
    } catch (Exception e) {
      logger.error("Failed to start new added store {} or add requirements to disk allocator",
          replica.getPartitionId());
    } finally {
      rwLock.writeLock().unlock();
    }
    return succeed;
  }

  /**
   * Start the BlobStore with given {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link BlobStore} which should be started.
   * @return {@code true} if start store was successful. {@code false} if not.
   */
  boolean startBlobStore(PartitionId id) {
    rwLock.readLock().lock();
    boolean succeed = false;
    try {
      BlobStore store = stores.get(id);
      if (store == null || !running) {
        logger.error("Failed to start store because {} is not found or DiskManager is not running.", id);
      } else if (store.isStarted()) {
        succeed = true;
      } else {
        store.start();
        succeed = true;
      }
    } catch (Exception e) {
      logger.error("Exception while starting store {} on disk {}", id, disk, e);
    } finally {
      rwLock.readLock().unlock();
    }
    return succeed;
  }

  /**
   * Shutdown the BlobStore with given {@link PartitionId} {@code id}.
   * @param id the {@link PartitionId} of the {@link BlobStore} which should be shutdown.
   * @return {@code true} if shutdown store was successful. {@code false} if not.
   */
  boolean shutdownBlobStore(PartitionId id) {
    rwLock.readLock().lock();
    boolean succeed = false;
    try {
      BlobStore store = stores.get(id);
      if (store == null || !running) {
        logger.error("Failed to shut down store because {} is not found or DiskManager is not running", id);
      } else if (!store.isStarted()) {
        succeed = true;
      } else {
        store.shutdown();
        succeed = true;
      }
    } catch (Exception e) {
      logger.error("Exception while shutting down store {} on disk {}", id, disk, e);
    } finally {
      rwLock.readLock().unlock();
    }
    return succeed;
  }

  /**
   * Given partition id, remove the corresponding blob store in disk manager
   * @param id the {@link PartitionId} of the {@link BlobStore} which should be removed.
   * @return {@code true} if store removal was successful. {@code false} if not.
   */
  boolean removeBlobStore(PartitionId id) {
    rwLock.writeLock().lock();
    boolean succeed = false;
    try {
      BlobStore store = stores.get(id);
      if (store == null) {
        logger.error("Store {} is not found in disk manager", id);
      } else if (!running || store.isStarted()) {
        logger.error("Removing store {} failed. Disk running = {}, store running = {}", id, running, store.isStarted());
      } else if (!compactionManager.removeBlobStore(store)) {
        logger.error("Fail to remove store {} from compaction manager.", id);
      } else {
        stores.remove(id);
        stoppedReplicas.remove(id.toPathString());
        partitionToReplicaMap.remove(id);
        logger.info("Store {} is successfully removed from disk manager", id);
        succeed = true;
      }
    } finally {
      rwLock.writeLock().unlock();
    }
    return succeed;
  }

  /**
   * Set the BlobStore stopped state with given {@link PartitionId} {@code id}.
   * @param partitionIds a list of {@link PartitionId} of the {@link BlobStore} whose stopped state should be set.
   * @param markStop whether to mark BlobStore as stopped ({@code true}) or started.
   * @return a list of {@link PartitionId} whose stopped state fails to be updated.
   */
  List<PartitionId> setBlobStoreStoppedState(List<PartitionId> partitionIds, boolean markStop) {
    Set<PartitionId> failToUpdateStores = new HashSet<>();
    List<ReplicaId> replicasToUpdate = new ArrayList<>();
    rwLock.readLock().lock();
    try {
      for (PartitionId id : partitionIds) {
        BlobStore store = stores.get(id);
        if (store == null) {
          // no need to check if the store is started because this method could be called after store is successfully shutdown.
          logger.error("store is not found on this disk when trying to update stoppedReplicas list");
          failToUpdateStores.add(id);
        } else {
          replicasToUpdate.add(partitionToReplicaMap.get(id));
        }
      }
    } finally {
      rwLock.readLock().unlock();
    }
    boolean updated = false;
    if (replicaStatusDelegate != null) {
      logger.trace("Setting replica stopped state via ReplicaStatusDelegate on replica {}",
          Arrays.toString(replicasToUpdate.toArray()));
      updated = markStop ? replicaStatusDelegate.markStopped(replicasToUpdate)
          : replicaStatusDelegate.unmarkStopped(replicasToUpdate);
    } else {
      logger.warn("The ReplicaStatusDelegate is not instantiated");
    }
    if (!updated) {
      // either mark/unmark operation fails or ReplicaStatusDelegate is not instantiated.
      failToUpdateStores.addAll(partitionIds);
    }
    return new ArrayList<>(failToUpdateStores);
  }

  /**
   * Gets all the throttlers that the {@link DiskIOScheduler} will be constructed with.
   * @param config the {@link StoreConfig} with configuration values.
   * @param time the {@link Time} instance to use in the throttlers
   * @return the throttlers that the {@link DiskIOScheduler} will be constructed with.
   */
  private Map<String, Throttler> getThrottlers(StoreConfig config, Time time) {
    Map<String, Throttler> throttlers = new HashMap<>();
    // compaction ops
    Throttler compactionOpsThrottler = new Throttler(config.storeCompactionOperationsBytesPerSec, -1, true, time);
    throttlers.put(BlobStoreCompactor.COMPACTION_CLEANUP_JOB_NAME, compactionOpsThrottler);
    // hard delete ops
    Throttler hardDeleteOpsThrottler = new Throttler(config.storeHardDeleteOperationsBytesPerSec, -1, true, time);
    throttlers.put(HardDeleter.HARD_DELETE_CLEANUP_JOB_NAME, hardDeleteOpsThrottler);
    // stats
    Throttler statsIndexScanThrottler = new Throttler(config.storeStatsIndexEntriesPerSecond, 1000, true, time);
    throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, statsIndexScanThrottler);
    return throttlers;
  }

  /**
   * @throws StoreException if the disk's mount path is inaccessible.
   */
  private void checkMountPathAccessible() throws StoreException {
    File mountPath = new File(disk.getMountPath());
    if (!mountPath.exists()) {
      metrics.diskMountPathFailures.inc();
      throw new StoreException("Mount path does not exist: " + mountPath + " ; cannot start stores on this disk",
          StoreErrorCodes.Initialization_Error);
    }
  }

  /**
   * Check if all stores on this disk are down.
   * @return {@code true} if all stores are down. {@code false} at least one store is up.
   */
  boolean areAllStoresDown() {
    rwLock.readLock().lock();
    boolean storesAllDown = true;
    try {
      for (BlobStore store : stores.values()) {
        if (store.isStarted()) {
          storesAllDown = false;
          break;
        }
      }
    } finally {
      rwLock.readLock().unlock();
    }
    return storesAllDown;
  }

  /**
   * @return unexpected directories on this disk.
   */
  List<String> getUnexpectedDirs() {
    return unexpectedDirs;
  }

  /**
   * Reports any unrecognized directories on disk
   * store dir and return swap segment to reserve pool if needed.
   */
  private void reportUnrecognizedDirs() {
    File[] dirs = new File(disk.getMountPath()).listFiles(File::isDirectory);
    if (dirs == null) {
      metrics.diskMountPathFailures.inc();
      logger.warn("Could not list the directories in {}", disk.getMountPath());
    } else {
      for (File dir : dirs) {
        String absPath = dir.getAbsolutePath();
        if (!expectedDirs.contains(absPath)) {
          unexpectedDirs.add(absPath);
        }
      }
      if (unexpectedDirs.size() > 0) {
        logger.warn("Encountered unexpected dirs in {} : {}", disk.getMountPath(), unexpectedDirs);
        metrics.unexpectedDirsOnDisk.inc(unexpectedDirs.size());
      }
    }
  }
}
