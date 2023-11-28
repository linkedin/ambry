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

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
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
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.StorageManager.*;
import static com.github.ambry.utils.Utils.*;


/**
 * Manages all the stores on a disk.
 */
public class DiskManager {

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
  private final Set<String> stoppedReplicas;
  private final List<ReplicaStatusDelegate> replicaStatusDelegates;
  private final Set<String> expectedDirs = new HashSet<>();
  private final StoreConfig storeConfig;
  private final ScheduledExecutorService scheduler;
  private final StoreMetrics storeMainMetrics;
  private final StoreMetrics storeUnderCompactionMetrics;
  private final StoreKeyFactory keyFactory;
  private final MessageStoreRecovery recovery;
  private final MessageStoreHardDelete hardDelete;
  private final List<String> unexpectedDirs = new ArrayList<>();
  private final AccountService accountService;
  private final DiskManagerConfig diskManagerConfig;
  private boolean running = false;
  private DiskHealthStatus diskHealthStatus;
  private final DiskHealthCheck diskHealthCheck;
  // Have a dedicated scheduler for persisting index segments to ensure index segments are always persisted
  private final ScheduledExecutorService indexPersistScheduler;

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
   * @param replicaStatusDelegates a list of {@link ReplicaStatusDelegate} representing replica status agent for each
   *                               cluster current node has participated in.
   * @param stoppedReplicas a set of replicas that have been stopped (which should be skipped during startup).
   * @param time the {@link Time} instance to use.
   * @param accountService the {@link AccountService} instance to use.
   */
  DiskManager(DiskId disk, List<ReplicaId> replicas, StoreConfig storeConfig, DiskManagerConfig diskManagerConfig,
      ScheduledExecutorService scheduler, StorageManagerMetrics metrics, StoreMetrics storeMainMetrics,
      StoreMetrics storeUnderCompactionMetrics, StoreKeyFactory keyFactory, MessageStoreRecovery recovery,
      MessageStoreHardDelete hardDelete, List<ReplicaStatusDelegate> replicaStatusDelegates,
      Set<String> stoppedReplicas, Time time, AccountService accountService) {
    this.disk = disk;
    this.storeConfig = storeConfig;
    this.diskManagerConfig = diskManagerConfig;
    this.scheduler = scheduler;
    this.metrics = metrics;
    this.storeMainMetrics = storeMainMetrics;
    this.storeUnderCompactionMetrics = storeUnderCompactionMetrics;
    this.keyFactory = keyFactory;
    this.recovery = recovery;
    this.hardDelete = hardDelete;
    this.accountService = accountService;
    this.time = time;
    diskIOScheduler = new DiskIOScheduler(getThrottlers(storeConfig, time));
    longLivedTaskScheduler = Utils.newScheduler(1, true);
    indexPersistScheduler = Utils.newScheduler(1, "index-persistor-for-disk-" + disk.getMountPath(), false);
    File reserveFileDir = new File(disk.getMountPath(), diskManagerConfig.diskManagerReserveFileDirName);
    diskSpaceAllocator = new DiskSpaceAllocator(diskManagerConfig.diskManagerEnableSegmentPooling, reserveFileDir,
        diskManagerConfig.diskManagerRequiredSwapSegmentsPerSize, metrics);
    diskHealthCheck = new DiskHealthCheck(diskManagerConfig.diskManagerDiskHealthCheckOperationTimeoutSeconds,
        diskManagerConfig.diskManagerDiskHealthCheckEnabled);
    this.replicaStatusDelegates = replicaStatusDelegates;
    this.stoppedReplicas = stoppedReplicas;
    expectedDirs.add(reserveFileDir.getAbsolutePath());
    for (ReplicaId replica : replicas) {
      if (disk.equals(replica.getDiskId())) {
        DiskMetrics diskMetrics = new DiskMetrics(storeMainMetrics.getRegistry(), disk.getMountPath(),
            storeConfig.storeDiskIoReservoirTimeWindowMs);
        BlobStore store =
            new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, diskIOScheduler, diskSpaceAllocator,
                storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, replicaStatusDelegates,
                time, accountService, diskMetrics, indexPersistScheduler);
        stores.put(replica.getPartitionId(), store);
        partitionToReplicaMap.put(replica.getPartitionId(), replica);
        expectedDirs.add(replica.getReplicaPath());
        // All these replicas are already present on this disk. Update the space used on this disk.
        disk.decreaseAvailableSpaceInBytes(store.getReplicaId().getCapacityInBytes());
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
            logger.error("Exception while starting store for the {}", partitionAndStore.getKey(), e);
          }
        }, false);
        thread.start();
        startupThreads.add(thread);
      }
      for (Thread startupThread : startupThreads) {
        startupThread.join();
      }
      if (numStoreFailures.get() > 0) {
        logger.error("Could not start {} out of {} stores on the disk {}", numStoreFailures.get(), stores.size(), disk);
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
      if (diskHealthCheck.isEnabled()) {
        logger.info("Starting Disk Healthchecker");
        scheduler.scheduleAtFixedRate(() -> diskHealthCheck.diskHealthTest(), 0,
            diskManagerConfig.diskManagerDiskHealthCheckIntervalSeconds, TimeUnit.SECONDS);
      }
    } catch (StoreException e) {
      logger.error("Error while starting the DiskManager for {} ; no stores will be accessible on this disk.",
          disk.getMountPath(), e);
      // Set the state of the disk to UNAVAILABLE. This will prevent new replicas to be bootstrapped on this disk.
      // TODO: We might need to use DiskFailureHandler to immediately reset existing replicas on this disk and reduce
      //  instance capacity so that existing replicas are reassigned to new hosts and Helix doesn't assign more replicas
      //  than the host can handle.
      logger.error("Setting disk {} as UNAVAILABLE locally", disk.getMountPath());
      disk.setState(HardwareState.UNAVAILABLE);
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
        logger.error("Could not shutdown {} out of {} stores on the disk {}", numFailures.get(), stores.size(), disk);
      }
      compactionManager.awaitTermination();
      longLivedTaskScheduler.shutdown();
      if (!longLivedTaskScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.error("Could not terminate long live tasks after DiskManager shutdown");
      }
      if (indexPersistScheduler != null) {
        shutDownExecutorService(indexPersistScheduler, 30, TimeUnit.SECONDS);
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
                storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, replicaStatusDelegates,
                time, accountService, null, indexPersistScheduler);
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
        createBootstrapFileIfAbsent(replica);
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
  boolean removeBlobStore(PartitionId id) throws IOException, StoreException {
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
        ReplicaId replicaId = partitionToReplicaMap.remove(id);
        logger.info("Store {} is successfully removed from disk manager", id);
        store.deleteStoreFiles();
        // Since all the files are either deleted or returned to reserve pool, increase available space in the disk
        disk.increaseAvailableSpaceInBytes(replicaId.getCapacityInBytes());
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
    boolean updated = true;
    if (replicaStatusDelegates != null && !replicaStatusDelegates.isEmpty()) {
      logger.trace("Setting replica stopped state via ReplicaStatusDelegate on replica {}",
          Arrays.toString(replicasToUpdate.toArray()));
      for (ReplicaStatusDelegate replicaStatusDelegate : replicaStatusDelegates) {
        updated &= markStop ? replicaStatusDelegate.markStopped(replicasToUpdate)
            : replicaStatusDelegate.unmarkStopped(replicasToUpdate);
      }
    } else {
      logger.warn("The ReplicaStatusDelegate is not instantiated");
      updated = false;
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
    Throttler compactionOpsThrottler =
        new Throttler(config.storeCompactionOperationsBytesPerSec, config.storeCompactionThrottlerCheckIntervalMs, true,
            time);
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
   *
   * @return bool - True if mount is accessible otherwise False
   */
  private boolean checkMountPathAccessible() throws StoreException {
    File mountPath = new File(disk.getMountPath());
    if (!mountPath.exists() && diskHealthStatus != DiskHealthStatus.MOUNT_NOT_ACCESSIBLE) {
      metrics.diskMountPathFailures.inc();
      diskHealthStatus = DiskHealthStatus.MOUNT_NOT_ACCESSIBLE;
      throw new StoreException("Mount path does not exist: " + mountPath + " ; cannot start stores on this disk",
          StoreErrorCodes.Initialization_Error);
    }
    return mountPath.exists();
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
   * Check if this disk has any stores on it.
   * @return
   */
  boolean hasAnyStore() {
    rwLock.readLock().lock();
    try {
      return !stores.isEmpty();
    } finally {
      rwLock.readLock().unlock();
    }
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
        metrics.registerUnexpectedDirsForMountPath(disk.getMountPath(), unexpectedDirs.size());
      }
    }
  }

  public DiskHealthStatus getDiskHealthStatus() {
    return diskHealthStatus;
  }

  /**
   Manages the disk healthchecking tasks such as creating,writing, reading, and deleting a file
   */
  private class DiskHealthCheck {
    private final int operationTimeoutSeconds;      //how long a read/write operation is allotted
    private final boolean enabled;                  //enabled healthchecking

    DiskHealthCheck(int operationTimeoutSeconds, boolean enabled) {
      this.enabled = enabled;
      this.operationTimeoutSeconds = operationTimeoutSeconds;
    }

    /**
     * Getter utility for enabled boolean
     * @return flag to indicate disk healthchecking or not
     */
    private boolean isEnabled() {
      return enabled;
    }

    /**
     * Attempts to close the filechannel and delete the file
     * @param fileChannel the file used for disk healthcheck
     * @return True/False depending on the close/delete was successful
     */
    private boolean deleteFile(AsynchronousFileChannel fileChannel, boolean successfulThusFar) {
      if (fileChannel.isOpen()) {
        try {
          fileChannel.close();
        } catch (IOException e) {
          //ensures that any error code won't be overwritten thus far
          if (successfulThusFar) {
            diskHealthStatus = diskHealthStatus.DELETE_IO_EXCEPTION;
          }
          logger.error("Exception occurred when deleting a file for disk healthcheck {}", disk.getMountPath(), e);
          return false;
        }
      }
      return true;
    }

    /**
     * Will perform either a read or write operation on the disk while checking for exceptions and canceling the
     * operation if there was an exception
     * @param fileChannel used to perform the read/write operation
     * @param buffer either used to write from or read to the buffer in the respective operation
     * @param timeoutStatus indicate if the timeout is from reading or writing
     * @param exceptionStatus indicate if the other exceptions are from reading or writing
     * @param isWriting inform the function to either write or read with the buffer and filechannel
     * @return True/False to indicate if the operation completed with no issues or not
     */
    private boolean performOperation(AsynchronousFileChannel fileChannel, ByteBuffer buffer,
        DiskHealthStatus timeoutStatus, DiskHealthStatus exceptionStatus, boolean isWriting) {
      Future<Integer> result = null;
      try {
        if (isWriting) {
          result = fileChannel.write(buffer, 0);
        } else {
          result = fileChannel.read(buffer, 0);
        }
        result.get(operationTimeoutSeconds, TimeUnit.SECONDS);
        return true;
      } catch (TimeoutException e) {
        diskHealthStatus = timeoutStatus;
        logger.error("Timeout Exception occurred when operating on a file for disk healthcheck {}", disk.getMountPath(),
            e);
      } catch (Exception e) {
        diskHealthStatus = exceptionStatus;
        logger.error("Exception occurred when operating on a file for disk healthcheck {}", disk.getMountPath(), e);
      }
      if (result != null) {
        result.cancel(true);
      }
      return false;
    }

    /**
     * creates the directory to store the file if it doesn't already exist and
     * creates filechannel used to perform the disk healthcheck read/write operations
     * @return filechannel if everything functioned without errors or null if there was an exception
     */
    private AsynchronousFileChannel createFileChannel() {
      try {
        if (!checkMountPathAccessible()) {
          return null;
        }
        Path path = Paths.get(disk.getMountPath(), "temp");
        AsynchronousFileChannel fileChannel =
            AsynchronousFileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ,
                StandardOpenOption.SYNC, StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE);
        return fileChannel;
      } catch (StoreException e) {
        if (e.getErrorCode() == StoreErrorCodes.Initialization_Error) {
          diskHealthStatus = DiskHealthStatus.MOUNT_NOT_ACCESSIBLE;
          logger.error("Exception occurred when checking if the mount is accessible {}", disk.getMountPath(), e);
        }
      } catch (Exception e) {
        diskHealthStatus = DiskHealthStatus.CREATE_EXCEPTION;
        logger.error("Exception occurred when creating a file/directory for disk healthcheck {}", disk.getMountPath(), e);
      }
      return null;
    }

    /**
     * Performing Disk HealthCheck Test in 5 steps
     * 1. Checks if there is sufficient space on disk
     * 2. Creates the file on the filepath
     * 3. Writes today's date to disk
     * 4. Read and compares what was written to disk
     * 5. Deletes the file
     */
    public void diskHealthTest() {
      //ensures disk healthchecks are enabled
      if (!enabled) {
        return;
      }

      logger.trace("Performing the disk healthcheck test");

      //Creates the directory and opens the file
      AsynchronousFileChannel fileChannel = createFileChannel();
      if (fileChannel == null) {
        return;
      }

      //Finds today's date and stores it
      String todayDate = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").format(Calendar.getInstance().getTime());
      ByteBuffer buffer = ByteBuffer.allocate(todayDate.getBytes().length);
      buffer.put(todayDate.getBytes());
      buffer.flip();

      boolean successful = false; //success indicator for write and read operation

      //Write to file in operationTimeoutSeconds seconds
      successful =
          performOperation(fileChannel, buffer, DiskHealthStatus.WRITE_TIMEOUT, DiskHealthStatus.WRITE_EXCEPTION, true);

      //Reads from the file in operationTimeoutSeconds seconds
      if (successful) {
        buffer.clear();
        successful =
            performOperation(fileChannel, buffer, DiskHealthStatus.READ_TIMEOUT, DiskHealthStatus.READ_EXCEPTION,
                false);
      }

      //Checks if the content matches what was written
      if (successful) {
        String fileContent = new String(buffer.array()).trim();
        successful = fileContent.equals(todayDate);
        if (!successful) {
          diskHealthStatus = DiskHealthStatus.READ_DIFFERENT;
        }
      }

      //Deletes this test file and only specifies healthy if everything successful
      if (deleteFile(fileChannel, successful) && successful) {
        diskHealthStatus = DiskHealthStatus.HEALTHY;
      }
    }
  }
}
