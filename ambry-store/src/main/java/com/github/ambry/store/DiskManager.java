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
import java.util.EnumSet;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
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
  private final BootstrapSessionManager bootstrapSessionManager;
  private final HashMap<PartitionId, Boolean> controlCompactionForBlobStoreMap = new HashMap<>();
  private final Set<String> stoppedReplicas;
  private final DiskMetrics diskMetrics;
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
  private volatile boolean running = false;
  private DiskHealthStatus diskHealthStatus;
  private final DiskHealthCheck diskHealthCheck;
  // Have a dedicated scheduler for persisting index segments to ensure index segments are always persisted
  private final ScheduledExecutorService indexPersistScheduler;
  //@formatter:off
  private final EnumSet<StoreErrorCodes> recoverableStoreErrorCodes =
      EnumSet.of(
          StoreErrorCodes.LogFileFormatError,
          StoreErrorCodes.IndexFileFormatError,
          StoreErrorCodes.LogEndOffsetError,
          StoreErrorCodes.IndexRecoveryError,
          StoreErrorCodes.StoreStaleError);
  //@formatter:on

  private final AtomicBoolean ioErrorCheckerScheduled = new AtomicBoolean(false);

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
      Set<String> stoppedReplicas, Time time, AccountService accountService) throws StoreException {
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
    diskMetrics = new DiskMetrics(storeMainMetrics.getRegistry(), disk.getMountPath(),
        storeConfig.storeDiskIoReservoirTimeWindowMs);
    for (ReplicaId replica : replicas) {
      if (disk.equals(replica.getDiskId())) {
        BlobStore store = new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, this, diskIOScheduler,
            diskSpaceAllocator, storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete,
            replicaStatusDelegates, time, accountService, diskMetrics, indexPersistScheduler);
        stores.put(replica.getPartitionId(), store);
        partitionToReplicaMap.put(replica.getPartitionId(), replica);
        expectedDirs.add(replica.getReplicaPath());
        // All these replicas are already present on this disk. Update the space used on this disk.
        disk.decreaseAvailableSpaceInBytes(store.getReplicaId().getCapacityInBytes());
      }
    }
    compactionManager = new CompactionManager(disk.getMountPath(), storeConfig, stores.values(), metrics, time);
    bootstrapSessionManager = new BootstrapSessionManager(diskManagerConfig, this::controlCompactionForBlobStoreStub);
  }

  /**
   * Starts all the stores on this disk.
   * @throws InterruptedException
   */
  void start(boolean shouldRemoveUnexpectedDirs) throws InterruptedException {
    long startTimeMs = time.milliseconds();
    final AtomicInteger numStoreFailures = new AtomicInteger(0);
    rwLock.readLock().lock();
    try {
      checkMountPathAccessible();
      // Report unrecognized directories before trying to satisfy disk space requirements.
      reportUnrecognizedDirs();
      if (shouldRemoveUnexpectedDirs) {
        // Try to remove all the unexpected directories before starting any blob store.
        // If the disk is near full because of the unexpected directoires, then blob store
        // might not be able to start since each blob store would request at least one log
        // segment.
        tryRemoveAllUnexpectedDirs();
      }

      ConcurrentHashMap<PartitionId, Exception> startExceptions = new ConcurrentHashMap<>();
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
            startExceptions.put(partitionAndStore.getKey(), e);
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
        maybeRecoverBlobStores(numStoreFailures.get(), startExceptions);
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
      bootstrapSessionManager.enable();
      running = true;
      if (diskHealthCheck.isEnabled()) {
        logger.info("Starting Disk Healthchecker");
        scheduler.scheduleAtFixedRate(() -> diskHealthCheck.diskHealthTest(), 0,
            diskManagerConfig.diskManagerDiskHealthCheckIntervalSeconds, TimeUnit.SECONDS);
      }
    } catch (StoreException e) {
      logger.error("Error while starting the DiskManager for {} ; no stores will be accessible on this disk.",
          disk.getMountPath(), e);
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
   * Maybe recover the blob stores that failed to start. The configuration has to be set to true and the number
   * of failed blob stores are not the same as the total number on the disk, then we would try to recover each
   * blob store. We only recover the blob store if the exception thrown in the start method is StoreException and
   * the store error code is one of the recoverable error codes.
   * @param numStoreFailures
   * @param startExceptions
   */
  void maybeRecoverBlobStores(int numStoreFailures, Map<PartitionId, Exception> startExceptions) {
    if (!storeConfig.storeRemoveDirectoryAndRestartBlobStore || numStoreFailures == stores.size()) {
      return;
    }
    for (Map.Entry<PartitionId, BlobStore> entry : stores.entrySet()) {
      PartitionId partitionId = entry.getKey();
      BlobStore store = entry.getValue();
      if (!shouldRemoveDirectory(partitionId, store, startExceptions.get(partitionId))) {
        continue;
      }
      logger.info("Remove directory for store {} and restart it", partitionId);

      // 1. Remove the blob store directory
      // 2. Create another blob store and restart it. since this time the blob store would be empty, we don't
      // have to use different threads for different blob stores.
      ReplicaId replica = partitionToReplicaMap.get(entry.getKey());
      String dataDir = store.getDataDir();
      try {
        Utils.deleteFileOrDirectory(new File(dataDir));
        BlobStore newStore =
            new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, this, diskIOScheduler,
                diskSpaceAllocator, storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete,
                replicaStatusDelegates, time, accountService, diskMetrics, indexPersistScheduler);
        newStore.start();
        entry.setValue(newStore);
      } catch (Exception e) {
        logger.error("Failed to remove directory for store {} and restart it", dataDir, e);
      }
    }
  }

  /**
   * Return true when we should wipe out the directory of the given blob store.
   * @param partitionId The partition id of the blob store.
   * @param store The blob store
   * @param startException The exceptions thrown by blobstore.start method.
   * @return
   */
  boolean shouldRemoveDirectory(PartitionId partitionId, BlobStore store, Exception startException) {
    if (store.isInitialized() || stoppedReplicas.contains(partitionId.toPathString())) {
      return false;
    }
    StoreException storeException = getRootCause(startException, StoreException.class);
    return storeException != null && recoverableStoreErrorCodes.contains(storeException.getErrorCode());
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
      bootstrapSessionManager.disable();
      controlCompactionForBlobStoreMap.clear();
      diskIOScheduler.disable();
      final AtomicInteger numFailures = new AtomicInteger(0);
      List<Thread> shutdownThreads = new ArrayList<>();
      for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
        if (!partitionAndStore.getValue().isInitialized()) {
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
   * Return true is the disk manager is running.
   * @return True when the disk manager is running.
   */
  boolean isRunning() {
    return running;
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
   * @param id the {@link PartitionId} to find the store for.
   * @return the associated {@link Store}, or {@code null} if the partition is not on this disk, or the store is not
   *         initialized.
   */
  Store getInitializedStore(PartitionId id) {
    BlobStore storeToReturn;
    rwLock.readLock().lock();
    try {
      BlobStore store = stores.get(id);
      storeToReturn = (running && store != null && (store.isInitialized())) ? store : null;
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
   * Also updates controlCompactionForBlobStoreMap which is further used by deferred compaction to prevent overriding
   * this control.
   * This is used by few places like State transitions, Admin store start/stop endpoints, Repairs flow etc.
   * @param id the {@link PartitionId} of the {@link BlobStore} on which compaction control is enabled or disabled.
   * @param enabled whether to enable ({@code true}) or disable.
   * @return {@code true} if enabling or disabling was successful. {@code false} if not.
   */
  public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
    controlCompactionForBlobStoreMap.put(id, enabled);
    return controlCompactionForBlobStoreStub(id, enabled);
  }

  /**
   * Return true if the compaction control has been set. Return the compaction control for the given partition id.
   * @param id the {@link PartitionId} of the {@link BlobStore} to check control for.
   * @return {@code true} if the compaction is under control. {@code false} if not.
   */
  public boolean isCompactionForBlobStoreUnderControl(PartitionId id) {
    return controlCompactionForBlobStoreMap.getOrDefault(id, false);
  }

  /**
   * Return true if the compaction control has been set.
   * @param id the {@link PartitionId} of the {@link BlobStore} to check control for.
   * @return {@code true} if the compaction is under control. {@code false} if not.
   */
  public boolean isCompactionControlBeenSetForBlobStore(PartitionId id) {
    return controlCompactionForBlobStoreMap.containsKey(id);
  }

  /**
   * Enable or disable compaction on the {@link PartitionId} {@code id}.
   * To be only used by Deferred Compaction. Api plane ensures using controlCompactionForBlobStoreMap that it doesn't
   * override this control.
   * @param id the {@link PartitionId} of the {@link BlobStore} on which compaction is disabled or enabled.
   * @param enabled whether to enable ({@code true}) or disable.
   * @return {@code true} if disabling was successful. {@code false} if not.
   */
  private boolean controlCompactionForBlobStoreStub(PartitionId id, boolean enabled) {
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
   * Return true if the compaction is disabled for the given partition id.
   * @param id
   * @return
   */
  boolean compactionDisabledForBlobStore(PartitionId id) {
    rwLock.readLock().lock();
    try {
      BlobStore store = stores.get(id);
      if (store == null) {
        throw new IllegalArgumentException("Failed to find store " + id);
      }
      return compactionManager.compactionDisabledForBlobStore(store);
    } finally {
      rwLock.readLock().unlock();
    }
  }


  /**
   * Add a new BlobStore with given {@link ReplicaId}.
   * @param replica the {@link ReplicaId} of the {@link Store} which would be added.
   * @return {@code true} if adding store was successful. {@code false} if not.
   */
  boolean addBlobStoreForFileCopy(ReplicaId replica) {
    rwLock.writeLock().lock();
    boolean succeed = false;
    try {
      if (!running) {
        logger.error("Failed to add {} because disk manager is not running", replica.getPartitionId());
      } else {
        // Directory is already created in prefilecopy steps. So no directory cleanup required.
        BlobStore store = new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, this, diskIOScheduler,
            diskSpaceAllocator, storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete,
            replicaStatusDelegates, time, accountService, null, indexPersistScheduler);
        store.start();
        // add store into CompactionManager
        compactionManager.addBlobStore(store);
        // add new created store into in-memory data structures.
        stores.put(replica.getPartitionId(), store);
        // create a bootstrap-in-progress file to distinguish it from regular stores (the file will be checked during
        // BOOTSTRAP -> STANDBY transition)
        createBootstrapFileIfAbsent(replica);
        logger.info("New store for partitionId {} is successfully added into DiskManager.", replica.getPartitionId());
        succeed = true;
      }
    } catch (Exception e) {
      logger.error("Failed to start new added store for partitionId {} for FileCopy based replication", replica.getPartitionId(),
          e);
    } finally {
      rwLock.writeLock().unlock();
    }
    return succeed;
  }

  /**
   * Adds and initializes a new BlobStore with given {@link ReplicaId}
   * @param replica {@link ReplicaId}
   * @return {@code true} if initialization succeeds {@code false} otherwise.
   */
  boolean initializeBlobStore(ReplicaId replica) {
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
        BlobStore store = new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, this, diskIOScheduler,
            diskSpaceAllocator, storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete,
            replicaStatusDelegates, time, accountService, null, indexPersistScheduler);
        store.initialize();

        // collect store segment requirements and add into DiskSpaceAllocator
        List<DiskSpaceRequirements> storeRequirements = Collections.singletonList(store.getDiskSpaceRequirements());
        diskSpaceAllocator.addRequiredSegments(diskSpaceAllocator.getOverallRequirements(storeRequirements), false);

        // add new created store into in-memory data structures.
        stores.put(replica.getPartitionId(), store);

        partitionToReplicaMap.put(replica.getPartitionId(), replica);
        succeed = true;
      }
    } catch (Exception e) {
      logger.error("Failed to initialize new added store {} ", replica.getPartitionId(), e);
    } finally {
      rwLock.writeLock().unlock();
    }
    return succeed;
  }

  /**
   * Loads the initialized Blobstore and starts it for given {@link ReplicaId}
   * @param replica {{@link ReplicaId}
   * @return {@code true} if loading succeeds {@code false} otherwise.
   */
  boolean loadInitializedBlobStore(ReplicaId replica) {
    rwLock.writeLock().lock();
    boolean succeed = false;
    try {
      if (!running) {
        logger.error("Failed to add {} because disk manager is not running", replica.getPartitionId());
      } else {
        BlobStore store = stores.get(replica.getPartitionId());
        store.load();
        // add store into CompactionManager
        compactionManager.addBlobStore(store);

        // create a bootstrap-in-progress file to distinguish it from regular stores (the file will be checked during
        // BOOTSTRAP -> STANDBY transition)
        createBootstrapFileIfAbsent(replica);
        logger.info("New store is successfully added into DiskManager for partitionId {}.", replica.getPartitionId());
        succeed = true;
      }
    } catch (Exception e) {
      stores.remove(replica.getPartitionId());
      partitionToReplicaMap.remove(replica.getPartitionId());
      logger.error("Failed to load new added store {} or add requirements to disk allocator", replica.getPartitionId(),
          e);
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
      } else if (!store.isInitialized()) {
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
      } else if (!running || store.isInitialized()) {
        logger.error("Removing store {} failed. Disk running = {}, store initialized = {}", id, running, store.isInitialized());
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
          StoreErrorCodes.InitializationError);
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
        if (store.isInitialized()) {
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

  /**
   * Try to remove all unexpected directories.
   */
  void tryRemoveAllUnexpectedDirs() {
    logger.info("Removing unexpected directories: {}", unexpectedDirs);
    for (String unexpectedDir : unexpectedDirs) {
      String[] segments = unexpectedDir.split(File.separator);
      if (segments.length > 1) {
        String partitionName = segments[segments.length - 1];
        try {
          File dirToDelete = new File(unexpectedDir);
          Utils.deleteFileOrDirectory(dirToDelete);
          logger.info("Removed directory {}", unexpectedDir);
          diskSpaceAllocator.deleteAllSegmentsForStoreIds(Collections.singletonList(partitionName));
        } catch (Exception e) {
          logger.error("Failed to delete directory for partition {}", partitionName, e);
        }
      }
    }
  }

  /**
   * Try to clean up the remaining files and directories for a failed bootstrap replica.
   * @param replica The failed bootstrap {@link ReplicaId}.
   */
  void tryRemoveFailedBootstrapBlobStore(ReplicaId replica) {
    // replica would most likely fail due to disk space requirement, here we have to remove this replica like it's
    // an unexpected directory
    String partitionName = replica.getPartitionId().toPathString();
    try {
      String replicaPath = replica.getReplicaPath();
      logger.info("Removing replica path {}", replicaPath);
      Utils.deleteFileOrDirectory(new File(replicaPath));
      diskSpaceAllocator.deleteAllSegmentsForStoreIds(Collections.singletonList(partitionName));
    } catch (Exception e) {
      logger.error("Failed to delete directories for failed blob store {}", partitionName, e);
    }
  }

  /**
   * A callback to invoke when a blob store encountered several io error and shut down itself. In this case,
   * we should test all blob stores on this disk. If it's indeed a disk error, all blob stores would be forced
   * to shut down themselves and enter ERROR state. Later disk failure handler would pick it up and move all
   * the replica out to a different place in FULL AUTO.
   */
  void onBlobStoreIOError() {
    if (storeConfig.storeProactivelyTestStorageAvailability && ioErrorCheckerScheduled.compareAndSet(false, true)) {
      // Schedule the ioError checker to go over all the blob stores on this disk
      logger.info("Submitting a task to test storage availability on disk {}", disk.getMountPath());
      scheduler.schedule(() -> {
        List<BlobStore> storesToCheck = new ArrayList<>(stores.values());
        for (BlobStore store : storesToCheck) {
          logger.info("Testing storage availability for blob store {} on disk {}", store.getDataDir(),
              disk.getMountPath());
          if (store.isStarted()) {
            // if any of the store has its underlying storage available, we stop the test on other
            // blob stores since we know it's not a disk error.
            if (store.testStorageAvailability()) {
              logger.info("Blob store {} on disk {} is still available", store.getDataDir(), disk.getMountPath());
              break;
            } else {
              logger.info("Blob store {} on disk {} become unavailable", store.getDataDir(), disk.getMountPath());
            }
          } else {
            logger.info("Blob store {} on disk {} is not started", store.getDataDir(), disk.getMountPath());
          }
        }
        ioErrorCheckerScheduled.set(false);
      }, storeConfig.storeProactiveTestDelayInSeconds, TimeUnit.SECONDS);
    }
  }

  public DiskHealthStatus getDiskHealthStatus() {
    return diskHealthStatus;
  }

  /**
   * Checks if the file exists on the disk
   * @param fileName
   * @return
   */
  public boolean isFileExists(String fileName) {
    String filePath = this.disk.getMountPath() + File.separator + fileName;
    return new File(filePath).exists();
  }

  /**
   * Gets the files for the given pattern from the disk
   */
  public List<File> getFilesForPattern(Pattern pattern) throws IOException {
    return Utils.getFilesForPattern(this.disk.getMountPath(), pattern);
  }

  public BootstrapSessionManager getBootstrapSessionManager() {
    return bootstrapSessionManager;
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
        if (e.getErrorCode() == StoreErrorCodes.InitializationError) {
          diskHealthStatus = DiskHealthStatus.MOUNT_NOT_ACCESSIBLE;
          logger.error("Exception occurred when checking if the mount is accessible {}", disk.getMountPath(), e);
        }
      } catch (Exception e) {
        diskHealthStatus = DiskHealthStatus.CREATE_EXCEPTION;
        logger.error("Exception occurred when creating a file/directory for disk healthcheck {}", disk.getMountPath(),
            e);
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
