/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A thread pool manager that handles file copy operations with disk-aware thread allocation.
 * This class manages a pool of {@link FileCopyThread}s while ensuring that the number of concurrent
 * operations per disk does not exceed a configured threshold. It provides the following key features:
 *
 * <ul>
 *   <li>Maintains separate thread pools for each disk to prevent disk overload</li>
 *   <li>Limits the maximum number of concurrent file copy operations per disk</li>
 *   <li>Tracks and manages the lifecycle of file copy threads</li>
 *   <li>Provides mechanisms to start, stop, and monitor file copy operations for specific replicas</li>
 *   <li>Supports graceful shutdown of all managed threads</li>
 * </ul>
 *
 * This implementation is thread-safe and uses locks to coordinate access to shared resources.
 */
public class DiskAwareFileCopyThreadPoolManager implements FileCopyBasedReplicationThreadPoolManager, Runnable {

  private final Map<ReplicaId, FileCopyThread> replicaToFileCopyThread;
  private final Map<DiskId, Set<FileCopyThread>> runningThreads;

  private final int numberOfThreadsPerDisk;
  private final ReentrantLock threadQueueLock;
  private boolean isRunning;
  private final CountDownLatch shutdownLatch;

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Constructor to initialize the thread pool manager with specified disk IDs and number of threads.
   * @param diskIds List of disk IDs to manage threads for
   * @param numberOfThreads Maximum number of threads allowed per disk
   */
  public DiskAwareFileCopyThreadPoolManager(List<DiskId> diskIds, int numberOfThreads) {
    this.numberOfThreadsPerDisk = numberOfThreads;
    this.runningThreads = new HashMap<>();
    this.replicaToFileCopyThread = new HashMap<>();
    diskIds.forEach(diskId -> {
      runningThreads.put(diskId, new HashSet<>());
    });
    isRunning = true;
    this.shutdownLatch = new CountDownLatch(1);
    this.threadQueueLock = new ReentrantLock(true);
  }

  /**
   * Returns the maximum number of threads allowed per disk.
   * @return Number of threads per disk
   */
  @Override
  public int getThreadPoolSize() {
    return numberOfThreadsPerDisk;
  }

  /**
   * Identifies disks that have capacity for additional threads.
   * Returns a list of disk IDs where the number of running threads
   * is less than the maximum allowed threads per disk.
   * @return List of disk IDs that can accommodate more threads
   */
  @Override
  public List<DiskId> getDiskIdsToHydrate() {
    List<DiskId> diskIds = new ArrayList<>();
    runningThreads.forEach((diskId, fileCopyThreads) -> {
      if (fileCopyThreads.size() < numberOfThreadsPerDisk) {
        diskIds.add(diskId);
      }
    });
    return diskIds;
  }

  /**
   * Submits a replica for hydration by creating and starting a new FileCopyThread.
   * The thread is associated with the replica's disk and tracked in the running threads map.
   * @param replicaId The replica to be hydrated
   * @param fileCopyStatusListener Listener for file copy status updates
   * @param fileCopyHandler Handler for performing the file copy operation
   */
  @Override
  public void submitReplicaForHydration(ReplicaId replicaId, FileCopyStatusListener fileCopyStatusListener,
      FileCopyHandler fileCopyHandler) {
    try {
      threadQueueLock.lock();
      DiskId diskId = replicaId.getDiskId();
      FileCopyThread fileCopyThread = new FileCopyThread(fileCopyHandler, fileCopyStatusListener);
      fileCopyThread.start();
      runningThreads.get(diskId).add(fileCopyThread);
      replicaToFileCopyThread.put(replicaId, fileCopyThread);
    } catch (Exception e) {
      logger.error("Error while submitting replica {} for hydration: {}", replicaId, e.getMessage());
      throw new StateTransitionException("Error while submitting replica " + replicaId + " for hydration",
          StateTransitionException.TransitionErrorCode.FileCopyProtocolFailure);
    } finally {
      threadQueueLock.unlock();
    }
  }

  /**
   * Stops and removes a replica's associated thread from the thread pool.
   * If the thread doesn't exist or is not alive, the method returns without action.
   * @param replicaId The replica whose thread should be stopped
   * @throws InterruptedException if the thread shutdown is interrupted
   */
  @Override
  public void stopAndRemoveReplicaFromThreadPool(ReplicaId replicaId) throws InterruptedException {
    threadQueueLock.lock();
    logger.info("Stopping and removing replica {} from thread pool", replicaId);
    FileCopyThread fileCopyThread = replicaToFileCopyThread.get(replicaId);

    if (fileCopyThread == null || !fileCopyThread.isAlive()) {
      logger.info("No thread found for replica {}. Nothing to stop.", replicaId);
      threadQueueLock.unlock();
      return;
    }
    long threadShutDownInitiationTime = System.currentTimeMillis();
    logger.info("Stopping thread for replica {}", replicaId);
    fileCopyThread.shutDown();
    logger.info("Thread for replica {} stopped in {} ms", replicaId,
        System.currentTimeMillis() - threadShutDownInitiationTime);
    threadQueueLock.unlock();
  }

  /**
   * Main run loop that manages the thread pool.
   * Continuously monitors and removes completed threads from the running threads map
   * until shutdown is initiated and all threads complete.
   */
  @Override
  public void run() {
    while (isRunning || areThreadsRunning()) {
      threadQueueLock.lock();
      runningThreads.forEach((diskId, fileCopyThreads) -> {
        fileCopyThreads.removeIf(fileCopyThread -> !fileCopyThread.isAlive());
      });
      threadQueueLock.unlock();
    }
    shutdownLatch.countDown();
  }

  /**
   * Checks if any threads are still running across all disks.
   * @return true if there are running threads on any disk, false otherwise
   */
  boolean areThreadsRunning() {
    return runningThreads.values().stream().noneMatch(Set::isEmpty);
  }

  /**
   * Initiates shutdown of the thread pool manager and waits for completion.
   * @throws InterruptedException if the shutdown wait is interrupted
   */
  @Override
  public void shutdown() throws InterruptedException {
    isRunning = false;
    shutdownLatch.await();
  }
}
