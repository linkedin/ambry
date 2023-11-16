/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that runs scheduled or on-demand compaction of blobs in cloud storage.
 */
public class CloudStorageCompactor extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageCompactor.class);
  protected CloudDestination cloudDestination;
  protected Set<PartitionId> partitions;
  protected VcrMetrics vcrMetrics;
  protected ScheduledThreadPoolExecutor executorService;

  protected CloudConfig cloudConfig;
  protected CountDownLatch doneLatch;
  protected int numBlobsErased;

  /**
   * Public constructor.
   * @param cloudDestination the cloud destination to use.
   * @param partitions the set of partitions to compact.
   * @param vcrMetrics the metrics to update.
   */
  public CloudStorageCompactor(CloudDestination cloudDestination, CloudConfig cloudConfig, Set<PartitionId> partitions,
      VcrMetrics vcrMetrics) {
    this.cloudDestination = cloudDestination;
    this.partitions = partitions;
    this.vcrMetrics = vcrMetrics;
    this.cloudConfig = cloudConfig;
    this.doneLatch = new CountDownLatch(1);
    this.numBlobsErased = -1;
    // Give threads a name, so they can be identified in a thread-dump and set them as daemon or background
    this.executorService = (ScheduledThreadPoolExecutor) Utils.newScheduler(this.cloudConfig.cloudCompactionNumThreads,
        "cloud-compaction-worker-", true);
    logger.info("[COMPACT] Created CloudStorageCompactor");
  }

  @Override
  public void run() {
    long compactionStartTime = System.currentTimeMillis();
    logger.info("[COMPACT] Starting cloud compaction for {} partitions", partitions.size());
    this.numBlobsErased = compactPartitions(); // Blocking call
    this.doneLatch.countDown();
    logger.info("[COMPACT] Completed cloud compaction and erased {} blobs in {} minutes",
        numBlobsErased, TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - compactionStartTime));
  }

  public int getNumBlobsErased() {
    return numBlobsErased;
  }

  /**
   * Returns compaction progress
   * @return True if compaction is complete
   */
  public boolean isCompactionDone(int waitSec) throws InterruptedException {
    this.doneLatch.await(waitSec, TimeUnit.SECONDS);
    return this.doneLatch.getCount() == 0;
  }

  /**
   * Shut down the compactor waiting for in progress operations to complete.
   */
  public void shutdown() {

    /*
      Here is the thread model of this compactor. There are three groups of threads.
      1. Main thread: This threads submits jobs to worker threads and waits for them to finish.
      2. Worker threads: These threads perform the actual task of compaction.
      3. Shutdown thread: It initiates a graceful shutdown of worker and main threads.
     */

    /*
       Shutdown worker threads, though this may not have an effect if workers are blocked on network calls.
       Drain the task queue and wait for all active worker threads to quit.
     */
    List<Runnable> list = new ArrayList<>();
    logger.info("[COMPACT] Dequeued {} tasks on shutdown and stopping {} active worker threads",
        executorService.getQueue().drainTo(list), executorService.getActiveCount());
    cloudDestination.stopCompaction();
    long endWaitTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(
        cloudConfig.cloudBlobCompactionShutdownTimeoutSecs);
    while (executorService.getActiveCount() > 0 && System.currentTimeMillis() < endWaitTime) { yield();}
    logger.info("[COMPACT] Number of active worker threads after stop attempt = {}", executorService.getActiveCount());

    /*
      Shutdown executor.
      This arbitrary wait is merely an attempt to allow the worker threads to gracefully exit.
      We will force a shutdown later. All workers are daemons and JVM _will_ exit when only daemons remain.
      Any data inconsistencies must be resolved separately, but not by trying to predict the right shutdown timeout.
      cloudBlobCompactionShutdownTimeoutSecs is useful for reducing test shutdown times.
    */
    logger.info("[COMPACT] Shutting down worker thread scheduler");
    Utils.shutDownExecutorService(executorService, cloudConfig.cloudBlobCompactionShutdownTimeoutSecs,
        TimeUnit.SECONDS);
  }

  /**
   * @return whether the compactor is shutting down.
   */
  boolean isShutDown() {
    return this.executorService.isShutdown();
  }

  protected boolean isPartitionOwned(PartitionId partitionId) {
    return partitions.contains(partitionId);
  }

  protected class CompactionTask implements Callable<Integer> {
    private final PartitionId partitionId;
    CompactionTask(PartitionId partitionId) {
      /*
        Save a reference to partitionId since it's a local variable in the submitting loop
        and will go out of scope before the task begins.
       */
      this.partitionId = partitionId;
    }
    @Override
    public Integer call() throws Exception {
      /*
        Jobs wait for execution until workers become available.
        If 100 jobs are submitted to 5 workers, only 5 jobs run while the rest wait.
        Partitions can be reassigned while jobs are in the queue.
        Verify ownership of the partition before job initiation.
       */
      if (!isPartitionOwned(partitionId)) {
        logger.info("[COMPACT] Skipping compaction partition-{} as it is disowned", partitionId.toPathString());
        return 0;
      }

      if (cloudDestination.isCompactionStopped()) {
        logger.info("[COMPACT] Skipping compaction partition-{} due to shut dowm", partitionId.toPathString());
        return 0;
      }

      try {
        return cloudDestination.compactPartition(partitionId.toPathString());
      } catch (Throwable throwable) {
        // Swallow all exceptions
        vcrMetrics.compactionFailureCount.inc();
        logger.error("[COMPACT] Failed to compact partition-{} due to {}",
            partitionId.toPathString(), throwable.getMessage());
      }
      return 0;
    }
  }

  protected Integer getResult(Future<Integer> future) {
    try {
      return future.get();
    } catch (Throwable throwable) {
      // Swallow all exceptions
      vcrMetrics.compactionFailureCount.inc();
      logger.error("[COMPACT] Failed to collect compaction result due to {}", throwable.getMessage());
    }
    return 0;
  }

  /**
   * Purge the inactive blobs in all managed partitions.
   * @return the total number of blobs purged.
   */
  public int compactPartitions() {
    try {
      /*
        Create an exclusive local copy of partition set to minimize concurrent accesses.
        Submit jobs and rely on the executor service for assignment and scheduling.
      */
      return executorService.invokeAll(
          new HashSet<>(partitions)
              .stream()
              .map(partitionId -> new CompactionTask(partitionId))
              .collect(Collectors.toSet()))
          .stream()
          .map(future -> getResult(future))
          .reduce(0, Integer::sum);
    } catch (Throwable throwable) {
      vcrMetrics.compactionFailureCount.inc();
      logger.error("[COMPACT] Failed to execute cloud-compaction tasks due to {}",
          throwable.getMessage());
    }
    return 0;
  }
}
