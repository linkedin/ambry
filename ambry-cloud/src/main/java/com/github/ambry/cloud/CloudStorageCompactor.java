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
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that runs scheduled or on-demand compaction of blobs in cloud storage.
 */
public class CloudStorageCompactor extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageCompactor.class);
  private final CloudDestination cloudDestination;
  private final Set<PartitionId> partitions;
  private final VcrMetrics vcrMetrics;
  private ExecutorService executorService;

  protected CloudConfig cloudConfig;
  protected AtomicReference<Thread> mainThread;

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
    // Give threads a name, so they can be identified in a thread-dump and set them as daemon or background
    this.executorService = Utils.newScheduler(this.cloudConfig.cloudCompactionNumThreads, "cloud-compaction-worker-", true);
    logger.info("[COMPACT] Created CloudStorageCompactor");
  }

  @Override
  public void run() {
    this.mainThread = new AtomicReference<>(Thread.currentThread());
    logger.info("[COMPACT] Thread info = {}", Thread.currentThread());

    // Start-up delay waiting to populate partitions
    long startUpDelaySecs = TimeUnit.SECONDS.toMillis(cloudConfig.cloudBlobCompactionStartupDelaySecs);
    logger.info("[COMPACT] Waiting {} seconds to populate partitions for compaction", startUpDelaySecs);
    try {
      Thread.sleep(startUpDelaySecs);
    } catch (InterruptedException e) {
      logger.error("[COMPACT] Thread start-up delay interrupted due to {}", e.getMessage());
    }

    // Main compaction loop
    while (true) {
      logger.info("[COMPACT] Starting cloud compaction");
      compactPartitions();
      // This shutdown-check prevents us from falling asleep.
      if (isShutDown()) {
        logger.info("[COMPACT] Breaking main loop because compaction executor is shutdown");
        break;
      }
      try {
        logger.info("[COMPACT] Sleeping for {} hours", this.cloudConfig.cloudBlobCompactionIntervalHours);
        Thread.sleep(TimeUnit.HOURS.toMillis(this.cloudConfig.cloudBlobCompactionIntervalHours));
      } catch (InterruptedException e) {
        logger.error("[COMPACT] Thread sleep interrupted due to {}", e.getMessage());
      }
    }
  }

  /**
   * Shut down the compactor waiting for in progress operations to complete.
   */
  public void shutdown() {

    /*
      Here is the thread model of this compactor. There are three groups of threads.
      1. Main thread: This threads submits jobs to worker threads and waits for them to finish. It sleeps between compaction cycles.
      2. Worker threads: These threads perform the actual task of compaction.
      3. Shutdown thread: It initiates a graceful shutdown of worker and main threads.
     */

    // Shutdown worker threads, though this may not have an effect if workers are blocked on network calls
    cloudDestination.stopCompaction();

    /*
      Shutdown executor.
      The arbitrary wait period is proportional to the number of worker threads, instead of a fixed timeout.
      It returns if all workers end before the timeout.
      This wait is merely an attempt to allow the worker threads to gracefully exit. We will force a shutdown later.
      All workers are daemons and JVM _will_ exit when only daemons remain.
      Any data inconsistencies must be resolved separately, but not by trying to predict the right shutdown timeout.
      cloudBlobCompactionShutdownTimeoutSecs is useful for reducing test shutdown times.
    */
    Utils.shutDownExecutorService(executorService,
        cloudConfig.cloudCompactionNumThreads * cloudConfig.cloudBlobCompactionShutdownTimeoutSecs,
        TimeUnit.SECONDS);

    // Interrupt main thread
    if (mainThread != null) {
      logger.info("[COMPACT] Interrupting thread {}", mainThread.get().getName());
      mainThread.get().interrupt();
    }
  }

  /**
   * @return whether the compactor is shutting down.
   */
  boolean isShutDown() {
    return this.executorService.isShutdown();
  }

  /**
   * Purge the inactive blobs in all managed partitions.
   * @return the total number of blobs purged.
   */
  public int compactPartitions() {
    long compactionStartTime = System.currentTimeMillis();
    int totalBlobsPurged = 0;
    HashMap<String, Future<Integer>> compactionTasks = new HashMap<>();
      /*
        Just submit the jobs and let the executor service handle the assignment and scheduling.
        Note that partitions can be updated between compaction cycles when a new replica is added or an old one removed.
        However, I do not know the behavior when a Collection is modified while it is being iterated.
       */
    for (PartitionId partitionId: partitions) {
      try {
        String partitionIdStr = partitionId.toPathString();
        Future<Integer> future = executorService.submit(() -> cloudDestination.compactPartition(partitionIdStr));
        compactionTasks.put(partitionIdStr, future);
      } catch (Throwable throwable) {
        vcrMetrics.compactionFailureCount.inc();
        logger.error("[COMPACT] Failed to submit compaction task for partition-{} due to {}",
            partitionId.toPathString(), throwable.getMessage());
      }
    }

    // Wait for completion or shutdown, don't use any timeouts
    for (String partitionIdStr : compactionTasks.keySet()) {
      // Although this is a finite loop, the future.get() is a blocking one, and we can't predict how long that'll be.
      // This shutdown-check short circuits the loop.
      if (executorService.isShutdown()) {
        logger.info("[COMPACT] Skipping Future.get because of shutdown");
        break;
      }
      try {
        totalBlobsPurged += compactionTasks.get(partitionIdStr).get();
      } catch (Throwable throwable) {
        vcrMetrics.compactionFailureCount.inc();
        logger.error("[COMPACT] Failed to compact a partition-{} due to {}",
            partitionIdStr, throwable.getMessage());
      }
    }
    logger.info("[COMPACT] Erased {} blobs from {} partitions in {} minutes",
        totalBlobsPurged, compactionTasks.size(),
        TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - compactionStartTime));
    return totalBlobsPurged;
  }
}
