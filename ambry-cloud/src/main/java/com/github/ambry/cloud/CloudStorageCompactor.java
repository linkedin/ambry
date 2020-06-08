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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that runs scheduled or on-demand compaction of blobs in cloud storage.
 */
public class CloudStorageCompactor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageCompactor.class);
  private final CloudDestination cloudDestination;
  private final Set<PartitionId> partitions;
  private final int shutDownTimeoutSecs;
  private final long compactionTimeLimitMs;
  private final VcrMetrics vcrMetrics;
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final int numThreads;
  private final AtomicReference<CountDownLatch> doneLatch = new AtomicReference<>();
  private final ExecutorCompletionService<Integer> executorCompletionService;

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
    this.shutDownTimeoutSecs = cloudConfig.cloudBlobCompactionShutdownTimeoutSecs;
    this.numThreads = cloudConfig.cloudCompactionNumThreads;
    compactionTimeLimitMs = TimeUnit.HOURS.toMillis(cloudConfig.cloudBlobCompactionIntervalHours);
    executorCompletionService = new ExecutorCompletionService<Integer>(Executors.newFixedThreadPool(numThreads));
    doneLatch.set(new CountDownLatch(0));
  }

  @Override
  public void run() {
    compactPartitions();
  }

  /**
   * Shut down the compactor waiting for in progress operations to complete.
   */
  public void shutdown() {
    shuttingDown.set(true);
    logger.info("Compactor received shutdown request, waiting up to {} seconds for in-flight operations to finish",
        shutDownTimeoutSecs);

    cloudDestination.stopCompaction();
    // VcrServer shuts down us before the destination.
    boolean success = false;
    try {
      success = doneLatch.get().await(shutDownTimeoutSecs, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
    }
    if (success) {
      logger.info("Compactor shut down successfully.");
    } else {
      logger.warn("Timed out or interrupted waiting for operations to finish.  If cloud provider uses separate stores"
          + " for data and metadata, some inconsistencies may be present.");
      vcrMetrics.compactionShutdownTimeoutCount.inc();
    }
  }

  /**
   * @return whether the compactor is shutting down.
   */
  boolean isShuttingDown() {
    return shuttingDown.get();
  }

  /**
   * Purge the inactive blobs in all managed partitions.
   * @return the total number of blobs purged.
   */
  public int compactPartitions() {
    if (partitions.isEmpty()) {
      logger.info("Skipping compaction as no partitions are assigned.");
      return 0;
    }
    List<PartitionId> partitionSnapshot = new ArrayList<>(partitions);
    long compactionStartTime = System.currentTimeMillis();
    long timeToQuit = System.currentTimeMillis() + compactionTimeLimitMs;
    int compactionInProgress = 0;
    doneLatch.set(new CountDownLatch(1));
    int totalBlobsPurged = 0;
    int compactedPartitionCount = 0;
    try {
      while (true) {
        while (compactionInProgress < numThreads) {
          if (partitionSnapshot.isEmpty()) {
            break;
          }
          PartitionId partitionId = partitionSnapshot.remove(0);
          executorCompletionService.submit(() -> compactPartition(partitionId));
          compactionInProgress++;
        }
        totalBlobsPurged += executorCompletionService.take().get();
        compactionInProgress--;
        compactedPartitionCount++;
        if (System.currentTimeMillis() >= timeToQuit) {
          logger.info("Compaction terminated due to time limit exceeded.");
          break;
        }
        if (isShuttingDown()) {
          logger.info("Compaction terminated due to shut down.");
          break;
        }
        if (partitionSnapshot.isEmpty()) {
          break;
        }
      }
      while (compactionInProgress > 0) {
        totalBlobsPurged += executorCompletionService.take().get();
        compactionInProgress--;
        compactedPartitionCount++;
      }
      doneLatch.get().countDown();
    } catch (Throwable th) {
      logger.error("Hit exception running compaction task", th);
    } finally {
      long compactionTime = (System.currentTimeMillis() - compactionStartTime) / 1000;
      logger.info("Purged {} blobs in {} partitions taking {} seconds", totalBlobsPurged, compactedPartitionCount,
          compactionTime);
    }
    return totalBlobsPurged;
  }

  /**
   * Purge the inactive blobs in the specified partitions.
   * @param partition the {@link PartitionId} to compact.
   * @return the total number of blobs purged in the partition.
   */
  private int compactPartition(PartitionId partition) {
    if (isShuttingDown()) {
      logger.info("Skipping compaction due to shut down.");
      return 0;
    }

    logger.info("Beginning dead blob compaction for partition {}", partition);

    String partitionPath = partition.toPathString();
    if (!partitions.contains(partition)) {
      // Looks like partition was reassigned since the loop started, so skip it
      logger.warn("Skipping compaction of Partition {} as the partition was reassgined", partition);
      return 0;
    }

    try {
      return cloudDestination.compactPartition(partitionPath);
    } catch (CloudStorageException ex) {
      logger.error("Compaction failed for partition {}", partitionPath, ex);
      vcrMetrics.compactionFailureCount.inc();
    }
    return 0;
  }
}
