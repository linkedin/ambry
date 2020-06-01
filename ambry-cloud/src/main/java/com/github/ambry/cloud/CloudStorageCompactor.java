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

import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudStorageCompactor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageCompactor.class);
  private final CloudDestination cloudDestination;
  private final Set<PartitionId> partitions;
  private final int shutDownTimeoutSecs;
  private final long compactionTimeLimitMs;
  private final VcrMetrics vcrMetrics;
  private final CloudRequestAgent requestAgent;
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final AtomicReference<CountDownLatch> doneLatch = new AtomicReference<>();

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
    compactionTimeLimitMs = TimeUnit.HOURS.toMillis(cloudConfig.cloudBlobCompactionIntervalHours);
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
    doneLatch.set(new CountDownLatch(0));
  }

  @Override
  public void run() {
    try {
      compactPartitions();
    } catch (Throwable th) {
      logger.error("Hit exception running compaction task", th);
    }
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
    if (isShuttingDown()) {
      logger.info("Skipping compaction due to shut down.");
      return 0;
    }

    // TODO: adjust count when compaction uses multiple threads
    doneLatch.set(new CountDownLatch(1));

    Set<PartitionId> partitionsSnapshot = new HashSet<>(partitions);
    logger.info("Beginning dead blob compaction for {} partitions", partitions.size());
    long now = System.currentTimeMillis();
    long compactionStartTime = now;
    long timeToQuit = now + compactionTimeLimitMs;
    int totalBlobsPurged = 0;
    for (PartitionId partitionId : partitionsSnapshot) {
      String partitionPath = partitionId.toPathString();
      if (!partitions.contains(partitionId)) {
        // Looks like partition was reassigned since the loop started, so skip it
        continue;
      }

      try {
        totalBlobsPurged += cloudDestination.compactPartition(partitionPath);
      } catch (CloudStorageException ex) {
        logger.error("Compaction failed for partition {}", partitionPath, ex);
        vcrMetrics.compactionFailureCount.inc();
      }

      if (System.currentTimeMillis() >= timeToQuit) {
        logger.info("Compaction terminated due to time limit exceeded.");
        break;
      }
      if (isShuttingDown()) {
        logger.info("Compaction terminated due to shut down.");
        break;
      }
    }
    doneLatch.get().countDown();
    long compactionTime = (System.currentTimeMillis() - compactionStartTime) / 1000;
    logger.info("Purged {} blobs in {} partitions taking {} seconds", totalBlobsPurged, partitionsSnapshot.size(),
        compactionTime);
    return totalBlobsPurged;
  }
}
