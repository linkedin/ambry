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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CloudStorageCompactor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageCompactor.class);
  private final CloudDestination cloudDestination;
  private final Set<PartitionId> partitions;
  private final int queryLimit;
  private final long compactionTimeLimitMs;
  private final VcrMetrics vcrMetrics;
  private final boolean testMode;
  private final CloudRequestAgent requestAgent;

  /**
   * Public constructor.
   * @param cloudDestination the cloud destination to use.
   * @param partitions the set of partitions to compact.
   * @param vcrMetrics the metrics to update.
   * @param testMode if true, the compaction methods only perform the query but do not actually remove the blobs.
   */
  public CloudStorageCompactor(CloudDestination cloudDestination, CloudConfig cloudConfig, Set<PartitionId> partitions,
      VcrMetrics vcrMetrics, boolean testMode) {
    this.cloudDestination = cloudDestination;
    this.partitions = partitions;
    this.vcrMetrics = vcrMetrics;
    this.queryLimit = cloudConfig.cloudBlobCompactionQueryLimit;
    compactionTimeLimitMs = TimeUnit.HOURS.toMillis(cloudConfig.cloudBlobCompactionIntervalHours);
    this.testMode = testMode;
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
  }

  @Override
  public void run() {
    Timer.Context timer = vcrMetrics.blobCompactionTime.time();
    try {
      compactPartitions();
    } catch (Throwable th) {
      logger.error("Hit exception running compaction task", th);
    } finally {
      timer.stop();
    }
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
    Set<PartitionId> partitionsSnapshot = new HashSet<>(partitions);
    logger.info("Beginning dead blob compaction for {} partitions", partitions.size());
    long startTime = System.currentTimeMillis();
    int totalBlobsPurged = 0;
    for (PartitionId partitionId : partitionsSnapshot) {
      String partitionPath = partitionId.toPathString();
      if (!partitions.contains(partitionId)) {
        // Looks like partition was reassigned since the loop started, so skip it
        continue;
      }
      logger.info("Running compaction on partition {}", partitionPath);
      try {
        totalBlobsPurged += compactPartition(partitionPath, startTime);
      } catch (CloudStorageException ex) {
        logger.error("Compaction failed for partition {}", partitionPath, ex);
      }
    }
    long compactionTime = (System.currentTimeMillis() - startTime) / 1000;
    logger.info("Purged {} blobs in {} partitions taking {} seconds", totalBlobsPurged, partitionsSnapshot.size(),
        compactionTime);
    return totalBlobsPurged;
  }

  /**
   * Purge the inactive blobs in the specified partition.
   * @param partitionPath the partition to compact.
   * @param cutoffTime the time at which a blob's active status should be evaluated.
   * @return the number of blobs purged or found.
   */
  public int compactPartition(String partitionPath, long cutoffTime) throws CloudStorageException {
    if (testMode) {
      return cloudDestination.getDeadBlobCount(partitionPath, cutoffTime);
    }

    int numDeadBlobs = 0;
    // Iterate until returned list size < limit or time runs out
    while (System.currentTimeMillis() < cutoffTime + compactionTimeLimitMs) {
      List<CloudBlobMetadata> deadBlobs =
          requestAgent.doWithRetries(() -> cloudDestination.getDeadBlobs(partitionPath, cutoffTime, queryLimit),
              "GetDeadBlobs");
      numDeadBlobs += cloudDestination.purgeBlobs(deadBlobs);
      if (deadBlobs.size() < queryLimit) {
        break;
      }
    }
    return numDeadBlobs;
  }
}
