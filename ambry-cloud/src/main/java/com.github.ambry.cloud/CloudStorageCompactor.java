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
import java.util.concurrent.Callable;
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
  private final CloudRequestAgent requestAgent;

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
    this.queryLimit = cloudConfig.cloudBlobCompactionQueryLimit;
    compactionTimeLimitMs = TimeUnit.HOURS.toMillis(cloudConfig.cloudBlobCompactionIntervalHours);
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
        int numPurged = compactPartition(partitionPath, CloudBlobMetadata.FIELD_DELETION_TIME, startTime);
        logger.info("Purged {} deleted blobs in partition {}", numPurged, partitionPath);
        totalBlobsPurged += numPurged;
        numPurged = compactPartition(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME, startTime);
        logger.info("Purged {} expired blobs in partition {}", numPurged, partitionPath);
        totalBlobsPurged += numPurged;
      } catch (CloudStorageException ex) {
        logger.error("Compaction failed for partition {}", partitionPath, ex);
      }
    }
    long compactionTime = (System.currentTimeMillis() - startTime) / 1000;
    logger.info("Purged {} blobs in {} partitions taking {} seconds", totalBlobsPurged, partitionsSnapshot.size(),
        compactionTime);
    return totalBlobsPurged;
  }

  public CloudBlobMetadata getOldestExpiredBlob(String partitionPath) throws CloudStorageException {
    List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(
        () -> cloudDestination.getExpiredBlobs(partitionPath, System.currentTimeMillis(), queryLimit), "GetDeadBlobs",
        partitionPath);
    return deadBlobs.isEmpty() ? null : deadBlobs.get(0);
  }

  public CloudBlobMetadata getOldestDeletedBlob(String partitionPath) throws CloudStorageException {
    List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(
        () -> cloudDestination.getDeletedBlobs(partitionPath, System.currentTimeMillis(), queryLimit), "GetDeadBlobs",
        partitionPath);
    return deadBlobs.isEmpty() ? null : deadBlobs.get(0);
  }

  /**
   * Purge the inactive blobs in the specified partition.
   * @param partitionPath the partition to compact.
   * @param fieldName the field name to query on. Allowed values are {@link CloudBlobMetadata#FIELD_DELETION_TIME}
   *                      and {@link CloudBlobMetadata#FIELD_EXPIRATION_TIME}.
   * @param cutoffTime the time at which a blob's active status should be evaluated.
   * @return the number of blobs purged or found.
   */
  public int compactPartition(String partitionPath, String fieldName, long cutoffTime) throws CloudStorageException {
    Callable<List<CloudBlobMetadata>> deadBlobsLambda;
    if (fieldName.equals(CloudBlobMetadata.FIELD_DELETION_TIME)) {
      deadBlobsLambda = () -> cloudDestination.getDeletedBlobs(partitionPath, cutoffTime, queryLimit);
    } else if (fieldName.equals(CloudBlobMetadata.FIELD_EXPIRATION_TIME)) {
      deadBlobsLambda = () -> cloudDestination.getExpiredBlobs(partitionPath, cutoffTime, queryLimit);
    } else {
      throw new IllegalArgumentException("Invalid field: " + fieldName);
    }

    // Iterate until returned list size < limit or time runs out
    int totalPurged = 0;
    while (System.currentTimeMillis() < cutoffTime + compactionTimeLimitMs) {
      List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(deadBlobsLambda, "GetDeadBlobs", partitionPath);
      if (deadBlobs.isEmpty()) {
        break;
      }
      int numPurged =
          requestAgent.doWithRetries(() -> cloudDestination.purgeBlobs(deadBlobs), "PurgeBlobs", partitionPath);
      logger.info("Purged {} blobs in partition {} based on {}", numPurged, partitionPath, fieldName);
      totalPurged += numPurged;
      if (deadBlobs.size() < queryLimit) {
        break;
      }
    }
    return totalPurged;
  }
}
