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
  private final int queryLimit;
  private final int shutDownTimeoutSecs;
  private final long retentionPeriodMs;
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
    this.retentionPeriodMs = TimeUnit.DAYS.toMillis(cloudConfig.cloudDeletedBlobRetentionDays);
    this.queryLimit = cloudConfig.cloudBlobCompactionQueryLimit;
    this.shutDownTimeoutSecs = cloudConfig.cloudBlobCompactionShutDownTimeoutSecs;
    compactionTimeLimitMs = TimeUnit.HOURS.toMillis(cloudConfig.cloudBlobCompactionIntervalHours);
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
    doneLatch.set(new CountDownLatch(0));
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
   * Shut down the compactor waiting for in progress operations to complete.
   */
  public void shutdown() {
    shuttingDown.set(true);
    logger.info("Compactor received shutdown request, waiting up to {} seconds for in-flight operations to finish",
        shutDownTimeoutSecs);
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
    long queryEndTime = now - retentionPeriodMs;
    // TODO: we can cache the latest timestamp that we know we have cleared and use that on subsequent calls
    // Starting from beginning of time is too expensive
    // Order partitions by earliest time at which dead blob exists
    // Can start with retention period and go back additional retention periods until no more found
    long queryStartTime = 1;
    int totalBlobsPurged = 0;
    for (PartitionId partitionId : partitionsSnapshot) {
      String partitionPath = partitionId.toPathString();
      if (!partitions.contains(partitionId)) {
        // Looks like partition was reassigned since the loop started, so skip it
        continue;
      }
      logger.info("Running compaction on partition {}", partitionPath);
      try {
        // TODO: before compacting, call getOldestBlob to get queryStartTime
        int numPurged =
            compactPartition(partitionPath, CloudBlobMetadata.FIELD_DELETION_TIME, queryStartTime, queryEndTime,
                timeToQuit);
        logger.info("Purged {} deleted blobs in partition {}", numPurged, partitionPath);
        totalBlobsPurged += numPurged;
        numPurged =
            compactPartition(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME, queryStartTime, queryEndTime,
                timeToQuit);
        logger.info("Purged {} expired blobs in partition {}", numPurged, partitionPath);
        totalBlobsPurged += numPurged;
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

  /**
   * Returns the expired blob in the specified partition with the earliest expiration time.
   * @param partitionPath the partition to check.
   * @return the {@link CloudBlobMetadata} for the expired blob, or NULL if none was found.
   * @throws CloudStorageException
   */
  public CloudBlobMetadata getOldestExpiredBlob(String partitionPath) throws CloudStorageException {
    // TODO: break this up into sequence of queries starting with current retention period and
    // moving backwards until no more blobs are found
    List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(
        () -> cloudDestination.getExpiredBlobs(partitionPath, 1, System.currentTimeMillis(), 1), "GetDeadBlobs",
        partitionPath);
    return deadBlobs.isEmpty() ? null : deadBlobs.get(0);
  }

  /**
   * Returns the deleted blob in the specified partition with the earliest deletion time.
   * @param partitionPath the partition to check.
   * @return the {@link CloudBlobMetadata} for the deleted blob, or NULL if none was found.
   * @throws CloudStorageException
   */
  public CloudBlobMetadata getOldestDeletedBlob(String partitionPath) throws CloudStorageException {
    List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(
        () -> cloudDestination.getDeletedBlobs(partitionPath, 1, System.currentTimeMillis(), 1), "GetDeadBlobs",
        partitionPath);
    return deadBlobs.isEmpty() ? null : deadBlobs.get(0);
  }

  /**
   * Purge the inactive blobs in the specified partition.
   * @param partitionPath the partition to compact.
   * @param fieldName the field name to query on. Allowed values are {@link CloudBlobMetadata#FIELD_DELETION_TIME}
   *                      and {@link CloudBlobMetadata#FIELD_EXPIRATION_TIME}.
   * @param queryStartTime the initial query start time, which will be adjusted as compaction progresses.
   * @param queryEndTime the query end time.
   * @param timeToQuit the time at which compaction should terminate.
   * @return the number of blobs purged or found.
   */
  public int compactPartition(String partitionPath, String fieldName, long queryStartTime, long queryEndTime,
      long timeToQuit) throws CloudStorageException {

    // Iterate until returned list size < limit, time runs out or we get shut down
    int totalPurged = 0;
    while (System.currentTimeMillis() < timeToQuit && !isShuttingDown()) {

      Callable<List<CloudBlobMetadata>> deadBlobsLambda =
          getCallable(partitionPath, fieldName, queryStartTime, queryEndTime);
      List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(deadBlobsLambda, "GetDeadBlobs", partitionPath);
      if (deadBlobs.isEmpty() || isShuttingDown()) {
        break;
      }
      totalPurged +=
          requestAgent.doWithRetries(() -> cloudDestination.purgeBlobs(deadBlobs), "PurgeBlobs", partitionPath);
      if (deadBlobs.size() < queryLimit) {
        break;
      }
      // Adjust startTime for next query
      CloudBlobMetadata lastBlob = deadBlobs.get(deadBlobs.size() - 1);
      long latestTime = fieldName.equals(CloudBlobMetadata.FIELD_DELETION_TIME) ? lastBlob.getDeletionTime()
          : lastBlob.getExpirationTime();
      logger.info("Purged {} blobs in partition {} up to {} {}", totalPurged, partitionPath, fieldName,
          new Date(latestTime));
      queryStartTime = latestTime;
    }
    return totalPurged;
  }

  private Callable<List<CloudBlobMetadata>> getCallable(String partitionPath, String fieldName, long queryStartTime,
      long queryEndTime) {
    if (fieldName.equals(CloudBlobMetadata.FIELD_DELETION_TIME)) {
      return () -> cloudDestination.getDeletedBlobs(partitionPath, queryStartTime, queryEndTime, queryLimit);
    } else if (fieldName.equals(CloudBlobMetadata.FIELD_EXPIRATION_TIME)) {
      return () -> cloudDestination.getExpiredBlobs(partitionPath, queryStartTime, queryEndTime, queryLimit);
    } else {
      throw new IllegalArgumentException("Invalid field: " + fieldName);
    }
  }
}
