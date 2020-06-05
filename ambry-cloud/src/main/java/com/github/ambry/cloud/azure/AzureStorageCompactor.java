/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.azure.storage.blob.models.BlobStorageException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudRequestAgent;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.config.CloudConfig;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that compacts partitions in the Azure cloud by purging inactive blobs from
 * ABS and Cosmos.
 */
public class AzureStorageCompactor {
  private static final Logger logger = LoggerFactory.getLogger(AzureStorageCompactor.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final AzureBlobDataAccessor azureBlobDataAccessor;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final int queryLimit;
  private final int queryBucketDays;
  private final int lookbackDays;
  private final long retentionPeriodMs;
  private final long compactionTimeLimitMs;
  private final VcrMetrics vcrMetrics;
  private final AzureMetrics azureMetrics;
  private final CloudRequestAgent requestAgent;
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  final Map<String, Long> emptyCheckpoints;
  static final String CHECKPOINT_CONTAINER = "compaction-checkpoints";
  static final long DEFAULT_TIME = 0L;

  /**
   * Public constructor.
   * @param azureBlobDataAccessor
   * @param cosmosDataAccessor
   * @param cloudConfig
   * @param vcrMetrics the metrics to update.
   */
  public AzureStorageCompactor(AzureBlobDataAccessor azureBlobDataAccessor, CosmosDataAccessor cosmosDataAccessor,
      CloudConfig cloudConfig, VcrMetrics vcrMetrics, AzureMetrics azureMetrics) {
    this.azureBlobDataAccessor = azureBlobDataAccessor;
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.vcrMetrics = vcrMetrics;
    this.azureMetrics = azureMetrics;
    this.retentionPeriodMs = TimeUnit.DAYS.toMillis(cloudConfig.cloudDeletedBlobRetentionDays);
    this.queryLimit = cloudConfig.cloudBlobCompactionQueryLimit;
    this.queryBucketDays = cloudConfig.cloudCompactionQueryBucketDays;
    this.lookbackDays = cloudConfig.cloudCompactionLookbackDays;
    // TODO: change this
    compactionTimeLimitMs = TimeUnit.HOURS.toMillis(cloudConfig.cloudBlobCompactionIntervalHours);
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
    emptyCheckpoints = new HashMap<>();
    emptyCheckpoints.put(CloudBlobMetadata.FIELD_DELETION_TIME, DEFAULT_TIME);
    emptyCheckpoints.put(CloudBlobMetadata.FIELD_EXPIRATION_TIME, DEFAULT_TIME);
  }

  /**
   * Shut down the compactor waiting for in progress operations to complete.
   */
  public void shutdown() {
    shuttingDown.set(true);
  }

  /**
   * @return whether the compactor is shutting down.
   */
  boolean isShuttingDown() {
    return shuttingDown.get();
  }

  /**
   * Purge the inactive blobs in the specified partition.
   * @return the total number of blobs purged.
   */
  public int compactPartition(String partitionPath) throws CloudStorageException {
    if (isShuttingDown()) {
      logger.info("Skipping compaction of {} due to shut down.", partitionPath);
      return 0;
    }

    Map<String, Long> checkpoints;
    try {
      checkpoints = getCompactionProgress(partitionPath);
    } catch (BlobStorageException | UncheckedIOException e) {
      // If checkpoint couldn't be read, skip and try later.
      throw new CloudStorageException("Compaction of " + partitionPath + " failed reading checkpoint", e);
    }

    long now = System.currentTimeMillis();
    long compactionStartTime = now;
    long timeToQuit = now + compactionTimeLimitMs;
    long queryEndTime = now - retentionPeriodMs;
    long queryStartTime = now - TimeUnit.DAYS.toMillis(lookbackDays);
    Date queryEndDate = new Date(queryEndTime);
    int totalBlobsPurged = 0;
    try {
      long deletionStartTime = Math.max(queryStartTime, checkpoints.get(CloudBlobMetadata.FIELD_DELETION_TIME));
      int numPurged =
          compactPartition(partitionPath, CloudBlobMetadata.FIELD_DELETION_TIME, deletionStartTime, queryEndTime,
              timeToQuit);
      logger.info("Purged {} deleted blobs in partition {} up to {}", numPurged, partitionPath, queryEndDate);
      totalBlobsPurged += numPurged;
      long expirationStartTime = Math.max(queryStartTime, checkpoints.get(CloudBlobMetadata.FIELD_EXPIRATION_TIME));
      numPurged =
          compactPartition(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME, expirationStartTime, queryEndTime,
              timeToQuit);
      logger.info("Purged {} expired blobs in partition {} up to {}", numPurged, partitionPath, queryEndDate);
      totalBlobsPurged += numPurged;
    } catch (CloudStorageException ex) {
      logger.error("Compaction failed for partition {}", partitionPath, ex);
      vcrMetrics.compactionFailureCount.inc();
    }
    if (System.currentTimeMillis() >= timeToQuit) {
      logger.info("Compaction terminated due to time limit exceeded.");
    }
    if (isShuttingDown()) {
      logger.info("Compaction terminated due to shut down.");
    }
    long compactionTime = (System.currentTimeMillis() - compactionStartTime) / 1000;
    return totalBlobsPurged;
  }

  /**
   * Purge the inactive blobs in the specified partition.
   * Long time windows are divided into smaller buckets.
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

    logger.info("Compacting partition {} for {} in time range {} - {}", partitionPath, fieldName,
        new Date(queryStartTime).toString(), new Date(queryEndTime).toString());
    // Iterate until returned list size < limit, time runs out or we get shut down
    int totalPurged = 0;
    long bucketTimeRange = TimeUnit.DAYS.toMillis(queryBucketDays);
    logger.debug("Dividing compaction query for {} into buckets of {} days", partitionPath, queryBucketDays);
    long bucketStartTime = queryStartTime;
    while (bucketStartTime < queryEndTime) {
      long bucketEndTime = Math.min(bucketStartTime + bucketTimeRange, queryEndTime);
      int numPurged = compactPartitionBucketed(partitionPath, fieldName, bucketStartTime, bucketEndTime, timeToQuit);
      totalPurged += numPurged;
      bucketStartTime += bucketTimeRange;
      if (isShuttingDown() || System.currentTimeMillis() >= timeToQuit) {
        break;
      }

      updateCompactionProgress(partitionPath, fieldName, bucketEndTime);

      if (numPurged == 0) {
        // TODO: Consider backing off since the last query might have been expensive
      } else {
        logger.info("Purged {} blobs in partition {} up to {} {}", totalPurged, partitionPath, fieldName,
            new Date(bucketEndTime));
      }
    }
    return totalPurged;
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
  private int compactPartitionBucketed(String partitionPath, String fieldName, long queryStartTime, long queryEndTime,
      long timeToQuit) throws CloudStorageException {

    if (queryEndTime - queryStartTime > TimeUnit.DAYS.toMillis(queryBucketDays)) {
      throw new IllegalArgumentException("Time window is longer than " + queryBucketDays + " days");
    }

    int totalPurged = 0;
    while (System.currentTimeMillis() < timeToQuit && !isShuttingDown()) {

      final long newQueryStartTime = queryStartTime; // just to use in lambda
      List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(
          () -> getDeadBlobs(partitionPath, fieldName, newQueryStartTime, queryEndTime, queryLimit), "GetDeadBlobs",
          partitionPath);
      if (deadBlobs.isEmpty() || isShuttingDown()) {
        break;
      }
      totalPurged += requestAgent.doWithRetries(() -> purgeBlobs(deadBlobs), "PurgeBlobs", partitionPath);
      vcrMetrics.blobCompactionRate.mark(deadBlobs.size());
      if (deadBlobs.size() < queryLimit) {
        break;
      }
      // Adjust startTime for next query
      CloudBlobMetadata lastBlob = deadBlobs.get(deadBlobs.size() - 1);
      long latestTime = fieldName.equals(CloudBlobMetadata.FIELD_DELETION_TIME) ? lastBlob.getDeletionTime()
          : lastBlob.getExpirationTime();
      logger.info("Purged partition {} up to {} {}", partitionPath, fieldName, new Date(latestTime));
      queryStartTime = latestTime;
    }
    return totalPurged;
  }

  /**
   * Get the list of blobs in the specified partition that have been inactive for at least the
   * configured retention period.
   * @param partitionPath the partition to query.
   * @param startTime the start of the query time range.
   * @param endTime the end of the query time range.
   * @param maxEntries the max number of metadata records to return.
   * @return a List of {@link CloudBlobMetadata} referencing the deleted blobs found.
   * @throws CloudStorageException
   */
  List<CloudBlobMetadata> getDeadBlobs(String partitionPath, String fieldName, long startTime, long endTime,
      int maxEntries) throws CloudStorageException {
    try {
      return cosmosDataAccessor.getDeadBlobs(partitionPath, fieldName, startTime, endTime, maxEntries);
    } catch (DocumentClientException dex) {
      throw AzureCloudDestination.toCloudStorageException(
          "Failed to query deleted blobs for partition " + partitionPath, dex, azureMetrics);
    }
  }

  /**
   * Permanently delete the specified blobs in Azure storage.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return the number of blobs successfully purged.
   * @throws CloudStorageException if the purge operation fails for any blob.
   */
  int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws CloudStorageException {
    if (blobMetadataList.isEmpty()) {
      return 0;
    }
    azureMetrics.blobDeleteRequestCount.inc(blobMetadataList.size());
    long t0 = System.currentTimeMillis();
    try {
      List<CloudBlobMetadata> deletedBlobs = azureBlobDataAccessor.purgeBlobs(blobMetadataList);
      long t1 = System.currentTimeMillis();
      int deletedCount = deletedBlobs.size();
      azureMetrics.blobDeleteErrorCount.inc(blobMetadataList.size() - deletedCount);
      if (deletedCount > 0) {
        azureMetrics.blobDeletedCount.inc(deletedCount);
        // Record as time per single blob deletion
        azureMetrics.blobDeletionTime.update((t1 - t0) / deletedCount, TimeUnit.MILLISECONDS);
      } else {
        return 0;
      }

      // Remove them from Cosmos too
      cosmosDataAccessor.deleteMetadata(deletedBlobs);
      long t2 = System.currentTimeMillis();
      // Record as time per single record deletion
      azureMetrics.documentDeleteTime.update((t2 - t1) / deletedCount, TimeUnit.MILLISECONDS);
      return deletedCount;
    } catch (Exception ex) {
      azureMetrics.blobDeleteErrorCount.inc(blobMetadataList.size());
      throw AzureCloudDestination.toCloudStorageException("Failed to purge all blobs", ex, azureMetrics);
    }
  }

  /**
   * Get the current compaction progress for a partition.
   * @param partitionPath the partition to check.
   * @return a {@link Map} containing the progress time for compaction based on deletion and expiration time.
   * @throws BlobStorageException if the checkpoint blob
   */
  Map<String, Long> getCompactionProgress(String partitionPath) throws BlobStorageException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
    boolean hasCheckpoint = azureBlobDataAccessor.downloadFile(CHECKPOINT_CONTAINER, partitionPath, baos, false);
    if (!hasCheckpoint) {
      return emptyCheckpoints;
    }
    try {
      // Payload format: {"expirationTime" : 12345, "deletionTime" : 67890}
      ObjectNode jsonNode = (ObjectNode) objectMapper.readTree(baos.toString());
      Map<String, Long> checkpoints = new HashMap<>();
      for (String fieldName : new String[]{CloudBlobMetadata.FIELD_DELETION_TIME,
          CloudBlobMetadata.FIELD_EXPIRATION_TIME}) {
        checkpoints.put(fieldName, jsonNode.has(fieldName) ? jsonNode.get(fieldName).longValue() : DEFAULT_TIME);
      }
      return checkpoints;
    } catch (IOException e) {
      logger.error("Could not retrieve compaction progress for {}", partitionPath, e);
      azureMetrics.compactionProgressReadErrorCount.inc();
      return emptyCheckpoints;
    }
  }

  /**
   * Update the compaction progress for a partition.
   * @param partitionPath the partition to update.
   * @param fieldName the compaction field (deletion or expiration time).
   * @param checkpointTime the updated progress time.
   */
  boolean updateCompactionProgress(String partitionPath, String fieldName, long checkpointTime) {
    try {
      // load existing progress checkpoint.
      Map<String, Long> checkpoints = getCompactionProgress(partitionPath);
      // Ensure we don't downgrade progress already recorded.
      if (checkpointTime <= checkpoints.getOrDefault(fieldName, DEFAULT_TIME)) {
        logger.info("Skipping update of compaction progress for {} because saved {} is more recent.", partitionPath,
            fieldName);
        return false;
      }
      checkpoints.put(fieldName, Math.max(checkpointTime, checkpoints.get(fieldName)));
      String json = objectMapper.writeValueAsString(checkpoints);
      ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes());
      azureBlobDataAccessor.uploadFile(CHECKPOINT_CONTAINER, partitionPath, bais);
      return true;
    } catch (BlobStorageException | IOException e) {
      logger.error("Could not save compaction progress for {}", partitionPath);
      azureMetrics.compactionProgressWriteErrorCount.inc();
      return false;
    }
  }

  // TODO: method that returns checkpoints for all partitions by iterating through the checkpoint container
  // Utility can sort the results to show which partitions are furthest behind.
}
