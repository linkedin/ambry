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

import com.azure.storage.blob.BlobContainerClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudRequestAgent;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Pair;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
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
  private static final int CHECKPOINT_BUFFER_SIZE = 64;
  private static final String[] compactionFields =
      {CloudBlobMetadata.FIELD_DELETION_TIME, CloudBlobMetadata.FIELD_EXPIRATION_TIME};
  private final AzureBlobDataAccessor azureBlobDataAccessor;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final int queryLimit;
  private final int purgeLimit;
  private final int queryBucketDays;
  private final int lookbackDays;
  private final int numThreads;
  private final long retentionPeriodMs;
  private final VcrMetrics vcrMetrics;
  private final AzureMetrics azureMetrics;
  private final CloudRequestAgent requestAgent;
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  static final Map<String, Long> emptyCheckpoints;
  static final String CHECKPOINT_CONTAINER = "compaction-checkpoints";
  static final long DEFAULT_TIME = 0L;

  static {
    Map<String, Long> temp = new HashMap<>();
    temp.put(CloudBlobMetadata.FIELD_DELETION_TIME, DEFAULT_TIME);
    temp.put(CloudBlobMetadata.FIELD_EXPIRATION_TIME, DEFAULT_TIME);
    emptyCheckpoints = Collections.unmodifiableMap(temp);
  }

  /**
   * Public constructor.
   * @param azureBlobDataAccessor the {@link AzureBlobDataAccessor} to use.
   * @param cosmosDataAccessor the {@link CosmosDataAccessor} to use.
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param vcrMetrics the VCR metrics to update.
   * @param azureMetrics  the Azure metrics to update.
   */
  public AzureStorageCompactor(AzureBlobDataAccessor azureBlobDataAccessor, CosmosDataAccessor cosmosDataAccessor,
      CloudConfig cloudConfig, VcrMetrics vcrMetrics, AzureMetrics azureMetrics) {
    this.azureBlobDataAccessor = azureBlobDataAccessor;
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.vcrMetrics = vcrMetrics;
    this.azureMetrics = azureMetrics;
    this.retentionPeriodMs = TimeUnit.DAYS.toMillis(cloudConfig.cloudDeletedBlobRetentionDays);
    this.queryLimit = cloudConfig.cloudBlobCompactionQueryLimit;
    this.purgeLimit = cloudConfig.cloudCompactionPurgeLimit;
    this.queryBucketDays = cloudConfig.cloudCompactionQueryBucketDays;
    this.lookbackDays = cloudConfig.cloudCompactionLookbackDays;
    this.numThreads = cloudConfig.cloudCompactionNumThreads;
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
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
   * @throws CloudStorageException if the compaction fails.
   */
  public int compactPartition(String partitionPath) throws CloudStorageException {
    if (isShuttingDown()) {
      logger.info("Skipping compaction of {} due to shut down.", partitionPath);
      return 0;
    }

    Map<String, Long> checkpoints = getCompactionProgress(partitionPath);

    long now = System.currentTimeMillis();
    long queryEndTime = now - retentionPeriodMs;
    long queryStartTime = now - TimeUnit.DAYS.toMillis(lookbackDays);
    int totalBlobsPurged = 0;

    for (String fieldName : compactionFields) {
      try {
        long optimizedStartTime = Math.max(queryStartTime, checkpoints.get(fieldName));
        int numPurged = compactPartition(partitionPath, fieldName, optimizedStartTime, queryEndTime);
        logger.info("Purged {} blobs in partition {} based on {}", numPurged, partitionPath, fieldName);
        totalBlobsPurged += numPurged;
      } catch (CloudStorageException ex) {
        logger.error("Compaction failed for partition {} based on {}", partitionPath, fieldName, ex);
        vcrMetrics.compactionFailureCount.inc();
      }
    }
    if (isShuttingDown()) {
      logger.info("Compaction terminated due to shut down.");
    }
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
   * @return the number of blobs purged or found.
   * @throws CloudStorageException if the compaction fails.
   */
  int compactPartition(String partitionPath, String fieldName, long queryStartTime, long queryEndTime)
      throws CloudStorageException {

    logger.info("Compacting partition {} for {} in time range {} - {}", partitionPath, fieldName,
        new Date(queryStartTime).toString(), new Date(queryEndTime).toString());
    // Iterate until returned list size < limit, time runs out or we get shut down
    int totalPurged = 0;
    long bucketTimeRange = TimeUnit.DAYS.toMillis(queryBucketDays);
    logger.debug("Dividing compaction query for {} into buckets of {} days", partitionPath, queryBucketDays);
    long bucketStartTime = queryStartTime;
    while (bucketStartTime < queryEndTime) {
      long bucketEndTime = Math.min(bucketStartTime + bucketTimeRange, queryEndTime);
      int numPurged = compactPartitionBucketed(partitionPath, fieldName, bucketStartTime, bucketEndTime);
      totalPurged += numPurged;

      bucketStartTime += bucketTimeRange;
      if (isShuttingDown()) {
        logger.debug("Shutting down for partition {}.", partitionPath);
        break;
      }

      if (totalPurged >= purgeLimit) {
        logger.debug("Reached limit for partition {} with {} blobs purged.", partitionPath, totalPurged);
        break;
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
   * @return the number of blobs purged or found.
   * @throws CloudStorageException if the compaction fails.
   */
  private int compactPartitionBucketed(String partitionPath, String fieldName, long queryStartTime, long queryEndTime)
      throws CloudStorageException {

    if (queryEndTime - queryStartTime > TimeUnit.DAYS.toMillis(queryBucketDays)) {
      throw new IllegalArgumentException("Time window is longer than " + queryBucketDays + " days");
    }

    int totalPurged = 0;
    long progressTime = 0;
    while (!isShuttingDown()) {

      final long newQueryStartTime = queryStartTime; // just to use in lambda
      List<CloudBlobMetadata> deadBlobs = requestAgent.doWithRetries(
          () -> getDeadBlobs(partitionPath, fieldName, newQueryStartTime, queryEndTime, queryLimit), "GetDeadBlobs",
          partitionPath);
      if (deadBlobs.isEmpty()) {
        // If query returned nothing, we can mark progress up to queryEndTime
        progressTime = queryEndTime;
        break;
      }
      if (isShuttingDown()) {
        break;
      }
      totalPurged += requestAgent.doWithRetries(() -> purgeBlobs(deadBlobs), "PurgeBlobs", partitionPath);
      vcrMetrics.blobCompactionRate.mark(deadBlobs.size());

      // Adjust startTime for next query
      CloudBlobMetadata lastBlob = deadBlobs.get(deadBlobs.size() - 1);
      progressTime = fieldName.equals(CloudBlobMetadata.FIELD_DELETION_TIME) ? lastBlob.getDeletionTime()
          : lastBlob.getExpirationTime();
      queryStartTime = progressTime;

      if (deadBlobs.size() < queryLimit) {
        // No more dead blobs to query.
        break;
      }

      if (totalPurged >= purgeLimit) {
        // Reached the purge threshold, give other partitions a chance.
        break;
      }
    }

    if (progressTime > 0) {
      updateCompactionProgress(partitionPath, fieldName, progressTime);
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
   * @throws CloudStorageException if the query fails.
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
        // Record as time per single blob deletion
        azureMetrics.blobDeletionTime.update((t1 - t0) / deletedCount, TimeUnit.MILLISECONDS);
      } else {
        // Note: should not get here since purgeBlobs throws exception if any blob exists and could not be deleted.
        return 0;
      }

      // Remove them from Cosmos too
      cosmosDataAccessor.deleteMetadata(deletedBlobs);
      long t2 = System.currentTimeMillis();
      // Record as time per single record deletion
      azureMetrics.documentDeleteTime.update((t2 - t1) / deletedCount, TimeUnit.MILLISECONDS);
      azureMetrics.blobDeletedCount.inc(deletedCount);
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
   * @throws CloudStorageException if the operation fails.
   */
  Map<String, Long> getCompactionProgress(String partitionPath) throws CloudStorageException {
    // TODO: change return type to POJO with getters and serde methods
    String payload = requestAgent.doWithRetries(() -> {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(CHECKPOINT_BUFFER_SIZE);
      boolean hasCheckpoint = azureBlobDataAccessor.downloadFile(CHECKPOINT_CONTAINER, partitionPath, baos, false);
      return hasCheckpoint ? baos.toString() : null;
    }, "Download compaction checkpoint", partitionPath);
    if (payload == null) {
      return new HashMap(emptyCheckpoints);
    }
    try {
      // Payload format: {"expirationTime" : 12345, "deletionTime" : 67890}
      ObjectNode jsonNode = (ObjectNode) objectMapper.readTree(payload);
      Map<String, Long> checkpoints = new HashMap<>();
      for (String fieldName : compactionFields) {
        checkpoints.put(fieldName, jsonNode.has(fieldName) ? jsonNode.get(fieldName).longValue() : DEFAULT_TIME);
      }
      return checkpoints;
    } catch (IOException e) {
      logger.error("Could not retrieve compaction progress for {}", partitionPath, e);
      azureMetrics.compactionProgressReadErrorCount.inc();
      return new HashMap(emptyCheckpoints);
    }
  }

  /**
   * Update the compaction progress for a partition.
   * @param partitionPath the partition to update.
   * @param fieldName the compaction field (deletion or expiration time).
   * @param progressTime the updated progress time.
   * @return true if the checkpoint file was updated, otherwise false.
   */
  boolean updateCompactionProgress(String partitionPath, String fieldName, long progressTime) {
    try {
      // load existing progress checkpoint.
      Map<String, Long> checkpoints = getCompactionProgress(partitionPath);
      // Ensure we don't downgrade progress already recorded.
      if (progressTime <= checkpoints.getOrDefault(fieldName, DEFAULT_TIME)) {
        logger.info("Skipping update of compaction progress for {} because saved {} is more recent.", partitionPath,
            fieldName);
        return false;
      }
      checkpoints.put(fieldName, Math.max(progressTime, checkpoints.get(fieldName)));
      String json = objectMapper.writeValueAsString(checkpoints);
      ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes());
      requestAgent.doWithRetries(() -> {
        azureBlobDataAccessor.uploadFile(CHECKPOINT_CONTAINER, partitionPath, bais);
        return null;
      }, "Update compaction progress", partitionPath);
      logger.info("Marked compaction of partition {} complete up to {} {}", partitionPath, fieldName,
          new Date(progressTime));
      return true;
    } catch (CloudStorageException | IOException e) {
      logger.error("Could not save compaction progress for {}", partitionPath, e);
      azureMetrics.compactionProgressWriteErrorCount.inc();
      return false;
    }
  }

  /**
   * Retrieve the compaction progress for all partitions, sorted by furthest behind.
   * @return a list of pairs each containing a partition and its latest progress time.
   * @throws Exception
   */
  List<Pair<String, Long>> getAllCompactionProgress() throws Exception {
    // Read all checkpoint files and dump results into sortable table.
    BlobContainerClient containerClient =
        azureBlobDataAccessor.getStorageClient().getBlobContainerClient(CHECKPOINT_CONTAINER);
    List<String> checkpoints = new ArrayList<>();
    containerClient.listBlobs().forEach(item -> {
      checkpoints.add(item.getName());
    });
    logger.info("Retrieving checkpoints for {} partitions", checkpoints.size());
    List<Pair<String, Long>> partitionProgressList = Collections.synchronizedList(new ArrayList<>());
    ForkJoinPool forkJoinPool = new ForkJoinPool(numThreads);
    forkJoinPool.submit(() -> {
      checkpoints.parallelStream().forEach(partition -> {
        try {
          Map<String, Long> map = getCompactionProgress(partition);
          long progressTime = Collections.min(map.values());
          partitionProgressList.add(new Pair(partition, progressTime));
        } catch (CloudStorageException cse) {
          logger.error("Failed for partition {}", partition, cse);
        }
      });
    }).get();

    Collections.sort(partitionProgressList, Comparator.comparingLong(Pair::getSecond));
    return partitionProgressList;
  }
}
