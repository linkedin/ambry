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

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.github.ambry.account.Container;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudContainerCompactor;
import com.github.ambry.cloud.CloudRequestAgent;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.cloud.azure.AzureCloudDestination.*;


/**
 * Class that compacts containers in the Azure cloud by purging blobs of deprecated containers from
 * ABS and Cosmos.
 */
public class AzureContainerCompactor implements CloudContainerCompactor {
  static final String CONTAINER_DELETION_CHECKPOINT_FILE = "container-deletion-checkpoint";
  private static final Logger logger = LoggerFactory.getLogger(AzureContainerCompactor.class);
  private final AzureBlobDataAccessor azureBlobDataAccessor;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final VcrMetrics vcrMetrics;
  private final AzureMetrics azureMetrics;
  private final CloudRequestAgent requestAgent;
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final int queryLimit;
  private final int containerDeletionQueryBatchSize;
  private long latestContainerDeletionTimestamp;

  /**
   * Constructor for {@link AzureContainerCompactor}.
   * @param azureBlobDataAccessor {@link AzureBlobDataAccessor} object to access Azure Blob Store.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object to access CosmosDb.
   * @param cloudConfig {@link CloudConfig} object.
   * @param vcrMetrics {@link VcrMetrics} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @throws CloudStorageException if case of any error.
   */
  public AzureContainerCompactor(AzureBlobDataAccessor azureBlobDataAccessor, CosmosDataAccessor cosmosDataAccessor,
      CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, VcrMetrics vcrMetrics, AzureMetrics azureMetrics)
      throws CloudStorageException {
    this.azureBlobDataAccessor = azureBlobDataAccessor;
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.vcrMetrics = vcrMetrics;
    this.azureMetrics = azureMetrics;
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
    this.queryLimit = azureCloudConfig.containerCompactionCosmosQueryLimit;
    this.containerDeletionQueryBatchSize = azureCloudConfig.cosmosContainerDeletionBatchSize;
    azureMetrics.trackLatestContainerDeletionTimestamp(this);
  }

  /**
   * Update newly deprecated containers from {@code deprecatedContainers} to CosmosDb since last checkpoint.
   * This method is one of the two entry points in {@link AzureContainerCompactor} along with
   * {@link AzureContainerCompactor#compactAssignedDeprecatedContainers(Collection)}.
   * @param deprecatedContainers {@link Collection} of deprecated {@link Container}s.
   * @param partitionIds list of partition ids from where the containers have to be removed.
   * @throws CloudStorageException in case of any error.
   */
  public void deprecateContainers(Collection<Container> deprecatedContainers, Collection<String> partitionIds)
      throws CloudStorageException {
    if (deprecatedContainers.isEmpty() || partitionIds.isEmpty()) {
      logger.warn(
          "Got either empty container set or empty partition list. Skipping update deprecated containers to cloud.");
      return;
    }
    latestContainerDeletionTimestamp = getLatestContainerDeletionTime();
    Set<CosmosContainerDeletionEntry> cosmosContainerDeletionEntries = deprecatedContainers.stream()
        .filter(container -> container.getDeleteTriggerTime() >= latestContainerDeletionTimestamp)
        .map(container -> CosmosContainerDeletionEntry.fromContainer(container, partitionIds))
        .collect(Collectors.toSet());
    long newLastUpdateContainerTimestamp =
        requestAgent.doWithRetries(() -> cosmosDataAccessor.deprecateContainers(cosmosContainerDeletionEntries),
            "updateDeprecatedContainers", null);
    logger.info(String.format("Deprecated %d new containers from %d timestamp", cosmosContainerDeletionEntries.size(),
        latestContainerDeletionTimestamp));
    vcrMetrics.deprecatedContainerCount.inc(cosmosContainerDeletionEntries.size());
    if (newLastUpdateContainerTimestamp != -1) {
      saveLatestContainerDeletionTime(newLastUpdateContainerTimestamp);
      latestContainerDeletionTimestamp = newLastUpdateContainerTimestamp;
    }
  }

  /**
   * Compact blobs of the deprecated container from cloud. This method is one of the two entry points in the
   * {@link AzureContainerCompactor} class along with {@link AzureContainerCompactor#deprecateContainers(Collection, Collection)}.
   * Note that this method is not thread safe as it is expected to run in a single thread.
   * @param assignedPartitions the {@link Collection} of {@link PartitionId}s assigned to this node.
   */
  @Override
  public void compactAssignedDeprecatedContainers(Collection<? extends PartitionId> assignedPartitions) {
    try {
      SortedSet<CosmosContainerDeletionEntry> containerDeletionEntrySet =
          fetchContainerDeletionEntries(assignedPartitions);
      while (!containerDeletionEntrySet.isEmpty()) {
        CosmosContainerDeletionEntry containerDeletionEntry = containerDeletionEntrySet.first();
        containerDeletionEntrySet.remove(containerDeletionEntry);
        for (String partitionId : containerDeletionEntry.getDeletePendingPartitions()) {
          try {
            logger.info("Starting compaction of container {}, account {} in partition {}",
                containerDeletionEntry.getContainerId(), containerDeletionEntry.getAccountId(), partitionId);
            int blobCompactedCount =
                compactContainer(containerDeletionEntry.getContainerId(), containerDeletionEntry.getAccountId(),
                    partitionId);
            logger.info("Compacted {} blobs of deprecated container {} account {} in partition {}", blobCompactedCount,
                containerDeletionEntry.getContainerId(), containerDeletionEntry.getAccountId(), partitionId);
            azureMetrics.deprecatedContainerCompactionSuccessCount.inc();
          } catch (CloudStorageException csEx) {
            azureMetrics.deprecatedContainerCompactionFailureCount.inc();
            logger.error("Container compaction failed for account {} container {} in partition {}",
                containerDeletionEntry.getAccountId(), containerDeletionEntry.getContainerId(), partitionId);
          }
        }
        if (containerDeletionEntrySet.isEmpty()) {
          containerDeletionEntrySet = fetchContainerDeletionEntries(assignedPartitions);
        }
      }
    } catch (CloudStorageException csEx) {
      logger.error("Container compaction failed due to {}", csEx.toString(), csEx);
    }
  }

  /**
   * @return the latest container deletion timestamp.
   */
  public long getLatestContainerDeletionTimestamp() {
    return latestContainerDeletionTimestamp;
  }

  /**
   * Shut down the compactor waiting for in progress operations to complete.
   */
  @Override
  public void shutdown() {
    shuttingDown.set(true);
  }

  /**
   * Read the deprecated container update checkpoint from Azure Blob Store.
   * @return latest delete trigger time checkpoint for deprecated containers.
   * @throws CloudStorageException in case of any error.
   */
  long getLatestContainerDeletionTime() throws CloudStorageException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
    requestAgent.doWithRetries(() -> {
      try {
        azureBlobDataAccessor.downloadFileAsync(AzureCloudDestination.CHECKPOINT_CONTAINER,
            CONTAINER_DELETION_CHECKPOINT_FILE, null, baos, false).join();
        return null;
      } catch (CompletionException e) {
        Exception ex = Utils.extractFutureExceptionCause(e);
        if (ex instanceof BlobStorageException
            && ((BlobStorageException) ex).getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
          return -1;
        }
        throw toCloudStorageException("Error downloading checkpoint file for container deletion",
            Utils.extractFutureExceptionCause(e), azureMetrics);
      }
    }, "read-container-deletion-checkpoint", null);
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(baos.toByteArray());
    buffer.flip();
    if (!buffer.hasRemaining()) {
      return -1;
    }
    // TODO test what happens if the downloaded file is empty
    return buffer.getLong();
  }

  /**
   * Save the deleted container update checkpoint {@code latestContainerDeletionTimestamp} to Azure Blob Store.
   * @param latestContainerDeletionTimestamp timestamp representing deleteTriggerTime upto which deleted containers have been updated in cloud.
   * @throws CloudStorageException in case of any error.
   */
  private void saveLatestContainerDeletionTime(long latestContainerDeletionTimestamp) throws CloudStorageException {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(latestContainerDeletionTimestamp);
      ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array());
      requestAgent.doWithRetries(() -> {
        try {
          azureBlobDataAccessor.uploadFileAsync(AzureCloudDestination.CHECKPOINT_CONTAINER,
              CONTAINER_DELETION_CHECKPOINT_FILE, bais).join();
          return null;
        } catch (CompletionException e) {
          throw toCloudStorageException("Error uploading checkpoint file for container deletion",
              Utils.extractFutureExceptionCause(e), null);
        }
      }, "update-container-deletion-checkpoint", null);
    } catch (CloudStorageException e) {
      logger.error("Could not save update deprecated container progress", e);
      throw e;
    }
  }

  /**
   * Purge all blobs of the specified container from the specified partition.
   * @param containerId container id of the specified container.
   * @param accountId account oid of the specified container.
   * @param partitionPath partition id from which the blobs have to be deleted.
   * @return number of blobs purged.
   * @throws CloudStorageException in case of any error.
   */
  private int compactContainer(short containerId, short accountId, String partitionPath) throws CloudStorageException {
    int totalPurged = 0;
    while (!isShuttingDown()) {
      List<CloudBlobMetadata> blobs = requestAgent.doWithRetries(() -> {
        try {
          return cosmosDataAccessor.getContainerBlobsAsync(partitionPath, accountId, containerId, queryLimit).join();
        } catch (CompletionException e) {
          throw toCloudStorageException(
              "Error getting blobs for container " + accountId + " and container " + containerId,
              Utils.extractFutureExceptionCause(e), azureMetrics);
        }
      }, "GetDeprecatedContainerBlobs", partitionPath);

      if (blobs.isEmpty()) {
        // this means all the blobs of this container have been purged from the partition
        updateCompactionProgress(containerId, accountId, partitionPath);
        break;
      }
      if (isShuttingDown()) {
        break;
      }
      totalPurged += requestAgent.doWithRetries(
          () -> AzureCompactionUtil.purgeBlobs(blobs, azureBlobDataAccessor, azureMetrics, cosmosDataAccessor),
          "PurgeBlobs", partitionPath);
      vcrMetrics.deprecatedContainerBlobCompactionRate.mark(blobs.size());
    }
    return totalPurged;
  }

  /**
   * Update the container deletion entry of the specified container to remove the partition from which all blobs of the
   * container have been compacted. If there are no more partitions left to compact then mark the container deletion entry as deleted.
   * @param containerId container id of the container.
   * @param accountId account if of the container.
   * @param partitionPath partition id from which all blobs of the container have been deleted.
   * @throws CloudStorageException in case of any error.
   */
  private void updateCompactionProgress(short containerId, short accountId, String partitionPath)
      throws CloudStorageException {
    // TODO: update the cache and cosmos container deletion entry table to remove the partitionId from deletePendingPartitions list

    requestAgent.doWithRetries(() -> cosmosDataAccessor.updateContainerDeletionEntry(containerId, accountId,
        (cosmosContainerDeletionEntry, fieldsChanged) -> {
          fieldsChanged.set(cosmosContainerDeletionEntry.removePartition(partitionPath));
          if (cosmosContainerDeletionEntry.getDeletePendingPartitions().isEmpty()) {
            cosmosContainerDeletionEntry.setDeleted(true);
            fieldsChanged.set(true);
          }
        }), "UpdateContainerDeletionProgress", partitionPath);
    logger.info(
        String.format("Compacted all blobs of deprecated container %d account %d from partition %s", containerId,
            accountId, partitionPath));
  }

  /**
   * Fetch the {@link CosmosContainerDeletionEntry} from cloud and create a cache with entries that have atleast one partition
   * assigned to current node.
   */
  private SortedSet<CosmosContainerDeletionEntry> fetchContainerDeletionEntries(
      Collection<? extends PartitionId> assignedPartitions) throws CloudStorageException {
    Set<CosmosContainerDeletionEntry> containerDeletionEntrySet =
        requestAgent.doWithRetries(() -> cosmosDataAccessor.getDeprecatedContainers(containerDeletionQueryBatchSize),
            "GetDeprecatedContainers", null);
    Set<CosmosContainerDeletionEntry> assignedPartitionContainerDeletionEntries = new HashSet<>();
    Set<String> assignedPartitionSet =
        assignedPartitions.stream().map(PartitionId::toPathString).collect(Collectors.toSet());
    for (CosmosContainerDeletionEntry containerDeletionEntry : containerDeletionEntrySet) {
      Set<String> assignedDeletePendingPartitions = containerDeletionEntry.getDeletePendingPartitions()
          .stream()
          .filter(assignedPartitionSet::contains)
          .collect(Collectors.toSet());
      if (assignedDeletePendingPartitions.size() > 0) {
        assignedPartitionContainerDeletionEntries.add(
            new CosmosContainerDeletionEntry(containerDeletionEntry.getContainerId(),
                containerDeletionEntry.getAccountId(), containerDeletionEntry.getDeleteTriggerTimestamp(), false,
                assignedDeletePendingPartitions));
      }
    }
    SortedSet<CosmosContainerDeletionEntry> sortedContainerDeletionEntrySet =
        new TreeSet<>(Comparator.comparing(CosmosContainerDeletionEntry::getDeleteTriggerTimestamp));
    sortedContainerDeletionEntrySet.addAll(assignedPartitionContainerDeletionEntries);
    return sortedContainerDeletionEntrySet;
  }

  /**
   * @return whether the compactor is shutting down.
   */
  boolean isShuttingDown() {
    return shuttingDown.get();
  }
}
