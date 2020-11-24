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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.ambry.account.Container;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudContainerCompactor;
import com.github.ambry.cloud.CloudRequestAgent;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  /**
   * Constructor for {@link AzureContainerCompactor}.
   * @param azureBlobDataAccessor {@link AzureBlobDataAccessor} object to access Azure Blob Store.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object to access CosmosDb.
   * @param cloudConfig {@link CloudConfig} object.
   * @param vcrMetrics {@link VcrMetrics} object.
   * @param azureMetrics {@link AzureMetrics} object.
   */
  public AzureContainerCompactor(AzureBlobDataAccessor azureBlobDataAccessor, CosmosDataAccessor cosmosDataAccessor,
      CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, VcrMetrics vcrMetrics, AzureMetrics azureMetrics) {
    this.azureBlobDataAccessor = azureBlobDataAccessor;
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.vcrMetrics = vcrMetrics;
    this.azureMetrics = azureMetrics;
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
    this.queryLimit = azureCloudConfig.containerCompactionCosmosQueryLimit;
    this.containerDeletionQueryBatchSize = azureCloudConfig.cosmosContainerDeletionBatchSize;
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
    long lastUpdatedContainerTimestamp = getLatestContainerDeletionTime();
    long newLastUpdateContainerTimestamp = requestAgent.doWithRetries(() -> cosmosDataAccessor.deprecateContainers(
        deprecatedContainers.stream()
            .filter(container -> container.getDeleteTriggerTime() >= lastUpdatedContainerTimestamp)
            .map(container -> CosmosContainerDeletionEntry.fromContainer(container, partitionIds))
            .collect(Collectors.toSet())), "updateDeprecatedContainers", null);

    if (newLastUpdateContainerTimestamp != -1) {
      saveLatestContainerDeletionTime(newLastUpdateContainerTimestamp);
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
            int blobCompactedCount =
                compactContainer(containerDeletionEntry.getContainerId(), containerDeletionEntry.getAccountId(),
                    partitionId);
          } catch (CloudStorageException csEx) {
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
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
      requestAgent.doWithRetries(() -> {
        azureBlobDataAccessor.downloadFile(AzureCloudDestination.CHECKPOINT_CONTAINER,
            CONTAINER_DELETION_CHECKPOINT_FILE, baos, false);
        return null;
      }, "read-container-deletion-checkpoint", null);
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.put(baos.toByteArray());
      buffer.flip();
      if (!buffer.hasRemaining()) {
        return -1;
      }
      // TODO test what happens if the downloaded file is empty
      return buffer.getLong();
    } catch (BlobStorageException bsex) {
      if (bsex.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
        return -1;
      }
      throw AzureCloudDestination.toCloudStorageException("Exception while reading deprecated container checkpoint",
          bsex, azureMetrics);
    }
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
        azureBlobDataAccessor.uploadFile(AzureCloudDestination.CHECKPOINT_CONTAINER, CONTAINER_DELETION_CHECKPOINT_FILE,
            bais);
        return null;
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
      List<CloudBlobMetadata> blobs = requestAgent.doWithRetries(
          () -> cosmosDataAccessor.getContainerBlobs(partitionPath, accountId, containerId, queryLimit),
          "GetDeprecatedContainerBlobs", partitionPath);

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

    ResourceResponse<Document> updatedDocument = requestAgent.doWithRetries(
        () -> cosmosDataAccessor.updateContainerDeletionEntry(containerId, accountId, (document, fieldsChanged) -> {
          Set<String> deletePendingPartitions = new HashSet<>();
          Iterator<JsonNode> iterator =
              ((ArrayNode) document.get(CosmosContainerDeletionEntry.DELETE_PENDING_PARTITIONS_KEY)).iterator();
          while (iterator.hasNext()) {
            deletePendingPartitions.add(iterator.next().textValue());
          }
          fieldsChanged.set(deletePendingPartitions.remove(partitionPath));
          document.set(CosmosContainerDeletionEntry.DELETE_PENDING_PARTITIONS_KEY, deletePendingPartitions);
          if (deletePendingPartitions.isEmpty()) {
            document.set(CosmosContainerDeletionEntry.DELETED_KEY, true);
            fieldsChanged.set(true);
          }
        }), "UpdateContainerDeletionProgress", partitionPath);
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
