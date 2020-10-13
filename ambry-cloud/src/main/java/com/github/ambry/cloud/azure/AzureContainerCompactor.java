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
import com.github.ambry.cloud.CloudRequestAgent;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.config.CloudConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that compacts containers in the Azure cloud by purging blobs of deprecated containers from
 * ABS and Cosmos.
 */
public class AzureContainerCompactor {
  static final String CONTAINER_DELETION_CHECKPOINT_FILE = "container-deletion-checkpoint";
  private static final Logger logger = LoggerFactory.getLogger(AzureContainerCompactor.class);

  private final AzureBlobDataAccessor azureBlobDataAccessor;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final CloudConfig cloudConfig;
  private final VcrMetrics vcrMetrics;
  private final AzureMetrics azureMetrics;
  private final CloudRequestAgent requestAgent;

  /**
   * Constructor for {@link AzureContainerCompactor}.
   * @param azureBlobDataAccessor {@link AzureBlobDataAccessor} object to access Azure Blob Store.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object to access CosmosDb.
   * @param cloudConfig {@link CloudConfig} object.
   * @param vcrMetrics {@link VcrMetrics} object.
   * @param azureMetrics {@link AzureMetrics} object.
   */
  public AzureContainerCompactor(AzureBlobDataAccessor azureBlobDataAccessor, CosmosDataAccessor cosmosDataAccessor,
      CloudConfig cloudConfig, VcrMetrics vcrMetrics, AzureMetrics azureMetrics) {
    this.azureBlobDataAccessor = azureBlobDataAccessor;
    this.cosmosDataAccessor = cosmosDataAccessor;
    this.cloudConfig = cloudConfig;
    this.vcrMetrics = vcrMetrics;
    this.azureMetrics = azureMetrics;
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);
  }

  /**
   * Update newly deprecated containers from {@code deprecatedContainers} to CosmosDb since last checkpoint.
   * @param deprecatedContainers {@link Collection} of deprecatedd {@link Container}s.
   * @throws CloudStorageException in case of any error.
   */
  public void deprecateContainers(Collection<Container> deprecatedContainers, Collection<String> partitionIds)
      throws CloudStorageException {
    if (deprecatedContainers.isEmpty()) {
      logger.info("Got empty set to update deprecated containers. Skipping update deprecated containers to cloud.");
      return;
    }
    long lastUpdatedContainerTimestamp = getLatestContainerDeletionTime();
    long newLastUpdateContainerTimestamp = requestAgent.doWithRetries(() -> cosmosDataAccessor.deprecateContainers(
        deprecatedContainers.stream()
            .filter(container -> container.getDeleteTriggerTime() >= lastUpdatedContainerTimestamp)
            .map(container -> ContainerDeletionEntry.fromContainer(container, partitionIds))
            .collect(Collectors.toSet())), "updateDeprecatedContainers", null);

    if (newLastUpdateContainerTimestamp != -1) {
      saveLatestContainerDeletionTime(newLastUpdateContainerTimestamp);
    }
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
            CONTAINER_DELETION_CHECKPOINT_FILE, baos, true);
        return null;
      }, "read-container-deletion-checkpoint", null);
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.put(baos.toByteArray());
      buffer.flip();
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
}
