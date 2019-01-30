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
package com.github.ambry.cloud.azure;

import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.commons.BlobId;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.InputStream;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link CloudDestination} that interacts with Azure Blob Storage service.
 */
class AzureCloudDestination implements CloudDestination {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureCloudDestination.class);
  private final CloudStorageAccount azureAccount;
  private final CloudBlobClient azureBlobClient;

  /**
   * Construct an Azure cloud destination from a container's replication config.
   * @param configSpec the config spec to use.
   * @throws Exception
   */
  AzureCloudDestination(String configSpec) throws Exception {
    this(configSpec, null);
  }

  /**
   * Construct an Azure cloud destination from a container's replication config and a {@link CloudStorageAccount} instance.
   * @param configSpec the config spec to use.
   * @param azureAccount the {@link CloudStorageAccount} to use.
   * @throws Exception
   */
  AzureCloudDestination(String configSpec, CloudStorageAccount azureAccount) throws Exception {
    if (azureAccount == null) {
      azureAccount = CloudStorageAccount.parse(configSpec);
    }
    this.azureAccount = azureAccount;

    // Create a blob client to interact with Blob storage
    azureBlobClient = azureAccount.createCloudBlobClient();
    LOGGER.info("Created Azure destination");
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long blobSize, InputStream blobInputStream) throws Exception {

    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(blobInputStream, "Input stream cannot be null");

    BlobRequestOptions options = null; // may want to set BlobEncryptionPolicy here
    OperationContext opContext = null;
    try {
      CloudBlobContainer azureContainer = getContainer(blobId, true);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId.getID());

      if (azureBlob.exists()) {
        LOGGER.debug("Skipping upload of blob {} as it already exists in Azure container {}.", blobId, azureContainer.getName());
        return false;
      }

      azureBlob.upload(blobInputStream, blobSize, null, options, opContext);
      LOGGER.debug("Uploaded blob {} to Azure container {}.", blobId, azureContainer.getName());
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to upload blob: " + blobId);
      throw e;
    }
  }

  @Override
  public boolean deleteBlob(BlobId blobId) throws Exception {
    Objects.requireNonNull(blobId);

    try {
      CloudBlobContainer azureContainer = getContainer(blobId, false);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId.getID());

      if (!azureBlob.exists()) {
        LOGGER.debug("Skipping deletion of blob {} as it does not exist in Azure container {}.", blobId, azureContainer.getName());
        return false;
      }

      azureBlob.delete();
      LOGGER.debug("Deleted blob {} from Azure container {}.", blobId, azureContainer.getName());
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to delete blob: " + blobId);
      throw e;
    }
  }

  @Override
  public boolean doesBlobExist(BlobId blobId) throws Exception {
    try {
      CloudBlobContainer azureContainer = getContainer(blobId, false);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId.getID());

      return azureBlob.exists();
    } catch (Exception e) {
      LOGGER.error("Could not check existence of blob: " + blobId);
      throw e;
    }
  }

  // TODO: get a CloudBlobDirectory within container reflecting accountId and containerId
  /**
   * Get an Azure container to place the specified {@link BlobId}.
   * @param blobId the {@link BlobId} that needs a container.
   * @param autoCreate flag indicating whether to create the container if it does not exist.
   * @return the created {@link CloudBlobContainer}.
   * @throws Exception
   */
  private CloudBlobContainer getContainer(BlobId blobId, boolean autoCreate) throws Exception {
    // Need clustermap to construct BlobId and partitionId
    // Either pass to our constructor or pass BlobId to methods
    String partitionPath = blobId.getPartition().toPathString();
    CloudBlobContainer azureContainer = azureBlobClient.getContainerReference(partitionPath);
    if (autoCreate) {
      azureContainer.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());
    }
    return azureContainer;
  }
}
