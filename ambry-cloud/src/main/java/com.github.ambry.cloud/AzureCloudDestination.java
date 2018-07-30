/**
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.Account;
import com.github.ambry.account.CloudReplicationConfig;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureCloudDestination implements CloudDestination {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestination.class);

  private String connectionString;
  private String containerName;
  private CloudStorageAccount azureAccount;
  private CloudBlobClient azureBlobClient;
  private CloudBlobContainer azureContainer;

  // For test mocking
  public void setAzureAccount(CloudStorageAccount azureAccount) {
    this.azureAccount = azureAccount;
  }

  public void initialize(CloudReplicationConfig config) throws Exception {
    // Parse the connection string and create a blob client to interact with Blob storage
    // Note: the account and blobClient will be different for every distinct Azure account.
    // Likely need thread pool to manage all the connections and reuse/garbage-collect them
    this.connectionString = config.getConfigSpec();
    this.containerName = config.getCloudContainerName();
    CloudStorageAccount azureStorageAccount =
        azureAccount != null ? azureAccount : CloudStorageAccount.parse(connectionString);
    azureBlobClient = azureStorageAccount.createCloudBlobClient();
    azureContainer = azureBlobClient.getContainerReference(containerName);
    azureContainer.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
        new OperationContext());
  }

  // TODO: track metrics on success/failure and upload time/throughput

  @Override
  public boolean uploadBlob(String blobId, long blobSize, InputStream blobInputStream) throws Exception {

    Objects.requireNonNull(blobId);
    Objects.requireNonNull(containerName);
    Objects.requireNonNull(blobInputStream);

    BlobRequestOptions options = null; // may want to set BlobEncryptionPolicy here
    OperationContext opContext = null;
    try {
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId);

      if (azureBlob.exists()) {
        logger.info("Skipping upload of blob {} as it already exists in Azure container {}.", blobId, containerName);
        return false;
      }

      azureBlob.upload(blobInputStream, blobSize, null, options, opContext);
      logger.info("Uploaded blob {} to Azure container {}.", blobId, containerName);
      return true;

    } catch (Exception e) {
      logger.error("Failed to upload blob: " + blobId, e);
      throw e;
    }
  }

  @Override
  public boolean deleteBlob(String blobId) throws Exception {
    Objects.requireNonNull(blobId);
    Objects.requireNonNull(containerName);

    try {
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId);

      if (!azureBlob.exists()) {
        logger.info("Skipping deletion of blob {} as it does not exist in Azure container {}.", blobId, containerName);
        return false;
      }

      azureBlob.delete();
      logger.info("Deleted blob {} from Azure container {}.", blobId, containerName);
      return true;

    } catch (Exception e) {
      logger.error("Failed to delete blob: " + blobId, e);
      throw e;
    }
  }

}
