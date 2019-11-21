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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Configuration;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data accessor class for Azure Blob Storage.
 */
public class AzureBlobDataAccessor {

  private static final Logger logger = LoggerFactory.getLogger(AzureBlobDataAccessor.class);
  private static final String SEPARATOR = "-";
  private final BlobServiceClient storageClient;
  private final AzureMetrics azureMetrics;
  private final String clusterName;
  // Containers known to exist in the storage account
  private final Set<String> knownContainers = ConcurrentHashMap.newKeySet();

  /**
   * Production constructor
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param azureCloudConfig the {@link AzureCloudConfig} to use.
   * @param clusterName the cluster name to use for blob naming.
   * @param azureMetrics the {@link AzureMetrics} to use.
   */
  AzureBlobDataAccessor(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, String clusterName,
      AzureMetrics azureMetrics) {
    this.clusterName = clusterName;
    this.azureMetrics = azureMetrics;
    Configuration storageConfiguration = new Configuration();
    // Check for proxy
    if (cloudConfig.vcrProxyHost != null) {
      logger.info("Using proxy: {}:{}", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
      try {
        // TODO: could add vcrProxyScheme to CloudConfig
        URL proxyUrl = new URL("http", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort, "/");
        storageConfiguration.put(Configuration.PROPERTY_HTTPS_PROXY, proxyUrl.toString());
        storageConfiguration.put(Configuration.PROPERTY_HTTP_PROXY, proxyUrl.toString());
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException("Bad proxy info");
      }
    }

    // TODO: set retry options depending on whether we are serving live data or replicating
    RequestRetryOptions requestRetryOptions = new RequestRetryOptions();
    storageClient = new BlobServiceClientBuilder().connectionString(azureCloudConfig.azureStorageConnectionString)
        .retryOptions(requestRetryOptions)
        .configuration(storageConfiguration)
        .buildClient();
  }

  /**
   * Test constructor
   * @param storageClient the {@link BlobServiceClient} to use.
   * @param clusterName the cluster name to use.
   * @param azureMetrics the {@link AzureMetrics} to use.
   */
  AzureBlobDataAccessor(BlobServiceClient storageClient, String clusterName, AzureMetrics azureMetrics) {
    this.storageClient = storageClient;
    this.clusterName = clusterName;
    this.azureMetrics = azureMetrics;
  }

  /**
   * Upload the blob to Azure storage if it does not already exist in the designated container.
   * @param blobId the blobId to upload
   * @param inputLength the input stream length, if known (-1 if not)
   * @param cloudBlobMetadata the blob metadata
   * @param blobInputStream the input stream
   * @return {@code true} if the upload was successful, {@code false} if the blob already exists.
   * @throws BlobStorageException for any error on ABS side.
   * @throws IOException for any error with supplied data stream.
   */
  public boolean uploadIfNotExists(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws BlobStorageException, IOException {
    BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfNoneMatch("*");
    azureMetrics.blobUploadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobUploadTime.time();
    try {
      BlockBlobClient blobClient = getAzureBlobReference(blobId, true);
      cloudBlobMetadata.setCloudBlobName(getAzureBlobName(blobId));
      Map<String, String> metadata = getMetadataMap(cloudBlobMetadata);
      Response<BlockBlobItem> response =
          blobClient.uploadWithResponse(blobInputStream, inputLength, null, metadata, null, null, blobRequestConditions,
              null, Context.NONE);
      logger.debug("Uploaded blob {} to ABS", blobId);
      azureMetrics.blobUploadSuccessCount.inc();
      return true;
    } catch (UncheckedIOException e) {
      // error processing input stream
      throw e.getCause();
    } catch (BlobStorageException e) {
      if (e.getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
        logger.debug("Skipped upload of existing blob {}", blobId);
        azureMetrics.blobUploadConflictCount.inc();
        return false;
      } else {
        throw e;
      }
    } finally {
      storageTimer.stop();
    }
  }

  /**
   * Download the blob from Azure storage.
   * @param blobId id of the Ambry blob to be downloaded
   * @param outputStream outputstream to populate the downloaded data with
   * @throws BlobStorageException on Azure side error.
   * @throws IOException on error writing to the output stream.
   */
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws BlobStorageException, IOException {
    azureMetrics.blobDownloadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobDownloadTime.time();
    try {
      BlockBlobClient blobClient = getAzureBlobReference(blobId, false);
      blobClient.download(outputStream);
      azureMetrics.blobDownloadSuccessCount.inc();
    } catch (UncheckedIOException e) {
      // error processing input stream
      azureMetrics.blobDownloadErrorCount.inc();
      throw e.getCause();
    } finally {
      storageTimer.stop();
    }
  }

  /**
   * Update the metadata for the specified blob.
   * @param blobId The {@link BlobId} to update.
   * @param fieldName The metadata field to modify.
   * @param value The new value.
   * @return {@code true} if the udpate succeeded, {@code false} if the blob was not found.
   * @throws BlobStorageException
   */
  public boolean updateBlobMetadata(BlobId blobId, String fieldName, Object value) throws BlobStorageException {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(fieldName, "Field name cannot be null");

    try {
      BlockBlobClient blobClient = getAzureBlobReference(blobId, false);
      if (!blobClient.exists()) {
        logger.debug("Blob {} not found.", blobId);
        return false;
      }
      Timer.Context storageTimer = azureMetrics.blobUpdateTime.time();
      try {
        BlobProperties blobProperties = blobClient.getProperties();
        if (blobProperties == null) {
          logger.debug("Blob {} not found.", blobId);
          return false;
        }
        String etag = blobProperties.getETag();
        Map<String, String> metadata = blobProperties.getMetadata();
        // Update only if value has changed
        String textValue = String.valueOf(value);
        if (!textValue.equals(metadata.get(fieldName))) {
          metadata.put(fieldName, textValue);
          // Set condition to ensure we don't clobber another update
          BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch(etag);
          Response response = blobClient.setMetadataWithResponse(metadata, blobRequestConditions, null, Context.NONE);
        }
        return true;
      } finally {
        storageTimer.stop();
      }
    } catch (BlobStorageException e) {
      if (e.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
        // TODO: blob was updated (race condition), retry the update
      }
      throw e;
    }
  }

  /**
   * Permanently delete the specified blobs in Azure storage.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return the number of blobs successfully purged.
   * @throws BlobStorageException if the purge operation fails.
   */
  public int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws BlobStorageException {
    // TODO: use batch api to delete all
    // https://github.com/Azure/azure-sdk-for-java/tree/master/sdk/storage/azure-storage-blob-batch
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Get the azure blob reference for blobid.
   * @param blobId id of the blob for which {@code CloudBlockBlob} reference is asked for.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code BlockBlobClient} reference.
   */
  private BlockBlobClient getAzureBlobReference(BlobId blobId, boolean autoCreateContainer) {
    BlobContainerClient containerClient = getContainer(blobId, autoCreateContainer);
    String azureBlobName = getAzureBlobName(blobId);
    return containerClient.getBlobClient(azureBlobName).getBlockBlobClient();
  }

  /**
   * Get an Azure container to place the specified {@link BlobId}.
   * @param blobId the {@link BlobId} that needs a container.
   * @param autoCreate flag indicating whether to create the container if it does not exist.
   * @return the created {@link BlobContainerClient}.
   */
  private BlobContainerClient getContainer(BlobId blobId, boolean autoCreate) {
    String containerName = getAzureContainerName(blobId.getPartition().toPathString());
    BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
    if (autoCreate) {
      if (!knownContainers.contains(containerName)) {
        try {
          if (!containerClient.exists()) {
            containerClient.create();
            logger.info("Created container {}", containerName);
          }
        } catch (BlobStorageException ex) {
          if (ex.getErrorCode() != BlobErrorCode.CONTAINER_ALREADY_EXISTS) {
            logger.error("Failed to create container {}", containerName);
            throw ex;
          }
        }
        knownContainers.add(containerName);
      }
    }
    return containerClient;
  }

  /**
   * @return the name of the Azure storage container where blobs in the specified partition are stored.
   * @param partitionPath the lexical path of the Ambry partition.
   */
  String getAzureContainerName(String partitionPath) {
    // Include Ambry cluster name in case the same storage account is used to backup multiple clusters.
    // Azure requires container names to be all lower case
    String rawContainerName = clusterName + SEPARATOR + partitionPath;
    return rawContainerName.toLowerCase();
  }

  /**
   * Get the blob name to use in Azure Blob Storage
   * @param blobId The {@link BlobId} to store.
   * @return An Azure-friendly blob name.
   */
  String getAzureBlobName(BlobId blobId) {
    // Use the last four chars as prefix to assist in Azure sharding, since beginning of blobId has little variation.
    String blobIdStr = blobId.getID();
    return blobIdStr.substring(blobIdStr.length() - 4) + SEPARATOR + blobIdStr;
  }

  /**
   * @param cloudBlobMetadata the {@link CloudBlobMetadata}.
   * @return a {@link HashMap} of metadata key-value pairs.
   */
  private static HashMap<String, String> getMetadataMap(CloudBlobMetadata cloudBlobMetadata) {
    HashMap<String, String> map = new HashMap<>();
    map.put(CloudBlobMetadata.FIELD_CREATION_TIME, String.valueOf(cloudBlobMetadata.getCreationTime()));
    map.put(CloudBlobMetadata.FIELD_UPLOAD_TIME, String.valueOf(cloudBlobMetadata.getUploadTime()));
    map.put(CloudBlobMetadata.FIELD_EXPIRATION_TIME, String.valueOf(cloudBlobMetadata.getExpirationTime()));
    map.put(CloudBlobMetadata.FIELD_ACCOUNT_ID, String.valueOf(cloudBlobMetadata.getAccountId()));
    map.put(CloudBlobMetadata.FIELD_CONTAINER_ID, String.valueOf(cloudBlobMetadata.getContainerId()));
    map.put(CloudBlobMetadata.FIELD_ENCRYPTION_ORIGIN, cloudBlobMetadata.getEncryptionOrigin().name());
    map.put(CloudBlobMetadata.FIELD_VCR_KMS_CONTEXT, String.valueOf(cloudBlobMetadata.getVcrKmsContext()));
    map.put(CloudBlobMetadata.FIELD_CRYPTO_AGENT_FACTORY, String.valueOf(cloudBlobMetadata.getCryptoAgentFactory()));
    map.put(CloudBlobMetadata.FIELD_CLOUD_BLOB_NAME, String.valueOf(cloudBlobMetadata.getCloudBlobName()));
    return map;
  }
}
