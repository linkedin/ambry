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

import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Configuration;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
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
  private final BlobBatchClient blobBatchClient;
  private final Configuration storageConfiguration;
  private final AzureMetrics azureMetrics;
  private final String clusterName;
  // Containers known to exist in the storage account
  private final Set<String> knownContainers = ConcurrentHashMap.newKeySet();
  private ProxyOptions proxyOptions;
  // TODO: add to AzureCloudConfig
  private int purgeBatchSize = 100;

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
    this.storageConfiguration = new Configuration();
    // Check for network proxy
    proxyOptions = (cloudConfig.vcrProxyHost == null) ? null : new ProxyOptions(ProxyOptions.Type.HTTP,
        new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort));
    if (proxyOptions != null) {
      logger.info("Using proxy: {}:{}", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
    }
    HttpClient client = new NettyAsyncHttpClientBuilder().proxy(proxyOptions).build();

    // TODO: may want to set different retry options depending on live serving or replication mode
    RequestRetryOptions requestRetryOptions = new RequestRetryOptions();
    storageClient = new BlobServiceClientBuilder().connectionString(azureCloudConfig.azureStorageConnectionString)
        .httpClient(client)
        .retryOptions(requestRetryOptions)
        .configuration(storageConfiguration)
        .buildClient();
    blobBatchClient = new BlobBatchClientBuilder(storageClient).buildClient();
  }

  /**
   * Test constructor
   * @param storageClient the {@link BlobServiceClient} to use.
   * @param blobBatchClient the {@link BlobBatchClient} to use.
   * @param clusterName the cluster name to use.
   * @param azureMetrics the {@link AzureMetrics} to use.
   */
  AzureBlobDataAccessor(BlobServiceClient storageClient, BlobBatchClient blobBatchClient, String clusterName,
      AzureMetrics azureMetrics) {
    this.storageClient = storageClient;
    this.storageConfiguration = new Configuration();
    this.clusterName = clusterName;
    this.azureMetrics = azureMetrics;
    this.blobBatchClient = blobBatchClient;
  }

  /**
   * Test utility.
   * @return the network {@link ProxyOptions} used to connect to ABS.
   */
  ProxyOptions getProxyOptions() {
    return proxyOptions;
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
      BlockBlobClient blobClient = getBlockBlobClient(blobId, true);
      cloudBlobMetadata.setCloudBlobName(getAzureBlobName(blobId));
      Map<String, String> metadata = getMetadataMap(cloudBlobMetadata);
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
   * Upload a file to blob storage.
   * @param containerName name of the container where blob is stored.
   * @param fileName the blob filename.
   * @param inputStream the input stream to use for upload.
   * @throws BlobStorageException for any error on ABS side.
   * @throws IOException for any error with supplied data stream.
   */
  public void uploadFile(String containerName, String fileName, InputStream inputStream)
      throws BlobStorageException, IOException {
    try {
      BlockBlobClient blobClient = getBlockBlobClient(containerName, fileName, true);
      blobClient.uploadWithResponse(inputStream, inputStream.available(), null, null, null, null, null, null,
          Context.NONE);
    } catch (UncheckedIOException e) {
      // error processing input stream
      throw e.getCause();
    }
  }

  /**
   * Delete a file from blob storage.
   * @param containerName name of the container containing blob to delete.
   * @param fileName name of the blob.
   * @return {@code true} if the deletion was successful, {@code false} if the blob was not found.
   * @throws BlobStorageException for any error on ABS side.
   */
  public boolean deleteFile(String containerName, String fileName) throws BlobStorageException {
    try {
      BlockBlobClient blobClient = getBlockBlobClient(containerName, fileName, false);
      blobClient.delete();
      return true;
    } catch (BlobStorageException e) {
      if (isNotFoundError(e.getErrorCode())) {
        return false;
      } else {
        throw e;
      }
    }
  }

  /**
   * Download a file from blob storage.
   * @param containerName name of the container containing blob to download.
   * @param fileName name of the blob.
   * @param outputStream the output stream to use for download.
   * @param errorOnNotFound If {@code true}, throw BlobStorageException on blob not found, otherwise return false.
   * @return {@code true} if the download was successful, {@code false} if the blob was not found.
   * @throws BlobStorageException for any error on ABS side.
   * @throws UncheckedIOException for any error with supplied data stream.
   */
  public boolean downloadFile(String containerName, String fileName, OutputStream outputStream, boolean errorOnNotFound)
      throws BlobStorageException {
    try {
      BlockBlobClient blobClient = getBlockBlobClient(containerName, fileName, false);
      blobClient.download(outputStream);
      return true;
    } catch (BlobStorageException e) {
      if (!errorOnNotFound && isNotFoundError(e.getErrorCode())) {
        return false;
      } else {
        throw e;
      }
    }
  }

  /**
   * Perform basic connectivity test.
   */
  void testConnectivity() {
    try {
      // TODO: Turn on verbose logging during this call (how to do in v12?)
      storageClient.getBlobContainerClient("partition-0").existsWithResponse(Duration.ofSeconds(1), Context.NONE);
      logger.info("Blob storage connection test succeeded.");
    } catch (BlobStorageException ex) {
      throw new IllegalStateException("Blob storage connection test failed", ex);
    }
  }

  /**
   * Download the blob from Azure storage.
   * @param blobId id of the Ambry blob to be downloaded
   * @param outputStream outputstream to populate the downloaded data with
   * @throws BlobStorageException on Azure side error.
   * @throws UncheckedIOException on error writing to the output stream.
   */
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws BlobStorageException {
    azureMetrics.blobDownloadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobDownloadTime.time();
    try {
      String containerName = getAzureContainerName(blobId.getPartition().toPathString());
      String blobName = getAzureBlobName(blobId);
      downloadFile(containerName, blobName, outputStream, true);
      azureMetrics.blobDownloadSuccessCount.inc();
    } catch (Exception e) {
      azureMetrics.blobDownloadErrorCount.inc();
      throw e;
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
      BlockBlobClient blobClient = getBlockBlobClient(blobId, false);
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
          blobClient.setMetadataWithResponse(metadata, blobRequestConditions, null, Context.NONE);
        }
        return true;
      } finally {
        storageTimer.stop();
      }
    } catch (BlobStorageException e) {
      if (isNotFoundError(e.getErrorCode())) {
        return false;
      }
      if (e.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
        // TODO: blob was updated (race condition), retry the update
      }
      throw e;
    }
  }

  /**
   * Permanently delete the specified blobs in Azure storage.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return list of {@link CloudBlobMetadata} referencing the blobs successfully purged.
   * @throws BlobStorageException if the purge operation fails.
   */
  public List<CloudBlobMetadata> purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws BlobStorageException {

    // Per docs.microsoft.com/en-us/rest/api/storageservices/blob-batch, must use batch size <= 256
    List<CloudBlobMetadata> deletedBlobs = new ArrayList<>();
    List<List<CloudBlobMetadata>> partitionedLists = Lists.partition(blobMetadataList, purgeBatchSize);
    for (List<CloudBlobMetadata> someBlobs : partitionedLists) {
      BlobBatch blobBatch = blobBatchClient.getBlobBatch();
      List<Response<Void>> responseList = new ArrayList<>();
      for (CloudBlobMetadata blobMetadata : someBlobs) {
        String containerName = getAzureContainerName(blobMetadata.getPartitionId());
        String blobName = blobMetadata.getCloudBlobName();
        responseList.add(blobBatch.deleteBlob(containerName, blobName));
      }
      blobBatchClient.submitBatchWithResponse(blobBatch, false, Duration.ofHours(1), Context.NONE);
      for (int j = 0; j < responseList.size(); j++) {
        Response<Void> response = responseList.get(j);
        CloudBlobMetadata blobMetadata = someBlobs.get(j);
        // Note: Response.getStatusCode() throws exception on any error.
        int statusCode;
        try {
          statusCode = response.getStatusCode();
        } catch (BlobStorageException bex) {
          statusCode = bex.getStatusCode();
        }
        switch (statusCode) {
          case HttpURLConnection.HTTP_OK:
          case HttpURLConnection.HTTP_ACCEPTED:
          case HttpURLConnection.HTTP_NOT_FOUND:
          case HttpURLConnection.HTTP_GONE:
            // blob was deleted or already gone
            deletedBlobs.add(blobMetadata);
            break;
          default:
            logger.error("Deleting blob {} got status {}", blobMetadata.getId(), statusCode);
        }
      }
    }
    return deletedBlobs;
  }

  /**
   * Get the block blob client for the supplied blobid.
   * @param blobId id of the blob for which {@code BlockBlobClient} is needed.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code BlockBlobClient} reference.
   */
  private BlockBlobClient getBlockBlobClient(BlobId blobId, boolean autoCreateContainer) {
    String containerName = getAzureContainerName(blobId.getPartition().toPathString());
    String blobName = getAzureBlobName(blobId);
    return getBlockBlobClient(containerName, blobName, autoCreateContainer);
  }

  /**
   * Get the block blob client for the supplied Azure container and blob name.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code BlockBlobClient} reference.
   */
  private BlockBlobClient getBlockBlobClient(String containerName, String blobName, boolean autoCreateContainer) {
    BlobContainerClient containerClient = getContainer(containerName, autoCreateContainer);
    return containerClient.getBlobClient(blobName).getBlockBlobClient();
  }

  /**
   * Get a reference to an Azure container, creating it if necessary.
   * @param containerName the container name.
   * @param autoCreate flag indicating whether to create the container if it does not exist.
   * @return the created {@link BlobContainerClient}.
   */
  private BlobContainerClient getContainer(String containerName, boolean autoCreate) {
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
  private String getAzureContainerName(String partitionPath) {
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
  private String getAzureBlobName(BlobId blobId) {
    // Use the last four chars as prefix to assist in Azure sharding, since beginning of blobId has little variation.
    String blobIdStr = blobId.getID();
    return blobIdStr.substring(blobIdStr.length() - 4) + SEPARATOR + blobIdStr;
  }

  /**
   * Utility to check if an error code corresponds to a not-found condition.
   * @param errorCode the {@link BlobErrorCode} to check.
   * @return {@code true} for a not-found error, otherwise {@code false}.
   */
  private static boolean isNotFoundError(BlobErrorCode errorCode) {
    return (errorCode == BlobErrorCode.BLOB_NOT_FOUND || errorCode == BlobErrorCode.CONTAINER_NOT_FOUND);
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
