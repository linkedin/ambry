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
import com.azure.storage.common.policy.RetryPolicyType;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy.BlobLayout;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data accessor class for Azure Blob Storage.
 */
public class AzureBlobDataAccessor {

  private static final Logger logger = LoggerFactory.getLogger(AzureBlobDataAccessor.class);
  private final BlobServiceClient storageClient;
  private final BlobBatchClient blobBatchClient;
  private final Configuration storageConfiguration;
  private final AzureMetrics azureMetrics;
  private final AzureBlobLayoutStrategy blobLayoutStrategy;
  // Containers known to exist in the storage account
  private final Set<String> knownContainers = ConcurrentHashMap.newKeySet();
  private ProxyOptions proxyOptions;
  private final int purgeBatchSize;
  private final Duration requestTimeout, uploadTimeout, batchTimeout;
  private final BlobRequestConditions defaultRequestConditions = null;
  private Callable<?> updateCallback = null;

  /**
   * Production constructor
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param azureCloudConfig the {@link AzureCloudConfig} to use.
   * @param blobLayoutStrategy the {@link AzureBlobLayoutStrategy} to use.
   * @param azureMetrics the {@link AzureMetrics} to use.
   */
  AzureBlobDataAccessor(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig,
      AzureBlobLayoutStrategy blobLayoutStrategy, AzureMetrics azureMetrics) {
    this.blobLayoutStrategy = blobLayoutStrategy;
    this.azureMetrics = azureMetrics;
    this.purgeBatchSize = azureCloudConfig.azurePurgeBatchSize;
    this.storageConfiguration = new Configuration();
    // Check for network proxy
    proxyOptions = (cloudConfig.vcrProxyHost == null) ? null : new ProxyOptions(ProxyOptions.Type.HTTP,
        new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort));
    if (proxyOptions != null) {
      logger.info("Using proxy: {}:{}", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
    }
    HttpClient client = new NettyAsyncHttpClientBuilder().proxy(proxyOptions).build();
    requestTimeout = Duration.ofMillis(cloudConfig.cloudRequestTimeout);
    uploadTimeout = Duration.ofMillis(cloudConfig.cloudUploadRequestTimeout);
    batchTimeout = Duration.ofMillis(cloudConfig.cloudBatchRequestTimeout);

    // Note: retry decisions are made at CloudBlobStore level.  Configure storageClient with no retries.
    RequestRetryOptions noRetries = new RequestRetryOptions(RetryPolicyType.FIXED, 1, null, null, null, null);
    storageClient = new BlobServiceClientBuilder().connectionString(azureCloudConfig.azureStorageConnectionString)
        .httpClient(client)
        .retryOptions(noRetries)
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
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName);
    this.azureMetrics = azureMetrics;
    this.blobBatchClient = blobBatchClient;
    this.purgeBatchSize = AzureCloudConfig.DEFAULT_PURGE_BATCH_SIZE;
    requestTimeout = null;
    uploadTimeout = null;
    batchTimeout = null;
  }

  /** Visible for testing */
  void setUpdateCallback(Callable<?> callback) {
    this.updateCallback = callback;
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
      Map<String, String> metadata = cloudBlobMetadata.toMap();
      blobClient.uploadWithResponse(blobInputStream, inputLength, null, metadata, null, null, blobRequestConditions,
          uploadTimeout, Context.NONE);
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
      blobClient.uploadWithResponse(inputStream, inputStream.available(), null, null, null, null,
          defaultRequestConditions, uploadTimeout, Context.NONE);
    } catch (UncheckedIOException e) {
      // error processing input stream
      throw e.getCause();
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
      blobClient.downloadWithResponse(outputStream, null, null, defaultRequestConditions, false, requestTimeout,
          Context.NONE);
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
      storageClient.getBlobContainerClient("partition-0").existsWithResponse(Duration.ofSeconds(5), Context.NONE);
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
      BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobId);
      downloadFile(blobLayout.containerName, blobLayout.blobFilePath, outputStream, true);
      azureMetrics.blobDownloadSuccessCount.inc();
    } catch (Exception e) {
      azureMetrics.blobDownloadErrorCount.inc();
      throw e;
    } finally {
      storageTimer.stop();
    }
  }

  /**
   * Retrieve the metadata for the specified blob.
   * @param blobId The {@link BlobId} to retrieve.
   * @return The {@link CloudBlobMetadata} if the blob was found, or null otherwise.
   * @throws BlobStorageException
   */
  public CloudBlobMetadata getBlobMetadata(BlobId blobId) throws BlobStorageException {
    BlockBlobClient blobClient = getBlockBlobClient(blobId, false);
    BlobProperties blobProperties = null;
    try {
      blobProperties =
          blobClient.getPropertiesWithResponse(defaultRequestConditions, requestTimeout, Context.NONE).getValue();
      if (blobProperties == null) {
        logger.debug("Blob {} not found.", blobId);
        return null;
      }
    } catch (BlobStorageException e) {
      if (isNotFoundError(e.getErrorCode())) {
        logger.debug("Blob {} not found.", blobId);
        return null;
      }
      throw e;
    }
    Map<String, String> metadata = blobProperties.getMetadata();
    return CloudBlobMetadata.fromMap(metadata);
  }

  /**
   * Update the metadata for the specified blob.
   * @param blobId The {@link BlobId} to update.
   * @param fieldName The metadata field to modify.
   * @param value The new value.
   * @return a {@link UpdateResponse} with the updated metadata.
   * @throws BlobStorageException if the blob does not exist or an error occurred.
   * @throws IllegalStateException on request timeout.
   */
  public UpdateResponse updateBlobMetadata(BlobId blobId, String fieldName, Object value) throws BlobStorageException {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(fieldName, "Field name cannot be null");

    try {
      BlockBlobClient blobClient = getBlockBlobClient(blobId, false);
      Timer.Context storageTimer = azureMetrics.blobUpdateTime.time();
      try {
        BlobProperties blobProperties =
            blobClient.getPropertiesWithResponse(defaultRequestConditions, requestTimeout, Context.NONE).getValue();
        // Note: above throws 404 exception if blob does not exist.
        String etag = blobProperties.getETag();
        Map<String, String> metadata = blobProperties.getMetadata();
        // Update only if value has changed
        String textValue = String.valueOf(value);
        if (!textValue.equals(metadata.get(fieldName))) {
          metadata.put(fieldName, textValue);
          if (updateCallback != null) {
            try {
              updateCallback.call();
            } catch (Exception ex) {
              logger.error("Error in update callback", ex);
            }
          }
          // Set condition to ensure we don't clobber a concurrent update
          BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch(etag);
          blobClient.setMetadataWithResponse(metadata, blobRequestConditions, requestTimeout, Context.NONE);
          return new UpdateResponse(true, metadata);
        } else {
          return new UpdateResponse(false, metadata);
        }
      } finally {
        storageTimer.stop();
      }
    } catch (BlobStorageException e) {
      if (isNotFoundError(e.getErrorCode())) {
        logger.warn("Blob {} not found, cannot update {}.", blobId, fieldName);
      }
      if (e.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
        azureMetrics.blobUpdateConflictCount.inc();
      }
      throw e;
    }
  }

  /**
   * Permanently delete the specified blobs in Azure storage.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return list of {@link CloudBlobMetadata} referencing the blobs successfully purged.
   * @throws BlobStorageException if the purge operation fails.
   * @throws RuntimeException if the request times out before a response is received.
   */
  public List<CloudBlobMetadata> purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws BlobStorageException {

    List<CloudBlobMetadata> deletedBlobs = new ArrayList<>();
    List<List<CloudBlobMetadata>> partitionedLists = Utils.partitionList(blobMetadataList, purgeBatchSize);
    for (List<CloudBlobMetadata> batchOfBlobs : partitionedLists) {
      BlobBatch blobBatch = blobBatchClient.getBlobBatch();
      List<Response<Void>> responseList = new ArrayList<>();
      for (CloudBlobMetadata blobMetadata : batchOfBlobs) {
        BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobMetadata);
        responseList.add(blobBatch.deleteBlob(blobLayout.containerName, blobLayout.blobFilePath));
      }
      blobBatchClient.submitBatchWithResponse(blobBatch, false, batchTimeout, Context.NONE);
      for (int j = 0; j < responseList.size(); j++) {
        Response<Void> response = responseList.get(j);
        CloudBlobMetadata blobMetadata = batchOfBlobs.get(j);
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
    BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobId);
    return getBlockBlobClient(blobLayout.containerName, blobLayout.blobFilePath, autoCreateContainer);
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
   * Utility to check if an error code corresponds to a not-found condition.
   * @param errorCode the {@link BlobErrorCode} to check.
   * @return {@code true} for a not-found error, otherwise {@code false}.
   */
  private static boolean isNotFoundError(BlobErrorCode errorCode) {
    return (errorCode == BlobErrorCode.BLOB_NOT_FOUND || errorCode == BlobErrorCode.CONTAINER_NOT_FOUND);
  }

  /**
   * Struct returned by updateBlobMetadata that tells the caller whether the metadata was updated
   * and also returns the (possibly modified) metadata.
   */
  static class UpdateResponse {
    /** Flag indicating whether the metadata was updated. */
    final boolean wasUpdated;
    /** The resulting metadata map. */
    final Map<String, String> metadata;

    UpdateResponse(boolean wasUpdated, Map<String, String> metadata) {
      this.wasUpdated = wasUpdated;
      this.metadata = metadata;
    }
  }
}
