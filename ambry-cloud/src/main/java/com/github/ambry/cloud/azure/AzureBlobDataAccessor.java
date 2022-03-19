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

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudUpdateValidator;
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
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data accessor class for Azure Blob Storage.
 */
public class AzureBlobDataAccessor {

  private static final Logger logger = LoggerFactory.getLogger(AzureBlobDataAccessor.class);
  private final AzureMetrics azureMetrics;
  private final AzureBlobLayoutStrategy blobLayoutStrategy;
  private ProxyOptions proxyOptions;
  private final int purgeBatchSize;
  private final Duration requestTimeout, uploadTimeout, batchTimeout;
  private final BlobRequestConditions defaultRequestConditions = null;
  private final StorageClient storageClient;
  private Callable<?> updateCallback = null;

  /**
   * Production constructor
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param azureCloudConfig the {@link AzureCloudConfig} to use.
   * @param blobLayoutStrategy the {@link AzureBlobLayoutStrategy} to use.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @throws ReflectiveOperationException
   */
  public AzureBlobDataAccessor(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig,
      AzureBlobLayoutStrategy blobLayoutStrategy, AzureMetrics azureMetrics) throws ReflectiveOperationException {
    this.blobLayoutStrategy = blobLayoutStrategy;
    this.azureMetrics = azureMetrics;
    this.purgeBatchSize = azureCloudConfig.azurePurgeBatchSize;
    // Check for network proxy
    proxyOptions = (cloudConfig.vcrProxyHost == null) ? null : new ProxyOptions(ProxyOptions.Type.HTTP,
        new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort));
    if (proxyOptions != null) {
      logger.info("Using proxy: {}:{}", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
    }
    requestTimeout = Duration.ofMillis(cloudConfig.cloudRequestTimeout);
    uploadTimeout = Duration.ofMillis(cloudConfig.cloudUploadRequestTimeout);
    batchTimeout = Duration.ofMillis(cloudConfig.cloudBatchRequestTimeout);

    storageClient = Utils.getObj(azureCloudConfig.azureStorageClientClass, cloudConfig, azureCloudConfig, azureMetrics,
        blobLayoutStrategy);
  }

  /**
   * Test constructor
   * @param blobServiceClient the {@link BlobServiceClient} to use.
   * @param blobBatchClient the {@link BlobBatchClient} to use.
   * @param clusterName the cluster name to use.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  AzureBlobDataAccessor(BlobServiceClient blobServiceClient, BlobBatchClient blobBatchClient, String clusterName,
      AzureMetrics azureMetrics, AzureCloudConfig azureCloudConfig) {
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName);
    try {
      this.storageClient =
          Utils.getObj(AzureCloudConfig.DEFAULT_AZURE_STORAGE_CLIENT_CLASS, blobServiceClient, blobBatchClient,
              azureMetrics, blobLayoutStrategy, azureCloudConfig);
    } catch (ReflectiveOperationException roEx) {
      throw new IllegalArgumentException("Unable to instantiate storage client: " + roEx.getMessage(), roEx);
    }
    this.azureMetrics = azureMetrics;
    this.purgeBatchSize = AzureCloudConfig.DEFAULT_PURGE_BATCH_SIZE;
    requestTimeout = null;
    uploadTimeout = null;
    batchTimeout = null;
  }

  /**
   * Visible for testing.
   * @return the underlying {@link BlobServiceClient}.
   */
  public BlobServiceClient getStorageClient() {
    return storageClient.getStorageClient();
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
      Map<String, String> metadata = cloudBlobMetadata.toMap();
      storageClient.uploadWithResponse(blobId, blobInputStream, inputLength, null, metadata, null, null,
          blobRequestConditions, uploadTimeout);
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
   * Upload a file to blob storage.  Any existing file with the same name will be replaced.
   * @param containerName name of the container where blob is stored.
   * @param fileName the blob filename.
   * @param inputStream the input stream to use for upload.
   * @throws BlobStorageException for any error on ABS side.
   * @throws IOException for any error with supplied data stream.
   */
  public void uploadFile(String containerName, String fileName, InputStream inputStream)
      throws BlobStorageException, IOException {
    try {
      storageClient.uploadWithResponse(containerName, fileName, true, inputStream, inputStream.available(), null, null,
          null, null, defaultRequestConditions, uploadTimeout);
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
      storageClient.downloadWithResponse(containerName, fileName, false, outputStream, null, null,
          defaultRequestConditions, false, uploadTimeout);
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
   * Delete a file from blob storage, if it exists.
   * @param containerName name of the container containing file to delete.
   * @param fileName name of the file to delete.
   * @return true if the file was deleted, otherwise false.
   * @throws BlobStorageException for any error on ABS side.
   */
  boolean deleteFile(String containerName, String fileName) throws BlobStorageException {
    return storageClient.deleteFile(containerName, fileName);
  }

  /**
   * Perform basic connectivity test.
   */
  void testConnectivity() {
    try {
      // TODO: Turn on verbose logging during this call (how to do in v12?)
      storageClient.testConnectivity();
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
    BlobProperties blobProperties = null;
    try {
      blobProperties = storageClient.getPropertiesWithResponse(blobId, defaultRequestConditions, requestTimeout);
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
   * @param updateFields Map of field names and new values to modify.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} validator for the update passed by the caller.
   * @return a {@link AzureCloudDestination.UpdateResponse} with the updated metadata.
   * @throws BlobStorageException if the blob does not exist or an error occurred.
   * @throws IllegalStateException on request timeout.
   */
  public AzureCloudDestination.UpdateResponse updateBlobMetadata(BlobId blobId, Map<String, Object> updateFields,
      CloudUpdateValidator cloudUpdateValidator) throws Exception {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    updateFields.keySet()
        .forEach(field -> Objects.requireNonNull(updateFields.get(field), String.format("%s cannot be null", field)));

    try {
      Timer.Context storageTimer = azureMetrics.blobUpdateTime.time();
      try {
        BlobProperties blobProperties =
            storageClient.getPropertiesWithResponse(blobId, defaultRequestConditions, requestTimeout);
        // Note: above throws 404 exception if blob does not exist.
        String etag = blobProperties.getETag();
        Map<String, String> metadata = blobProperties.getMetadata();

        if (!cloudUpdateValidator.validateUpdate(CloudBlobMetadata.fromMap(metadata), blobId, updateFields)) {
          return new AzureCloudDestination.UpdateResponse(false, metadata);
        }

        // Update only if any of the values have changed
        Map<String, String> changedFields = updateFields.entrySet()
            .stream()
            .filter(entry -> !String.valueOf(entry.getValue()).equals(metadata.get(entry.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));
        if (changedFields.size() > 0) {
          changedFields.forEach(metadata::put);
          if (updateCallback != null) {
            try {
              updateCallback.call();
            } catch (Exception ex) {
              logger.error("Error in update callback", ex);
            }
          }
          // Set condition to ensure we don't clobber a concurrent update
          BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch(etag);
          storageClient.setMetadataWithResponse(blobId, metadata, blobRequestConditions, requestTimeout, Context.NONE);
          return new AzureCloudDestination.UpdateResponse(true, metadata);
        } else {
          return new AzureCloudDestination.UpdateResponse(false, metadata);
        }
      } finally {
        storageTimer.stop();
      }
    } catch (BlobStorageException e) {
      if (isNotFoundError(e.getErrorCode())) {
        logger.warn("Blob {} not found, cannot update {}.", blobId, updateFields.keySet());
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
      List<Response<Void>> responseList = storageClient.deleteBatch(batchOfBlobs, batchTimeout);
      for (int j = 0; j < responseList.size(); j++) {
        Response<Void> response = responseList.get(j);
        CloudBlobMetadata blobMetadata = batchOfBlobs.get(j);
        // Note: Response.getStatusCode() throws exception on any error.
        try {
          response.getStatusCode();
        } catch (BlobStorageException bex) {
          int statusCode = bex.getStatusCode();
          // Don't worry if blob is already gone
          if (statusCode != HttpURLConnection.HTTP_NOT_FOUND && statusCode != HttpURLConnection.HTTP_GONE) {
            logger.error("Deleting blob {} got status {}", blobMetadata.getId(), statusCode);
            throw bex;
          }
        }
        deletedBlobs.add(blobMetadata);
      }
    }
    return deletedBlobs;
  }

  /**
   * Utility to check if an error code corresponds to a not-found condition.
   * @param errorCode the {@link BlobErrorCode} to check.
   * @return {@code true} for a not-found error, otherwise {@code false}.
   */
  private static boolean isNotFoundError(BlobErrorCode errorCode) {
    return (errorCode == BlobErrorCode.BLOB_NOT_FOUND || errorCode == BlobErrorCode.CONTAINER_NOT_FOUND);
  }
}
