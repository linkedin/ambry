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
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudUpdateValidator;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy.BlobLayout;
import com.github.ambry.cloud.azure.AzureCloudDestination.UpdateResponse;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.FutureUtils;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.store.StoreException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
  private final AsyncStorageClient asyncStorageClient;
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

    asyncStorageClient =
        Utils.getObj(azureCloudConfig.azureStorageClientClass, cloudConfig, azureCloudConfig, azureMetrics,
            blobLayoutStrategy);
  }

  /**
   * Test constructor
   * @param blobServiceAsyncClient the {@link BlobServiceAsyncClient} to use.
   * @param blobBatchAsyncClient the {@link BlobBatchAsyncClient} to use.
   * @param clusterName the cluster name to use.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param cloudConfig {@link CloudConfig} object.
   */
  AzureBlobDataAccessor(BlobServiceAsyncClient blobServiceAsyncClient, BlobBatchAsyncClient blobBatchAsyncClient,
      String clusterName, AzureMetrics azureMetrics, AzureCloudConfig azureCloudConfig, CloudConfig cloudConfig) {
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName);
    try {
      this.asyncStorageClient =
          Utils.getObj(azureCloudConfig.azureStorageClientClass, blobServiceAsyncClient, blobBatchAsyncClient,
              azureMetrics, blobLayoutStrategy, azureCloudConfig);
    } catch (ReflectiveOperationException roEx) {
      throw new IllegalArgumentException("Unable to instantiate storage client: " + roEx.getMessage(), roEx);
    }
    this.azureMetrics = azureMetrics;
    this.purgeBatchSize = AzureCloudConfig.DEFAULT_PURGE_BATCH_SIZE;
    requestTimeout = Duration.ofMillis(cloudConfig.cloudRequestTimeout);
    uploadTimeout = Duration.ofMillis(cloudConfig.cloudUploadRequestTimeout);
    batchTimeout = Duration.ofMillis(cloudConfig.cloudBatchRequestTimeout);
  }

  /**
   * Visible for testing.
   * @return the underlying {@link BlobServiceClient}.
   */
  public BlobServiceAsyncClient getStorageClient() {
    return asyncStorageClient.getStorageClient();
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
   * Upload the blob to Azure storage asynchronously if it does not already exist in the designated container.
   * @param blobId the blobId to upload
   * @param inputLength the input stream length, if known (-1 if not)
   * @param cloudBlobMetadata the blob metadata
   * @param blobInputStream the input stream
   * @return a CompletableFuture of type {@link Boolean} that will eventually be {@code true} if the upload was successful,
   *         or {@code false} if the blob already exists. It will contain an exception if an error occurred.
   */
  public CompletableFuture<Boolean> uploadIfNotExists(BlobId blobId, long inputLength,
      CloudBlobMetadata cloudBlobMetadata, InputStream blobInputStream) {
    BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfNoneMatch("*");
    azureMetrics.blobUploadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobUploadTime.time();
    final CompletableFuture<Void> responseFuture = FutureUtils.orTimeout(
        asyncStorageClient.uploadWithResponse(blobId, blobInputStream, inputLength, null, cloudBlobMetadata.toMap(),
            null, null, blobRequestConditions), uploadTimeout);
    return responseFuture.handle((blockBlobItemResponse, throwable) -> {
      storageTimer.stop();
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (ex instanceof BlobStorageException
            && ((BlobStorageException) ex).getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
          logger.debug("Skipped upload of existing blob {}", blobId);
          azureMetrics.blobUploadConflictCount.inc();
          // Received exception due to blob being already present
          return false;
        } else {
          // Received some other exception
          throw new CompletionException(ex);
        }
      } else {
        // Blob uploaded successfully
        logger.debug("Uploaded blob {} to ABS", blobId);
        azureMetrics.blobUploadSuccessCount.inc();
        return true;
      }
    });
  }

  /**
   * Upload a file to blob storage asynchronously.  Any existing file with the same name will be replaced.
   * @param containerName name of the container where blob is stored.
   * @param fileName the blob filename.
   * @param inputStream the input stream to use for upload.
   * @throws BlobStorageException for any error on ABS side.
   * @throws IOException for any error with supplied data stream.
   * @return a CompletableFuture of type Void that will eventually complete when block blob is uploaded or an exception
   *         if an error occurred.
   */
  public CompletableFuture<Void> uploadFile(String containerName, String fileName, InputStream inputStream)
      throws Exception {
    return FutureUtils.orTimeout(
        asyncStorageClient.uploadWithResponse(containerName, fileName, true, inputStream, inputStream.available(), null,
            null, null, null, defaultRequestConditions), uploadTimeout);
  }

  /**
   * Download a file from blob storage asynchronously.
   * @param containerName name of the container containing blob to download.
   * @param fileName name of the blob.
   * @param outputStream the output stream to use for download.
   * @param errorOnNotFound If {@code true}, throw BlobStorageException on blob not found, otherwise return false.
   * @return a CompletableFuture of type {@link Boolean} that will eventually be {@code true}  if the download was successful,
   *         or {@code false} if the blob was not found. It will contain an exception if an error occurred.
   */
  public CompletableFuture<Boolean> downloadFile(String containerName, String fileName, OutputStream outputStream,
      boolean errorOnNotFound) {
    final CompletableFuture<Void> responseFuture = FutureUtils.orTimeout(
        asyncStorageClient.downloadWithResponse(containerName, fileName, false, outputStream, null, null,
            defaultRequestConditions, false), uploadTimeout);
    return responseFuture.handle((unused, throwable) -> {
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (!errorOnNotFound && ex instanceof BlobStorageException && isNotFoundError(
            ((BlobStorageException) ex).getErrorCode())) {
          // Received exception for blob not found.
          return false;
        } else {
          // Received some other exception
          throw new CompletionException(ex);
        }
      } else {
        // Blob downloaded successfully
        return true;
      }
    });
  }

  /**
   * Delete a file from blob storage asynchronously, if it exists.
   * @param containerName name of the container containing file to delete.
   * @param fileName name of the file to delete.
   * @return a CompletableFuture of type Boolean that will eventually complete successfully when the file is deleted or
   *         will complete exceptionally if an error occurs.
   */
  CompletableFuture<Boolean> deleteFile(String containerName, String fileName) throws Exception {
    return asyncStorageClient.deleteFile(containerName, fileName);
  }

  /**
   * Perform basic connectivity test.
   */
  void testConnectivity() {
    try {
      // TODO: Turn on verbose logging during this call (how to do in v12?)
      asyncStorageClient.testConnectivity();
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
   * @return
   */
  public CompletableFuture<Boolean> downloadBlob(BlobId blobId, OutputStream outputStream) {
    azureMetrics.blobDownloadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobDownloadTime.time();
    BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobId);
    return downloadFile(blobLayout.containerName, blobLayout.blobFilePath, outputStream, true).whenComplete(
        (downloadSuccess, throwable) -> {
          storageTimer.stop();
          if (throwable != null) {
            azureMetrics.blobDownloadErrorCount.inc();
          } else {
            if (downloadSuccess) {
              azureMetrics.blobDownloadSuccessCount.inc();
            }
          }
        });
  }

  /**
   * Retrieve the metadata for the specified blob.
   * @param blobId The {@link BlobId} to retrieve.
   * @return The {@link CloudBlobMetadata} if the blob was found, or null otherwise.
   * @throws BlobStorageException
   */
  public CompletableFuture<CloudBlobMetadata> getBlobMetadata(BlobId blobId) {
    final CompletableFuture<BlobProperties> responseFuture =
        FutureUtils.orTimeout(asyncStorageClient.getPropertiesWithResponse(blobId, defaultRequestConditions),
            requestTimeout);
    return responseFuture.handle((properties, throwable) -> {
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (ex instanceof BlobStorageException && isNotFoundError(((BlobStorageException) ex).getErrorCode())) {
          // Received exception for blob not found.
          logger.debug("Blob {} not found.", blobId);
          return null;
        }
        // Received some other exception
        throw new CompletionException(ex);
      } else {
        if (properties == null) {
          logger.debug("Blob {} not found.", blobId);
          return null;
        } else {
          return CloudBlobMetadata.fromMap(properties.getMetadata());
        }
      }
    });
  }

  /**
   * Update the metadata for the specified blob.
   * @param blobId The {@link BlobId} to update.
   * @param updateFields Map of field names and new values to modify.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} validator for the update passed by the caller.
   * @return a {@link UpdateResponse} with the updated metadata.
   */
  public CompletableFuture<UpdateResponse> updateBlobMetadata(BlobId blobId, Map<String, Object> updateFields,
      CloudUpdateValidator cloudUpdateValidator) {

    CompletableFuture<UpdateResponse> completableFuture = new CompletableFuture<>();

    Objects.requireNonNull(blobId, "BlobId cannot be null");
    updateFields.keySet()
        .forEach(field -> Objects.requireNonNull(updateFields.get(field), String.format("%s cannot be null", field)));

    Timer.Context storageTimer = azureMetrics.blobUpdateTime.time();

    // TODO: Pass executor instead of default async method.
    asyncStorageClient.getPropertiesWithResponse(blobId, defaultRequestConditions)
        .whenCompleteAsync((blobProperties, throwable) -> {
          if (throwable != null) {
            // If blob is not found, adding warning message
            Exception ex = Utils.extractFutureExceptionCause(throwable);
            if (ex instanceof BlobStorageException && isNotFoundError(((BlobStorageException) ex).getErrorCode())) {
              logger.warn("Blob {} not found, cannot update {}.", blobId, updateFields.keySet());
            }
            completableFuture.completeExceptionally(ex);
          } else {
            String etag = blobProperties.getETag();
            Map<String, String> metadata = blobProperties.getMetadata();
            try {
              // Validate the sanity of update operation
              if (!cloudUpdateValidator.validateUpdate(CloudBlobMetadata.fromMap(metadata), blobId, updateFields)) {
                completableFuture.complete(new UpdateResponse(false, metadata));
                storageTimer.stop();
              }
            } catch (StoreException e) {
              completableFuture.completeExceptionally(e);
              storageTimer.stop();
            }

            if (!completableFuture.isDone()) {
              // Update only if any of the values have changed
              Map<String, String> changedFields = updateFields.entrySet()
                  .stream()
                  .filter(entry -> !String.valueOf(entry.getValue()).equals(metadata.get(entry.getKey())))
                  .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));

              if (changedFields.size() <= 0) {
                // There are no changed fields to update
                completableFuture.complete(new UpdateResponse(false, metadata));
                storageTimer.stop();
              } else {
                // Modify the metadata with changes
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

                // Update the metadata
                asyncStorageClient.setMetadataWithResponse(blobId, metadata, blobRequestConditions, Context.NONE)
                    .whenComplete((response, throwableOnUpdate) -> {
                      if (throwableOnUpdate != null) {
                        Exception exOnUpdate = Utils.extractFutureExceptionCause(throwableOnUpdate);
                        if (exOnUpdate instanceof BlobStorageException
                            && ((BlobStorageException) exOnUpdate).getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
                          azureMetrics.blobUpdateConflictCount.inc();
                        }
                        completableFuture.completeExceptionally(exOnUpdate);
                      } else {
                        completableFuture.complete(new UpdateResponse(true, metadata));
                      }
                      storageTimer.stop();
                    });
              }
            }
          }
        });
    return completableFuture;
  }

  /**
   * Permanently delete the specified blobs in Azure storage.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return list of {@link CloudBlobMetadata} referencing the blobs successfully purged.
   */
  public CompletableFuture<List<CloudBlobMetadata>> purgeBlobs(List<CloudBlobMetadata> blobMetadataList) {
    List<CompletableFuture<Void>> operationFutures = new ArrayList<>();
    List<CloudBlobMetadata> deletedBlobs = new ArrayList<>();
    List<List<CloudBlobMetadata>> partitionedLists = Utils.partitionList(blobMetadataList, purgeBatchSize);
    for (List<CloudBlobMetadata> batchOfBlobs : partitionedLists) {
      CompletableFuture<Void> operationFuture = new CompletableFuture<>();
      asyncStorageClient.deleteBatch(batchOfBlobs).whenComplete((responseList, throwable) -> {
        if (throwable != null) {
          operationFuture.completeExceptionally(Utils.extractFutureExceptionCause(throwable));
        } else {
          for (int j = 0; j < responseList.size(); j++) {
            Response<Void> response = responseList.get(j);
            CloudBlobMetadata blobMetadata = batchOfBlobs.get(j);
            // Note: Response.getStatusCode() throws exception on any error.
            try {
              response.getStatusCode();
            } catch (BlobStorageException bse) {
              int statusCode = bse.getStatusCode();
              // Don't worry if blob is already gone
              if (statusCode != HttpURLConnection.HTTP_NOT_FOUND && statusCode != HttpURLConnection.HTTP_GONE) {
                logger.error("Deleting blob {} got status {}", blobMetadata.getId(), statusCode);
                operationFuture.completeExceptionally(bse);
              }
            }
            deletedBlobs.add(blobMetadata);
          }
          // Complete the results for current iteration.
          operationFuture.complete(null);
        }
      });
      operationFutures.add(operationFuture);
    }

    return CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture<?>[0])).thenApply(v -> deletedBlobs);
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
