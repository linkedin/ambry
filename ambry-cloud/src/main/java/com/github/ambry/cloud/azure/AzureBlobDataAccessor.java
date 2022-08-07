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
import java.util.concurrent.atomic.AtomicReference;
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
  private final AzureStorageClient storageClient;
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

    if (azureCloudConfig.azureStorageAccountInfo != null && !azureCloudConfig.azureStorageAccountInfo.isEmpty()) {
      storageClient = new ShardedStorageClient(cloudConfig, azureCloudConfig, azureMetrics, blobLayoutStrategy);
    } else {
      storageClient =
          Utils.getObj(azureCloudConfig.azureStorageClientClass, cloudConfig, azureCloudConfig, azureMetrics,
              blobLayoutStrategy, null);
    }
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
      if (azureCloudConfig.azureStorageAccountInfo != null && !azureCloudConfig.azureStorageAccountInfo.isEmpty()) {
        this.storageClient =
            new ShardedStorageClient(blobServiceAsyncClient, blobBatchAsyncClient, azureMetrics, blobLayoutStrategy,
                azureCloudConfig);
      } else {
        this.storageClient =
            Utils.getObj(azureCloudConfig.azureStorageClientClass, blobServiceAsyncClient, blobBatchAsyncClient,
                azureMetrics, blobLayoutStrategy, azureCloudConfig, null);
      }
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
   * Upload the blob to Azure storage asynchronously if it does not already exist in the designated container.
   * @param blobId the blobId to upload
   * @param inputLength the input stream length, if known (-1 if not)
   * @param cloudBlobMetadata the blob metadata
   * @param blobInputStream the input stream
   * @return a CompletableFuture of type {@link Boolean} that will eventually be {@code true} if the upload was successful,
   *         or {@code false} if the blob already exists. If there was an error on ABS side, it will complete exceptionally
   *         containing the BlobStorageException.
   */
  public CompletableFuture<Boolean> uploadAsyncIfNotExists(BlobId blobId, long inputLength,
      CloudBlobMetadata cloudBlobMetadata, InputStream blobInputStream) {
    BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfNoneMatch("*");
    azureMetrics.blobUploadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobUploadTime.time();
    return FutureUtils.orTimeout(
        storageClient.uploadWithResponse(blobId, blobInputStream, inputLength, null, cloudBlobMetadata.toMap(), null,
            null, blobRequestConditions), uploadTimeout).handle((response, throwable) -> {
      storageTimer.stop();
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (ex instanceof BlobStorageException
            && ((BlobStorageException) ex).getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
          // Received exception due to blob being already present
          logger.debug("Skipped upload of existing blob {}", blobId);
          azureMetrics.blobUploadConflictCount.inc();
          return false;
        }
        throw throwable instanceof CompletionException ? (CompletionException) throwable
            : new CompletionException(throwable);
      }
      // Blob uploaded successfully
      logger.debug("Uploaded blob {} to ABS", blobId);
      azureMetrics.blobUploadSuccessCount.inc();
      return true;
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
  public CompletableFuture<Void> uploadFileAsync(String containerName, String fileName, InputStream inputStream)
      throws Exception {
    return FutureUtils.orTimeout(
        storageClient.uploadWithResponse(containerName, fileName, true, inputStream, inputStream.available(), null,
            null, null, null, defaultRequestConditions), uploadTimeout);
  }

  /**
   * Download a file from blob storage asynchronously.
   * @param containerName name of the container containing blob to download.
   * @param fileName name of the blob.
   * @param blobId Ambry {@link BlobId} associated with the {@code fileName}.
   * @param outputStream the output stream to use for download.
   * @param errorOnNotFound If {@code true}, throw BlobStorageException on blob not found, otherwise return false.
   * @return a CompletableFuture of type {@link Boolean} that will eventually be {@code true} if the download was successful,
   *         or {@code false} if the blob was not found.  If there was an error on ABS side, it will complete exceptionally
   *         containing the BlobStorageException.
   */
  public CompletableFuture<Boolean> downloadFileAsync(String containerName, String fileName, BlobId blobId,
      OutputStream outputStream, boolean errorOnNotFound) {
    return FutureUtils.orTimeout(
        storageClient.downloadWithResponse(containerName, fileName, blobId, false, outputStream, null, null,
            defaultRequestConditions, false), uploadTimeout).handle((unused, throwable) -> {
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (!errorOnNotFound && ex instanceof BlobStorageException && isNotFoundError(
            ((BlobStorageException) ex).getErrorCode())) {
          // Received exception for blob not found.
          return false;
        }
        throw throwable instanceof CompletionException ? (CompletionException) throwable
            : new CompletionException(throwable);
      }
      // Blob downloaded successfully
      return true;
    });
  }

  /**
   * Delete a file from blob storage asynchronously, if it exists.
   * @param containerName name of the container containing file to delete.
   * @param fileName name of the file to delete.
   * @return a CompletableFuture of type Boolean that will eventually complete successfully when the file is deleted or
   *         will complete exceptionally if an error occurs.
   */
  CompletableFuture<Boolean> deleteFileAsync(String containerName, String fileName) {
    return FutureUtils.orTimeout(storageClient.deleteFile(containerName, fileName), uploadTimeout);
  }

  /**
   * Perform basic connectivity test.
   */
  public void testConnectivity() {
    try {
      // TODO: Turn on verbose logging during this call (how to do in v12?)
      storageClient.testConnectivity();
      logger.info("Blob storage connection test succeeded.");
    } catch (BlobStorageException ex) {
      throw new IllegalStateException("Blob storage connection test failed", ex);
    }
  }

  /**
   * Download the blob from Azure storage asynchronously.
   * @param blobId id of the Ambry blob to be downloaded
   * @param outputStream outputstream to populate the downloaded data with
   * @return a CompletableFuture of type {@link Boolean} that will eventually be {@code true} if the blob was downloaded
   *         successfully or {@code false} if the blob was not found.  If there was an error on ABS side, it will
   *         complete exceptionally containing the BlobStorageException.
   */
  public CompletableFuture<Boolean> downloadBlobAsync(BlobId blobId, OutputStream outputStream) {
    azureMetrics.blobDownloadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobDownloadTime.time();
    BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobId);
    return downloadFileAsync(blobLayout.containerName, blobLayout.blobFilePath, blobId, outputStream,
        true).whenComplete((downloadSuccess, throwable) -> {
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
   * Retrieve the metadata for the specified blob asynchronously.
   * @param blobId The {@link BlobId} to retrieve.
   * @return a CompletableFuture that will eventually contain the {@link CloudBlobMetadata} if the blob
   *         was found, or null otherwise. If there was an error on ABS side, it will complete exceptionally containing
   *         the BlobStorageException.
   */
  public CompletableFuture<CloudBlobMetadata> getBlobMetadataAsync(BlobId blobId) {
    return FutureUtils.orTimeout(storageClient.getPropertiesWithResponse(blobId, defaultRequestConditions),
        requestTimeout).handle((properties, throwable) -> {
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (ex instanceof BlobStorageException && isNotFoundError(((BlobStorageException) ex).getErrorCode())) {
          // Received exception for blob not found.
          logger.debug("Blob {} not found.", blobId);
          return null;
        }
        throw throwable instanceof CompletionException ? (CompletionException) throwable
            : new CompletionException(throwable);
      }
      if (properties == null) {
        // If blob properties is null, return null
        logger.debug("Blob {} not found.", blobId);
        return null;
      }
      return CloudBlobMetadata.fromMap(properties.getMetadata());
    });
  }

  /**
   * Update the metadata for the specified blob asynchronously.
   * @param blobId The {@link BlobId} to update.
   * @param updateFields Map of field names and new values to modify.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} validator for the update passed by the caller.
   * @return a CompletableFuture that will eventually contain the {@link UpdateResponse} with the updated metadata.
   *         If there was an error on ABS side, it will complete exceptionally containing the BlobStorageException.
   */
  public CompletableFuture<UpdateResponse> updateBlobMetadataAsync(BlobId blobId, Map<String, Object> updateFields,
      CloudUpdateValidator cloudUpdateValidator) {

    CompletableFuture<UpdateResponse> resultFuture = new CompletableFuture<>();
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    updateFields.keySet()
        .forEach(field -> Objects.requireNonNull(updateFields.get(field), String.format("%s cannot be null", field)));

    Timer.Context storageTimer = azureMetrics.blobUpdateTime.time();
    AtomicReference<Map<String, String>> metadata = new AtomicReference<>();

    // TODO: Pass executor instead of default async method.
    FutureUtils.orTimeout(storageClient.getPropertiesWithResponse(blobId, defaultRequestConditions), requestTimeout)
        .thenCompose((blobProperties) -> {
          // Got the current metadata record of the blob. Check for validity of the update and update the blob metadata.
          String etag = blobProperties.getETag();
          metadata.set(blobProperties.getMetadata());

          try {
            // Validate the sanity of update operation by comparing the fields we want to update against existing metadata
            // fields in Azure. This can throw StoreExceptions which are propagated to caller.
            if (!cloudUpdateValidator.validateUpdate(CloudBlobMetadata.fromMap(metadata.get()), blobId, updateFields)) {
              storageTimer.stop();
              resultFuture.complete(new UpdateResponse(false, metadata.get()));
            }
          } catch (StoreException e) {
            // Received an exception during the update validation. Complete the response future.
            storageTimer.stop();
            throw new CompletionException(e);
          }

          if (!resultFuture.isDone()) {
            // Check for list of changed fields
            Map<String, String> changedFields = updateFields.entrySet()
                .stream()
                .filter(entry -> !String.valueOf(entry.getValue()).equals(metadata.get().get(entry.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));

            // Update the record only if any of its values have changed
            if (changedFields.size() <= 0) {
              // There are no changed fields to update
              storageTimer.stop();
              resultFuture.complete(new UpdateResponse(false, metadata.get()));
            } else {
              // Modify the metadata with changes
              changedFields.forEach(metadata.get()::put);

              // Invoke the update Callback. This is only needed for our tests.
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
              return FutureUtils.orTimeout(
                  storageClient.setMetadataWithResponse(blobId, metadata.get(), blobRequestConditions, Context.NONE),
                  requestTimeout);
            }
          }
          return null;
        })
        .whenComplete((response, throwable) -> {
          storageTimer.stop();
          if (throwable != null) {
            Exception ex = Utils.extractFutureExceptionCause(throwable);
            if (ex instanceof BlobStorageException) {
              if (isNotFoundError(((BlobStorageException) ex).getErrorCode())) {
                logger.warn("Blob {} not found, cannot update {}.", blobId, updateFields.keySet());
              } else if (((BlobStorageException) ex).getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
                logger.warn("Blob {} update condition not met, cannot update {}.", blobId, updateFields.keySet());
                azureMetrics.blobUpdateConflictCount.inc();
              }
            }
            resultFuture.completeExceptionally(ex);
          } else {
            resultFuture.complete(new UpdateResponse(true, metadata.get()));
          }
        });
    return resultFuture;
  }

  /**
   * Permanently delete the specified blobs in Azure storage asynchronously.
   * @param blobMetadataList the list of {@link CloudBlobMetadata} referencing the blobs to purge.
   * @return a CompletableFuture that will eventually contain the list of {@link CloudBlobMetadata} records referencing
   *         the blobs successfully purged or a {@link BlobStorageException} if the purge operation fails.
   */
  public CompletableFuture<List<CloudBlobMetadata>> purgeBlobsAsync(List<CloudBlobMetadata> blobMetadataList) {
    List<CloudBlobMetadata> deletedBlobs = new ArrayList<>();
    CompletableFuture<Void> resultFuture = CompletableFuture.completedFuture(null);
    List<List<CloudBlobMetadata>> partitionedLists = Utils.partitionList(blobMetadataList, purgeBatchSize);
    for (List<CloudBlobMetadata> batchOfBlobs : partitionedLists) {
      // Issue batch deletes serially one after another since issuing all in parallel can be expensive resulting in 429s.
      resultFuture = resultFuture.thenCompose(
          unused -> FutureUtils.orTimeout(storageClient.deleteBatch(batchOfBlobs), batchTimeout)
              .thenAccept(responses -> {
                for (int j = 0; j < responses.size(); j++) {
                  Response<Void> response = responses.get(j);
                  CloudBlobMetadata blobMetadata = batchOfBlobs.get(j);
                  // Note: Response.getStatusCode() throws exception on any error.
                  try {
                    response.getStatusCode();
                  } catch (BlobStorageException bse) {
                    int statusCode = bse.getStatusCode();
                    // Don't worry if blob is already gone
                    if (statusCode != HttpURLConnection.HTTP_NOT_FOUND && statusCode != HttpURLConnection.HTTP_GONE) {
                      logger.error("Deleting blob {} got status {}", blobMetadata.getId(), statusCode);
                      throw new CompletionException(bse);
                    }
                  }
                  deletedBlobs.add(blobMetadata);
                }
              }));
    }

    return Objects.requireNonNull(resultFuture).thenApply(unused -> deletedBlobs);
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
