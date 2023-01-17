/**
 * Copyright 2020  LinkedIn Corp. All rights reserved.
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
import com.azure.core.util.FluxUtil;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.FutureUtils;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class to encapsulate ABS client operations. Please note that all its operations are asynchronous.
 */
public abstract class StorageClient implements AzureStorageClient {
  Logger logger = LoggerFactory.getLogger(StorageClient.class);
  private final AtomicReference<BlobServiceAsyncClient> storageAsyncClientRef;
  private final AtomicReference<BlobBatchAsyncClient> blobBatchAsyncClientRef;
  private final CloudConfig cloudConfig;
  protected final AzureCloudConfig azureCloudConfig;
  private final AzureCloudConfig.StorageAccountInfo storageAccountInfo;
  private final AzureBlobLayoutStrategy blobLayoutStrategy;
  protected final AzureMetrics azureMetrics;
  // Containers known to exist in the storage account
  private final Set<String> knownContainers = ConcurrentHashMap.newKeySet();
  // All storage client are retried once if the retry condition is met.
  private final int retries = 1;
  private final Predicate<Throwable> retryPredicate =
      throwable -> throwable instanceof BlobStorageException && handleExceptionAndHintRetry(
          (BlobStorageException) throwable);

  /**
   * Constructor for {@link StorageClient}.
   * @param cloudConfig {@link CloudConfig} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   */
  public StorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, AzureMetrics azureMetrics,
      AzureBlobLayoutStrategy blobLayoutStrategy, AzureCloudConfig.StorageAccountInfo storageAccountInfo) {
    this.storageAccountInfo = storageAccountInfo;
    this.azureCloudConfig = azureCloudConfig;
    this.cloudConfig = cloudConfig;
    this.blobLayoutStrategy = blobLayoutStrategy;
    this.azureMetrics = azureMetrics;
    storageAsyncClientRef = new AtomicReference<>(createBlobStorageAsyncClient());
    blobBatchAsyncClientRef =
        new AtomicReference<>(new BlobBatchClientBuilder(storageAsyncClientRef.get()).buildAsyncClient());
  }

  /**
   * Constructor for {@link StorageClient}.
   * @param storageAsyncClient {@link BlobServiceAsyncClient} object.
   * @param blobBatchAsyncClient {@link BlobBatchAsyncClient} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  public StorageClient(BlobServiceAsyncClient storageAsyncClient, BlobBatchAsyncClient blobBatchAsyncClient,
      AzureMetrics azureMetrics, AzureBlobLayoutStrategy blobLayoutStrategy, AzureCloudConfig azureCloudConfig,
      AzureCloudConfig.StorageAccountInfo storageAccountInfo) {
    this.storageAccountInfo = storageAccountInfo;
    this.blobLayoutStrategy = blobLayoutStrategy;
    this.azureMetrics = azureMetrics;
    this.cloudConfig = null;
    this.azureCloudConfig = azureCloudConfig;
    this.storageAsyncClientRef = new AtomicReference<>(storageAsyncClient);
    this.blobBatchAsyncClientRef = new AtomicReference<>(blobBatchAsyncClient);
  }

  AzureCloudConfig.StorageAccountInfo storageAccountInfo() {
    return storageAccountInfo;
  }

  /**
   * Visible for testing.
   * @return the underlying {@link BlobServiceAsyncClient}.
   */
  public BlobServiceAsyncClient getStorageClient() {
    return storageAsyncClientRef.get();
  }

  /**
   * Creates a new block blob, or updates the content of an existing block blob asynchronously.
   * @param blobId {@link BlobId} of the blob to upload.
   * @param data The data to write to the blob.
   * @param length The exact length of the data. It is important that this value match precisely the length of the
   * data provided in the {@link InputStream}.
   * @param headers {@link BlobHttpHeaders}
   * @param metadata Metadata to associate with the blob.
   * @param tier {@link AccessTier} for the destination blob.
   * @param contentMd5 An MD5 hash of the block content.
   * @param requestConditions {@link BlobRequestConditions}
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete when block blob is uploaded
   *         or will contain an exception if an error occurred.
   */
  public CompletableFuture<Void> uploadWithResponse(BlobId blobId, InputStream data, long length,
      BlobHttpHeaders headers, Map<String, String> metadata, AccessTier tier, byte[] contentMd5,
      BlobRequestConditions requestConditions) {
    return FutureUtils.retry(() -> {
      BlockBlobAsyncClient blobAsyncClient = getBlockBlobAsyncClient(blobId, true);
      return blobAsyncClient.uploadWithResponse(FluxUtil.toFluxByteBuffer(data), length, headers, metadata, tier,
          contentMd5, requestConditions).toFuture().thenApply(blockBlobItemResponse -> null);
    }, retries, retryPredicate, this::onRetriableError, this::onError);
  }

  /**
   * Creates a new block blob, or updates the content of an existing block blob asynchronously.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @param data The data to write to the blob.
   * @param length The exact length of the data. It is important that this value match precisely the length of the
   * data provided in the {@link InputStream}.
   * @param headers {@link BlobHttpHeaders}
   * @param metadata Metadata to associate with the blob.
   * @param tier {@link AccessTier} for the destination blob.
   * @param contentMd5 An MD5 hash of the block content.
   * @param requestConditions {@link BlobRequestConditions}
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete when block blob is uploaded
   *         or an exception if an error occurred.
   */
  public CompletableFuture<Void> uploadWithResponse(String containerName, String blobName, boolean autoCreateContainer,
      InputStream data, long length, BlobHttpHeaders headers, Map<String, String> metadata, AccessTier tier,
      byte[] contentMd5, BlobRequestConditions requestConditions) {
    return FutureUtils.retry(() -> {
      BlockBlobAsyncClient blobAsyncClient = getBlockBlobAsyncClient(containerName, blobName, autoCreateContainer);
      return blobAsyncClient.uploadWithResponse(FluxUtil.toFluxByteBuffer(data), length, headers, metadata, tier,
          contentMd5, requestConditions).toFuture().thenApply(blockBlobItemResponse -> null);
    }, retries, retryPredicate, this::onRetriableError, this::onError);
  }

  /**
   * Downloads a range of bytes from a blob asynchronously into an output stream.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param blobId Ambry {@link BlobId} associated with the {@code fileName}. Null value indicates blobName refers
   *               to a metadata blob owned by Ambry.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @param stream A non-null {@link OutputStream} instance where the downloaded data will be written.
   * @param range {@link BlobRange}
   * @param options {@link DownloadRetryOptions}
   * @param requestConditions {@link BlobRequestConditions}
   * @param getRangeContentMd5 Whether the contentMD5 for the specified blob range should be returned.
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete when blob is downloaded
   *         or an exception if an error occurred.
   */
  public CompletableFuture<Void> downloadWithResponse(String containerName, String blobName, BlobId blobId,
      boolean autoCreateContainer, OutputStream stream, BlobRange range, DownloadRetryOptions options,
      BlobRequestConditions requestConditions, boolean getRangeContentMd5) {
    // Might as well use same timeout for upload and download
    return FutureUtils.retry(() -> {
      CompletableFuture<Void> completableFuture = new CompletableFuture<>();
      BlockBlobAsyncClient blobAsyncClient = getBlockBlobAsyncClient(containerName, blobName, autoCreateContainer);
      blobAsyncClient.downloadWithResponse(range, options, requestConditions, getRangeContentMd5)
          .subscribe(response -> response.getValue().subscribe(byteBuffer -> {
                try {
                  stream.write(byteBuffer.array());
                } catch (IOException e) {
                  completableFuture.completeExceptionally(e);
                }
              }, completableFuture::completeExceptionally, () -> completableFuture.complete(null)),
              completableFuture::completeExceptionally);
      return completableFuture;
    }, retries, retryPredicate, this::onRetriableError, this::onError);
  }

  /**
   * Delete a file from blob storage asynchronously, if it exists.
   * @param containerName name of the container containing file to delete.
   * @param fileName name of the file to delete.
   * @return a {@link CompletableFuture} of type {@link Boolean} that will eventually complete successfully when the file
   *         is deleted or will complete exceptionally if an error occurs.
   */
 public CompletableFuture<Boolean> deleteFile(String containerName, String fileName) throws BlobStorageException {
    return FutureUtils.retry(() -> {
      CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
      BlockBlobAsyncClient blobAsyncClient = getBlockBlobAsyncClient(containerName, fileName, false);
      return blobAsyncClient.exists().toFuture().thenCompose((response) -> {
        if (response) {
          blobAsyncClient.delete().toFuture().thenAccept((result) -> completableFuture.complete(true));
        } else {
          completableFuture.complete(false);
        }
        return completableFuture;
      });
    }, retries, retryPredicate, this::onRetriableError, this::onError);
  }

  /**
   * Perform basic connectivity test.
   */
 public void testConnectivity() {
    CompletableFuture<Response<Boolean>> completableFuture = FutureUtils.retry(
        () -> storageAsyncClientRef.get().getBlobContainerAsyncClient("partition-0").existsWithResponse().toFuture(),
        retries, retryPredicate, this::onRetriableError, this::onError);
    completableFuture.join();
  }

  /**
   * Returns the blob's metadata and properties.
   * @param blobId {@link BlobId}
   * @param requestConditions {@link BlobRequestConditions}
   * @return a {@link CompletableFuture} that will eventually contain blob properties and metadata or an exception
   *         if an error occurred.
   */
  public CompletableFuture<BlobProperties> getPropertiesWithResponse(BlobId blobId,
      BlobRequestConditions requestConditions) {
    return FutureUtils.retry(() -> getBlockBlobAsyncClient(blobId, false).getPropertiesWithResponse(requestConditions)
        .toFuture()
        .thenApply(Response::getValue), retries, retryPredicate, this::onRetriableError, this::onError);
  }

  /**
   * Changes a blob's metadata. The specified metadata in this method will replace existing metadata. If old values
   * must be preserved, they must be downloaded and included in the call to this method.
   * @param blobId {@link BlobId} object.
   * @param metadata Metadata to associate with the blob.
   * @param requestConditions {@link BlobRequestConditions}
   * @param context Additional context that is passed through the Http pipeline during the service call.
   * @return a {@link CompletableFuture} of type {@link Void} that will eventually complete successfully when blob
   *         metadata is changed or completes exceptionally if an error occurred.
   */
  public CompletableFuture<Void> setMetadataWithResponse(BlobId blobId, Map<String, String> metadata,
      BlobRequestConditions requestConditions, Context context) {
    return FutureUtils.retry(
        () -> getBlockBlobAsyncClient(blobId, false).setMetadataWithResponse(metadata, requestConditions)
            .toFuture()
            .thenApply(response -> null), retries, retryPredicate, this::onRetriableError, this::onError);
  }

  /**
   * Deletes a list of blobs asynchronously.
   * @param batchOfBlobs {@link List} of {@link CloudBlobMetadata} objects.
   * @return a {@link CompletableFuture} that will eventually contain {@link List} of {@link Response}s for the blobs
   *         in the batch or an exception if an error occurred.
   */
  public CompletableFuture<List<Response<Void>>> deleteBatch(List<CloudBlobMetadata> batchOfBlobs) {
    return FutureUtils.retry(() -> {
      List<Response<Void>> responseList = new ArrayList<>();
      BlobBatch blobBatch = blobBatchAsyncClientRef.get().getBlobBatch();
      for (CloudBlobMetadata blobMetadata : batchOfBlobs) {
        AzureBlobLayoutStrategy.BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobMetadata);
        responseList.add(blobBatch.deleteBlob(blobLayout.containerName, blobLayout.blobFilePath));
      }
      return blobBatchAsyncClientRef.get()
          .submitBatchWithResponse(blobBatch, false)
          .toFuture()
          .thenApply(response -> responseList);
    }, retries, retryPredicate, this::onRetriableError, this::onError);
  }

  /**
   * Get the block blob async client for the supplied blobid.
   * @param blobId id of the blob for which {@code BlockBlobClient} is needed.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code BlockBlobClient} reference.
   */
  private BlockBlobAsyncClient getBlockBlobAsyncClient(BlobId blobId, boolean autoCreateContainer) {
    AzureBlobLayoutStrategy.BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobId);
    return getBlockBlobAsyncClient(blobLayout.containerName, blobLayout.blobFilePath, autoCreateContainer);
  }

  /**
   * Get the block blob async client for the supplied Azure container and blob name.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code BlockBlobClient} reference.
   */
  BlockBlobAsyncClient getBlockBlobAsyncClient(String containerName, String blobName, boolean autoCreateContainer) {
    BlobContainerAsyncClient containerAsyncClient = getAsyncContainerClient(containerName, autoCreateContainer);
    return containerAsyncClient.getBlobAsyncClient(blobName).getBlockBlobAsyncClient();
  }

  /**
   * Get a reference to an Azure container async client, creating it if necessary.
   * @param containerName the container name.
   * @param autoCreate flag indicating whether to create the container if it does not exist.
   * @return the created {@link BlobContainerClient}.
   */
  private BlobContainerAsyncClient getAsyncContainerClient(String containerName, boolean autoCreate) {
    BlobContainerAsyncClient containerAsyncClient =
        storageAsyncClientRef.get().getBlobContainerAsyncClient(containerName);
    if (autoCreate) {
      if (!knownContainers.contains(containerName)) {
        try {
          if (!containerAsyncClient.exists().toFuture().join()) {
            //TODO: Can make checking of container and its creation to async as well.
            containerAsyncClient.create().toFuture().join();
            logger.info("Created container {}", containerName);
          }
        } catch (CompletionException ex) {
          Exception e = Utils.extractFutureExceptionCause(ex);
          if (e instanceof BlobStorageException
              && ((BlobStorageException) e).getErrorCode() != BlobErrorCode.CONTAINER_ALREADY_EXISTS) {
            logger.error("Failed to create container {}", containerName);
            throw ex;
          }
        }
        knownContainers.add(containerName);
      }
    }
    return containerAsyncClient;
  }

  /**
   * Create the {@link BlobServiceAsyncClient} object.
   * @return {@link BlobServiceAsyncClient} object.
   */
  protected BlobServiceAsyncClient createBlobStorageAsyncClient() {
    validateABSAuthConfigs(azureCloudConfig);
    Configuration storageConfiguration = new Configuration();
    // Check for network proxy
    ProxyOptions proxyOptions = (cloudConfig.vcrProxyHost == null) ? null : new ProxyOptions(ProxyOptions.Type.HTTP,
        new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort));
    if (proxyOptions != null) {
      logger.info("Using proxy: {}:{}", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
    }
    HttpClient client = new NettyAsyncHttpClientBuilder().proxy(proxyOptions).build();

    // Note: retry decisions are made at CloudBlobStore level.  Configure storageClient with no retries.
    RequestRetryOptions noRetries = new RequestRetryOptions(RetryPolicyType.FIXED, 1, (Integer) null, null, null, null);
    try {
      return buildBlobServiceAsyncClient(client, storageConfiguration, noRetries, azureCloudConfig);
    } catch (MalformedURLException | InterruptedException | ExecutionException ex) {
      logger.error("Error building ABS blob service client: {}", ex.getMessage());
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Set the references for storage and blob async clients atomically. Note this method is not thread safe and must always be
   * called within a thread safe context.
   * @param blobServiceAsyncClient {@link BlobServiceClient} object.
   */
  protected void setAsyncClientReferences(BlobServiceAsyncClient blobServiceAsyncClient) {
    storageAsyncClientRef.set(blobServiceAsyncClient);
    blobBatchAsyncClientRef.set(new BlobBatchClientBuilder(storageAsyncClientRef.get()).buildAsyncClient());
  }

  /**
   * Validate that all the required configs for ABS authentication are present.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  protected abstract void validateABSAuthConfigs(AzureCloudConfig azureCloudConfig);

  /**
   * Build {@link BlobServiceAsyncClient}.
   * @param httpClient {@link HttpClient} object.
   * @param configuration {@link Configuration} object.
   * @param retryOptions {@link RequestRetryOptions} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @return {@link BlobServiceAsyncClient} object.
   */
  protected abstract BlobServiceAsyncClient buildBlobServiceAsyncClient(HttpClient httpClient,
      Configuration configuration, RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException;

  /**
   * Check if the exception can be handled and return a flag indicating if it can be retried.
   * Note that if this method changes state of this class, then it should do it in a thread safe way.
   * @param blobStorageException {@link BlobStorageException} object.
   * @return true if the operation can be retried. false otherwise.
   */
  protected abstract boolean handleExceptionAndHintRetry(BlobStorageException blobStorageException);

  /**
   * Handle any errors. For now, we update the error metrics.
   * @param t associated with this error.
   */
  private void onError(Throwable t) {
    if (t instanceof BlobStorageException
        && ((BlobStorageException) t).getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
      // Common case first
      logger.debug("Blob store operation failed due to exception: " + t);
    } else {
      logger.error("Blob store operation failed due to exception: " + t);
      azureMetrics.storageClientOperationExceptionCount.inc();
      // All retries must have been completed internally by now.
      azureMetrics.storageClientFailureAfterRetryCount.inc();
    }
  }

  /**
   * Handle any retriable errors. For now, we update the error metrics.
   * @param throwable associated with this error.
   */
  private void onRetriableError(Throwable throwable) {
    logger.info("Retrying blob store operation due to exception: " + throwable.toString());
    azureMetrics.storageClientOperationRetryCount.inc();
  }
}
