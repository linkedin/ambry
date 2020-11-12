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

import com.azure.core.exception.UnexpectedLengthException;
import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Configuration;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.microsoft.azure.cosmosdb.RetryOptions;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class encapsulation ABS client operations.
 */
public abstract class StorageClient {
  Logger logger = LoggerFactory.getLogger(StorageClient.class);
  private final AtomicReference<BlobServiceClient> storageClientRef;
  private final AtomicReference<BlobBatchClient> blobBatchClientRef;
  private final CloudConfig cloudConfig;
  private final AzureCloudConfig azureCloudConfig;
  private final AzureBlobLayoutStrategy blobLayoutStrategy;
  protected final AzureMetrics azureMetrics;
  // Containers known to exist in the storage account
  private final Set<String> knownContainers = ConcurrentHashMap.newKeySet();

  /**
   * Constructor for {@link StorageClient}.
   * @param cloudConfig {@link CloudConfig} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   */
  public StorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, AzureMetrics azureMetrics,
      AzureBlobLayoutStrategy blobLayoutStrategy) {
    this.azureCloudConfig = azureCloudConfig;
    this.cloudConfig = cloudConfig;
    this.blobLayoutStrategy = blobLayoutStrategy;
    this.azureMetrics = azureMetrics;
    storageClientRef = new AtomicReference<>(createBlobStorageClient());
    blobBatchClientRef = new AtomicReference<>(new BlobBatchClientBuilder(storageClientRef.get()).buildClient());
  }

  /**
   * Constructor for {@link StorageClient}.
   * @param blobServiceClient {@link BlobServiceClient} object.
   * @param blobBatchClient {@link BlobBatchClient} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   */
  public StorageClient(BlobServiceClient blobServiceClient, BlobBatchClient blobBatchClient, AzureMetrics azureMetrics,
      AzureBlobLayoutStrategy blobLayoutStrategy) {
    this.storageClientRef = new AtomicReference<>(blobServiceClient);
    this.blobBatchClientRef = new AtomicReference<>(blobBatchClient);
    this.blobLayoutStrategy = blobLayoutStrategy;
    this.azureMetrics = azureMetrics;
    this.cloudConfig = null;
    this.azureCloudConfig = null;
  }

  /**
   * Visible for testing.
   * @return the underlying {@link BlobServiceClient}.
   */
  public BlobServiceClient getStorageClient() {
    return storageClientRef.get();
  }

  /**
   * Creates a new block blob, or updates the content of an existing block blob.
   * @param blobId {@link BlobId} of the blob to upload.
   * @param data The data to write to the blob.
   * @param length The exact length of the data. It is important that this value match precisely the length of the
   * data provided in the {@link InputStream}.
   * @param headers {@link BlobHttpHeaders}
   * @param metadata Metadata to associate with the blob.
   * @param tier {@link AccessTier} for the destination blob.
   * @param contentMd5 An MD5 hash of the block content.
   * @param requestConditions {@link BlobRequestConditions}
   * @param timeout An optional timeout value beyond which a {@link RuntimeException} will be raised.
   * @return The information of the uploaded block blob.
   * @throws UnexpectedLengthException when the length of data does not match the input {@code length}.
   * @throws NullPointerException if the input data is null.
   * @throws UncheckedIOException If an I/O error occurs
   */
  public Response<BlockBlobItem> uploadWithResponse(BlobId blobId, InputStream data, long length,
      BlobHttpHeaders headers, Map<String, String> metadata, AccessTier tier, byte[] contentMd5,
      BlobRequestConditions requestConditions, Duration timeout) {
    return doStorageClientOperation(
        () -> getBlockBlobClient(blobId, true).uploadWithResponse(data, length, headers, metadata, tier, contentMd5,
            requestConditions, timeout, Context.NONE));
  }

  /**
   * Creates a new block blob, or updates the content of an existing block blob.
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
   * @param timeout An optional timeout value beyond which a {@link RuntimeException} will be raised.
   * @return The information of the uploaded block blob.
   * @throws UnexpectedLengthException when the length of data does not match the input {@code length}.
   * @throws NullPointerException if the input data is null.
   * @throws UncheckedIOException If an I/O error occurs
   */
  public Response<BlockBlobItem> uploadWithResponse(String containerName, String blobName, boolean autoCreateContainer,
      InputStream data, long length, BlobHttpHeaders headers, Map<String, String> metadata, AccessTier tier,
      byte[] contentMd5, BlobRequestConditions requestConditions, Duration timeout) {
    return doStorageClientOperation(
        () -> getBlockBlobClient(containerName, blobName, autoCreateContainer).uploadWithResponse(data, length, headers,
            metadata, tier, contentMd5, requestConditions, timeout, Context.NONE));
  }

  /**
   * Downloads a range of bytes from a blob into an output stream.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @param stream A non-null {@link OutputStream} instance where the downloaded data will be written.
   * @param range {@link BlobRange}
   * @param options {@link DownloadRetryOptions}
   * @param requestConditions {@link BlobRequestConditions}
   * @param getRangeContentMd5 Whether the contentMD5 for the specified blob range should be returned.
   * @param timeout An optional timeout value beyond which a {@link RuntimeException} will be raised.
   * @return A response containing status code and HTTP headers.
   * @throws UncheckedIOException If an I/O error occurs.
   * @throws NullPointerException if {@code stream} is null
   */
  public BlobDownloadResponse downloadWithResponse(String containerName, String blobName, boolean autoCreateContainer,
      OutputStream stream, BlobRange range, DownloadRetryOptions options, BlobRequestConditions requestConditions,
      boolean getRangeContentMd5, Duration timeout) {
    // Might as well use same timeout for upload and download
    return doStorageClientOperation(
        () -> getBlockBlobClient(containerName, blobName, false).downloadWithResponse(stream, null, null,
            requestConditions, false, timeout, Context.NONE));
  }

  /**
   * Delete a file from blob storage, if it exists.
   * @param containerName name of the container containing file to delete.
   * @param fileName name of the file to delete.
   * @return true if the file was deleted, otherwise false.
   * @throws BlobStorageException for any error on ABS side.
   */
  boolean deleteFile(String containerName, String fileName) throws BlobStorageException {
    AtomicReference<Boolean> retValRef = new AtomicReference<>(false);
    doStorageClientOperation(() -> {
      BlockBlobClient blobClient = getBlockBlobClient(containerName, fileName, false);
      if (blobClient.exists()) {
        blobClient.delete();
        retValRef.set(true);
      }
      return null;
    });
    return retValRef.get();
  }

  /**
   * Perform basic connectivity test.
   */
  void testConnectivity() {
    doStorageClientOperation(() -> storageClientRef.get()
        .getBlobContainerClient("partition-0")
        .existsWithResponse(Duration.ofSeconds(5), Context.NONE));
  }

  /**
   * Returns the blob's metadata and properties.
   * @param blobId {@link BlobId}
   * @param requestConditions {@link BlobRequestConditions}
   * @param timeout An optional timeout value beyond which a {@link RuntimeException} will be raised.
   * @return The blob properties and metadata.
   */
  public BlobProperties getPropertiesWithResponse(BlobId blobId, BlobRequestConditions requestConditions,
      Duration timeout) {
    return doStorageClientOperation(
        () -> getBlockBlobClient(blobId, false).getPropertiesWithResponse(requestConditions, timeout, Context.NONE)
            .getValue());
  }

  /**
   * Changes a blob's metadata. The specified metadata in this method will replace existing metadata. If old values
   * must be preserved, they must be downloaded and included in the call to this method.
   * @param blobId {@link BlobId} object.
   * @param metadata Metadata to associate with the blob.
   * @param requestConditions {@link BlobRequestConditions}
   * @param timeout An optional timeout value beyond which a {@link RuntimeException} will be raised.
   * @param context Additional context that is passed through the Http pipeline during the service call.
   */
  public void setMetadataWithResponse(BlobId blobId, Map<String, String> metadata,
      BlobRequestConditions requestConditions, Duration timeout, Context context) {
    doStorageClientOperation(
        () -> getBlockBlobClient(blobId, false).setMetadataWithResponse(metadata, requestConditions, timeout,
            Context.NONE));
  }

  /**
   * Deletes a list of blobs.
   * @param batchOfBlobs {@link List} of {@link CloudBlobMetadata} objects.
   * @param timeout An optional timeout value beyond which a {@link RuntimeException} will be raised.
   * @return {@link List} of {@link Response}s for the blobs in the batch.
   * @throws RuntimeException If the {@code timeout} duration completes before a response is returned.
   * @throws BlobStorageException If the batch request is malformed.
   * @throws BlobBatchStorageException If {@code throwOnAnyFailure} is {@code true} and any request in the
   * {@link BlobBatch} failed.
   */
  public List<Response<Void>> deleteBatch(List<CloudBlobMetadata> batchOfBlobs, Duration timeout) {
    List<Response<Void>> responseList = new ArrayList<>();
    doStorageClientOperation(() -> {
      BlobBatch blobBatch = blobBatchClientRef.get().getBlobBatch();
      for (CloudBlobMetadata blobMetadata : batchOfBlobs) {
        AzureBlobLayoutStrategy.BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobMetadata);
        responseList.add(blobBatch.deleteBlob(blobLayout.containerName, blobLayout.blobFilePath));
      }
      blobBatchClientRef.get().submitBatchWithResponse(blobBatch, false, timeout, Context.NONE);
      return null;
    });
    return responseList;
  }

  /**
   * Get the block blob client for the supplied blobid.
   * @param blobId id of the blob for which {@code BlockBlobClient} is needed.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code BlockBlobClient} reference.
   */
  private BlockBlobClient getBlockBlobClient(BlobId blobId, boolean autoCreateContainer) {
    AzureBlobLayoutStrategy.BlobLayout blobLayout = blobLayoutStrategy.getDataBlobLayout(blobId);
    return getBlockBlobClient(blobLayout.containerName, blobLayout.blobFilePath, autoCreateContainer);
  }

  /**
   * Get the block blob client for the supplied Azure container and blob name.
   * @param containerName name of the Azure container where the blob lives.
   * @param blobName name of the blob.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code BlockBlobClient} reference.
   */
  BlockBlobClient getBlockBlobClient(String containerName, String blobName, boolean autoCreateContainer) {
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
    BlobContainerClient containerClient = storageClientRef.get().getBlobContainerClient(containerName);
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
   * Create the {@link BlobServiceClient} object.
   * @param {@link CloudConfig} object.
   * @param {@link AzureCloudConfig} object.
   * @return {@link BlobServiceClient} object.
   */
  protected BlobServiceClient createBlobStorageClient() {
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
    RequestRetryOptions noRetries = new RequestRetryOptions(RetryPolicyType.FIXED, 1, null, null, null, null);
    try {
      return buildBlobServiceClient(client, storageConfiguration, noRetries, azureCloudConfig);
    } catch (MalformedURLException | InterruptedException | ExecutionException ex) {
      logger.error("Error building ABS blob service client: {}", ex.getMessage());
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Set the references for storage and blob clients atomically. Note this method is not thread safe and must always be
   * called within a thread safe context.
   * @param blobServiceClient {@link BlobServiceClient} object.
   */
  protected void setClientReferences(BlobServiceClient blobServiceClient) {
    storageClientRef.set(blobServiceClient);
    blobBatchClientRef.set(new BlobBatchClientBuilder(storageClientRef.get()).buildClient());
  }

  /**
   * Execute the storage client operation represented by {@code operation}
   * @param operation {@link Callable} representing the operation.
   * @param <T> type of return value.
   * @return the return value of the operation.
   * @throws BlobStorageException
   */
  private <T> T doStorageClientOperation(Callable<T> operation) {
    int attempts = 0;
    T result = null;
    while (attempts <= 1) {
      attempts++;
      try {
        result = operation.call();
        break;
      } catch (BlobStorageException bsEx) {
        if (attempts == 1 && handleExceptionAndHintRetry(bsEx)) {
          logger.info("Retrying blob store operation due to exception: " + bsEx.toString());
          azureMetrics.storageClientOperationRetryCount.inc();
          continue;
        }
        throw bsEx;
      } catch (Exception ex) {
        // this should never happen.
        azureMetrics.storageClientOperationExceptionCount.inc();
        if (attempts == 1) {
          azureMetrics.storageClientFailureAfterRetryCount.inc();
        }
        throw new IllegalStateException("Unknown blob storage exception", ex);
      }
    }
    return result;
  }

  /**
   * Validate that all the required configs for ABS authentication are present.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  protected abstract void validateABSAuthConfigs(AzureCloudConfig azureCloudConfig);

  /**
   * Build {@link BlobServiceClient}.
   * @param httpClient {@link HttpClient} object.
   * @param configuration {@link Configuration} object.
   * @param retryOptions {@link RetryOptions} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @return {@link BlobServiceClient} object.
   */
  protected abstract BlobServiceClient buildBlobServiceClient(HttpClient httpClient, Configuration configuration,
      RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException;

  /**
   * Check if the exception can be handled and return a flag indicating if it can be retried.
   * Note that if this method changes state of this class, then it should do it in a thread safe way.
   * @param blobStorageException {@link BlobStorageException} object.
   * @return true if the operation can be retried. false otherwise.
   */
  protected abstract boolean handleExceptionAndHintRetry(BlobStorageException blobStorageException);
}
