/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.account.Container;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudBlobStore;
import com.github.ambry.cloud.CloudBlobStoreV2;
import com.github.ambry.cloud.CloudContainerCompactor;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.CloudUpdateValidator;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureCloudDestinationSync implements CloudDestination {

  protected AzureBlobLayoutStrategy azureBlobLayoutStrategy;
  protected AzureCloudConfig azureCloudConfig;
  protected AzureMetrics azureMetrics;
  protected BlobServiceClient azureStorageClient;
  protected CloudConfig cloudConfig;
  protected ClusterMap clusterMap;
  protected ClusterMapConfig clusterMapConfig;
  protected ConcurrentHashMap<String, BlobContainerClient> partitionToAzureStore;
  protected VcrMetrics vcrMetrics;
  public static final Logger logger = LoggerFactory.getLogger(AzureCloudDestinationSync.class);

  /**
   * Constructor
   * @param verifiableProperties
   * @param metricRegistry
   * @param clusterMap
   */
  public AzureCloudDestinationSync(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws ReflectiveOperationException {
    /*
     * These are the configs to be changed for vcr-2.0
     *
     *    azureCloudConfig.azureNameSchemeVersion = 1
     *    azureCloudConfig.azureBlobContainerStrategy = PARTITION
     *    cloudConfig.cloudMaxAttempts = 1; retries are handled by azure-sdk
     *    cloudConfig.recentBlobCacheLimit = 0; unnecessary, repl-logic avoids duplicate messages any ways
     *    cloudConfig.vcrMinTtlDays = Infinite; Just upload each blob, don't complicate it.
     */
    this.azureCloudConfig = new AzureCloudConfig(verifiableProperties);
    this.azureMetrics = new AzureMetrics(metricRegistry);
    this.cloudConfig = new CloudConfig(verifiableProperties);
    this.clusterMap = clusterMap;
    this.clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    this.partitionToAzureStore = new ConcurrentHashMap<>();
    this.vcrMetrics = new VcrMetrics(metricRegistry);
    this.azureBlobLayoutStrategy = new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName, azureCloudConfig);
    logger.info("azureCloudConfig.azureStorageClientClass = {}", azureCloudConfig.azureStorageClientClass);
    this.azureStorageClient =
        ((StorageClient) Utils.getObj(azureCloudConfig.azureStorageClientClass, cloudConfig, azureCloudConfig, azureMetrics)).getStorageSyncClient();
    testAzureStorageConnectivity();
    logger.info("Created AzureCloudDestinationSync");
  }

  /**
   * Tests connection to Azure blob storage
   */
  protected void testAzureStorageConnectivity() {
    PagedIterable<BlobContainerItem> blobContainerItemPagedIterable = azureStorageClient.listBlobContainers();
    for (BlobContainerItem blobContainerItem : blobContainerItemPagedIterable) {
      logger.info("Azure blob storage container = {}", blobContainerItem.getName());
      break;
    }
  }

  /**
   * Returns blob container in Azure for an Ambry partition
   * @param partitionId Ambry partition
   * @return blobContainerClient
   */
  protected BlobContainerClient getBlobStore(String partitionId) {
    BlobContainerClient blobContainerClient;
    try {
      blobContainerClient = azureStorageClient.getBlobContainerClient(String.valueOf(partitionId));
    } catch (BlobStorageException blobStorageException) {
      if (blobStorageException.getErrorCode().equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
        logger.error("Azure blob storage container for partition {} not found due to {}", partitionId,
            blobStorageException.getServiceMessage());
        return null;
      }
      vcrMetrics.azureStoreContainerGetError.inc();
      logger.error("Failed to get Azure blob storage container for partition {} due to {}", partitionId,
          blobStorageException.getServiceMessage());
      throw blobStorageException;
    }
    return blobContainerClient;
  }

  /**
   * Creates blob container in Azure for an Ambry partition
   * @param partitionId Ambry partition
   * @return blobContainerClient
   */
  protected BlobContainerClient createBlobStore(String partitionId) {
    BlobContainerClient blobContainerClient;
    try {
      blobContainerClient = azureStorageClient.createBlobContainer(String.valueOf(partitionId));
    } catch (BlobStorageException blobStorageException) {
      if (blobStorageException.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
        logger.info("Azure blob storage container for partition {} already exists", partitionId);
        return getBlobStore(partitionId);
      }
      vcrMetrics.azureStoreContainerGetError.inc();
      logger.error("Failed to create Azure blob storage container for partition {} due to {}", partitionId,
          blobStorageException.getServiceMessage());
      throw blobStorageException;
    }
    return blobContainerClient;
  }

  /**
   * Creates an object that stores blobs in Azure blob storage
   * @param partitionId Partition ID
   * @return {@link CloudBlobStoreV2}
   */
  protected BlobContainerClient createOrGetBlobStore(String partitionId) {
    // Get or create container
    BlobContainerClient blobContainerClient = partitionToAzureStore.get(partitionId);
    if (blobContainerClient == null) {
      blobContainerClient = createBlobStore(partitionId);
    }

    // If it is still null, throw the error and emit a metric
    if (blobContainerClient == null) {
      vcrMetrics.azureStoreContainerGetError.inc();
      String errMsg = String.format("Azure blob storage container for partition %s is null", partitionId);
      logger.error(errMsg);
      throw new RuntimeException(errMsg);
    }

    partitionToAzureStore.put(partitionId, blobContainerClient);
    return blobContainerClient;
  }

  protected Map<String, String> cloudBlobMetadatatoMap(CloudBlobMetadata cloudBlobMetadata) {
    Map<String, String> metadata = cloudBlobMetadata.toMap();
    if (!metadata.containsKey(CloudBlobMetadata.FIELD_LIFE_VERSION)) {
      /*
          Add the life-version explicitly, even if it is 0.
          Make it easier to determine what is the life-version of a blob instead of interpreting its
          absence as 0 and confusing the reader.
       */
      metadata.put(CloudBlobMetadata.FIELD_LIFE_VERSION, "0");
    }
    return metadata;
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws CloudStorageException {
    /*
     * Current impl of this fn is inefficient because the caller does a 0-4MB memcpy from src to dst stream
     * in CloudBlobStore.appendFrom - for each blob !
     * A memcpy makes sense if we mutate the buffer before uploading - encrypt, compress etc. - but we don't.
     * So it's just a remnant from legacy code probably hindering performance.
     *
     * Efficient way is to just pass the src input-stream to azure-sdk as shown below.
     * messageSievingInputStream has a list of input-streams already, where each stream is a blob.
     *
     *  If CloudBlobStore.put(messageWriteSet) can directly call AzureCloudDestinationSync.uploadBlob(messageWriteSet),
     *  then we can do this here:
     *
     *     MessageSievingInputStream messageSievingInputStream =
     *         (MessageSievingInputStream) ((MessageFormatWriteSet) messageSetToWrite).getStreamToWrite();
     *     List<InputStream> messageStreamList = messageSievingInputStream.getValidMessageStreamList();
     *     // Each input stream is a blob
     *     ListIterator<InputStream> messageStreamListIter = messageStreamList.listIterator();
     *     // Pass the stream to azure-sdk, no need of a memcpy
     *     BlobParallelUploadOptions blobParallelUploadOptions = new BlobParallelUploadOptions(messageStreamListIter.next());
     *
     */
    Timer.Context storageTimer = null;
    // setNameSchemeVersion is an ugly remnant of legacy code. We have to set it explicitly.
    cloudBlobMetadata.setNameSchemeVersion(azureCloudConfig.azureNameSchemeVersion);
    // Azure cloud container names must be 3 - 63 char
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(cloudBlobMetadata);
    String blobIdStr = blobLayout.blobFilePath;
    try {
      // Prepare to upload blob to Azure blob storage
      // There is no parallelism, but we still need to create and pass this object to SDK.
      BlobParallelUploadOptions blobParallelUploadOptions =
          new BlobParallelUploadOptions(blobInputStream);
      // To avoid overwriting, pass "*" to setIfNoneMatch(String ifNoneMatch)
      // https://learn.microsoft.com/en-us/java/api/com.azure.storage.blob.blobclient?view=azure-java-stable
      blobParallelUploadOptions.setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"));
      // This is ambry metadata, uninterpreted by azure
      blobParallelUploadOptions.setMetadata(cloudBlobMetadatatoMap(cloudBlobMetadata));
      // Without content-type, get-blob floods log with warnings
      blobParallelUploadOptions.setHeaders(new BlobHttpHeaders().setContentType("application/octet-stream"));

      BlobContainerClient blobContainerClient = createOrGetBlobStore(blobLayout.containerName);
      ////////////////////////////////// Upload blob to Azure blob storage ////////////////////////////////////////
      storageTimer = azureMetrics.blobUploadTime.time();
      Response<BlockBlobItem> blockBlobItemResponse =
          blobContainerClient.getBlobClient(blobIdStr)
              .uploadWithResponse(blobParallelUploadOptions, Duration.ofMillis(cloudConfig.cloudRequestTimeout),
                  Context.NONE);
      ////////////////////////////////// Upload blob to Azure blob storage ////////////////////////////////////////

      // Metrics and log
      // Success rate is effective, Counter is ineffective because it just monotonically increases
      azureMetrics.blobUploadSuccessRate.mark();
      // Measure ingestion rate, helps decide fleet size
      azureMetrics.backupSuccessByteRate.mark(inputLength);
      logger.debug("Successful upload of blob {} to Azure blob storage with statusCode = {}, etag = {}",
          blobIdStr, blockBlobItemResponse.getStatusCode(),
          blockBlobItemResponse.getValue().getETag());
    } catch (Exception e) {
      if (e instanceof BlobStorageException
          && ((BlobStorageException) e).getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
        // Since VCR replicates from all replicas, a blob can be uploaded by at least two threads concurrently.
        azureMetrics.blobUploadConflictCount.inc();
        logger.debug("Failed to upload blob {} to Azure blob storage because it already exists", blobLayout);
        // We should rarely be here because we get here from replication logic which checks if a blob exists or not before uploading it.
        // However, we end up here, return true to allow replication to proceed instead of halting it. Else the replication token will not advance.
        // The blob in the cloud is safe as Azure prevented us from overwriting it due to an ETag check.
        return true;
      }
      azureMetrics.blobUploadErrorCount.inc();
      String error = String.format("Failed to upload blob %s to Azure blob storage because %s", blobLayout, e.getMessage());
      logger.error(error);
      throw new CloudStorageException(error, new StoreException(error, StoreErrorCodes.Unknown_Error));
    } finally {
      if (storageTimer != null) {
        storageTimer.stop();
      }
    } // try-catch
    return true;
  }

  @Override
  public CompletableFuture<Boolean> uploadBlobAsync(BlobId blobId, long inputLength,
      CloudBlobMetadata cloudBlobMetadata, InputStream blobInputStream) {
    throw new UnsupportedOperationException("uploadBlobAsync will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException {
    throw new UnsupportedOperationException("downloadBlob will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public CompletableFuture<Void> downloadBlobAsync(BlobId blobId, OutputStream outputStream) {
    throw new UnsupportedOperationException("downloadBlobAsync will not be implemented for AzureCloudDestinationSync");
  }

  /**
   * Synchronously update blob metadata
   * @param blobLayout Blob layout
   * @param metadata Blob metadata
   * @return HTTP response and blob metadata
   */
  protected Response<Void>
  updateMetadata(AzureBlobLayoutStrategy.BlobLayout blobLayout, BlobProperties blobProperties, Map<String, String> metadata) {
    BlobClient blobClient = createOrGetBlobStore(blobLayout.containerName).getBlobClient(blobLayout.blobFilePath);
    BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch(blobProperties.getETag());
    return blobClient.setMetadataWithResponse(metadata, blobRequestConditions,
        Duration.ofMillis(cloudConfig.cloudRequestTimeout), Context.NONE);
  }

  /**
   * Returns blob properties from Azure
   * @param blobLayout BlobLayout
   * @return Blob properties
   * @throws CloudStorageException
   */
  protected BlobProperties getBlobProperties(AzureBlobLayoutStrategy.BlobLayout blobLayout)
      throws CloudStorageException {
    try {
      return createOrGetBlobStore(blobLayout.containerName).getBlobClient(blobLayout.blobFilePath).getProperties();
    } catch (Throwable t) {
      // Due to legacy CloudBlobStore, we have to handle 404 as a special case when it can actually be a general case.
      if (t instanceof BlobStorageException
          && ((BlobStorageException) t).getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
        String error = String.format("Failed to get blob properties for %s from Azure blob storage because it does not exist", blobLayout);
        logger.error(error);
        throw new CloudStorageException(error, new StoreException(error, StoreErrorCodes.ID_Not_Found), CloudBlobStore.STATUS_NOT_FOUND, false, null);
      }
      String error = String.format("Failed to get blob properties for %s from Azure blob storage because %s", blobLayout, t.getMessage());
      logger.error(error);
      throw new CloudStorageException(error, new StoreException(error, StoreErrorCodes.Unknown_Error));
    }
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator unused) throws CloudStorageException {
    Timer.Context storageTimer = azureMetrics.blobUpdateDeleteTimeLatency.time();
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    Map<String, String> newMetadata = new HashMap<>();
    newMetadata.put(CloudBlobMetadata.FIELD_DELETION_TIME, String.valueOf(deletionTime));
    newMetadata.put(CloudBlobMetadata.FIELD_LIFE_VERSION, String.valueOf(lifeVersion));
    BlobProperties blobProperties = getBlobProperties(blobLayout);
    Map<String, String> cloudMetadata = blobProperties.getMetadata();
    // lifeVersion must always be present
    short cloudlifeVersion = Short.parseShort(cloudMetadata.get(CloudBlobMetadata.FIELD_LIFE_VERSION));
    // This is the correct behavior. For ref, look at BlobStore::delete and ReplicaThread::applyUpdatesToBlobInLocalStore
    if (cloudlifeVersion == lifeVersion && cloudMetadata.containsKey(CloudBlobMetadata.FIELD_DELETION_TIME)
        && Long.parseLong(cloudMetadata.get(CloudBlobMetadata.FIELD_DELETION_TIME)) != Utils.Infinite_Time ) {
      String error = String.format("Cannot delete id %s since it is already marked as deleted in cloud", blobIdStr);
      throw new CloudStorageException(error, new StoreException(error, StoreErrorCodes.ID_Deleted));
    }
    // Don't rely on the CloudBlobStore.recentCache to do the "right" thing.
    // This is the correct behavior. For ref, look at BlobStore::delete and ReplicaThread::applyUpdatesToBlobInLocalStore
    // Repl layer handles it.
    if (cloudlifeVersion > lifeVersion) {
      String error = String.format("Cannot delete id %s since it has a higher lifeVersion than the message info: %s > %s",
          blobIdStr, cloudlifeVersion, lifeVersion);
      throw new CloudStorageException(error, new StoreException(error, StoreErrorCodes.Life_Version_Conflict));
    }
    cloudMetadata.putAll(newMetadata);
    try {
      logger.debug("Updating deleteTime of blob {} in Azure blob storage ", blobLayout.blobFilePath);
      Response<Void> response = updateMetadata(blobLayout, blobProperties, cloudMetadata);
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobUpdateDeleteTimeSucessRate.mark();
      logger.debug("Successfully updated deleteTime of blob {} in Azure blob storage with statusCode = {}, etag = {}",
          blobLayout.blobFilePath, response.getStatusCode(), response.getHeaders().get(HttpHeaderName.ETAG));
      return true;
    } catch (Throwable t) {
      azureMetrics.blobUpdateDeleteTimeErrorCount.inc();
      String error = String.format("Failed to update deleteTime of blob %s in Azure blob storage due to (%s)", blobLayout, t.getMessage());
      logger.error(error);
      throw new CloudStorageException(error, new StoreException(t, StoreErrorCodes.Unknown_Error));
    } finally {
      storageTimer.stop();
    } // try-catch
  }

  @Override
  public CompletableFuture<Boolean> deleteBlobAsync(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) {
    throw new UnsupportedOperationException("deleteBlobAsync will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public short undeleteBlob(BlobId blobId, short lifeVersion, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException {
    return 0;
  }

  @Override
  public CompletableFuture<Short> undeleteBlobAsync(BlobId blobId, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) {
    throw new UnsupportedOperationException("undeleteBlobAsync will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public short updateBlobExpiration(BlobId blobId, long expirationTime, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException {
    return 0;
  }

  @Override
  public CompletableFuture<Short> updateBlobExpirationAsync(BlobId blobId, long expirationTime,
      CloudUpdateValidator cloudUpdateValidator) {
    throw new UnsupportedOperationException("updateBlobExpirationAsync will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException {
    return null;
  }

  @Override
  public CompletableFuture<Map<String, CloudBlobMetadata>> getBlobMetadataAsync(List<BlobId> blobIds) {
    throw new UnsupportedOperationException("getBlobMetadataAsync will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public FindResult findEntriesSince(String partitionPath, FindToken findToken, long maxTotalSizeOfEntries)
      throws CloudStorageException {
    throw new UnsupportedOperationException("findEntriesSince will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public int compactPartition(String partitionPath) throws CloudStorageException {
    return 0;
  }

  @Override
  public void persistTokens(String partitionPath, String tokenFileName, InputStream inputStream)
      throws CloudStorageException {

  }

  @Override
  public boolean retrieveTokens(String partitionPath, String tokenFileName, OutputStream outputStream)
      throws CloudStorageException {
    return false;
  }

  @Override
  public void stopCompaction() {

  }

  @Override
  public void deprecateContainers(Collection<Container> deprecatedContainers) throws CloudStorageException {

  }

  @Override
  public CloudContainerCompactor getContainerCompactor() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
