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
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableErrorCode;
import com.azure.data.tables.models.TableItem;
import com.azure.data.tables.models.TableServiceException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountUtils;
import com.github.ambry.account.Container;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudContainerCompactor;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudStorageCompactor;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.CloudUpdateValidator;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.AmbryCache;
import com.github.ambry.commons.AmbryCacheEntry;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AzureCloudDestinationSync implements CloudDestination {

  private final MetricRegistry metrics;
  protected AzureBlobLayoutStrategy azureBlobLayoutStrategy;
  protected AzureCloudConfig azureCloudConfig;
  protected AzureMetrics azureMetrics;
  protected BlobServiceClient azureStorageClient;
  protected TableServiceClient azureTableServiceClient;
  protected CloudConfig cloudConfig;
  protected ClusterMap clusterMap;
  protected ClusterMapConfig clusterMapConfig;
  protected ConcurrentHashMap<String, BlobContainerClient> partitionToAzureStore;
  protected ConcurrentHashMap<String, TableClient> tableClientMap;
  protected final AtomicBoolean shutdownCompaction = new AtomicBoolean(false);
  protected AccountService accountService;
  protected StoreConfig storeConfig;
  public static final Logger logger = LoggerFactory.getLogger(AzureCloudDestinationSync.class);
  ThreadLocal<AmbryCache> threadLocalMdCache;
  protected class BlobMetadata implements AmbryCacheEntry {

    private final BlobProperties properties;
    public BlobMetadata(BlobProperties properties) {
      this.properties = properties;
    }

    public BlobProperties getProperties() {
      return properties;
    }
  }
  /**
   * Constructor for AzureCloudDestinationSync
   * @param verifiableProperties Configuration properties
   * @param metricRegistry Registry of metrics emitted
   * @param clusterMap Cluster map object containing partition info etc
   * @param accountService Account service client object
   */
  public AzureCloudDestinationSync(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      ClusterMap clusterMap, AccountService accountService)
      throws ReflectiveOperationException {
    /*
     * These are the configs to be changed for vcr-2.0
     *
     *    azureCloudConfig.azureBlobContainerStrategy = PARTITION
     *    azureCloudConfig.azureNameSchemeVersion = 1
     *    azureCloudConfig.azureStorageAccountInfo = null; legacy remnant
     *    cloudConfig.ambryBackupVersion = 2.0
     *    cloudConfig.cloudMaxAttempts = 1; retries are handled by azure-sdk
     *    cloudConfig.cloudRecentBlobCacheLimit = 0; unnecessary, as repl-logic avoids duplicate messages any ways
     *    cloudConfig.vcrMinTtlDays = Infinite; Just upload each blob, don't complicate it.
     *
     *    Client configs
     *    ==============
     *    azureStorageClientClass = com.github.ambry.cloud.azure.ConnectionStringBasedStorageClient
     *    azureStorageConnectionString = <must be a valid string if using ConnectionStringBasedStorageClient>
     *
     *    OR,
     *
     *    azureStorageClientClass = com.github.ambry.cloud.azure.ClientSecretCredentialStorageClient
     *    azureIdentityTenantId = <must be a valid string if using ClientSecretCredentialStorageClient>
     *    azureIdentityClientId = <must be a valid string if using ClientSecretCredentialStorageClient>
     *    azureIdentitySecret   = <must be a valid string if using ClientSecretCredentialStorageClient>
     *    azureIdentityProxyHost = null
     *    azureIdentityProxyPort = null
     *
     *    azureStorageEndpoint = https://<account name>.blob.core.windows.net
     *    vcrProxyHost = null
     *    vcrProxyPort = null
     *
     *    Compaction Configs
     *    ==================
     *
     *    azureBlobStorageMaxResultsPerPage = 5000; default, max, can change for testing
     * 	  cloudCompactionGracePeriodDays = 7; default, can change for testing
     *    cloudBlobCompactionEnabled = true; default
     *    cloudBlobCompactionIntervalHours ; gap between consecutive compaction cycles
     *    cloudBlobCompactionShutdownTimeoutSecs = 10; arbitrary timeout
     *    cloudBlobCompactionStartupDelaySecs = 600; Wait to populate partition set
     *    cloudCompactionDryRunEnabled = false; default, used for testing
     *    cloudCompactionNumThreads = 5; default, can change for testing
     *    cloudContainerCompactionEnabled = false; Azure Containers map to Ambry partitions, we do not delete partitions
     *    storeContainerDeletionRetentionDays = 14; default, can change for testing
     */
    this.accountService = accountService;
    this.azureCloudConfig = new AzureCloudConfig(verifiableProperties);
    this.azureMetrics = new AzureMetrics(metricRegistry);
    this.cloudConfig = new CloudConfig(verifiableProperties);
    this.storeConfig = new StoreConfig(verifiableProperties);
    this.clusterMap = clusterMap;
    this.clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    this.partitionToAzureStore = new ConcurrentHashMap<>();
    this.tableClientMap = new ConcurrentHashMap<>();
    this.azureBlobLayoutStrategy = new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName, azureCloudConfig);
    logger.info("azureCloudConfig.azureStorageClientClass = {}", azureCloudConfig.azureStorageClientClass);
    StorageClient storageClient =
        Utils.getObj(azureCloudConfig.azureStorageClientClass, cloudConfig, azureCloudConfig, azureMetrics);
    threadLocalMdCache = new ThreadLocal<>();
    metrics = metricRegistry;
    this.azureStorageClient = storageClient.getStorageSyncClient();
    this.azureTableServiceClient = storageClient.getTableServiceClient();
    testAzureStorageConnectivity();
    logger.info("Created AzureCloudDestinationSync");
  }

  AmbryCache getThreadLocalMdCache() {
    if (threadLocalMdCache.get() == null) {
      // The thread that creates this client object is not the thread that uploads blobs to Azure,
      // So we need this fn to create thread-specific caches
      String cacheName = "thread-local-mdcache-" + Thread.currentThread().getName();
      threadLocalMdCache.set(new AmbryCache(cacheName, true, 2<<10, metrics));
      logger.info("Created AmbryCache {}", threadLocalMdCache.get().toString());
    }
    return threadLocalMdCache.get();
  }

  /**
   * Tests connection to Azure blob storage
   */
  protected void testAzureStorageConnectivity() {
    logger.info("Testing Azure Storage connectivity");
    // Test connection to Blob Service
    PagedIterable<BlobContainerItem> blobContainerItemPagedIterable = azureStorageClient.listBlobContainers();
    for (BlobContainerItem blobContainerItem : blobContainerItemPagedIterable) {
      logger.info("Azure blob storage container = {}", blobContainerItem.getName());
      break;
    }
    logger.info("Successful connection to Azure Storage");
    // Test connection to Table Service
    PagedIterable<TableItem>  tableItemPagedIterable = azureTableServiceClient.listTables();
    for (TableItem tableItem : tableItemPagedIterable) {
      // List one item to populate lazy iterable and confirm
      logger.info("Azure Table = {}", tableItem.getName());
      break;
    }
    logger.info("Successful connection to Azure Table Service");
  }


  /**
   * Gets a table client to Azure Data Table Service
   * @param tableName Table Name
   * @return {@link TableClient}
   */
  public TableClient getTableClient(String tableName) {
    try {
      tableClientMap.computeIfAbsent(tableName,
          key -> azureTableServiceClient.createTableIfNotExists(tableName));
      return tableClientMap.computeIfAbsent(tableName,
          key -> azureTableServiceClient.getTableClient(tableName));
    } catch (Throwable e) {
      azureMetrics.tableCreateErrorCount.inc();
      logger.error("Failed to create or get table {} in Azure Table Service due to {}", tableName, e);
      throw e;
    }
  }

  /**
   * Inserts a row in Azure Table
   * An Azure Table Entity is a row with paritionKey and rowKey
   * @param tableName Name of the table in Azure Table Service
   * @param tableEntity Table row to insert
   */
  public void createTableEntity(String tableName, TableEntity tableEntity) {
    Throwable throwable = null;
    try {
      getTableClient(tableName).createEntity(tableEntity);
    } catch (TableServiceException tse) {
      throwable = (tse.getValue().getErrorCode() != TableErrorCode.ENTITY_ALREADY_EXISTS) ? tse : null;
    } catch (Throwable e) {
      throwable = e;
    } finally {
      if (throwable != null) {
        azureMetrics.tableEntityCreateErrorCount.inc();
        logger.error("Failed to insert table entity {}/{} in {} due to {}",
            tableEntity.getPartitionKey(), tableEntity.getRowKey(), tableName, throwable);
        throwable.printStackTrace();
      }
    }
  }

  /**
   * Upserts a row in Azure Table An Azure Table Entity is a row with paritionKey and rowKey
   *
   * @param tableName   Name of the table in Azure Table Service
   * @param tableEntity Table row to insert
   * @return
   */
  public boolean upsertTableEntity(String tableName, TableEntity tableEntity) {
    Throwable throwable = null;
    try {
      getTableClient(tableName).upsertEntity(tableEntity);
      return true;
    } catch (Throwable e) {
      throwable = e;
      return false;
    } finally {
      // Executed before return statement
      if (throwable != null) {
        azureMetrics.tableEntityCreateErrorCount.inc();
        logger.error("Failed to upsert table entity {}/{} in {} due to {}",
            tableEntity.getPartitionKey(), tableEntity.getRowKey(), tableName, throwable);
        throwable.printStackTrace();
      }
    }
  }

  /**
   * Fetches a row in Azure Table
   * An Azure Table Entity is a row with partitionKey and rowKey
   * @param tableName Name of the table in Azure Table Service
   */
  public TableEntity getTableEntity(String tableName, String partitionKey, String rowKey) {
    Throwable throwable = null;
    try {
      return getTableClient(tableName).getEntity(partitionKey, rowKey);
    } catch (Throwable e) {
      throwable = e;
    } finally {
      if (throwable != null) {
        logger.error("Failed to fetch table entity {}/{} from {} due to {}",
            partitionKey, rowKey, tableName, throwable);
        throwable.printStackTrace();
      }
    }
    return null;
  }

  /**
   * Returns blob container in Azure for an Ambry partition
   * @param partitionId Ambry partition
   * @return blobContainerClient
   */
  public BlobContainerClient getBlobStore(String partitionId) {
    BlobContainerClient blobContainerClient = azureStorageClient.getBlobContainerClient(String.valueOf(partitionId));
    try {
      if (!blobContainerClient.exists()) {
        logger.error("Azure blob storage container for partition {} does not exist", partitionId);
        return null;
      }
    } catch (BlobStorageException blobStorageException) {
      azureMetrics.blobContainerErrorCount.inc();
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
      azureMetrics.blobContainerErrorCount.inc();
      logger.error("Failed to create Azure blob storage container for partition {} due to {}", partitionId,
          blobStorageException.getServiceMessage());
      throw blobStorageException;
    }
    return blobContainerClient;
  }

  /**
   * Gets a descriptor to Azure blob storage container
   * @param partitionId Partition ID
   * @return {@link BlobContainerClient}
   */
  public BlobContainerClient getBlobStoreCached(String partitionId) {
    // Get container ref from local cache
    BlobContainerClient blobContainerClient = partitionToAzureStore.get(partitionId);

    // If cache miss, then get container ref from cloud
    if (blobContainerClient == null) {
      blobContainerClient = getBlobStore(partitionId);
    }

    // If it is still null, throw the error and emit a metric
    if (blobContainerClient == null) {
      azureMetrics.blobContainerErrorCount.inc();
      String errMsg = String.format("Azure blob storage container for partition %s is null", partitionId);
      logger.error(errMsg);
      throw new RuntimeException(errMsg);
    }

    partitionToAzureStore.put(partitionId, blobContainerClient);
    return blobContainerClient;
  }

  /**
   * Creates an object that stores blobs in Azure blob storage
   * @param partitionId Partition ID
   * @return {@link BlobContainerClient}
   */
  public BlobContainerClient createOrGetBlobStore(String partitionId) {
    // Get container ref from local cache
    BlobContainerClient blobContainerClient = partitionToAzureStore.get(partitionId);

    // If cache miss, then get container ref from cloud
    if (blobContainerClient == null) {
      blobContainerClient = getBlobStore(partitionId);
    }

    // If container absent, then create container and get ref
    if (blobContainerClient == null) {
      blobContainerClient = createBlobStore(partitionId);
    }

    // If it is still null, throw the error and emit a metric
    if (blobContainerClient == null) {
      azureMetrics.blobContainerErrorCount.inc();
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

  /**
   * Uploads a stream of blobs to Azure cloud
   * @param messageSetToWrite Messages replicated from servr
   * @return Unused boolean
   * @throws CloudStorageException
   */
  public boolean uploadBlobs(MessageFormatWriteSet messageSetToWrite) throws CloudStorageException {
    // Each input stream is a blob
    Timer.Context storageTimer = azureMetrics.blobBatchUploadLatency.time();
    MessageSievingInputStream stream = (MessageSievingInputStream) messageSetToWrite.getStreamToWrite();
    ListIterator<InputStream> messageStreamListIter = stream.getValidMessageStreamList().listIterator();
    boolean unused_ret = true;
    for (MessageInfo messageInfo: messageSetToWrite.getMessageSetInfo()) {
      BlobId blobId = (BlobId) messageInfo.getStoreKey();
      CloudBlobMetadata cloudBlobMetadata =
          new CloudBlobMetadata(blobId, messageInfo.getOperationTimeMs(), messageInfo.getExpirationTimeInMs(),
              messageInfo.getSize(), CloudBlobMetadata.EncryptionOrigin.NONE, messageInfo.getLifeVersion());
      cloudBlobMetadata.setReplicaLocation(stream.getReplicaLocation());
      unused_ret &= uploadBlob(blobId, messageInfo.getSize(), cloudBlobMetadata, messageStreamListIter.next());
    }
    storageTimer.stop();
    // Unused return value
    return unused_ret;
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
    // setNameSchemeVersion is a remnant of legacy code. We have to set it explicitly.
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
      storageTimer = azureMetrics.blobUploadLatency.time();
      Response<BlockBlobItem> blockBlobItemResponse =
          blobContainerClient.getBlobClient(blobIdStr)
              .uploadWithResponse(blobParallelUploadOptions, Duration.ofMillis(cloudConfig.cloudRequestTimeout),
                  Context.NONE);
      ////////////////////////////////// Upload blob to Azure blob storage ////////////////////////////////////////

      // Metrics and log
      // Success rate is effective, Counter is ineffective because it just monotonically increases
      azureMetrics.blobUploadSuccessRate.mark();
      // Measure ingestion rate, helps decide fleet size
      azureMetrics.blobUploadByteRate.mark(inputLength);
      logger.trace("Successful upload of blob {} to Azure blob storage with statusCode = {}, etag = {}",
          blobIdStr, blockBlobItemResponse.getStatusCode(),
          blockBlobItemResponse.getValue().getETag());
    } catch (Exception e) {
      if (e instanceof BlobStorageException
          && ((BlobStorageException) e).getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
        // Since VCR replicates from all replicas, a blob can be uploaded by at least two threads concurrently.
        azureMetrics.blobUploadConflictCount.inc();
        logger.error("Failed to upload blob {} from {} to Azure blob storage because it already exists", blobLayout,
            cloudBlobMetadata.getReplicaLocation());
        // We should rarely be here because we get here from replication logic which checks if a blob exists or not before uploading it.
        // However, if we end up here, return true to allow replication to proceed instead of halting it. Else the replication token will not advance.
        // The blob in the cloud is safe as Azure prevented us from overwriting it due to an ETag check.
        return true;
      }
      azureMetrics.blobUploadErrorCount.inc();
      String error = String.format("Failed to upload blob %s to Azure blob storage because %s", blobLayout, e.getMessage());
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, e, null);
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
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    BlobContainerClient blobContainerClient = createOrGetBlobStore(blobLayout.containerName);
    Timer.Context storageTimer = azureMetrics.blobDownloadLatency.time();
    try {
      blobContainerClient.getBlobClient(blobIdStr).downloadStream(outputStream);
      azureMetrics.blobDownloadSuccessRate.mark();
    } catch (Throwable e) {
      azureMetrics.blobDownloadErrorCount.inc();
      String error = String.format("Failed to download blob %s from Azure blob storage due to %s",
          blobLayout, e.getMessage());
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, e, null);
    } finally {
      if (storageTimer != null) {
        storageTimer.stop();
      }
    } // try-catch
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
  protected Response<Void> updateBlobMetadata(AzureBlobLayoutStrategy.BlobLayout blobLayout,
      BlobProperties blobProperties, Map<String, String> metadata) {
    BlobClient blobClient = createOrGetBlobStore(blobLayout.containerName).getBlobClient(blobLayout.blobFilePath);
    /**
     * When replicating, we might receive a TTL-UPDATE for a blob from replica-A and a DELETE for the same blob from replica-B.
     * The thread-local cache functions as a write-through cache. We process the TTL-UPDATE from replica-A through this cache,
     * followed by the DELETE from replica-B. The ETag changes in the cloud after each update, and we avoid additional read
     * requests by not reading it from the cloud between updates. This approach means we are not performing read-modify-write
     * operations on the blob metadata in the cloud, so we cannot rely on ETag matching to constrain updates.
     * Additionally, ETag matching is primarily useful for concurrent updates, which is not relevant here because all
     * replicas for a partition are scanned serially by the same thread. This is a departure from the previous design,
     * where all replicas for a partition were scanned concurrently, often resulting in races during cloud updates.
     */
    BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch("*");
    Response<Void> response = blobClient.setMetadataWithResponse(metadata, blobRequestConditions,
        Duration.ofMillis(cloudConfig.cloudRequestTimeout), Context.NONE);
    /**
     * Must update the only after updating cloud. If we are here, then it means cloud-update succeeded
     * and it is safe to update thread-local cache. If the cloud-update failed, we will never reach this line
     * and the thread-local cache will have the previous safe copy of metadata consistent with cloud.
     */
    getThreadLocalMdCache().putObject(blobLayout.blobFilePath, new BlobMetadata(blobProperties));
    return response;
  }

  /**
   * Returns cached blob properties from Azure
   * @param blobLayout BlobLayout
   * @return Blob properties
   * @throws CloudStorageException
   */
  protected BlobProperties getBlobPropertiesCached(AzureBlobLayoutStrategy.BlobLayout blobLayout)
      throws CloudStorageException {
    BlobMetadata entry = (BlobMetadata) getThreadLocalMdCache().getObject(blobLayout.blobFilePath);
    BlobProperties blobProperties;
    if (entry == null) {
      blobProperties = getBlobProperties(blobLayout);
      getThreadLocalMdCache().putObject(blobLayout.blobFilePath, new BlobMetadata(blobProperties));
    } else {
      blobProperties = entry.getProperties();
    }
    return blobProperties;
  }

  /**
   * Returns blob properties from Azure
   * @param blobLayout BlobLayout
   * @return Blob properties
   * @throws CloudStorageException
   */
  protected BlobProperties getBlobProperties(AzureBlobLayoutStrategy.BlobLayout blobLayout)
      throws CloudStorageException {
    Timer.Context storageTimer = azureMetrics.blobGetPropertiesLatency.time();
    try {
      BlobProperties blobProperties = createOrGetBlobStore(blobLayout.containerName).getBlobClient(blobLayout.blobFilePath).getProperties();
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobGetPropertiesSuccessRate.mark();
      logger.trace("Successfully got blob-properties for {} from Azure blob storage with etag = {}",
          blobLayout.blobFilePath, blobProperties.getETag());
      return blobProperties;
    } catch (BlobStorageException bse) {
      String msg = String.format("Failed to get blob properties for %s from Azure blob storage due to %s", blobLayout, bse.getMessage());
      if (bse.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
        /*
          We encounter many BLOB_NOT_FOUND when uploading new blobs.
          Trace log will not flood the logs when we encounter BLOB_NOT_FOUND.
          Set azureMetrics to null so that we don't unnecessarily increment any metrics for this common case.
         */
        logger.trace(msg);
        throw AzureCloudDestination.toCloudStorageException(msg, bse, null);
      }
      azureMetrics.blobGetPropertiesErrorCount.inc();
      logger.error(msg);
      throw AzureCloudDestination.toCloudStorageException(msg, bse, null);
    } catch (Throwable t) {
      azureMetrics.blobGetPropertiesErrorCount.inc();
      String error = String.format("Failed to get blob properties for %s from Azure blob storage due to %s", blobLayout, t.getMessage());
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, t, null);
    } finally {
      storageTimer.stop();
    }
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) throws CloudStorageException {
    Timer.Context storageTimer = azureMetrics.blobUpdateDeleteTimeLatency.time();
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    Map<String, Object> newMetadata = new HashMap<>();
    newMetadata.put(CloudBlobMetadata.FIELD_DELETION_TIME, String.valueOf(deletionTime));
    newMetadata.put(CloudBlobMetadata.FIELD_LIFE_VERSION, lifeVersion);
    BlobProperties blobProperties = getBlobPropertiesCached(blobLayout);
    Map<String, String> cloudMetadata = blobProperties.getMetadata();

    try {
      if (!cloudUpdateValidator.validateUpdate(CloudBlobMetadata.fromMap(cloudMetadata), blobId, newMetadata)) {
        // lifeVersion must always be present
        short cloudlifeVersion = Short.parseShort(cloudMetadata.get(CloudBlobMetadata.FIELD_LIFE_VERSION));
        if (cloudlifeVersion > lifeVersion) {
          String error = String.format("Failed to update deleteTime of blob %s as it has a higher life version in cloud than replicated message: %s > %s",
              blobIdStr, cloudlifeVersion, lifeVersion);
          logger.trace(error);
          throw AzureCloudDestination.toCloudStorageException(error, new StoreException(error, StoreErrorCodes.Life_Version_Conflict), null);
        }
        String error = String.format("Failed to update deleteTime of blob %s as it is marked for deletion in cloud", blobIdStr);
        logger.trace(error);
        throw AzureCloudDestination.toCloudStorageException(error, new StoreException(error, StoreErrorCodes.ID_Deleted), null);
      }
    } catch (StoreException e) {
      azureMetrics.blobUpdateDeleteTimeErrorCount.inc();
      String error = String.format("Failed to update deleteTime of blob %s in Azure blob storage due to (%s)", blobLayout, e.getMessage());
      throw AzureCloudDestination.toCloudStorageException(error, e, null);
    }

    newMetadata.forEach((k,v) -> cloudMetadata.put(k, String.valueOf(v)));
    try {
      logger.trace("Updating deleteTime of blob {} in Azure blob storage ", blobLayout.blobFilePath);
      Response<Void> response = updateBlobMetadata(blobLayout, blobProperties, cloudMetadata);
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobUpdateDeleteTimeSuccessRate.mark();
      logger.trace("Successfully updated deleteTime of blob {} in Azure blob storage with statusCode = {}, etag = {}",
          blobLayout.blobFilePath, response.getStatusCode(), response.getHeaders().get(HttpHeaderName.ETAG));
      return true;
    } catch (BlobStorageException bse) {
      String error = String.format("Failed to update deleteTime of blob %s in Azure blob storage due to (%s)", blobLayout, bse.getMessage());
      if (bse.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
        /*
          If we are here, it just means that two threads tried to delete concurrently. This is ok.
         */
        logger.trace(error);
        return true;
      }
      azureMetrics.blobUpdateDeleteTimeErrorCount.inc();
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, bse, null);
    } catch (Throwable t) {
      // Unknown error
      azureMetrics.blobUpdateDeleteTimeErrorCount.inc();
      String error = String.format("Failed to update deleteTime of blob %s in Azure blob storage due to (%s)", blobLayout, t.getMessage());
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, t, null);
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
    Timer.Context storageTimer = azureMetrics.blobUndeleteLatency.time();
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    BlobProperties blobProperties = getBlobPropertiesCached(blobLayout);
    Map<String, String> cloudMetadata = blobProperties.getMetadata();
    Map<String, Object> newMetadata = new HashMap<>();
    newMetadata.put(CloudBlobMetadata.FIELD_LIFE_VERSION, lifeVersion);

    // Don't rely on the CloudBlobStore.recentCache to do the "right" thing.
    // Below is the correct behavior. For ref, look at BlobStore::undelete and ReplicaThread::applyUndelete
    try {
      if (!cloudUpdateValidator.validateUpdate(CloudBlobMetadata.fromMap(cloudMetadata), blobId, newMetadata)) {
        /*
          If we are here, it means the cloudLifeVersion >= replicaLifeVersion.
          Cloud is either ahead of server or caught up.
         */
        // lifeVersion must always be present
        short cloudlifeVersion = Short.parseShort(cloudMetadata.get(CloudBlobMetadata.FIELD_LIFE_VERSION));
        /*
          if cloudLifeVersion == replicaLifeVersion && deleteTime absent in cloudMetatadata, then something is wrong. Throw Life_Version_Conflict.
          if cloudLifeVersion == replicaLifeVersion && deleteTime != -1, then something is wrong. Throw Life_Version_Conflict.
         */
        if (cloudlifeVersion == lifeVersion && !cloudMetadata.containsKey(CloudBlobMetadata.FIELD_DELETION_TIME)) {
          String error = String.format("Failed to undelete blob %s as it is undeleted in cloud", blobIdStr);
          logger.trace(error);
          throw AzureCloudDestination.toCloudStorageException(error, new StoreException(error, StoreErrorCodes.ID_Undeleted), null);
        }
        String error = String.format("Failed to undelete blob %s as it has a same or higher life version in cloud than replicated message : %s >= %s",
            blobIdStr, cloudlifeVersion, lifeVersion);
        logger.trace(error);
        throw AzureCloudDestination.toCloudStorageException(error, new StoreException(error, StoreErrorCodes.Life_Version_Conflict), null);
      }
    } catch (StoreException e) {
      azureMetrics.blobUndeleteErrorCount.inc();
      String error = String.format("Failed to undelete blob %s in Azure blob storage because %s", blobLayout, e.getMessage());
      throw AzureCloudDestination.toCloudStorageException(error, e, null);
    }

    newMetadata.forEach((k,v) -> cloudMetadata.put(k, String.valueOf(v)));
    /*
      Just remove the deletion time, instead of setting it to -1.
      It just leads to two cases in code later in compaction, for deleted blobs.
     */
    cloudMetadata.remove(CloudBlobMetadata.FIELD_DELETION_TIME);

    try {
      logger.trace("Resetting deleteTime of blob {} in Azure blob storage ", blobLayout.blobFilePath);
      Response<Void> response = updateBlobMetadata(blobLayout, blobProperties, cloudMetadata);
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobUndeleteSucessRate.mark();
      logger.trace("Successfully reset deleteTime of blob {} in Azure blob storage with statusCode = {}, etag = {}",
          blobLayout.blobFilePath, response.getStatusCode(), response.getHeaders().get(HttpHeaderName.ETAG));
      return lifeVersion;
    } catch (BlobStorageException bse) {
      String error = String.format("Failed to undelete blob %s in Azure blob storage due to (%s)", blobLayout, bse.getMessage());
      if (bse.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
        /*
          If we are here, it just means that two threads tried to un-delete concurrently. This is ok.
        */
        logger.trace(error);
        return lifeVersion;
      }
      azureMetrics.blobUndeleteErrorCount.inc();
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, bse, null);
    } catch (Throwable t) {
      // Unknown error
      azureMetrics.blobUndeleteErrorCount.inc();
      String error = String.format("Failed to undelete blob %s in Azure blob storage due to (%s)", blobLayout, t.getMessage());
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, t, null);
    } finally {
      storageTimer.stop();
    } // try-catch
  }

  @Override
  public CompletableFuture<Short> undeleteBlobAsync(BlobId blobId, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) {
    throw new UnsupportedOperationException("undeleteBlobAsync will not be implemented for AzureCloudDestinationSync");
  }

  protected boolean isDeleted(Map<String, String> cloudMetadata) {
    return cloudMetadata.containsKey(CloudBlobMetadata.FIELD_DELETION_TIME)
        && Long.parseLong(cloudMetadata.get(CloudBlobMetadata.FIELD_DELETION_TIME)) != Utils.Infinite_Time;
  }

  /**
   * Returns true if a blob exists in Azure storage, false otherwise.
   * @param blobId
   * @return
   */
  @Override
  public boolean doesBlobExist(BlobId blobId) {
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    try {
      return createOrGetBlobStore(blobLayout.containerName).getBlobClient(blobLayout.blobFilePath).exists();
    } catch (Throwable t) {
      azureMetrics.blobCheckError.inc();
      logger.error("Failed to check if blob {} exists in Azure blob storage due to {}", blobLayout, t.getMessage());
      return false;
    }
  }

  @Override
  public short updateBlobExpiration(BlobId blobId, long expirationTime, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException {
    Timer.Context storageTimer = azureMetrics.blobUpdateTTLLatency.time();
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    BlobProperties blobProperties = getBlobPropertiesCached(blobLayout);
    Map<String, String> cloudMetadata = blobProperties.getMetadata();

    // Below is the correct behavior. For ref, look at BlobStore::updateTTL and ReplicaThread::applyTtlUpdate.
    // We should never hit this case however because ReplicaThread::applyUpdatesToBlobInLocalStore does all checks.
    // It is absorbed by applyTtlUpdate::L1395.
    // The validator doesn't check for this, perhaps another gap in legacy code.
    if (isDeleted(cloudMetadata)) {
      // Replication must first undelete the deleted blob, and then update-TTL.
      String error = String.format("Unable to update TTL of %s as it is marked for deletion in cloud", blobIdStr);
      logger.trace(error);
      throw AzureCloudDestination.toCloudStorageException(error, new StoreException(error, StoreErrorCodes.ID_Deleted), null);
    }

    try {
      // preTtlUpdateValidation doesn't use the updateFields arg
      if (!cloudUpdateValidator.validateUpdate(CloudBlobMetadata.fromMap(cloudMetadata), blobId, null)) {
        /*
          Legacy cloudBlobStore does not expect an exception. However, below is the correct behavior.
          For ref, look at BlobStore::updateTTL and ReplicaThread::applyTtlUpdate.
          ReplicaThread::applyUpdatesToBlobInLocalStore does all necessary checks before calling updateTTL however,
          ReplicaThread::handleGetResponse does not.

          We can come here from ReplicaThread::handleGetResponse L1258 that applies PUT+TTL without checking if the blob
          uploaded is permanent. The validator reports the blob has already been ttl-updated, and we end up flooding the logs
          and incrementing metrics. To avoid this, just don't inc any error metric or print any logs. However, this would
          mask a scenario where we are erroneously trying to update the ttl of permanent blob. This is ok because we prevent
          an unnecessary request to cloud without sacrificing any correctness. The ReplicaThread should actually check before
          applying a ttl-update, but it was written for disk-based log, and not a cloud-based backup system.
         */
        // azureMetrics.blobUpdateTTLErrorCount.inc(); do not update error metric as this is a flaw in repl-layer
        String error = String.format("Unable to update TTL of %s as its TTL is already updated cloud", blobIdStr);
        logger.trace(error);
        /*
          Set azureMetrics to null to prevent updating any metrics.
          Throw Already_Updated and the caller will handle it at applyTtlUpdate::L1395.
          Don't rely on the recentBlobCache as it could experience an eviction.
         */
        throw AzureCloudDestination.toCloudStorageException(error, new StoreException(error, StoreErrorCodes.Already_Updated), null);
      }
    } catch (StoreException e) {
      // Auth error from validator
      azureMetrics.blobUpdateTTLErrorCount.inc();
      String error = String.format("Unable to update TTL of blob %s in Azure blob storage due to (%s)", blobLayout, e.getMessage());
      throw AzureCloudDestination.toCloudStorageException(error, e, null);
    }

    /*
      Just remove the expiration time, instead of setting it to -1.
      It just leads to two cases in code later in compaction and recovery, for permanent blobs.
     */
    cloudMetadata.remove(CloudBlobMetadata.FIELD_EXPIRATION_TIME);

    // lifeVersion must always be present because we add it explicitly before PUT
    short cloudlifeVersion = Short.parseShort(cloudMetadata.get(CloudBlobMetadata.FIELD_LIFE_VERSION));

    try {
      logger.trace("Updating TTL of blob {} in Azure blob storage ", blobLayout.blobFilePath);
      Response<Void> response = updateBlobMetadata(blobLayout, blobProperties, cloudMetadata);
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobUpdateTTLSucessRate.mark();
      logger.trace("Successfully updated TTL of blob {} in Azure blob storage with statusCode = {}, etag = {}",
          blobLayout.blobFilePath, response.getStatusCode(), response.getHeaders().get(HttpHeaderName.ETAG));
      return cloudlifeVersion;
    } catch (BlobStorageException bse) {
      String error = String.format("Failed to update TTL of blob %s in Azure blob storage due to (%s)", blobLayout, bse.getMessage());
      if (bse.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
        /*
          If we are here, it just means that two threads tried to update-ttl concurrently. This is ok.
         */
        logger.trace(error);
        return cloudlifeVersion;
      }
      azureMetrics.blobUpdateTTLErrorCount.inc();
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, bse, null);
    } catch (Throwable t) {
      azureMetrics.blobUpdateTTLErrorCount.inc();
      String error = String.format("Failed to update TTL of blob %s in Azure blob storage due to (%s)", blobLayout, t.getMessage());
      logger.error(error);
      throw AzureCloudDestination.toCloudStorageException(error, t, null);
    } finally {
      storageTimer.stop();
    } // try-catch
  }

  @Override
  public CompletableFuture<Short> updateBlobExpirationAsync(BlobId blobId, long expirationTime,
      CloudUpdateValidator cloudUpdateValidator) {
    throw new UnsupportedOperationException("updateBlobExpirationAsync will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException {
    /*
        This is used by findMissingKeys, which doesn't even use the metadata provided.
        It just discards it. But due to legacy reasons, we are required to impl this way.
        We could use one-liner Azure SDK provided lightweight exists() method, which internally fetches blob-properties
        and does the same thing that findMissingKeys does.
     */
    Map<String, CloudBlobMetadata> cloudBlobMetadataMap = new HashMap<>();
    for (BlobId blobId: blobIds) {
      AzureBlobLayoutStrategy.BlobLayout blobLayout = this.azureBlobLayoutStrategy.getDataBlobLayout(blobId);
      try {
        BlobProperties blobProperties = getBlobPropertiesCached(blobLayout);
        cloudBlobMetadataMap.put(blobId.getID(), CloudBlobMetadata.fromMap(blobProperties.getMetadata()));
      } catch (CloudStorageException cse) {
        if (cse.getCause() instanceof BlobStorageException &&
            ((BlobStorageException) cse.getCause()).getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
          /*
            We mostly get here from findMissingKeys, and we will encounter many blobs missing from cloud before we upload them.
           */
          continue;
        }
        throw cse;
      } catch (Throwable t) {
        // Unknown error, increment the generic metric in azureMetrics
        String error = String.format("Failed to get blob metadata for %s from Azure blob storage due to %s", blobLayout, t.getMessage());
        logger.error(error);
        throw AzureCloudDestination.toCloudStorageException(error, t, azureMetrics);
      }
    }
    return cloudBlobMetadataMap;
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

  /**
   * Erases blobs from a given list of blobs in cloud
   * @param blobItemList List of blobs in a container
   * @param blobContainerClient BlobContainer client
   * @return The number of blobs erased
   */
  protected int eraseBlobs(List<BlobItem> blobItemList, BlobContainerClient blobContainerClient,
      Set<Pair<Short, Short>> deletedContainers, Supplier<Boolean> stopCompaction) {
    int numBlobsPurged = 0;
    long now = System.currentTimeMillis();
    long gracePeriod = TimeUnit.DAYS.toMillis(cloudConfig.cloudCompactionGracePeriodDays);
    Iterator<BlobItem> blobItemIterator = blobItemList.iterator();
    /*
        while (blobItemIterator.hasNext() && !stopCompaction.get()) invokes stopCompaction once per blob.
        while (!stopCompaction.get() && blobItemIterator.hasNext()) invokes stopCompaction twice per blob.
        Useful to know for testCompactPartitionDisown.
     */
    while (blobItemIterator.hasNext() && !stopCompaction.get()) {
      BlobItem blobItem = blobItemIterator.next();
      Map<String, String> metadata = blobItem.getMetadata();
      Pair<Short, Short> accountContainerIds = new Pair<>(Short.parseShort(metadata.get(CloudBlobMetadata.FIELD_ACCOUNT_ID)),
          Short.parseShort(metadata.get(CloudBlobMetadata.FIELD_CONTAINER_ID)));
      boolean eraseBlob = false;
      String eraseReason;

      if (metadata.containsKey(CloudBlobMetadata.FIELD_DELETION_TIME)) {
        long deletionTime = Long.parseLong(metadata.get(CloudBlobMetadata.FIELD_DELETION_TIME));
        eraseBlob = (deletionTime + gracePeriod) < now;
        eraseReason = String.format("%s: (%s + %s) < %s", CloudBlobMetadata.FIELD_DELETION_TIME, deletionTime, gracePeriod, now);
      } else if (metadata.containsKey(CloudBlobMetadata.FIELD_EXPIRATION_TIME)) {
        long expirationTime = Long.parseLong(metadata.get(CloudBlobMetadata.FIELD_EXPIRATION_TIME));
        eraseBlob = (expirationTime + gracePeriod) < now;
        eraseReason = String.format("%s: (%s + %s) < %s", CloudBlobMetadata.FIELD_EXPIRATION_TIME, expirationTime, gracePeriod, now);
      } else if (deletedContainers.contains(accountContainerIds)) {
        eraseBlob = true;
        eraseReason = String.format("account = %s, deleted_container = %s", accountContainerIds.getFirst(), accountContainerIds.getSecond());
      } else {
        // nothing to do, blob cannot be deleted
        eraseReason = String.format("No reason to erase blob %s", blobItem.getName());
      }

      if (eraseBlob) {
        if (cloudConfig.cloudCompactionDryRunEnabled) {
          logger.trace("[DRY-RUN][COMPACT] Can erase blob {} from Azure blob storage because {}", blobItem.getName(), eraseReason);
          numBlobsPurged += 1;
        } else {
          Timer.Context storageTimer = azureMetrics.blobCompactionLatency.time();
          Response<Void> response = blobContainerClient.getBlobClient(blobItem.getName())
              .deleteWithResponse(DeleteSnapshotsOptionType.INCLUDE, null, null, null);
          storageTimer.stop();
          switch (response.getStatusCode()) {
            case HttpStatus.SC_ACCEPTED:
              logger.trace("[COMPACT] Erased blob {} from Azure blob storage, reason = {}, status = {}", blobItem.getName(),
                  eraseReason, response.getStatusCode());
              numBlobsPurged += 1;
              azureMetrics.blobCompactionSuccessRate.mark();
              break;
            case HttpStatus.SC_NOT_FOUND:
            default:
              // Just increment a counter and set an alert on it. No need to throw an error and fail the thread.
              azureMetrics.blobCompactionErrorCount.inc();
              logger.error("[COMPACT] Failed to erase blob {} from Azure blob storage with status {}", blobItem.getName(),
                  response.getStatusCode());
          }
        }
      } else {
        logger.trace("[COMPACT] Cannot erase blob {} from Azure blob storage because condition not met: {}", blobItem.getName(), eraseReason);
      }

    }
    return numBlobsPurged;
  }

  /**
   *
   * @return Returns containers from account-service which are in DELETE_IN_PROGRESS state for N days
   */
  protected Set<Pair<Short, Short>> getDeletedContainers() {
    Set<Container> deletedContainers = AccountUtils.getDeprecatedContainers(accountService, storeConfig.storeContainerDeletionRetentionDays);
    Set<Pair<Short, Short>> accountContainerPairSet = new HashSet<>();
    for (Container container: deletedContainers) {
      accountContainerPairSet.add(new Pair<>(container.getParentAccountId(), container.getId()));
    }
    return accountContainerPairSet;
  }

  /**
   * Compacts a partition
   * Stub for backward compatibility
   * @return the number of blobs erased from cloud
   * @throws CloudStorageException
   */
  @Override
  public int compactPartition(String partitionPath) throws CloudStorageException {
    return compactPartition(partitionPath, null);
  }

  /**
   * Compacts a partition
   * @param partitionPath the path of the partitions to compact.
   * @return the number of blobs erased from cloud
   * @throws CloudStorageException
   */
  public int compactPartition(String partitionPath, CloudStorageCompactor compactor) throws CloudStorageException {
    /*
      BlobContainerStrategy must be PARTITION, otherwise compaction will not work because it cannot find the container in ABS.
      For BlobContainerStrategy to be CONTAINER, we need a blobId to extract accountId and containerId and we don't have that here.
     */
    if (AzureBlobLayoutStrategy.BlobContainerStrategy.get(azureCloudConfig.azureBlobContainerStrategy) !=
        AzureBlobLayoutStrategy.BlobContainerStrategy.PARTITION) {
      logger.info("[COMPACT] Unable to compact because BlobContainerStrategy is {} when it must be {}", azureCloudConfig.azureBlobContainerStrategy,
          AzureBlobLayoutStrategy.BlobContainerStrategy.PARTITION);
      return 0;
    }
    Set<Pair<Short, Short>> deletedContainers = getDeletedContainers();
    String containerName = azureBlobLayoutStrategy.getClusterAwareAzureContainerName(partitionPath);
    BlobContainerClient blobContainerClient = createOrGetBlobStore(containerName);
    ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setDetails(new BlobListDetails()
        .setRetrieveMetadata(true)).setMaxResultsPerPage(azureCloudConfig.azureBlobStorageMaxResultsPerPage);
    String continuationToken = null;
    int numBlobsPurged = 0, totalNumBlobs = 0;
    Timer.Context storageTimer = azureMetrics.partitionCompactionLatency.time();
    try {
      logger.info("[COMPACT] Compacting partition {} in Azure blob storage", containerName);
      Supplier<Boolean> stopCompaction = () -> isCompactionStopped(partitionPath, compactor);
      while (!stopCompaction.get()) {
        PagedResponse<BlobItem> blobItemPagedResponse =
            blobContainerClient.listBlobs(listBlobsOptions, null)
                .iterableByPage(continuationToken).iterator().next();
        continuationToken = blobItemPagedResponse.getContinuationToken();
        logger.debug("[COMPACT] Acquired continuation-token {} for partition {}", continuationToken, containerName);
        List<BlobItem> blobItemList = blobItemPagedResponse.getValue();
        totalNumBlobs += blobItemList.size();
        numBlobsPurged += eraseBlobs(blobItemList, blobContainerClient, deletedContainers, stopCompaction);
        if (continuationToken == null) {
          logger.trace("[COMPACT] Reached end-of-partition {} as Azure blob storage continuationToken is null", containerName);
          break;
        }
      }
    } catch (Throwable t) {
      azureMetrics.partitionCompactionErrorCount.inc();
      String error = String.format("[COMPACT] Failed to compact partition %s due to %s", partitionPath, t.getMessage());
      logger.error(error);
      // Swallow error & return partial result. We want to be as close as possible to the real number of blobs deleted.
    } finally {
      storageTimer.stop();
    }
    logger.info("[COMPACT] Erased {} blobs out of {} from partition {} in Azure blob storage",
        numBlobsPurged, totalNumBlobs, partitionPath);
    return numBlobsPurged;
  }

  @Override
  public void persistTokens(String partitionPath, String tokenFileName, InputStream inputStream) {
    throw new UnsupportedOperationException("AzureCloudDestinationSync::persistTokens is unsupported");
  }

  @Override
  public boolean retrieveTokens(String partitionPath, String tokenFileName, OutputStream outputStream) {
    throw new UnsupportedOperationException("AzureCloudDestinationSync::retrieveTokens is unsupported");
  }

  @Override
  public boolean stopCompaction() {
    return shutdownCompaction.compareAndSet(false, true);
  }

  @Override
  public boolean isCompactionStopped(String partition, CloudStorageCompactor compactor) {
    if (shutdownCompaction.get()) {
      logger.info("[COMPACT] Stopping compaction for partition {} due to shutdown", partition);
      return true;
    }
    if (compactor != null && !compactor.isPartitionOwned(clusterMap.getPartitionIdByName(partition))) {
      logger.info("[COMPACT] Stopping compaction for partition {} due to loss of partition ownership", partition);
      return true;
    }
    return false;
  }

  @Override
  public void deprecateContainers(Collection<Container> deprecatedContainers) {
    throw new UnsupportedOperationException("AzureCloudDestinationSync::deprecateContainers is unsupported");
  }

  @Override
  public CloudContainerCompactor getContainerCompactor() {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
