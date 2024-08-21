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
import com.azure.core.http.HttpResponse;
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

  public static final String X_MS_REQUEST_ID = "x-ms-request-id";

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
  protected class AzureBlobProperties implements AmbryCacheEntry {

    private final BlobProperties properties;
    private final int azureStatus;
    private final String azureRequestId;
    public AzureBlobProperties(BlobProperties properties, String azureRequestId, int azureStatus) {
      this.properties = properties;
      this.azureRequestId = azureRequestId;
      this.azureStatus = azureStatus;
    }

    public BlobProperties getProperties() {
      return properties;
    }

    public String getAzureRequestId() {
      return azureRequestId;
    }

    public int getAzureStatus() {
      return azureStatus;
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
      // Enable cache ifd size > 0, else disable
      threadLocalMdCache.set(new AmbryCache(cacheName, cloudConfig.recentBlobCacheLimit > 0,
          cloudConfig.recentBlobCacheLimit, metrics));
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
    try {
      return getTableClient(tableName).getEntity(partitionKey, rowKey);
    } catch (Throwable e) {
      logger.error("Failed to fetch table entity {}/{} from {} due to {}",
          partitionKey, rowKey, tableName, e.getMessage());
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
    // If permanent blob, then just remove the expiry = -1
    if (metadata.containsKey(CloudBlobMetadata.FIELD_EXPIRATION_TIME) &&
        metadata.get(CloudBlobMetadata.FIELD_EXPIRATION_TIME).equals(String.valueOf(Utils.Infinite_Time))) {
      metadata.remove(CloudBlobMetadata.FIELD_EXPIRATION_TIME);
    }
    metadata.remove(CloudBlobMetadata.FIELD_UPLOAD_TIME); // unused field
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
    for (MessageInfo messageInfo: stream.getValidMessageInfoList()) {
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
        AzureBlobProperties properties =
            (AzureBlobProperties) getThreadLocalMdCache().getObject(blobLayout.blobFilePath);
        String status = "no record of a look-up request";
        if (properties != null) {
          status = String.format("previous look-up request %s returned %s", properties.getAzureRequestId(),
              properties.getAzureStatus());
        }
        logger.error("Failed to upload blob {} from {} to Azure as it already exists & {}",
            blobLayout, cloudBlobMetadata.getReplicaLocation(), status);
        // We should rarely be here because we get here from replication logic which checks if a blob exists or not before uploading it.
        // However, if we end up here, return true to allow replication to proceed instead of halting it. Else the replication token will not advance.
        // The blob in the cloud is safe as Azure prevented us from overwriting it due to an ETag check.
        return true;
      }
      azureMetrics.blobUploadErrorCount.inc();
      String error = String.format("Failed to upload blob %s to Azure blob storage because %s", blobLayout, e.getMessage());
      logger.error(error);
      throw new CloudStorageException(error);
    } finally {
      if (storageTimer != null) {
        storageTimer.stop();
      }
      getThreadLocalMdCache().deleteObject(blobLayout.blobFilePath);
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
      throw new CloudStorageException(error);
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
   * @return HTTP response and blob metadata
   */
  protected Response<Void> updateBlobMetadata(AzureBlobLayoutStrategy.BlobLayout blobLayout,
      BlobProperties blobProperties) {
    Map<String, String> metadata = blobProperties.getMetadata();
    BlobClient blobClient = createOrGetBlobStore(blobLayout.containerName).getBlobClient(blobLayout.blobFilePath);
    /**
     * When replicating, we might receive a TTL-UPDATE for a blob from replica-A and a DELETE for the same blob from replica-B.
     * The thread-local cache functions as a write-through cache. We write the TTL-UPDATE from replica-A through this cache,
     * followed by the DELETE from replica-B. The ETag changes in the cloud after each update, and we avoid additional read
     * requests from the cloud between updates. This means we are not performing read-modify-write
     * operations on the blob metadata in the cloud, so we cannot rely on ETag matching to constrain updates.
     * Additionally, ETag matching is primarily useful for concurrent updates, which is not relevant here because all
     * replicas for a partition are scanned serially by the same thread. This is a departure from the previous design,
     * where all replicas for a partition were scanned concurrently, often resulting in races during cloud updates.
     */
    BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch("*");
    Response<Void> response = blobClient.setMetadataWithResponse(metadata, blobRequestConditions,
        Duration.ofMillis(cloudConfig.cloudRequestTimeout), Context.NONE);
    return response;
  }

  /**
   * Returns cached blob properties from Azure
   * @param blobLayout BlobLayout
   * @return Blob properties
   * @throws CloudStorageException
   */
  protected AzureBlobProperties getAzureBlobPropertiesCached(AzureBlobLayoutStrategy.BlobLayout blobLayout)
      throws CloudStorageException {
    AzureBlobProperties azureBlobProperties = (AzureBlobProperties) getThreadLocalMdCache().getObject(blobLayout.blobFilePath);
    if (azureBlobProperties != null && azureBlobProperties.getProperties() != null &&
        azureBlobProperties.getAzureStatus() == HttpStatus.SC_OK) {
      return azureBlobProperties;
    }
    azureBlobProperties = getBlobProperties(blobLayout);
    getThreadLocalMdCache().putObject(blobLayout.blobFilePath, azureBlobProperties);
    return azureBlobProperties;
  }

  /**
   * Returns blob properties from Azure
   * @param blobLayout BlobLayout
   * @return Blob properties
   * @throws CloudStorageException
   */
  protected AzureBlobProperties getBlobProperties(AzureBlobLayoutStrategy.BlobLayout blobLayout)
      throws CloudStorageException {
    Timer.Context storageTimer = azureMetrics.blobGetPropertiesLatency.time();
    String errfmt = "Failed to get blob properties for %s from Azure blob storage due to %s";
    try {
      Response<BlobProperties> response = createOrGetBlobStore(blobLayout.containerName)
          .getBlobClient(blobLayout.blobFilePath)
          .getPropertiesWithResponse(null, null, Context.NONE);
      BlobProperties blobProperties = response.getValue();
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobGetPropertiesSuccessRate.mark();
      logger.trace("Successfully got blob-properties for {} from Azure blob storage with etag = {}",
          blobLayout.blobFilePath, blobProperties.getETag());
      return new AzureBlobProperties(blobProperties, response.getHeaders().getValue(X_MS_REQUEST_ID),
          response.getStatusCode());
    } catch (BlobStorageException bse) {
      String msg = String.format(errfmt, blobLayout, bse.getMessage());
      if (bse.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
        logger.trace(msg);
        HttpResponse response = bse.getResponse();
        return new AzureBlobProperties(null, bse.getResponse().getHeaders().getValue(X_MS_REQUEST_ID),
            response.getStatusCode());
      }
      azureMetrics.blobGetPropertiesErrorCount.inc();
      logger.error(msg);
      throw new CloudStorageException(msg);
    } catch (Throwable t) {
      azureMetrics.blobGetPropertiesErrorCount.inc();
      String error = String.format(errfmt, blobLayout, t.getMessage());
      logger.error(error);
      throw new CloudStorageException(error);
    } finally {
      storageTimer.stop();
    }
  }


  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator unused) throws CloudStorageException, StoreException {
    Timer.Context storageTimer = azureMetrics.blobDeleteLatency.time();
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    String errfmt = "Uninitialized string";
    try {
      // AzureBlobDeletePolicy.IMMEDIATE
      if (azureCloudConfig.azureBlobDeletePolicy.equals(AzureBlobDeletePolicy.IMMEDIATE)) {
        errfmt = "Failed to delete blob %s in Azure blob storage due to (%s)";
        int status = eraseBlob(createOrGetBlobStore(blobLayout.containerName).getBlobClient(blobLayout.blobFilePath),
            "CUSTOMER_DELETE_REQUEST");
        if(status != HttpStatus.SC_ACCEPTED) {
          // Throw CloudStorageException for operations that reached Azure, else StoreException.
          // Throw an error to halt replication. If we don't, then we risk orphan blobs in Azure costing storage.
          // The message here will be concatenated with errfmt string in the catch-clause.
          throw new CloudStorageException(String.format("%s error code", status), status);
        }
        azureMetrics.blobDeleteSuccessRate.mark();
        return true; // this return value is unused
      }

      // AzureBlobDeletePolicy.EVENTUAL
      BlobProperties blobPropertiesCached = getAzureBlobPropertiesCached(blobLayout).getProperties();
      Map<String, String> cloudMetadataCached = blobPropertiesCached.getMetadata();
      // lifeVersion must always be present
      short cloudlifeVersion = Short.parseShort(cloudMetadataCached.get(CloudBlobMetadata.FIELD_LIFE_VERSION));
      if (cloudlifeVersion > lifeVersion) {
        String error = String.format("Failed to update deleteTime of blob %s as it has a higher life version in cloud than replicated message: %s > %s",
            blobIdStr, cloudlifeVersion, lifeVersion);
        logger.trace(error);
        throw new StoreException(error, StoreErrorCodes.Life_Version_Conflict);
      }
      if (cloudlifeVersion == lifeVersion && cloudMetadataCached.containsKey(CloudBlobMetadata.FIELD_DELETION_TIME)) {
        String error = String.format("Failed to update deleteTime of blob %s as it is marked for deletion in cloud", blobIdStr);
        logger.trace(error);
        throw new StoreException(error, StoreErrorCodes.ID_Deleted);
      }
      errfmt = "Failed to update deleteTime of blob %s in Azure blob storage due to (%s)";
      Map<String, Object> newMetadata = new HashMap<>();
      newMetadata.put(CloudBlobMetadata.FIELD_DELETION_TIME, String.valueOf(deletionTime));
      newMetadata.put(CloudBlobMetadata.FIELD_LIFE_VERSION, lifeVersion);
      newMetadata.forEach((k,v) -> cloudMetadataCached.put(k, String.valueOf(v)));
      logger.trace("Updating deleteTime of blob {} in Azure blob storage ", blobLayout.blobFilePath);
      Response<Void> response = updateBlobMetadata(blobLayout, blobPropertiesCached);
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobDeleteSuccessRate.mark();
      logger.trace("Successfully updated deleteTime of blob {} in Azure blob storage with statusCode = {}, etag = {}",
          blobLayout.blobFilePath, response.getStatusCode(), response.getHeaders().get(HttpHeaderName.ETAG));
      return true;
    } catch (StoreException e) {
      azureMetrics.blobDeleteErrorCount.inc();
      throw e;
    } catch (Throwable t) {
      azureMetrics.blobDeleteErrorCount.inc();
      String error = String.format(errfmt, blobLayout, t.getMessage());
      logger.error(error);
      throw new CloudStorageException(error);
    } finally {
      storageTimer.stop();
      getThreadLocalMdCache().deleteObject(blobLayout.blobFilePath);
    } // try-catch
  }

  @Override
  public CompletableFuture<Boolean> deleteBlobAsync(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) {
    throw new UnsupportedOperationException("deleteBlobAsync will not be implemented for AzureCloudDestinationSync");
  }

  @Override
  public short undeleteBlob(BlobId blobId, short lifeVersion, CloudUpdateValidator unused)
      throws CloudStorageException, StoreException {
    Timer.Context storageTimer = azureMetrics.blobUndeleteLatency.time();
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    BlobProperties blobPropertiesCached = getAzureBlobPropertiesCached(blobLayout).getProperties();
    Map<String, String> cloudMetadataCached = blobPropertiesCached.getMetadata();
    Map<String, Object> newMetadata = new HashMap<>();
    newMetadata.put(CloudBlobMetadata.FIELD_LIFE_VERSION, lifeVersion);
    try {
      // If we are here, it means the cloudLifeVersion >= replicaLifeVersion.
      // Cloud is either ahead of server or caught up.
      // lifeVersion must always be present
      short cloudlifeVersion = Short.parseShort(cloudMetadataCached.get(CloudBlobMetadata.FIELD_LIFE_VERSION));
      // if cloudLifeVersion == replicaLifeVersion && deleteTime absent in cloudMetatadata, then something is wrong. Throw Life_Version_Conflict.
      // if cloudLifeVersion == replicaLifeVersion && deleteTime != -1, then something is wrong. Throw Life_Version_Conflict.
      if (cloudlifeVersion == lifeVersion && !cloudMetadataCached.containsKey(CloudBlobMetadata.FIELD_DELETION_TIME)) {
        String error = String.format("Failed to undelete blob %s as it is undeleted in cloud", blobIdStr);
        logger.trace(error);
        throw new StoreException(error, StoreErrorCodes.ID_Undeleted);
      }
      if (cloudlifeVersion > lifeVersion) {
        String error = String.format("Failed to undelete blob %s as it has a higher life version in cloud than replicated message : %s > %s",
            blobIdStr, cloudlifeVersion, lifeVersion);
        logger.trace(error);
        throw new StoreException(error, StoreErrorCodes.Life_Version_Conflict);
      }
      // Just remove the deletion time, instead of setting it to -1.
      // It just leads to two cases in code later in compaction, for deleted blobs.
      newMetadata.forEach((k,v) -> cloudMetadataCached.put(k, String.valueOf(v)));
      cloudMetadataCached.remove(CloudBlobMetadata.FIELD_DELETION_TIME);
      logger.trace("Resetting deleteTime of blob {} in Azure blob storage ", blobLayout.blobFilePath);
      Response<Void> response = updateBlobMetadata(blobLayout, blobPropertiesCached);
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobUndeleteSuccessRate.mark();
      logger.trace("Successfully reset deleteTime of blob {} in Azure blob storage with statusCode = {}, etag = {}",
          blobLayout.blobFilePath, response.getStatusCode(), response.getHeaders().get(HttpHeaderName.ETAG));
      return lifeVersion;
    } catch (BlobStorageException bse) {
      String error =
          String.format("Failed to undelete blob %s in Azure blob storage due to (%s)", blobLayout, bse.getMessage());
      if (bse.getErrorCode() == BlobErrorCode.CONDITION_NOT_MET) {
        /*
          If we are here, it just means that two threads tried to un-delete concurrently. This is ok.
        */
        logger.trace(error);
        return lifeVersion;
      }
      azureMetrics.blobUndeleteErrorCount.inc();
      logger.error(error);
      throw new CloudStorageException(error);
    } catch (StoreException e) {
      azureMetrics.blobUndeleteErrorCount.inc();
      throw e;
    } catch (Throwable t) {
      // Unknown error
      azureMetrics.blobUndeleteErrorCount.inc();
      String error = String.format("Failed to undelete blob %s in Azure blob storage due to (%s)", blobLayout, t.getMessage());
      logger.error(error);
      throw new CloudStorageException(error);
    } finally {
      storageTimer.stop();
      getThreadLocalMdCache().deleteObject(blobLayout.blobFilePath);
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
  public boolean doesBlobExist(BlobId blobId) throws CloudStorageException {
    try {
      AzureBlobLayoutStrategy.BlobLayout blobLayout = this.azureBlobLayoutStrategy.getDataBlobLayout(blobId);
      return getAzureBlobPropertiesCached(blobLayout).getProperties() != null;
    } catch (CloudStorageException cse) {
      throw cse;
    } catch (Throwable t) {
      throw new CloudStorageException(t.getMessage());
    }
  }

  @Override
  public short updateBlobExpiration(BlobId blobId, long unused, CloudUpdateValidator unused2)
      throws CloudStorageException, StoreException {
    Timer.Context storageTimer = azureMetrics.blobUpdateTTLLatency.time();
    AzureBlobLayoutStrategy.BlobLayout blobLayout = azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    String blobIdStr = blobLayout.blobFilePath;
    BlobProperties blobPropertiesCached = getAzureBlobPropertiesCached(blobLayout).getProperties();
    Map<String, String> cloudMetadataCached = blobPropertiesCached.getMetadata();
    // lifeVersion must always be present because we add it explicitly before PUT
    short cloudlifeVersion = Short.parseShort(cloudMetadataCached.get(CloudBlobMetadata.FIELD_LIFE_VERSION));
    try {
      // Below is the correct behavior. For ref, look at BlobStore::updateTTL and ReplicaThread::applyTtlUpdate.
      // We should never hit this case however because ReplicaThread::applyUpdatesToBlobInLocalStore does all checks.
      // It is absorbed by applyTtlUpdate::L1395.
      // The validator doesn't check for this, perhaps another gap in legacy code.
      if (isDeleted(cloudMetadataCached)) {
        // Replication must first undelete the deleted blob, and then update-TTL.
        String error = String.format("Unable to update TTL of %s as it is marked for deletion in cloud", blobIdStr);
        logger.trace(error);
        throw new StoreException(error, StoreErrorCodes.ID_Deleted);
      }
      if (!cloudMetadataCached.containsKey(CloudBlobMetadata.FIELD_EXPIRATION_TIME)) {
        String error = String.format("Unable to update TTL of %s as its TTL is already updated cloud", blobIdStr);
        logger.trace(error);
        throw new StoreException(error, StoreErrorCodes.Already_Updated);
      }
      // Just remove the expiration time, instead of setting it to -1.
      // It just leads to two cases in code later in compaction and recovery, for permanent blobs.
      cloudMetadataCached.remove(CloudBlobMetadata.FIELD_EXPIRATION_TIME);
      logger.trace("Updating TTL of blob {} in Azure blob storage ", blobLayout.blobFilePath);
      Response<Void> response = updateBlobMetadata(blobLayout, blobPropertiesCached);
      // Success rate is effective, success counter is ineffective because it just monotonically increases
      azureMetrics.blobUpdateTTLSuccessRate.mark();
      logger.trace("Successfully updated TTL of blob {} in Azure blob storage with statusCode = {}, etag = {}",
          blobLayout.blobFilePath, response.getStatusCode(), response.getHeaders().get(HttpHeaderName.ETAG));
      return cloudlifeVersion;
    } catch (StoreException e) {
      // Auth error from validator
      azureMetrics.blobUpdateTTLErrorCount.inc();
      throw e;
    } catch (Throwable t) {
      azureMetrics.blobUpdateTTLErrorCount.inc();
      String error = String.format("Failed to update TTL of blob %s in Azure blob storage due to (%s)", blobLayout, t.getMessage());
      logger.error(error);
      throw new CloudStorageException(error);
    } finally {
      storageTimer.stop();
      getThreadLocalMdCache().deleteObject(blobLayout.blobFilePath);
    } // try-catch
  }

  @Override
  public CompletableFuture<Short> updateBlobExpirationAsync(BlobId blobId, long expirationTime,
      CloudUpdateValidator cloudUpdateValidator) {
    throw new UnsupportedOperationException("updateBlobExpirationAsync will not be implemented for AzureCloudDestinationSync");
  }

  /**
   * Get azure-blob-properties and wraps it in ambry cloud-blob-metadata object.
   * @param blobId
   * @return
   * @throws CloudStorageException
   */
  @Override
  public CloudBlobMetadata getCloudBlobMetadata(BlobId blobId) throws CloudStorageException {
    AzureBlobLayoutStrategy.BlobLayout blobLayout = this.azureBlobLayoutStrategy.getDataBlobLayout(blobId);
    try {
      AzureBlobProperties azureBlobProperties = getAzureBlobPropertiesCached(blobLayout);
      if (azureBlobProperties.getProperties() == null) {
        throw new CloudStorageException(String.format("Failed to find blob properties for %s due to %s",
            blobLayout.blobFilePath, azureBlobProperties.getAzureStatus()));
      }
      return CloudBlobMetadata.fromMap(azureBlobProperties.getProperties().getMetadata());
    } catch (CloudStorageException cse) {
      throw cse;
    } catch (Throwable t) {
      throw new CloudStorageException(t.getMessage());
    }
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException {
    /**
     * Just keeping this fn around for tests and legacy reasons but imo this is a horrible function.
     * The fn takes a list as an arg, why not just a single blob ? It is only ever used to fetch metadata of a blob,
     * not a list. And because of this design, the higher levels have to wrap the blob-id in a singleton-list.
     * More importantly, the return type is map. The higher levels have to explicitly look for the result in the map
     * with blob-id. If there is typo in the code in looking up the map, for eg. instead of storeKey.getId(),
     * if we type storeKey() alone, then we make a mistake of thinking the blob is not found. Just stop using maps!
     * Such lookup errors are easy to miss in code-reviews. And most legacy tests are broken beyond repair.
     */
    Map<String, CloudBlobMetadata> cloudBlobMetadataMap = new HashMap<>();
    for (BlobId blobId: blobIds) {
      try {
        CloudBlobMetadata cloudBlobMetadata = getCloudBlobMetadata(blobId);
        if (cloudBlobMetadata != null) {
          cloudBlobMetadataMap.put(blobId.getID(), cloudBlobMetadata);
        }
      } catch (Throwable t) {
        throw t;
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
   * Erases a blob permanently, including all snapshots of it from Azure Storage.
   *
   * @param blobClient  Client for the blob
   * @param eraseReason Reason to delete
   * @return True if blob deleted, else false.
   */
  protected int eraseBlob(BlobClient blobClient, String eraseReason) {
    Timer.Context storageTimer = azureMetrics.blobCompactionLatency.time();
    BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch("*");
    Response<Void> response = blobClient.deleteWithResponse(DeleteSnapshotsOptionType.INCLUDE,
        blobRequestConditions, Duration.ofMillis(cloudConfig.cloudRequestTimeout), Context.NONE);
    storageTimer.stop();
    switch (response.getStatusCode()) {
      case HttpStatus.SC_ACCEPTED:
        logger.trace("[ERASE] Erased blob {}/{} from Azure blob storage, reason = {}, status = {}",
            blobClient.getContainerName(), blobClient.getBlobName(), eraseReason, response.getStatusCode());
        break;
      case HttpStatus.SC_NOT_FOUND:
        // If you're trying to delete a blob, then it must exist.
        // If it doesn't, then there is something wrong in the code. Go figure it out !
      default:
        // Just increment a counter and set an alert on it. No need to throw an error and fail the thread.
        logger.error("[ERASE] Failed to erase blob {}/{} from Azure blob storage, reason = {}, status {}",
            blobClient.getContainerName(), blobClient.getBlobName(), eraseReason, response.getStatusCode());
    }
    return response.getStatusCode();
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
        eraseBlob = expirationTime < now; // no grace-period for expired blobs; just get rid of them
        eraseReason = String.format("%s: %s < %s", CloudBlobMetadata.FIELD_EXPIRATION_TIME, expirationTime, now);
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
          int status = eraseBlob(blobContainerClient.getBlobClient(blobItem.getName()), eraseReason);
          if (status == HttpStatus.SC_ACCEPTED) {
            numBlobsPurged += 1;
            azureMetrics.blobCompactionSuccessRate.mark();
          } else {
            azureMetrics.blobCompactionErrorCount.inc();
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
