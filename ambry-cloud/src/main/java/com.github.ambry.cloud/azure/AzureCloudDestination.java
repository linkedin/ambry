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

import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudFindToken;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link CloudDestination} that interacts with Azure Blob Storage service.
 */
class AzureCloudDestination implements CloudDestination {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestination.class);
  private static final String THRESHOLD_PARAM = "@threshold";
  private static final String LIMIT_PARAM = "@limit";
  private static final String TIME_SINCE_PARAM = "@timesince";
  private static final String BATCH_ID_QUERY_TEMPLATE = "SELECT * FROM c WHERE c.id IN (%s)";
  static final int ID_QUERY_BATCH_SIZE = 1000;
  static final String DEAD_BLOBS_QUERY_TEMPLATE =
      "SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE (c." + CloudBlobMetadata.FIELD_DELETION_TIME + " BETWEEN 1 AND "
          + THRESHOLD_PARAM + ")" + " OR (c." + CloudBlobMetadata.FIELD_EXPIRATION_TIME + " BETWEEN 1 AND "
          + THRESHOLD_PARAM + ")" + " ORDER BY c." + CloudBlobMetadata.FIELD_UPLOAD_TIME + " ASC";
  // Note: ideally would like to order by uploadTime and id, but Cosmos doesn't allow without composite index.
  // It is unlikely (but not impossible) for two blobs in same partition to have the same uploadTime (would have to
  // be multiple VCR's uploading same partition).  We track the lastBlobId in the CloudFindToken and skip it if
  // is returned in successive queries.
  static final String ENTRIES_SINCE_QUERY_TEMPLATE =
      "SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " >= "
          + TIME_SINCE_PARAM + " ORDER BY c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " ASC";
  private static final String SEPARATOR = "-";
  private static final int findSinceQueryLimit = 1000;
  private final CloudStorageAccount azureAccount;
  private final CloudBlobClient azureBlobClient;
  private final DocumentClient documentClient;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final OperationContext blobOpContext = new OperationContext();
  private final AzureMetrics azureMetrics;
  private final String clusterName;
  private final long retentionPeriodMs;
  private final int deadBlobsQueryLimit;
  // Containers known to exist in the storage account
  private final Set<String> knownContainers = ConcurrentHashMap.newKeySet();

  /**
   * Construct an Azure cloud destination from config properties.
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param azureCloudConfig the {@link AzureCloudConfig} to use.
   * @param clusterName the name of the Ambry cluster.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @throws InvalidKeyException if credentials in the connection string contain an invalid key.
   * @throws URISyntaxException if the connection string specifies an invalid URI.
   */
  AzureCloudDestination(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, String clusterName,
      AzureMetrics azureMetrics) throws URISyntaxException, InvalidKeyException {
    this.azureMetrics = azureMetrics;
    azureAccount = CloudStorageAccount.parse(azureCloudConfig.azureStorageConnectionString);
    azureBlobClient = azureAccount.createCloudBlobClient();
    this.clusterName = clusterName;
    // Check for proxy
    if (cloudConfig.vcrProxyHost != null) {
      logger.info("Using proxy: {}:{}", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
      OperationContext.setDefaultProxy(
          new Proxy(Proxy.Type.HTTP, new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort)));
    }
    // Set up CosmosDB connection, including any proxy setting
    ConnectionPolicy connectionPolicy = new ConnectionPolicy();
    if (azureCloudConfig.cosmosDirectHttps) {
      logger.info("Using CosmosDB DirectHttps connection mode");
      connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
    }
    if (cloudConfig.vcrProxyHost != null) {
      connectionPolicy.setProxy(new HttpHost(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort));
      connectionPolicy.setHandleServiceUnavailableFromProxy(true);
    }
    // TODO: test option to set connectionPolicy.setEnableEndpointDiscovery(false);
    documentClient = new DocumentClient(azureCloudConfig.cosmosEndpoint, azureCloudConfig.cosmosKey, connectionPolicy,
        ConsistencyLevel.Session);
    cosmosDataAccessor = new CosmosDataAccessor(documentClient, azureCloudConfig, azureMetrics);
    this.retentionPeriodMs = TimeUnit.DAYS.toMillis(cloudConfig.cloudDeletedBlobRetentionDays);
    this.deadBlobsQueryLimit = cloudConfig.cloudBlobCompactionQueryLimit;
    logger.info("Created Azure destination");
  }

  /**
   * Test constructor.
   * @param azureAccount the {@link CloudStorageAccount} to use.
   * @param documentClient the {@link DocumentClient} to use.
   * @param cosmosCollectionLink the CosmosDB collection link to use.
   * @param clusterName the name of the Ambry cluster.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @throws CloudStorageException if the destination could not be created.
   */
  AzureCloudDestination(CloudStorageAccount azureAccount, DocumentClient documentClient, String cosmosCollectionLink,
      String clusterName, AzureMetrics azureMetrics) {
    this.azureAccount = azureAccount;
    this.documentClient = documentClient;
    this.azureMetrics = azureMetrics;
    this.clusterName = clusterName;
    this.retentionPeriodMs = TimeUnit.DAYS.toMillis(CloudConfig.DEFAULT_RETENTION_DAYS);
    this.deadBlobsQueryLimit = CloudConfig.DEFAULT_COMPACTION_QUERY_LIMIT;

    // Create a blob client to interact with Blob storage
    azureBlobClient = azureAccount.createCloudBlobClient();
    cosmosDataAccessor =
        new CosmosDataAccessor(documentClient, cosmosCollectionLink, AzureCloudConfig.DEFAULT_COSMOS_MAX_RETRIES,
            azureMetrics);
  }

  /**
   * Test connectivity to Azure endpoints
   */
  void testAzureConnectivity() {
    testStorageConnectivity();
    cosmosDataAccessor.testConnectivity();
  }

  /**
   * Test connectivity to Azure Blob Storage
   */
  void testStorageConnectivity() {
    try {
      // Turn on verbose logging just for this call
      blobOpContext.setLoggingEnabled(true);
      blobOpContext.setLogger(logger);
      azureBlobClient.getContainerReference("partition-0").exists(null, null, blobOpContext);
      logger.info("Blob storage connection test succeeded.");
    } catch (StorageException | URISyntaxException ex) {
      throw new IllegalStateException("Blob storage connection test failed", ex);
    } finally {
      // Disable logging for future requests
      blobOpContext.setLoggingEnabled(false);
      blobOpContext.setLogger(null);
    }
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws CloudStorageException {

    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(blobInputStream, "Input stream cannot be null");
    try {
      Timer.Context backupTimer = azureMetrics.backupSuccessLatency.time();
      boolean uploaded = uploadIfNotExists(blobId, inputLength, cloudBlobMetadata, blobInputStream);
      // Note: if uploaded is false, still attempt to insert the metadata document
      // since it is possible that a previous attempt failed.

      cosmosDataAccessor.upsertMetadata(cloudBlobMetadata);
      backupTimer.stop();
      if (uploaded) {
        azureMetrics.backupSuccessByteRate.mark(inputLength);
      }
      return uploaded;
    } catch (URISyntaxException | StorageException | DocumentClientException | IOException e) {
      azureMetrics.backupErrorCount.inc();
      updateErrorMetrics(e);
      throw new CloudStorageException("Error uploading blob " + blobId, e);
    }
  }

  /**
   * Upload the blob to Azure storage if it does not already exist in the designated container.
   * @param blobId the blobId to upload
   * @param inputLength the input stream length, if known (-1 if not)
   * @param cloudBlobMetadata the blob metadata
   * @param blobInputStream the input stream
   * @return {@code true} if the upload was successful, {@code false} if the blob already exists.
   * @throws StorageException
   * @throws URISyntaxException
   * @throws IOException
   */
  private boolean uploadIfNotExists(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws StorageException, URISyntaxException, IOException {
    BlobRequestOptions options = null; // may want to set BlobEncryptionPolicy here
    AccessCondition condition = AccessCondition.generateIfNoneMatchCondition("*");
    azureMetrics.blobUploadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobUploadTime.time();
    try {
      CloudBlockBlob azureBlob = getAzureBlobReference(blobId, true);
      cloudBlobMetadata.setCloudBlobName(getAzureBlobName(blobId));
      azureBlob.setMetadata(getMetadataMap(cloudBlobMetadata));
      azureBlob.upload(blobInputStream, inputLength, condition, options, blobOpContext);
      logger.debug("Uploaded blob {} to Azure container {}.", blobId, getContainer(blobId, true).getName());
      azureMetrics.blobUploadSuccessCount.inc();
      return true;
    } catch (StorageException sex) {
      if (sex.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        // The blob already exists
        logger.debug("Skipped upload of existing blob {}.", blobId);
        azureMetrics.blobUploadConflictCount.inc();
        return false;
      } else {
        throw sex;
      }
    } finally {
      storageTimer.stop();
    }
  }

  @Override
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException {
    azureMetrics.blobDownloadRequestCount.inc();
    Timer.Context storageTimer = azureMetrics.blobDownloadTime.time();
    try {
      CloudBlockBlob azureBlob = getAzureBlobReference(blobId, false);
      azureBlob.download(outputStream);
      azureMetrics.blobDownloadSuccessCount.inc();
    } catch (URISyntaxException | StorageException e) {
      azureMetrics.blobDownloadErrorCount.inc();
      throw new CloudStorageException("Error downloading blob " + blobId, e);
    } finally {
      storageTimer.stop();
    }
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime) throws CloudStorageException {
    return updateBlobMetadata(blobId, CloudBlobMetadata.FIELD_DELETION_TIME, deletionTime);
  }

  @Override
  public boolean updateBlobExpiration(BlobId blobId, long expirationTime) throws CloudStorageException {
    return updateBlobMetadata(blobId, CloudBlobMetadata.FIELD_EXPIRATION_TIME, expirationTime);
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException {
    Objects.requireNonNull(blobIds, "blobIds cannot be null");
    if (blobIds.isEmpty()) {
      return Collections.emptyMap();
    }

    // CosmosDB has query size limit of 256k chars.
    // Break list into chunks if necessary to avoid overflow.
    List<CloudBlobMetadata> metadataList;
    if (blobIds.size() > ID_QUERY_BATCH_SIZE) {
      metadataList = new ArrayList<>();
      for (int j = 0; j < blobIds.size() / ID_QUERY_BATCH_SIZE + 1; j++) {
        int start = j * ID_QUERY_BATCH_SIZE;
        if (start >= blobIds.size()) {
          break;
        }
        int end = Math.min((j + 1) * ID_QUERY_BATCH_SIZE, blobIds.size());
        List<BlobId> someBlobIds = blobIds.subList(start, end);
        metadataList.addAll(getBlobMetadataChunked(someBlobIds));
      }
    } else {
      metadataList = getBlobMetadataChunked(blobIds);
    }

    return metadataList.stream().collect(Collectors.toMap(m -> m.getId(), Function.identity()));
  }

  private List<CloudBlobMetadata> getBlobMetadataChunked(List<BlobId> blobIds) throws CloudStorageException {
    if (blobIds.isEmpty() || blobIds.size() > ID_QUERY_BATCH_SIZE) {
      throw new IllegalArgumentException("Invalid input list size: " + blobIds.size());
    }
    String quotedBlobIds =
        String.join(",", blobIds.stream().map(s -> '"' + s.getID() + '"').collect(Collectors.toList()));
    String query = String.format(BATCH_ID_QUERY_TEMPLATE, quotedBlobIds);
    String partitionPath = blobIds.get(0).getPartition().toPathString();
    try {
      return cosmosDataAccessor.queryMetadata(partitionPath, new SqlQuerySpec(query),
          azureMetrics.missingKeysQueryTime);
    } catch (DocumentClientException dex) {
      throw new CloudStorageException("Failed to query blob metadata for partition " + partitionPath, dex);
    }
  }

  @Override
  public List<CloudBlobMetadata> getDeadBlobs(String partitionPath) throws CloudStorageException {
    long now = System.currentTimeMillis();
    long retentionThreshold = now - retentionPeriodMs;
    SqlQuerySpec deadBlobsQuery = new SqlQuerySpec(DEAD_BLOBS_QUERY_TEMPLATE,
        new SqlParameterCollection(new SqlParameter(LIMIT_PARAM, deadBlobsQueryLimit),
            new SqlParameter(THRESHOLD_PARAM, retentionThreshold)));
    try {
      return cosmosDataAccessor.queryMetadata(partitionPath, deadBlobsQuery, azureMetrics.deadBlobsQueryTime);
    } catch (DocumentClientException dex) {
      throw new CloudStorageException("Failed to query dead blobs for partition " + partitionPath, dex);
    }
  }

  @Override
  public List<CloudBlobMetadata> findEntriesSince(String partitionPath, CloudFindToken findToken,
      long maxTotalSizeOfEntries) throws CloudStorageException {
    SqlQuerySpec entriesSinceQuery = new SqlQuerySpec(ENTRIES_SINCE_QUERY_TEMPLATE,
        new SqlParameterCollection(new SqlParameter(LIMIT_PARAM, findSinceQueryLimit),
            new SqlParameter(TIME_SINCE_PARAM, findToken.getLastUpdateTime())));
    try {
      List<CloudBlobMetadata> queryResults =
          cosmosDataAccessor.queryMetadata(partitionPath, entriesSinceQuery, azureMetrics.findSinceQueryTime);
      if (queryResults.isEmpty()) {
        return queryResults;
      }
      if (queryResults.get(0).getLastUpdateTime() == findToken.getLastUpdateTime()) {
        filterOutLastReadBlobs(queryResults, findToken.getLastUpdateTimeReadBlobIds(), findToken.getLastUpdateTime());
      }
      return CloudBlobMetadata.capMetadataListBySize(queryResults, maxTotalSizeOfEntries);
    } catch (DocumentClientException dex) {
      throw new CloudStorageException("Failed to query blobs for partition " + partitionPath, dex);
    }
  }

  /**
   * Filter out {@link CloudBlobMetadata} objects from lastUpdateTime ordered {@code cloudBlobMetadataList} whose
   * lastUpdateTime is {@code lastUpdateTime} and id is in {@code lastReadBlobIds}.
   * @param cloudBlobMetadataList list of {@link CloudBlobMetadata} objects to filter out from.
   * @param lastReadBlobIds set if blobIds which need to be filtered out.
   * @param lastUpdateTime lastUpdateTime of the blobIds to filter out.
   */
  private void filterOutLastReadBlobs(List<CloudBlobMetadata> cloudBlobMetadataList, Set<String> lastReadBlobIds,
      long lastUpdateTime) {
    ListIterator<CloudBlobMetadata> iterator = cloudBlobMetadataList.listIterator();
    int numRemovedBlobs = 0;
    while(iterator.hasNext()) {
      CloudBlobMetadata cloudBlobMetadata = iterator.next();
      if(numRemovedBlobs == lastReadBlobIds.size() || cloudBlobMetadata.getLastUpdateTime() > lastUpdateTime) {
        break;
      }
      if (lastReadBlobIds.contains(cloudBlobMetadata.getId())) {
        iterator.remove();
        numRemovedBlobs++;
      }
    }
  }

  /**
   * Update the metadata for the specified blob.
   * @param blobId The {@link BlobId} to update.
   * @param fieldName The metadata field to modify.
   * @param value The new value.
   * @return {@code true} if the udpate succeeded, {@code false} if the metadata record was not found.
   * @throws DocumentClientException
   */
  private boolean updateBlobMetadata(BlobId blobId, String fieldName, Object value) throws CloudStorageException {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(fieldName, "Field name cannot be null");

    // We update the blob metadata value in two places:
    // 1) the CosmosDB metadata collection
    // 2) the blob storage entry metadata (to enable rebuilding the database)

    try {
      CloudBlockBlob azureBlob = getAzureBlobReference(blobId, false);

      if (!azureBlob.exists(null, null, blobOpContext)) {
        logger.debug("Blob {} not found in Azure container {}.", blobId, getContainer(blobId, false).getName());
        return false;
      }

      Timer.Context storageTimer = azureMetrics.blobUpdateTime.time();
      try {
        azureBlob.downloadAttributes(null, null, blobOpContext); // Makes sure we have latest
        // Update only if value has changed
        String textValue = String.valueOf(value);
        if (!textValue.equals(azureBlob.getMetadata().get(fieldName))) {
          azureBlob.getMetadata().put(fieldName, textValue);
          azureBlob.uploadMetadata(null, null, blobOpContext);
        }
      } finally {
        storageTimer.stop();
      }

      ResourceResponse<Document> response = cosmosDataAccessor.readMetadata(blobId);
      //CloudBlobMetadata blobMetadata = response.getResource().toObject(CloudBlobMetadata.class);
      Document doc = response.getResource();
      if (doc == null) {
        logger.warn("Blob metadata record not found: {}", blobId.getID());
        return false;
      }
      // Update only if value has changed
      if (!value.equals(doc.get(fieldName))) {
        doc.set(fieldName, value);
        cosmosDataAccessor.replaceMetadata(blobId, doc);
      }
      logger.debug("Updated blob {} metadata set {} to {}.", blobId, fieldName, value);
      azureMetrics.blobUpdatedCount.inc();
      return true;
    } catch (URISyntaxException | StorageException | DocumentClientException e) {
      azureMetrics.blobUpdateErrorCount.inc();
      updateErrorMetrics(e);
      throw new CloudStorageException("Error updating blob metadata: " + blobId, e);
    }
  }

  @Override
  public boolean purgeBlob(CloudBlobMetadata blobMetadata) throws CloudStorageException {
    String blobId = blobMetadata.getId();
    String blobFileName = blobMetadata.getCloudBlobName();
    String partitionPath = blobMetadata.getPartitionId();
    String containerName = getAzureContainerName(partitionPath);
    azureMetrics.blobDeleteRequestCount.inc();
    Timer.Context deleteTimer = azureMetrics.blobDeletionTime.time();
    try {
      // delete blob from storage
      CloudBlobContainer azureContainer = azureBlobClient.getContainerReference(containerName);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobFileName);
      boolean deletionDone = azureBlob.deleteIfExists(DeleteSnapshotsOption.NONE, null, null, blobOpContext);

      // Delete the document too
      try {
        cosmosDataAccessor.deleteMetadata(blobMetadata);
        deletionDone = true;
        logger.debug("Purged blob {} from partition {}.", blobId, partitionPath);
      } catch (DocumentClientException dex) {
        if (dex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          logger.warn("Could not find metadata for blob {} to delete", blobId);
        } else {
          throw dex;
        }
      }
      azureMetrics.blobDeletedCount.inc(deletionDone ? 1 : 0);
      return deletionDone;
    } catch (Exception e) {
      azureMetrics.blobDeleteErrorCount.inc();
      String error = (e instanceof DocumentClientException) ? "Failed to delete metadata document for blob " + blobId
          : "Failed to delete blob " + blobId + ", storage path: " + containerName + "/" + blobFileName;
      throw new CloudStorageException(error, e);
    } finally {
      deleteTimer.stop();
    }
  }

  @Override
  public int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws CloudStorageException {
    int numPurged = 0;
    for (CloudBlobMetadata blobMetadata : blobMetadataList) {
      if (purgeBlob(blobMetadata)) {
        numPurged++;
      }
    }
    logger.info("Purged {} blobs", numPurged);
    return numPurged;
  }

  @Override
  public boolean doesBlobExist(BlobId blobId) throws CloudStorageException {
    try {
      CloudBlockBlob azureBlob = getAzureBlobReference(blobId, false);
      return azureBlob.exists(null, null, blobOpContext);
    } catch (URISyntaxException | StorageException e) {
      throw new CloudStorageException("Could not check existence of blob: " + blobId, e);
    }
  }

  /**
   * Get an Azure container to place the specified {@link BlobId}.
   * @param blobId the {@link BlobId} that needs a container.
   * @param autoCreate flag indicating whether to create the container if it does not exist.
   * @return the created {@link CloudBlobContainer}.
   * @throws Exception
   */
  private CloudBlobContainer getContainer(BlobId blobId, boolean autoCreate)
      throws URISyntaxException, StorageException {
    String containerName = getAzureContainerName(blobId.getPartition().toPathString());
    CloudBlobContainer azureContainer = azureBlobClient.getContainerReference(containerName);
    if (autoCreate) {
      ensureCreated(azureContainer);
    }
    return azureContainer;
  }

  /**
   * Utility method to ensure that the requested container exists in the storage account.
   * @param azureContainer the container that must exist.
   * @throws StorageException if the operation fails.
   */
  private void ensureCreated(CloudBlobContainer azureContainer) throws StorageException {
    String containerName = azureContainer.getName();
    if (!knownContainers.contains(containerName)) {
      try {
        if (azureContainer.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
            blobOpContext)) {
          logger.info("Created container {}", containerName);
        }
      } catch (StorageException ex) {
        logger.error("Failed to create container {}", containerName);
        throw ex;
      }
      knownContainers.add(containerName);
    }
  }

  @Override
  public void persistTokens(String partitionPath, String tokenFileName, InputStream inputStream)
      throws CloudStorageException {
    // Path is partitionId path string
    // Write to container partitionPath, blob filename "replicaTokens"
    try {
      String containerName = getAzureContainerName(partitionPath);
      CloudBlobContainer azureContainer = azureBlobClient.getContainerReference(containerName);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(tokenFileName);
      azureBlob.upload(inputStream, -1, null, null, blobOpContext);
    } catch (IOException | URISyntaxException | StorageException e) {
      throw new CloudStorageException("Could not persist token: " + partitionPath, e);
    }
  }

  @Override
  public boolean retrieveTokens(String partitionPath, String tokenFileName, OutputStream outputStream)
      throws CloudStorageException {
    try {
      String containerName = getAzureContainerName(partitionPath);
      CloudBlobContainer azureContainer = azureBlobClient.getContainerReference(containerName);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(tokenFileName);
      if (!azureBlob.exists(null, null, blobOpContext)) {
        return false;
      }
      azureBlob.download(outputStream, null, null, blobOpContext);
      return true;
    } catch (URISyntaxException | StorageException e) {
      throw new CloudStorageException("Could not retrieve token: " + partitionPath, e);
    }
  }

  /**
   * Get the azure blob reference for blobid.
   * @param blobId id of the blob for which {@code CloudBlockBlob} reference is asked for.
   * @param autoCreateContainer flag indicating whether to create the container if it does not exist.
   * @return {@code CloudBlockBlob} reference.
   * @throws StorageException if storage service error occured.
   * @throws URISyntaxException if resource name or uri is invalid.
   */
  private CloudBlockBlob getAzureBlobReference(BlobId blobId, boolean autoCreateContainer)
      throws StorageException, URISyntaxException {
    CloudBlobContainer azureContainer = getContainer(blobId, autoCreateContainer);
    String azureBlobName = getAzureBlobName(blobId);
    return azureContainer.getBlockBlobReference(azureBlobName);
  }

  /**
   * @return the name of the Azure storage container where blobs in the specified partition are stored.
   * @param partitionPath the lexical path of the Ambry partition.
   */
  String getAzureContainerName(String partitionPath) {
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
  String getAzureBlobName(BlobId blobId) {
    // Use the last four chars as prefix to assist in Azure sharding, since beginning of blobId has little variation.
    String blobIdStr = blobId.getID();
    return blobIdStr.substring(blobIdStr.length() - 4) + SEPARATOR + blobIdStr;
  }

  /**
   * Visible for test.
   * @return the CosmosDB DocumentClient
   */
  DocumentClient getDocumentClient() {
    return documentClient;
  }

  /**
   * Visible for test.
   * @return the {@link CosmosDataAccessor}
   */
  CosmosDataAccessor getCosmosDataAccessor() {
    return cosmosDataAccessor;
  }

  /**
   * Visible for test.
   * @return the blob storage operation context.
   */
  OperationContext getBlobOpContext() {
    return blobOpContext;
  }

  /**
   * Update the appropriate error metrics corresponding to the thrown exception.
   * @param e the exception thrown.
   */
  private void updateErrorMetrics(Exception e) {
    if (e instanceof DocumentClientException) {
      azureMetrics.documentErrorCount.inc();
    } else {
      azureMetrics.storageErrorCount.inc();
    }
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
    map.put(CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN, String.valueOf(cloudBlobMetadata.getLastUpdateTime()));
    return map;
  }
}
