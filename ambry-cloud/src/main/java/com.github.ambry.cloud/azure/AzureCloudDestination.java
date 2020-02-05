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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudFindToken;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy.BlobLayout;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.RetryOptions;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  private static final String DEAD_BLOBS_QUERY_TEMPLATE =
      "SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE (c." + CloudBlobMetadata.FIELD_DELETION_TIME + " BETWEEN 1 AND "
          + THRESHOLD_PARAM + ")" + " OR (c." + CloudBlobMetadata.FIELD_EXPIRATION_TIME + " BETWEEN 1 AND "
          + THRESHOLD_PARAM + ")" + " ORDER BY c." + CloudBlobMetadata.FIELD_UPLOAD_TIME + " ASC";
  // Note: ideally would like to order by uploadTime and id, but Cosmos doesn't allow without composite index.
  // It is unlikely (but not impossible) for two blobs in same partition to have the same uploadTime (would have to
  // be multiple VCR's uploading same partition).  We track the lastBlobId in the CloudFindToken and skip it if
  // is returned in successive queries.
  private static final String ENTRIES_SINCE_QUERY_TEMPLATE =
      "SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " >= "
          + TIME_SINCE_PARAM + " ORDER BY c." + CosmosDataAccessor.COSMOS_LAST_UPDATED_COLUMN + " ASC";
  private static final String SEPARATOR = "-";
  private static final int findSinceQueryLimit = 1000;
  private final AzureBlobDataAccessor azureBlobDataAccessor;
  private final AzureBlobLayoutStrategy blobLayoutStrategy;
  private final AsyncDocumentClient asyncDocumentClient;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final AzureMetrics azureMetrics;
  private final String clusterName;
  private final long retentionPeriodMs;
  private final int deadBlobsQueryLimit;

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
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
    this.azureBlobDataAccessor =
        new AzureBlobDataAccessor(cloudConfig, azureCloudConfig, blobLayoutStrategy, azureMetrics);
    this.clusterName = clusterName;
    // Set up CosmosDB connection, including retry options and any proxy setting
    ConnectionPolicy connectionPolicy = new ConnectionPolicy();
    RetryOptions retryOptions = new RetryOptions();
    retryOptions.setMaxRetryAttemptsOnThrottledRequests(azureCloudConfig.cosmosMaxRetries);
    connectionPolicy.setRetryOptions(retryOptions);
    if (azureCloudConfig.cosmosDirectHttps) {
      logger.info("Using CosmosDB DirectHttps connection mode");
      connectionPolicy.setConnectionMode(ConnectionMode.Direct);
    }
    if (cloudConfig.vcrProxyHost != null) {
      connectionPolicy.setProxy(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
    }
    // TODO: test option to set connectionPolicy.setEnableEndpointDiscovery(false);
    asyncDocumentClient = new AsyncDocumentClient.Builder().withServiceEndpoint(azureCloudConfig.cosmosEndpoint)
        .withMasterKeyOrResourceToken(azureCloudConfig.cosmosKey)
        .withConnectionPolicy(connectionPolicy)
        .withConsistencyLevel(ConsistencyLevel.Session)
        .build();
    cosmosDataAccessor = new CosmosDataAccessor(asyncDocumentClient, azureCloudConfig, azureMetrics);
    this.retentionPeriodMs = TimeUnit.DAYS.toMillis(cloudConfig.cloudDeletedBlobRetentionDays);
    this.deadBlobsQueryLimit = cloudConfig.cloudBlobCompactionQueryLimit;
    logger.info("Created Azure destination");
  }

  /**
   * Test constructor.
   * @param storageClient the {@link BlobServiceClient} to use.
   * @param asyncDocumentClient the {@link AsyncDocumentClient} to use.
   * @param cosmosCollectionLink the CosmosDB collection link to use.
   * @param clusterName the name of the Ambry cluster.
   * @param azureMetrics the {@link AzureMetrics} to use.
   */
  AzureCloudDestination(BlobServiceClient storageClient, BlobBatchClient blobBatchClient,
      AsyncDocumentClient asyncDocumentClient, String cosmosCollectionLink, String clusterName,
      AzureMetrics azureMetrics) {
    this.azureBlobDataAccessor = new AzureBlobDataAccessor(storageClient, blobBatchClient, clusterName, azureMetrics);
    this.asyncDocumentClient = asyncDocumentClient;
    this.azureMetrics = azureMetrics;
    this.clusterName = clusterName;
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName);
    this.retentionPeriodMs = TimeUnit.DAYS.toMillis(CloudConfig.DEFAULT_RETENTION_DAYS);
    this.deadBlobsQueryLimit = CloudConfig.DEFAULT_COMPACTION_QUERY_LIMIT;
    cosmosDataAccessor = new CosmosDataAccessor(asyncDocumentClient, cosmosCollectionLink, azureMetrics);
  }

  /**
   * Test connectivity to Azure endpoints
   */
  void testAzureConnectivity() {
    azureBlobDataAccessor.testConnectivity();
    cosmosDataAccessor.testConnectivity();
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws CloudStorageException {

    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(blobInputStream, "Input stream cannot be null");
    try {
      Timer.Context backupTimer = azureMetrics.backupSuccessLatency.time();
      boolean uploaded =
          azureBlobDataAccessor.uploadIfNotExists(blobId, inputLength, cloudBlobMetadata, blobInputStream);
      // Note: if uploaded is false, still attempt to insert the metadata document
      // since it is possible that a previous attempt failed.

      cosmosDataAccessor.upsertMetadata(cloudBlobMetadata);
      backupTimer.stop();
      if (uploaded) {
        azureMetrics.backupSuccessByteRate.mark(inputLength);
      }
      return uploaded;
    } catch (BlobStorageException | DocumentClientException | IOException e) {
      azureMetrics.backupErrorCount.inc();
      updateErrorMetrics(e);
      throw toCloudStorageException("Error uploading blob " + blobId, e);
    }
  }

  @Override
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException {
    try {
      azureBlobDataAccessor.downloadBlob(blobId, outputStream);
    } catch (BlobStorageException e) {
      updateErrorMetrics(e);
      throw toCloudStorageException("Error downloading blob " + blobId, e);
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

    // TODO: For single blob GET request, get metadata from ABS
    // CosmosDB has query size limit of 256k chars.
    // Break list into chunks if necessary to avoid overflow.
    List<CloudBlobMetadata> metadataList = new ArrayList<>();
    List<List<BlobId>> chunkedBlobIdList = Utils.partitionList(blobIds, ID_QUERY_BATCH_SIZE);
    for (List<BlobId> batchOfBlobs : chunkedBlobIdList) {
      metadataList.addAll(getBlobMetadataChunked(batchOfBlobs));
    }
    return metadataList.stream().collect(Collectors.toMap(CloudBlobMetadata::getId, Function.identity()));
  }

  private List<CloudBlobMetadata> getBlobMetadataChunked(List<BlobId> blobIds) throws CloudStorageException {
    if (blobIds.isEmpty() || blobIds.size() > ID_QUERY_BATCH_SIZE) {
      throw new IllegalArgumentException("Invalid input list size: " + blobIds.size());
    }
    String quotedBlobIds = blobIds.stream().map(s -> '"' + s.getID() + '"').collect(Collectors.joining(","));
    String query = String.format(BATCH_ID_QUERY_TEMPLATE, quotedBlobIds);
    String partitionPath = blobIds.get(0).getPartition().toPathString();
    try {
      return cosmosDataAccessor.queryMetadata(partitionPath, new SqlQuerySpec(query),
          azureMetrics.missingKeysQueryTime);
    } catch (DocumentClientException dex) {
      throw toCloudStorageException("Failed to query blob metadata for partition " + partitionPath, dex);
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
      throw toCloudStorageException("Failed to query dead blobs for partition " + partitionPath, dex);
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
      throw toCloudStorageException("Failed to query blobs for partition " + partitionPath, dex);
    }
  }

  /**
   * Getter for {@link AsyncDocumentClient} object.
   * @return {@link AsyncDocumentClient} object.
   */
  AsyncDocumentClient getAsyncDocumentClient() {
    return asyncDocumentClient;
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
    while (iterator.hasNext()) {
      CloudBlobMetadata cloudBlobMetadata = iterator.next();
      if (numRemovedBlobs == lastReadBlobIds.size() || cloudBlobMetadata.getLastUpdateTime() > lastUpdateTime) {
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
   * @throws CloudStorageException if the update fails.
   */
  private boolean updateBlobMetadata(BlobId blobId, String fieldName, Object value) throws CloudStorageException {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(fieldName, "Field name cannot be null");

    // We update the blob metadata value in two places:
    // 1) the CosmosDB metadata collection
    // 2) the blob storage entry metadata (to enable rebuilding the database)

    try {
      if (!azureBlobDataAccessor.updateBlobMetadata(blobId, fieldName, value)) {
        return false;
      }

      ResourceResponse<Document> response = cosmosDataAccessor.readMetadata(blobId);
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
    } catch (BlobStorageException | DocumentClientException e) {
      azureMetrics.blobUpdateErrorCount.inc();
      updateErrorMetrics(e);
      throw toCloudStorageException("Error updating blob metadata: " + blobId, e);
    }
  }

  @Override
  public int purgeBlobs(List<CloudBlobMetadata> blobMetadataList) throws CloudStorageException {
    if (blobMetadataList.isEmpty()) {
      return 0;
    }
    azureMetrics.blobDeleteRequestCount.inc(blobMetadataList.size());
    Timer.Context deleteTimer = azureMetrics.blobDeletionTime.time();
    try {
      List<CloudBlobMetadata> deletedBlobs = azureBlobDataAccessor.purgeBlobs(blobMetadataList);
      azureMetrics.blobDeletedCount.inc(deletedBlobs.size());
      azureMetrics.blobDeleteErrorCount.inc(blobMetadataList.size() - deletedBlobs.size());

      // Remove them from Cosmos too
      for (CloudBlobMetadata blobMetadata : deletedBlobs) {
        deleteFromCosmos(blobMetadata);
      }
      return deletedBlobs.size();
    } catch (Exception ex) {
      azureMetrics.blobDeleteErrorCount.inc(blobMetadataList.size());
      throw toCloudStorageException("Failed to purge all blobs", ex);
    } finally {
      deleteTimer.stop();
    }
  }

  /**
   * Delete a blob metadata record from Cosmos.
   * @param blobMetadata the record to delete.
   * @return {@code true} if the record was deleted, {@code false} if it was not found.
   * @throws DocumentClientException
   */
  private boolean deleteFromCosmos(CloudBlobMetadata blobMetadata) throws DocumentClientException {
    try {
      cosmosDataAccessor.deleteMetadata(blobMetadata);
      return true;
    } catch (DocumentClientException dex) {
      if (dex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        logger.warn("Could not find metadata for blob {} to delete", blobMetadata.getId());
        return false;
      } else {
        throw dex;
      }
    }
  }

  @Override
  public void persistTokens(String partitionPath, String tokenFileName, InputStream inputStream)
      throws CloudStorageException {
    // Path is partitionId path string
    // Write to container partitionPath, blob filename "replicaTokens"
    try {
      BlobLayout tokenLayout = blobLayoutStrategy.getTokenBlobLayout(partitionPath, tokenFileName);
      azureBlobDataAccessor.uploadFile(tokenLayout.containerName, tokenLayout.blobFilePath, inputStream);
    } catch (IOException | BlobStorageException e) {
      throw toCloudStorageException("Could not persist token: " + partitionPath, e);
    }
  }

  @Override
  public boolean retrieveTokens(String partitionPath, String tokenFileName, OutputStream outputStream)
      throws CloudStorageException {
    try {
      BlobLayout tokenLayout = blobLayoutStrategy.getTokenBlobLayout(partitionPath, tokenFileName);
      return azureBlobDataAccessor.downloadFile(tokenLayout.containerName, tokenLayout.blobFilePath, outputStream,
          false);
    } catch (BlobStorageException e) {
      throw toCloudStorageException("Could not retrieve token: " + partitionPath, e);
    }
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
   * @return the {@link AzureBlobDataAccessor}
   */
  AzureBlobDataAccessor getAzureBlobDataAccessor() {
    return azureBlobDataAccessor;
  }

  /**
   * Construct a {@link CloudStorageException} from a root cause exception.
   * @param message the exception message.
   * @param e the root cause exception.
   * @return the {@link CloudStorageException}.
   */
  private static final CloudStorageException toCloudStorageException(String message, Exception e) {
    boolean isRetryable = false;
    Long retryDelayMs = null;
    int statusCode = -1;
    if (e instanceof BlobStorageException) {
      statusCode = ((BlobStorageException) e).getStatusCode();
    } else if (e instanceof DocumentClientException) {
      statusCode = ((DocumentClientException) e).getStatusCode();
      retryDelayMs = ((DocumentClientException) e).getRetryAfterInMilliseconds();
    }
    // Everything is retryable except NOT_FOUND
    isRetryable = (statusCode != HttpConstants.StatusCodes.NOTFOUND);
    return new CloudStorageException(message, e, isRetryable, retryDelayMs);
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
}
