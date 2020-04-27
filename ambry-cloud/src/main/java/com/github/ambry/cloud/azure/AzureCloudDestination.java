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
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy.BlobLayout;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link CloudDestination} that interacts with Azure Blob Storage service.
 */
class AzureCloudDestination implements CloudDestination {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestination.class);
  private static final String BATCH_ID_QUERY_TEMPLATE = "SELECT * FROM c WHERE c.id IN (%s)";
  private final AzureBlobDataAccessor azureBlobDataAccessor;
  private final AzureBlobLayoutStrategy blobLayoutStrategy;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final AzureReplicationFeed azureReplicationFeed;
  private final AzureMetrics azureMetrics;
  private final int queryBatchSize;
  private final boolean isVcr;

  /**
   * Construct an Azure cloud destination from config properties.
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param azureCloudConfig the {@link AzureCloudConfig} to use.
   * @param clusterName the name of the Ambry cluster.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @param azureReplicationFeedType {@link AzureReplicationFeed.FeedType} to use for replication from Azure.
   */
  AzureCloudDestination(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, String clusterName,
      AzureMetrics azureMetrics, AzureReplicationFeed.FeedType azureReplicationFeedType) {
    this.azureMetrics = azureMetrics;
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
    this.azureBlobDataAccessor =
        new AzureBlobDataAccessor(cloudConfig, azureCloudConfig, blobLayoutStrategy, azureMetrics);
    this.queryBatchSize = azureCloudConfig.cosmosQueryBatchSize;
    this.cosmosDataAccessor = new CosmosDataAccessor(cloudConfig, azureCloudConfig, azureMetrics);
    this.azureReplicationFeed =
        getReplicationFeedObj(azureReplicationFeedType, cosmosDataAccessor, azureMetrics, queryBatchSize);
    isVcr = cloudConfig.cloudIsVcr;
    logger.info("Created Azure destination");
  }

  /**
   * Test constructor.
   * @param storageClient the {@link BlobServiceClient} to use.
   * @param asyncDocumentClient the {@link AsyncDocumentClient} to use.
   * @param cosmosCollectionLink the CosmosDB collection link to use.
   * @param clusterName the name of the Ambry cluster.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @param azureReplicationFeedType the {@link AzureReplicationFeed.FeedType} to use for replication from Azure.
   * @param isVcr whether this instance is a VCR.
   */
  AzureCloudDestination(BlobServiceClient storageClient, BlobBatchClient blobBatchClient,
      AsyncDocumentClient asyncDocumentClient, String cosmosCollectionLink, String clusterName,
      AzureMetrics azureMetrics, AzureReplicationFeed.FeedType azureReplicationFeedType, boolean isVcr) {
    this.azureMetrics = azureMetrics;
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName);
    this.azureBlobDataAccessor = new AzureBlobDataAccessor(storageClient, blobBatchClient, clusterName, azureMetrics);
    this.queryBatchSize = AzureCloudConfig.DEFAULT_QUERY_BATCH_SIZE;
    this.cosmosDataAccessor = new CosmosDataAccessor(asyncDocumentClient, cosmosCollectionLink, azureMetrics);
    this.azureReplicationFeed =
        getReplicationFeedObj(azureReplicationFeedType, cosmosDataAccessor, azureMetrics, queryBatchSize);
    this.isVcr = isVcr;
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
    } catch (Exception e) {
      azureMetrics.backupErrorCount.inc();
      throw toCloudStorageException("Error uploading blob " + blobId, e);
    }
  }

  @Override
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException {
    try {
      azureBlobDataAccessor.downloadBlob(blobId, outputStream);
    } catch (Exception e) {
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

    // For single blob GET request (isVcr = false), get metadata from ABS instead of Cosmos
    // Note: findMissingKeys (isVcr = true) needs to query Cosmos regardless, because a partial failure in a
    // previous upload could have resulted in a missing record in Cosmos; the findMissingKeys result
    // needs to include that store key to replay the upload.
    if (!isVcr && blobIds.size() == 1) {
      CloudBlobMetadata metadata = azureBlobDataAccessor.getBlobMetadata(blobIds.get(0));
      return metadata == null ? Collections.emptyMap() : Collections.singletonMap(metadata.getId(), metadata);
    }

    // CosmosDB has query size limit of 256k chars.
    // Break list into chunks if necessary to avoid overflow.
    List<CloudBlobMetadata> metadataList = new ArrayList<>();
    List<List<BlobId>> chunkedBlobIdList = Utils.partitionList(blobIds, queryBatchSize);
    for (List<BlobId> batchOfBlobs : chunkedBlobIdList) {
      metadataList.addAll(getBlobMetadataChunked(batchOfBlobs));
    }
    return metadataList.stream().collect(Collectors.toMap(CloudBlobMetadata::getId, Function.identity()));
  }

  @Override
  public void close() throws IOException {
    azureReplicationFeed.close();
  }

  /**
   * Get metadata for specified list of blobs.
   * @param blobIds {@link List} of {@link BlobId}s to get metadata of.
   * @return {@link List} of {@link CloudBlobMetadata} for the blobs list.
   * @throws CloudStorageException
   */
  private List<CloudBlobMetadata> getBlobMetadataChunked(List<BlobId> blobIds) throws CloudStorageException {
    if (blobIds.isEmpty() || blobIds.size() > queryBatchSize) {
      throw new IllegalArgumentException("Invalid input list size: " + blobIds.size());
    }
    String quotedBlobIds = blobIds.stream().map(s -> '"' + s.getID() + '"').collect(Collectors.joining(","));
    String query = String.format(BATCH_ID_QUERY_TEMPLATE, quotedBlobIds);
    String partitionPath = blobIds.get(0).getPartition().toPathString();
    try {
      return cosmosDataAccessor.queryMetadata(partitionPath, query, azureMetrics.missingKeysQueryTime);
    } catch (DocumentClientException dex) {
      throw toCloudStorageException(
          "Failed to query metadata for " + blobIds.size() + " blobs in partition " + partitionPath, dex);
    }
  }

  @Override
  public List<CloudBlobMetadata> getDeletedBlobs(String partitionPath, long startTime, long endTime, int maxEntries)
      throws CloudStorageException {
    try {
      return cosmosDataAccessor.getDeadBlobs(partitionPath, CloudBlobMetadata.FIELD_DELETION_TIME, startTime, endTime,
          maxEntries);
    } catch (DocumentClientException dex) {
      throw toCloudStorageException("Failed to query deleted blobs for partition " + partitionPath, dex);
    }
  }

  @Override
  public List<CloudBlobMetadata> getExpiredBlobs(String partitionPath, long startTime, long endTime, int maxEntries)
      throws CloudStorageException {
    try {
      return cosmosDataAccessor.getDeadBlobs(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME, startTime, endTime,
          maxEntries);
    } catch (DocumentClientException dex) {
      throw toCloudStorageException("Failed to query expired blobs for partition " + partitionPath, dex);
    }
  }

  @Override
  public FindResult findEntriesSince(String partitionPath, FindToken findToken, long maxTotalSizeOfEntries)
      throws CloudStorageException {
    try {
      return azureReplicationFeed.getNextEntriesAndUpdatedToken(findToken, maxTotalSizeOfEntries, partitionPath);
    } catch (DocumentClientException dex) {
      throw toCloudStorageException("Failed to query blobs for partition " + partitionPath, dex);
    }
  }

  /**
   * Update the metadata for the specified blob.
   * @param blobId The {@link BlobId} to update.
   * @param fieldName The metadata field to modify.
   * @param value The new value.
   * @return {@code true} if the update succeeded, {@code false} if no update was needed.
   * @throws CloudStorageException if the update fails.
   */
  private boolean updateBlobMetadata(BlobId blobId, String fieldName, Object value) throws CloudStorageException {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(fieldName, "Field name cannot be null");

    // We update the blob metadata value in two places:
    // 1) the blob storage entry metadata (so GET's can be served entirely from ABS)
    // 2) the CosmosDB metadata collection
    try {
      boolean updatedStorage = false;
      Map<String, String> metadataMap = null;
      try {
        AzureBlobDataAccessor.UpdateResponse updateResponse =
            azureBlobDataAccessor.updateBlobMetadata(blobId, fieldName, value);
        // Note: if blob does not exist will throw exception with NOT_FOUND status
        metadataMap = updateResponse.metadata;
        updatedStorage = updateResponse.wasUpdated;
      } catch (BlobStorageException bex) {
        if (bex.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
          // There is a corner case where compaction of this partition hit partial failure leaving the record
          // in Cosmos but not ABS.  If that happens, a late arriving update event can get stuck in a loop
          // where findMissingKeys says the blob exists (because Cosmos has it), but the subsequent update
          // attempt fails.  So we check for that case here.
          CloudBlobMetadata cosmosMetadata = cosmosDataAccessor.getMetadataOrNull(blobId);
          if (cosmosMetadata != null) {
            if (cosmosMetadata.isDeletedOrExpired()) {
              logger.warn("Inconsistency: Cosmos contains record for inactive blob {}, removing it.", blobId.getID());
              cosmosDataAccessor.deleteMetadata(cosmosMetadata);
              azureMetrics.blobUpdateRecoverCount.inc();
              return false;
            } else {
              // If the blob is still active but ABS does not have it, we are in deeper trouble.
              logger.error("Inconsistency: Cosmos contains record for active blob {} that is missing from ABS!", blobId.getID());
              throw bex;
            }
          } else {
            // Blob is not in Cosmos either, so simple NOT_FOUND error.
            throw bex;
          }
        } else {
          // Other type of error, just throw.
          throw bex;
        }
      }

      // Note: even if nothing changed in blob storage, still attempt to update Cosmos since this could be a retry
      // of a request where ABS was updated but Cosmos update failed.
      boolean updatedCosmos = false;
      try {
        ResourceResponse<Document> response = cosmosDataAccessor.updateMetadata(blobId, fieldName, value);
        updatedCosmos = response != null;
      } catch (DocumentClientException dex) {
        if (dex.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND) {
          // blob exists in ABS but not Cosmos - inconsistent state
          // Recover by inserting the updated map into cosmos
          cosmosDataAccessor.upsertMetadata(CloudBlobMetadata.fromMap(metadataMap));
          azureMetrics.blobUpdateRecoverCount.inc();
          updatedCosmos = true;
        } else {
          throw dex;
        }
      }

      if (updatedStorage || updatedCosmos) {
        logger.debug("Updated blob {} metadata set {} to {}.", blobId, fieldName, value);
        azureMetrics.blobUpdatedCount.inc();
        return true;
      } else {
        logger.debug("Blob {} already has {} = {} in ABS and Cosmos", blobId, fieldName, value);
        return false;
      }
    } catch (Exception e) {
      azureMetrics.blobUpdateErrorCount.inc();
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
        // Can happen if this is a retry
        logger.debug("Could not find metadata for blob {} to delete", blobMetadata.getId());
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
   * Return {@code queryBatchSize}
   * @return value of {@code queryBatchSize}
   */
  public int getQueryBatchSize() {
    return queryBatchSize;
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
   * Visible for test
   * @return the {@link AzureMetrics}
   */
  AzureMetrics getAzureMetrics() {
    return azureMetrics;
  }

  /**
   * Construct a {@link CloudStorageException} from a root cause exception.
   * @param message the exception message.
   * @param e the root cause exception.
   * @return the {@link CloudStorageException}.
   */
  private CloudStorageException toCloudStorageException(String message, Exception e) {
    Long retryDelayMs = null;
    int statusCode = -1;
    if (e instanceof BlobStorageException) {
      azureMetrics.storageErrorCount.inc();
      statusCode = ((BlobStorageException) e).getStatusCode();
    } else if (e instanceof DocumentClientException) {
      azureMetrics.documentErrorCount.inc();
      statusCode = ((DocumentClientException) e).getStatusCode();
      retryDelayMs = ((DocumentClientException) e).getRetryAfterInMilliseconds();
    } else {
      // Note: catch-all since ABS can throw things like IOException, IllegalStateException
      azureMetrics.storageErrorCount.inc();
      statusCode = StatusCodes.INTERNAL_SERVER_ERROR;
    }
    // Everything is retryable except NOT_FOUND
    boolean isRetryable = (statusCode != StatusCodes.NOTFOUND);
    return new CloudStorageException(message, e, statusCode, isRetryable, retryDelayMs);
  }

  /**
   * Return corresponding {@link AzureReplicationFeed} object for specified {@link AzureReplicationFeed.FeedType}.
   * @param azureReplicationFeedType replication feed type.
   * @param cosmosDataAccessor {@link CosmosDataAccessor} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param queryBatchSize batch size of query for replication feed.
   * @return {@link AzureReplicationFeed} object.
   */
  private static AzureReplicationFeed getReplicationFeedObj(AzureReplicationFeed.FeedType azureReplicationFeedType,
      CosmosDataAccessor cosmosDataAccessor, AzureMetrics azureMetrics, int queryBatchSize) {
    switch (azureReplicationFeedType) {
      case COSMOS_CHANGE_FEED:
        return new CosmosChangeFeedBasedReplicationFeed(cosmosDataAccessor, azureMetrics, queryBatchSize);
      case COSMOS_UPDATE_TIME:
        return new CosmosUpdateTimeBasedReplicationFeed(cosmosDataAccessor, azureMetrics, queryBatchSize);
      default:
        throw new IllegalArgumentException(
            String.format("Unknown cloud replication feed type: %s", azureReplicationFeedType));
    }
  }
}
