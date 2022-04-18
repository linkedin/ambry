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

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.account.Container;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudContainerCompactor;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.CloudUpdateValidator;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy.BlobLayout;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.FutureUtils;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.StoreException;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.azure.cosmos.implementation.HttpConstants.StatusCodes.*;


/**
 * Implementation of {@link CloudDestination} that interacts with Azure Blob Storage service.
 */
class AzureCloudDestination implements CloudDestination {

  static final String CHECKPOINT_CONTAINER = "compaction-checkpoints";
  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestination.class);
  private static final String BATCH_ID_QUERY_TEMPLATE = "SELECT * FROM c WHERE c.id IN (%s)";
  private final AzureBlobDataAccessor azureBlobDataAccessor;
  private final AzureBlobLayoutStrategy blobLayoutStrategy;
  private final CosmosDataAccessor cosmosDataAccessor;
  private final AzureStorageCompactor azureStorageCompactor;
  private final AzureContainerCompactor azureContainerCompactor;
  private final AzureReplicationFeed azureReplicationFeed;
  private final AzureMetrics azureMetrics;
  private final int queryBatchSize;
  private final boolean isVcr;
  private final CloudConfig cloudConfig;
  private final ClusterMap clusterMap;

  /**
   * Construct an Azure cloud destination from config properties.
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param azureCloudConfig the {@link AzureCloudConfig} to use.
   * @param clusterName the name of the Ambry cluster.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @param azureReplicationFeedType {@link AzureReplicationFeed.FeedType} to use for replication from Azure.
   * @param clusterMap {@link ClusterMap}.
   * @throws ReflectiveOperationException
   * @throws CloudStorageException
   */
  AzureCloudDestination(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, String clusterName,
      VcrMetrics vcrMetrics, AzureMetrics azureMetrics, AzureReplicationFeed.FeedType azureReplicationFeedType,
      ClusterMap clusterMap) throws ReflectiveOperationException, CloudStorageException {
    this.azureMetrics = azureMetrics;
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
    this.azureBlobDataAccessor =
        new AzureBlobDataAccessor(cloudConfig, azureCloudConfig, blobLayoutStrategy, azureMetrics);
    this.queryBatchSize = azureCloudConfig.cosmosQueryBatchSize;

    this.cosmosDataAccessor = new CosmosDataAccessor(cloudConfig, azureCloudConfig, vcrMetrics, azureMetrics);
    this.azureStorageCompactor =
        new AzureStorageCompactor(azureBlobDataAccessor, cosmosDataAccessor, cloudConfig, vcrMetrics, azureMetrics);
    this.azureContainerCompactor =
        new AzureContainerCompactor(azureBlobDataAccessor, cosmosDataAccessor, cloudConfig, azureCloudConfig,
            vcrMetrics, azureMetrics);
    this.azureReplicationFeed =
        getReplicationFeedObj(azureReplicationFeedType, cosmosDataAccessor, azureMetrics, queryBatchSize);
    this.cloudConfig = cloudConfig;
    isVcr = cloudConfig.cloudIsVcr;
    this.clusterMap = clusterMap;
    logger.info("Created Azure destination");
  }

  /**
   * Test constructor.
   * @param storageAsyncClient the {@link BlobServiceAsyncClient} to use.
   * @param blobBatchAsyncClient the {@link BlobBatchAsyncClient} to use.
   * @param cosmosAsyncClient the {@link CosmosAsyncClient} to use.
   * @param cosmosAsyncDatabase the {@link CosmosAsyncDatabase} to use.
   * @param cosmosAsyncContainer the {@link CosmosAsyncContainer} to use.
   * @param cosmosDatabase the cosmos Database to use.
   * @param cosmosContainerForMetadata the CosmosDB collection to use for blob metadata.
   * @param cosmosContainerForDeletedAmbryContainers the CosmosDB collection to use for deleted Containers.
   * @param clusterName the name of the Ambry cluster.
   * @param azureMetrics the {@link AzureMetrics} to use.
   * @param azureReplicationFeedType the {@link AzureReplicationFeed.FeedType} to use for replication from Azure.
   * @param clusterMap {@link ClusterMap} object.
   * @param isVcr whether this instance is a VCR.
   */
  AzureCloudDestination(BlobServiceAsyncClient storageAsyncClient, BlobBatchAsyncClient blobBatchAsyncClient,
      CosmosAsyncClient cosmosAsyncClient, CosmosAsyncDatabase cosmosAsyncDatabase,
      CosmosAsyncContainer cosmosAsyncContainer, String cosmosDatabase, String cosmosContainerForMetadata,
      String cosmosContainerForDeletedAmbryContainers, String clusterName, AzureMetrics azureMetrics,
      AzureReplicationFeed.FeedType azureReplicationFeedType, ClusterMap clusterMap, boolean isVcr,
      Properties configProps) throws CloudStorageException {
    this.azureMetrics = azureMetrics;
    this.blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName);
    this.cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    this.azureBlobDataAccessor =
        new AzureBlobDataAccessor(storageAsyncClient, blobBatchAsyncClient, clusterName, azureMetrics, azureCloudConfig,
            cloudConfig);
    this.queryBatchSize = AzureCloudConfig.DEFAULT_QUERY_BATCH_SIZE;
    VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());
    this.cosmosDataAccessor =
        new CosmosDataAccessor(cosmosAsyncClient, cosmosAsyncDatabase, cosmosAsyncContainer, cosmosDatabase,
            cosmosContainerForMetadata, cosmosContainerForDeletedAmbryContainers, vcrMetrics, azureMetrics);
    this.azureStorageCompactor =
        new AzureStorageCompactor(azureBlobDataAccessor, cosmosDataAccessor, cloudConfig, vcrMetrics, azureMetrics);
    this.azureContainerCompactor =
        new AzureContainerCompactor(azureBlobDataAccessor, cosmosDataAccessor, cloudConfig, azureCloudConfig,
            vcrMetrics, azureMetrics);
    this.azureReplicationFeed =
        getReplicationFeedObj(azureReplicationFeedType, cosmosDataAccessor, azureMetrics, queryBatchSize);
    this.isVcr = isVcr;
    this.clusterMap = clusterMap;
  }

  static CloudStorageException toCloudStorageException(String message, Exception e, AzureMetrics azureMetrics) {
    Long retryDelayMs = null;
    int statusCode;
    if (e instanceof BlobStorageException) {
      azureMetrics.storageErrorCount.inc();
      statusCode = ((BlobStorageException) e).getStatusCode();
    } else if (e instanceof CosmosException) {
      azureMetrics.documentErrorCount.inc();
      statusCode = ((CosmosException) e).getStatusCode();
      retryDelayMs = ((CosmosException) e).getRetryAfterDuration().toMillis();
    } else {
      // Note: catch-all since ABS can throw things like IOException, IllegalStateException
      if (azureMetrics != null) {
        azureMetrics.storageErrorCount.inc();
      }
      statusCode = HttpConstants.StatusCodes.INTERNAL_SERVER_ERROR;
    }
    // Everything is retryable except NOT_FOUND
    boolean isRetryable = (statusCode != NOTFOUND && !(e instanceof StoreException));
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
    try {
      return uploadBlobAsync(blobId, inputLength, cloudBlobMetadata, blobInputStream).join();
    } catch (Exception e) {
      Exception ex = Utils.extractFutureExceptionCause(e);
      if (ex instanceof CloudStorageException) {
        throw ((CloudStorageException) ex);
      }
      throw new RuntimeException("Error uploading blob " + blobId, ex);
    }
  }

  @Override
  public CompletableFuture<Boolean> uploadBlobAsync(BlobId blobId, long inputLength,
      CloudBlobMetadata cloudBlobMetadata, InputStream blobInputStream) {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(blobInputStream, "Input stream cannot be null");
    CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
    Timer.Context backupTimer = azureMetrics.backupSuccessLatency.time();
    azureBlobDataAccessor.uploadIfNotExists(blobId, inputLength, cloudBlobMetadata, blobInputStream)
        .whenCompleteAsync((blobUploadResponse, throwable) -> {
          if (throwable != null) {
            azureMetrics.backupErrorCount.inc();
            Exception ex = Utils.extractFutureExceptionCause(throwable);
            resultFuture.completeExceptionally(toCloudStorageException("Error uploading blob " + blobId, ex));
            backupTimer.stop();
          } else {
            // Note: if uploaded is false, still attempt to insert the metadata document
            // since it is possible that a previous attempt failed.
            cosmosDataAccessor.upsertMetadata(cloudBlobMetadata).whenComplete(((metadataResponse, cosmosThrowable) -> {
              backupTimer.stop();
              if (cosmosThrowable != null) {
                azureMetrics.backupErrorCount.inc();
                Exception ex = Utils.extractFutureExceptionCause(cosmosThrowable);
                resultFuture.completeExceptionally(toCloudStorageException("Error uploading blob " + blobId, ex));
              } else {
                azureMetrics.backupSuccessByteRate.mark(inputLength);
                resultFuture.complete(true);
              }
            }));
          }
        });

    return resultFuture;
  }

  @Override
  public void downloadBlob(BlobId blobId, OutputStream outputStream) throws CloudStorageException {
    try {
      downloadBlobAsync(blobId, outputStream).join();
    } catch (Exception e) {
      Exception ex = Utils.extractFutureExceptionCause(e);
      if (ex instanceof CloudStorageException) {
        throw ((CloudStorageException) ex);
      }
      throw new RuntimeException("Error downloading blob " + blobId, ex);
    }
  }

  @Override
  public CompletableFuture<Void> downloadBlobAsync(BlobId blobId, OutputStream outputStream) {
    CompletableFuture<Void> resultFuture = new CompletableFuture<>();
    azureBlobDataAccessor.downloadBlob(blobId, outputStream).whenComplete(((response, throwable) -> {
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        resultFuture.completeExceptionally(toCloudStorageException("Error downloading blob " + blobId, ex));
      } else {
        resultFuture.complete(null);
      }
    }));
    return resultFuture;
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) throws CloudStorageException {
    try {
      return deleteBlobAsync(blobId, deletionTime, lifeVersion, cloudUpdateValidator).join();
    } catch (Exception e) {
      Exception ex = Utils.extractFutureExceptionCause(e);
      if (ex instanceof CloudStorageException) {
        throw ((CloudStorageException) ex);
      }
      throw new RuntimeException("Error deleting blob " + blobId, ex);
    }
  }

  @Override
  public CompletableFuture<Boolean> deleteBlobAsync(BlobId blobId, long deletionTime, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) {
    Map<String, Object> updateFields = new HashMap<>();
    // TODO Frontend support needs to handle the special case of life version = MessageInfo.LIFE_VERSION_FROM_FRONTEND
    updateFields.put(CloudBlobMetadata.FIELD_LIFE_VERSION, lifeVersion);
    updateFields.put(CloudBlobMetadata.FIELD_DELETION_TIME, deletionTime);
    return updateBlobMetadata(blobId, updateFields, cloudUpdateValidator).thenApply(
        (updateResponse -> updateResponse.wasUpdated));
  }

  @Override
  public short updateBlobExpiration(BlobId blobId, long expirationTime, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException {
    try {
      return updateBlobExpirationAsync(blobId, expirationTime, cloudUpdateValidator).join();
    } catch (Exception e) {
      Exception ex = Utils.extractFutureExceptionCause(e);
      if (ex instanceof CloudStorageException) {
        throw ((CloudStorageException) ex);
      }
      throw new RuntimeException("Error updating blob " + blobId, ex);
    }
  }

  @Override
  public CompletableFuture<Short> updateBlobExpirationAsync(BlobId blobId, long expirationTime,
      CloudUpdateValidator cloudUpdateValidator) {
    return updateBlobMetadata(blobId, Collections.singletonMap(CloudBlobMetadata.FIELD_EXPIRATION_TIME, expirationTime),
        cloudUpdateValidator).thenApply(
        (updateResponse -> updateResponse.metadata.containsKey(CloudBlobMetadata.FIELD_LIFE_VERSION) ? Short.parseShort(
            updateResponse.metadata.get(CloudBlobMetadata.FIELD_LIFE_VERSION)) : 0));
  }

  @Override
  public short undeleteBlob(BlobId blobId, short lifeVersion, CloudUpdateValidator cloudUpdateValidator)
      throws CloudStorageException {
    try {
      return undeleteBlobAsync(blobId, lifeVersion, cloudUpdateValidator).join();
    } catch (Exception e) {
      Exception ex = Utils.extractFutureExceptionCause(e);
      if (ex instanceof CloudStorageException) {
        throw ((CloudStorageException) ex);
      }
      throw new RuntimeException("Error undeleting blob " + blobId, ex);
    }
  }

  @Override
  public CompletableFuture<Short> undeleteBlobAsync(BlobId blobId, short lifeVersion,
      CloudUpdateValidator cloudUpdateValidator) {
    Map<String, Object> updateFields = new HashMap<>();
    // TODO Frontend support needs to handle the special case of life version = MessageInfo.LIFE_VERSION_FROM_FRONTEND
    updateFields.put(CloudBlobMetadata.FIELD_LIFE_VERSION, lifeVersion);
    updateFields.put(CloudBlobMetadata.FIELD_DELETION_TIME, Utils.Infinite_Time);
    return updateBlobMetadata(blobId, updateFields, cloudUpdateValidator).thenApply((updateResponse -> Short.parseShort(
        updateResponse.metadata.getOrDefault(CloudBlobMetadata.FIELD_LIFE_VERSION, "0"))));
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException {
    try {
      return getBlobMetadataAsync(blobIds).join();
    } catch (Exception e) {
      Exception ex = Utils.extractFutureExceptionCause(e);
      if (ex instanceof CloudStorageException) {
        throw ((CloudStorageException) ex);
      }
      throw new RuntimeException("Error getting metadata of blobs " + blobIds, ex);
    }
  }

  @Override
  public CompletableFuture<Map<String, CloudBlobMetadata>> getBlobMetadataAsync(List<BlobId> blobIds) {
    Objects.requireNonNull(blobIds, "blobIds cannot be null");
    CompletableFuture<Map<String, CloudBlobMetadata>> resultFuture = new CompletableFuture<>();

    if (blobIds.isEmpty()) {
      resultFuture.complete(Collections.emptyMap());
      return resultFuture;
    }

    // For single blob GET request (isVcr = false), get metadata from ABS instead of Cosmos
    // Note: findMissingKeys (isVcr = true) needs to query Cosmos regardless, because a partial failure in a
    // previous upload could have resulted in a missing record in Cosmos; the findMissingKeys result
    // needs to include that store key to replay the upload.
    if (!isVcr && blobIds.size() == 1) {
      azureBlobDataAccessor.getBlobMetadata(blobIds.get(0)).whenComplete(((cloudBlobMetadata, throwable) -> {
        if (throwable != null) {
          Exception ex = Utils.extractFutureExceptionCause(throwable);
          resultFuture.completeExceptionally(
              toCloudStorageException("Failed to query metadata for blob" + blobIds.get(0), ex));
        } else {
          if (cloudBlobMetadata == null) {
            resultFuture.complete(Collections.emptyMap());
          } else {
            resultFuture.complete(Collections.singletonMap(cloudBlobMetadata.getId(), cloudBlobMetadata));
          }
        }
      }));
      return resultFuture;
    }

    // CosmosDB has query size limit of 256k chars.
    // Break list into chunks if necessary to avoid overflow.
    List<CompletableFuture<Void>> operationFutures = new ArrayList<>();
    List<CloudBlobMetadata> metadataList = new ArrayList<>();
    List<List<BlobId>> chunkedBlobIdList = Utils.partitionList(blobIds, queryBatchSize);

    for (List<BlobId> batchOfBlobs : chunkedBlobIdList) {
      // Get metadata of specified list of blobs
      CompletableFuture<Void> operationFuture = getBlobMetadataChunked(batchOfBlobs).thenAccept((metadataList::addAll));
      operationFutures.add(operationFuture);
    }

    // Complete the result future which is completed when all the individual operation futures are completed.
    CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture<?>[0]))
        .whenComplete(((unused, throwable) -> {
          if (throwable != null) {
            Exception ex = Utils.extractFutureExceptionCause(throwable);
            resultFuture.completeExceptionally(ex);
          } else {
            resultFuture.complete(metadataList.stream()
                .collect(Collectors.toMap(CloudBlobMetadata::getId, Function.identity(), (x, y) -> x)));
          }
        }));
    return resultFuture;
  }

  @Override
  public void close() throws IOException {
    azureReplicationFeed.close();
  }

  @Override
  public void stopCompaction() {
    azureStorageCompactor.shutdown();
  }

  /**
   * Get metadata for specified list of blobs asynchronously.
   * @param blobIds {@link List} of {@link BlobId}s to get metadata of.
   * @return a {@link CompletableFuture} that will eventually contain either the {@link List} of
   *         {@link CloudBlobMetadata} for the blobs list or an exception if an error occurred.
   */
  private CompletableFuture<List<CloudBlobMetadata>> getBlobMetadataChunked(List<BlobId> blobIds) {
    CompletableFuture<List<CloudBlobMetadata>> resultFuture = new CompletableFuture<>();
    if (blobIds.isEmpty() || blobIds.size() > queryBatchSize) {
      resultFuture.completeExceptionally(new IllegalArgumentException("Invalid input list size: " + blobIds.size()));
      return resultFuture;
    }
    String quotedBlobIds = blobIds.stream().map(s -> '"' + s.getID() + '"').collect(Collectors.joining(","));
    String query = String.format(BATCH_ID_QUERY_TEMPLATE, quotedBlobIds);
    String partitionPath = blobIds.get(0).getPartition().toPathString();
    cosmosDataAccessor.queryMetadata(partitionPath, query, azureMetrics.missingKeysQueryTime)
        .whenComplete(((blobMetadataList, throwable) -> {
          Exception ex = Utils.extractFutureExceptionCause(throwable);
          if (throwable != null) {
            resultFuture.completeExceptionally(toCloudStorageException(
                "Failed to query metadata for " + blobIds.size() + " blobs in partition " + partitionPath, ex));
          } else {
            resultFuture.complete(blobMetadataList);
          }
        }));
    return resultFuture;
  }

  @Override
  public FindResult findEntriesSince(String partitionPath, FindToken findToken, long maxTotalSizeOfEntries)
      throws CloudStorageException {
    try {
      return azureReplicationFeed.getNextEntriesAndUpdatedToken(findToken, maxTotalSizeOfEntries, partitionPath);
    } catch (CosmosException cex) {
      throw toCloudStorageException("Failed to query blobs for partition " + partitionPath, cex);
    }
  }

  /**
   * Update the metadata for the specified blob asynchronously.
   * @param blobId The {@link BlobId} to update.
   * @param updateFields map of fields and new values to update.
   * @param cloudUpdateValidator {@link CloudUpdateValidator} passed by the caller to validate the update.
   * @return a {@link CompletableFuture} that will eventually contain either the {@link UpdateResponse} object
   *         containing updated metadata or an exception if an error occurred.
   */
  private CompletableFuture<UpdateResponse> updateBlobMetadata(BlobId blobId, Map<String, Object> updateFields,
      CloudUpdateValidator cloudUpdateValidator) {
    Objects.requireNonNull(blobId, "BlobId cannot be null");
    updateFields.keySet().forEach(field -> Objects.requireNonNull(updateFields.get(field)));

    CompletableFuture<UpdateResponse> resultFuture = new CompletableFuture<>();

    // We update the blob metadata value in two places:
    // 1) the blob storage entry metadata (so GET's can be served entirely from ABS)
    // 2) the CosmosDB metadata collection
    azureBlobDataAccessor.updateBlobMetadata(blobId, updateFields, cloudUpdateValidator)
        .whenCompleteAsync(((updateBlobMetadataResponse, throwable) -> {
          if (throwable == null) {
            // Blob is updated in blob storage. Now update in cosmos.
            // Note: even if nothing changed in blob storage, still attempt to update Cosmos since this could be a retry
            // of a request where ABS was updated but Cosmos update failed.
            AtomicReference<Map<String, String>> metadataMap =
                new AtomicReference<>(updateBlobMetadataResponse.metadata);
            AtomicBoolean updatedStorage = new AtomicBoolean(updateBlobMetadataResponse.wasUpdated);

            cosmosDataAccessor.updateMetadata(blobId, metadataMap.get())
                .whenComplete(((cosmosMetadata, cosmosUpdateThrowable) -> {
                  AtomicBoolean updatedCosmos = new AtomicBoolean(false);
                  if (cosmosUpdateThrowable == null) {
                    // Updated cosmos successfully.
                    updatedCosmos.set(cosmosMetadata != null);
                    azureMetrics.blobUpdatedCount.inc();
                    logger.debug("Updated metadata response {} for blob {} metadata fields {} to values {}.",
                        updatedStorage.get() || updatedCosmos.get(), blobId, updateFields.keySet(),
                        updateFields.values());
                    resultFuture.complete(new UpdateResponse(updatedStorage.get() || updatedCosmos.get(),
                        updateBlobMetadataResponse.metadata));
                  } else {
                    Exception ex = Utils.extractFutureExceptionCause(cosmosUpdateThrowable);
                    if (ex instanceof CosmosException && ((CosmosException) ex).getStatusCode() == NOTFOUND) {
                      // blob exists in ABS but not Cosmos - inconsistent state
                      // Recover by inserting the updated map into cosmos
                      azureMetrics.blobUpdateRecoverCount.inc();
                      cosmosDataAccessor.upsertMetadata(CloudBlobMetadata.fromMap(metadataMap.get()))
                          .whenComplete(((recoveredCosmosMetadata, cosmosRecoverUpdateThrowable) -> {
                            if (cosmosRecoverUpdateThrowable != null) {
                              azureMetrics.blobUpdateErrorCount.inc();
                              resultFuture.completeExceptionally(
                                  toCloudStorageException("Error updating blob metadata: " + blobId,
                                      (Exception) cosmosRecoverUpdateThrowable));
                            } else {
                              updatedCosmos.set(recoveredCosmosMetadata != null);
                              resultFuture.complete(new UpdateResponse(updatedStorage.get() || updatedCosmos.get(),
                                  updateBlobMetadataResponse.metadata));
                            }
                          }));
                    } else {
                      // We got some other exception apart from blob_not_found when updating cosmos. Consider the update operation as failed.
                      resultFuture.completeExceptionally(
                          toCloudStorageException("Error updating blob metadata: " + blobId, ex));
                    }
                  }
                }));
          } else {
            // There is an error updating the blob metadata in ABS.
            azureMetrics.blobUpdateErrorCount.inc();
            Exception blobUpdateException = Utils.extractFutureExceptionCause(throwable);
            CloudStorageException cloudStorageException =
                toCloudStorageException("Error updating blob metadata: " + blobId, blobUpdateException);
            if (blobUpdateException instanceof BlobStorageException
                && ((BlobStorageException) blobUpdateException).getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
              // There is a corner case where compaction of this partition hit partial failure leaving the record
              // in Cosmos but not ABS.  If that happens, a late arriving update event can get stuck in a loop
              // where findMissingKeys says the blob exists (because Cosmos has it), but the subsequent update
              // attempt fails.  So we check for that case here.
              cosmosDataAccessor.getMetadataOrNull(blobId).whenComplete(((cloudBlobMetadata, cosmosGetThrowable) -> {
                if (cosmosGetThrowable != null) {
                  // Failed to find blob metadata in cosmos. It should be okay. Complete the result future.
                  resultFuture.completeExceptionally(cloudStorageException);
                } else {
                  if (cloudBlobMetadata.isCompactionCandidate(
                      TimeUnit.HOURS.toMillis(cloudConfig.cloudBlobCompactionIntervalHours))) {
                    logger.warn("Inconsistency: Cosmos contains record for inactive blob {}, removing it.",
                        blobId.getID());
                    // Delete the record in cosmos and complete the result future after that.
                    cosmosDataAccessor.deleteMetadata(cloudBlobMetadata)
                        .whenComplete(
                            ((deleteMetadataResponse, cosmosDeleteThrowable) -> resultFuture.completeExceptionally(
                                cloudStorageException)));
                    azureMetrics.blobUpdateRecoverCount.inc();
                  } else {
                    // If the blob is still active but ABS does not have it, we are in deeper trouble.
                    logger.error("Inconsistency: Cosmos contains record for active blob {} that is missing from ABS!",
                        blobId.getID());
                    resultFuture.completeExceptionally(blobUpdateException);
                  }
                }
              }));
            } else {
              // For any other exception other than NOT_FOUND, complete the result future immediately.
              resultFuture.completeExceptionally(
                  toCloudStorageException("Error updating blob metadata: " + blobId, (Exception) throwable));
            }
          }
        }));
    return resultFuture;
  }

  @Override
  public int compactPartition(String partitionPath) throws CloudStorageException {
    return azureStorageCompactor.compactPartition(partitionPath);
  }

  @Override
  public void persistTokens(String partitionPath, String tokenFileName, InputStream inputStream)
      throws CloudStorageException {
    // Path is partitionId path string
    // Write to container partitionPath, blob filename "replicaTokens"
    try {
      BlobLayout tokenLayout = blobLayoutStrategy.getTokenBlobLayout(partitionPath, tokenFileName);
      azureBlobDataAccessor.uploadFile(tokenLayout.containerName, tokenLayout.blobFilePath, inputStream).join();
    } catch (Exception e) {
      azureMetrics.absTokenPersistFailureCount.inc();
      throw toCloudStorageException("Could not persist token: " + partitionPath, Utils.extractFutureExceptionCause(e));
    }
  }

  @Override
  public boolean retrieveTokens(String partitionPath, String tokenFileName, OutputStream outputStream)
      throws CloudStorageException {
    try {
      BlobLayout tokenLayout = blobLayoutStrategy.getTokenBlobLayout(partitionPath, tokenFileName);
      return azureBlobDataAccessor.downloadFile(tokenLayout.containerName, tokenLayout.blobFilePath, outputStream,
          false).join();
    } catch (Exception e) {
      throw toCloudStorageException("Could not retrieve token: " + partitionPath, Utils.extractFutureExceptionCause(e));
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
   * @return the {@link AzureStorageCompactor}
   */
  AzureStorageCompactor getAzureStorageCompactor() {
    return azureStorageCompactor;
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
    return toCloudStorageException(message, e, azureMetrics);
  }

  @Override
  public void deprecateContainers(Collection<Container> deletedContainers) throws CloudStorageException {
    //TODO need to account for all possible partition classes in call to getAllPartitionIds.
    azureContainerCompactor.deprecateContainers(deletedContainers,
        clusterMap.getAllPartitionIds(null).stream().map(PartitionId::toPathString).collect(Collectors.toSet()));
  }

  @Override
  public CloudContainerCompactor getContainerCompactor() {
    return azureContainerCompactor;
  }

  /**
   * Struct returned by updateBlobMetadata that tells the caller whether the metadata was updated
   * and also returns the (possibly modified) metadata.
   */
  static class UpdateResponse {
    /** Flag indicating whether the metadata was updated. */
    final boolean wasUpdated;
    /** The resulting metadata map. */
    final Map<String, String> metadata;

    UpdateResponse(boolean wasUpdated, Map<String, String> metadata) {
      this.wasUpdated = wasUpdated;
      this.metadata = metadata;
    }
  }
}
