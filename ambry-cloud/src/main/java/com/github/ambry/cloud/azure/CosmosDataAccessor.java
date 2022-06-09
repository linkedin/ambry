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
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosAsyncStoredProcedure;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.codahale.metrics.Timer;
import com.github.ambry.account.Container;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudRequestAgent;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;


public class CosmosDataAccessor {
  private static final Logger logger = LoggerFactory.getLogger(CosmosDataAccessor.class);
  public static final String COSMOS_LAST_UPDATED_COLUMN = "_ts";
  private static final String START_TIME_PARAM = "@startTime";
  private static final String END_TIME_PARAM = "@endTime";
  private static final String LIMIT_PARAM = "@limit";
  private static final String ACCOUNT_ID_PARAM = "@accountId";
  private static final String CONTAINER_ID_PARAM = "@containerId";
  private static final String EXPIRED_BLOBS_QUERY = constructDeadBlobsQuery(CloudBlobMetadata.FIELD_EXPIRATION_TIME);
  private static final String DELETED_BLOBS_QUERY = constructDeadBlobsQuery(CloudBlobMetadata.FIELD_DELETION_TIME);
  private static final String CONTAINER_BLOBS_QUERY =
      "SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE c.accountId=" + ACCOUNT_ID_PARAM + " and c.containerId="
          + CONTAINER_ID_PARAM;
  private static final String BULK_DELETE_QUERY = "SELECT c._self FROM c WHERE c.id IN (%s)";
  private static final String DEPRECATED_CONTAINERS_QUERY =
      "SELECT TOP " + LIMIT_PARAM + " * from c WHERE c.deleted=false order by c.deleteTriggerTimestamp";
  static final String BULK_DELETE_SPROC_ID = "bulkDelete";
  static final String PROPERTY_CONTINUATION = "continuation";
  static final String PROPERTY_DELETED = "deleted";
  static final int STATUS_NOT_FOUND = 404;

  private final CosmosAsyncClient cosmosAsyncClient;
  private CosmosAsyncDatabase cosmosAsyncDatabase;
  private CosmosAsyncContainer cosmosAsyncContainer;
  private final CloudRequestAgent requestAgent;
  private final String cosmosDatabase;
  private final String cosmosCollection;
  private final String cosmosDeletedContainerCollection;
  private final AzureMetrics azureMetrics;
  private Callable<?> updateCallback = null;
  private final int continuationTokenLimitKb;
  private final int requestChargeThreshold;
  private final int purgeBatchSize;
  private boolean bulkDeleteEnabled = false;

  /** Production constructor */
  CosmosDataAccessor(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, VcrMetrics vcrMetrics,
      AzureMetrics azureMetrics) {
    // Set up CosmosDB connection, including retry options and any proxy setting
    // Note: retry decisions are made at CloudBlobStore level.  Configure Cosmos with no retries.
    ThrottlingRetryOptions throttlingRetryOptions = new ThrottlingRetryOptions();
    throttlingRetryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
    CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder().endpoint(azureCloudConfig.cosmosEndpoint)
        .key(getCosmosKey(azureCloudConfig))
        .throttlingRetryOptions(throttlingRetryOptions)
        .consistencyLevel(com.azure.cosmos.ConsistencyLevel.SESSION);

    // It looks like we can no longer configure request timeouts on GateWayConnection mode
    // since cosmos team has hidden them in latest versions. (https://github.com/Azure/azure-sdk-for-java/pull/11702).
    // Default value seems to be 60 seconds. For DirectConnection (which uses TCP to talk directly to cosmos storage nodes),
    // the value can be configured but has lower limit of 5 seconds and upper limit of 10 seconds.
    // We can probably reach out to them if we see requests taking longer than these defaults.
    // For reference, here is difference between Gateway and direct connection modes -
    // https://docs.microsoft.com/en-us/azure/cosmos-db/sql/sql-sdk-connection-modes
    GatewayConnectionConfig gatewayConnectionConfig = new GatewayConnectionConfig();
    if (cloudConfig.vcrProxyHost != null) {
      gatewayConnectionConfig.setProxy(new ProxyOptions(ProxyOptions.Type.HTTP,
          new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort)));
    }
    if (azureCloudConfig.cosmosDirectHttps) {
      logger.info("Using CosmosDB DirectHttps connection mode");
      cosmosClientBuilder.directMode(new DirectConnectionConfig(), gatewayConnectionConfig);
    } else {
      cosmosClientBuilder.gatewayMode(gatewayConnectionConfig);
    }
    cosmosAsyncClient = cosmosClientBuilder.buildAsyncClient();

    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);

    this.continuationTokenLimitKb = azureCloudConfig.cosmosContinuationTokenLimitKb;
    this.requestChargeThreshold = azureCloudConfig.cosmosRequestChargeThreshold;
    this.purgeBatchSize = azureCloudConfig.cosmosPurgeBatchSize;
    this.azureMetrics = azureMetrics;
    this.cosmosDatabase = azureCloudConfig.cosmosDatabase;
    this.cosmosCollection = azureCloudConfig.cosmosCollection;
    this.cosmosDeletedContainerCollection = azureCloudConfig.cosmosDeletedContainerCollection;
  }

  /** Test constructor */
  CosmosDataAccessor(CosmosAsyncClient cosmosAsyncClient, CosmosAsyncDatabase cosmosAsyncDatabase,
      CosmosAsyncContainer cosmosAsyncContainer, String cosmosDatabase, String cosmosCollection,
      String cosmosDeletedContainerCollection, VcrMetrics vcrMetrics, AzureMetrics azureMetrics) {
    this.cosmosAsyncClient = cosmosAsyncClient;
    this.cosmosAsyncDatabase = cosmosAsyncDatabase;
    this.cosmosAsyncContainer = cosmosAsyncContainer;
    this.cosmosDatabase = cosmosDatabase;
    this.cosmosCollection = cosmosCollection;
    this.cosmosDeletedContainerCollection = cosmosDeletedContainerCollection;
    this.continuationTokenLimitKb = AzureCloudConfig.DEFAULT_COSMOS_CONTINUATION_TOKEN_LIMIT;
    this.requestChargeThreshold = AzureCloudConfig.DEFAULT_COSMOS_REQUEST_CHARGE_THRESHOLD;
    this.purgeBatchSize = AzureCloudConfig.DEFAULT_PURGE_BATCH_SIZE;
    this.azureMetrics = azureMetrics;
    this.bulkDeleteEnabled = true;
    CloudConfig testCloudConfig = new CloudConfig(new VerifiableProperties(new Properties()));
    requestAgent = new CloudRequestAgent(testCloudConfig, vcrMetrics);
  }

  /** Visible for testing */
  void setUpdateCallback(Callable<?> callback) {
    this.updateCallback = callback;
  }

  /**
   * Test connectivity to Azure CosmosDB
   */
  void testConnectivity() {
    //  Check if database and container exists
    try {
      cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cosmosDatabase);
      CosmosDatabaseResponse cosmosDatabaseResponse = cosmosAsyncDatabase.read().block();
      if (cosmosDatabaseResponse == null || cosmosDatabaseResponse.getStatusCode() == STATUS_NOT_FOUND) {
        throw new IllegalStateException("CosmosDB Database not found: " + cosmosDatabase);
      }

      cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(cosmosCollection);
      CosmosContainerResponse cosmosContainerResponse = cosmosAsyncContainer.read().block();
      if (cosmosContainerResponse == null || cosmosContainerResponse.getStatusCode() == STATUS_NOT_FOUND) {
        throw new IllegalStateException("CosmosDB container not found: " + cosmosCollection);
      }
    } catch (Exception ex) {
      if (ex instanceof CosmosException && ((CosmosException) ex).getStatusCode() == STATUS_NOT_FOUND) {
        //Client-specific errors
        throw new IllegalStateException(
            "CosmosDB database " + cosmosDatabase + "or container " + cosmosCollection + " not found", ex);
      }
      throw new RuntimeException(
          "Error connecting to cosmos database " + cosmosDatabase + "or container " + cosmosCollection, ex);
    }

    logger.info("CosmosDB connection test succeeded.");

    if (purgeBatchSize > 1) {
      // Check for existence of BulkDelete stored procedure.
      // Source: https://github.com/Azure/azure-cosmosdb-js-server/blob/master/samples/stored-procedures/bulkDelete.js
      try {
        CosmosAsyncStoredProcedure cosmosAsyncStoredProcedure =
            cosmosAsyncContainer.getScripts().getStoredProcedure(BULK_DELETE_SPROC_ID);
        CosmosStoredProcedureResponse cosmosStoredProcedureResponse = cosmosAsyncStoredProcedure.read().block();

        if (cosmosStoredProcedureResponse == null
            || cosmosStoredProcedureResponse.getStatusCode() == STATUS_NOT_FOUND) {
          logger.error("Did not find stored procedure {}, falling back to individual record deletions",
              BULK_DELETE_SPROC_ID);
        } else {
          logger.info("Found stored procedure {}, will use it with batch size {}.", BULK_DELETE_SPROC_ID,
              purgeBatchSize);
          bulkDeleteEnabled = true;
        }
      } catch (Exception ex) {
        logger.error("Did not find stored procedure {}, falling back to individual record deletions",
            BULK_DELETE_SPROC_ID, ex);
      }
    } else {
      logger.info("Cosmos purge batch size = 1 ==> disabling bulk deletes.");
      bulkDeleteEnabled = false;
    }
  }

  /**
   * Upsert the blob metadata document in the CosmosDB container asynchronously.
   * @param blobMetadata the blob metadata document.
   * @return a {@link CompletableFuture} that will eventually contain either the upserted {@link CloudBlobMetadata} if
   *         successful or {@link CosmosException} exception if an error occurred.
   */
  CompletableFuture<CloudBlobMetadata> upsertMetadataAsync(CloudBlobMetadata blobMetadata) {

    Timer.Context operationTime = azureMetrics.documentCreateTime.time();
    return cosmosAsyncContainer.upsertItem(blobMetadata, new PartitionKey(blobMetadata.getPartitionId()),
        new CosmosItemRequestOptions()).toFuture().handle((response, throwable) -> {
      operationTime.stop();
      if (throwable != null) {
        throw throwable instanceof CompletionException ? (CompletionException) throwable
            : new CompletionException(throwable);
      }
      return response.getItem();
    });
  }

  /**
   * Delete the blob metadata document in the CosmosDB container asynchronously, if it exists.
   * @param blobMetadata the blob metadata document to delete.
   * @return a {@link CompletableFuture} that will eventually contain {@link Boolean} flag indicating if the record was
   *         deleted, or a {@link CosmosException} if an error occurred.
   */
  CompletableFuture<Boolean> deleteMetadataAsync(CloudBlobMetadata blobMetadata) {
    // Note: not timing here since bulk deletions are timed.
    return cosmosAsyncContainer.deleteItem(blobMetadata.getId(), new PartitionKey(blobMetadata.getPartitionId()))
        .toFuture()
        .handle((response, throwable) -> {
          if (throwable != null) {
            Exception ex = Utils.extractFutureExceptionCause(throwable);
            if (ex instanceof CosmosException && ((CosmosException) ex).getStatusCode() == STATUS_NOT_FOUND) {
              // If the cosmos exception is due to entry not being found, return false. This can happen on retry of delete operation.
              logger.debug("Could not find metadata for blob {} to delete", blobMetadata.getId());
              return false;
            }
            throw throwable instanceof CompletionException ? (CompletionException) throwable
                : new CompletionException(ex);
          }
          return true;
        });
  }

  /**
   * Delete the blob metadata documents in the CosmosDB collection.
   * @param blobMetadataList the list of blob metadata documents to delete.
   * @throws CosmosException if the operation failed.
   */
  void deleteMetadata(List<CloudBlobMetadata> blobMetadataList) throws CosmosException {
    String partitionPath = blobMetadataList.get(0).getPartitionId();
    if (bulkDeleteEnabled) {
      for (List<CloudBlobMetadata> batchOfBlobs : Utils.partitionList(blobMetadataList, purgeBatchSize)) {
        // Retry each batch since there may be many of them.
        try {
          requestAgent.doWithRetries(() -> {
            try {
              return bulkDeleteMetadata(batchOfBlobs);
            } catch (CosmosException cex) {
              throw new CloudStorageException("BulkDelete failed", cex, cex.getStatusCode(), true,
                  cex.getRetryAfterDuration().toMillis());
            }
          }, "BulkDelete", partitionPath);
        } catch (CloudStorageException cse) {
          if (cse.getCause() != null && cse.getCause() instanceof CosmosException) {
            throw (CosmosException) cse.getCause();
          } else {
            throw new IllegalStateException("Unexpected error in BulkDelete partition " + partitionPath,
                cse.getCause());
          }
        }
      }
    } else {
      List<CompletableFuture<Boolean>> operationFutures = new ArrayList<>();
      blobMetadataList.forEach(metadata -> operationFutures.add(deleteMetadataAsync(metadata)));
      try {
        // Wait for all async delete operations to complete.
        CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture<?>[0])).join();
      } catch (CompletionException ex) {
        Exception e = Utils.extractFutureExceptionCause(ex);
        if (e instanceof CosmosException) {
          throw (CosmosException) e;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Delete the blob metadata documents from CosmosDB using the BulkDelete stored procedure.
   * @param blobMetadataList the list of blob metadata documents to delete.
   * @return the number of documents deleted.
   * @throws CosmosException if the operation failed.
   */
  private int bulkDeleteMetadata(List<CloudBlobMetadata> blobMetadataList) throws CosmosException {
    String partitionPath = blobMetadataList.get(0).getPartitionId();

    // stored proc link provided in config.  Test for it at startup and use if available.
    String quotedBlobIds =
        blobMetadataList.stream().map(metadata -> '"' + metadata.getId() + '"').collect(Collectors.joining(","));
    String query = String.format(BULK_DELETE_QUERY, quotedBlobIds);
    boolean more = true;
    int deleteCount = 0;
    double requestCharge = 0;

    CosmosStoredProcedureRequestOptions cosmosStoredProcedureRequestOptions = new CosmosStoredProcedureRequestOptions();
    cosmosStoredProcedureRequestOptions.setPartitionKey(new PartitionKey(partitionPath));

    try {
      while (more) {
        CosmosStoredProcedureResponse cosmosStoredProcedureResponse = cosmosAsyncContainer.getScripts()
            .getStoredProcedure(BULK_DELETE_SPROC_ID)
            .execute(Collections.singletonList(query), cosmosStoredProcedureRequestOptions)
            .block();
        Document document;
        if (cosmosStoredProcedureResponse != null) {
          document = new Document(cosmosStoredProcedureResponse.getResponseAsString());
          more = document.getBoolean(PROPERTY_CONTINUATION);
          deleteCount += document.getInt(PROPERTY_DELETED);
          requestCharge += cosmosStoredProcedureResponse.getRequestCharge();
        } else {
          more = false;
        }
      }
      if (requestCharge >= requestChargeThreshold) {
        logger.info("Bulk delete partition {} request charge {} for {} records", partitionPath, requestCharge,
            deleteCount);
      }
      return deleteCount;
    } catch (Exception ex) {
      if (ex instanceof CosmosException) {
        throw (CosmosException) ex;
      }
      throw ex;
    }
  }

  /**
   * Get the metadata record for a single blob asynchronously.
   * @param blobId the blob to read.
   * @return a {@link CompletableFuture} that will eventually contain the {@link CloudBlobMetadata} for the blob if
   *         it is found or otherwise null. If an error occurred, it will contain a {@link CosmosException}.
   */
  CompletableFuture<CloudBlobMetadata> getMetadataOrNullAsync(BlobId blobId) {
    Timer.Context operationTimer = azureMetrics.documentReadTime.time();
    return cosmosAsyncContainer.readItem(blobId.getID(), new PartitionKey(blobId.getPartition().toPathString()),
        CloudBlobMetadata.class).toFuture().handle((response, throwable) -> {
      operationTimer.stop();
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (ex instanceof CosmosException && ((CosmosException) ex).getStatusCode() == STATUS_NOT_FOUND) {
          return null;
        }
        throw throwable instanceof CompletionException ? (CompletionException) throwable : new CompletionException(ex);
      }
      return response.getItem();
    });
  }

  /**
   * Update the blob metadata document in the CosmosDB collection asynchronously.
   * @param blobId the {@link BlobId} for which metadata is replaced.
   * @param updateFields Map of field names and new values to update.
   * @return a {@link CompletableFuture} that will eventually contain the {@link CloudBlobMetadata} returned by the
   *         operation, if successful or {@code Null} if the field already has the specified value. If the record is not
   *         found or an error occurred, it will contain a {@link CosmosException}.
   */
  CompletableFuture<CloudBlobMetadata> updateMetadataAsync(BlobId blobId, Map<String, String> updateFields) {

    CompletableFuture<CloudBlobMetadata> resultFuture = new CompletableFuture<>();
    Timer.Context documentReadTimer = azureMetrics.documentReadTime.time();
    AtomicReference<Timer.Context> documentUpdateTimer = new AtomicReference<>();

    // Read the existing record
    cosmosAsyncContainer.readItem(blobId.getID(), new PartitionKey(blobId.getPartition().toPathString()),
        CloudBlobMetadata.class).toFuture().handle((readItemResponse, throwableOnRead) -> {
      // Stop the document read timer
      documentReadTimer.stop();
      if (throwableOnRead != null) {
        // If there is an error in reading the current metadata record, complete the result exceptionally.
        resultFuture.completeExceptionally(throwableOnRead);
        return null;
      } else {
        // Get the metadata from cosmos response
        CloudBlobMetadata receivedMetadata = readItemResponse.getItem();

        // Update the metadata record only if value has changed
        Map<String, String> receivedFields = receivedMetadata.toMap();
        Map<String, String> fieldsToUpdate = updateFields.entrySet()
            .stream()
            .filter(map -> !String.valueOf(updateFields.get(map.getKey())).equals(receivedFields.get(map.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, map -> String.valueOf(map.getValue())));
        if (fieldsToUpdate.isEmpty()) {
          logger.debug("No change in value for {} in blob {}", updateFields.keySet(), blobId.getID());
          // If there is no change in value of metadata, complete the result.
          resultFuture.complete(null);
        }

        // For testing conflict handling
        if (updateCallback != null) {
          try {
            updateCallback.call();
          } catch (Exception ex) {
            logger.error("Error in update callback", ex);
          }
        }

        // Update the modified fields
        fieldsToUpdate.forEach(receivedFields::put);

        // Set condition to ensure we don't clobber a concurrent update
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfMatchETag(readItemResponse.getETag());

        // Replace the item in cosmos db asynchronously.
        documentUpdateTimer.set(azureMetrics.documentUpdateTime.time());
        return cosmosAsyncContainer.replaceItem(CloudBlobMetadata.fromMap(receivedFields), blobId.getID(),
            new PartitionKey(blobId.getPartition().toPathString()), requestOptions).toFuture();
      }
    }).thenCompose(result -> {
      // Since previous stage returns CompletionStage<CompletionStage<T>>, using #thenCompose helps to flatten the result.
      return result;
    }).whenComplete((replaceItemResponse, throwable) -> {
      if (documentUpdateTimer.get() != null) {
        documentUpdateTimer.get().stop();
      }
      if (throwable != null) {
        Exception ex = Utils.extractFutureExceptionCause(throwable);
        if (((CosmosException) ex).getStatusCode() == HttpConstants.StatusCodes.PRECONDITION_FAILED) {
          // Received an exception during metadata replacement due to concurrent update. Increase the conflict counter
          azureMetrics.blobUpdateConflictCount.inc();
        }
        resultFuture.completeExceptionally(ex);
      } else {
        // Updated the metadata record successfully
        resultFuture.complete(replaceItemResponse.getItem());
      }
    });

    return resultFuture;
  }

  /**
   * Get the list of blobs in the specified partition that have been deleted or expired for at least the
   * configured retention period asynchronously.
   * @param partitionPath the partition to query.
   * @param fieldName the field name to query on. Allowed values are {@link CloudBlobMetadata#FIELD_DELETION_TIME} and
   *                  {@link CloudBlobMetadata#FIELD_EXPIRATION_TIME}.
   * @param startTime the start of the query time range.
   * @param endTime the end of the query time range.
   * @param maxEntries the max number of metadata records to return.
   * @return a {@link CompletableFuture} that will eventually contain a list of {@link CloudBlobMetadata} referencing
   *         the dead blobs found or an exception if an error occurred.
   * @throws CosmosException if the operation failed.
   */
  CompletableFuture<List<CloudBlobMetadata>> getDeadBlobsAsync(String partitionPath, String fieldName, long startTime,
      long endTime, int maxEntries) {

    CompletableFuture<List<CloudBlobMetadata>> resultFuture = new CompletableFuture<>();
    String deadBlobsQuery;
    if (fieldName.equals(CloudBlobMetadata.FIELD_DELETION_TIME)) {
      deadBlobsQuery = DELETED_BLOBS_QUERY;
    } else if (fieldName.equals(CloudBlobMetadata.FIELD_EXPIRATION_TIME)) {
      deadBlobsQuery = EXPIRED_BLOBS_QUERY;
    } else {
      throw new IllegalArgumentException("Invalid field: " + fieldName);
    }

    SqlQuerySpec sqlQuerySpec = new SqlQuerySpec(deadBlobsQuery, new SqlParameter(LIMIT_PARAM, maxEntries),
        new SqlParameter(START_TIME_PARAM, startTime), new SqlParameter(END_TIME_PARAM, endTime));
    CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();
    queryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
    queryRequestOptions.setResponseContinuationTokenLimitInKb(continuationTokenLimitKb);

    List<CloudBlobMetadata> deadBlobsList = new ArrayList<>();
    AtomicReference<Double> requestCharge = new AtomicReference<>(0.0);

    Timer.Context deadBlobsQueryTime = azureMetrics.deadBlobsQueryTime.time();

    // Execute cosmos query
    CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse = executeCosmosQueryAsync(sqlQuerySpec, queryRequestOptions);
    //Set the maximum entries to be as returned in each page. There seems to be no way to set maximum number of entries across all pages.
    pagedFluxResponse.byPage(maxEntries).subscribe(fluxResponse -> {
      logger.debug("Got a page of query result with " + fluxResponse.getResults().size() + " items(s)"
          + " and request charge of " + fluxResponse.getRequestCharge());
      requestCharge.updateAndGet(v -> v + fluxResponse.getRequestCharge());
      deadBlobsList.addAll(fluxResponse.getResults());
    }, throwable -> {
      deadBlobsQueryTime.stop();
      if (throwable instanceof CosmosException) {
        logger.warn("Dead blobs query {} partition {} got {}", deadBlobsQuery, partitionPath,
            ((CosmosException) throwable).getStatusCode());
      }
      resultFuture.completeExceptionally(throwable);
    }, () -> {
      deadBlobsQueryTime.stop();
      if (requestCharge.get() >= requestChargeThreshold) {
        logger.info("Dead blobs query partition {} endTime {} request charge {} for {} records", partitionPath,
            new Date(endTime), requestCharge.get(), deadBlobsList.size());
      }
      resultFuture.complete(deadBlobsList);
    });
    return resultFuture;
  }

  /**
   * Get the list of blobs in the specified partition that belong to the specified container asynchronously.
   * @param partitionPath the partition to query.
   * @param accountId account id of the container.
   * @param containerId container id of the container.
   * @param queryLimit max number of blobs to return.
   * @return a {@link CompletableFuture} that will eventually contain a list of {@link CloudBlobMetadata} referencing
   *         the blobs belonging to the specified container or an exception if an error occurred.
   */
  CompletableFuture<List<CloudBlobMetadata>> getContainerBlobsAsync(String partitionPath, short accountId,
      short containerId, int queryLimit) {
    CompletableFuture<List<CloudBlobMetadata>> resultFuture = new CompletableFuture<>();
    SqlQuerySpec sqlQuerySpec = new SqlQuerySpec(CONTAINER_BLOBS_QUERY, new SqlParameter(LIMIT_PARAM, queryLimit),
        new SqlParameter(ACCOUNT_ID_PARAM, accountId), new SqlParameter(CONTAINER_ID_PARAM, containerId));

    CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();
    queryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
    queryRequestOptions.setResponseContinuationTokenLimitInKb(continuationTokenLimitKb);
    List<CloudBlobMetadata> containerBlobsList = new ArrayList<>();
    AtomicReference<Double> requestCharge = new AtomicReference<>(0.0);

    Timer.Context deletedContainerBlobsQueryTime = azureMetrics.deletedContainerBlobsQueryTime.time();

    // Execute cosmos query
    CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse = executeCosmosQueryAsync(sqlQuerySpec, queryRequestOptions);

    // Set the queryLimit as preferred number of items to be fetched in each page. There seems to be no way to set limit across all pages.
    pagedFluxResponse.byPage(queryLimit).subscribe(fluxResponse -> {
      logger.debug("Got a page of query result with " + fluxResponse.getResults().size() + " items(s)"
          + " and request charge of " + fluxResponse.getRequestCharge());
      requestCharge.updateAndGet(v -> v + fluxResponse.getRequestCharge());
      containerBlobsList.addAll(fluxResponse.getResults());
    }, throwable -> {
      deletedContainerBlobsQueryTime.stop();
      if (throwable instanceof CosmosException) {
        logger.warn("Container blobs query {} partition {} got {}", sqlQuerySpec.getQueryText(), partitionPath,
            ((CosmosException) throwable).getStatusCode());
      }
      resultFuture.completeExceptionally(throwable);
    }, () -> {
      deletedContainerBlobsQueryTime.stop();
      if (requestCharge.get() >= requestChargeThreshold) {
        logger.info("Container blobs query partition {} containerId {} accountId {} request charge {} for {} records",
            partitionPath, containerId, accountId, requestCharge, containerBlobsList.size());
      }
      resultFuture.complete(containerBlobsList);
    });

    return resultFuture;
  }

  /**
   * Returns a query like:
   * SELECT TOP 500 * FROM c WHERE c.deletionTime BETWEEN 1 AND <7 days ago> ORDER BY c.deletionTime ASC
   * @param fieldName the field to use in the filter condition.  Must be deletionTime or expirationTime.
   * @return the query text.
   */
  private static String constructDeadBlobsQuery(String fieldName) {
    StringBuilder builder = new StringBuilder("SELECT TOP " + LIMIT_PARAM + " * FROM c WHERE c.").append(fieldName)
        .append(" BETWEEN " + START_TIME_PARAM + " AND " + END_TIME_PARAM)
        .append(" ORDER BY c.")
        .append(fieldName)
        .append(" ASC");
    return builder.toString();
  }

  /**
   * Get the list of blobs in the specified partition matching the specified DocumentDB query asynchronously.
   * @param partitionPath the partition to query.
   * @param queryText the DocumentDB query to execute.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a {@link CompletableFuture} that will eventually contain a list of {@link CloudBlobMetadata} referencing
   *         the matching blobs.
   */
  CompletableFuture<List<CloudBlobMetadata>> queryMetadataAsync(String partitionPath, String queryText, Timer timer)
      throws CosmosException {
    return queryMetadataAsync(partitionPath, new SqlQuerySpec(queryText), timer);
  }

  /**
   * Get the list of blobs in the specified partition matching the specified DocumentDB query spec asynchronously.
   * @param partitionPath the partition to query.
   * @param sqlQuerySpec representing the sql query.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a {@link CompletableFuture} that will eventually contain a list of {@link CloudBlobMetadata} referencing
   *         the matching blobs.
   */
  CompletableFuture<List<CloudBlobMetadata>> queryMetadataAsync(String partitionPath, SqlQuerySpec sqlQuerySpec,
      Timer timer) throws CosmosException {

    CompletableFuture<List<CloudBlobMetadata>> resultFuture = new CompletableFuture<>();
    CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();
    queryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
    queryRequestOptions.setResponseContinuationTokenLimitInKb(continuationTokenLimitKb);

    Timer.Context operationTime = timer.time();
    List<CloudBlobMetadata> metadataList = new ArrayList<>();

    // Since the requestCharge is being used inside lambda expression below, it needs to be atomic.
    AtomicReference<Double> requestCharge = new AtomicReference<>(0.0);

    // Execute cosmos query
    CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse = executeCosmosQueryAsync(sqlQuerySpec, queryRequestOptions);
    pagedFluxResponse.byPage().subscribe(fluxResponse -> {
      logger.debug("Got a page of query result with " + fluxResponse.getResults().size() + " items(s)"
          + " and request charge of " + fluxResponse.getRequestCharge());
      requestCharge.updateAndGet(v -> v + fluxResponse.getRequestCharge());
      metadataList.addAll(fluxResponse.getResults());
    }, throwable -> {
      Exception ex = Utils.extractFutureExceptionCause(throwable);
      operationTime.stop();
      if (ex instanceof CosmosException) {
        logger.warn("Query {} on partition {} got {}", sqlQuerySpec.getQueryText(), partitionPath,
            ((CosmosException) ex).getStatusCode());
      }
      resultFuture.completeExceptionally(ex);
    }, () -> {
      operationTime.stop();
      if (requestCharge.get() >= requestChargeThreshold) {
        logger.info("Query partition {} request charge {} for {} records", partitionPath, requestCharge,
            metadataList.size());
      }
      resultFuture.complete(metadataList);
    });
    return resultFuture;
  }

  /**
   * Query Cosmos change feed to get the next set of {@code CloudBlobMetadata} objects in specified {@code partitionPath}
   * after {@code requestContinationToken} asynchronously, capped by specified {@code maxFeedSize} representing the max
   * number of items to be queried from the change feed.
   * @param requestContinuationToken Continuation token after which change feed is requested.
   * @param maxFeedSize max item count to be requested in the feed query.
   * @param changeFeed {@link CloudBlobMetadata} {@code List} to be populated with the next set of entries returned by change feed query.
   * @param partitionPath partition for which the change feed is requested.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a {@link CompletableFuture} that will eventually contain next continuation token or {@link CosmosException}
   *         in case of any error.
   */
  public CompletableFuture<String> queryChangeFeedAsync(String requestContinuationToken, int maxFeedSize,
      List<CloudBlobMetadata> changeFeed, String partitionPath, Timer timer) {

    azureMetrics.changeFeedQueryCount.inc();

    CosmosChangeFeedRequestOptions cosmosChangeFeedRequestOptions;
    if (Utils.isNullOrEmpty(requestContinuationToken)) {
      cosmosChangeFeedRequestOptions = CosmosChangeFeedRequestOptions.createForProcessingFromBeginning(
          FeedRange.forLogicalPartition(new PartitionKey(partitionPath)));
    } else {
      cosmosChangeFeedRequestOptions =
          CosmosChangeFeedRequestOptions.createForProcessingFromContinuation(requestContinuationToken);
    }
    // Set the maximum number of items to be returned in this change feed request.
    cosmosChangeFeedRequestOptions.setMaxItemCount(maxFeedSize);
    return queryChangeFeedAsync(cosmosChangeFeedRequestOptions, changeFeed, timer);
  }

  /**
   * Query Cosmos change feed to get the next set of {@code CloudBlobMetadata} objects asynchronously.
   * @param cosmosChangeFeedRequestOptions {@link CosmosChangeFeedRequestOptions} containing the options for the change feed request.
   * @param changeFeed {@link CloudBlobMetadata} {@code List} to be populated with the next set of entries returned by change feed query.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a {@link CompletableFuture} that will eventually contain next continuation token or {@link CosmosException}
   *         in case of any error.
   */
  CompletableFuture<String> queryChangeFeedAsync(CosmosChangeFeedRequestOptions cosmosChangeFeedRequestOptions,
      List<CloudBlobMetadata> changeFeed, Timer timer) {

    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    azureMetrics.changeFeedQueryCount.inc();
    Timer.Context operationTime = timer.time();

    // Since the requestCharge is being used inside lambda expression below, it needs to be atomic.
    AtomicReference<String> continuationToken = new AtomicReference<>();

    // Query change feed
    CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse =
        cosmosAsyncContainer.queryChangeFeed(cosmosChangeFeedRequestOptions, CloudBlobMetadata.class);

    // Read the pages containing the cosmos change feed asynchronously and update the continuation token as we are reading.
    pagedFluxResponse.byPage().subscribe(fluxResponse -> {
      logger.debug("Got a page of query result with " + fluxResponse.getResults().size() + " items(s)"
          + " and request charge of " + fluxResponse.getRequestCharge());
      changeFeed.addAll(fluxResponse.getResults());
      continuationToken.set(fluxResponse.getContinuationToken());
    }, throwable -> {
      azureMetrics.changeFeedQueryFailureCount.inc();
      resultFuture.completeExceptionally(throwable);
      operationTime.stop();
    }, () -> {
      operationTime.stop();
      resultFuture.complete(continuationToken.get());
    });

    return resultFuture;
  }

  /**
   * Add the {@link CosmosContainerDeletionEntry} for newly deprecated {@link Container}s to cosmos table.
   * @param deprecatedContainers {@link Set} of deleted {@link CosmosContainerDeletionEntry}s.
   * @return the max deletion trigger time of all the added containers to serve as checkpoint for future update.
   * @throws CosmosException in case of any error.
   */
  public long deprecateContainers(Set<CosmosContainerDeletionEntry> deprecatedContainers) throws CosmosException {
    long latestContainerDeletionTimestamp = -1;

    CosmosAsyncContainer cosmosContainer = cosmosAsyncDatabase.getContainer(cosmosDeletedContainerCollection);
    CosmosContainerResponse cosmosContainerResponse = cosmosContainer.read().block();
    if (cosmosContainerResponse == null) {
      throw new IllegalStateException(
          "CosmosDB container for storing deprecated Ambry containers not found: " + cosmosDeletedContainerCollection);
    }

    for (CosmosContainerDeletionEntry containerDeletionEntry : deprecatedContainers) {
      try {
        executeCosmosAction(
            () -> cosmosContainer.createItem(containerDeletionEntry, new PartitionKey(containerDeletionEntry.getId()),
                new CosmosItemRequestOptions()).block(), azureMetrics.containerDeprecationDocumentCreateTime);
        if (containerDeletionEntry.getDeleteTriggerTimestamp() > latestContainerDeletionTimestamp) {
          latestContainerDeletionTimestamp = containerDeletionEntry.getDeleteTriggerTimestamp();
        }
      } catch (CosmosException cex) {
        if (cex.getStatusCode() == HttpConstants.StatusCodes.CONFLICT) {
          // If the cosmos exception is due to container being already deprecated, continue.
          logger.info("Container with accountid {} and containerid {} already deprecated. Skipping.",
              containerDeletionEntry.getAccountId(), containerDeletionEntry.getContainerId());
        } else {
          throw cex;
        }
      }
    }
    return latestContainerDeletionTimestamp;
  }

  /**
   * Fetch a {@link Set} of {@link CosmosContainerDeletionEntry} objects from cosmos db that are not marked as deleted.
   * @param maxEntries Max number of entries to fetch on one query.
   * @return {@link Set} of {@link CosmosContainerDeletionEntry} objects.
   * @throws CosmosException in case of any error.
   */
  public Set<CosmosContainerDeletionEntry> getDeprecatedContainers(int maxEntries) throws CosmosException {
    SqlQuerySpec sqlQuerySpec =
        new SqlQuerySpec(DEPRECATED_CONTAINERS_QUERY, new SqlParameter(LIMIT_PARAM, maxEntries));
    Set<CosmosContainerDeletionEntry> containerDeletionEntries = new HashSet<>();

    try {
      // Execute cosmos query
      CosmosPagedFlux<CosmosContainerDeletionEntry> pagedFluxResponse =
          executeCosmosQueryAsync(cosmosDeletedContainerCollection, sqlQuerySpec, new CosmosQueryRequestOptions(),
              CosmosContainerDeletionEntry.class);
      pagedFluxResponse.byPage().flatMapSequential(fluxResponse -> {
        containerDeletionEntries.addAll(fluxResponse.getResults());
        return Flux.empty();
      }).blockLast();
    } catch (Exception ex) {
      if (ex instanceof CosmosException) {
        //Client-specific errors
        CosmosException cex = (CosmosException) ex;
        logger.warn("Get deprecated containers query {} got {}", sqlQuerySpec.getQueryText(), cex.getStatusCode());
        throw cex;
      }
      throw ex;
    }

    return containerDeletionEntries;
  }

  /**
   * Update the container deletion entry document in the CosmosDB collection.
   * @param containerId the container id for which document is replaced.
   * @param accountId the account id for which document is replaced.
   * @param updateFields {@link BiConsumer} object to use as callback to update the required fields.
   * @return the {@link CosmosContainerDeletionEntry} entry if successful.
   * Returns {@Null} if the field already has the specified value.
   * @throws CosmosException if the record was not found or if the operation failed.
   */
  CosmosContainerDeletionEntry updateContainerDeletionEntry(short containerId, short accountId,
      BiConsumer<CosmosContainerDeletionEntry, AtomicBoolean> updateFields) throws CosmosException {

    CosmosAsyncContainer cosmosContainerForDeletedCollection =
        cosmosAsyncDatabase.getContainer(cosmosDeletedContainerCollection);
    CosmosContainerResponse cosmosContainerResponse = cosmosContainerForDeletedCollection.read().block();
    if (cosmosContainerResponse == null) {
      throw new IllegalStateException(
          "CosmosDB container for storing deprecated Ambry containers not found: " + cosmosDeletedContainerCollection);
    }

    // Read the existing record
    String id = CosmosContainerDeletionEntry.generateContainerDeletionEntryId(accountId, containerId);
    CosmosItemResponse<CosmosContainerDeletionEntry> cosmosItemResponse = executeCosmosAction(
        () -> cosmosContainerForDeletedCollection.readItem(id, new PartitionKey(id), CosmosContainerDeletionEntry.class)
            .block(), azureMetrics.continerDeletionEntryReadTime);
    CosmosContainerDeletionEntry containerDeletionEntry = cosmosItemResponse.getItem();

    // Update the record
    AtomicBoolean fieldsChanged = new AtomicBoolean(false);
    updateFields.accept(containerDeletionEntry, fieldsChanged);
    if (!fieldsChanged.get()) {
      logger.debug("No change in value for container deletion entry {}", containerDeletionEntry.toJson());
      return null;
    }

    // For testing conflict handling
    if (updateCallback != null) {
      try {
        updateCallback.call();
      } catch (Exception ex) {
        logger.error("Error in update callback", ex);
      }
    }

    // Set condition to ensure we don't clobber a concurrent update and replace the record.
    CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
    requestOptions.setIfMatchETag(cosmosItemResponse.getETag());
    try {
      return executeCosmosAction(
          () -> cosmosContainerForDeletedCollection.replaceItem(containerDeletionEntry, id, new PartitionKey(id),
              requestOptions).block(), azureMetrics.documentUpdateTime).getItem();
    } catch (CosmosException cex) {
      if (cex.getStatusCode() == HttpConstants.StatusCodes.PRECONDITION_FAILED) {
        // Keep track of failures due to conflicts.
        azureMetrics.blobUpdateConflictCount.inc();
      }
      throw cex;
    }
  }

  /**
   * Utility method to call a Cosmos method.
   * @param action the action to call.
   * @param timer the {@link Timer} to use to time the action.  May be null.
   * @return the result of the action.
   * @throws CosmosException in case of any error while executing the Cosmos method.
   */
  static <T> CosmosItemResponse<T> executeCosmosAction(Callable<? extends CosmosItemResponse<T>> action, Timer timer)
      throws CosmosException {
    CosmosItemResponse<T> cosmosItemResponse;
    Timer.Context operationTimer = null;
    try {
      if (timer != null) {
        operationTimer = timer.time();
      }
      cosmosItemResponse = action.call();
    } catch (Exception ex) {
      if (ex instanceof CosmosException) {
        throw (CosmosException) ex;
      }
      throw new RuntimeException("Exception calling action " + action, ex);
    } finally {
      if (operationTimer != null) {
        operationTimer.stop();
      }
    }
    return cosmosItemResponse;
  }

  /**
   * Utility method to call Cosmos document query method and record the query time.
   * @param sqlQuerySpec the DocumentDB query to execute.
   * @param cosmosQueryRequestOptions {@link CosmosQueryRequestOptions} object specifying the options associated with the method.
   * @return {@link CosmosPagedFlux} containing the query response.
   */
  private CosmosPagedFlux<CloudBlobMetadata> executeCosmosQueryAsync(SqlQuerySpec sqlQuerySpec,
      CosmosQueryRequestOptions cosmosQueryRequestOptions) {
    return executeCosmosQueryAsync(cosmosCollection, sqlQuerySpec, cosmosQueryRequestOptions, CloudBlobMetadata.class);
  }

  /**
   * Utility method to call Cosmos document query method and record the query time.
   * @param containerName collection link of the collection to execute query on.
   * @param sqlQuerySpec the DocumentDB query to execute.
   * @param cosmosQueryRequestOptions {@link CosmosQueryRequestOptions} object specifying the options associated with the method.
   * @param classType type of Class.
   * @return {@link CosmosPagedFlux} containing the query response.
   */
  <T> CosmosPagedFlux<T> executeCosmosQueryAsync(String containerName, SqlQuerySpec sqlQuerySpec,
      CosmosQueryRequestOptions cosmosQueryRequestOptions, Class<T> classType) {
    azureMetrics.documentQueryCount.inc();
    // Check for null value of partition key.
    if (cosmosQueryRequestOptions.getPartitionKey() != null && !Utils.isNullOrEmpty(
        cosmosQueryRequestOptions.getPartitionKey().toString())) {
      logger.debug("Running query on partition {}: {}", cosmosQueryRequestOptions.getPartitionKey().toString(),
          sqlQuerySpec.getQueryText());
    } else {
      logger.debug("Running query on partition {}", sqlQuerySpec.getQueryText());
    }

    CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(containerName);
    return cosmosAsyncContainer.queryItems(sqlQuerySpec, cosmosQueryRequestOptions, classType);
  }

  /**
   * Fetch the key either directly from configs, or indirectly by looking for it in an Azure KeyVault.
   * @param azureCloudConfig the config
   * @return the CosmosDB key.
   */
  private static String getCosmosKey(AzureCloudConfig azureCloudConfig) {
    if (!azureCloudConfig.cosmosKey.isEmpty()) {
      return azureCloudConfig.cosmosKey;
    }
    if (azureCloudConfig.cosmosKeySecretName.isEmpty() || azureCloudConfig.cosmosVaultUrl.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("One of the required configs for fetching the cosmos key from a keyvault (%s, %s) missing",
              AzureCloudConfig.COSMOS_KEY_SECRET_NAME, AzureCloudConfig.COSMOS_VAULT_URL));
    }
    // check that all required azure identity configs are present if keyvault lookup is used.
    AzureUtils.validateAzureIdentityConfigs(azureCloudConfig);
    SecretClient secretClient = new SecretClientBuilder().vaultUrl(azureCloudConfig.cosmosVaultUrl)
        .credential(AzureUtils.getClientSecretCredential(azureCloudConfig))
        .buildClient();
    return secretClient.getSecret(azureCloudConfig.cosmosKeySecretName).getValue();
  }
}
