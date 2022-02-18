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
import com.azure.cosmos.*;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import rx.observables.BlockingObservable;


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
  static final String BULK_DELETE_SPROC_ID = "BulkDelete";
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
   * Upsert the blob metadata document in the CosmosDB collection.
   * @param blobMetadata the blob metadata document.
   * @return the {@link CosmosItemResponse} containing {@link CloudBlobMetadata} returned by the operation, if successful.
   * @throws CosmosException if the operation failed.
   */
  CosmosItemResponse<CloudBlobMetadata> upsertMetadata(CloudBlobMetadata blobMetadata) throws CosmosException {
    return executeCosmosAction(
        () -> cosmosAsyncContainer.upsertItem(blobMetadata, new PartitionKey(blobMetadata.getPartitionId()),
            new CosmosItemRequestOptions()).block(), azureMetrics.documentCreateTime);
  }

  /**
   * Delete the blob metadata document in the CosmosDB collection, if it exists.
   * @param blobMetadata the blob metadata document to delete.
   * @return {@code true} if the record was deleted, {@code false} if it was not found.
   * @throws CosmosException if the operation failed.
   */
  boolean deleteMetadata(CloudBlobMetadata blobMetadata) throws CosmosException {
    // Note: not timing here since bulk deletions are timed.
    try {
      executeCosmosAction(
          () -> cosmosAsyncContainer.deleteItem(blobMetadata.getId(), new PartitionKey(blobMetadata.getPartitionId()))
              .block(), null);
      return true;
    } catch (CosmosException cex) {
      if (cex.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND) {
        // Can happen on retry
        logger.debug("Could not find metadata for blob {} to delete", blobMetadata.getId());
        return false;
      }
      throw cex;
    }
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
      for (CloudBlobMetadata metadata : blobMetadataList) {
        deleteMetadata(metadata);
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
   * Get the metadata record for a single blob.
   * @param blobId the blob to read.
   * @return the {@link CloudBlobMetadata} for the blob if it is found, otherwise null.
   * @throws CosmosException on any other error.
   */
  CloudBlobMetadata getMetadataOrNull(BlobId blobId) throws CosmosException {
    try {
      CosmosItemResponse<CloudBlobMetadata> readResponse = executeCosmosAction(
          () -> cosmosAsyncContainer.readItem(blobId.getID(), new PartitionKey(blobId.getPartition().toPathString()),
              CloudBlobMetadata.class).block(), azureMetrics.documentReadTime);
      return readResponse.getItem();
    } catch (CosmosException cex) {
      if (cex.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND) {
        return null;
      }
      throw cex;
    }
  }

  /**
   * Update the blob metadata document in the CosmosDB collection.
   * @param blobId the {@link BlobId} for which metadata is replaced.
   * @param updateFields Map of field names and new values to update.
   * @return the {@link CosmosItemResponse} containing {@link CloudBlobMetadata} object returned by the operation, if successful.
   * Returns Null if the field already has the specified value.
   * @throws CosmosException if the record was not found or if the operation failed.
   */
  CosmosItemResponse<CloudBlobMetadata> updateMetadata(BlobId blobId, Map<String, String> updateFields)
      throws CosmosException {

    // Read the existing record
    CosmosItemResponse<CloudBlobMetadata> cosmosItemResponse = executeCosmosAction(
        () -> cosmosAsyncContainer.readItem(blobId.getID(), new PartitionKey(blobId.getPartition().toPathString()),
            CloudBlobMetadata.class).block(), azureMetrics.documentReadTime);
    CloudBlobMetadata receivedMetadata = cosmosItemResponse.getItem();

    // Update only if value has changed
    Map<String, String> receivedFields = receivedMetadata.toMap();
    Map<String, String> fieldsToUpdate = updateFields.entrySet()
        .stream()
        .filter(map -> !String.valueOf(updateFields.get(map.getKey())).equals(receivedFields.get(map.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, map -> String.valueOf(map.getValue())));
    if (fieldsToUpdate.size() == 0) {
      logger.debug("No change in value for {} in blob {}", updateFields.keySet(), blobId.getID());
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

    // Update the modified fields
    fieldsToUpdate.forEach(receivedFields::put);
    // Set condition to ensure we don't clobber a concurrent update
    CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
    requestOptions.setIfMatchETag(cosmosItemResponse.getETag());

    // Replace the record
    try {
      return executeCosmosAction(
          () -> cosmosAsyncContainer.replaceItem(CloudBlobMetadata.fromMap(receivedFields), blobId.getID(),
              new PartitionKey(blobId.getPartition().toPathString()), requestOptions).block(),
          azureMetrics.documentUpdateTime);
    } catch (CosmosException cex) {
      if (cex.getStatusCode() == HttpConstants.StatusCodes.PRECONDITION_FAILED) {
        azureMetrics.blobUpdateConflictCount.inc();
      }
      throw cex;
    }
  }

  /**
   * Get the list of blobs in the specified partition that have been deleted or expired for at least the
   * configured retention period.
   * @param partitionPath the partition to query.
   * @param fieldName the field name to query on. Allowed values are {@link CloudBlobMetadata#FIELD_DELETION_TIME} and
   *                  {@link CloudBlobMetadata#FIELD_EXPIRATION_TIME}.
   * @param startTime the start of the query time range.
   * @param endTime the end of the query time range.
   * @param maxEntries the max number of metadata records to return.
   * @return a List of {@link CloudBlobMetadata} objects referencing the dead blobs found.
   * @throws CosmosException if the operation failed.
   */
  List<CloudBlobMetadata> getDeadBlobs(String partitionPath, String fieldName, long startTime, long endTime,
      int maxEntries) throws CosmosException {

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

    try {
      List<CloudBlobMetadata> deadBlobsList = new ArrayList<>();
      AtomicReference<Double> requestCharge = new AtomicReference<>(0.0);

      // Execute cosmos query
      CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse =
          executeCosmosQuery(sqlQuerySpec, queryRequestOptions, azureMetrics.deadBlobsQueryTime);
      pagedFluxResponse.byPage(maxEntries).flatMapSequential(fluxResponse -> {
        requestCharge.updateAndGet(v -> v + fluxResponse.getRequestCharge());
        deadBlobsList.addAll(fluxResponse.getResults());
        return Flux.empty();
      }).blockLast();

      if (requestCharge.get() >= requestChargeThreshold) {
        logger.info("Dead blobs query partition {} endTime {} request charge {} for {} records", partitionPath,
            new Date(endTime), requestCharge.get(), deadBlobsList.size());
      }

      return deadBlobsList;
    } catch (Exception ex) {
      if (ex instanceof CosmosException) {
        //Client-specific errors
        CosmosException cex = (CosmosException) ex;
        logger.warn("Dead blobs query {} partition {} got {}", deadBlobsQuery, partitionPath, cex.getStatusCode());
        throw cex;
      }
      throw ex;
    }
  }

  /**
   * Get the list of blobs in the specified partition that belong to the specified container.
   * @param partitionPath the partition to query.
   * @param accountId account id of the container.
   * @param containerId container id of the container.
   * @param queryLimit max number of blobs to return.
   * @return a List of {@link CloudBlobMetadata} objects referencing the blobs belonging to the deprecated containers.
   * @throws CosmosException in case of any error.
   */
  List<CloudBlobMetadata> getContainerBlobs(String partitionPath, short accountId, short containerId, int queryLimit)
      throws CosmosException {

    SqlQuerySpec sqlQuerySpec = new SqlQuerySpec(CONTAINER_BLOBS_QUERY, new SqlParameter(LIMIT_PARAM, queryLimit),
        new SqlParameter(ACCOUNT_ID_PARAM, accountId), new SqlParameter(CONTAINER_ID_PARAM, containerId));

    CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();
    queryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
    queryRequestOptions.setResponseContinuationTokenLimitInKb(continuationTokenLimitKb);

    try {
      List<CloudBlobMetadata> containerBlobsList = new ArrayList<>();
      AtomicReference<Double> requestCharge = new AtomicReference<>(0.0);

      // Execute cosmos query
      CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse =
          executeCosmosQuery(sqlQuerySpec, queryRequestOptions, azureMetrics.deletedContainerBlobsQueryTime);
      pagedFluxResponse.byPage(queryLimit).flatMapSequential(fluxResponse -> {
        requestCharge.updateAndGet(v -> v + fluxResponse.getRequestCharge());
        containerBlobsList.addAll(fluxResponse.getResults());
        return Flux.empty();
      }).blockLast();

      if (requestCharge.get() >= requestChargeThreshold) {
        logger.info("Container blobs query partition {} containerId {} accountId {} request charge {} for {} records",
            partitionPath, containerId, accountId, requestCharge, containerBlobsList.size());
      }

      return containerBlobsList;
    } catch (Exception ex) {
      if (ex instanceof CosmosException) {
        //Client-specific errors
        CosmosException cex = (CosmosException) ex;
        logger.warn("Container blobs query {} partition {} got {}", sqlQuerySpec.getQueryText(), partitionPath,
            cex.getStatusCode());
        throw cex;
      }
      throw ex;
    }
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
   * Get the list of blobs in the specified partition matching the specified DocumentDB query.
   * @param partitionPath the partition to query.
   * @param queryText the DocumentDB query to execute.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a List of {@link CloudBlobMetadata} referencing the matching blobs.
   */
  List<CloudBlobMetadata> queryMetadata(String partitionPath, String queryText, Timer timer) throws CosmosException {
    return queryMetadata(partitionPath, new SqlQuerySpec(queryText), timer);
  }

  /**
   * Get the list of blobs in the specified partition matching the specified DocumentDB query spec.
   * @param partitionPath the partition to query.
   * @param sqlQuerySpec representing the sql query.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a List of {@link CloudBlobMetadata} referencing the matching blobs.
   */
  List<CloudBlobMetadata> queryMetadata(String partitionPath, SqlQuerySpec sqlQuerySpec, Timer timer)
      throws CosmosException {

    CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();
    queryRequestOptions.setPartitionKey(new PartitionKey(partitionPath));
    queryRequestOptions.setResponseContinuationTokenLimitInKb(continuationTokenLimitKb);

    try {
      List<CloudBlobMetadata> metadataList = new ArrayList<>();
      AtomicReference<Double> requestCharge = new AtomicReference<>(0.0);

      // Execute cosmos query
      CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse =
          executeCosmosQuery(sqlQuerySpec, queryRequestOptions, timer);
      pagedFluxResponse.byPage().flatMapSequential(fluxResponse -> {
        requestCharge.updateAndGet(v -> v + fluxResponse.getRequestCharge());
        metadataList.addAll(fluxResponse.getResults());
        return Flux.empty();
      }).blockLast();

      if (requestCharge.get() >= requestChargeThreshold) {
        logger.info("Query partition {} request charge {} for {} records", partitionPath, requestCharge,
            metadataList.size());
      }

      return metadataList;
    } catch (Exception ex) {
      if (ex instanceof CosmosException) {
        //Client-specific errors
        CosmosException cex = (CosmosException) ex;
        logger.warn("Query {} on partition {} got {}", sqlQuerySpec.getQueryText(), partitionPath, cex.getStatusCode());
        throw cex;
      }
      throw ex;
    }
  }

  /**
   * Query Cosmos change feed to get the next set of {@code CloudBlobMetadata} objects in specified {@code partitionPath}
   * after {@code requestContinationToken}, capped by specified {@code maxFeedSize} representing the max number of items to
   * be queried from the change feed.
   * @param requestContinuationToken Continuation token after which change feed is requested.
   * @param maxFeedSize max item count to be requested in the feed query.
   * @param changeFeed {@link CloudBlobMetadata} {@code List} to be populated with the next set of entries returned by change feed query.
   * @param partitionPath partition for which the change feed is requested.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return next continuation token.
   * @throws CosmosException in case of any error.
   */
  public String queryChangeFeed(String requestContinuationToken, int maxFeedSize, List<CloudBlobMetadata> changeFeed,
      String partitionPath, Timer timer) throws CosmosException {
    CosmosChangeFeedRequestOptions cosmosChangeFeedRequestOptions;
    if (Utils.isNullOrEmpty(requestContinuationToken)) {
      cosmosChangeFeedRequestOptions =
          CosmosChangeFeedRequestOptions.createForProcessingFromBeginning(FeedRange.forFullRange());
    } else {
      cosmosChangeFeedRequestOptions =
          CosmosChangeFeedRequestOptions.createForProcessingFromContinuation(requestContinuationToken);
    }
    cosmosChangeFeedRequestOptions.setMaxItemCount(maxFeedSize);
    return queryChangeFeed(cosmosChangeFeedRequestOptions, changeFeed, timer);
  }

  /**
   * Query Cosmos change feed to get the next set of {@code CloudBlobMetadata} objects.
   * @param cosmosChangeFeedRequestOptions {@link CosmosChangeFeedRequestOptions} containing the options for the change feed request.
   * @param changeFeed {@link CloudBlobMetadata} {@code List} to be populated with the next set of entries returned by change feed query.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return timer the {@link Timer} to use to record query time (excluding waiting).
   */
  String queryChangeFeed(CosmosChangeFeedRequestOptions cosmosChangeFeedRequestOptions,
      List<CloudBlobMetadata> changeFeed, Timer timer) {

    azureMetrics.changeFeedQueryCount.inc();
    AtomicReference<String> continuationToken = new AtomicReference<>();

    try {
      CosmosPagedFlux<CloudBlobMetadata> pagedFluxResponse =
          executeCosmosChangeFeedQuery(cosmosChangeFeedRequestOptions, timer);
      pagedFluxResponse.byPage().flatMapSequential(fluxResponse -> {
        changeFeed.addAll(fluxResponse.getResults());
        continuationToken.set(fluxResponse.getContinuationToken());
        return Flux.empty();
      }).blockLast();

      return continuationToken.get();
    } catch (Exception ex) {
      azureMetrics.changeFeedQueryFailureCount.inc();
      if (ex instanceof CosmosException) {
        throw (CosmosException) ex;
      }
      throw ex;
    }
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
          "CosmosDB container for storing deprecated Ambry containers not found: " + cosmosCollection);
    }

    for (CosmosContainerDeletionEntry containerDeletionEntry : deprecatedContainers) {
      executeCosmosAction(
          () -> cosmosContainer.createItem(containerDeletionEntry, new PartitionKey(containerDeletionEntry.getId()),
              new CosmosItemRequestOptions()).block(), azureMetrics.containerDeprecationDocumentCreateTime);
      if (containerDeletionEntry.getDeleteTriggerTimestamp() > latestContainerDeletionTimestamp) {
        latestContainerDeletionTimestamp = containerDeletionEntry.getDeleteTriggerTimestamp();
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
    Timer timer = new Timer();
    Set<CosmosContainerDeletionEntry> containerDeletionEntries = new HashSet<>();

    try {
      // Execute cosmos query
      CosmosPagedFlux<CosmosContainerDeletionEntry> pagedFluxResponse =
          executeCosmosQuery(cosmosDeletedContainerCollection, sqlQuerySpec, new CosmosQueryRequestOptions(),
              CosmosContainerDeletionEntry.class, timer);
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

    CosmosAsyncContainer cosmosContainer = cosmosAsyncDatabase.getContainer(cosmosDeletedContainerCollection);
    CosmosContainerResponse cosmosContainerResponse = cosmosContainer.read().block();
    if (cosmosContainerResponse == null) {
      throw new IllegalStateException(
          "CosmosDB container for storing deprecated Ambry containers not found: " + cosmosCollection);
    }

    // Read the existing record
    String id = CosmosContainerDeletionEntry.generateContainerDeletionEntryId(accountId, containerId);
    CosmosItemResponse<CosmosContainerDeletionEntry> cosmosItemResponse = executeCosmosAction(
        () -> cosmosContainer.readItem(id, new PartitionKey(id), CosmosContainerDeletionEntry.class).block(),
        azureMetrics.continerDeletionEntryReadTime);
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
    return executeCosmosAction(
        () -> cosmosAsyncContainer.replaceItem(containerDeletionEntry, id, new PartitionKey(id), requestOptions)
            .block(), azureMetrics.documentUpdateTime).getItem();
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
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return {@link BlockingObservable} object containing the query response.
   */
  private CosmosPagedFlux<CloudBlobMetadata> executeCosmosQuery(SqlQuerySpec sqlQuerySpec,
      CosmosQueryRequestOptions cosmosQueryRequestOptions, Timer timer) {
    return executeCosmosQuery(cosmosCollection, sqlQuerySpec, cosmosQueryRequestOptions, CloudBlobMetadata.class,
        timer);
  }

  /**
   * Utility method to call Cosmos document query method and record the query time.
   * @param containerName collection link of the collection to execute query on.
   * @param sqlQuerySpec the DocumentDB query to execute.
   * @param cosmosQueryRequestOptions {@link CosmosQueryRequestOptions} object specifying the options associated with the method.
   * @param classType type of Class.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return {@link BlockingObservable} object containing the query response.
   */
  <T> CosmosPagedFlux<T> executeCosmosQuery(String containerName, SqlQuerySpec sqlQuerySpec,
      CosmosQueryRequestOptions cosmosQueryRequestOptions, Class<T> classType, Timer timer) {
    azureMetrics.documentQueryCount.inc();
    if (!Utils.isNullOrEmpty(cosmosQueryRequestOptions.getPartitionKey().toString())) {
      logger.debug("Running query on partition {}: {}", cosmosQueryRequestOptions.getPartitionKey().toString(),
          sqlQuerySpec.getQueryText());
    } else {
      logger.debug("Running query on partition {}", sqlQuerySpec.getQueryText());
    }
    Timer.Context operationTimer = timer.time();
    try {
      return cosmosAsyncDatabase.getContainer(containerName)
          .queryItems(sqlQuerySpec, cosmosQueryRequestOptions, classType);
    } finally {
      operationTimer.stop();
    }
  }

  /**
   * Utility method to call Cosmos change feed query method and record the query time.
   * @param changeFeedOptions {@link CosmosChangeFeedRequestOptions} object specifying the options associated with the method.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return {@link CosmosPagedFlux} object representing the query response in the form of pages.
   */
  private CosmosPagedFlux<CloudBlobMetadata> executeCosmosChangeFeedQuery(
      CosmosChangeFeedRequestOptions changeFeedOptions, Timer timer) {
    Timer.Context operationTimer = timer.time();
    try {
      // FIXME: Using single() for the observable returned by toBlocking() works for now. But if a high enough maxFeedSize
      //  is passed, to result in multiple feed pages, single() will throw an exception.
      return cosmosAsyncContainer.queryChangeFeed(changeFeedOptions, CloudBlobMetadata.class);
    } finally {
      operationTimer.stop();
    }
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
