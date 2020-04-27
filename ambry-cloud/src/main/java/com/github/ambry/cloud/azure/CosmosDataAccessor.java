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
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.AccessCondition;
import com.microsoft.azure.cosmosdb.ChangeFeedOptions;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.RetryOptions;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.observables.BlockingObservable;


public class CosmosDataAccessor {
  private static final Logger logger = LoggerFactory.getLogger(CosmosDataAccessor.class);
  private static final String DOCS = "/docs/";
  public static final String COSMOS_LAST_UPDATED_COLUMN = "_ts";
  private static final String START_TIME_PARAM = "@startTime";
  private static final String END_TIME_PARAM = "@endTime";
  private static final String LIMIT_PARAM = "@limit";
  private static final String EXPIRED_BLOBS_QUERY = constructDeadBlobsQuery(CloudBlobMetadata.FIELD_EXPIRATION_TIME);
  private static final String DELETED_BLOBS_QUERY = constructDeadBlobsQuery(CloudBlobMetadata.FIELD_DELETION_TIME);
  private final AsyncDocumentClient asyncDocumentClient;
  private final String cosmosCollectionLink;
  private final AzureMetrics azureMetrics;
  private Callable<?> updateCallback = null;
  private final int continuationTokenLimitKb;
  private final int requestChargeThreshold;

  /** Production constructor */
  CosmosDataAccessor(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, AzureMetrics azureMetrics) {
    // Set up CosmosDB connection, including retry options and any proxy setting
    ConnectionPolicy connectionPolicy = new ConnectionPolicy();
    // TODO: would like to use different timeouts for queries and single-doc reads/writes
    connectionPolicy.setRequestTimeoutInMillis(cloudConfig.cloudQueryRequestTimeout);
    // Note: retry decisions are made at CloudBlobStore level.  Configure Cosmos with no retries.
    RetryOptions noRetries = new RetryOptions();
    noRetries.setMaxRetryAttemptsOnThrottledRequests(0);
    connectionPolicy.setRetryOptions(noRetries);
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

    this.cosmosCollectionLink = azureCloudConfig.cosmosCollectionLink;
    this.continuationTokenLimitKb = azureCloudConfig.cosmosContinuationTokenLimitKb;
    this.requestChargeThreshold = azureCloudConfig.cosmosRequestChargeThreshold;
    this.azureMetrics = azureMetrics;
  }

  /** Test constructor */
  CosmosDataAccessor(AsyncDocumentClient asyncDocumentClient, String cosmosCollectionLink, AzureMetrics azureMetrics) {
    this.asyncDocumentClient = asyncDocumentClient;
    this.cosmosCollectionLink = cosmosCollectionLink;
    this.continuationTokenLimitKb = AzureCloudConfig.DEFAULT_COSMOS_CONTINUATION_TOKEN_LIMIT;
    this.requestChargeThreshold = AzureCloudConfig.DEFAULT_COSMOS_REQUEST_CHARGE_THRESHOLD;
    this.azureMetrics = azureMetrics;
  }

  /** Visible for testing */
  void setUpdateCallback(Callable<?> callback) {
    this.updateCallback = callback;
  }

  /**
   * Test connectivity to Azure CosmosDB
   */
  void testConnectivity() {
    ResourceResponse<DocumentCollection> response =
        asyncDocumentClient.readCollection(cosmosCollectionLink, new RequestOptions()).toBlocking().single();
    if (response.getResource() == null) {
      throw new IllegalStateException("CosmosDB collection not found: " + cosmosCollectionLink);
    }
    logger.info("CosmosDB connection test succeeded.");
  }

  /**
   * Upsert the blob metadata document in the CosmosDB collection.
   * @param blobMetadata the blob metadata document.
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * @throws DocumentClientException if the operation failed.
   */
  ResourceResponse<Document> upsertMetadata(CloudBlobMetadata blobMetadata) throws DocumentClientException {
    RequestOptions options = getRequestOptions(blobMetadata.getPartitionId());
    return executeCosmosAction(
        () -> asyncDocumentClient.upsertDocument(cosmosCollectionLink, blobMetadata, options, true)
            .toBlocking()
            .single(), azureMetrics.documentCreateTime);
  }

  /**
   * Delete the blob metadata document in the CosmosDB collection.
   * @param blobMetadata the blob metadata document.
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * @throws DocumentClientException if the operation failed.
   */
  ResourceResponse<Document> deleteMetadata(CloudBlobMetadata blobMetadata) throws DocumentClientException {
    String docLink = getDocumentLink(blobMetadata.getId());
    RequestOptions options = getRequestOptions(blobMetadata.getPartitionId());
    return executeCosmosAction(() -> asyncDocumentClient.deleteDocument(docLink, options).toBlocking().single(),
        azureMetrics.documentDeleteTime);
  }

  /**
   * Get the metadata record for a single blob.
   * @param blobId the blob to read.
   * @return the {@link CloudBlobMetadata} for the blob if it is found, otherwise null.
   * @throws DocumentClientException on any other error.
   */
  CloudBlobMetadata getMetadataOrNull(BlobId blobId) throws DocumentClientException {
    String docLink = getDocumentLink(blobId.getID());
    RequestOptions options = getRequestOptions(blobId.getPartition().toPathString());
    try {
      ResourceResponse<Document> readResponse =
          executeCosmosAction(() -> asyncDocumentClient.readDocument(docLink, options).toBlocking().single(),
              azureMetrics.documentReadTime);
      return createMetadataFromDocument(readResponse.getResource());
    } catch (DocumentClientException dex) {
      if (dex.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND) {
        return null;
      } else {
        throw dex;
      }
    }
  }

  /**
   * Update the blob metadata document in the CosmosDB collection.
   * @param blobId the {@link BlobId} for which metadata is replaced.
   * @param fieldName the metadata field to update.
   * @param value the new value for the field.
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * Returns {@Null} if the field already has the specified value.
   * @throws DocumentClientException if the record was not found or if the operation failed.
   */
  ResourceResponse<Document> updateMetadata(BlobId blobId, String fieldName, Object value)
      throws DocumentClientException {

    // Read the existing record
    String docLink = getDocumentLink(blobId.getID());
    RequestOptions options = getRequestOptions(blobId.getPartition().toPathString());
    ResourceResponse<Document> readResponse =
        executeCosmosAction(() -> asyncDocumentClient.readDocument(docLink, options).toBlocking().single(),
            azureMetrics.documentReadTime);
    Document doc = readResponse.getResource();

    // Update only if value has changed
    if (value.equals(doc.get(fieldName))) {
      logger.debug("No change in value for {} in blob {}", fieldName, blobId.getID());
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

    // Perform the update
    doc.set(fieldName, value);
    // Set condition to ensure we don't clobber a concurrent update
    AccessCondition accessCondition = new AccessCondition();
    accessCondition.setCondition(doc.getETag());
    options.setAccessCondition(accessCondition);
    try {
      return executeCosmosAction(() -> asyncDocumentClient.replaceDocument(doc, options).toBlocking().single(),
          azureMetrics.documentUpdateTime);
    } catch (DocumentClientException e) {
      if (e.getStatusCode() == HttpConstants.StatusCodes.PRECONDITION_FAILED) {
        azureMetrics.blobUpdateConflictCount.inc();
      }
      throw e;
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
   * @return a List of {@link CloudBlobMetadata} referencing the dead blobs found.
   * @throws DocumentClientException
   */
  List<CloudBlobMetadata> getDeadBlobs(String partitionPath, String fieldName, long startTime, long endTime,
      int maxEntries) throws DocumentClientException {

    String deadBlobsQuery;
    if (fieldName.equals(CloudBlobMetadata.FIELD_DELETION_TIME)) {
      deadBlobsQuery = DELETED_BLOBS_QUERY;
    } else if (fieldName.equals(CloudBlobMetadata.FIELD_EXPIRATION_TIME)) {
      deadBlobsQuery = EXPIRED_BLOBS_QUERY;
    } else {
      throw new IllegalArgumentException("Invalid field: " + fieldName);
    }
    SqlQuerySpec querySpec = new SqlQuerySpec(deadBlobsQuery,
        new SqlParameterCollection(new SqlParameter(LIMIT_PARAM, maxEntries),
            new SqlParameter(START_TIME_PARAM, startTime), new SqlParameter(END_TIME_PARAM, endTime)));

    FeedOptions feedOptions = new FeedOptions();
    feedOptions.setMaxItemCount(maxEntries);
    feedOptions.setResponseContinuationTokenLimitInKb(continuationTokenLimitKb);
    feedOptions.setPartitionKey(new PartitionKey(partitionPath));
    try {
      Iterator<FeedResponse<Document>> iterator =
          executeCosmosQuery(partitionPath, querySpec, feedOptions, azureMetrics.deadBlobsQueryTime).getIterator();
      List<CloudBlobMetadata> deadBlobsList = new ArrayList<>();
      double requestCharge = 0.0;
      while (iterator.hasNext()) {
        FeedResponse<Document> response = iterator.next();
        requestCharge += response.getRequestCharge();
        response.getResults().iterator().forEachRemaining(doc -> deadBlobsList.add(createMetadataFromDocument(doc)));
      }
      if (requestCharge >= requestChargeThreshold) {
        logger.info("Dead blobs query on partition {} got request charge {} for {} records", partitionPath,
            requestCharge, deadBlobsList.size());
      }
      return deadBlobsList;
    } catch (RuntimeException rex) {
      if (rex.getCause() instanceof DocumentClientException) {
        logger.warn("Dead blobs query {} on partition {} got {}", deadBlobsQuery, partitionPath,
            ((DocumentClientException) rex.getCause()).getStatusCode());
        throw (DocumentClientException) rex.getCause();
      }
      throw rex;
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
  List<CloudBlobMetadata> queryMetadata(String partitionPath, String queryText, Timer timer)
      throws DocumentClientException {
    return queryMetadata(partitionPath, new SqlQuerySpec(queryText), timer);
  }

  /**
   * Get the list of blobs in the specified partition matching the specified DocumentDB query spec.
   * @param partitionPath the partition to query.
   * @param querySpec the DocumentDB {@link SqlQuerySpec} to execute.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a List of {@link CloudBlobMetadata} referencing the matching blobs.
   */
  List<CloudBlobMetadata> queryMetadata(String partitionPath, SqlQuerySpec querySpec, Timer timer)
      throws DocumentClientException {
    FeedOptions feedOptions = new FeedOptions();
    // TODO: set maxItemCount
    feedOptions.setResponseContinuationTokenLimitInKb(continuationTokenLimitKb);
    feedOptions.setPartitionKey(new PartitionKey(partitionPath));
    // TODO: consolidate error count here
    try {
      Iterator<FeedResponse<Document>> iterator =
          executeCosmosQuery(partitionPath, querySpec, feedOptions, timer).getIterator();
      List<CloudBlobMetadata> metadataList = new ArrayList<>();
      double requestCharge = 0.0;
      while (iterator.hasNext()) {
        FeedResponse<Document> response = iterator.next();
        requestCharge += response.getRequestCharge();
        response.getResults().iterator().forEachRemaining(doc -> metadataList.add(createMetadataFromDocument(doc)));
      }
      if (requestCharge >= requestChargeThreshold) {
        logger.info("Query partition {} request charge {} for {} records", partitionPath, requestCharge,
            metadataList.size());
      }
      return metadataList;
    } catch (RuntimeException rex) {
      if (rex.getCause() instanceof DocumentClientException) {
        logger.warn("Query {} on partition {} got {}", querySpec.getQueryText(), partitionPath,
            ((DocumentClientException) rex.getCause()).getStatusCode());
        throw (DocumentClientException) rex.getCause();
      }
      throw rex;
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
   * @throws DocumentClientException
   */
  public String queryChangeFeed(String requestContinuationToken, int maxFeedSize, List<CloudBlobMetadata> changeFeed,
      String partitionPath, Timer timer) throws DocumentClientException {
    azureMetrics.changeFeedQueryCount.inc();
    ChangeFeedOptions changeFeedOptions = new ChangeFeedOptions();
    changeFeedOptions.setPartitionKey(new PartitionKey(partitionPath));
    changeFeedOptions.setMaxItemCount(maxFeedSize);
    if (Utils.isNullOrEmpty(requestContinuationToken)) {
      changeFeedOptions.setStartFromBeginning(true);
    } else {
      changeFeedOptions.setRequestContinuation(requestContinuationToken);
    }
    try {
      FeedResponse<Document> feedResponse = executeCosmosChangeFeedQuery(changeFeedOptions, timer);
      feedResponse.getResults().stream().map(doc -> createMetadataFromDocument(doc)).forEach(changeFeed::add);
      return feedResponse.getResponseContinuation();
    } catch (RuntimeException rex) {
      azureMetrics.changeFeedQueryFailureCount.inc();
      if (rex.getCause() instanceof DocumentClientException) {
        throw (DocumentClientException) rex.getCause();
      }
      throw rex;
    } catch (Exception ex) {
      azureMetrics.changeFeedQueryFailureCount.inc();
      throw ex;
    }
  }

  /**
   * Create {@link CloudBlobMetadata} object from {@link Document} object.
   * @param document {@link Document} object from which {@link CloudBlobMetadata} object will be created.
   * @return {@link CloudBlobMetadata} object.
   */
  private CloudBlobMetadata createMetadataFromDocument(Document document) {
    CloudBlobMetadata cloudBlobMetadata = document.toObject(CloudBlobMetadata.class);
    cloudBlobMetadata.setLastUpdateTime(document.getLong(COSMOS_LAST_UPDATED_COLUMN));
    return cloudBlobMetadata;
  }

  /**
   * Utility method to call a Cosmos method and extract any nested DocumentClientException.
   * @param action the action to call.
   * @return the result of the action.
   * @throws DocumentClientException
   */
  private ResourceResponse<Document> executeCosmosAction(Callable<? extends ResourceResponse<Document>> action,
      Timer timer) throws DocumentClientException {
    ResourceResponse<Document> resourceResponse;
    Timer.Context operationTimer = null;
    try {
      operationTimer = timer.time();
      resourceResponse = action.call();
      // TODO: add partition, tally request charge per partition and log on 429
    } catch (RuntimeException rex) {
      if (rex.getCause() instanceof DocumentClientException) {
        throw (DocumentClientException) rex.getCause();
      }
      throw rex;
    } catch (Exception ex) {
      throw new RuntimeException("Exception calling action " + action, ex);
    } finally {
      if (operationTimer != null) {
        operationTimer.stop();
      }
    }
    return resourceResponse;
  }

  /**
   * Utility method to call Cosmos document query method and record the query time.
   * @param partitionPath the partition to query.
   * @param sqlQuerySpec the DocumentDB query to execute.
   * @param feedOptions {@link FeedOptions} object specifying the options associated with the method.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return {@link BlockingObservable} object containing the query response.
   */
  private BlockingObservable<FeedResponse<Document>> executeCosmosQuery(String partitionPath, SqlQuerySpec sqlQuerySpec,
      FeedOptions feedOptions, Timer timer) {
    azureMetrics.documentQueryCount.inc();
    logger.debug("Running query on partition {}: {}", partitionPath, sqlQuerySpec.getQueryText());
    Timer.Context operationTimer = timer.time();
    try {
      return asyncDocumentClient.queryDocuments(cosmosCollectionLink, sqlQuerySpec, feedOptions).toBlocking();
    } finally {
      operationTimer.stop();
    }
  }

  /**
   * Utility method to call Cosmos change feed query method and record the query time.
   * @param changeFeedOptions {@link ChangeFeedOptions} object specifying the options associated with the method.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return {@link FeedResponse} object representing the query response.
   */
  private FeedResponse<Document> executeCosmosChangeFeedQuery(ChangeFeedOptions changeFeedOptions, Timer timer) {
    Timer.Context operationTimer = timer.time();
    try {
      // FIXME: Using single() for the observable returned by toBlocking() works for now. But if a high enough maxFeedSize
      //  is passed, to result in multiple feed pages, single() will throw an exception.
      return asyncDocumentClient.queryDocumentChangeFeed(cosmosCollectionLink, changeFeedOptions)
          .limit(1)
          .toBlocking()
          .single();
    } finally {
      operationTimer.stop();
    }
  }

  /**
   * Getter for {@link AsyncDocumentClient} object.
   * @return {@link AsyncDocumentClient} object.
   */
  AsyncDocumentClient getAsyncDocumentClient() {
    return asyncDocumentClient;
  }

  private String getDocumentLink(String documentId) {
    return cosmosCollectionLink + DOCS + documentId;
  }

  private RequestOptions getRequestOptions(String partitionPath) {
    RequestOptions options = new RequestOptions();
    options.setPartitionKey(new PartitionKey(partitionPath));
    return options;
  }
}
