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
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CosmosDataAccessor {
  private static final Logger logger = LoggerFactory.getLogger(CosmosDataAccessor.class);
  private static final int HTTP_TOO_MANY_REQUESTS = 429;
  private static final String DOCS = "/docs/";
  public static final String COSMOS_LAST_UPDATED_COLUMN = "_ts";
  private final DocumentClient documentClient;
  private final String cosmosCollectionLink;
  private final AzureMetrics azureMetrics;
  private final int maxRetries;

  /** Production constructor */
  public CosmosDataAccessor(DocumentClient documentClient, AzureCloudConfig azureCloudConfig,
      AzureMetrics azureMetrics) {
    this(documentClient, azureCloudConfig.cosmosCollectionLink, azureCloudConfig.cosmosMaxRetries, azureMetrics);
  }

  /** Test constructor */
  public CosmosDataAccessor(DocumentClient documentClient, String cosmosCollectionLink, int maxRetries,
      AzureMetrics azureMetrics) {
    this.documentClient = documentClient;
    this.cosmosCollectionLink = cosmosCollectionLink;
    this.azureMetrics = azureMetrics;
    this.maxRetries = maxRetries;
  }

  /**
   * Test connectivity to Azure CosmosDB
   */
  void testConnectivity() {
    try {
      ResourceResponse<DocumentCollection> response =
          documentClient.readCollection(cosmosCollectionLink, new RequestOptions());
      if (response.getResource() == null) {
        throw new IllegalStateException("CosmosDB collection not found: " + cosmosCollectionLink);
      }
      logger.info("CosmosDB connection test succeeded.");
    } catch (DocumentClientException ex) {
      throw new IllegalStateException("CosmosDB connection test failed", ex);
    }
  }

  /**
   * Upsert the blob metadata document in the CosmosDB collection, retrying as necessary.
   * @param blobMetadata the blob metadata document.
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * @throws DocumentClientException if the operation failed, or retry limit is exhausted.
   */
  public ResourceResponse<Document> upsertMetadata(CloudBlobMetadata blobMetadata) throws DocumentClientException {
    RequestOptions options = getRequestOptions(blobMetadata.getPartitionId());
    return retryOperationWithThrottling(
        () -> documentClient.upsertDocument(cosmosCollectionLink, blobMetadata, options, true),
        azureMetrics.documentCreateTime);
  }

  /**
   * Delete the blob metadata document in the CosmosDB collection, retrying as necessary.
   * @param blobMetadata the blob metadata document.
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * @throws DocumentClientException if the operation failed, or retry limit is exhausted.
   */
  public ResourceResponse<Document> deleteMetadata(CloudBlobMetadata blobMetadata) throws DocumentClientException {
    String docLink = getDocumentLink(blobMetadata.getId());
    RequestOptions options = getRequestOptions(blobMetadata.getPartitionId());
    options.setPartitionKey(new PartitionKey(blobMetadata.getPartitionId()));
    return retryOperationWithThrottling(() -> documentClient.deleteDocument(docLink, options),
        azureMetrics.documentDeleteTime);
  }

  /**
   * Read the blob metadata document in the CosmosDB collection, retrying as necessary.
   * @param blobId the {@link BlobId} for which metadata is requested.
   * @return the {@link ResourceResponse} containing the metadata document.
   * @throws DocumentClientException if the operation failed, or retry limit is exhausted.
   */
  public ResourceResponse<Document> readMetadata(BlobId blobId) throws DocumentClientException {
    String docLink = getDocumentLink(blobId.getID());
    RequestOptions options = getRequestOptions(blobId.getPartition().toPathString());
    return retryOperationWithThrottling(() -> documentClient.readDocument(docLink, options),
        azureMetrics.documentReadTime);
  }

  /**
   * Replace the blob metadata document in the CosmosDB collection, retrying as necessary.
   * @param blobId the {@link BlobId} for which metadata is replaced.
   * @param doc the blob metadata document.
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * @throws DocumentClientException if the operation failed, or retry limit is exhausted.
   */
  public ResourceResponse<Document> replaceMetadata(BlobId blobId, Document doc) throws DocumentClientException {
    RequestOptions options = getRequestOptions(blobId.getPartition().toPathString());
    return retryOperationWithThrottling(() -> documentClient.replaceDocument(doc, options),
        azureMetrics.documentUpdateTime);
  }

  /**
   * Get the list of blobs in the specified partition matching the specified DocumentDB query.
   * @param partitionPath the partition to query.
   * @param querySpec the DocumentDB query to execute.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a List of {@link CloudBlobMetadata} referencing the matching blobs.
   * @throws DocumentClientException
   */
  List<CloudBlobMetadata> queryMetadata(String partitionPath, SqlQuerySpec querySpec, Timer timer)
      throws DocumentClientException {
    azureMetrics.documentQueryCount.inc();
    FeedOptions feedOptions = new FeedOptions();
    feedOptions.setPartitionKey(new PartitionKey(partitionPath));
    // TODO: consolidate error count here
    FeedResponse<Document> response =
        retryQueryWithThrottling(() -> documentClient.queryDocuments(cosmosCollectionLink, querySpec, feedOptions),
            timer);
    try {
      // Note: internal query iterator wraps DocumentClientException in IllegalStateException!
      List<CloudBlobMetadata> metadataList = new ArrayList<>();
      // TODO: this iteration can also get TOO_MANY_REQUESTS so should be inside retry loop
      response.getQueryIterable().iterator().forEachRemaining(doc -> metadataList.add(createMetadataFromDocument(doc)));
      return metadataList;
    } catch (RuntimeException rex) {
      if (rex.getCause() instanceof DocumentClientException) {
        azureMetrics.documentErrorCount.inc();
        throw (DocumentClientException) rex.getCause();
      } else {
        throw rex;
      }
    }
  }

  /**
   * Create {@link CloudBlobMetadata} object from {@link Document} object.
   * @param document {@link Document} object from which {@link CloudBlobMetadata} object will be created.
   * @return {@link CloudBlobMetadata} object.
   */
  CloudBlobMetadata createMetadataFromDocument(Document document) {
    CloudBlobMetadata cloudBlobMetadata = document.toObject(CloudBlobMetadata.class);
    cloudBlobMetadata.setLastUpdateTime(document.getLong(COSMOS_LAST_UPDATED_COLUMN));
    return cloudBlobMetadata;
  }

  /**
   * Run the supplied DocumentClient action. If CosmosDB returns status 429 (TOO_MANY_REQUESTS),
   * retry after the requested wait period, up to the configured retry limit.
   * @param operation the DocumentClient resource operation to execute, wrapped in a {@link Callable}.
   * @param timer the {@link Timer} to use to record execution time (excluding waiting).
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * @throws DocumentClientException if Cosmos returns a different error status, or if the retry limit is reached.
   */
  private ResourceResponse<Document> retryOperationWithThrottling(Callable<ResourceResponse<Document>> operation,
      Timer timer) throws DocumentClientException {
    return (ResourceResponse<Document>) retryWithThrottling(operation, timer);
  }

  /**
   * Run the supplied DocumentClient query. If CosmosDB returns status 429 (TOO_MANY_REQUESTS),
   * retry after the requested wait period, up to the configured retry limit.
   * @param query the DocumentClient query to execute, wrapped in a {@link Callable}.
   * @param timer the {@link Timer} to use to record execution time (excluding waiting).
   * @return the {@link FeedResponse} returned by the query, if successful.
   * @throws DocumentClientException if Cosmos returns a different error status, or if the retry limit is reached.
   */
  private FeedResponse<Document> retryQueryWithThrottling(Callable<FeedResponse<Document>> query, Timer timer)
      throws DocumentClientException {
    return (FeedResponse<Document>) retryWithThrottling(query, timer);
  }

  /**
   * Run the supplied DocumentClient action. If CosmosDB returns status 429 (TOO_MANY_REQUESTS),
   * retry after the requested wait period, up to the configured retry limit.
   * @param action the DocumentClient action to execute, wrapped in a {@link Callable}.
   * @param timer the {@link Timer} to use to record execution time (excluding waiting).
   * @return the {@link Object} returned by the action, if successful.
   * @throws DocumentClientException if Cosmos returns a different error status, or if the retry limit is reached.
   */
  private Object retryWithThrottling(Callable<? extends Object> action, Timer timer) throws DocumentClientException {
    int count = 0;
    long waitTime = 0;
    do {
      try {
        waitForMs(waitTime);
        Timer.Context docTimer = timer.time();
        Object response = executeCosmosAction(action);
        docTimer.stop();
        return response;
      } catch (DocumentClientException dex) {
        // Azure tells us how long to wait before retrying.
        if (dex.getStatusCode() == HTTP_TOO_MANY_REQUESTS) {
          waitTime = dex.getRetryAfterInMilliseconds();
          azureMetrics.retryCount.inc();
          logger.debug("Got {} from Cosmos, will wait {} ms before retrying.", HTTP_TOO_MANY_REQUESTS, waitTime);
        } else {
          // Something else, not retryable.
          throw dex;
        }
      } catch (Exception e) {
        azureMetrics.documentErrorCount.inc();
        throw new RuntimeException("Exception calling action " + action, e);
      }
      count++;
    } while (count <= maxRetries);
    azureMetrics.retryCount.dec(); // number of retries, not total tries
    azureMetrics.documentErrorCount.inc();
    String message = "Max number of retries reached while retrying with action " + action;
    throw new RuntimeException(message);
  }

  /**
   * Utility method to call a Cosmos method and extract any nested DocumentClientException.
   * @param action the action to call.
   * @return the result of the action.
   * @throws Exception
   */
  private Object executeCosmosAction(Callable<? extends Object> action) throws Exception {
    try {
      return action.call();
    } catch (DocumentClientException dex) {
      throw dex;
    } catch (IllegalStateException ex) {
      if (ex.getCause() instanceof DocumentClientException) {
        throw (DocumentClientException) ex.getCause();
      } else {
        throw ex;
      }
    }
  }

  private String getDocumentLink(String documentId) {
    return cosmosCollectionLink + DOCS + documentId;
  }

  private RequestOptions getRequestOptions(String partitionPath) {
    RequestOptions options = new RequestOptions();
    options.setPartitionKey(new PartitionKey(partitionPath));
    return options;
  }

  /**
   * Wait for the specified time, or until interrupted.
   * @param waitTimeInMillis the time to wait.
   */
  void waitForMs(long waitTimeInMillis) {
    if (waitTimeInMillis > 0) {
      Timer.Context waitTimer = azureMetrics.retryWaitTime.time();
      try {
        TimeUnit.MILLISECONDS.sleep(waitTimeInMillis);
      } catch (InterruptedException e) {
        logger.warn("Interrupted while waiting for retry");
      }
      waitTimer.stop();
    }
  }
}
