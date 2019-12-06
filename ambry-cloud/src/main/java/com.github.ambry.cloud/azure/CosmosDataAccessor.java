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
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CosmosDataAccessor {
  private static final Logger logger = LoggerFactory.getLogger(CosmosDataAccessor.class);
  private static final String DOCS = "/docs/";
  public static final String COSMOS_LAST_UPDATED_COLUMN = "_ts";
  private final AsyncDocumentClient asyncDocumentClient;
  private final String cosmosCollectionLink;
  private final AzureMetrics azureMetrics;

  /** Production constructor */
  CosmosDataAccessor(AsyncDocumentClient asyncDocumentClient, AzureCloudConfig azureCloudConfig,
      AzureMetrics azureMetrics) {
    this(asyncDocumentClient, azureCloudConfig.cosmosCollectionLink, azureMetrics);
  }

  /** Test constructor */
  CosmosDataAccessor(AsyncDocumentClient asyncDocumentClient, String cosmosCollectionLink, AzureMetrics azureMetrics) {
    this.asyncDocumentClient = asyncDocumentClient;
    this.cosmosCollectionLink = cosmosCollectionLink;
    this.azureMetrics = azureMetrics;
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
    options.setPartitionKey(new PartitionKey(blobMetadata.getPartitionId()));
    return executeCosmosAction(() -> asyncDocumentClient.deleteDocument(docLink, options).toBlocking().single(),
        azureMetrics.documentDeleteTime);
  }

  /**
   * Read the blob metadata document in the CosmosDB collection.
   * @param blobId the {@link BlobId} for which metadata is requested.
   * @return the {@link ResourceResponse} containing the metadata document.
   * @throws DocumentClientException if the operation failed.
   */
  ResourceResponse<Document> readMetadata(BlobId blobId) throws DocumentClientException {
    String docLink = getDocumentLink(blobId.getID());
    RequestOptions options = getRequestOptions(blobId.getPartition().toPathString());
    return executeCosmosAction(() -> asyncDocumentClient.readDocument(docLink, options).toBlocking().single(),
        azureMetrics.documentReadTime);
  }

  /**
   * Replace the blob metadata document in the CosmosDB collection, retrying as necessary.
   * @param blobId the {@link BlobId} for which metadata is replaced.
   * @param doc the blob metadata document.
   * @return the {@link ResourceResponse} returned by the operation, if successful.
   * @throws DocumentClientException if the operation failed.
   */
  ResourceResponse<Document> replaceMetadata(BlobId blobId, Document doc) throws DocumentClientException {
    RequestOptions options = getRequestOptions(blobId.getPartition().toPathString());
    return executeCosmosAction(() -> asyncDocumentClient.replaceDocument(doc, options).toBlocking().single(),
        azureMetrics.documentUpdateTime);
  }

  /**
   * Get the list of blobs in the specified partition matching the specified DocumentDB query.
   * @param partitionPath the partition to query.
   * @param querySpec the DocumentDB query to execute.
   * @param timer the {@link Timer} to use to record query time (excluding waiting).
   * @return a List of {@link CloudBlobMetadata} referencing the matching blobs.
   */
  List<CloudBlobMetadata> queryMetadata(String partitionPath, SqlQuerySpec querySpec, Timer timer)
      throws DocumentClientException {
    azureMetrics.documentQueryCount.inc();
    FeedOptions feedOptions = new FeedOptions();
    feedOptions.setPartitionKey(new PartitionKey(partitionPath));
    // TODO: consolidate error count here
    try {
      Timer.Context operationTimer = timer.time();
      Iterator<FeedResponse<Document>> iterator =
          asyncDocumentClient.queryDocuments(cosmosCollectionLink, querySpec, feedOptions).toBlocking().getIterator();
      operationTimer.stop();
      List<CloudBlobMetadata> metadataList = new ArrayList<>();
      while (iterator.hasNext()) {
        iterator.next()
            .getResults()
            .iterator()
            .forEachRemaining(doc -> metadataList.add(createMetadataFromDocument(doc)));
      }
      return metadataList;
    } catch (RuntimeException rex) {
      if (rex.getCause() instanceof DocumentClientException) {
        throw (DocumentClientException) rex.getCause();
      }
      throw rex;
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

  private String getDocumentLink(String documentId) {
    return cosmosCollectionLink + DOCS + documentId;
  }

  private RequestOptions getRequestOptions(String partitionPath) {
    RequestOptions options = new RequestOptions();
    options.setPartitionKey(new PartitionKey(partitionPath));
    return options;
  }
}
