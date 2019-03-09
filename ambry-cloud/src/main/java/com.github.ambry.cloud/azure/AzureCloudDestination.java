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

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link CloudDestination} that interacts with Azure Blob Storage service.
 */
@SuppressWarnings({"ALL", "MagicConstant"})
class AzureCloudDestination implements CloudDestination {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestination.class);
  private static final String BATCH_ID_QUERY_TEMPLATE = "SELECT * FROM c WHERE c.id IN (%s)";
  private final CloudStorageAccount azureAccount;
  private final CloudBlobClient azureBlobClient;
  private final DocumentClient documentClient;
  private final String cosmosCollectionLink; // eg "/dbs/ambry-metadata/colls/blob-metadata"
  private final RequestOptions defaultRequestOptions = new RequestOptions();

  public static final String STORAGE_CONFIG_SPEC = "storageConfigSpec";
  public static final String COSMOS_ENDPOINT = "cosmosEndpoint";
  public static final String COSMOS_COLLECTION_LINK = "cosmosCollectionLink";
  public static final String COSMOS_KEY = "cosmosKey";

  /**
   * Construct an Azure cloud destination from config properties.
   * @param verProps the {@link VerifiableProperties} to use.
   * @throws InvalidKeyException if credentials in the connection string contain an invalid key.
   * @throws URISyntaxException if the connection string specifies an invalid URI.
   */
  AzureCloudDestination(VerifiableProperties verProps) throws URISyntaxException, InvalidKeyException {
    String configSpec = verProps.getString(STORAGE_CONFIG_SPEC);
    azureAccount = CloudStorageAccount.parse(configSpec);
    azureBlobClient = azureAccount.createCloudBlobClient();
    String cosmosServiceEndpoint = verProps.getString(COSMOS_ENDPOINT);
    String cosmosKey = verProps.getString(COSMOS_KEY);
    cosmosCollectionLink = verProps.getString(COSMOS_COLLECTION_LINK);
    documentClient =
        new DocumentClient(cosmosServiceEndpoint, cosmosKey, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
    // check that it works
    try {
      ResourceResponse<DocumentCollection> response =
          documentClient.readCollection(cosmosCollectionLink, defaultRequestOptions);
      if (response.getResource() == null) {
        throw new IllegalArgumentException("CosmosDB collection not found: " + cosmosCollectionLink);
      }
    } catch (DocumentClientException ex) {
      throw new IllegalArgumentException("Invalid CosmosDB properties", ex);
    }
    logger.info("Created Azure destination");
  }

  /**
   * Test constructor.
   * @param azureAccount the {@link CloudStorageAccount} to use.
   * @param documentClient the {@link DocumentClient} to use.
   * @param cosmosCollectionLink the CosmosDB collection link to use.
   * @throws CloudStorageException if the destination could not be created.
   */
  AzureCloudDestination(CloudStorageAccount azureAccount, DocumentClient documentClient, String cosmosCollectionLink) {
    this.azureAccount = azureAccount;
    this.documentClient = documentClient;
    this.cosmosCollectionLink = cosmosCollectionLink;

    // Create a blob client to interact with Blob storage
    azureBlobClient = azureAccount.createCloudBlobClient();
  }

  /**
   * For integration test
   * @return the CosmosDB DocumentClient.
   */
  DocumentClient getDocumentClient() {
    return documentClient;
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long blobSize, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws CloudStorageException {

    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(blobInputStream, "Input stream cannot be null");

    BlobRequestOptions options = null; // may want to set BlobEncryptionPolicy here
    OperationContext opContext = null;
    try {
      CloudBlobContainer azureContainer = getContainer(blobId, true);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId.getID());

      if (azureBlob.exists()) {
        logger.debug("Skipping upload of blob {} as it already exists in Azure container {}.", blobId,
            azureContainer.getName());
        return false;
      }

      azureBlob.setMetadata(getMetadataMap(cloudBlobMetadata));
      azureBlob.upload(blobInputStream, blobSize, null, options, opContext);

      documentClient.createDocument(cosmosCollectionLink, cloudBlobMetadata, defaultRequestOptions, true);

      logger.debug("Uploaded blob {} to Azure container {}.", blobId, azureContainer.getName());
      return true;
    } catch (URISyntaxException | StorageException | DocumentClientException | IOException e) {
      throw new CloudStorageException("Failed to upload blob: " + blobId, e);
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
    String quotedBlobIds =
        String.join(",", blobIds.stream().map(s -> '"' + s.getID() + '"').collect(Collectors.toList()));
    String querySpec = String.format(BATCH_ID_QUERY_TEMPLATE, quotedBlobIds);
    FeedOptions feedOptions = new FeedOptions();
    feedOptions.setPartitionKey(new PartitionKey(blobIds.get(0).getPartition().toPathString()));
    FeedResponse<Document> response = documentClient.queryDocuments(cosmosCollectionLink, querySpec, feedOptions);
    try {
      // Note: internal query iterator wraps DocumentClientException in IllegalStateException!
      Map<String, CloudBlobMetadata> metadataMap = new HashMap<>();
      response.getQueryIterable()
          .iterator()
          .forEachRemaining(doc -> metadataMap.put(doc.getId(), doc.toObject(CloudBlobMetadata.class)));
      return metadataMap;
    } catch (RuntimeException rex) {
      if (rex.getCause() instanceof DocumentClientException) {
        throw new CloudStorageException("Failed to query blob metadata", rex.getCause());
      } else {
        throw rex;
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

    // We update the blob deletion time in two places:
    // 1) the CosmosDB metadata collection
    // 2) the blob storage entry metadata (to enable rebuilding the database)

    try {
      CloudBlobContainer azureContainer = getContainer(blobId, false);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId.getID());

      if (!azureBlob.exists()) {
        logger.debug("Blob {} not found in Azure container {}.", blobId, azureContainer.getName());
        return false;
      }
      azureBlob.downloadAttributes(); // Makes sure we have latest
      azureBlob.getMetadata().put(fieldName, String.valueOf(value));
      azureBlob.uploadMetadata();

      String docLink = cosmosCollectionLink + "/docs/" + blobId.getID();
      RequestOptions options = new RequestOptions();
      options.setPartitionKey(new PartitionKey(blobId.getPartition().toPathString()));
      ResourceResponse<Document> response = documentClient.readDocument(docLink, options);
      //CloudBlobMetadata blobMetadata = response.getResource().toObject(CloudBlobMetadata.class);
      Document doc = response.getResource();
      if (doc == null) {
        logger.warn("Blob metadata record not found: " + docLink);
        return false;
      }
      doc.set(fieldName, value);
      documentClient.replaceDocument(doc, options);
      logger.debug("Updated blob {} metadata set {} to {}.", blobId, fieldName, value);
      return true;
    } catch (URISyntaxException | StorageException | DocumentClientException e) {
      throw new CloudStorageException("Failed to update blob metadata: " + blobId, e);
    }
  }

  @Override
  public boolean doesBlobExist(BlobId blobId) throws CloudStorageException {
    try {
      CloudBlobContainer azureContainer = getContainer(blobId, false);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(blobId.getID());
      return azureBlob.exists();
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
    // Need clustermap to construct BlobId and partitionId
    // Either pass to our constructor or pass BlobId to methods
    // TODO: "clustername-<pid>"
    String partitionPath = "partition-" + blobId.getPartition().toPathString();
    CloudBlobContainer azureContainer = azureBlobClient.getContainerReference(partitionPath);
    if (autoCreate) {
      azureContainer.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
          new OperationContext());
    }
    return azureContainer;
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
    return map;
  }
}
