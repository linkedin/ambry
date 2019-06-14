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
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
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
import com.microsoft.azure.storage.AccessCondition;
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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link CloudDestination} that interacts with Azure Blob Storage service.
 */
class AzureCloudDestination implements CloudDestination {

  private static final Logger logger = LoggerFactory.getLogger(AzureCloudDestination.class);
  private static final String BATCH_ID_QUERY_TEMPLATE = "SELECT * FROM c WHERE c.id IN (%s)";
  private static final String SEPARATOR = "-";
  private final CloudStorageAccount azureAccount;
  private final CloudBlobClient azureBlobClient;
  private final DocumentClient documentClient;
  private final String cosmosCollectionLink;
  private final RequestOptions defaultRequestOptions = new RequestOptions();
  private final OperationContext blobOpContext = new OperationContext();
  private final AzureMetrics azureMetrics;
  private final String clusterName;
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
      blobOpContext.setDefaultProxy(
          new Proxy(Proxy.Type.HTTP, new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort)));
    }
    // Set up CosmosDB connection, including any proxy setting
    cosmosCollectionLink = azureCloudConfig.cosmosCollectionLink;
    ConnectionPolicy connectionPolicy = new ConnectionPolicy();
    if (cloudConfig.vcrProxyHost != null) {
      connectionPolicy.setProxy(new HttpHost(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort));
      connectionPolicy.setHandleServiceUnavailableFromProxy(true);
    }
    documentClient = new DocumentClient(azureCloudConfig.cosmosEndpoint, azureCloudConfig.cosmosKey, connectionPolicy,
        ConsistencyLevel.Session);
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
    this.cosmosCollectionLink = cosmosCollectionLink;
    this.azureMetrics = azureMetrics;
    this.clusterName = clusterName;

    // Create a blob client to interact with Blob storage
    azureBlobClient = azureAccount.createCloudBlobClient();
  }

  /**
   * Test connectivity to Azure endpoints
   */
  void testAzureConnectivity() {
    testStorageConnectivity();
    testCosmosConnectivity();
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

  /**
   * Test connectivity to Azure CosmosDB
   */
  void testCosmosConnectivity() {
    try {
      ResourceResponse<DocumentCollection> response =
          documentClient.readCollection(cosmosCollectionLink, defaultRequestOptions);
      if (response.getResource() == null) {
        throw new IllegalStateException("CosmosDB collection not found: " + cosmosCollectionLink);
      }
      logger.info("CosmosDB connection test succeeded.");
    } catch (DocumentClientException ex) {
      throw new IllegalStateException("CosmosDB connection test failed", ex);
    }
  }

  /**
   * Visible for test.
   * @return the CosmosDB DocumentClient.
   */
  DocumentClient getDocumentClient() {
    return documentClient;
  }

  /**
   * Visible for test.
   * @return the blob storage operation context.
   */
  OperationContext getBlobOpContext() {
    return blobOpContext;
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long inputLength, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws CloudStorageException {

    Objects.requireNonNull(blobId, "BlobId cannot be null");
    Objects.requireNonNull(blobInputStream, "Input stream cannot be null");
    try {
      Timer.Context backupTimer = azureMetrics.backupSuccessLatency.time();
      boolean uploaded = uploadIfNotExists(blobId, inputLength, cloudBlobMetadata, blobInputStream);
      if (!uploaded) {
        return false;
      }

      Timer.Context docTimer = azureMetrics.documentCreateTime.time();
      try {
        documentClient.upsertDocument(cosmosCollectionLink, cloudBlobMetadata, defaultRequestOptions, true);
      } finally {
        docTimer.stop();
      }
      backupTimer.stop();
      azureMetrics.backupSuccessByteRate.mark(inputLength);
      return true;
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
      CloudBlobContainer azureContainer = getContainer(blobId, true);
      String azureBlobName = getAzureBlobName(blobId);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(azureBlobName);
      cloudBlobMetadata.setCloudBlobName(getAzureBlobName(blobId));
      azureBlob.setMetadata(getMetadataMap(cloudBlobMetadata));
      azureBlob.upload(blobInputStream, inputLength, condition, options, blobOpContext);
      logger.debug("Uploaded blob {} to Azure container {}.", blobId, azureContainer.getName());
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
    azureMetrics.documentQueryCount.inc();
    Timer.Context queryTimer = azureMetrics.documentQueryTime.time();
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
      queryTimer.stop();
      return metadataMap;
    } catch (RuntimeException rex) {
      if (rex.getCause() instanceof DocumentClientException) {
        azureMetrics.documentErrorCount.inc();
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
      String azureBlobName = getAzureBlobName(blobId);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(azureBlobName);

      if (!azureBlob.exists()) {
        logger.debug("Blob {} not found in Azure container {}.", blobId, azureContainer.getName());
        return false;
      }

      Timer.Context storageTimer = azureMetrics.blobUpdateTime.time();
      try {
        azureBlob.downloadAttributes(); // Makes sure we have latest
        azureBlob.getMetadata().put(fieldName, String.valueOf(value));
        azureBlob.uploadMetadata();
      } finally {
        storageTimer.stop();
      }

      Timer.Context docTimer = azureMetrics.documentUpdateTime.time();
      try {
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
      } finally {
        docTimer.stop();
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
  public boolean doesBlobExist(BlobId blobId) throws CloudStorageException {
    try {
      CloudBlobContainer azureContainer = getContainer(blobId, false);
      String azureBlobName = getAzureBlobName(blobId);
      CloudBlockBlob azureBlob = azureContainer.getBlockBlobReference(azureBlobName);
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
      if (!azureBlob.exists()) {
        return false;
      }
      azureBlob.download(outputStream, null, null, blobOpContext);
      return true;
    } catch (URISyntaxException | StorageException e) {
      throw new CloudStorageException("Could not retrieve token: " + partitionPath, e);
    }
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
    return map;
  }
}
