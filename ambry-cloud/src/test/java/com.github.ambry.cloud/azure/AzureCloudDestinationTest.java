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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudFindToken;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/** Test cases for {@link AzureCloudDestination} */
@RunWith(MockitoJUnitRunner.class)
public class AzureCloudDestinationTest {

  private final String base64key = Base64.encodeBase64String("ambrykey".getBytes());
  private final String storageConnection =
      "DefaultEndpointsProtocol=https;AccountName=ambry;AccountKey=" + base64key + ";EndpointSuffix=core.windows.net";
  private final String clusterName = "main";
  private final ObjectMapper objectMapper = new ObjectMapper();
  private Properties configProps = new Properties();
  private AzureCloudDestination azureDest;
  private CloudStorageAccount mockAzureAccount;
  private CloudBlobClient mockAzureClient;
  private CloudBlobContainer mockAzureContainer;
  private CloudBlockBlob mockBlob;
  private DocumentClient mockumentClient;
  private AzureMetrics azureMetrics;
  private int blobSize = 1024;
  byte dataCenterId = 66;
  short accountId = 101;
  short containerId = 5;
  long partition = 666;
  private BlobId blobId;
  private long creationTime = System.currentTimeMillis();
  private long deletionTime = creationTime + 10000;
  private long expirationTime = Utils.Infinite_Time;

  @Before
  public void setup() throws Exception {
    mockAzureAccount = mock(CloudStorageAccount.class);
    mockAzureClient = mock(CloudBlobClient.class);
    mockAzureContainer = mock(CloudBlobContainer.class);
    mockBlob = mock(CloudBlockBlob.class);
    when(mockAzureAccount.createCloudBlobClient()).thenReturn(mockAzureClient);
    when(mockAzureClient.getContainerReference(anyString())).thenReturn(mockAzureContainer);
    when(mockAzureContainer.createIfNotExists(any(), any(), any())).thenReturn(true);
    when(mockAzureContainer.getName()).thenReturn("666");
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenReturn(mockBlob);
    mockBlobExistence(false);
    when(mockBlob.getMetadata()).thenReturn(new HashMap<>());
    Mockito.doNothing().when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    Mockito.doNothing().when(mockBlob).download(any());

    mockumentClient = mock(DocumentClient.class);
    ResourceResponse<Document> mockResponse = mock(ResourceResponse.class);
    Document metadataDoc = new Document();
    when(mockResponse.getResource()).thenReturn(metadataDoc);
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenReturn(mockResponse);

    PartitionId partitionId = new MockPartitionId(partition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);

    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, storageConnection);
    configProps.setProperty(AzureCloudConfig.COSMOS_ENDPOINT, "http://ambry.beyond-the-cosmos.com");
    configProps.setProperty(AzureCloudConfig.COSMOS_COLLECTION_LINK, "ambry/metadata");
    configProps.setProperty(AzureCloudConfig.COSMOS_KEY, "cosmos-key");
    configProps.setProperty("clustermap.cluster.name", "main");
    configProps.setProperty("clustermap.datacenter.name", "uswest");
    configProps.setProperty("clustermap.host.name", "localhost");
    azureMetrics = new AzureMetrics(new MetricRegistry());
    azureDest = new AzureCloudDestination(mockAzureAccount, mockumentClient, "foo", clusterName, azureMetrics);
  }

  /**
   * Test normal upload.
   * @throws Exception
   */
  @Test
  public void testUpload() throws Exception {
    assertTrue("Expected success", uploadDefaultBlob());
    assertEquals(1, azureMetrics.blobUploadRequestCount.getCount());
    assertEquals(1, azureMetrics.blobUploadSuccessCount.getCount());
    assertEquals(0, azureMetrics.blobUploadConflictCount.getCount());
    assertEquals(0, azureMetrics.backupErrorCount.getCount());
    assertEquals(1, azureMetrics.blobUploadTime.getCount());
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
  }

  /**
   * Test normal download.
   * @throws Exception
   */
  @Test
  public void testDownload() throws Exception {
    downloadBlob(blobId);
    assertEquals(1, azureMetrics.blobDownloadRequestCount.getCount());
    assertEquals(1, azureMetrics.blobDownloadSuccessCount.getCount());
    assertEquals(0, azureMetrics.blobDownloadErrorCount.getCount());
    assertEquals(1, azureMetrics.blobDownloadTime.getCount());
  }

  /** Test normal delete. */
  @Test
  public void testDelete() throws Exception {
    mockBlobExistence(true);
    assertTrue("Expected success", azureDest.deleteBlob(blobId, deletionTime));
    assertEquals(1, azureMetrics.blobUpdatedCount.getCount());
    assertEquals(0, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(1, azureMetrics.blobUpdateTime.getCount());
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test normal expiration. */
  @Test
  public void testExpire() throws Exception {
    mockBlobExistence(true);
    assertTrue("Expected success", azureDest.updateBlobExpiration(blobId, expirationTime));
    assertEquals(1, azureMetrics.blobUpdatedCount.getCount());
    assertEquals(0, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(1, azureMetrics.blobUpdateTime.getCount());
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test purge success. */
  @Test
  public void testPurge() throws Exception {
    when(mockBlob.deleteIfExists(any(), any(), any(), any())).thenReturn(true);
    CloudBlobMetadata cloudBlobMetadata =
        new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize,
            CloudBlobMetadata.EncryptionOrigin.NONE);
    assertTrue("Expected success", azureDest.purgeBlob(cloudBlobMetadata));
    assertEquals(1, azureMetrics.blobDeletedCount.getCount());
    assertEquals(0, azureMetrics.blobDeleteErrorCount.getCount());
    assertEquals(1, azureMetrics.documentDeleteTime.getCount());
  }

  /** Test purge not found. */
  @Test
  public void testPurgeNotFound() throws Exception {
    // Unsuccessful case
    when(mockBlob.deleteIfExists(any(), any(), any(), any())).thenReturn(false);
    when(mockumentClient.deleteDocument(anyString(), any(RequestOptions.class))).thenThrow(
        new DocumentClientException(404));
    CloudBlobMetadata cloudBlobMetadata =
        new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize,
            CloudBlobMetadata.EncryptionOrigin.NONE);
    assertFalse("Expected false", azureDest.purgeBlob(cloudBlobMetadata));
    assertEquals(0, azureMetrics.blobDeletedCount.getCount());
    assertEquals(0, azureMetrics.blobDeleteErrorCount.getCount());
  }

  /** Test upload of existing blob. */
  @Test
  public void testUploadExists() throws Exception {
    Mockito.doThrow(new StorageException("Exists", "Exists", HttpURLConnection.HTTP_CONFLICT, null, null))
        .when(mockBlob)
        .upload(any(), anyLong(), any(), any(), any());
    assertFalse("Upload of existing blob should return false", uploadDefaultBlob());
    assertEquals(1, azureMetrics.blobUploadRequestCount.getCount());
    assertEquals(0, azureMetrics.blobUploadSuccessCount.getCount());
    assertEquals(1, azureMetrics.blobUploadConflictCount.getCount());
    assertEquals(0, azureMetrics.backupErrorCount.getCount());
    // Make sure the metadata doc was created
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
  }

  /**
   * Test download of non existent blob
   * @throws Exception
   */
  @Test
  public void testDownloadNotExists() throws Exception {
    doThrow(StorageException.class).when(mockBlob).download(any());
    expectCloudStorageException(() -> downloadBlob(blobId), StorageException.class);
    verifyDownloadErrorMetrics();
  }

  /** Test delete of nonexistent blob. */
  @Test
  public void testDeleteNotExists() throws Exception {
    mockBlobExistence(false);
    assertFalse("Delete of nonexistent blob should return false", azureDest.deleteBlob(blobId, deletionTime));
    assertEquals(0, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(0, azureMetrics.blobUpdateTime.getCount());
    assertEquals(0, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test update of nonexistent blob. */
  @Test
  public void testUpdateNotExists() throws Exception {
    mockBlobExistence(false);
    assertFalse("Update of nonexistent blob should return false",
        azureDest.updateBlobExpiration(blobId, expirationTime));
    assertEquals(0, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(0, azureMetrics.blobUpdateTime.getCount());
    assertEquals(0, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test querying metadata. */
  @Test
  public void testQueryMetadata() throws Exception {
    int batchSize = AzureCloudDestination.ID_QUERY_BATCH_SIZE;
    testQueryMetadata(0, 0);
    testQueryMetadata(batchSize, 1);
    testQueryMetadata(batchSize + 1, 2);
    testQueryMetadata(batchSize * 2 - 1, 2);
    testQueryMetadata(batchSize * 2, 2);
  }

  /**
   * Test metadata query for different input count.
   * @param numBlobs the number of blobs to query.
   * @param expectedQueries the number of internal queries made after batching.
   * @throws Exception
   */
  private void testQueryMetadata(int numBlobs, int expectedQueries) throws Exception {
    // Reset metrics
    azureMetrics = new AzureMetrics(new MetricRegistry());
    azureDest = new AzureCloudDestination(mockAzureAccount, mockumentClient, "foo", clusterName, azureMetrics);
    QueryIterable<Document> mockIterable = mock(QueryIterable.class);
    List<BlobId> blobIdList = new ArrayList<>();
    List<Document> docList = new ArrayList<>();
    for (int j = 0; j < numBlobs; j++) {
      BlobId blobId = generateBlobId();
      blobIdList.add(blobId);
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      docList.add(AzureTestUtils.createDocumentFromCloudBlobMetadata(inputMetadata, objectMapper));
    }
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    when(feedResponse.getQueryIterable()).thenReturn(mockIterable);
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        feedResponse);
    Map<String, CloudBlobMetadata> metadataMap = azureDest.getBlobMetadata(blobIdList);
    assertEquals("Wrong map size", blobIdList.size(), metadataMap.size());
    for (BlobId blobId : blobIdList) {
      assertEquals("Unexpected id in metadata", blobId.getID(), metadataMap.get(blobId.getID()).getId());
    }
    assertEquals(expectedQueries, azureMetrics.documentQueryCount.getCount());
    assertEquals(expectedQueries, azureMetrics.missingKeysQueryTime.getCount());
  }

  /** Test getDeadBlobs */
  @Test
  public void testGetDeadBlobs() throws Exception {
    QueryIterable<Document> mockIterable = mock(QueryIterable.class);
    when(mockIterable.iterator()).thenReturn(Collections.emptyIterator());
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    when(feedResponse.getQueryIterable()).thenReturn(mockIterable);
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        feedResponse);
    List<CloudBlobMetadata> metadataList = azureDest.getDeadBlobs(blobId.getPartition().toPathString());
    assertEquals("Expected no dead blobs", 0, metadataList.size());
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.deadBlobsQueryTime.getCount());
  }

  /** Test findEntriesSince. */
  @Test
  public void testFindEntriesSince() throws Exception {
    testFindEntriesSinceWithUniqueUpdateTimes();
    testFindEntriesSinceWithNonUniqueUpdateTimes();
  }

  /**
   * Test findEntriesSince with all entries having unique updateTimes.
   * @throws Exception
   */
  private void testFindEntriesSinceWithUniqueUpdateTimes() throws Exception {
    long chunkSize = 110000;
    long maxTotalSize = 1000000; // between 9 and 10 chunks
    long startTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
    int totalBlobs = 20;

    // create metadata list where total size > maxTotalSize
    List<Document> docList = new ArrayList<>();
    List<String> blobIdList = new ArrayList<>();
    for (int j = 0; j < totalBlobs; j++) {
      BlobId blobId = generateBlobId();
      blobIdList.add(blobId.getID());
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, chunkSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      inputMetadata.setUploadTime(startTime + j);
      docList.add(AzureTestUtils.createDocumentFromCloudBlobMetadata(inputMetadata, startTime + j, objectMapper));
    }
    QueryIterable<Document> mockIterable = mock(QueryIterable.class);
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    when(feedResponse.getQueryIterable()).thenReturn(mockIterable);
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        feedResponse);
    CloudFindToken findToken = new CloudFindToken();
    // Run the query
    List<CloudBlobMetadata> firstResult =
        azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    assertEquals("Did not get expected doc count", maxTotalSize / chunkSize, firstResult.size());

    docList = docList.subList(firstResult.size(), docList.size());
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    findToken = CloudFindToken.getUpdatedToken(findToken, firstResult);
    assertEquals("Find token has wrong last update time", findToken.getLastUpdateTime(),
        firstResult.get(firstResult.size() - 1).getLastUpdateTime());
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", findToken.getLastUpdateTimeReadBlobIds(),
        new HashSet<>(Collections.singletonList(firstResult.get(firstResult.size() - 1).getId())));

    List<CloudBlobMetadata> secondResult =
        azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    assertEquals("Unexpected doc count", maxTotalSize / chunkSize, secondResult.size());
    assertEquals("Unexpected first blobId", blobIdList.get(firstResult.size()), secondResult.get(0).getId());

    // Rerun with max size below blob size, and make sure it returns one result
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    assertEquals("Expected one result", 1,
        azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, chunkSize / 2).size());
  }

  /**
   * Test findEntriesSince with entries having non unique updateTimes.
   * @throws Exception
   */
  private void testFindEntriesSinceWithNonUniqueUpdateTimes() throws Exception {
    long chunkSize = 110000;
    long maxTotalSize = 1000000; // between 9 and 10 chunks
    long startTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
    int totalBlobs = 20;

    // create metadata list where total size > maxTotalSize
    List<Document> docList = new ArrayList<>();
    List<String> blobIdList = new ArrayList<>();
    for (int j = 0; j < totalBlobs - 1; j++) {
      BlobId blobId = generateBlobId();
      blobIdList.add(blobId.getID());
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, chunkSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      docList.add(AzureTestUtils.createDocumentFromCloudBlobMetadata(inputMetadata, startTime, objectMapper));
    }
    BlobId blobId = generateBlobId();
    blobIdList.add(blobId.getID());
    CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, chunkSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    docList.add(AzureTestUtils.createDocumentFromCloudBlobMetadata(inputMetadata, startTime + 1, objectMapper));

    QueryIterable<Document> mockIterable = mock(QueryIterable.class);
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    when(feedResponse.getQueryIterable()).thenReturn(mockIterable);
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        feedResponse);
    CloudFindToken findToken = new CloudFindToken();
    // Run the query
    List<CloudBlobMetadata> firstResult =
        azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    assertEquals("Did not get expected doc count", maxTotalSize / chunkSize, firstResult.size());

    when(mockIterable.iterator()).thenReturn(docList.iterator());
    findToken = CloudFindToken.getUpdatedToken(findToken, firstResult);
    assertEquals("Find token has wrong last update time", findToken.getLastUpdateTime(),
        firstResult.get(firstResult.size() - 1).getLastUpdateTime());
    Set<String> resultBlobIdSet = firstResult.stream().map(d -> d.getId()).collect(Collectors.toSet());
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", findToken.getLastUpdateTimeReadBlobIds(),
        resultBlobIdSet);

    List<CloudBlobMetadata> secondResult =
        azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    assertEquals("Unexpected doc count", maxTotalSize / chunkSize, secondResult.size());
    assertEquals("Unexpected first blobId", blobIdList.get(firstResult.size()), secondResult.get(0).getId());

    CloudFindToken secondFindToken = CloudFindToken.getUpdatedToken(findToken, secondResult);
    assertEquals("Find token has wrong last update time", secondFindToken.getLastUpdateTime(),
        firstResult.get(firstResult.size() - 1).getLastUpdateTime());
    resultBlobIdSet.addAll(secondResult.stream().map(d -> d.getId()).collect(Collectors.toSet()));
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", secondFindToken.getLastUpdateTimeReadBlobIds(),
        resultBlobIdSet);

    // Rerun with max size below blob size, and make sure it returns one result
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    assertEquals("Expected one result", 1,
        azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, chunkSize / 2).size());

    // Rerun final time, and make sure that it returns all the remaining blobs
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    List<CloudBlobMetadata> thirdResult =
        azureDest.findEntriesSince(blobId.getPartition().toPathString(), secondFindToken, maxTotalSize);
    assertEquals("Unexpected doc count", totalBlobs - (firstResult.size() + secondResult.size()), thirdResult.size());
    assertEquals("Unexpected first blobId", blobIdList.get(firstResult.size() + secondResult.size()),
        thirdResult.get(0).getId());

    CloudFindToken thirdFindToken = CloudFindToken.getUpdatedToken(secondFindToken, thirdResult);
    assertEquals("Find token has wrong last update time", thirdFindToken.getLastUpdateTime(), startTime + 1);
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", thirdFindToken.getLastUpdateTimeReadBlobIds(),
        new HashSet<>(Collections.singletonList(thirdResult.get(thirdResult.size() - 1).getId())));
  }

  /** Test blob existence check. */
  @Test
  public void testExistenceCheck() throws Exception {
    mockBlobExistence(true);
    assertTrue("Expected doesBlobExist to return true", azureDest.doesBlobExist(blobId));

    mockBlobExistence(false);
    assertFalse("Expected doesBlobExist to return false", azureDest.doesBlobExist(blobId));
  }

  private void mockBlobExistence(boolean exists) throws Exception {
    when(mockBlob.exists(any(), any(), any())).thenReturn(exists);
  }

  /** Test constructor with invalid connection string. */
  @Test
  public void testInitClientException() throws Exception {
    CloudDestinationFactory factory =
        new AzureCloudDestinationFactory(new VerifiableProperties(configProps), new MetricRegistry());
    try {
      factory.getCloudDestination();
      fail("Expected exception");
    } catch (IllegalStateException ex) {
    }
  }

  @Test
  @Ignore // until we set the retry policy to fail faster
  public void testAzureConnection() throws Exception {
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureCloudDestination dest = new AzureCloudDestination(cloudConfig, azureConfig, clusterName, azureMetrics);
    try {
      dest.testStorageConnectivity();
      fail("Expected exception");
    } catch (IllegalStateException expected) {
    }
    try {
      dest.getCosmosDataAccessor().testConnectivity();
      fail("Expected exception");
    } catch (IllegalStateException expected) {
    }
  }

  /** Test initializing AzureCloudDestination with a proxy */
  @Test
  public void testProxy() throws Exception {

    // Test without proxy
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureCloudDestination dest = new AzureCloudDestination(cloudConfig, azureConfig, clusterName, azureMetrics);
    // check operation context proxy
    assertNull("Expected null proxy in blob op context", OperationContext.getDefaultProxy());
    assertNull("Expected null proxy in doc client", dest.getDocumentClient().getConnectionPolicy().getProxy());

    // Test with proxy
    String proxyHost = "azure-proxy.randomcompany.com";
    int proxyPort = 80;
    configProps.setProperty(CloudConfig.VCR_PROXY_HOST, proxyHost);
    configProps.setProperty(CloudConfig.VCR_PROXY_PORT, String.valueOf(proxyPort));
    cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    dest = new AzureCloudDestination(cloudConfig, azureConfig, clusterName, azureMetrics);
    assertNotNull("Expected proxy in blob op context", OperationContext.getDefaultProxy());
    HttpHost policyProxy = dest.getDocumentClient().getConnectionPolicy().getProxy();
    assertNotNull("Expected proxy in doc client", policyProxy);
    assertEquals("Wrong host", proxyHost, policyProxy.getHostName());
    assertEquals("Wrong port", proxyPort, policyProxy.getPort());
  }

  /** Test upload when client throws exception. */
  @Test
  public void testUploadContainerReferenceException() throws Exception {
    when(mockAzureClient.getContainerReference(anyString())).thenThrow(StorageException.class);
    expectCloudStorageException(() -> uploadDefaultBlob(), StorageException.class);
    verifyUploadErrorMetrics(false);
  }

  /** Test upload when container throws exception. */
  @Test
  public void testUploadContainerException() throws Exception {
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenThrow(StorageException.class);
    expectCloudStorageException(() -> uploadDefaultBlob(), StorageException.class);
    verifyUploadErrorMetrics(false);
  }

  /** Test upload when blob throws exception. */
  @Test
  public void testUploadBlobException() throws Exception {
    Mockito.doThrow(StorageException.class).when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    expectCloudStorageException(() -> uploadDefaultBlob(), StorageException.class);
    verifyUploadErrorMetrics(false);
  }

  /** Test upload when doc client throws exception. */
  @Test
  public void testUploadDocClientException() throws Exception {
    when(mockumentClient.upsertDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenThrow(
        DocumentClientException.class);
    expectCloudStorageException(() -> uploadDefaultBlob(), DocumentClientException.class);
    verifyUploadErrorMetrics(true);
  }

  /** Test upload when client throws exception. */
  @Test
  public void testDownloadContainerReferenceException() throws Exception {
    when(mockAzureClient.getContainerReference(anyString())).thenThrow(StorageException.class);
    expectCloudStorageException(() -> downloadBlob(blobId), StorageException.class);
    verifyDownloadErrorMetrics();
  }

  /** Test download when container throws exception. */
  @Test
  public void testDownloadContainerException() throws Exception {
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenThrow(StorageException.class);
    expectCloudStorageException(() -> downloadBlob(blobId), StorageException.class);
    verifyDownloadErrorMetrics();
  }

  /** Test update methods when blob throws exception. */
  @Test
  public void testUpdateBlobException() throws Exception {
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenThrow(StorageException.class);
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId, deletionTime), StorageException.class);
    expectCloudStorageException(() -> azureDest.updateBlobExpiration(blobId, expirationTime), StorageException.class);
    verifyUpdateErrorMetrics(2, false);
  }

  /** Test update methods when doc client throws exception. */
  @Test
  public void testUpdateDocClientException() throws Exception {
    mockBlobExistence(true);
    when(mockumentClient.readDocument(anyString(), any())).thenThrow(DocumentClientException.class);
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId, deletionTime), DocumentClientException.class);
    expectCloudStorageException(() -> azureDest.updateBlobExpiration(blobId, expirationTime),
        DocumentClientException.class);
    verifyUpdateErrorMetrics(2, true);
  }

  /** Test token methods. */
  @Test
  public void testTokens() throws Exception {
    String path = blobId.getPartition().toPathString();
    String tokenFile = "replicaTokens";
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(100);
    azureDest.persistTokens(path, tokenFile, new ByteArrayInputStream(new byte[100]));
    mockBlobExistence(true);
    assertTrue("Expected retrieveTokens to return true", azureDest.retrieveTokens(path, tokenFile, outputStream));
    mockBlobExistence(false);
    assertFalse("Expected retrieveTokens to return false", azureDest.retrieveTokens(path, tokenFile, outputStream));
  }

  /**
   * Verify the metric values after an upload error.
   * @param isDocument Flag indicating a DocumentClientException thrown.
   */
  private void verifyUploadErrorMetrics(boolean isDocument) {
    assertEquals(1, azureMetrics.blobUploadRequestCount.getCount());
    assertEquals(0, azureMetrics.blobUploadConflictCount.getCount());
    assertEquals(0, azureMetrics.backupSuccessByteRate.getCount());
    assertEquals(1, azureMetrics.backupErrorCount.getCount());
    assertEquals(isDocument ? 0 : 1, azureMetrics.storageErrorCount.getCount());
    assertEquals(isDocument ? 1 : 0, azureMetrics.documentErrorCount.getCount());
  }

  /**
   * Verify the metric values after an upload error.
   */
  private void verifyDownloadErrorMetrics() {
    assertEquals(1, azureMetrics.blobDownloadRequestCount.getCount());
    assertEquals(0, azureMetrics.blobDownloadSuccessCount.getCount());
    assertEquals(1, azureMetrics.blobDownloadErrorCount.getCount());
    assertEquals(1, azureMetrics.blobDownloadTime.getCount());
  }

  /**
   * Verify the metric values after an update error.
   * @param numUpdates the number of update operations made.
   * @param isDocument Flag indicating a DocumentClientException thrown.
   */
  private void verifyUpdateErrorMetrics(int numUpdates, boolean isDocument) {
    assertEquals(0, azureMetrics.blobUpdatedCount.getCount());
    assertEquals(numUpdates, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(isDocument ? 0 : numUpdates, azureMetrics.storageErrorCount.getCount());
    assertEquals(isDocument ? numUpdates : 0, azureMetrics.documentErrorCount.getCount());
  }

  /**
   * Upload a blob with default properties.
   * @return the result of the uploadBlob call.
   * @throws CloudStorageException on error.
   */
  private boolean uploadDefaultBlob() throws CloudStorageException {
    InputStream inputStream = getBlobInputStream(blobSize);
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    return azureDest.uploadBlob(blobId, blobSize, metadata, inputStream);
  }

  /**
   * Download a blob with the given blobId.
   * @param blobId blobid of the blob to be downloaded.
   */
  private void downloadBlob(BlobId blobId) throws CloudStorageException {
    azureDest.downloadBlob(blobId, new ByteArrayOutputStream(blobSize));
  }

  /**
   * Utility method to run some code and verify the expected exception was thrown.
   * @param runnable the code to run.
   * @param nestedExceptionClass the expected nested exception class.
   */
  private void expectCloudStorageException(TestUtils.ThrowingRunnable runnable, Class nestedExceptionClass) {
    try {
      runnable.run();
      fail("Expected CloudStorageException");
    } catch (CloudStorageException csex) {
      assertEquals("Expected " + nestedExceptionClass.getSimpleName(), nestedExceptionClass,
          csex.getCause().getClass());
    } catch (Exception ex) {
      fail("Expected CloudStorageException, got " + ex.getClass().getSimpleName());
    }
  }

  /**
   * Utility method to generate a BlobId.
   * @return a BlobId for the default attributes.
   */
  private BlobId generateBlobId() {
    return new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, blobId.getPartition(), false,
        BlobDataType.DATACHUNK);
  }

  /**
   * Utility method to get blob input stream.
   * @param blobSize size of blob to consider.
   * @return the blob input stream.
   */
  private static InputStream getBlobInputStream(int blobSize) {
    byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
    return new ByteArrayInputStream(randomBytes);
  }
}
