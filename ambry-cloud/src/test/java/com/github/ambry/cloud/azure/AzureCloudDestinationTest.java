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

import com.azure.core.http.rest.Response;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.DummyCloudUpdateValidator;
import com.github.ambry.cloud.FindResult;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.github.ambry.cloud.azure.CosmosDataAccessor.*;
import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;


/** Test cases for {@link AzureCloudDestination} */
@RunWith(MockitoJUnitRunner.class)
public class AzureCloudDestinationTest {

  private final String base64key = Base64.encodeBase64String("ambrykey".getBytes());
  private final String storageConnection =
      "DefaultEndpointsProtocol=https;AccountName=ambry;AccountKey=" + base64key + ";EndpointSuffix=core.windows.net";
  private final String clusterName = "main";
  private final AzureReplicationFeed.FeedType defaultAzureReplicationFeedType =
      AzureReplicationFeed.FeedType.COSMOS_CHANGE_FEED;
  private final Properties configProps = new Properties();
  private AzureCloudDestination azureDest;
  private ClusterMap clusterMap;
  private CosmosAsyncClient mockCosmosAsyncClient;
  private CosmosAsyncDatabase mockCosmosAsyncDatabase;
  private CosmosAsyncContainer mockCosmosAsyncContainer;
  private CosmosException mockNotFoundCosmosException;
  private BlockBlobAsyncClient mockBlockBlobAsyncClient;
  private BlobBatchAsyncClient mockBlobBatchAsyncClient;
  private BlobServiceAsyncClient mockServiceAsyncClient;
  private VcrMetrics vcrMetrics;
  private AzureMetrics azureMetrics;
  private final DummyCloudUpdateValidator dummyCloudUpdateValidator = new DummyCloudUpdateValidator();
  private final int blobSize = 1024;
  private final byte dataCenterId = 66;
  private final short accountId = 101;
  private final short containerId = 5;
  private BlobId blobId;
  private final long creationTime = System.currentTimeMillis();
  private final long deletionTime = creationTime + 10000;
  private final long expirationTime = Utils.Infinite_Time;

  /**
   * Utility method to get blob input stream.
   * @param blobSize size of blob to consider.
   * @return the blob input stream.
   */
  private static InputStream getBlobInputStream(int blobSize) {
    byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
    return new ByteArrayInputStream(randomBytes);
  }

  @Before
  public void setup() throws Exception {
    long partition = 666;
    PartitionId partitionId = new MockPartitionId(partition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    CloudBlobMetadata blobMetadata =
        new CloudBlobMetadata(blobId, 0, Utils.Infinite_Time, blobSize, CloudBlobMetadata.EncryptionOrigin.NONE);

    mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
    mockBlobBatchAsyncClient = mock(BlobBatchAsyncClient.class);
    mockBlockBlobAsyncClient = AzureBlobDataAccessorTest.setupMockBlobAsyncClient(mockServiceAsyncClient);
    mockBlobExistence(false);
    when(mockBlockBlobAsyncClient.uploadWithResponse(any(), anyLong(), any(), any(), any(), any(), any())).thenReturn(
        Mono.just(mock(Response.class)));

    Response<BlobProperties> blobPropertiesResponse = mock(Response.class);
    when(blobPropertiesResponse.getValue()).thenReturn(mock(BlobProperties.class));
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.just(blobPropertiesResponse));
    when(mockBlockBlobAsyncClient.setMetadataWithResponse(any(), any())).thenReturn(Mono.just(mock(Response.class)));

    mockCosmosAsyncClient = mock(CosmosAsyncClient.class);
    mockCosmosAsyncDatabase = mock(CosmosAsyncDatabase.class);
    mockCosmosAsyncContainer = mock(CosmosAsyncContainer.class);
    when(mockCosmosAsyncDatabase.getContainer(anyString())).thenReturn(mockCosmosAsyncContainer);
    CosmosItemResponse cosmosItemResponse = mock(CosmosItemResponse.class);
    when(cosmosItemResponse.getItem()).thenReturn(blobMetadata);
    when(mockCosmosAsyncContainer.readItem(anyString(), any(), any())).thenReturn(Mono.just(cosmosItemResponse));
    when(mockCosmosAsyncContainer.upsertItem(any(), any(), any())).thenReturn(Mono.just(cosmosItemResponse));
    when(mockCosmosAsyncContainer.replaceItem(any(), anyString(), any(), any())).thenReturn(
        Mono.just(cosmosItemResponse));
    when(mockCosmosAsyncContainer.deleteItem(anyString(), (PartitionKey) any())).thenReturn(Mono.empty());
    Duration mockDuration = mock(Duration.class);
    when(mockDuration.toMillis()).thenReturn(Long.valueOf(10));
    mockNotFoundCosmosException = mock(CosmosException.class);
    when(mockNotFoundCosmosException.getStatusCode()).thenReturn(STATUS_NOT_FOUND);
    when(mockNotFoundCosmosException.getRetryAfterDuration()).thenReturn(mockDuration);

    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, storageConnection);
    configProps.setProperty(AzureCloudConfig.COSMOS_ENDPOINT, "http://ambry.beyond-the-cosmos.com:443");
    configProps.setProperty(AzureCloudConfig.COSMOS_DATABASE, "ambry");
    configProps.setProperty(AzureCloudConfig.COSMOS_COLLECTION, "metadata");
    configProps.setProperty(AzureCloudConfig.COSMOS_DELETED_CONTAINER_COLLECTION, "deletedContainer");
    configProps.setProperty(AzureCloudConfig.COSMOS_KEY, "cosmos-key");
    configProps.setProperty("clustermap.cluster.name", "main");
    configProps.setProperty("clustermap.datacenter.name", "uswest");
    configProps.setProperty("clustermap.host.name", "localhost");
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_AUTHORITY,
        "https://login.microsoftonline.com/test-account/");
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_CLIENTID, "client-id");
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_SECRET, "client-secret");
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_ENDPOINT, "https://azure_storage.blob.core.windows.net");
    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_CLIENT_CLASS,
        "com.github.ambry.cloud.azure.ConnectionStringBasedStorageClient");
    vcrMetrics = new VcrMetrics(new MetricRegistry());
    azureMetrics = new AzureMetrics(new MetricRegistry());
    clusterMap = mock(ClusterMap.class);
    azureDest = new AzureCloudDestination(mockServiceAsyncClient, mockBlobBatchAsyncClient, mockCosmosAsyncClient,
        mockCosmosAsyncDatabase, mockCosmosAsyncContainer, "ambry", "metadata", "deletedContainer", clusterName,
        azureMetrics, defaultAzureReplicationFeedType, clusterMap, false, configProps);
  }

  @After
  public void tearDown() throws Exception {
    if (azureDest != null) {
      azureDest.close();
    }
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
    mockBlobExistence(true);
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
    assertTrue("Expected success", azureDest.deleteBlob(blobId, deletionTime, (short) 0, dummyCloudUpdateValidator));
    assertEquals(1, azureMetrics.blobUpdatedCount.getCount());
    assertEquals(0, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(1, azureMetrics.blobUpdateTime.getCount());
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test normal undelete. */
  @Test
  public void testUndelete() throws Exception {
    mockBlobExistence(true);
    assertEquals(0, azureDest.undeleteBlob(blobId, (short) 0, dummyCloudUpdateValidator));
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
    try {
      azureDest.updateBlobExpiration(blobId, expirationTime, dummyCloudUpdateValidator);
    } catch (Exception ex) {
      fail("Expected success, instead for exception " + ex.toString());
    }
    assertEquals(1, azureMetrics.blobUpdatedCount.getCount());
    assertEquals(0, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(1, azureMetrics.blobUpdateTime.getCount());
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test upload of existing blob. */
  @Test
  public void testUploadExists() throws Exception {
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_ALREADY_EXISTS);
    when(mockBlockBlobAsyncClient.uploadWithResponse(any(), anyLong(), any(), any(), any(), any(), any())).thenReturn(
        Mono.error(ex));
    assertFalse("Upload of existing blob should return false", uploadDefaultBlob());
    assertEquals(1, azureMetrics.blobUploadRequestCount.getCount());
    assertEquals(0, azureMetrics.blobUploadSuccessCount.getCount());
    assertEquals(1, azureMetrics.blobUploadConflictCount.getCount());
    assertEquals(0, azureMetrics.backupErrorCount.getCount());
    // Make sure the metadata doc was created
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
  }

  /** Test upload when blob throws exception. */
  @Test
  public void testUploadBlobException() {
    BlobStorageException ex = mockStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(mockBlockBlobAsyncClient.uploadWithResponse(any(), anyLong(), any(), any(), any(), any(), any())).thenReturn(
        Mono.error(ex));
    expectCloudStorageException(() -> uploadDefaultBlob(), BlobStorageException.class);
    verifyUploadErrorMetrics(false);
  }

  /** Test upload when doc client throws exception. */
  @Test
  public void testUploadDocClientException() throws Exception {
    when(mockCosmosAsyncContainer.upsertItem(any(), any(), any())).thenReturn(Mono.error(mockNotFoundCosmosException));
    expectCloudStorageException(() -> uploadDefaultBlob(), CosmosException.class);
  }

  /**
   * Test download of non existent blob
   */
  @Test
  public void testDownloadNotFound() {
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(Mono.error(ex));
    expectCloudStorageException(() -> downloadBlob(blobId), BlobStorageException.class);
    verifyDownloadErrorMetrics();
  }

  /** Test download when blob throws exception. */
  @Test
  public void testDownloadBlobException() {
    BlobStorageException ex = mockStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(Mono.error(ex));
    expectCloudStorageException(() -> downloadBlob(blobId), BlobStorageException.class);
    verifyDownloadErrorMetrics();
  }

  /** Test delete of nonexistent blob. */
  @Test
  public void testDeleteBlobNotFound() {
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.error(ex));
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId, deletionTime, (short) 0, dummyCloudUpdateValidator),
        BlobStorageException.class);
    assertEquals(1, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(1, azureMetrics.storageErrorCount.getCount());
  }

  /** Test undelete of nonexistent blob. */
  @Test
  public void testUndeleteBlobNotFound() {
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.error(ex));
    expectCloudStorageException(() -> azureDest.undeleteBlob(blobId, (short) 0, dummyCloudUpdateValidator),
        BlobStorageException.class);
    assertEquals(1, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(1, azureMetrics.storageErrorCount.getCount());
  }

  /** Test update of nonexistent blob. */
  @Test
  public void testUpdateBlobNotFound() {
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.error(ex));
    expectCloudStorageException(() -> azureDest.updateBlobExpiration(blobId, expirationTime, dummyCloudUpdateValidator),
        BlobStorageException.class);
    assertEquals(1, azureMetrics.blobUpdateErrorCount.getCount());
    assertEquals(1, azureMetrics.storageErrorCount.getCount());
  }

  /** Test update methods when ABS throws exception. */
  @Test
  public void testUpdateBlobException() {
    BlobStorageException ex = mockStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(mockBlockBlobAsyncClient.setMetadataWithResponse(any(), any())).thenReturn(Mono.error(ex));
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId, deletionTime, (short) 0, dummyCloudUpdateValidator),
        BlobStorageException.class);
    expectCloudStorageException(() -> azureDest.updateBlobExpiration(blobId, expirationTime, dummyCloudUpdateValidator),
        BlobStorageException.class);
    expectCloudStorageException(() -> azureDest.undeleteBlob(blobId, (short) 0, dummyCloudUpdateValidator),
        BlobStorageException.class);
    verifyUpdateErrorMetrics(3, false);
  }

  /** Test update methods when record not found in Cosmos. */
  @Test
  public void testUpdateCosmosNotFound() throws Exception {
    mockBlobExistence(true);
    when(mockCosmosAsyncContainer.readItem(anyString(), any(), any())).thenReturn(
        Mono.error(mockNotFoundCosmosException));
    assertTrue("Expected update to recover",
        azureDest.deleteBlob(blobId, deletionTime, (short) 0, dummyCloudUpdateValidator));
    assertTrue("Expected update to recover", azureDest.undeleteBlob(blobId, (short) 0, dummyCloudUpdateValidator) == 0);
    try {
      azureDest.updateBlobExpiration(blobId, expirationTime, dummyCloudUpdateValidator);
    } catch (Exception ex) {
      fail("Expected update to recover");
    }
    verify(mockCosmosAsyncContainer, times(3)).upsertItem(any(), any(), any());
    assertEquals("Expected two recoveries", 3, azureMetrics.blobUpdateRecoverCount.getCount());
  }

  /** Test delete when record is in Cosmos but not ABS. */
  @Test
  public void testDeleteAfterPartialCompaction() throws Exception {
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.error(ex));
    // Rig Cosmos to return us a deleted blob on read request
    long deletionTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10);
    CloudBlobMetadata deletedMetadata = new CloudBlobMetadata().setId(blobId.getID()).setDeletionTime(deletionTime);
    CosmosItemResponse mockResponse = mock(CosmosItemResponse.class);
    when(mockResponse.getItem()).thenReturn(deletedMetadata);
    when(mockCosmosAsyncContainer.readItem(anyString(), any(), any())).thenReturn(Mono.just(mockResponse));
    // Now delete the puppy, Cosmos record should get purged.
    try {
      assertFalse("Expected update to recover and return false",
          azureDest.deleteBlob(blobId, deletionTime, (short) 0, dummyCloudUpdateValidator));
    } catch (CloudStorageException cex) {
      assertTrue(cex.getCause() instanceof BlobStorageException);
      assertEquals(((BlobStorageException) cex.getCause()).getErrorCode(), BlobErrorCode.BLOB_NOT_FOUND);
    }
    assertEquals("Expected recovery", 1, azureMetrics.blobUpdateRecoverCount.getCount());
    verify(mockCosmosAsyncContainer).deleteItem(anyString(), (PartitionKey) any());
  }

  /** Test update methods when Cosmos throws other exception. */
  @Test
  public void testUpdateCosmosException() {
    mockBlobExistence(true);
    Duration mockDuration = mock(Duration.class);
    when(mockDuration.toMillis()).thenReturn(Long.valueOf(10));
    CosmosException cosmosException = mock(CosmosException.class);
    when(cosmosException.getStatusCode()).thenReturn(500);
    when(cosmosException.getRetryAfterDuration()).thenReturn(mockDuration);
    when(mockCosmosAsyncContainer.readItem(anyString(), any(), any())).thenReturn(Mono.error(cosmosException));
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId, deletionTime, (short) 0, dummyCloudUpdateValidator),
        CosmosException.class);
    expectCloudStorageException(() -> azureDest.updateBlobExpiration(blobId, expirationTime, dummyCloudUpdateValidator),
        CosmosException.class);
    expectCloudStorageException(() -> azureDest.undeleteBlob(blobId, (short) 0, dummyCloudUpdateValidator),
        CosmosException.class);
    verifyUpdateErrorMetrics(3, true);
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

  /** Test to make sure that getting metadata for single blob calls ABS when not vcr and Cosmos when vcr. */
  @Test
  public void testGetOneMetadata() throws Exception {
    //
    // Test 1: isVcr = false (already setup)
    //
    // Get for existing blob
    Response<BlobProperties> mockResponse = mock(Response.class);
    BlobProperties mockProperties = mock(BlobProperties.class);
    CloudBlobMetadata blobMetadata = new CloudBlobMetadata(blobId, 0, -1, 0, null);
    Map<String, String> propertyMap = blobMetadata.toMap();
    when(mockProperties.getMetadata()).thenReturn(propertyMap);
    when(mockResponse.getValue()).thenReturn(mockProperties);
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.just(mockResponse));
    List<BlobId> singleBlobList = Collections.singletonList(blobId);
    Map<String, CloudBlobMetadata> metadataMap = azureDest.getBlobMetadata(singleBlobList);
    assertEquals("Expected map of one", 1, metadataMap.size());
    verifyZeroInteractions(mockCosmosAsyncContainer);
    verify(mockBlockBlobAsyncClient).getPropertiesWithResponse(any());

    // Get for nonexistent blob
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.error(ex));
    metadataMap = azureDest.getBlobMetadata(singleBlobList);
    assertTrue("Expected empty map", metadataMap.isEmpty());
    verifyZeroInteractions(mockCosmosAsyncContainer);
    verify(mockBlockBlobAsyncClient, times(2)).getPropertiesWithResponse(any());

    //
    // Test 2: isVcr = true
    //
    azureDest.close();
    azureDest = new AzureCloudDestination(mockServiceAsyncClient, mockBlobBatchAsyncClient, mockCosmosAsyncClient,
        mockCosmosAsyncDatabase, mockCosmosAsyncContainer, "ambry", "metadata", "deletedContainer", clusterName,
        azureMetrics, defaultAzureReplicationFeedType, clusterMap, true, configProps);
    // Existing blob
    FeedResponse feedResponse = mock(FeedResponse.class);
    when(feedResponse.getResults()).thenReturn(Collections.singletonList(blobMetadata));
    CosmosPagedFlux cosmosPagedFlux = mock(CosmosPagedFlux.class);
    when(cosmosPagedFlux.byPage()).thenReturn(Flux.just(feedResponse));
    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(cosmosPagedFlux);
    metadataMap = azureDest.getBlobMetadata(singleBlobList);
    assertEquals("Expected map of one", 1, metadataMap.size());
    verify(mockBlockBlobAsyncClient, times(2)).getPropertiesWithResponse(any());
  }

  /** Test querying metadata. */
  @Test
  public void testQueryMetadata() throws Exception {
    int batchSize = AzureCloudConfig.DEFAULT_QUERY_BATCH_SIZE;
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
    try {
      azureDest = new AzureCloudDestination(mockServiceAsyncClient, mockBlobBatchAsyncClient, mockCosmosAsyncClient,
          mockCosmosAsyncDatabase, mockCosmosAsyncContainer, "ambry", "metadata", "deletedContainer", clusterName,
          azureMetrics, defaultAzureReplicationFeedType, clusterMap, false, configProps);
      List<BlobId> blobIdList = new ArrayList<>();
      List<CloudBlobMetadata> cloudBlobMetadataList = new ArrayList<>();
      for (int j = 0; j < numBlobs; j++) {
        BlobId blobId = generateBlobId();
        blobIdList.add(blobId);
        CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
            CloudBlobMetadata.EncryptionOrigin.NONE);
        cloudBlobMetadataList.add(inputMetadata);
      }

      FeedResponse feedResponse = mock(FeedResponse.class);
      when(feedResponse.getResults()).thenReturn(cloudBlobMetadataList);
      CosmosPagedFlux cosmosPagedFlux = mock(CosmosPagedFlux.class);
      when(cosmosPagedFlux.byPage()).thenReturn(Flux.just(feedResponse));
      when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(cosmosPagedFlux);

      Set<BlobId> blobIdSet = new HashSet<>(blobIdList);
      assertEquals(blobIdList.size(), blobIdSet.size());
      Map<String, CloudBlobMetadata> metadataMap = azureDest.getBlobMetadata(blobIdList);
      for (BlobId blobId : blobIdList) {
        assertEquals("Unexpected id in metadata", blobId.getID(), metadataMap.get(blobId.getID()).getId());
      }
      assertEquals(expectedQueries, azureMetrics.documentQueryCount.getCount());
      assertEquals(expectedQueries, azureMetrics.missingKeysQueryTime.getCount());
    } finally {
      azureDest.close();
    }
  }

  /** Test findEntriesSince when cloud destination uses change feed based token. */
  @Test
  public void testFindEntriesSinceUsingChangeFeed() throws Exception {
    long chunkSize = 110000;
    long maxTotalSize = 1000000; // between 9 and 10 chunks
    long startTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
    int totalBlobs = 20;

    // create metadata list where total size > maxTotalSize
    List<String> blobIdList = new ArrayList<>();
    List<CloudBlobMetadata> cloudBlobMetadataList = new ArrayList<>();
    for (int j = 0; j < totalBlobs; j++) {
      BlobId blobId = generateBlobId();
      blobIdList.add(blobId.getID());
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, chunkSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      inputMetadata.setUploadTime(startTime + j);
      cloudBlobMetadataList.add(inputMetadata);
    }

    MockChangeFeedQuery mockChangeFeedQuery = new MockChangeFeedQuery();
    AzureReplicationFeed azureReplicationFeed = null;
    try {
      azureReplicationFeed =
          new CosmosChangeFeedBasedReplicationFeed(mockChangeFeedQuery, azureMetrics, azureDest.getQueryBatchSize());
      FieldSetter.setField(azureDest, azureDest.getClass().getDeclaredField("azureReplicationFeed"),
          azureReplicationFeed);
      cloudBlobMetadataList.stream().forEach(doc -> mockChangeFeedQuery.add(doc));
      CosmosChangeFeedFindToken findToken = new CosmosChangeFeedFindToken();
      // Run the query
      FindResult findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
      List<CloudBlobMetadata> firstResult = findResult.getMetadataList();
      findToken = (CosmosChangeFeedFindToken) findResult.getUpdatedFindToken();
      assertEquals("Did not get expected doc count", maxTotalSize / chunkSize, firstResult.size());

      assertEquals("Find token has wrong end continuation token", (findToken).getIndex(), firstResult.size());
      assertEquals("Find token has wrong totalItems count", (findToken).getTotalItems(),
          Math.min(blobIdList.size(), azureDest.getQueryBatchSize()));
      assertEquals("Unexpected change feed cache miss count", 1, azureMetrics.changeFeedCacheMissRate.getCount());
      assertEquals("Unexpected change feed cache refresh count", 0, azureMetrics.changeFeedCacheRefreshRate.getCount());
      assertEquals("Unexpected change feed cache hit count", 0, azureMetrics.changeFeedCacheHitRate.getCount());

      cloudBlobMetadataList = cloudBlobMetadataList.subList(firstResult.size(), cloudBlobMetadataList.size());

      findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
      List<CloudBlobMetadata> secondResult = findResult.getMetadataList();
      findToken = (CosmosChangeFeedFindToken) findResult.getUpdatedFindToken();

      assertEquals("Unexpected doc count", maxTotalSize / chunkSize, secondResult.size());
      assertEquals("Unexpected first blobId", blobIdList.get(firstResult.size()), secondResult.get(0).getId());

      assertEquals("Find token has wrong totalItems count", (findToken).getTotalItems(),
          Math.min(blobIdList.size(), azureDest.getQueryBatchSize()));

      assertEquals("Unexpected change feed cache miss count", 1, azureMetrics.changeFeedCacheMissRate.getCount());
      assertEquals("Unexpected change feed cache refresh count", 0, azureMetrics.changeFeedCacheRefreshRate.getCount());
      assertEquals("Unexpected change feed cache hit count", 1, azureMetrics.changeFeedCacheHitRate.getCount());

      // Rerun with max size below blob size, and make sure it returns one result
      findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, chunkSize - 1);
      List<CloudBlobMetadata> thirdResult = findResult.getMetadataList();
      assertEquals("Expected one result", 1, thirdResult.size());
      findToken = (CosmosChangeFeedFindToken) findResult.getUpdatedFindToken();

      assertEquals("Unexpected change feed cache miss count", 1, azureMetrics.changeFeedCacheMissRate.getCount());
      assertEquals("Unexpected change feed cache refresh count", 0, azureMetrics.changeFeedCacheRefreshRate.getCount());
      assertEquals("Unexpected change feed cache hit count", 2, azureMetrics.changeFeedCacheHitRate.getCount());

      // Add more than AzureCloudConfig.cosmosQueryBatchSize blobs and test for correct change feed cache hits and misses.
      AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
      for (int j = 0; j < azureConfig.cosmosQueryBatchSize + 5; j++) {
        BlobId blobId = generateBlobId();
        blobIdList.add(blobId.getID());
        CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, 10,
            CloudBlobMetadata.EncryptionOrigin.NONE);
        inputMetadata.setUploadTime(startTime + j);
        cloudBlobMetadataList.add(inputMetadata);
      }
      cloudBlobMetadataList.stream().forEach(doc -> mockChangeFeedQuery.add(doc));

      // Final correct query to drain out all the blobs and trigger a cache refresh.
      String prevEndToken = findToken.getEndContinuationToken();
      findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, 1000000);
      findToken = (CosmosChangeFeedFindToken) findResult.getUpdatedFindToken();

      assertEquals("Unexpected change feed cache miss count", 1, azureMetrics.changeFeedCacheMissRate.getCount());
      assertEquals("Unexpected change feed cache refresh count", 1, azureMetrics.changeFeedCacheRefreshRate.getCount());
      assertEquals("Unexpected change feed cache hit count", 3, azureMetrics.changeFeedCacheHitRate.getCount());
      assertEquals("Since this would have triggered refresh, start token should have been previous token's end token",
          prevEndToken, findToken.getStartContinuationToken());

      // Query changefeed with invalid token and check for cache miss
      testFindEntriesSinceUsingChangeFeedWithInvalidToken(findToken);
    } finally {
      if (azureReplicationFeed != null) {
        azureReplicationFeed.close();
      }
    }
  }

  /** Test findEntriesSince when cloud destination uses update time based token. */
  @Test
  public void testFindEntriesSinceUsingUpdateTime() throws Exception {
    AzureCloudDestination updateTimeBasedAzureCloudDestination = null;
    try {
      updateTimeBasedAzureCloudDestination =
          new AzureCloudDestination(mockServiceAsyncClient, mockBlobBatchAsyncClient, mockCosmosAsyncClient,
              mockCosmosAsyncDatabase, mockCosmosAsyncContainer, "ambry", "metadata", "deletedContainer", clusterName,
              azureMetrics, AzureReplicationFeed.FeedType.COSMOS_UPDATE_TIME, clusterMap, false, configProps);
      testFindEntriesSinceWithUniqueUpdateTimes(updateTimeBasedAzureCloudDestination);
      testFindEntriesSinceWithNonUniqueUpdateTimes(updateTimeBasedAzureCloudDestination);
    } finally {
      if (updateTimeBasedAzureCloudDestination != null) {
        updateTimeBasedAzureCloudDestination.close();
      }
    }
  }

  /**
   * Query changefeed with invalid token and check for cache miss.
   * @param findToken {@link CosmosChangeFeedFindToken} to continue from.
   * @throws CloudStorageException
   */
  private void testFindEntriesSinceUsingChangeFeedWithInvalidToken(CosmosChangeFeedFindToken findToken)
      throws CloudStorageException {
    // Invalid session id.
    CosmosChangeFeedFindToken invalidFindToken =
        new CosmosChangeFeedFindToken(findToken.getBytesRead(), findToken.getStartContinuationToken(),
            findToken.getEndContinuationToken(), findToken.getIndex(), findToken.getTotalItems(),
            UUID.randomUUID().toString(), findToken.getVersion());
    FindResult findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), invalidFindToken, 10);
    findToken = (CosmosChangeFeedFindToken) findResult.getUpdatedFindToken();

    assertEquals("Unexpected change feed cache miss count", 2, azureMetrics.changeFeedCacheMissRate.getCount());
    assertEquals("Unexpected change feed cache refresh count", 1, azureMetrics.changeFeedCacheRefreshRate.getCount());
    assertEquals("Unexpected change feed cache hit count", 3, azureMetrics.changeFeedCacheHitRate.getCount());

    // invalid end token.
    invalidFindToken =
        new CosmosChangeFeedFindToken(findToken.getBytesRead(), findToken.getStartContinuationToken(), "5000",
            findToken.getIndex(), findToken.getTotalItems(), findToken.getCacheSessionId(), findToken.getVersion());
    findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), invalidFindToken, 10);
    findToken = (CosmosChangeFeedFindToken) findResult.getUpdatedFindToken();

    assertEquals("Unexpected change feed cache miss count", 3, azureMetrics.changeFeedCacheMissRate.getCount());
    assertEquals("Unexpected change feed cache refresh count", 1, azureMetrics.changeFeedCacheRefreshRate.getCount());
    assertEquals("Unexpected change feed cache hit count", 3, azureMetrics.changeFeedCacheHitRate.getCount());

    // invalid start token.
    invalidFindToken =
        new CosmosChangeFeedFindToken(findToken.getBytesRead(), "5000", findToken.getEndContinuationToken(),
            findToken.getIndex(), findToken.getTotalItems(), findToken.getCacheSessionId(), findToken.getVersion());
    try {
      azureDest.findEntriesSince(blobId.getPartition().toPathString(), invalidFindToken, 10);
    } catch (Exception ex) {
    }

    assertEquals("Unexpected change feed cache miss count", 4, azureMetrics.changeFeedCacheMissRate.getCount());
    assertEquals("Unexpected change feed cache refresh count", 1, azureMetrics.changeFeedCacheRefreshRate.getCount());
    assertEquals("Unexpected change feed cache hit count", 3, azureMetrics.changeFeedCacheHitRate.getCount());

    // invalid total items.
    invalidFindToken = new CosmosChangeFeedFindToken(findToken.getBytesRead(), findToken.getStartContinuationToken(),
        findToken.getEndContinuationToken(), findToken.getIndex(), 9000, findToken.getCacheSessionId(),
        findToken.getVersion());
    azureDest.findEntriesSince(blobId.getPartition().toPathString(), invalidFindToken, 10);

    assertEquals("Unexpected change feed cache miss count", 5, azureMetrics.changeFeedCacheMissRate.getCount());
    assertEquals("Unexpected change feed cache refresh count", 1, azureMetrics.changeFeedCacheRefreshRate.getCount());
    assertEquals("Unexpected change feed cache hit count", 3, azureMetrics.changeFeedCacheHitRate.getCount());
  }

  /**
   * Test findEntriesSince with all entries having unique updateTimes.
   * @throws Exception
   */
  private void testFindEntriesSinceWithUniqueUpdateTimes(AzureCloudDestination azureDest) throws Exception {
    long chunkSize = 110000;
    long maxTotalSize = 1000000; // between 9 and 10 chunks
    long startTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
    int totalBlobs = 20;

    // create metadata list where total size > maxTotalSize
    List<CloudBlobMetadata> docList = new ArrayList<>();
    List<String> blobIdList = new ArrayList<>();
    for (int j = 0; j < totalBlobs; j++) {
      BlobId blobId = generateBlobId();
      blobIdList.add(blobId.getID());
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, chunkSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      inputMetadata.setUploadTime(startTime + j);
      inputMetadata.setLastUpdateTime(startTime + j);
      docList.add(inputMetadata);
    }

    FeedResponse mockFeedResponse = mock(FeedResponse.class);
    when(mockFeedResponse.getResults()).thenReturn(docList);
    CosmosPagedFlux mockCosmosPagedFlux = mock(CosmosPagedFlux.class);
    when(mockCosmosPagedFlux.byPage()).thenReturn(Flux.just(mockFeedResponse));
    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(mockCosmosPagedFlux);

    CosmosUpdateTimeFindToken findToken = new CosmosUpdateTimeFindToken();
    // Run the query
    FindResult findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    List<CloudBlobMetadata> firstResult = findResult.getMetadataList();
    findToken = (CosmosUpdateTimeFindToken) findResult.getUpdatedFindToken();
    assertEquals("Did not get expected doc count", maxTotalSize / chunkSize, firstResult.size());

    docList = docList.subList(firstResult.size(), docList.size());
    assertEquals("Find token has wrong last update time", findToken.getLastUpdateTime(),
        firstResult.get(firstResult.size() - 1).getLastUpdateTime());
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", findToken.getLastUpdateTimeReadBlobIds(),
        new HashSet<>(Collections.singletonList(firstResult.get(firstResult.size() - 1).getId())));

    reset(mockFeedResponse);
    when(mockFeedResponse.getResults()).thenReturn(docList);

    findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    List<CloudBlobMetadata> secondResult = findResult.getMetadataList();
    findToken = (CosmosUpdateTimeFindToken) findResult.getUpdatedFindToken();
    assertEquals("Unexpected doc count", maxTotalSize / chunkSize, secondResult.size());
    assertEquals("Unexpected first blobId", blobIdList.get(firstResult.size()), secondResult.get(0).getId());

    reset(mockFeedResponse);
    when(mockFeedResponse.getResults()).thenReturn(docList);

    findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, chunkSize / 2);
    // Rerun with max size below blob size, and make sure it returns one result
    assertEquals("Expected one result", 1, findResult.getMetadataList().size());
  }

  /**
   * Test findEntriesSince with entries having non unique updateTimes.
   * @throws Exception
   */
  private void testFindEntriesSinceWithNonUniqueUpdateTimes(AzureCloudDestination azureDest) throws Exception {
    long chunkSize = 110000;
    long maxTotalSize = 1000000; // between 9 and 10 chunks
    long startTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
    int totalBlobs = 20;

    // create metadata list where total size > maxTotalSize
    List<CloudBlobMetadata> docList = new ArrayList<>();
    List<String> blobIdList = new ArrayList<>();
    for (int j = 0; j < totalBlobs - 1; j++) {
      BlobId blobId = generateBlobId();
      blobIdList.add(blobId.getID());
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, chunkSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      inputMetadata.setLastUpdateTime(startTime);
      docList.add(inputMetadata);
    }
    BlobId blobId = generateBlobId();
    blobIdList.add(blobId.getID());
    CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, chunkSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    inputMetadata.setLastUpdateTime(startTime + 1);
    docList.add(inputMetadata);

    FeedResponse mockFeedResponse = mock(FeedResponse.class);
    when(mockFeedResponse.getResults()).thenReturn(docList);
    CosmosPagedFlux mockCosmosPagedFlux = mock(CosmosPagedFlux.class);
    when(mockCosmosPagedFlux.byPage()).thenReturn(Flux.just(mockFeedResponse));
    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(mockCosmosPagedFlux);

    CosmosUpdateTimeFindToken findToken = new CosmosUpdateTimeFindToken();
    // Run the query
    FindResult findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    List<CloudBlobMetadata> firstResult = findResult.getMetadataList();
    findToken = (CosmosUpdateTimeFindToken) findResult.getUpdatedFindToken();
    assertEquals("Did not get expected doc count", maxTotalSize / chunkSize, firstResult.size());

    assertEquals("Find token has wrong last update time", findToken.getLastUpdateTime(),
        firstResult.get(firstResult.size() - 1).getLastUpdateTime());
    Set<String> resultBlobIdSet = firstResult.stream().map(CloudBlobMetadata::getId).collect(Collectors.toSet());
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", findToken.getLastUpdateTimeReadBlobIds(),
        resultBlobIdSet);

    findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, maxTotalSize);
    List<CloudBlobMetadata> secondResult = findResult.getMetadataList();
    CosmosUpdateTimeFindToken secondFindToken = (CosmosUpdateTimeFindToken) findResult.getUpdatedFindToken();
    assertEquals("Unexpected doc count", maxTotalSize / chunkSize, secondResult.size());
    assertEquals("Unexpected first blobId", blobIdList.get(firstResult.size()), secondResult.get(0).getId());

    assertEquals("Find token has wrong last update time", secondFindToken.getLastUpdateTime(),
        firstResult.get(firstResult.size() - 1).getLastUpdateTime());
    resultBlobIdSet.addAll(secondResult.stream().map(CloudBlobMetadata::getId).collect(Collectors.toSet()));
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", secondFindToken.getLastUpdateTimeReadBlobIds(),
        resultBlobIdSet);

    // Rerun with max size below blob size, and make sure it returns one result
    findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), findToken, chunkSize / 2);
    List<CloudBlobMetadata> finalResult = findResult.getMetadataList();
    assertEquals("Expected one result", 1, finalResult.size());

    // Rerun final time, and make sure that it returns all the remaining blobs
    findResult = azureDest.findEntriesSince(blobId.getPartition().toPathString(), secondFindToken, maxTotalSize);
    List<CloudBlobMetadata> thirdResult = findResult.getMetadataList();
    CosmosUpdateTimeFindToken thirdFindToken = (CosmosUpdateTimeFindToken) findResult.getUpdatedFindToken();
    assertEquals("Unexpected doc count", totalBlobs - (firstResult.size() + secondResult.size()), thirdResult.size());
    assertEquals("Unexpected first blobId", blobIdList.get(firstResult.size() + secondResult.size()),
        thirdResult.get(0).getId());

    assertEquals("Find token has wrong last update time", thirdFindToken.getLastUpdateTime(), startTime + 1);
    assertEquals("Find token has wrong lastUpdateTimeReadBlobIds", thirdFindToken.getLastUpdateTimeReadBlobIds(),
        new HashSet<>(Collections.singletonList(thirdResult.get(thirdResult.size() - 1).getId())));
  }

  private void mockBlobExistence(boolean exists) {
    when(mockBlockBlobAsyncClient.exists()).thenReturn(Mono.just(exists));
    if (exists) {
      BlobDownloadAsyncResponse response = mock(BlobDownloadAsyncResponse.class);
      byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
      when(response.getValue()).thenReturn(Flux.just(ByteBuffer.wrap(randomBytes)));
      when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(
          Mono.just(response));
    } else {
      BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
      when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(Mono.error(ex));
    }
  }

  /** Test constructor with invalid connection string. */
  @Test
  public void testInitClientException() throws IOException {
    CloudDestinationFactory factory =
        new AzureCloudDestinationFactory(new VerifiableProperties(configProps), new MetricRegistry(), clusterMap);
    CloudDestination cloudDestination = null;
    try {
      cloudDestination = factory.getCloudDestination();
      fail("Expected exception");
    } catch (IllegalStateException ex) {
    } finally {
      if (cloudDestination != null) {
        cloudDestination.close();
      }
    }
  }

  @Test
  @Ignore // until we set the retry policy to fail faster
  public void testAzureConnection() throws Exception {
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureCloudDestination dest = null;
    try {
      dest = new AzureCloudDestination(cloudConfig, azureConfig, clusterName, vcrMetrics, azureMetrics,
          defaultAzureReplicationFeedType, clusterMap);
      try {
        dest.getAzureBlobDataAccessor().testConnectivity();
        fail("Expected exception");
      } catch (IllegalStateException expected) {
      }
      try {
        dest.getCosmosDataAccessor().testConnectivity();
        fail("Expected exception");
      } catch (IllegalStateException expected) {
      }
    } finally {
      dest.close();
    }
  }

  /** Test initializing AzureCloudDestination with a proxy. This is currently ignored because there is no good way to
   * instantiate a {@link CosmosAsyncClient} without it attempting to contact the proxy.
   * @throws Exception
   */
  @Ignore
  @Test
  public void testProxy() throws Exception {
    AzureCloudDestination dest = null;
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    try {
      // Test without proxy
      dest = new AzureCloudDestination(cloudConfig, azureConfig, clusterName, vcrMetrics, azureMetrics,
          defaultAzureReplicationFeedType, clusterMap);
      assertNull("Expected null proxy for ABS", dest.getAzureBlobDataAccessor().getProxyOptions());
    } finally {
      if (dest != null) {
        dest.close();
      }
    }

    // Test with proxy
    String proxyHost = "azure-proxy.randomcompany.com";
    int proxyPort = 80;
    configProps.setProperty(CloudConfig.VCR_PROXY_HOST, proxyHost);
    configProps.setProperty(CloudConfig.VCR_PROXY_PORT, String.valueOf(proxyPort));
    cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    try {
      dest = new AzureCloudDestination(cloudConfig, azureConfig, clusterName, vcrMetrics, azureMetrics,
          defaultAzureReplicationFeedType, clusterMap);
      assertNotNull("Expected proxy for ABS", dest.getAzureBlobDataAccessor().getProxyOptions());
    } finally {
      if (dest != null) {
        dest.close();
      }
    }
  }

  /**
   * Verify the metric values after an upload error.
   * @param isDocument Flag indicating a CosmosException thrown.
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
   * @param isDocument Flag indicating a CosmosException thrown.
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
   * @return a {@link BlobStorageException} with given error code.
   * @param errorCode the {@link BlobErrorCode} to return.
   */
  private BlobStorageException mockStorageException(BlobErrorCode errorCode) {
    BlobStorageException mockException = mock(BlobStorageException.class);
    when(mockException.getErrorCode()).thenReturn(errorCode);
    return mockException;
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
}
