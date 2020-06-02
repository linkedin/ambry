/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.StoredProcedureResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import rx.Observable;

import static com.github.ambry.cloud.azure.AzureTestUtils.*;
import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;


/** Test cases for {@link AzureStorageCompactor} */
@RunWith(MockitoJUnitRunner.class)
public class AzureStorageCompactorTest {

  private final String base64key = Base64.encodeBase64String("ambrykey".getBytes());
  private final String storageConnection =
      "DefaultEndpointsProtocol=https;AccountName=ambry;AccountKey=" + base64key + ";EndpointSuffix=core.windows.net";
  private final String collectionLink = "ambry/metadata";
  private final String clusterName = "main";
  private Properties configProps = new Properties();
  private AzureStorageCompactor azureStorageCompactor;
  private BlobServiceClient mockServiceClient;
  private BlobBatchClient mockBlobBatchClient;
  private AsyncDocumentClient mockumentClient;
  private AzureMetrics azureMetrics;
  private int blobSize = 1024;
  private byte dataCenterId = 66;
  private short accountId = 101;
  private short containerId = 5;
  private BlobId blobId;
  private CloudBlobMetadata blobMetadata;
  private String partitionPath;

  @Before
  public void setup() throws Exception {
    long partition = 666;
    PartitionId partitionId = new MockPartitionId(partition, MockClusterMap.DEFAULT_PARTITION_CLASS);
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    blobMetadata =
        new CloudBlobMetadata(blobId, 0, Utils.Infinite_Time, blobSize, CloudBlobMetadata.EncryptionOrigin.NONE);
    partitionPath = blobMetadata.getPartitionId();

    mockServiceClient = mock(BlobServiceClient.class);
    mockBlobBatchClient = mock(BlobBatchClient.class);
    mockumentClient = mock(AsyncDocumentClient.class);

    configProps.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, storageConnection);
    configProps.setProperty(AzureCloudConfig.COSMOS_ENDPOINT, "http://ambry.beyond-the-cosmos.com:443");
    configProps.setProperty(AzureCloudConfig.COSMOS_COLLECTION_LINK, collectionLink);
    configProps.setProperty(AzureCloudConfig.COSMOS_KEY, "cosmos-key");
    configProps.setProperty("clustermap.cluster.name", "main");
    configProps.setProperty("clustermap.datacenter.name", "uswest");
    configProps.setProperty("clustermap.host.name", "localhost");
    azureMetrics = new AzureMetrics(new MetricRegistry());
    AzureBlobDataAccessor azureBlobDataAccessor =
        new AzureBlobDataAccessor(mockServiceClient, mockBlobBatchClient, clusterName, azureMetrics);
    CosmosDataAccessor cosmosDataAccessor = new CosmosDataAccessor(mockumentClient, collectionLink, azureMetrics);
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(new Properties()));
    VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());

    azureStorageCompactor =
        new AzureStorageCompactor(azureBlobDataAccessor, cosmosDataAccessor, cloudConfig, vcrMetrics, azureMetrics);
  }

  @After
  public void tearDown() throws Exception {
    if (azureStorageCompactor != null) {
      azureStorageCompactor.shutdown();
    }
  }

  /** Test compaction method */
  @Test
  public void testCompaction() throws Exception {
    Observable<FeedResponse<Document>> mockResponse = mock(Observable.class);
    List<Document> docList =
        Collections.singletonList(AzureTestUtils.createDocumentFromCloudBlobMetadata(blobMetadata));
    mockObservableForQuery(docList, mockResponse);
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        mockResponse);
    BlobBatch mockBatch = mock(BlobBatch.class);
    when(mockBlobBatchClient.getBlobBatch()).thenReturn(mockBatch);
    Response<Void> okResponse = mock(Response.class);
    when(okResponse.getStatusCode()).thenReturn(202);
    when(mockBatch.deleteBlob(anyString(), anyString())).thenReturn(okResponse);
    Observable<StoredProcedureResponse> mockBulkDeleteResponse = getMockBulkDeleteResponse(1);
    when(mockumentClient.executeStoredProcedure(anyString(), any(RequestOptions.class), any())).thenReturn(
        mockBulkDeleteResponse);
    azureStorageCompactor.compactPartition(partitionPath);
    verify(mockumentClient, atLeast(2)).queryDocuments(eq(collectionLink), any(SqlQuerySpec.class), any());
    verify(mockumentClient, atLeast(1)).executeStoredProcedure(eq(collectionLink + CosmosDataAccessor.BULK_DELETE_SPROC),
        any(), any());
    verify(mockBlobBatchClient, atLeast(1)).submitBatchWithResponse(any(BlobBatch.class), anyBoolean(), any(), any());
  }

  /** Test getDeadBlobs method */
  @Test
  public void testGetDeadBlobs() throws Exception {
    Observable<FeedResponse<Document>> mockResponse = getMockedObservableForQueryWithNoResults();
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        mockResponse);
    long now = System.currentTimeMillis();
    List<CloudBlobMetadata> metadataList =
        azureStorageCompactor.getDeadBlobs(blobId.getPartition().toPathString(), CloudBlobMetadata.FIELD_DELETION_TIME,
            1, now, 10);
    assertEquals("Expected no deleted blobs", 0, metadataList.size());
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.deadBlobsQueryTime.getCount());

    metadataList = azureStorageCompactor.getDeadBlobs(blobId.getPartition().toPathString(),
        CloudBlobMetadata.FIELD_EXPIRATION_TIME, 1, now, 10);
    assertEquals("Expected no expired blobs", 0, metadataList.size());
    assertEquals(2, azureMetrics.documentQueryCount.getCount());
    assertEquals(2, azureMetrics.deadBlobsQueryTime.getCount());
  }

  /** Test purgeBlobs success */
  @Test
  public void testPurge() throws Exception {
    BlobBatch mockBatch = mock(BlobBatch.class);
    when(mockBlobBatchClient.getBlobBatch()).thenReturn(mockBatch);
    Response<Void> okResponse = mock(Response.class);
    when(okResponse.getStatusCode()).thenReturn(202);
    when(mockBatch.deleteBlob(anyString(), anyString())).thenReturn(okResponse);
    Observable<StoredProcedureResponse> mockBulkDeleteResponse = getMockBulkDeleteResponse(1);
    when(mockumentClient.executeStoredProcedure(anyString(), any(RequestOptions.class), any())).thenReturn(
        mockBulkDeleteResponse);
    CloudBlobMetadata cloudBlobMetadata =
        new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize,
            CloudBlobMetadata.EncryptionOrigin.NONE);
    assertEquals("Expected success", 1, azureStorageCompactor.purgeBlobs(Collections.singletonList(cloudBlobMetadata)));
    assertEquals(1, azureMetrics.blobDeletedCount.getCount());
    assertEquals(0, azureMetrics.blobDeleteErrorCount.getCount());
  }

  /** Test purgeBlobs error */
  @Test
  public void testPurgeError() throws Exception {
    // Unsuccessful case
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_ARCHIVED);
    BlobBatch mockBatch = mock(BlobBatch.class);
    Response<Void> mockResponse = mock(Response.class);
    when(mockResponse.getStatusCode()).thenThrow(ex);
    when(mockBlobBatchClient.getBlobBatch()).thenReturn(mockBatch);
    when(mockBatch.deleteBlob(anyString(), anyString())).thenReturn(mockResponse);
    CloudBlobMetadata cloudBlobMetadata =
        new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize,
            CloudBlobMetadata.EncryptionOrigin.NONE);
    try {
      azureStorageCompactor.purgeBlobs(Collections.singletonList(cloudBlobMetadata));
      fail("Expected CloudStorageException");
    } catch (CloudStorageException bex) {
    }
    assertEquals(0, azureMetrics.blobDeletedCount.getCount());
    assertEquals(1, azureMetrics.blobDeleteErrorCount.getCount());
  }

  /**
   * @return a {@link BlobStorageException} with given error code.
   * @param errorCode the {@link BlobErrorCode} to return.
   */
  private BlobStorageException mockStorageException(BlobErrorCode errorCode) {
    BlobStorageException mockException = mock(BlobStorageException.class);
    lenient().when(mockException.getErrorCode()).thenReturn(errorCode);
    return mockException;
  }
}
