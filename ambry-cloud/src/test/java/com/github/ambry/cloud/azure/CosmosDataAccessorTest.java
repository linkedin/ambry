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
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.ChangeFeedOptions;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import rx.Observable;

import static com.github.ambry.cloud.azure.AzureTestUtils.*;
import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/** Test cases for {@link CosmosDataAccessor} */
@RunWith(MockitoJUnitRunner.class)
public class CosmosDataAccessorTest {

  private CosmosDataAccessor cosmosAccessor;
  private AsyncDocumentClient mockumentClient;
  private AzureMetrics azureMetrics;
  private BlobId blobId;
  private int blobSize = 1024;
  private CloudBlobMetadata blobMetadata;

  @Before
  public void setup() {
    mockumentClient = mock(AsyncDocumentClient.class);
    byte dataCenterId = 66;
    short accountId = 101;
    short containerId = 5;
    PartitionId partitionId = new MockPartitionId();
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    blobMetadata = new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    azureMetrics = new AzureMetrics(new MetricRegistry());
    cosmosAccessor = new CosmosDataAccessor(mockumentClient, "ambry/metadata", azureMetrics);
  }

  /**
   * Test normal upsert.
   * @throws Exception
   */
  @Test
  public void testUpsertNormal() throws Exception {
    Observable<ResourceResponse<Document>> mockResponse = getMockedObservableForSingleResource(blobMetadata);
    when(mockumentClient.upsertDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenReturn(
        mockResponse);
    cosmosAccessor.upsertMetadata(blobMetadata);
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
  }

  /** Test update. */
  @Test
  public void testUpdateNormal() throws Exception {
    Observable<ResourceResponse<Document>> mockResponse = getMockedObservableForSingleResource(blobMetadata);
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenReturn(mockResponse);
    when(mockumentClient.replaceDocument(any(), any())).thenReturn(mockResponse);
    cosmosAccessor.updateMetadata(blobId, CloudBlobMetadata.FIELD_DELETION_TIME, System.currentTimeMillis());
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test update with conflict. */
  @Test
  public void testUpdateConflict() throws Exception {
    Observable<ResourceResponse<Document>> mockResponse = getMockedObservableForSingleResource(blobMetadata);
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenReturn(mockResponse);
    when(mockumentClient.replaceDocument(any(), any())).thenThrow(
        new RuntimeException(new DocumentClientException(HttpConstants.StatusCodes.PRECONDITION_FAILED)));
    try {
      cosmosAccessor.updateMetadata(blobId, CloudBlobMetadata.FIELD_DELETION_TIME, System.currentTimeMillis());
      fail("Expected exception");
    } catch (DocumentClientException ex) {
      // expected
    }
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
    assertEquals(1, azureMetrics.blobUpdateConflictCount.getCount());
  }

  /** Test query metadata. */
  @Test
  public void testQueryNormal() throws Exception {
    Observable<FeedResponse<Document>> mockResponse = mock(Observable.class);
    List<Document> docList =
        Collections.singletonList(AzureTestUtils.createDocumentFromCloudBlobMetadata(blobMetadata));
    mockObservableForQuery(docList, mockResponse);
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        mockResponse);
    List<CloudBlobMetadata> metadataList = doQueryMetadata();
    assertEquals("Expected single entry", 1, metadataList.size());
    CloudBlobMetadata outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.missingKeysQueryTime.getCount());
  }

  /** Test change feed query. */
  @Test
  public void testQueryChangeFeedNormal() throws Exception {
    Observable<FeedResponse<Document>> mockResponse = mock(Observable.class);
    List<Document> docList =
        Collections.singletonList(AzureTestUtils.createDocumentFromCloudBlobMetadata(blobMetadata));
    mockObservableForChangeFeedQuery(docList, mockResponse);
    when(mockumentClient.queryDocumentChangeFeed(anyString(), any(ChangeFeedOptions.class))).thenReturn(mockResponse);
    // test with non null requestContinuationToken
    List<CloudBlobMetadata> metadataList = doQueryChangeFeed("test");
    assertEquals("Expected single entry", 1, metadataList.size());
    CloudBlobMetadata outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(1, azureMetrics.changeFeedQueryCount.getCount());
    assertEquals(0, azureMetrics.changeFeedQueryFailureCount.getCount());

    // test with a null continuation token
    metadataList = doQueryChangeFeed(null);
    assertEquals("Expected single entry", 1, metadataList.size());
    outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(2, azureMetrics.changeFeedQueryCount.getCount());
    assertEquals(0, azureMetrics.changeFeedQueryFailureCount.getCount());

    // test when queryChangeFeed throws exception
    when(mockumentClient.queryDocumentChangeFeed(anyString(), any(ChangeFeedOptions.class))).thenThrow(
        new RuntimeException("mock exception", new DocumentClientException(404)));
    try {
      doQueryChangeFeed(null);
    } catch (DocumentClientException e) {
    }
    assertEquals(3, azureMetrics.changeFeedQueryCount.getCount());
    assertEquals(1, azureMetrics.changeFeedQueryFailureCount.getCount());
  }

  /** Utility method to run metadata query with default parameters. */
  private List<CloudBlobMetadata> doQueryMetadata() throws Exception {
    return cosmosAccessor.queryMetadata(blobId.getPartition().toPathString(), "select * from c",
        azureMetrics.missingKeysQueryTime);
  }

  /** Utility method to run metadata query with default parameters. */
  private List<CloudBlobMetadata> doQueryChangeFeed(String continuationToken) throws Exception {
    List<CloudBlobMetadata> changeFeed = new ArrayList<>();
    cosmosAccessor.queryChangeFeed(continuationToken, 1000, changeFeed, blobId.getPartition().toPathString(),
        azureMetrics.changeFeedQueryTime);
    return changeFeed;
  }
}
