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
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import rx.Observable;
import rx.observables.BlockingObservable;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/** Test cases for {@link CosmosDataAccessor} */
@RunWith(MockitoJUnitRunner.class)
public class CosmosDataAccessorTest {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private CosmosDataAccessor cosmosAccessor;
  private AsyncDocumentClient mockumentClient;
  private ResourceResponse<Document> mockResponse;
  private AzureMetrics azureMetrics;
  private BlobId blobId;
  private int blobSize = 1024;
  private CloudBlobMetadata blobMetadata;
  int maxRetries = 3;

  @Before
  public void setup() throws Exception {
    mockumentClient = mock(AsyncDocumentClient.class);
    mockResponse = mock(ResourceResponse.class);

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
    Observable<ResourceResponse<Document>> mockResponse = mock(Observable.class);
    BlockingObservable<ResourceResponse<Document>> mockBlockingObservable = mock(BlockingObservable.class);
    when(mockResponse.toBlocking()).thenReturn(mockBlockingObservable);
    ResourceResponse<Document> mockResourceResponse = mock(ResourceResponse.class);
    when(mockBlockingObservable.single()).thenReturn(mockResourceResponse);
    when(mockumentClient.upsertDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenReturn(
        mockResponse);
    cosmosAccessor.upsertMetadata(blobMetadata);
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
    assertEquals(0, azureMetrics.retryCount.getCount());
    assertEquals(0, azureMetrics.retryWaitTime.getCount());
  }

  /** Test read. */
  @Test
  public void testReadNormal() throws Exception {
    Observable<ResourceResponse<Document>> mockResponse = mock(Observable.class);
    BlockingObservable<ResourceResponse<Document>> mockBlockingObservable = mock(BlockingObservable.class);
    when(mockResponse.toBlocking()).thenReturn(mockBlockingObservable);
    ResourceResponse<Document> mockResourceResponse = mock(ResourceResponse.class);
    when(mockBlockingObservable.single()).thenReturn(mockResourceResponse);
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenReturn(mockResponse);
    cosmosAccessor.readMetadata(blobId);
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(0, azureMetrics.retryCount.getCount());
    assertEquals(0, azureMetrics.retryWaitTime.getCount());
  }

  /** Test query metadata. */
  @Test
  public void testQueryNormal() throws Exception {
    Observable<FeedResponse<Document>> feedResponseObservable = getFeedResponseObservable();
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        feedResponseObservable);
    List<CloudBlobMetadata> metadataList = doQueryMetadata();
    assertEquals("Expected single entry", 1, metadataList.size());
    CloudBlobMetadata outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.missingKeysQueryTime.getCount());
    assertEquals(0, azureMetrics.retryCount.getCount());
    assertEquals(0, azureMetrics.retryWaitTime.getCount());
  }

  /**
   * @return a FeedResponse with a single document.
   */
  private Observable<FeedResponse<Document>> getFeedResponseObservable() throws Exception {
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    Observable<FeedResponse<Document>> mockResponse = mock(Observable.class);
    BlockingObservable<FeedResponse<Document>> mockBlockingObservable = mock(BlockingObservable.class);
    when(mockResponse.toBlocking()).thenReturn(mockBlockingObservable);
    Iterator<FeedResponse<Document>> mockIterator = mock(Iterator.class);
    when(mockBlockingObservable.getIterator()).thenReturn(mockIterator);
    List<Document> docList =
        Collections.singletonList(AzureTestUtils.createDocumentFromCloudBlobMetadata(blobMetadata, objectMapper));
    when(mockIterator.hasNext()).thenReturn(true).thenReturn(false);
    when(mockIterator.next()).thenReturn(feedResponse);
    when(feedResponse.getResults()).thenReturn(docList);

    return mockResponse;
  }

  /** Utility method to run metadata query with default parameters. */
  private List<CloudBlobMetadata> doQueryMetadata() throws Exception {
    return cosmosAccessor.queryMetadata(blobId.getPartition().toPathString(), new SqlQuerySpec("select * from c"),
        azureMetrics.missingKeysQueryTime);
  }
}
