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
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.Error;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/** Test cases for {@link CosmosDataAccessor} */
@RunWith(MockitoJUnitRunner.class)
public class CosmosDataAccessorTest {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private CosmosDataAccessor cosmosAccessor;
  private DocumentClient mockumentClient;
  private ResourceResponse<Document> mockResponse;
  private DocumentClientException retryException;
  private AzureMetrics azureMetrics;
  private BlobId blobId;
  private int blobSize = 1024;
  private CloudBlobMetadata blobMetadata;
  int maxRetries = 3;

  @Before
  public void setup() throws Exception {
    mockumentClient = mock(DocumentClient.class);
    mockResponse = mock(ResourceResponse.class);
    retryException = new DocumentClientException(HttpConstants.StatusCodes.TOO_MANY_REQUESTS, new Error(),
        Collections.singletonMap(HttpConstants.HttpHeaders.RETRY_AFTER_IN_MILLISECONDS, "1"));

    byte dataCenterId = 66;
    short accountId = 101;
    short containerId = 5;
    PartitionId partitionId = new MockPartitionId();
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    blobMetadata = new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    azureMetrics = new AzureMetrics(new MetricRegistry());
    cosmosAccessor = new CosmosDataAccessor(mockumentClient, "ambry/metadata", maxRetries, azureMetrics);
  }

  /**
   * Test normal upsert.
   * @throws Exception
   */
  @Test
  public void testUpsertNormal() throws Exception {
    // Request succeeds first time
    when(mockumentClient.upsertDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenReturn(
        mockResponse);
    cosmosAccessor.upsertMetadata(blobMetadata);
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
    assertEquals(0, azureMetrics.retryCount.getCount());
    assertEquals(0, azureMetrics.retryWaitTime.getCount());
  }

  /**
   * Test upsert with one retry.
   * @throws Exception
   */
  @Test
  public void testUpsertRetry() throws Exception {
    // Request gets 420 on first try, succeeds on retry
    when(mockumentClient.upsertDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenThrow(
        retryException).thenReturn(mockResponse);
    cosmosAccessor.upsertMetadata(blobMetadata);
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
    assertEquals(1, azureMetrics.retryCount.getCount());
    assertEquals(1, azureMetrics.retryWaitTime.getCount());
  }

  /**
   * Test upsert exhausts retries.
   * @throws Exception
   */
  @Test
  public void testUpsertExhaustRetries() throws Exception {
    // Request keeps getting 420 until we give up
    when(mockumentClient.upsertDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenThrow(
        retryException);
    try {
      cosmosAccessor.upsertMetadata(blobMetadata);
      fail("Expected operation to fail after too many retries");
    } catch (RuntimeException expected) {
    }
    assertEquals(0, azureMetrics.documentCreateTime.getCount());
    assertEquals(maxRetries, azureMetrics.retryCount.getCount());
    assertEquals(maxRetries, azureMetrics.retryWaitTime.getCount());
  }

  /** Test read. */
  @Test
  public void testReadNormal() throws Exception {
    // Request succeeds first time
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenReturn(mockResponse);
    cosmosAccessor.readMetadata(blobId);
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(0, azureMetrics.retryCount.getCount());
    assertEquals(0, azureMetrics.retryWaitTime.getCount());
  }

  /**
   * Test read with one retry.
   * @throws Exception
   */
  @Test
  public void testReadRetry() throws Exception {
    // Request gets 420 on first try, succeeds on retry
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenThrow(retryException)
        .thenReturn(mockResponse);
    cosmosAccessor.readMetadata(blobId);
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.retryCount.getCount());
    assertEquals(1, azureMetrics.retryWaitTime.getCount());
  }

  /**
   * Test read exhausts retries.
   * @throws Exception
   */
  @Test
  public void testReadExhaustRetries() throws Exception {
    // Request keeps getting 420 until we give up
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenThrow(retryException);
    try {
      cosmosAccessor.readMetadata(blobId);
      fail("Expected operation to fail after too many retries");
    } catch (RuntimeException expected) {
    }
    assertEquals(0, azureMetrics.documentReadTime.getCount());
    assertEquals(maxRetries, azureMetrics.retryCount.getCount());
    assertEquals(maxRetries, azureMetrics.retryWaitTime.getCount());
  }

  /** Test query metadata. */
  @Test
  public void testQueryNormal() throws Exception {
    FeedResponse<Document> feedResponse = getFeedResponse();
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenReturn(
        feedResponse);
    List<CloudBlobMetadata> metadataList = doQueryMetadata();
    assertEquals("Expected single entry", 1, metadataList.size());
    CloudBlobMetadata outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.missingKeysQueryTime.getCount());
    assertEquals(0, azureMetrics.retryCount.getCount());
    assertEquals(0, azureMetrics.retryWaitTime.getCount());
  }

  /** Test query one retry. */
  @Test
  public void testQueryRetry() throws Exception {
    FeedResponse<Document> feedResponse = getFeedResponse();
    when(mockumentClient.queryDocuments(anyString(), any(SqlQuerySpec.class), any(FeedOptions.class))).thenThrow(
        new IllegalStateException(retryException)).thenReturn(feedResponse);
    List<CloudBlobMetadata> metadataList = doQueryMetadata();
    assertEquals("Expected single entry", 1, metadataList.size());
    CloudBlobMetadata outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.missingKeysQueryTime.getCount());
    assertEquals(1, azureMetrics.retryCount.getCount());
    assertEquals(1, azureMetrics.retryWaitTime.getCount());
  }

  /**
   * @return a FeedResponse with a single document.
   */
  private FeedResponse<Document> getFeedResponse() throws Exception {
    QueryIterable<Document> mockIterable = mock(QueryIterable.class);
    List<Document> docList =
        Collections.singletonList(AzureTestUtils.createDocumentFromCloudBlobMetadata(blobMetadata, objectMapper));
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    when(feedResponse.getQueryIterable()).thenReturn(mockIterable);
    return feedResponse;
  }

  /** Utility method to run metadata query with default parameters. */
  private List<CloudBlobMetadata> doQueryMetadata() throws Exception {
    return cosmosAccessor.queryMetadata(blobId.getPartition().toPathString(), new SqlQuerySpec("select * from c"),
        azureMetrics.missingKeysQueryTime);
  }
}
