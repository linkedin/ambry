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

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.azure.cosmos.implementation.HttpConstants.StatusCodes.*;
import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/** Test cases for {@link CosmosDataAccessor} */
@RunWith(MockitoJUnitRunner.class)
public class CosmosDataAccessorTest {

  private CosmosDataAccessor cosmosAccessor;
  private CosmosAsyncClient mockCosmosAsyncClient;
  private CosmosAsyncDatabase mockCosmosAsyncDatabase;
  private CosmosAsyncContainer mockCosmosAsyncContainer;
  private AzureMetrics azureMetrics;
  private BlobId blobId;
  private final int blobSize = 1024;
  private CloudBlobMetadata blobMetadata;

  @Before
  public void setup() {
    mockCosmosAsyncClient = mock(CosmosAsyncClient.class);
    mockCosmosAsyncDatabase = mock(CosmosAsyncDatabase.class);
    mockCosmosAsyncContainer = mock(CosmosAsyncContainer.class);
    byte dataCenterId = 66;
    short accountId = 101;
    short containerId = 5;
    PartitionId partitionId = new MockPartitionId();
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);
    blobMetadata = new CloudBlobMetadata(blobId, System.currentTimeMillis(), Utils.Infinite_Time, blobSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    azureMetrics = new AzureMetrics(new MetricRegistry());
    VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());

    when(mockCosmosAsyncDatabase.getContainer(anyString())).thenReturn(mockCosmosAsyncContainer);
    // Mock read, upsert, replacing of items.
    CosmosItemResponse cosmosItemResponse = mock(CosmosItemResponse.class);
    when(cosmosItemResponse.getItem()).thenReturn(blobMetadata);
    when(mockCosmosAsyncContainer.readItem(anyString(), any(), any())).thenReturn(Mono.just(cosmosItemResponse));
    when(mockCosmosAsyncContainer.upsertItem(any(), any(), any())).thenReturn(Mono.just(cosmosItemResponse));
    when(mockCosmosAsyncContainer.replaceItem(any(), anyString(), any(), any())).thenReturn(
        Mono.just(cosmosItemResponse));

    // Mock querying items
    FeedResponse feedResponse = mock(FeedResponse.class);
    when(feedResponse.getResults()).thenReturn(Collections.singletonList(blobMetadata));
    CosmosPagedFlux cosmosPagedFlux = mock(CosmosPagedFlux.class);
    when(cosmosPagedFlux.byPage()).thenReturn(Flux.just(feedResponse));
    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(cosmosPagedFlux);

    // Mock querying change feed
    when(mockCosmosAsyncContainer.queryChangeFeed(any(), any())).thenReturn(cosmosPagedFlux);

    cosmosAccessor =
        new CosmosDataAccessor(mockCosmosAsyncClient, mockCosmosAsyncDatabase, mockCosmosAsyncContainer, "ambry",
            "metadata", "deletedContainer", vcrMetrics, azureMetrics);
  }

  /**
   * Test normal upsert.
   */
  @Test
  public void testUpsertNormal() {
    cosmosAccessor.upsertMetadataAsync(blobMetadata).join();
    assertEquals(1, azureMetrics.documentCreateTime.getCount());
  }

  /** Test update. */
  @Test
  public void testUpdateNormal() {
    cosmosAccessor.updateMetadataAsync(blobId,
        Collections.singletonMap(CloudBlobMetadata.FIELD_DELETION_TIME, Long.toString(System.currentTimeMillis())))
        .join();
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
  }

  /** Test update with conflict. */
  @Test
  public void testUpdateConflict() {
    CosmosException cosmosException = mock(CosmosException.class);
    when(cosmosException.getStatusCode()).thenReturn(PRECONDITION_FAILED);
    when(mockCosmosAsyncContainer.replaceItem(any(), anyString(), any(), any())).thenReturn(
        Mono.error(cosmosException));
    try {
      cosmosAccessor.updateMetadataAsync(blobId,
          Collections.singletonMap(CloudBlobMetadata.FIELD_DELETION_TIME, Long.toString(System.currentTimeMillis())))
          .join();
      fail("Expected exception");
    } catch (CompletionException ex) {
      // expected
      assertTrue(ex.getCause() instanceof CosmosException);
    }
    assertEquals(1, azureMetrics.documentReadTime.getCount());
    assertEquals(1, azureMetrics.documentUpdateTime.getCount());
    assertEquals(1, azureMetrics.blobUpdateConflictCount.getCount());
  }

  /** Test query metadata. */
  @Test
  public void testQueryNormal() {
    List<CloudBlobMetadata> metadataList = doQueryMetadata();
    assertEquals("Expected single entry", 1, metadataList.size());
    CloudBlobMetadata outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.missingKeysQueryTime.getCount());
  }

  /** Test change feed query. Ignoring for now since {@link CosmosChangeFeedRequestOptions} instance needed for change
   * feed query might also need to be mocked in some way before executing the query */
  @Test
  public void testQueryChangeFeedNormal() {
    List<CloudBlobMetadata> metadataList = doQueryChangeFeed();
    assertEquals("Expected single entry", 1, metadataList.size());
    CloudBlobMetadata outputMetadata = metadataList.get(0);
    assertEquals("Returned metadata does not match original", blobMetadata, outputMetadata);
    assertEquals(1, azureMetrics.changeFeedQueryCount.getCount());
    assertEquals(0, azureMetrics.changeFeedQueryFailureCount.getCount());

    // test when queryChangeFeed throws exception
    CosmosPagedFlux mockCosmosPagedFlux = mock(CosmosPagedFlux.class);
    when(mockCosmosPagedFlux.byPage()).thenReturn(Flux.error(mock(CosmosException.class)));
    when(mockCosmosAsyncContainer.queryChangeFeed(any(), any())).thenReturn(mockCosmosPagedFlux);

    try {
      doQueryChangeFeed();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof CosmosException);
    }
    assertEquals(2, azureMetrics.changeFeedQueryCount.getCount());
    assertEquals(1, azureMetrics.changeFeedQueryFailureCount.getCount());
  }

  /** Utility method to run metadata query with default parameters. */
  private List<CloudBlobMetadata> doQueryMetadata() {
    return cosmosAccessor.queryMetadataAsync(blobId.getPartition().toPathString(), "select * from c",
        azureMetrics.missingKeysQueryTime).join();
  }

  /** Utility method to run metadata query with default parameters. */
  private List<CloudBlobMetadata> doQueryChangeFeed() {
    List<CloudBlobMetadata> changeFeed = new ArrayList<>();
    CosmosChangeFeedRequestOptions mockFeedRequestOptions = mock(CosmosChangeFeedRequestOptions.class);
    cosmosAccessor.queryChangeFeedAsync(mockFeedRequestOptions, changeFeed, azureMetrics.changeFeedQueryTime).join();
    return changeFeed;
  }
}
