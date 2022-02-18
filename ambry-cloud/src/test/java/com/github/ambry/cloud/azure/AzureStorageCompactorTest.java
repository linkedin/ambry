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
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosAsyncScripts;
import com.azure.cosmos.CosmosAsyncStoredProcedure;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.github.ambry.cloud.azure.AzureTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;


/** Test cases for {@link AzureStorageCompactor} */
@RunWith(MockitoJUnitRunner.class)
public class AzureStorageCompactorTest {

  private final String base64key = Base64.encodeBase64String("ambrykey".getBytes());
  private final String storageConnection =
      "DefaultEndpointsProtocol=https;AccountName=ambry;AccountKey=" + base64key + ";EndpointSuffix=core.windows.net";
  private final int blobSize = 1024;
  private final String partitionPath = String.valueOf(partition);
  private final int numBlobsPerQuery = 50;
  private final int numQueryBuckets = 4; // number of time range buckets to use
  private final long testTime = System.currentTimeMillis();
  private final Properties configProps = new Properties();
  private final List<CloudBlobMetadata> blobMetadataList = new ArrayList<>(numBlobsPerQuery);
  private AzureStorageCompactor azureStorageCompactor;
  private BlobServiceClient mockServiceClient;
  private BlockBlobClient mockBlockBlobClient;
  private BlobBatchClient mockBlobBatchClient;
  private CosmosAsyncClient mockCosmosAsyncClient;
  private CosmosAsyncDatabase mockCosmosAsyncDatabase;
  private CosmosAsyncContainer mockCosmosAsyncContainer;
  private CosmosAsyncStoredProcedure cosmosAsyncStoredProcedure;
  private AzureMetrics azureMetrics;
  private AzureBlobDataAccessor azureBlobDataAccessor;
  private CosmosDataAccessor cosmosDataAccessor;

  @Before
  public void setup() throws Exception {
    mockServiceClient = mock(BlobServiceClient.class);
    mockBlockBlobClient = AzureBlobDataAccessorTest.setupMockBlobClient(mockServiceClient);
    mockBlobBatchClient = mock(BlobBatchClient.class);
    mockCosmosAsyncClient = mock(CosmosAsyncClient.class);
    mockCosmosAsyncDatabase = mock(CosmosAsyncDatabase.class);
    mockCosmosAsyncContainer = mock(CosmosAsyncContainer.class);
    azureMetrics = new AzureMetrics(new MetricRegistry());

    int lookbackDays =
        CloudConfig.DEFAULT_RETENTION_DAYS + numQueryBuckets * CloudConfig.DEFAULT_COMPACTION_QUERY_BUCKET_DAYS;
    configProps.setProperty(CloudConfig.CLOUD_COMPACTION_LOOKBACK_DAYS, String.valueOf(lookbackDays));
    AzureTestUtils.setConfigProperties(configProps);
    buildCompactor(configProps);
  }

  private void buildCompactor(Properties configProps) throws Exception {
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());
    String clusterName = "main";
    azureBlobDataAccessor = new AzureBlobDataAccessor(mockServiceClient, mockBlobBatchClient, clusterName, azureMetrics,
        new AzureCloudConfig(new VerifiableProperties(configProps)));
    String cosmosDatabaseName = "ambry";
    String containerForMetadata = "metadata";
    String containerForAmbryDeletedContainers = "deletedContainer";
    cosmosDataAccessor =
        new CosmosDataAccessor(mockCosmosAsyncClient, mockCosmosAsyncDatabase, mockCosmosAsyncContainer,
            cosmosDatabaseName, containerForMetadata, containerForAmbryDeletedContainers, vcrMetrics, azureMetrics);
    azureStorageCompactor =
        new AzureStorageCompactor(azureBlobDataAccessor, cosmosDataAccessor, cloudConfig, vcrMetrics, azureMetrics);

    // Mocks for getDeadBlobs query
    List<CloudBlobMetadata> docList = new ArrayList<>();
    for (int j = 0; j < numBlobsPerQuery; j++) {
      BlobId blobId = generateBlobId();
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, testTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      blobMetadataList.add(inputMetadata);
    }

    when(mockCosmosAsyncDatabase.getContainer(anyString())).thenReturn(mockCosmosAsyncContainer);

    FeedResponse feedResponse = mock(FeedResponse.class);
    when(feedResponse.getResults()).thenReturn(Collections.singletonList(docList));
    when(feedResponse.getRequestCharge()).thenReturn(10.0);
    CosmosPagedFlux cosmosPagedFlux = mock(CosmosPagedFlux.class);
    when(cosmosPagedFlux.byPage()).thenReturn(Flux.just(feedResponse));
    when(cosmosPagedFlux.byPage(anyInt())).thenReturn(Flux.just(feedResponse));
    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(cosmosPagedFlux);

    // Mocks for purge
    BlobBatch mockBatch = mock(BlobBatch.class);
    when(mockBlobBatchClient.getBlobBatch()).thenReturn(mockBatch);
    Response<Void> okResponse = mock(Response.class);
    when(okResponse.getStatusCode()).thenReturn(202);
    when(mockBatch.deleteBlob(anyString(), anyString())).thenReturn(okResponse);

    Document responseDoc = new Document();
    responseDoc.set(CosmosDataAccessor.PROPERTY_CONTINUATION, "false");
    responseDoc.set(CosmosDataAccessor.PROPERTY_DELETED, String.valueOf(1));
    CosmosStoredProcedureResponse cosmosStoredProcedureResponse = mock(CosmosStoredProcedureResponse.class);
    when(cosmosStoredProcedureResponse.getResponseAsString()).thenReturn(responseDoc.toString());
    when(cosmosStoredProcedureResponse.getRequestCharge()).thenReturn(1.0);
    cosmosAsyncStoredProcedure = mock(CosmosAsyncStoredProcedure.class);
    when(cosmosAsyncStoredProcedure.execute(any(), any())).thenReturn(Mono.just(cosmosStoredProcedureResponse));
    CosmosAsyncScripts cosmosAsyncScripts = mock(CosmosAsyncScripts.class);
    when(cosmosAsyncScripts.getStoredProcedure(anyString())).thenReturn(cosmosAsyncStoredProcedure);
    when(mockCosmosAsyncContainer.getScripts()).thenReturn(cosmosAsyncScripts);

    String checkpointJson = objectMapper.writeValueAsString(AzureStorageCompactor.emptyCheckpoints);
    mockCheckpointDownload(true, checkpointJson);
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
    int expectedNumQUeries = numQueryBuckets * 2;
    int expectedPurged = numBlobsPerQuery * expectedNumQUeries;
    assertEquals(expectedPurged, azureStorageCompactor.compactPartition(partitionPath));
    verify(mockCosmosAsyncContainer, times(expectedNumQUeries)).queryItems((SqlQuerySpec) any(), any(), any());
    verify(cosmosAsyncStoredProcedure, times(expectedNumQUeries)).execute(any(), any());
    verify(mockBlobBatchClient, times(expectedNumQUeries)).submitBatchWithResponse(any(BlobBatch.class), anyBoolean(),
        any(), any());
  }

  /** Test compaction on checkpoint not found. */
  @Test
  public void testCompactionProceedsOnCheckpointNotFound() throws Exception {
    mockCheckpointDownload(false, null);
    int expectedNumQUeries = numQueryBuckets * 2;
    int expectedPurged = numBlobsPerQuery * expectedNumQUeries;
    assertEquals(expectedPurged, azureStorageCompactor.compactPartition(partitionPath));
    verify(mockCosmosAsyncContainer, times(expectedNumQUeries)).queryItems((SqlQuerySpec) any(), any(), any());
  }

  /** Test compaction on error reading checkpoint. */
  @Test
  public void testCompactionFailsOnCheckpointReadError() throws Exception {
    BlobStorageException ex = mockStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(mockBlockBlobClient.downloadWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any())).thenThrow(
        ex);
    try {
      azureStorageCompactor.compactPartition(partitionPath);
      fail("Expected compaction to fail");
    } catch (CloudStorageException cse) {
      // expected
    }
  }

  /** Test compaction stops when purge limit reached. */
  @Test
  public void testCompactionStopsAfterPurgeLimit() throws Exception {
    int purgeLimit = numBlobsPerQuery * 2;
    configProps.setProperty(CloudConfig.CLOUD_COMPACTION_PURGE_LIMIT, String.valueOf(purgeLimit));
    buildCompactor(configProps);
    // Times 2 since it applies separately to deleted and expired blobs
    assertEquals(purgeLimit * 2, azureStorageCompactor.compactPartition(partitionPath));
  }

  /** Test getDeadBlobs method */
  @Test
  public void testGetDeadBlobs() throws Exception {
    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(
        getMockedPagedFluxForQueryWithNoResults());
    long now = System.currentTimeMillis();
    List<CloudBlobMetadata> metadataList =
        azureStorageCompactor.getDeadBlobs(partitionPath, CloudBlobMetadata.FIELD_DELETION_TIME, 1, now, 10);
    assertEquals("Expected no deleted blobs", 0, metadataList.size());
    assertEquals(1, azureMetrics.documentQueryCount.getCount());
    assertEquals(1, azureMetrics.deadBlobsQueryTime.getCount());

    metadataList =
        azureStorageCompactor.getDeadBlobs(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME, 1, now, 10);
    assertEquals("Expected no expired blobs", 0, metadataList.size());
    assertEquals(2, azureMetrics.documentQueryCount.getCount());
    assertEquals(2, azureMetrics.deadBlobsQueryTime.getCount());
  }

  /** Test purgeBlobs success */
  @Test
  public void testPurge() throws Exception {
    assertEquals("Expected success", numBlobsPerQuery,
        AzureCompactionUtil.purgeBlobs(blobMetadataList, azureBlobDataAccessor, azureMetrics, cosmosDataAccessor));
    assertEquals(numBlobsPerQuery, azureMetrics.blobDeletedCount.getCount());
    assertEquals(0, azureMetrics.blobDeleteErrorCount.getCount());
  }

  /** Test purgeBlobs with ABS error */
  @Test
  public void testPurgeWithStorageError() {
    // Unsuccessful case
    BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_ARCHIVED);
    BlobBatch mockBatch = mock(BlobBatch.class);
    Response<Void> mockResponse = mock(Response.class);
    when(mockResponse.getStatusCode()).thenThrow(ex);
    when(mockBlobBatchClient.getBlobBatch()).thenReturn(mockBatch);
    when(mockBatch.deleteBlob(anyString(), anyString())).thenReturn(mockResponse);
    try {
      AzureCompactionUtil.purgeBlobs(blobMetadataList, azureBlobDataAccessor, azureMetrics, cosmosDataAccessor);
      fail("Expected CloudStorageException");
    } catch (CloudStorageException bex) {
    }
    assertEquals(0, azureMetrics.blobDeletedCount.getCount());
    assertEquals(numBlobsPerQuery, azureMetrics.blobDeleteErrorCount.getCount());
  }

  /** Test purgeBlobs with Cosmos bulk delete error */
  @Test
  public void testPurgeWithCosmosBulkDeleteError() {
    CosmosException mockCosmosException = mock(CosmosException.class);
    when(mockCosmosException.getStatusCode()).thenReturn(HttpConstants.StatusCodes.TOO_MANY_REQUESTS);
    when(cosmosAsyncStoredProcedure.execute(any(), any())).thenReturn(Mono.error(mockCosmosException));
    try {
      AzureCompactionUtil.purgeBlobs(blobMetadataList, azureBlobDataAccessor, azureMetrics, cosmosDataAccessor);
      fail("Expected CloudStorageException");
    } catch (CloudStorageException bex) {
    }
    assertEquals(0, azureMetrics.blobDeletedCount.getCount());
    assertEquals(numBlobsPerQuery, azureMetrics.blobDeleteErrorCount.getCount());
  }

  /** Test compaction progress methods, normal cases */
  @Test
  public void testCheckpoints() throws Exception {
    // Existing checkpoint
    Map<String, Long> realCheckpoints = new HashMap<>();
    long now = System.currentTimeMillis();
    realCheckpoints.put(CloudBlobMetadata.FIELD_DELETION_TIME, now - TimeUnit.DAYS.toMillis(1));
    realCheckpoints.put(CloudBlobMetadata.FIELD_EXPIRATION_TIME, now - TimeUnit.DAYS.toMillis(2));
    mockCheckpointDownload(true, objectMapper.writeValueAsString(realCheckpoints));
    Map<String, Long> checkpoints = azureStorageCompactor.getCompactionProgress(partitionPath);
    assertEquals("Expected checkpoint to match", realCheckpoints, checkpoints);
    // Successful update
    assertTrue("Expected update to return true",
        azureStorageCompactor.updateCompactionProgress(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME, now));
    // Update skipped due to earlier time
    assertFalse("Expected update to return false",
        azureStorageCompactor.updateCompactionProgress(partitionPath, CloudBlobMetadata.FIELD_EXPIRATION_TIME,
            now - TimeUnit.DAYS.toMillis(3)));

    // No checkpoint
    mockCheckpointDownload(false, null);
    checkpoints = azureStorageCompactor.getCompactionProgress(partitionPath);
    assertEquals("Expected empty checkpoint", AzureStorageCompactor.emptyCheckpoints, checkpoints);
  }

  /** Test compaction progress methods, error cases */
  @Test
  public void testCheckpointErrors() throws Exception {
    // Corrupted checkpoint
    mockCheckpointDownload(true, "You can't do this!");
    Map<String, Long> checkpoints = azureStorageCompactor.getCompactionProgress(partitionPath);
    assertEquals(AzureStorageCompactor.emptyCheckpoints, checkpoints);
    assertEquals(1, azureMetrics.compactionProgressReadErrorCount.getCount());

    // Upload error
    mockCheckpointDownload(false, null);
    BlobStorageException ex = mockStorageException(BlobErrorCode.CONTAINER_DISABLED);
    when(mockBlockBlobClient.uploadWithResponse(any(), anyLong(), any(), any(), any(), any(), any(), any(),
        any())).thenThrow(ex);
    long now = System.currentTimeMillis();
    azureStorageCompactor.updateCompactionProgress(partitionPath, CloudBlobMetadata.FIELD_DELETION_TIME, now);
    assertEquals(1, azureMetrics.compactionProgressWriteErrorCount.getCount());
  }

  /** Test compaction checkpoint behavior */
  @Test
  public void testCompactionCheckpoints() throws Exception {
    AzureStorageCompactor compactorSpy = spy(azureStorageCompactor);
    doReturn(true).when(compactorSpy).updateCompactionProgress(anyString(), anyString(), anyLong());
    String fieldName = CloudBlobMetadata.FIELD_DELETION_TIME;
    long startTime = testTime - TimeUnit.DAYS.toMillis(numBlobsPerQuery);
    long endTime = testTime;

    // When dead blobs query returns results, progress gets updated to last record's dead time
    List<CloudBlobMetadata> docList = new ArrayList<>();
    long lastDeadTime = 0;
    for (int j = 0; j < numBlobsPerQuery; j++) {
      BlobId blobId = generateBlobId();
      CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, testTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      lastDeadTime = startTime + TimeUnit.HOURS.toMillis(j);
      inputMetadata.setDeletionTime(lastDeadTime);
      blobMetadataList.add(inputMetadata);
      docList.add(inputMetadata);
    }

    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(
        getMockPagedFluxForQueryOrChangeFeed(docList));
    compactorSpy.compactPartition(partitionPath, fieldName, startTime, endTime);
    verify(compactorSpy, atLeastOnce()).updateCompactionProgress(eq(partitionPath), eq(fieldName), eq(lastDeadTime));
    verify(compactorSpy, never()).updateCompactionProgress(eq(partitionPath), eq(fieldName), eq(endTime));

    // When dead blobs query returns no results, progress gets updated to queryEndtime
    when(mockCosmosAsyncContainer.queryItems((SqlQuerySpec) any(), any(), any())).thenReturn(
        getMockedPagedFluxForQueryWithNoResults());
    compactorSpy.compactPartition(partitionPath, fieldName, startTime, endTime);
    verify(compactorSpy).updateCompactionProgress(eq(partitionPath), eq(fieldName), eq(endTime));
  }

  private void mockCheckpointDownload(boolean exists, String checkpointValue) throws IOException {
    when(mockBlockBlobClient.exists()).thenReturn(exists);
    if (exists) {
      BlobDownloadResponse mockResponse = mock(BlobDownloadResponse.class);
      when(mockBlockBlobClient.downloadWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any())).thenAnswer(
          invocation -> {
            OutputStream outputStream = invocation.getArgument(0);
            if (outputStream != null) {
              outputStream.write(checkpointValue.getBytes());
            }
            return mockResponse;
          });
    } else {
      BlobStorageException ex = mockStorageException(BlobErrorCode.BLOB_NOT_FOUND);
      doThrow(ex).when(mockBlockBlobClient)
          .downloadWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any());
    }
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
