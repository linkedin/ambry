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
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
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
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
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

  private final String configSpec = "AccountName=ambry;AccountKey=ambry-kay";
  private final ObjectMapper objectMapper = new ObjectMapper();
  private AzureCloudDestination azureDest;
  private CloudStorageAccount mockAzureAccount;
  private CloudBlobClient mockAzureClient;
  private CloudBlobContainer mockAzureContainer;
  private CloudBlockBlob mockBlob;
  private DocumentClient mockumentClient;
  private int blobSize = 1024;
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
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenReturn(mockBlob);
    when(mockBlob.exists()).thenReturn(false);
    when(mockBlob.getMetadata()).thenReturn(new HashMap<>());
    Mockito.doNothing().when(mockBlob).upload(any(), anyLong(), any(), any(), any());

    mockumentClient = mock(DocumentClient.class);
    ResourceResponse<Document> mockResponse = mock(ResourceResponse.class);
    Document metadataDoc = new Document();
    when(mockResponse.getResource()).thenReturn(metadataDoc);
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenReturn(mockResponse);

    byte dataCenterId = 66;
    short accountId = 101;
    short containerId = 5;
    PartitionId partitionId = new MockPartitionId();
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);

    azureDest = new AzureCloudDestination(mockAzureAccount, mockumentClient, "foo");
  }

  /**
   * Test normal upload.
   * @throws Exception
   */
  @Test
  public void testUpload() throws Exception {
    assertTrue("Expected success", uploadDefaultBlob());
  }

  /** Test normal delete. */
  @Test
  public void testDelete() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    assertTrue("Expected success", azureDest.deleteBlob(blobId, deletionTime));
  }

  /** Test normal expiration. */
  @Test
  public void testExpire() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    assertTrue("Expected success", azureDest.updateBlobExpiration(blobId, expirationTime));
  }

  /** Test upload of existing blob. */
  @Test
  public void testUploadExists() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    assertFalse("Upload of existing blob should return false", uploadDefaultBlob());
  }

  /** Test delete of nonexistent blob. */
  @Test
  public void testDeleteNotExists() throws Exception {
    when(mockBlob.exists()).thenReturn(false);
    assertFalse("Delete of nonexistent blob should return false", azureDest.deleteBlob(blobId, deletionTime));
  }

  /** Test update of nonexistent blob. */
  @Test
  public void testUpdateNotExists() throws Exception {
    when(mockBlob.exists()).thenReturn(false);
    assertFalse("Update of nonexistent blob should return false",
        azureDest.updateBlobExpiration(blobId, expirationTime));
  }

  /** Test querying metadata. */
  @Test
  public void testQueryMetadata() throws Exception {
    QueryIterable<Document> mockIterable = mock(QueryIterable.class);
    CloudBlobMetadata inputMetadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize);
    List<Document> docList = Collections.singletonList(new Document(objectMapper.writeValueAsString(inputMetadata)));
    when(mockIterable.iterator()).thenReturn(docList.iterator());
    FeedResponse<Document> feedResponse = mock(FeedResponse.class);
    when(feedResponse.getQueryIterable()).thenReturn(mockIterable);
    when(mockumentClient.queryDocuments(anyString(), anyString(), any(FeedOptions.class))).thenReturn(feedResponse);
    Map<String, CloudBlobMetadata> metadataMap = azureDest.getBlobMetadata(Collections.singletonList(blobId));
    assertEquals("Expected single entry", 1, metadataMap.size());
    CloudBlobMetadata outputMetadata = metadataMap.get(blobId.getID());
    assertEquals("Returned metadata does not match original", inputMetadata, outputMetadata);
  }

  /** Test blob existence check. */
  @Test
  public void testExistenceCheck() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    assertTrue("Expected doesBlobExist to return true", azureDest.doesBlobExist(blobId));

    when(mockBlob.exists()).thenReturn(false);
    assertFalse("Expected doesBlobExist to return false", azureDest.doesBlobExist(blobId));
  }

  /** Test constructor with invalid connection string. */
  @Test
  public void testInitClientException() throws Exception {
    Properties props = new Properties();
    props.setProperty(AzureCloudConfig.STORAGE_CONNECTION_STRING, configSpec);
    props.setProperty(AzureCloudConfig.COSMOS_ENDPOINT, "http://ambry.cosmos.com");
    props.setProperty(AzureCloudConfig.COSMOS_COLLECTION_LINK, "ambry/metadata");
    props.setProperty(AzureCloudConfig.COSMOS_KEY, "dummykey");
    CloudDestinationFactory factory = new AzureCloudDestinationFactory(new VerifiableProperties(props), new MetricRegistry());
    try {
      factory.getCloudDestination();
      fail("Expected exception to be thrown");
    } catch (IllegalStateException ex) {
      assertEquals("Unexpected exception", InvalidKeyException.class, ex.getCause().getClass());
    }
  }

  /** Test upload when client throws exception. */
  @Test
  public void testUploadContainerReferenceException() throws Exception {
    when(mockAzureClient.getContainerReference(anyString())).thenThrow(StorageException.class);
    expectCloudStorageException(() -> uploadDefaultBlob(), StorageException.class);
  }

  /** Test upload when container throws exception. */
  @Test
  public void testUploadContainerException() throws Exception {
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenThrow(StorageException.class);
    expectCloudStorageException(() -> uploadDefaultBlob(), StorageException.class);
  }

  /** Test upload when blob throws exception. */
  @Test
  public void testUploadBlobException() throws Exception {
    Mockito.doThrow(StorageException.class).when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    expectCloudStorageException(() -> uploadDefaultBlob(), StorageException.class);
  }

  /** Test upload when doc client throws exception. */
  @Test
  public void testUploadDocClientException() throws Exception {
    when(mockumentClient.createDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenThrow(
        DocumentClientException.class);
    expectCloudStorageException(() -> uploadDefaultBlob(), DocumentClientException.class);
  }

  /** Test update methods when doc client throws exception. */
  @Test
  public void testUpdateDocClientException() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    when(mockumentClient.readDocument(anyString(), any())).thenThrow(DocumentClientException.class);
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId, deletionTime), DocumentClientException.class);
    expectCloudStorageException(() -> azureDest.updateBlobExpiration(blobId, expirationTime),
        DocumentClientException.class);
  }

  /**
   * Upload a blob with default properties.
   * @return the result of the uploadBlob call.
   * @throws CloudStorageException on error.
   */
  private boolean uploadDefaultBlob() throws CloudStorageException {
    InputStream inputStream = getBlobInputStream(blobSize);
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize);
    return azureDest.uploadBlob(blobId, blobSize, metadata, inputStream);
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
   * Utility method to get blob input stream.
   * @param blobSize size of blob to consider.
   * @return the blob input stream.
   */
  private static InputStream getBlobInputStream(int blobSize) {
    byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
    return new ByteArrayInputStream(randomBytes);
  }
}
