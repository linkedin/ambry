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

<<<<<<< Updated upstream
=======
import com.github.ambry.cloud.CloudBlobMetadata;
>>>>>>> Stashed changes
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
<<<<<<< Updated upstream
import com.github.ambry.utils.TestUtils;
=======
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
>>>>>>> Stashed changes
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.InvalidKeyException;
<<<<<<< Updated upstream
import org.junit.Before;
import org.junit.Ignore;
=======
import java.util.HashMap;
import java.util.Properties;
import org.junit.Before;
>>>>>>> Stashed changes
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

  private String configSpec = "AccountName=ambry;AccountKey=ambry-kay";
  private AzureCloudDestination azureDest;
  private CloudStorageAccount mockAzureAccount;
  private CloudBlobClient mockAzureClient;
  private CloudBlobContainer mockAzureContainer;
  private CloudBlockBlob mockBlob;
<<<<<<< Updated upstream
  private int blobSize = 1024;
  private BlobId blobId;
=======
  private DocumentClient mockumentClient;
  private int blobSize = 1024;
  private BlobId blobId;
  private long creationTime = System.currentTimeMillis();
  private long deletionTime = creationTime + 10000;
  private long expirationTime = Utils.Infinite_Time;
>>>>>>> Stashed changes

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
<<<<<<< Updated upstream
    Mockito.doNothing().when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    Mockito.doNothing().when(mockBlob).delete();
=======
    when(mockBlob.getMetadata()).thenReturn(new HashMap<>());
    Mockito.doNothing().when(mockBlob).upload(any(), anyLong(), any(), any(), any());

    mockumentClient = mock(DocumentClient.class);
    ResourceResponse<Document> mockResponse = mock(ResourceResponse.class);
    Document metadataDoc = new Document();
    when(mockResponse.getResource()).thenReturn(metadataDoc);
    when(mockumentClient.readDocument(anyString(), any(RequestOptions.class))).thenReturn(mockResponse);
>>>>>>> Stashed changes

    byte dataCenterId = 66;
    short accountId = 101;
    short containerId = 5;
    PartitionId partitionId = new MockPartitionId();
    blobId = new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, false,
        BlobDataType.DATACHUNK);

<<<<<<< Updated upstream
    azureDest = new AzureCloudDestination(mockAzureAccount);
=======
    azureDest = new AzureCloudDestination(mockAzureAccount, mockumentClient, "foo");
>>>>>>> Stashed changes
  }

  /**
   * Test normal upload.
   * @throws Exception
   */
  @Test
  public void testUpload() throws Exception {
    InputStream inputStream = getBlobInputStream(blobSize);
<<<<<<< Updated upstream
    assertTrue("Expected upload to return true", azureDest.uploadBlob(blobId, blobSize, inputStream));

=======
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, blobSize);
    assertTrue("Expected success", azureDest.uploadBlob(blobId, blobSize, metadata, inputStream));
>>>>>>> Stashed changes
  }

  /** Test normal delete. */
  @Test
  public void testDelete() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
<<<<<<< Updated upstream
    assertTrue("Expected deletion to return true", azureDest.deleteBlob(blobId));
=======
    assertTrue("Expected success", azureDest.deleteBlob(blobId, deletionTime));
  }

  /** Test normal expiration. */
  @Test
  public void testExpire() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    assertTrue("Expected success", azureDest.updateBlobExpiration(blobId, expirationTime));
>>>>>>> Stashed changes
  }

  /** Test upload of existing blob. */
  @Test
  public void testUploadExists() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    InputStream inputStream = getBlobInputStream(blobSize);
<<<<<<< Updated upstream
    assertFalse("Upload on existing blob should return false", azureDest.uploadBlob(blobId, blobSize, inputStream));
=======
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, blobSize);
    assertFalse("Upload on existing blob should return false",
        azureDest.uploadBlob(blobId, blobSize, metadata, inputStream));
>>>>>>> Stashed changes
  }

  /** Test delete of nonexistent blob. */
  @Test
  public void testDeleteNotExists() throws Exception {
    when(mockBlob.exists()).thenReturn(false);
<<<<<<< Updated upstream
    assertFalse("Delete of nonexistent blob should return false", azureDest.deleteBlob(blobId));
=======
    assertFalse("Delete of nonexistent blob should return false", azureDest.deleteBlob(blobId, deletionTime));
  }

  /** Test update of nonexistent blob. */
  @Test
  public void testUpdateNotExists() throws Exception {
    when(mockBlob.exists()).thenReturn(false);
    assertFalse("Update of nonexistent blob should return false", azureDest.updateBlobExpiration(blobId, expirationTime));
>>>>>>> Stashed changes
  }

  /** Test blob existence check. */
  @Test
  public void testExistenceCheck() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    assertTrue("Expected doesBlobExist to return true", azureDest.doesBlobExist(blobId));

<<<<<<< Updated upstream

=======
>>>>>>> Stashed changes
    when(mockBlob.exists()).thenReturn(false);
    assertFalse("Expected doesBlobExist to return false", azureDest.doesBlobExist(blobId));
  }

  /** Test constructor with invalid config spec. */
  @Test(expected = InvalidKeyException.class)
  public void testInitClientException() throws Exception {
<<<<<<< Updated upstream
    azureDest = new AzureCloudDestination(configSpec);
=======
    Properties props = new Properties();
    props.setProperty(AzureCloudDestination.STORAGE_CONFIG_SPEC, configSpec);
    azureDest = new AzureCloudDestination(new VerifiableProperties(props));
>>>>>>> Stashed changes
  }

  /** Test upload when client throws exception. */
  @Test
  public void testUploadContainerReferenceException() throws Exception {
    when(mockAzureClient.getContainerReference(anyString())).thenThrow(StorageException.class);
    InputStream inputStream = getBlobInputStream(blobSize);
<<<<<<< Updated upstream
    expectCloudStorageException(() -> azureDest.uploadBlob(blobId, blobSize, inputStream), StorageException.class);
=======
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, blobSize);
    expectCloudStorageException(() -> azureDest.uploadBlob(blobId, blobSize, metadata, inputStream),
        StorageException.class);
>>>>>>> Stashed changes
  }

  /** Test upload when container throws exception. */
  @Test
  public void testUploadContainerException() throws Exception {
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenThrow(StorageException.class);
    InputStream inputStream = getBlobInputStream(blobSize);
<<<<<<< Updated upstream
    expectCloudStorageException(() -> azureDest.uploadBlob(blobId, blobSize, inputStream), StorageException.class);
=======
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, blobSize);
    expectCloudStorageException(() -> azureDest.uploadBlob(blobId, blobSize, metadata, inputStream),
        StorageException.class);
>>>>>>> Stashed changes
  }

  /** Test upload when blob throws exception. */
  @Test
  public void testUploadBlobException() throws Exception {
    Mockito.doThrow(StorageException.class).when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    InputStream inputStream = getBlobInputStream(blobSize);
<<<<<<< Updated upstream
    expectCloudStorageException(() -> azureDest.uploadBlob(blobId, blobSize, inputStream), StorageException.class);
  }

  /** Test delete when blob throws exception. */
  @Test
  public void testDeleteBlobException() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    Mockito.doThrow(StorageException.class).when(mockBlob).delete();
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId), StorageException.class);
=======
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, blobSize);
    expectCloudStorageException(() -> azureDest.uploadBlob(blobId, blobSize, metadata, inputStream),
        StorageException.class);
  }

  /** Test upload when doc client throws exception. */
  @Test
  public void testUploadDocClientException() throws Exception {
    when(mockumentClient.createDocument(anyString(), any(), any(RequestOptions.class), anyBoolean())).thenThrow(
        DocumentClientException.class);
    InputStream inputStream = getBlobInputStream(blobSize);
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, blobSize);
    expectCloudStorageException(() -> azureDest.uploadBlob(blobId, blobSize, metadata, inputStream),
        DocumentClientException.class);
  }

  /** Test update methods when doc client throws exception. */
  @Test
  public void testUpdateDocClientException() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    when(mockumentClient.readDocument(anyString(), any())).thenThrow(DocumentClientException.class);
    expectCloudStorageException(() -> azureDest.deleteBlob(blobId, deletionTime), DocumentClientException.class);
    expectCloudStorageException(() -> azureDest.updateBlobExpiration(blobId, expirationTime), DocumentClientException.class);
>>>>>>> Stashed changes
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
