/**
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.account.CloudReplicationConfig;
import com.github.ambry.utils.TestUtils;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class AzureCloudDestinationTest {

  private String configSpec = "AccountName=ambry;AccountKey=ambry-kay";
  private String containerName = "ambrytest";
  private AzureCloudDestination azureDest;
  private CloudReplicationConfig azureConfig;
  private CloudStorageAccount mockAzureAccount;
  private CloudBlobClient mockAzureClient;
  private CloudBlobContainer mockAzureContainer;
  private CloudBlockBlob mockBlob;
  private int blobSize = 1024;
  private String blobId = "A123G789cafebabe";

  @Before
  public void setup() throws Exception {
    mockAzureAccount = mock(CloudStorageAccount.class);
    mockAzureClient = mock(CloudBlobClient.class);
    mockAzureContainer = mock(CloudBlobContainer.class);
    mockBlob = mock(CloudBlockBlob.class);
    when(mockAzureAccount.createCloudBlobClient()).thenReturn(mockAzureClient);
    when(mockAzureClient.getContainerReference(anyString())).thenReturn(mockAzureContainer);
    when(mockAzureContainer.createIfNotExists()).thenReturn(true);
    when(mockAzureContainer.createIfNotExists(any(), any(), any())).thenReturn(true);
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenReturn(mockBlob);
    when(mockBlob.exists()).thenReturn(false);
    Mockito.doNothing().when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    Mockito.doNothing().when(mockBlob).delete();

    azureConfig = new CloudReplicationConfig(CloudDestinationType.AZURE.name(), configSpec, containerName);
    azureDest = new AzureCloudDestination();
    azureDest.setAzureAccount(mockAzureAccount);
    azureDest.initialize(azureConfig);
  }

  @Test
  public void testNormal() throws Exception {
    InputStream inputStream = getBlobInputStream(blobSize);
    boolean success = azureDest.uploadBlob(blobId, blobSize, inputStream);
    assertTrue(success);

    when(mockBlob.exists()).thenReturn(true);
    success = azureDest.deleteBlob(blobId);
    assertTrue(success);
  }

  @Test
  public void testUploadExists() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    InputStream inputStream = getBlobInputStream(blobSize);
    boolean success = azureDest.uploadBlob(blobId, blobSize, inputStream);
    assertFalse(success);
  }

  @Test
  public void testDeleteNotExists() throws Exception {
    when(mockBlob.exists()).thenReturn(false);
    InputStream inputStream = getBlobInputStream(blobSize);
    boolean success = azureDest.deleteBlob(blobId);
    assertFalse(success);
  }

  @Test(expected = StorageException.class)
  public void testInitClientException() throws Exception {
    when(mockAzureClient.getContainerReference(anyString())).thenThrow(StorageException.class);
    azureDest = new AzureCloudDestination();
    azureDest.setAzureAccount(mockAzureAccount);
    azureDest.initialize(azureConfig);
  }

  @Test(expected = InstantiationException.class)
  public void testInitBadConnection() throws Exception {
    CloudDestinationFactory factory = new AmbryCloudDestinationFactory(null);
    azureDest = (AzureCloudDestination) factory.getCloudDestination(azureConfig);
  }

  @Test(expected = StorageException.class)
  public void testUploadContainerException() throws Exception {
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenThrow(StorageException.class);
    InputStream inputStream = getBlobInputStream(blobSize);
    azureDest.uploadBlob(blobId, blobSize, inputStream);
  }

  @Test(expected = StorageException.class)
  public void testUploadBlobException() throws Exception {
    Mockito.doThrow(StorageException.class).when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    InputStream inputStream = getBlobInputStream(blobSize);
    azureDest.uploadBlob(blobId, blobSize, inputStream);
  }

  @Test(expected = StorageException.class)
  public void testDeleteBlobException() throws Exception {
    when(mockBlob.exists()).thenReturn(true);
    Mockito.doThrow(StorageException.class).when(mockBlob).delete();
    azureDest.deleteBlob(blobId);
  }

  private static InputStream getBlobInputStream(int blobSize) {
    byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
    return new ByteArrayInputStream(randomBytes);
  }
}