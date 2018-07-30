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
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.File;
import java.io.FileInputStream;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


public class AzureCloudDestinationTest {

  String connectionString = System.getProperty("azure.connection.string");
  String blobFilePath = System.getProperty("blob.file.path");
  String containerName = System.getProperty("azure.container.name");

  public void initMocks() throws Exception {
    // TODO: stupid account class is final and can't be mocked
    CloudStorageAccount mockAzureAccount = mock(CloudStorageAccount.class);
    CloudBlobClient mockAzureClient = mock(CloudBlobClient.class);
    CloudBlobContainer mockAzureContainer = mock(CloudBlobContainer.class);
    CloudBlockBlob mockBlob = mock(CloudBlockBlob.class);
    when(mockAzureAccount.createCloudBlobClient()).thenReturn(mockAzureClient);
    when(mockAzureClient.getContainerReference(anyString())).thenReturn(mockAzureContainer);
    when(mockAzureContainer.createIfNotExists()).thenReturn(true);
    when(mockAzureContainer.createIfNotExists(any(), any(), any())).thenReturn(true);
    when(mockAzureContainer.getBlockBlobReference(anyString())).thenReturn(mockBlob);
    Mockito.doNothing().when(mockBlob).upload(any(), anyLong(), any(), any(), any());
    when(mockBlob.deleteIfExists()).thenReturn(true);
  }

  @Test
  @Ignore
  // Integration test, needs live Azure account
  public void testPublishDataToAzure() throws Exception {
    CloudDestinationFactory factory = new AmbryCloudDestinationFactory(null);
    CloudReplicationConfig config =
        new CloudReplicationConfig(CloudDestinationType.AZURE.name(), connectionString, containerName);
    CloudDestination dest = factory.getCloudDestination(config);

    // (AzureCloudDestination)dest).setAzureAccount(mockAzureAccount);

    // TODO: test cases
    // Successful upload
    // Successful delete
    // getContainerReference throws exceptions, upload fails
    // getBlockBlobReference throws exceptions, upload fails
    // upload throws exceptions, upload fails
    // blob exists returns true, upload returns false

    File inputFile = new File(blobFilePath);
    if (!inputFile.canRead()) {
      throw new RuntimeException("Can't read input file: " + blobFilePath);
    }
    long blobSize = inputFile.length();
    String blobId = inputFile.getName();
    FileInputStream inputStream = new FileInputStream(blobFilePath);
    boolean success = dest.uploadBlob(blobId, blobSize, inputStream);
    System.out.println("Result of uploadBlob is " + success);
  }
}
