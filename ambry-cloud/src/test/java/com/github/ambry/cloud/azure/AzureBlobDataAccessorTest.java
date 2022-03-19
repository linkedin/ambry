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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.DummyCloudUpdateValidator;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/** Test cases for {@link AzureCloudDestination} */
@RunWith(MockitoJUnitRunner.class)
public class AzureBlobDataAccessorTest {

  private final String clusterName = "main";
  private Properties configProps = new Properties();
  private AzureBlobDataAccessor dataAccessor;
  private BlockBlobClient mockBlockBlobClient;
  private BlobBatchClient mockBatchClient;
  private BlobServiceClient mockServiceClient;
  private AzureMetrics azureMetrics;
  private DummyCloudUpdateValidator dummyCloudUpdateValidator = new DummyCloudUpdateValidator();
  private int blobSize = 1024;
  private BlobId blobId;
  private long creationTime = System.currentTimeMillis();
  private long deletionTime = creationTime + 10000;
  private long expirationTime = Utils.Infinite_Time;

  @Before
  public void setup() throws Exception {

    mockServiceClient = mock(BlobServiceClient.class);
    mockBlockBlobClient = setupMockBlobClient(mockServiceClient);
    mockBatchClient = mock(BlobBatchClient.class);

    mockBlobExistence(false);

    blobId = AzureTestUtils.generateBlobId();
    AzureTestUtils.setConfigProperties(configProps);
    azureMetrics = new AzureMetrics(new MetricRegistry());
    dataAccessor = new AzureBlobDataAccessor(mockServiceClient, mockBatchClient, clusterName, azureMetrics,
        new AzureCloudConfig(new VerifiableProperties(configProps)));
  }

  /**
   * Test normal upload.
   * @throws Exception
   */
  @Test
  public void testUpload() throws Exception {
    assertTrue("Expected success", uploadDefaultBlob());
    assertEquals(1, azureMetrics.blobUploadRequestCount.getCount());
    assertEquals(1, azureMetrics.blobUploadSuccessCount.getCount());
    assertEquals(0, azureMetrics.blobUploadConflictCount.getCount());
    assertEquals(0, azureMetrics.backupErrorCount.getCount());
    assertEquals(1, azureMetrics.blobUploadTime.getCount());
  }

  /**
   * Test normal download.
   * @throws Exception
   */
  @Test
  public void testDownload() throws Exception {
    downloadBlob(blobId);
    assertEquals(1, azureMetrics.blobDownloadRequestCount.getCount());
    assertEquals(1, azureMetrics.blobDownloadSuccessCount.getCount());
    assertEquals(0, azureMetrics.blobDownloadErrorCount.getCount());
    assertEquals(1, azureMetrics.blobDownloadTime.getCount());
  }

  /** Test normal delete. */
  @Test
  public void testMarkDelete() throws Exception {
    mockBlobExistence(true);
    AzureCloudDestination.UpdateResponse response =
        dataAccessor.updateBlobMetadata(blobId, Collections.singletonMap("deletionTime", deletionTime),
            dummyCloudUpdateValidator);
    assertTrue("Expected was updated", response.wasUpdated);
    assertNotNull("Expected metadata", response.metadata);
  }

  /** Test normal expiration. */
  @Test
  public void testExpire() throws Exception {
    mockBlobExistence(true);
    AzureCloudDestination.UpdateResponse response =
        dataAccessor.updateBlobMetadata(blobId, Collections.singletonMap("expirationTime", expirationTime),
            dummyCloudUpdateValidator);
    assertTrue("Expected was updated", response.wasUpdated);
    assertNotNull("Expected metadata", response.metadata);
  }

  /** Test purge */
  @Test
  public void testPurge() throws Exception {
    // purge 3 blobs, response status (202, 404, 503)
    String blobNameOkStatus = "andromeda";
    String blobNameNotFoundStatus = "sirius";
    String blobNameErrorStatus = "mutant";
    BlobBatch mockBatch = mock(BlobBatch.class);
    when(mockBatchClient.getBlobBatch()).thenReturn(mockBatch);
    Response<Void> okResponse = mock(Response.class);
    when(okResponse.getStatusCode()).thenReturn(202);
    when(mockBatch.deleteBlob(anyString(), endsWith(blobNameOkStatus))).thenReturn(okResponse);
    BlobStorageException notFoundException = mock(BlobStorageException.class);
    when(notFoundException.getStatusCode()).thenReturn(404);
    Response<Void> notFoundResponse = mock(Response.class);
    when(notFoundResponse.getStatusCode()).thenThrow(notFoundException);
    when(mockBatch.deleteBlob(anyString(), endsWith(blobNameNotFoundStatus))).thenReturn(notFoundResponse);
    BlobStorageException badException = mock(BlobStorageException.class);
    when(badException.getStatusCode()).thenReturn(503);
    Response<Void> badResponse = mock(Response.class);
    when(badResponse.getStatusCode()).thenThrow(badException);
    when(mockBatch.deleteBlob(anyString(), endsWith(blobNameErrorStatus))).thenReturn(badResponse);
    List<CloudBlobMetadata> purgeList = new ArrayList<>();
    purgeList.add(new CloudBlobMetadata().setId(blobNameOkStatus));
    purgeList.add(new CloudBlobMetadata().setId(blobNameNotFoundStatus));
    // Purge first 2 and expect success
    List<CloudBlobMetadata> purgeResponseList = dataAccessor.purgeBlobs(purgeList);
    assertEquals("Wrong response size", 2, purgeResponseList.size());
    assertEquals("Wrong blob name", blobNameOkStatus, purgeResponseList.get(0).getId());
    assertEquals("Wrong blob name", blobNameNotFoundStatus, purgeResponseList.get(1).getId());
    // Including last one should fail
    purgeList.add(new CloudBlobMetadata().setId(blobNameErrorStatus));
    try {
      dataAccessor.purgeBlobs(purgeList);
      fail("Expected purge to fail");
    } catch (BlobStorageException bex) {
      assertEquals("Unexpected status code", 503, bex.getStatusCode());
    }
  }

  /** Test initializing with a proxy */
  @Test
  public void testProxy() throws Exception {

    // Test without proxy
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureBlobLayoutStrategy blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureConfig);
    AzureBlobDataAccessor dataAccessor =
        new AzureBlobDataAccessor(cloudConfig, azureConfig, blobLayoutStrategy, azureMetrics);

    // Test with proxy
    String proxyHost = "azure-proxy.randomcompany.com";
    int proxyPort = 80;
    configProps.setProperty(CloudConfig.VCR_PROXY_HOST, proxyHost);
    configProps.setProperty(CloudConfig.VCR_PROXY_PORT, String.valueOf(proxyPort));
    cloudConfig = new CloudConfig(new VerifiableProperties(configProps));
    dataAccessor = new AzureBlobDataAccessor(cloudConfig, azureConfig, blobLayoutStrategy, azureMetrics);
  }

  /** Test upload of existing blob. */
  @Test
  public void testUploadExists() throws Exception {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getErrorCode()).thenReturn(BlobErrorCode.BLOB_ALREADY_EXISTS);
    when(mockBlockBlobClient.uploadWithResponse(any(), anyLong(), any(), any(), any(), any(), any(), any(),
        any())).thenThrow(ex);

    assertFalse("Upload of existing blob should return false", uploadDefaultBlob());
    assertEquals(1, azureMetrics.blobUploadRequestCount.getCount());
    assertEquals(0, azureMetrics.blobUploadSuccessCount.getCount());
    assertEquals(1, azureMetrics.blobUploadConflictCount.getCount());
    assertEquals(0, azureMetrics.backupErrorCount.getCount());
  }

  /**
   * Test download of non existent blob
   * @throws Exception
   */
  @Test
  public void testDownloadNotExists() throws Exception {
    doThrow(BlobStorageException.class).when(mockBlockBlobClient)
        .downloadWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any());
    expectBlobStorageException(() -> downloadBlob(blobId));
  }

  /** Test update of nonexistent blob. */
  @Test
  public void testUpdateNotExists() throws Exception {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobClient.getPropertiesWithResponse(any(), any(), any())).thenThrow(ex);
    expectBlobStorageException(
        () -> dataAccessor.updateBlobMetadata(blobId, Collections.singletonMap("expirationTime", expirationTime),
            eq(dummyCloudUpdateValidator)));
  }

  /** Test file deletion. */
  @Test
  public void testFileDelete() throws Exception {
    mockBlobExistence(true);
    assertTrue("Expected delete to return true", dataAccessor.deleteFile("containerName", "fileName"));
    mockBlobExistence(false);
    assertFalse("Expected delete to return false", dataAccessor.deleteFile("containerName", "fileName"));
  }

  @Test
  public void testStorageClientFactoriesConfigValidation() throws Exception {
    Properties properties = new Properties();
    AzureTestUtils.setConfigProperties(properties);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CLIENT_CLASS,
        ConnectionStringBasedStorageClient.class.getCanonicalName());
    properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, "");
    try {
      AzureBlobDataAccessor azureBlobDataAccessor =
          new AzureBlobDataAccessor(new CloudConfig(verifiableProperties), new AzureCloudConfig(verifiableProperties),
              new AzureBlobLayoutStrategy("test"), azureMetrics);
      fail("Creating azure blob data accessor with ConnectionStringBasedStorageClientFactory should throw exception"
          + "without connection string config");
    } catch (ReflectiveOperationException roEx) {
    }

    AzureTestUtils.setConfigProperties(properties);
    properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CLIENT_CLASS,
        ADAuthBasedStorageClient.class.getCanonicalName());
    properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CLIENTID, "");
    verifiableProperties = new VerifiableProperties(properties);
    try {
      AzureBlobDataAccessor azureBlobDataAccessor =
          new AzureBlobDataAccessor(new CloudConfig(verifiableProperties), new AzureCloudConfig(verifiableProperties),
              new AzureBlobLayoutStrategy("test"), azureMetrics);
      fail("Creating azure blob data accessor with ADAuthBasedStorageClientFactory should throw exception"
          + "without one of the required configs");
    } catch (ReflectiveOperationException roEx) {
    }
  }

  @Test
  public void testRetryIfAuthTokenExpired() {
    // verify that BlockBlobClient.downloadWithResponse is called 2 times when throwing exception but tryHandleExceptionAndHintRetry returns true.
    AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureBlobLayoutStrategy blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureConfig);
    ADAuthBasedStorageClient adAuthBasedStorageClient =
        spy(new ADAuthBasedStorageClient(mockServiceClient, mockBatchClient, azureMetrics, blobLayoutStrategy,
            new AzureCloudConfig(new VerifiableProperties(configProps))));
    BlobStorageException mockBlobStorageException = mock(BlobStorageException.class);
    when(mockBlockBlobClient.downloadWithResponse(null, null, null, null, false, null, Context.NONE)).thenThrow(
        mockBlobStorageException).thenReturn(null);
    doReturn(true).when(adAuthBasedStorageClient).handleExceptionAndHintRetry(mockBlobStorageException);
    doReturn(mockBlockBlobClient).when(adAuthBasedStorageClient).getBlockBlobClient(null, null, false);
    adAuthBasedStorageClient.downloadWithResponse(null, null, false, null, null, null, null, false, null);
    verify(mockBlockBlobClient, times(2)).downloadWithResponse(null, null, null, null, false, null, Context.NONE);

    // verify that BlockBlobClient.downloadWithResponse is called only once when not throwing any exception.
    when(mockBlockBlobClient.downloadWithResponse(null, null, null, null, false, null, Context.NONE)).thenReturn(null);
    adAuthBasedStorageClient.downloadWithResponse(null, null, false, null, null, null, null, false, null);
    verify(mockBlockBlobClient, times(3)).downloadWithResponse(null, null, null, null, false, null, Context.NONE);

    // verify that BlockBlobClient.downloadWithResponse is called only once when throwing exception but tryHandleExceptionAndHintRetry returns false.
    BlobStorageException blobStorageException = new BlobStorageException("test failure", null, null);
    doReturn(false).when(adAuthBasedStorageClient).handleExceptionAndHintRetry(blobStorageException);
    when(mockBlockBlobClient.downloadWithResponse(null, null, null, null, false, null, Context.NONE)).thenThrow(
        blobStorageException).thenReturn(null);
    try {
      adAuthBasedStorageClient.downloadWithResponse(null, null, false, null, null, null, null, false, null);
    } catch (BlobStorageException bsEx) {
    }
    verify(mockBlockBlobClient, times(4)).downloadWithResponse(null, null, null, null, false, null, Context.NONE);
  }

  static BlockBlobClient setupMockBlobClient(BlobServiceClient mockServiceClient) {
    BlobContainerClient mockContainerClient = mock(BlobContainerClient.class);
    BlobClient mockBlobClient = mock(BlobClient.class);
    BlockBlobClient mockBlockBlobClient = mock(BlockBlobClient.class);
    when(mockServiceClient.getBlobContainerClient(anyString())).thenReturn(mockContainerClient);
    when(mockContainerClient.getBlobClient(anyString())).thenReturn(mockBlobClient);
    when(mockContainerClient.exists()).thenReturn(false);
    when(mockBlobClient.getBlockBlobClient()).thenReturn(mockBlockBlobClient);
    // Rest is to mock getPropertiesWithResponse and not needed everywhere
    BlobProperties mockBlobProperties = mock(BlobProperties.class);
    Map<String, String> metadataMap = new HashMap<>();
    lenient().when(mockBlobProperties.getMetadata()).thenReturn(metadataMap);
    Response<BlobProperties> mockPropertiesResponse = mock(Response.class);
    lenient().when(mockPropertiesResponse.getValue()).thenReturn(mockBlobProperties);
    lenient().when(mockBlockBlobClient.getPropertiesWithResponse(any(), any(), any()))
        .thenReturn(mockPropertiesResponse);
    return mockBlockBlobClient;
  }

  private void mockBlobExistence(boolean exists) {
    when(mockBlockBlobClient.exists()).thenReturn(exists);
  }

  /**
   * Upload a blob with default properties.
   * @return the result of the uploadBlob call.
   * @throws IOException on error.
   */
  private boolean uploadDefaultBlob() throws IOException {
    InputStream inputStream = AzureTestUtils.getBlobInputStream(blobSize);
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    return dataAccessor.uploadIfNotExists(blobId, blobSize, metadata, inputStream);
  }

  /**
   * Download a blob with the given blobId.
   * @param blobId blobid of the blob to be downloaded.
   */
  private void downloadBlob(BlobId blobId) throws IOException {
    dataAccessor.downloadBlob(blobId, new ByteArrayOutputStream(blobSize));
  }

  /**
   * Utility method to run some code and verify the expected exception was thrown.
   * @param runnable the code to run.
   */
  private void expectBlobStorageException(TestUtils.ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      fail("Expected BlobStorageException");
    } catch (BlobStorageException e) {
    }
  }
}
