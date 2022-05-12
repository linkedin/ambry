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

import com.azure.core.credential.AccessToken;
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.DummyCloudUpdateValidator;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/** Test cases for {@link AzureCloudDestination} */
@RunWith(Parameterized.class)
public class AzureBlobDataAccessorTest {

  private final String clusterName = "main";
  private Properties configProps = new Properties();
  private AzureBlobDataAccessor dataAccessor;
  private BlockBlobAsyncClient mockBlockBlobAsyncClient;
  private BlobBatchAsyncClient mockBatchAsyncClient;
  private BlobServiceAsyncClient mockServiceAsyncClient;
  private AzureMetrics azureMetrics;
  private DummyCloudUpdateValidator dummyCloudUpdateValidator = new DummyCloudUpdateValidator();
  private int blobSize = 1024;
  private BlobId blobId;
  private long creationTime = System.currentTimeMillis();
  private long deletionTime = creationTime + 10000;
  private long expirationTime = Utils.Infinite_Time;
  private final Integer numStorageAccountParameter;

  public AzureBlobDataAccessorTest(final Integer numStorageAccountParameter) {
    this.numStorageAccountParameter = numStorageAccountParameter;
  }

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);

    mockServiceAsyncClient = mock(BlobServiceAsyncClient.class);
    mockBlockBlobAsyncClient = setupMockBlobAsyncClient(mockServiceAsyncClient);
    mockBatchAsyncClient = mock(BlobBatchAsyncClient.class);

    mockBlobExistence(false);
    when(mockBlockBlobAsyncClient.uploadWithResponse(any(), anyLong(), any(), any(), any(), any(), any())).thenReturn(
        Mono.just(mock(Response.class)));
    BlobDownloadAsyncResponse response = mock(BlobDownloadAsyncResponse.class);
    byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
    when(response.getValue()).thenReturn(Flux.just(ByteBuffer.wrap(randomBytes)));
    when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(
        Mono.just(response));
    Response<BlobProperties> blobPropertiesResponse = mock(Response.class);
    when(blobPropertiesResponse.getValue()).thenReturn(mock(BlobProperties.class));
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.just(blobPropertiesResponse));
    when(mockBlockBlobAsyncClient.setMetadataWithResponse(any(), any())).thenReturn(Mono.just(mock(Response.class)));

    blobId = AzureTestUtils.generateBlobId();
    AzureTestUtils.setConfigProperties(configProps, numStorageAccountParameter);
    azureMetrics = new AzureMetrics(new MetricRegistry());
    dataAccessor = new AzureBlobDataAccessor(mockServiceAsyncClient, mockBatchAsyncClient, clusterName, azureMetrics,
        new AzureCloudConfig(new VerifiableProperties(configProps)),
        new CloudConfig(new VerifiableProperties(configProps)));
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
    AzureCloudDestination.UpdateResponse updateResponse =
        dataAccessor.updateBlobMetadataAsync(blobId, Collections.singletonMap("deletionTime", deletionTime),
            dummyCloudUpdateValidator).join();
    assertTrue("Expected was updated", updateResponse.wasUpdated);
    assertNotNull("Expected metadata", updateResponse.metadata);
  }

  /** Test normal expiration. */
  @Test
  public void testExpire() throws Exception {
    AzureCloudDestination.UpdateResponse updateResponse =
        dataAccessor.updateBlobMetadataAsync(blobId, Collections.singletonMap("expirationTime", expirationTime),
            dummyCloudUpdateValidator).join();
    assertTrue("Expected was updated", updateResponse.wasUpdated);
    assertNotNull("Expected metadata", updateResponse.metadata);
  }

  /** Test purge */
  @Test
  public void testPurge() throws Exception {
    // purge 3 blobs, response status (202, 404, 503)
    String blobNameOkStatus = "andromeda";
    String blobNameNotFoundStatus = "sirius";
    String blobNameErrorStatus = "mutant";
    BlobBatch mockBatch = mock(BlobBatch.class);
    when(mockBatchAsyncClient.getBlobBatch()).thenReturn(mockBatch);
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

    Response<Void> response = mock(Response.class);
    when(mockBatchAsyncClient.submitBatchWithResponse(any(), anyBoolean())).thenReturn(Mono.just(response));

    // Purge first 2 and expect success
    List<CloudBlobMetadata> purgeResponseList = dataAccessor.purgeBlobsAsync(purgeList).join();
    assertEquals("Wrong response size", 2, purgeResponseList.size());
    assertEquals("Wrong blob name", blobNameOkStatus, purgeResponseList.get(0).getId());
    assertEquals("Wrong blob name", blobNameNotFoundStatus, purgeResponseList.get(1).getId());
    // Including last one should fail
    purgeList.add(new CloudBlobMetadata().setId(blobNameErrorStatus));
    try {
      dataAccessor.purgeBlobsAsync(purgeList).join();
      fail("Expected purge to fail");
    } catch (CompletionException ex) {
      Exception e = Utils.extractFutureExceptionCause(ex);
      assertTrue("Unexpected exception", e instanceof BlobStorageException);
      assertEquals("Unexpected status code", 503, ((BlobStorageException) e).getStatusCode());
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
    when(mockBlockBlobAsyncClient.uploadWithResponse(any(), anyLong(), any(), any(), any(), any(), any())).thenReturn(
        Mono.error(ex));

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
    when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(
        Mono.error(mock(BlobStorageException.class)));
    expectBlobStorageException(() -> downloadBlob(blobId));
  }

  /** Test update of nonexistent blob. */
  @Test
  public void testUpdateNotExists() throws Exception {
    BlobStorageException ex = mock(BlobStorageException.class);
    when(ex.getErrorCode()).thenReturn(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlockBlobAsyncClient.getPropertiesWithResponse(any())).thenReturn(Mono.error(ex));
    expectBlobStorageException(
        () -> dataAccessor.updateBlobMetadataAsync(blobId, Collections.singletonMap("expirationTime", expirationTime),
            eq(dummyCloudUpdateValidator)).join());
  }

  /** Test file deletion. */
  @Test
  public void testFileDelete() throws Exception {
    mockBlobExistence(true);
    when(mockBlockBlobAsyncClient.delete()).thenReturn(Mono.just("EMPTY").then());
    assertTrue("Expected delete to return true", dataAccessor.deleteFileAsync("containerName", "fileName").join());
    mockBlobExistence(false);
    assertFalse("Expected delete to return false", dataAccessor.deleteFileAsync("containerName", "fileName").join());
  }

  @Test
  public void testStorageClientFactoriesConfigValidation() throws Exception {
    Properties properties = new Properties();
    if (numStorageAccountParameter == 0) {
      AzureTestUtils.setConfigProperties(properties, 0);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CLIENT_CLASS, ConnectionStringBasedStorageClient.class.getCanonicalName());
      properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, "");
      try {
        AzureBlobDataAccessor azureBlobDataAccessor =
            new AzureBlobDataAccessor(new CloudConfig(verifiableProperties), new AzureCloudConfig(verifiableProperties),
                new AzureBlobLayoutStrategy("test"), azureMetrics);
        fail("Creating azure blob data accessor with ConnectionStringBasedStorageClientFactory should throw exception" + "without connection string config");
      } catch (ReflectiveOperationException roEx) {
      }

      AzureTestUtils.setConfigProperties(properties, 0);
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

    if (numStorageAccountParameter == 1) {
      AzureTestUtils.setConfigProperties(properties, 1);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CLIENT_CLASS, ConnectionStringBasedStorageClient.class.getCanonicalName());
      properties.setProperty(AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO,
          "{\n" + "    \"storageAccountInfo\":[\n" + "      {\n" + "        \"name\":\"ambry\",\n" + "        \"partitionRange\":\"0-1000000\",\n"
              + "        \"storageConnectionString\":\"\",\n" + "        \"storageEndpoint\":\"https://azure_storage.blob.core.windows.net\",\n" + "      }\n"
              + "    ]\n" + "    }");
      try {
        AzureBlobDataAccessor azureBlobDataAccessor =
            new AzureBlobDataAccessor(new CloudConfig(verifiableProperties), new AzureCloudConfig(verifiableProperties),
                new AzureBlobLayoutStrategy("test"), azureMetrics);
        fail("Creating azure blob data accessor with ConnectionStringBasedStorageClientFactory should throw exception" + "without connection string config");
      } catch (ReflectiveOperationException roEx) {
      }
    }
  }

  @Test
  public void testRetryIfAuthTokenExpired() throws Exception {
    // verify that BlockBlobAsyncClient.downloadWithResponse is called 2 times when throwing exception but tryHandleExceptionAndHintRetry returns true.
    AzureCloudConfig azureConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureBlobLayoutStrategy blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureConfig);
    AccessToken accessToken = mock(AccessToken.class);
    when(accessToken.isExpired()).thenReturn(false);
    ADAuthBasedStorageClient adAuthBasedStorageClient =
        spy(new ADAuthBasedStorageClient(mockServiceAsyncClient, mockBatchAsyncClient, azureMetrics, blobLayoutStrategy,
            new AzureCloudConfig(new VerifiableProperties(configProps)), accessToken, null));
    BlobStorageException mockBlobStorageException = mock(BlobStorageException.class);
    when(mockBlobStorageException.getStatusCode()).thenReturn(HttpStatus.SC_FORBIDDEN);
    BlobDownloadAsyncResponse response = mock(BlobDownloadAsyncResponse.class);
    byte[] randomBytes = TestUtils.getRandomBytes(blobSize);
    when(response.getValue()).thenReturn(Flux.just(ByteBuffer.wrap(randomBytes)));

    when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(
        Mono.error(mockBlobStorageException), Mono.just(response));

    adAuthBasedStorageClient.downloadWithResponse("containerName", "blobName", null, false,
        new ByteArrayOutputStream(blobSize), null, null, null, false).join();
    verify(mockBlockBlobAsyncClient, times(2)).downloadWithResponse(any(), any(), any(), anyBoolean());

    // verify that BlockBlobAsyncClient.downloadWithResponse is called only once when not throwing any exception.
    when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(
        Mono.just(response));
    adAuthBasedStorageClient.downloadWithResponse("containerName", "blobName", null, false,
        new ByteArrayOutputStream(blobSize), null, null, null, false).join();
    verify(mockBlockBlobAsyncClient, times(3)).downloadWithResponse(any(), any(), any(), anyBoolean());

    // verify that BlockBlobAsyncClient.downloadWithResponse is called only once when throwing exception but tryHandleExceptionAndHintRetry returns false.
    when(mockBlobStorageException.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(mockBlockBlobAsyncClient.downloadWithResponse(any(), any(), any(), anyBoolean())).thenReturn(
        Mono.error(mockBlobStorageException)).thenReturn(Mono.just(response));
    expectBlobStorageException(
        () -> adAuthBasedStorageClient.downloadWithResponse("containerName", "blobName", null, false,
            new ByteArrayOutputStream(blobSize), null, null, null, false).join());
    verify(mockBlockBlobAsyncClient, times(4)).downloadWithResponse(any(), any(), any(), anyBoolean());
  }

  static BlockBlobAsyncClient setupMockBlobAsyncClient(BlobServiceAsyncClient mockServiceAsyncClient) {
    BlobContainerAsyncClient mockContainerAsyncClient = mock(BlobContainerAsyncClient.class);
    BlobAsyncClient mockBlobAsyncClient = mock(BlobAsyncClient.class);
    BlockBlobAsyncClient mockBlockBlobAsyncClient = mock(BlockBlobAsyncClient.class);
    when(mockServiceAsyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(mockContainerAsyncClient);
    when(mockContainerAsyncClient.getBlobAsyncClient(anyString())).thenReturn(mockBlobAsyncClient);
    when(mockContainerAsyncClient.exists()).thenReturn(Mono.just(false));
    when(mockContainerAsyncClient.create()).thenReturn(Mono.empty());
    when(mockBlobAsyncClient.getBlockBlobAsyncClient()).thenReturn(mockBlockBlobAsyncClient);
    return mockBlockBlobAsyncClient;
  }

  private void mockBlobExistence(boolean exists) {
    when(mockBlockBlobAsyncClient.exists()).thenReturn(Mono.just(exists));
  }

  /**
   * Upload a blob with default properties.
   * @return the result of the uploadBlob call.
   */
  private boolean uploadDefaultBlob() throws Exception {
    InputStream inputStream = AzureTestUtils.getBlobInputStream(blobSize);
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, creationTime, Utils.Infinite_Time, blobSize,
        CloudBlobMetadata.EncryptionOrigin.NONE);
    return dataAccessor.uploadAsyncIfNotExists(blobId, blobSize, metadata, inputStream).join();
  }

  /**
   * Download a blob with the given blobId.
   * @param blobId blobid of the blob to be downloaded.
   */
  private void downloadBlob(BlobId blobId) throws Exception {
    dataAccessor.downloadBlobAsync(blobId, new ByteArrayOutputStream(blobSize)).join();
  }

  /**
   * Utility method to run some code and verify the expected exception was thrown.
   * @param runnable the code to run.
   */
  private void expectBlobStorageException(TestUtils.ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      fail("Expected BlobStorageException");
    } catch (Exception e) {
      e = Utils.extractFutureExceptionCause(e);
      if (e instanceof BlobStorageException) {

      } else {
        throw e;
      }
    }
  }

  /**
   * Parameters determine the number of storage accounts to use during the sharded storage account testing.
   * The value of zero refers to no storage account sharding.
   * @return The number of storage accounts to use.
   */
  @Parameterized.Parameters
  public static Collection<Integer[]> getParameters() {
    return Lists.newArrayList(new Integer[] { 0 }, new Integer[] { 1 });
  }
}
