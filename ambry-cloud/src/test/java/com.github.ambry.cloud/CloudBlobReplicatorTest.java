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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.CloudReplicationConfig;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.InputStreamReadableStreamChannel;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.CryptoService;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.KeyManagementService;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.SingleKeyManagementServiceFactory;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.storage.StorageException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.crypto.spec.SecretKeySpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class CloudBlobReplicatorTest {

  private Router router;
  private AccountService accountService;
  private InMemoryBlobEventSource eventSource;
  private CloudDestinationFactory destinationFactory;
  private CloudDestination destination;
  private CloudReplicationMetrics metrics;
  private CloudBlobReplicator replicator;
  private short accountId = 101;
  private short containerId = 201;
  private short containerIdNoRepl = 202;
  private String accountName = "ambryTeam";
  private String containerName = "ambrytest";
  private String serviceId = "TestService";
  private long defaultBlobSize = 1024l;

  // Only need these for intg test
  private boolean useRealCloudService = false;
  private String configSpec;

  @Before
  public void setup() throws Exception {

    useRealCloudService = Boolean.parseBoolean(System.getProperty("use.real.cloud.service"));
    if (useRealCloudService) {
      configSpec = System.getProperty("cloud.config.spec");
      destinationFactory = new AmbryCloudDestinationFactory(null);
    } else {
      configSpec = "AccountName=ambry;AccountKey=ambry-kay";
      destinationFactory = mock(CloudDestinationFactory.class);
      CloudDestination mockDestination = mock(CloudDestination.class);
      when(mockDestination.uploadBlob(anyString(), anyLong(), any())).thenReturn(true);
      when(mockDestination.deleteBlob(anyString())).thenReturn(true);
      CloudReplicationConfig config =
          new CloudReplicationConfig(CloudDestinationType.AZURE.name(), configSpec, containerName);
      when(destinationFactory.getCloudDestination(eq(config))).thenReturn(mockDestination);
      destination = mockDestination;
    }

    Properties properties = new Properties();
    // hex string that decodes to 16 bytes
    String hexKey = "32ab01de67cafe6732ab01de67cafe67";
    properties.setProperty("kms.default.container.key", hexKey);
    VerifiableProperties verProps = new VerifiableProperties(properties);
    ClusterMap clusterMap = new MockClusterMap();
    router = new InMemoryRouter(verProps, clusterMap);
    accountService = new InMemAccountService(false, true);

    //
    // Build an account with two containers, one configured to publish to the cloud and the other not.
    //

    //CryptoService cryptoService = new GCMCryptoServiceFactory(props, null).getCryptoService();
    CryptoService cryptoService = new DummyCryptoService();
    KeyManagementService kms = new SingleKeyManagementServiceFactory(verProps, null, null).getKeyManagementService();

    // Encrypt config spec
    Object keySpec = kms.getKey(accountId, containerId);
    ByteBuffer fuzzyBuf = cryptoService.encrypt(ByteBuffer.wrap(configSpec.getBytes()), keySpec);
    configSpec = new String(fuzzyBuf.array());

    CloudReplicationConfig containerCloudConfig =
        new CloudReplicationConfig(CloudDestinationType.AZURE.name(), configSpec, containerName);
    Container containerWithReplication =
        new ContainerBuilder(containerId, containerName, Container.ContainerStatus.ACTIVE, null,
            accountId).setCloudConfig(containerCloudConfig).build();
    Container containerWithoutReplication =
        new ContainerBuilder(containerIdNoRepl, "dontreplicateme", Container.ContainerStatus.ACTIVE, null,
            accountId).build();

    Account account = new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(
        ImmutableList.of(containerWithReplication, containerWithoutReplication)).build();
    accountService.updateAccounts(ImmutableList.of(account));
    eventSource = new InMemoryBlobEventSource();
    metrics = new CloudReplicationMetrics(new MetricRegistry());
    replicator = new CloudBlobReplicator().router(router)
        .accountService(accountService)
        .eventSource(eventSource)
        .destinationFactory(destinationFactory)
        .metrics(metrics)
        .cryptoService(cryptoService)
        .keyService(kms);
    replicator.startup();
  }

  @After
  public void teardown() {
    if (replicator != null) {
      replicator.shutdown();
    }
  }

  @Test
  public void testBlobReplication() throws Exception {

    String blobFileName = "profile-photo.jpg";
    URL blobUrl = this.getClass().getClassLoader().getResource(blobFileName);
    String blobFilePath = blobUrl.getPath();
    File inputFile = new File(blobFilePath);
    if (!inputFile.canRead()) {
      throw new RuntimeException("Can't load resource: " + blobFilePath);
    }
    long blobSize = inputFile.length();
    FileInputStream inputStream = new FileInputStream(blobFilePath);

    BlobProperties blobProps = new BlobProperties(blobSize, serviceId, accountId, containerId, false);
    String blobId = putBlob(blobProps, inputStream);
    assertNotNull(blobId);

    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.PUT));
    if (!useRealCloudService) {
      verify(destination).uploadBlob(eq(blobId), anyLong(), any());
    }
    assertTrue(eventSource.q.isEmpty());
    checkUploadMetrics(1, 1, 0);

    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.DELETE));
    if (!useRealCloudService) {
      verify(destination).deleteBlob(eq(blobId));
    }
    assertTrue(eventSource.q.isEmpty());
    checkDeleteMetrics(1, 1, 0);
  }

  @Test
  public void testBlobWithTTL() throws Exception {
    // Blob with TTL should not be uploaded
    BlobProperties blobProps = new BlobProperties(defaultBlobSize, serviceId, accountId, containerId, false);
    blobProps.setTimeToLiveInSeconds(3600l);
    String blobId = putRandomBlob(blobProps);
    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.PUT));
    assertTrue(eventSource.q.isEmpty());
    checkUploadMetrics(1, 0, 0);

    // UPDATE blob with no TTL should be uploaded
    router.updateBlobTtl(blobId, serviceId, Utils.Infinite_Time);
    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.UPDATE));
    checkUploadMetrics(2, 1, 0);
  }

  @Test
  public void testBlobWithNoReplication() throws Exception {
    // put a blob generated from random bytes
    BlobProperties blobProps = new BlobProperties(defaultBlobSize, serviceId, accountId, containerIdNoRepl, false);
    String blobId = putRandomBlob(blobProps);
    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.PUT));
    // There should be no request recorded
    checkUploadMetrics(0, 0, 0);

    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.DELETE));
    checkDeleteMetrics(0, 0, 0);
  }

  @Test
  public void testUploadExistingBlob() throws Exception {
    if (useRealCloudService) {
      return;
    }
    when(destination.uploadBlob(anyString(), anyLong(), any())).thenReturn(false);

    BlobProperties blobProps = new BlobProperties(defaultBlobSize, serviceId, accountId, containerId, false);
    String blobId = putRandomBlob(blobProps);
    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.PUT));
    checkUploadMetrics(1, 0, 0);
  }

  @Test
  public void testDeleteNonexistantBlob() throws Exception {
    if (useRealCloudService) {
      return;
    }
    when(destination.deleteBlob(anyString())).thenReturn(false);

    BlobProperties blobProps = new BlobProperties(defaultBlobSize, serviceId, accountId, containerId, false);
    String blobId = putRandomBlob(blobProps);
    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.DELETE));
    checkDeleteMetrics(1, 0, 0);
  }

  // Simulate exceptions, use bad account, container, blobId, configSpec
  @Test
  public void testReplicationWithExceptions() throws Exception {
    if (useRealCloudService) {
      return;
    }
    StorageException sex = new StorageException("INVALID_KEY", "Invalid storage key", null);
    when(destinationFactory.getCloudDestination(any(CloudReplicationConfig.class))).thenThrow(InstantiationException.class);

    BlobProperties blobProps = new BlobProperties(defaultBlobSize, serviceId, accountId, containerId, false);
    String blobId = putRandomBlob(blobProps);
    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.PUT));
    assertFalse(eventSource.q.isEmpty());
    checkUploadMetrics(1, 0, 1);

    when(destinationFactory.getCloudDestination(any(CloudReplicationConfig.class))).thenReturn(destination);
    when(destination.uploadBlob(anyString(), anyLong(), any())).thenThrow(sex);

    eventSource.pumpEvent(new BlobEvent(blobId, BlobOperation.PUT));
    assertFalse(eventSource.q.isEmpty());
    checkUploadMetrics(2, 0, 2);
  }

  private String putBlob(BlobProperties blobProperties, InputStream inputStream) throws Exception {
    ReadableStreamChannel channel = new InputStreamReadableStreamChannel(inputStream, blobProperties.getBlobSize(),
        Executors.newSingleThreadExecutor());
    Future<String> fut = router.putBlob(blobProperties, null, channel, null);
    return fut.get();
  }

  // put a blob generated from random bytes
  private String putRandomBlob(BlobProperties blobProperties) throws Exception {
    byte[] randomBytes = TestUtils.getRandomBytes((int) blobProperties.getBlobSize());
    InputStream inputStream = new ByteArrayInputStream(randomBytes);
    return putBlob(blobProperties, inputStream);
  }

  private void checkUploadMetrics(long expectedRequestCount, long expectedUploadedCount, long expectedErrorCount) {
    assertEquals("Unexpected blobUploadRequestCount", expectedRequestCount, metrics.blobUploadRequestCount.getCount());
    assertEquals("Unexpected blobUploadedCount", expectedUploadedCount, metrics.blobUploadedCount.getCount());
    assertEquals("Unexpected blobUploadErrorCount", expectedErrorCount, metrics.blobUploadErrorCount.getCount());
  }

  private void checkDeleteMetrics(long expectedRequestCount, long expectedDeletedCount, long expectedErrorCount) {
    assertEquals("Unexpected blobDeleteRequestCount", expectedRequestCount, metrics.blobDeleteRequestCount.getCount());
    assertEquals("Unexpected blobDeletedCount", expectedDeletedCount, metrics.blobDeletedCount.getCount());
    assertEquals("Unexpected blobDeleteErrorCount", expectedErrorCount, metrics.blobDeleteErrorCount.getCount());
  }

  private static class DummyCryptoService implements CryptoService<SecretKeySpec> {

    @Override
    public ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
      return toEncrypt;
    }

    @Override
    public ByteBuffer decrypt(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
      return toDecrypt;
    }

    @Override
    public ByteBuffer encryptKey(SecretKeySpec toEncrypt, SecretKeySpec key) throws GeneralSecurityException {
      return null;
    }

    @Override
    public SecretKeySpec decryptKey(ByteBuffer toDecrypt, SecretKeySpec key) throws GeneralSecurityException {
      return null;
    }
  }

  public static class InMemoryBlobEventSource implements BlobEventSource {
    private BlobEventConsumer consumer;
    private Queue<BlobEvent> q;

    public InMemoryBlobEventSource() {
      q = new ArrayBlockingQueue<BlobEvent>(100);
    }

    public void pumpEvent(BlobEvent blobEvent) {
      // put event in queue
      q.add(blobEvent);

      // Drain the queue
      while (!q.isEmpty()) {
        BlobEvent event = q.peek();
        if (consumer.onBlobEvent(event)) {
          q.remove();
        } else {
          break;
        }
      }
    }

    @Override
    public void subscribe(BlobEventConsumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void unsubscribe(BlobEventConsumer consumer) {
      // whatever
    }
  }
}
