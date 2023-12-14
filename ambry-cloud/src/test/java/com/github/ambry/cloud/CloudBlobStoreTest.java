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
package com.github.ambry.cloud;

import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.Response;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.cloud.azure.CosmosChangeFeedFindToken;
import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.replication.BlobIdTransformer;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.MockFindToken;
import com.github.ambry.replication.MockFindTokenHelper;
import com.github.ambry.replication.MockHost;
import com.github.ambry.replication.MockNetworkClient;
import com.github.ambry.replication.MockNetworkClientFactory;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockMessageWriteSet;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.cloud.CloudTestUtil.*;
import static com.github.ambry.replication.ReplicationTest.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;


/**
 * Test class testing behavior of CloudBlobStore class.
 */
@RunWith(Parameterized.class)
public class CloudBlobStoreTest {
  public static final Logger logger = LoggerFactory.getLogger(CloudBlobStoreTest.class);
  private static final int SMALL_BLOB_SIZE = 100;
  private final boolean isVcr;
  private CloudBlobStore store;
  private CloudDestination dest;
  private PartitionId partitionId;
  private ClusterMap clusterMap;
  private VcrMetrics vcrMetrics;
  private Random random = new Random();
  private short refAccountId = 50;
  private short refContainerId = 100;
  private long operationTime = System.currentTimeMillis();
  private final int defaultCacheLimit = 1000;
  private CloudConfig cloudConfig;

  protected String ambryBackupVersion;
  protected int currentCacheLimit;
  protected Properties properties;
  protected boolean compactionDryRun = true;

  protected AccountService accountService = mock(AccountService.class);

  /**
   * Test parameters
   * Version 1 = Legacy VCR code and tests
   * Version 2 = VCR 2.0 with Sync cloud destination - AzureCloudDestinationSync
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{CloudConfig.AMBRY_BACKUP_VERSION_1}, {CloudConfig.AMBRY_BACKUP_VERSION_2}});
  }

  public CloudBlobStoreTest(String ambryBackupVersion) throws Exception {
    this.ambryBackupVersion = ambryBackupVersion;
    this.isVcr = true; // Just hardcode true. false is for blueshift, which is deprecated.
    partitionId = new MockPartitionId();
    clusterMap = new MockClusterMap();
  }

  /**
   * Setup the cloud blobstore.
   * @param inMemoryDestination whether to use in-memory cloud destination instead of mock
   * @param requireEncryption value of requireEncryption flag in CloudConfig.
   * @param cacheLimit size of the store's recent blob cache.
   * @param start whether to start the store.
   */
  private void setupCloudStore(boolean inMemoryDestination, boolean requireEncryption, int cacheLimit, boolean start)
      throws ReflectiveOperationException {
    properties = new Properties();
    // Required clustermap properties
    setBasicProperties(properties);
    // Require encryption for uploading
    properties.setProperty(CloudConfig.VCR_MIN_TTL_DAYS, String.valueOf(0)); // Disable this. Makes testing difficult.
    properties.setProperty(CloudConfig.VCR_REQUIRE_ENCRYPTION, Boolean.toString(requireEncryption));
    properties.setProperty(CloudConfig.CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS,
        TestCloudBlobCryptoAgentFactory.class.getName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    cloudConfig = new CloudConfig(verifiableProperties);
    MetricRegistry metricRegistry = new MetricRegistry();
    vcrMetrics = new VcrMetrics(metricRegistry);
    if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_1)) {
      currentCacheLimit = cacheLimit;
      properties.setProperty(CloudConfig.CLOUD_RECENT_BLOB_CACHE_LIMIT, String.valueOf(currentCacheLimit));
      dest = inMemoryDestination ? new LatchBasedInMemoryCloudDestination(Collections.emptyList(), clusterMap)
          : mock(CloudDestination.class);
    } else if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2)) {
      // TODO: This test suite needs improvements. It has a mixture of code from 3 different use-cases.
      properties.setProperty(AzureCloudConfig.AZURE_NAME_SCHEME_VERSION, "1");
      properties.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "PARTITION");
      properties.setProperty(CloudConfig.CLOUD_MAX_ATTEMPTS, "1");
      properties.setProperty(CloudConfig.CLOUD_COMPACTION_DRY_RUN_ENABLED, String.valueOf(this.compactionDryRun));
      properties.setProperty(CloudConfig.CLOUD_COMPACTION_GRACE_PERIOD_DAYS, String.valueOf(0));
      properties.setProperty(AzureCloudConfig.AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE, String.valueOf(1));
      /*
       * snalli@:
       * Just disable the cache. It just adds another layer of complexity and more of a nuisance than any help.
       * The intent of the cache was to absorb duplicate writes from hitting Azure and mimic a disk-based store in error-handling.
       * It fails to do that. The cache was probably introduced to prevent throttling from CosmosDB or the author didn't fully understand replication or caching.
       * V2 doesn't use CosmosDB.
       * 1. Duplicate writes will be rare because replication checks for a blob in cloud at the expected state before uploading or updating it.
       *    Duplicate writes can also be avoided by Azure using an ETag conditional check in the HTTP request. So why cache ?
       * 2. Caching should improve performance but must never alter program behavior.
       *    But the current backup stack V1 in VCR behaves differently when the cache is present from when it is absent.
       *    If an entry is not found in the cache, then the request goes through to Azure leading to an unexpected result.
       *    Set the cache-size to 0 for V1 tests and see for yourself !
       * 3. For the cache to be effective, it should be populated on the getMetadata() path, but it's not. So what's the point of the cache ?
       *
       * I don't have the cycles to fix CloudBlobStore V1 and reason about cache management.
       * Its just wrong the way its implemented and I cannot have V2 code behave differently because of it.
       * However, I have tested it with both a null cache and a valid cache.
       */
      currentCacheLimit = 0;
      properties.setProperty(CloudConfig.CLOUD_RECENT_BLOB_CACHE_LIMIT, String.valueOf(currentCacheLimit));
      // Azure container name will be <clusterName>-<partitionId>, so dev-0 for this test
      dest = new AzuriteUtils().getAzuriteClient(properties, metricRegistry, clusterMap, accountService);
    }
    store = new CloudBlobStore(verifiableProperties, partitionId, dest, clusterMap, vcrMetrics);
    if (start) {
      store.start();
    }
  }

  /**
   * Method to set basic required properties
   * @param properties the Properties to set
   */
  private void setBasicProperties(Properties properties) {
    properties.setProperty("clustermap.cluster.name", "dev");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.resolve.hostnames", "false");
    properties.setProperty("kms.default.container.key", TestUtils.getRandomKey(64));
    properties.setProperty(CloudConfig.CLOUD_IS_VCR, String.valueOf(isVcr));
  }

  @Before
  public void beforeTest() {
    boolean isV1 = ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_1);
    boolean isV2 = ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2);
    boolean isConnectToAzurite = isV2 ? new AzuriteUtils().connectToAzurite() : false;
    if (!(isV1 || (isV2 && isConnectToAzurite))) {
      logger.error("isV1 = {}, isV2 = {}, isConnectToAzurite = {}", isV1, isV2, isConnectToAzurite);
    }
    assumeTrue(isV1 || (isV2 && isConnectToAzurite));
  }

  protected void v1TestOnly() {
    assumeTrue(ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_1));
  }
  protected void v2TestOnly() {
    assumeTrue(ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2) && new AzuriteUtils().connectToAzurite());
  }

  protected void clearContainer(PartitionId testPartitionId, AzureCloudDestinationSync azureCloudDestinationSync, VerifiableProperties verifiableProperties) {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    AzureBlobLayoutStrategy azureBlobLayoutStrategy = new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName, new AzureCloudConfig(verifiableProperties));
    String blobContainerName = azureBlobLayoutStrategy.getClusterAwareAzureContainerName(String.valueOf(testPartitionId.getId()));
    BlobContainerClient blobContainerClient = azureCloudDestinationSync.getBlobStore(blobContainerName);
    if (blobContainerClient == null) {
      logger.info("Blob container {} does not exist", blobContainerName);
      return;
    }
    ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setDetails(new BlobListDetails().setRetrieveMetadata(true));
    String continuationToken = null;
    for (PagedResponse<BlobItem> blobItemPagedResponse :
        blobContainerClient.listBlobs(listBlobsOptions, null).iterableByPage(continuationToken)) {
      continuationToken = blobItemPagedResponse.getContinuationToken();
      for (BlobItem blobItem : blobItemPagedResponse.getValue()) {
        blobContainerClient.getBlobClient(blobItem.getName()).delete();
      }
      if (continuationToken == null) {
        logger.info("Reached end-of-partition as Azure blob storage continuationToken is null");
        break;
      }
    }
  }

  protected BlobContainerClient getTestBlobContainerClient(AzureCloudDestinationSync azureCloudDestinationSync, PartitionId partitionId) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    AzureBlobLayoutStrategy azureBlobLayoutStrategy = new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName, new AzureCloudConfig(verifiableProperties));
    String blobContainerName = azureBlobLayoutStrategy.getClusterAwareAzureContainerName(String.valueOf(partitionId.getId()));
    return azureCloudDestinationSync.getBlobStore(blobContainerName);
  }

  /**
   * Tests that compaction handles failures incl. 404 and other errors
   * @throws ReflectiveOperationException
   * @throws StoreException
   * @throws CloudStorageException
   */
  @Test
  public void testCompactionFailure()
      throws ReflectiveOperationException, StoreException, CloudStorageException {
    v2TestOnly();
    this.compactionDryRun = false;
    setupCloudStore(false, false, 0, true);
    clearContainer(partitionId, (AzureCloudDestinationSync) dest, new VerifiableProperties(properties));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    /*
      Add some permanent blobs.
      set isVcr = true so that the lifeVersion is 0, else it is -1
     */
    int numBlobs = 10;
    IntStream.range(0,numBlobs).forEach(i -> CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024,
        Utils.Infinite_Time, refAccountId, refContainerId, false, false, partitionId,
        System.currentTimeMillis(), isVcr));

    /*
      Spy on these objects to intercept certain calls. We don't want to mock all of them because we want
      to execute the real methods by contacting Azurite - storage emulator. However, we want to inject failures at
      certain points for testing. Besides, mocking would involve a lot of interface change which is just ugly.
      Might be easier if dependency injection is used.

      Object map:
      CloudBlobStore -> AzureCloudDestinationSync -> BlobContainerClient -> BlobClient
     */
    MetricRegistry metricRegistry = new MetricRegistry();
    VcrMetrics vcrMetrics = new VcrMetrics(metricRegistry);
    AzureCloudDestinationSync spyDest = spy(new AzuriteUtils().getAzuriteClient(properties, metricRegistry,
        clusterMap, accountService));
    CloudBlobStore spyStore = spy(new CloudBlobStore(new VerifiableProperties(properties), partitionId, spyDest,
        clusterMap, vcrMetrics));
    spyStore.start();
    BlobContainerClient spyContainerClient = spy(getTestBlobContainerClient(spyDest, partitionId));
    when(spyDest.getBlobStore(any())).thenReturn(spyContainerClient);

    // Put blobs
    spyStore.put(messageWriteSet);

    // Delete blobs
    spyStore.delete(messageWriteSet.getMessageSetInfo());

    // Compact blobs, return error
    BlobClient mockBlobClient = mock(BlobClient.class);
    Response<Void> mockResponse = mock(Response.class);
    when(spyContainerClient.getBlobClient(any())).thenReturn(mockBlobClient);
    when(mockBlobClient.deleteWithResponse(any(), any(), any(), any())).thenReturn(mockResponse);
    // Return error when compaction tries to delete blobs and check nothing is deleted
    when(mockResponse.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    assertEquals(0, spyDest.compactPartition(String.valueOf(partitionId.getId())));
    assertEquals(numBlobs, vcrMetrics.compactionFailureCount.getCount());
    when(mockResponse.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    assertEquals(0, spyDest.compactPartition(String.valueOf(partitionId.getId())));
    assertEquals(numBlobs * 2, vcrMetrics.compactionFailureCount.getCount());
    // Check blob exists
    assertEquals(0, vcrMetrics.blobCompactionRate.getCount());
    when(spyContainerClient.getBlobClient(any())).thenCallRealMethod();
    messageWriteSet.getMessageSetInfo().forEach(messageInfo ->
        assertTrue(spyDest.doesBlobExist((BlobId) messageInfo.getStoreKey())));

    // Really compact and check blobs are gone
    when(mockBlobClient.deleteWithResponse(any(), any(), any(), any())).thenCallRealMethod();
    assertEquals(numBlobs, spyDest.compactPartition(String.valueOf(partitionId.getId())));
    assertEquals(numBlobs, vcrMetrics.blobCompactionRate.getCount());
    messageWriteSet.getMessageSetInfo().forEach(messageInfo ->
        assertFalse(spyDest.doesBlobExist((BlobId) messageInfo.getStoreKey())));
  }

  @Test
  public void testCompactDeletedAccountContainer()
      throws ReflectiveOperationException, CloudStorageException, InterruptedException, StoreException {
    testCompactDeletedAccountContainer(true);
    testCompactDeletedAccountContainer(false);
  }

  protected void testCompactDeletedAccountContainer(boolean deleteContainer)
      throws ReflectiveOperationException, StoreException, CloudStorageException, InterruptedException {
    v2TestOnly();
    this.compactionDryRun = false;
    setupCloudStore(false, false, 0, true);
    clearContainer(partitionId, (AzureCloudDestinationSync) dest, new VerifiableProperties(properties));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    for (int j = 0; j < 10; j++) {
      // permanent blobs
      // set isVcr = true so that the lifeVersion is 0
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024, Utils.Infinite_Time, refAccountId, refContainerId, false,
          false, partitionId, System.currentTimeMillis(), isVcr);
    }

    // Put blobs
    store.put(messageWriteSet);

    if (deleteContainer) {
      Container container = new ContainerBuilder()
          .setParentAccountId(refAccountId)
          .setId(refContainerId)
          .setName("testContainer")
          .setStatus(Container.ContainerStatus.DELETE_IN_PROGRESS)
          .build();
      when(accountService.getContainersByStatus(any())).thenReturn(Collections.singleton(container));
    } else {
      when(accountService.getContainersByStatus(any())).thenReturn(Collections.emptySet());
    }

    // Compact blobs
    List<MessageInfo> messageInfoList = messageWriteSet.getMessageSetInfo();
    if (deleteContainer) {
      assertEquals(messageInfoList.size(), dest.compactPartition(String.valueOf(partitionId.getId())));
    } else {
      assertEquals(0, dest.compactPartition(String.valueOf(partitionId.getId())));
    }
    for (MessageInfo messageInfo: messageInfoList) {
      // if deleteContainer = true, then blob does not exist
      assertEquals(deleteContainer, !dest.doesBlobExist((BlobId) messageInfo.getStoreKey()));
    }
  }

  @Test
  public void testCompactionShutdown()
      throws ReflectiveOperationException, StoreException, CloudStorageException, InterruptedException {
    v2TestOnly();
    this.compactionDryRun = false;
    setupCloudStore(false, false, 0, true);
    clearContainer(partitionId, (AzureCloudDestinationSync) dest, new VerifiableProperties(properties));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    for (int j = 0; j < 10; j++) {
      // permanent blobs
      // set isVcr = true so that the lifeVersion is 0
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024, Utils.Infinite_Time, refAccountId, refContainerId, false,
          false, partitionId, System.currentTimeMillis(), isVcr);
    }

    // Put blobs
    store.put(messageWriteSet);

    // Try to Compact blobs
    dest.stopCompaction();
    List<MessageInfo> messageInfoList = messageWriteSet.getMessageSetInfo();
    assertEquals(0, dest.compactPartition(String.valueOf(partitionId.getId())));
    for (MessageInfo messageInfo: messageInfoList) {
      assertTrue(dest.doesBlobExist((BlobId) messageInfo.getStoreKey()));
    }
  }

  @Test
  public void testCompactPermanentBlobs()
      throws ReflectiveOperationException, StoreException, CloudStorageException, InterruptedException {
    v2TestOnly();
    this.compactionDryRun = false;
    setupCloudStore(false, false, 0, true);
    clearContainer(partitionId, (AzureCloudDestinationSync) dest, new VerifiableProperties(properties));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    for (int j = 0; j < 10; j++) {
      // permanent blobs
      // set isVcr = true so that the lifeVersion is 0
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024, Utils.Infinite_Time, refAccountId, refContainerId, false,
          false, partitionId, System.currentTimeMillis(), isVcr);
    }

    // Put blobs
    store.put(messageWriteSet);

    // Try to Compact blobs
    List<MessageInfo> messageInfoList = messageWriteSet.getMessageSetInfo();
    assertEquals(0, dest.compactPartition(String.valueOf(partitionId.getId())));
    for (MessageInfo messageInfo: messageInfoList) {
      assertTrue(dest.doesBlobExist((BlobId) messageInfo.getStoreKey()));
    }
  }

  @Test
  public void testCompactDeletedBlobs()
      throws ReflectiveOperationException, CloudStorageException, InterruptedException, StoreException {
    testCompactDeletedBlobs(false);
    testCompactDeletedBlobs(true);
  }

  protected void testCompactDeletedBlobs(boolean dryRun)
      throws ReflectiveOperationException, StoreException, CloudStorageException, InterruptedException {
    v2TestOnly();
    this.compactionDryRun = dryRun;
    setupCloudStore(false, false, 0, true);
    clearContainer(partitionId, (AzureCloudDestinationSync) dest, new VerifiableProperties(properties));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    for (int j = 0; j < 10; j++) {
      // permanent blobs
      // set isVcr = true so that the lifeVersion is 0
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024, Utils.Infinite_Time, refAccountId, refContainerId, false,
          false, partitionId, System.currentTimeMillis(), isVcr);
    }

    // Put blobs
    store.put(messageWriteSet);
    TimeUnit.SECONDS.sleep(1);

    // Delete blobs
    List<MessageInfo> messageInfoList = messageWriteSet.getMessageSetInfo();
    store.delete(messageInfoList);

    // Compact blobs
    assertEquals(messageInfoList.size(), dest.compactPartition(String.valueOf(partitionId.getId())));
    for (MessageInfo messageInfo: messageInfoList) {
      // if dryRun = true, then blob must exist
      assertEquals(dryRun, dest.doesBlobExist((BlobId) messageInfo.getStoreKey()));
    }
  }

  /**
   * Tests that compaction stops when partition ownership is removed from host
   * @throws ReflectiveOperationException
   * @throws StoreException
   * @throws InterruptedException
   * @throws CloudStorageException
   */
  @Test
  public void testCompactPartitionDisown()
      throws ReflectiveOperationException, StoreException, InterruptedException, CloudStorageException {
    v2TestOnly();
    this.compactionDryRun = false;
    setupCloudStore(false, false, 0, true);
    clearContainer(partitionId, (AzureCloudDestinationSync) dest, new VerifiableProperties(properties));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    for (int j = 0; j < 10; j++) {
      // permanent blobs
      // set isVcr = true so that the lifeVersion is 0
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024, Utils.Infinite_Time, refAccountId, refContainerId, false,
          false, partitionId, System.currentTimeMillis(), isVcr);
    }

    // Put blobs
    store.put(messageWriteSet);
    TimeUnit.SECONDS.sleep(1);

    // Delete blobs
    List<MessageInfo> messageInfoList = messageWriteSet.getMessageSetInfo();
    store.delete(messageInfoList);

    // Compact blobs
    CloudDestination spyDest = spy(dest);
    AtomicInteger numInvoc = new AtomicInteger(0);
    // The number of results per page from ABS is 1.
    // isCompactionStopped() is invoked twice - once to prior to getting the page and once prior to deleting the blob.
    // If we have 10 blobs, by the 5-th blob we have 10 invocations of isCompactionStopped.
    // We want to delete first 5 blobs and retain the last 5.
    Mockito.lenient().when(spyDest.isCompactionStopped(any(), any())).thenAnswer(invocation -> numInvoc.incrementAndGet() > messageInfoList.size());
    assertEquals(Math.floorDiv(messageInfoList.size(), 2), spyDest.compactPartition(String.valueOf(partitionId.getId())));
  }

  @Test
  public void testCompactExpiredBlobs()
      throws ReflectiveOperationException, CloudStorageException, InterruptedException, StoreException {
    testCompactExpiredBlobs(false);
    testCompactExpiredBlobs(true);
  }

  protected void testCompactExpiredBlobs(boolean dryRun)
      throws ReflectiveOperationException, StoreException, CloudStorageException, InterruptedException {
    v2TestOnly();
    this.compactionDryRun = dryRun;
    setupCloudStore(false, false, 0, true);
    clearContainer(partitionId, (AzureCloudDestinationSync) dest, new VerifiableProperties(properties));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    long maxExpiry = System.currentTimeMillis();
    for (int j = 0; j < 10; j++) {
      // permanent blobs
      // set isVcr = true so that the lifeVersion is 0
      long now = System.currentTimeMillis();
      long expiry = now + 1000;
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024, expiry, refAccountId, refContainerId, false,
          false, partitionId, now, isVcr);
      maxExpiry = Math.max(expiry, maxExpiry);
    }

    // Put blobs
    store.put(messageWriteSet);

    // Wait for blobs to expire
    long timeToSleep = maxExpiry - System.currentTimeMillis() + 1000; // a buffer period of 1000 to ensure expiry
    if (timeToSleep > 0) {
      logger.info("Sleeping for {} milliseconds for blobs to expire", timeToSleep);
      TimeUnit.MILLISECONDS.sleep(timeToSleep);
    }

    // Compact blobs
    List<MessageInfo> messageInfoList = messageWriteSet.getMessageSetInfo();
    assertEquals(messageInfoList.size(), dest.compactPartition(String.valueOf(partitionId.getId())));
    for (MessageInfo messageInfo: messageInfoList) {
      // if dryRun = true, then blob must exist
      assertEquals(dryRun, dest.doesBlobExist((BlobId) messageInfo.getStoreKey()));
    }
  }

  /** Test the CloudBlobStore put method. */
  @Test
  public void testStorePuts() throws Exception {
    testStorePuts(false);
    testStorePuts(true);
  }

  /**
   * Test the CloudBlobStore put method with specified encryption.
   * @param requireEncryption true for encrypted puts. false otherwise.
   */
  private void testStorePuts(boolean requireEncryption) throws Exception {
    setupCloudStore(true, requireEncryption, defaultCacheLimit, true);
    // Put blobs with and without expiration and encryption
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 1;
    int expectedUploads = 0;
    long expectedBytesUploaded = 0;
    int expectedEncryptions = 0;
    for (int j = 0; j < count; j++) {
      long size = Math.abs(random.nextLong()) % 10000;
      // Permanent and encrypted, should be uploaded and not reencrypted
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, true,
          false, partitionId, operationTime, isVcr);
      expectedUploads++;
      expectedBytesUploaded += size;
      // Permanent and unencrypted
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, false,
          false, partitionId, operationTime, isVcr);
      expectedUploads++;
      expectedBytesUploaded += size;
      if (requireEncryption) {
        expectedEncryptions++;
      }
    }

    /*
        Test 1: Put blob.
        V1: Success
        V2: Success
    */
    store.put(messageWriteSet);
    if (dest instanceof LatchBasedInMemoryCloudDestination) {
      LatchBasedInMemoryCloudDestination inMemoryDest = (LatchBasedInMemoryCloudDestination) dest;
      assertEquals("Unexpected blobs count", expectedUploads, inMemoryDest.getBlobsUploaded());
      assertEquals("Unexpected byte count", expectedBytesUploaded, inMemoryDest.getBytesUploaded());
    }
    assertEquals("Unexpected encryption count", expectedEncryptions, vcrMetrics.blobEncryptionCount.getCount());
    verifyCacheHits(expectedUploads, 0);

    /*
      Test 2: Try to put the same blobs again (e.g. from another replica).
      V1: Ex thrown, which is probably wrong.
      V2: No ex thrown, Azure throws an err and V2 absorbs it to let repl advance. It would be wise to throw an error
      and let the repl layer handle it, but repl-layer doesn't.
     */
    messageWriteSet.resetBuffers();
    for (MessageInfo messageInfo : messageWriteSet.getMessageSetInfo()) {
      try {
        MockMessageWriteSet mockMessageWriteSet = new MockMessageWriteSet();
        CloudTestUtil.addBlobToMessageSet(mockMessageWriteSet, (BlobId) messageInfo.getStoreKey(),
            messageInfo.getSize(), messageInfo.getExpirationTimeInMs(), operationTime, isVcr);
        store.put(mockMessageWriteSet);
        if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_1)) {
          fail("Uploading already uploaded blob should throw error");
        }
      } catch (StoreException ex) {
        assertEquals(ex.getErrorCode(), StoreErrorCodes.Already_Exist);
      }
    }
    int expectedSkips = isVcr ? expectedUploads : 0;
    if (dest instanceof LatchBasedInMemoryCloudDestination) {
      LatchBasedInMemoryCloudDestination inMemoryDest = (LatchBasedInMemoryCloudDestination) dest;
      assertEquals("Unexpected blobs count", expectedUploads, inMemoryDest.getBlobsUploaded());
      assertEquals("Unexpected byte count", expectedBytesUploaded, inMemoryDest.getBytesUploaded());
    }
    if (currentCacheLimit > 0) {
      assertEquals("Unexpected skipped count", expectedSkips, vcrMetrics.blobUploadSkippedCount.getCount());
    }
    verifyCacheHits(2 * expectedUploads, expectedUploads);

    /*
      Test 3: Try to upload a set of blobs containing duplicates
      V1: Failure expected
      V2: Failure expected
     */
    MessageInfo duplicateMessageInfo =
        messageWriteSet.getMessageSetInfo().get(messageWriteSet.getMessageSetInfo().size() - 1);
    CloudTestUtil.addBlobToMessageSet(messageWriteSet, (BlobId) duplicateMessageInfo.getStoreKey(),
        duplicateMessageInfo.getSize(), duplicateMessageInfo.getExpirationTimeInMs(), operationTime, isVcr);
    try {
      store.put(messageWriteSet);
    } catch (IllegalArgumentException iaex) {
      assertTrue(iaex.getMessage().startsWith("list contains duplicates"));
    }
    verifyCacheHits(2 * expectedUploads, expectedUploads);

    /*
      Test 4: Verify that a blob marked as deleted is not uploaded.
      This will never happen because repl logic never uploads a blob marked for deletion.
      snalli@: I am not fixing this test case or changing V2 to accommodate it.
     */
    MockMessageWriteSet deletedMessageWriteSet = new MockMessageWriteSet();
    CloudTestUtil.addBlobToMessageSet(deletedMessageWriteSet, 100, Utils.Infinite_Time, refAccountId, refContainerId,
        true, true, partitionId, operationTime, isVcr);
    store.put(deletedMessageWriteSet);
    expectedSkips++;
    verifyCacheHits(2 * expectedUploads, expectedUploads);
    if (currentCacheLimit > 0) {
      assertEquals(expectedSkips, vcrMetrics.blobUploadSkippedCount.getCount());
    }

    /*
        Test 5: Verify that a blob that is expiring soon is not uploaded for vcr but is uploaded for frontend.
        V1: Success
        V2: Success, V2 just uploads the blob. If it expires, then compaction will take care of it.
        These cloudConfigs were probably introduced to avoid throttling from CosmosDB and are not needed anymore.
        They just make deployment, testing and troubleshooting unnecessarily difficult, for eg. vcrMinTtlDays.
     */
    messageWriteSet = new MockMessageWriteSet();
    CloudTestUtil.addBlobToMessageSet(messageWriteSet, 100, operationTime + cloudConfig.vcrMinTtlDays - 1, refAccountId,
        refContainerId, true, false, partitionId, operationTime, isVcr);
    store.put(messageWriteSet);
    int expectedLookups = (2 * expectedUploads) + 1;
    if (isVcr) {
      expectedSkips++;
    } else {
      expectedUploads++;
    }
    verifyCacheHits(expectedLookups, expectedUploads);
    if (currentCacheLimit > 0) {
      assertEquals(expectedSkips, vcrMetrics.blobUploadSkippedCount.getCount());
    }
  }

  /** Test the CloudBlobStore delete method. */
  @Test
  public void testStoreDeletes() throws Exception {
    // FIXED: snalli@: V1 test that was broken all along, but fixed it now for both V1 and V2
    setupCloudStore(false, true, defaultCacheLimit, true);
    int count = 1;
    long now = System.currentTimeMillis();
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    Map<BlobId, MessageInfo> messageInfoMap = new HashMap<>();
    short initialLifeVersion = 100;
    for (int j = 0; j < count; j++) {
      BlobId blobId = getUniqueId(refAccountId, refContainerId, true, partitionId);
      MessageInfo info = new MessageInfo(blobId, SMALL_BLOB_SIZE, refAccountId, refContainerId, now, initialLifeVersion);
      messageInfoMap.put(blobId, info);
      messageWriteSet.add(info, ByteBuffer.wrap(TestUtils.getRandomBytes(SMALL_BLOB_SIZE)));
    }

    // Put some blobs for deletion
    store.put(messageWriteSet);
    int numPuts = count;

    // Test 1: Delete. This should succeed.
    store.delete(new ArrayList<>(messageInfoMap.values()));
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(count)).deleteBlob(any(BlobId.class), eq(now), anyShort(), any(CloudUpdateValidator.class));
    }
    verifyCacheHits(count + numPuts, 0);

    // Test 2: Call second time with same life version. This should fail.
    for (MessageInfo messageInfo : messageInfoMap.values()) {
      try {
        store.delete(Collections.singletonList(messageInfo));
        // FIXME: snalli@: V1 throws the wrong error code in the presence of a cache and no error when there is no cache.
        // FIXME: I don't have the time to fix it. Check BlobStore::delete for correct behavior. V2 does the right thing however.
        fail("delete must throw an exception with errcode = ID_Deleted");
      } catch (StoreException ex) {
        assertEquals(ex.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
    }
    if (mockingDetails(dest).isMock()) {
      int expectedCount = isVcr ? count : count * 2;
      verify(dest, times(expectedCount)).deleteBlob(any(BlobId.class), eq(now), anyShort(),
          any(CloudUpdateValidator.class));
    }
    verifyCacheHits(count * 2 + numPuts, count);

    // Test 3: Call again with a smaller life version. This should fail.
    // FIXED: snalli@: This test created this new array newMessageInfoMap but never used it. Fixed it now.
    Map<BlobId, MessageInfo> newMessageInfoMap = new HashMap<>();
    for (BlobId blobId : messageInfoMap.keySet()) {
      newMessageInfoMap.put(blobId,
          new MessageInfo(blobId, SMALL_BLOB_SIZE, refAccountId, refContainerId, now, (short) (initialLifeVersion-1)));
    }
    for (MessageInfo messageInfo : newMessageInfoMap.values()) {
      try {
        store.delete(Collections.singletonList(messageInfo));
        fail("delete must throw an exception with errcode = ID_Deleted");
      } catch (StoreException ex) {
        // FIXME: snalli@: V1 throws the wrong error code with the cache and no error without the cache.
        // FIXME: I don't have the time to fix it. Check BlobStore::delete for correct behavior. V2 does the right thing however.
        StoreErrorCodes storeErrorCode = currentCacheLimit > 0 ? StoreErrorCodes.ID_Deleted : StoreErrorCodes.Life_Version_Conflict;
        assertEquals(ex.getErrorCode(), storeErrorCode);
      }
    }
    if (mockingDetails(dest).isMock()) {
      int expectedCount = isVcr ? count : count * 3;
      verify(dest, times(expectedCount)).deleteBlob(any(BlobId.class), eq(now), anyShort(),
          any(CloudUpdateValidator.class));
    }
    verifyCacheHits(count * 3 + numPuts, count * 2);

    // Test 4: Call again with a larger life version. This should succeed.
    newMessageInfoMap = new HashMap<>();
    for (BlobId blobId : messageInfoMap.keySet()) {
      newMessageInfoMap.put(blobId,
          new MessageInfo(blobId, SMALL_BLOB_SIZE, refAccountId, refContainerId, now, (short) (initialLifeVersion+1)));
    }
    for (MessageInfo messageInfo : newMessageInfoMap.values()) {
      store.delete(Collections.singletonList(messageInfo));
      // FIXED: snalli@: V1 never threw an error and yet there was a catch block. Unnecessary and confusing.
    }
    if (mockingDetails(dest).isMock()) {
      int expectedCount = isVcr ? count * 2 : count * 4;
      verify(dest, times(expectedCount)).deleteBlob(any(BlobId.class), eq(now), anyShort(),
          any(CloudUpdateValidator.class));
    }
    verifyCacheHits(count * 4 + numPuts, count * 2);

    // Test 5: Try to upload a set of blobs containing duplicates. This should fail.
    List<MessageInfo> messageInfoList = new ArrayList<>(messageInfoMap.values());
    messageInfoList.add(messageInfoList.get(messageInfoMap.values().size() - 1));
    try {
      store.delete(messageInfoList);
      fail("delete must throw an exception for duplicates");
    } catch (IllegalArgumentException iaex) {
      assumeTrue(iaex.getMessage().startsWith("list contains duplicates"));
    }
    verifyCacheHits(count * 4 + numPuts, count * 2);
  }

  /** Test the CloudBlobStore updateTtl method. */
  @Test
  public void testStoreTtlUpdates() throws Exception {
    setupCloudStore(true, true, defaultCacheLimit, true);
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int expectedCount = 0;
    long now = System.currentTimeMillis();
    short initialLifeVersion = 100;
    BlobId blobId = getUniqueId(refAccountId, refContainerId, true, partitionId);
    MessageInfo info = new MessageInfo(blobId, SMALL_BLOB_SIZE, false, false, false, System.currentTimeMillis() + 600000, null, refAccountId, refContainerId, now, initialLifeVersion);
    messageWriteSet.add(info, ByteBuffer.wrap(TestUtils.getRandomBytes(SMALL_BLOB_SIZE)));

    // Put a blob for ttl-update
    store.put(messageWriteSet);
    int numPuts = 1;

    /*
      Test 1: Update TTL
      V1: Should pass with no exc
      V2: Should pass with no exc
     */
    MessageInfo ttlUpdateMessage = new MessageInfo.Builder(messageWriteSet.getMessageSetInfo().get(0))
        .expirationTimeInMs(-1)
        .isTtlUpdated(true)
        .build();
    store.updateTtl(Collections.singletonList(ttlUpdateMessage));
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(1)).updateBlobExpiration(any(BlobId.class), anyLong(), any(CloudUpdateValidator.class));
    }
    verifyCacheHits(1 + numPuts, 0);

    /*
        Test 2: Call a second time
        V1: Should pass with no exc
        V2: Exception expected, Already_Updated
     */
    try {
      store.updateTtl(Collections.singletonList(ttlUpdateMessage));
      if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2)) {
        fail("UpdateTTL must throw Already_Updated error");
      }
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Already_Updated, e.getErrorCode());
    }
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(1)).updateBlobExpiration(any(BlobId.class), anyLong(),
          any(CloudUpdateValidator.class));
    }
    verifyCacheHits(2 + numPuts, 1);

    /*
        Test 3: delete, ttl-update
        V1: Should pass with an exc
        V2: Should pass with an exc, ID_Deleted
     */
    MessageInfo deleteMessage = new MessageInfo.Builder(ttlUpdateMessage)
        .isDeleted(true)
        .build();
    store.delete(Collections.singletonList(deleteMessage));
    int numDeletes = 1;
    try {
      store.updateTtl(Collections.singletonList(ttlUpdateMessage));
      if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2)) {
        fail("UpdateTTL must throw Already_Updated error");
      }
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.ID_Deleted, e.getErrorCode());
    }
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(1)).deleteBlob(any(BlobId.class), anyLong(), anyShort(), any(CloudUpdateValidator.class));
    }
    verifyCacheHits(3 + numPuts + numDeletes, 2);

    /*
        Test 4: undelete, ttl-update
        V1: Should pass with an exc
        V2: Should pass with an exc, Already_Updated
     */
    MessageInfo unDeleteMessage = new MessageInfo.Builder(deleteMessage)
        .isUndeleted(true)
        .lifeVersion((short) (initialLifeVersion+1))
        .build();
    store.undelete(unDeleteMessage);
    int numUnDeletes = 1;
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(1)).undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class));
    }
    ttlUpdateMessage = new MessageInfo.Builder(unDeleteMessage)
        .expirationTimeInMs(-1)
        .build();
    try {
      store.updateTtl(Collections.singletonList(ttlUpdateMessage));
      if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2)) {
        fail("UpdateTTL must throw Already_Updated error");
      }
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Already_Updated, e.getErrorCode());
    }
    if (mockingDetails(dest).isMock()) {
      expectedCount = isVcr ? expectedCount : expectedCount + 1;
      verify(dest, times(expectedCount)).updateBlobExpiration(any(BlobId.class), anyLong(),
          any(CloudUpdateValidator.class));
    }
    verifyCacheHits(4 + numPuts + numDeletes + numUnDeletes, 3);

    /*
        Test 5: ttl update with finite expiration time fails
        V1: Should fail
        V2: Should fail
     */
    messageWriteSet = new MockMessageWriteSet();
    CloudTestUtil.addBlobToMessageSet(messageWriteSet, SMALL_BLOB_SIZE, System.currentTimeMillis() + 20000,
        refAccountId, refContainerId, true, false, partitionId, operationTime, isVcr);
    try {
      store.updateTtl(messageWriteSet.getMessageSetInfo());
    } catch (StoreException ex) {
      assertEquals(ex.getErrorCode(), StoreErrorCodes.Update_Not_Allowed);
    }
  }

  /** Test the CloudBlobStore undelete method. */
  @Test
  public void testStoreUndeletes() throws Exception {
    setupCloudStore(true, true, defaultCacheLimit, true);
    long now = System.currentTimeMillis();
    short initialLifeVersion = 100;
    MessageInfo messageInfo =
        new MessageInfo(getUniqueId(refAccountId, refContainerId, true, partitionId), SMALL_BLOB_SIZE, refAccountId,
            refContainerId, now, initialLifeVersion);

    // Test 1: undelete for an absent blob.
    // V1: Should fail and throw ID_Not_Found
    // V2: Should fail and throw ID_Not_Found
    if (mockingDetails(dest).isMock()) {
      when(dest.undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class)))
          .thenThrow(new CloudStorageException("absent blob",
              new StoreException("absent blob", StoreErrorCodes.ID_Not_Found), CloudBlobStore.STATUS_NOT_FOUND, false, null));
    }
    try {
      store.undelete(messageInfo);
      fail("Undelete for a non existent blob should throw exception");
    } catch (StoreException ex) {
      // The expected value must come first
      assertSame(StoreErrorCodes.ID_Not_Found, ex.getErrorCode());
    }
    int numUndelete = 1;
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(1)).undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class));
    }

    // Test 2: Put and Undelete
    // It's possible VCR sees this pattern of request
    // Peer replica : PUT[0] -> DELETE[0] -> UNDELETE[1]
    // VCR : PUT[0] -> DELETE/UNDELETE[1] in the same message
    // V1: Should pass
    // V2: Should pass
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    messageWriteSet.add(messageInfo, ByteBuffer.wrap(TestUtils.getRandomBytes((int) messageInfo.getSize())));
    store.put(messageWriteSet);
    int numPuts = 1;
    if (mockingDetails(dest).isMock()) {
      when(dest.undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class))).thenReturn(
          (short) (initialLifeVersion+1));
    }
    // store.delete(Collections.singletonList(messageInfo));
    int numDeletes = 0;
    MessageInfo undeleteMessage = new MessageInfo.Builder(messageInfo)
        .isDeleted(false)
        .isUndeleted(true)
        .lifeVersion((short) (initialLifeVersion+1))
        .build();
    assertEquals(initialLifeVersion+1, store.undelete(undeleteMessage));
    verifyCacheHits(1 + numPuts + numDeletes + numUndelete, 0);
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(2)).undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class));
    }

    // Test 2: Call second time with same life version.
    // V1: Expects ID_Undeleted. Without the recentBlobCache, no exception is thrown and the requests hits the cloud.
    // FIXME: V1 recentBlobCache is broken as mentioned above and I don't want to reason about cache-mgmt or fix it.
    // V2: Expects ID_Undeleted, passes with or without the cache.
    // Ref BlobStore::undelete() and ReplicaThread::applyUndelete() for correct behavior.
    if (mockingDetails(dest).isMock()) {
      when(dest.undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class)))
          .thenThrow(new CloudStorageException("undeleted blob",
              new StoreException("undeleted blob", StoreErrorCodes.ID_Undeleted)));
    }
    try {
      store.undelete(undeleteMessage);
      fail("Undelete is expected to throw an exception");
    } catch (StoreException ex) {
      // The expected value must come first
      assertEquals(StoreErrorCodes.ID_Undeleted, ex.getErrorCode());
    }
    verifyCacheHits(2 + numPuts + numDeletes + numUndelete, 1);
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(2)).undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class));
    }

    // Test 3: Call again with a smaller life version.
    // V1: Expects ID_Undeleted. Without the recentBlobCache, no exception is thrown and the requests hits the cloud.
    // FIXME: V1 recentBlobCache is broken as mentioned above and I don't want to reason about cache-mgmt or fix it.
    // V2: Expects Life_Version_Conflict. If the recentBlobCache is present, then it absorbs the request and throws ID_Undeleted, which is wrong
    // Ref BlobStore::undelete() and ReplicaThread::applyUndelete() for correct behavior.
    messageInfo =
        new MessageInfo(messageInfo.getStoreKey(), SMALL_BLOB_SIZE, refAccountId, refContainerId, now, (short) (initialLifeVersion - 1));
    if (mockingDetails(dest).isMock()) {
      when(dest.undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class)))
          .thenThrow(new CloudStorageException("undeleted blob",
              new StoreException("undeleted blob", StoreErrorCodes.ID_Undeleted)));
    }
    undeleteMessage = new MessageInfo.Builder(messageInfo)
        .lifeVersion((short) (initialLifeVersion-1))
        .build();
    try {
      store.undelete(undeleteMessage);
      fail("Undelete is expected to throw an exception");
    } catch (StoreException ex) {
      StoreErrorCodes storeErrorCode = currentCacheLimit > 0 ? StoreErrorCodes.ID_Undeleted : StoreErrorCodes.Life_Version_Conflict;
      // The expected value must come first
      assertEquals(storeErrorCode, ex.getErrorCode());
    }
    verifyCacheHits(3 + numPuts + numDeletes + numUndelete, 2);
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(2)).undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class));
    }

    // Test 4: Call again with a higher life version.
    // V1: Should pass and return the higher life version
    // V2: Should pass and return the higher life version
    if (mockingDetails(dest).isMock()) {
      when(dest.undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class))).thenReturn(
          (short) (initialLifeVersion+2));
    }
    undeleteMessage = new MessageInfo.Builder(messageInfo)
        .lifeVersion((short) (initialLifeVersion+2))
        .build();
    // The expected value must come first
    assertEquals(initialLifeVersion+2, store.undelete(undeleteMessage));
    verifyCacheHits(4 + numPuts + numDeletes + numUndelete, 2);
    if (mockingDetails(dest).isMock()) {
      verify(dest, times(3)).undeleteBlob(any(BlobId.class), anyShort(), any(CloudUpdateValidator.class));
    }

    // Test 5: Put, delete and Undelete
    // V1: Should pass
    // V2: Should pass
    messageWriteSet = new MockMessageWriteSet();
    messageInfo =
        new MessageInfo(getUniqueId(refAccountId, refContainerId, true, partitionId), SMALL_BLOB_SIZE, refAccountId,
            refContainerId, now, initialLifeVersion);
    messageWriteSet.add(messageInfo, ByteBuffer.wrap(TestUtils.getRandomBytes((int) messageInfo.getSize())));
    store.put(messageWriteSet);
    store.delete(Collections.singletonList(messageInfo));
    undeleteMessage = new MessageInfo.Builder(messageInfo)
        .isDeleted(false)
        .isUndeleted(true)
        .lifeVersion((short) (initialLifeVersion+1))
        .build();
    assertEquals(initialLifeVersion+1, store.undelete(undeleteMessage));
  }

  /** Test the CloudBlobStore findMissingKeys method. */
  @Test
  public void testFindMissingKeys() throws Exception {
    // FIXED: Incomplete test, but fixed now.
    setupCloudStore(true, true, defaultCacheLimit, true);
    int count = 1;
    List<StoreKey> keys = new ArrayList<>();
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    long now = System.currentTimeMillis();
    short initialLifeVersion = 100;
    Map<String, CloudBlobMetadata> metadataMap = new HashMap<>();
    List<MessageInfo> missingBlobIds = new ArrayList<>();
    for (int j = 0; j < count; j++) {
      // Blobs present in cloud
      BlobId existentBlobId = getUniqueId(refAccountId, refContainerId, false, partitionId);
      MessageInfo info = new MessageInfo(existentBlobId, SMALL_BLOB_SIZE, refAccountId, refContainerId, now, initialLifeVersion);
      messageWriteSet.add(info, ByteBuffer.wrap(TestUtils.getRandomBytes(SMALL_BLOB_SIZE)));
      keys.add(existentBlobId);
      metadataMap.put(existentBlobId.getID(),
          new CloudBlobMetadata(existentBlobId, operationTime, Utils.Infinite_Time, 1024,
              CloudBlobMetadata.EncryptionOrigin.ROUTER));
      // Blobs absent in cloud
      BlobId nonexistentBlobId = getUniqueId(refAccountId, refContainerId, false, partitionId);
      info = new MessageInfo(nonexistentBlobId, SMALL_BLOB_SIZE, refAccountId, refContainerId, now, initialLifeVersion);
      missingBlobIds.add(info);
      keys.add(nonexistentBlobId);
    }

    /*
      Add some keys and test for missing keys
     */
    store.put(messageWriteSet);
    int numPuts = messageWriteSet.getMessageSetInfo().size();
    if (mockingDetails(dest).isMock()) {
      when(dest.getBlobMetadata(anyList())).thenReturn(metadataMap);
    }
    Set<StoreKey> missingKeys = store.findMissingKeys(keys);
    if (mockingDetails(dest).isMock()) {
      verify(dest).getBlobMetadata(anyList());
    }
    verifyCacheHits(keys.size() + numPuts, numPuts);
    /*
      The way to determine the correctness of findMissinKeys is to take the set diff
      ok keys we know will be missing and keys returned by findMissingKeys.
     */
    Set<String> keysPresentInCloud = metadataMap.keySet();
    Set<String> allKeySet = keys.stream().map(k -> k.getID()).collect(Collectors.toSet());
    Set<String> missingKeySet = missingKeys.stream().map(k -> k.getID()).collect(Collectors.toSet());
    missingKeySet.addAll(keysPresentInCloud);
    missingKeySet.removeAll(allKeySet);
    assertTrue(missingKeySet.isEmpty());

    /*
      Upload the missing keys and check that findMissingKeys returns empty
     */
    messageWriteSet = new MockMessageWriteSet();
    for (MessageInfo info : missingBlobIds) {
      messageWriteSet.add(info, ByteBuffer.wrap(TestUtils.getRandomBytes(SMALL_BLOB_SIZE)));
    }
    store.put(messageWriteSet);
    missingKeys = store.findMissingKeys(keys);
    assertTrue("Expected no missing keys", missingKeys.isEmpty());
    verifyCacheHits(messageWriteSet.getMessageSetInfo().size() + keys.size() + keys.size() + numPuts,
         keys.size() + numPuts);
    if (mockingDetails(dest).isMock()) {
      // getBlobMetadata should not have been called a second time.
      verify(dest).getBlobMetadata(anyList());
    }
  }

  /** Test the CloudBlobStore findEntriesSince method. */
  @Test
  public void testFindEntriesSince() throws Exception {
    v1TestOnly();
    setupCloudStore(false, true, defaultCacheLimit, true);
    long maxTotalSize = 1000000;
    // 1) start with empty token, call find, return some data
    long blobSize = 200000;
    int numBlobsFound = 5;
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken =
        new CosmosChangeFeedFindToken(blobSize * numBlobsFound, "start", "end", 0, numBlobsFound,
            UUID.randomUUID().toString());
    //create a list of 10 blobs with total size less than maxSize, and return it as part of query ChangeFeed
    when(dest.findEntriesSince(anyString(), any(CosmosChangeFeedFindToken.class), anyLong())).thenReturn(
        new FindResult(Collections.emptyList(), cosmosChangeFeedFindToken));
    CosmosChangeFeedFindToken startToken = new CosmosChangeFeedFindToken();
    // remote node host name and replica path are not really used by cloud store, it's fine to keep them null
    FindInfo findInfo = store.findEntriesSince(startToken, maxTotalSize, null, null);
    CosmosChangeFeedFindToken outputToken = (CosmosChangeFeedFindToken) findInfo.getFindToken();
    assertEquals(blobSize * numBlobsFound, outputToken.getBytesRead());
    assertEquals(numBlobsFound, outputToken.getTotalItems());
    assertEquals(0, outputToken.getIndex());

    // 2) call find with new token, return more data including lastBlob, verify token updated
    cosmosChangeFeedFindToken =
        new CosmosChangeFeedFindToken(blobSize * 2 * numBlobsFound, "start2", "end2", 0, numBlobsFound,
            UUID.randomUUID().toString());
    when(dest.findEntriesSince(anyString(), any(CosmosChangeFeedFindToken.class), anyLong())).thenReturn(
        new FindResult(Collections.emptyList(), cosmosChangeFeedFindToken));
    findInfo = store.findEntriesSince(outputToken, maxTotalSize, null, null);
    outputToken = (CosmosChangeFeedFindToken) findInfo.getFindToken();
    assertEquals(blobSize * 2 * numBlobsFound, outputToken.getBytesRead());
    assertEquals(numBlobsFound, outputToken.getTotalItems());
    assertEquals(0, outputToken.getIndex());

    // 3) call find with new token, no more data, verify token unchanged
    when(dest.findEntriesSince(anyString(), any(CosmosChangeFeedFindToken.class), anyLong())).thenReturn(
        new FindResult(Collections.emptyList(), outputToken));
    findInfo = store.findEntriesSince(outputToken, maxTotalSize, null, null);
    assertTrue(findInfo.getMessageEntries().isEmpty());
    FindToken finalToken = findInfo.getFindToken();
    assertEquals(outputToken, finalToken);
  }

  /** Test CloudBlobStore cache eviction. */
  @Test
  public void testCacheEvictionOrder() throws Exception {
    v1TestOnly();
    assumeTrue(isVcr);

    // setup store with small cache size
    int cacheSize = 10;
    setupCloudStore(false, false, cacheSize, true);
    // put blobs to fill up cache
    List<StoreKey> blobIdList = new ArrayList<>();
    for (int j = 0; j < cacheSize; j++) {
      blobIdList.add(getUniqueId(refAccountId, refContainerId, false, partitionId));
      store.addToCache(blobIdList.get(j).getID(), (short) 0, CloudBlobStore.BlobState.CREATED);
    }

    // findMissingKeys should stay in cache
    store.findMissingKeys(blobIdList);
    verify(dest, never()).getBlobMetadata(anyList());
    int expectedLookups = blobIdList.size();
    int expectedHits = expectedLookups;
    verifyCacheHits(expectedLookups, expectedHits);
    // Perform access on first 5 blobs
    int delta = 5;
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    for (int j = 0; j < delta; j++) {
      CloudTestUtil.addBlobToMessageSet(messageWriteSet, (BlobId) blobIdList.get(j), SMALL_BLOB_SIZE,
          Utils.Infinite_Time, operationTime, isVcr);
    }
    store.updateTtl(messageWriteSet.getMessageSetInfo());
    expectedLookups += delta;
    // Note: should be cache misses since blobs are still in CREATED state.
    verifyCacheHits(expectedLookups, expectedHits);

    // put 5 more blobs
    for (int j = cacheSize; j < cacheSize + delta; j++) {
      blobIdList.add(getUniqueId(refAccountId, refContainerId, false, partitionId));
      store.addToCache(blobIdList.get(j).getID(), (short) 0, CloudBlobStore.BlobState.CREATED);
    }
    // get same 1-5 which should be still cached.
    store.findMissingKeys(blobIdList.subList(0, delta));
    expectedLookups += delta;
    expectedHits += delta;
    verifyCacheHits(expectedLookups, expectedHits);
    verify(dest, never()).getBlobMetadata(anyList());
    // call findMissingKeys on 6-10 which should trigger getBlobMetadata
    store.findMissingKeys(blobIdList.subList(delta, cacheSize));
    expectedLookups += delta;
    verifyCacheHits(expectedLookups, expectedHits);
    verify(dest).getBlobMetadata(anyList());
  }

  /**
   * Verify that the cache stats are expected.
   * @param expectedLookups Expected cache lookup count.
   * @param expectedHits Expected cache hit count.
   */
  private void verifyCacheHits(int expectedLookups, int expectedHits) {
    if (currentCacheLimit == 0) {
      return;
    }
    assertEquals("Cache lookup count", expectedLookups, vcrMetrics.blobCacheLookupCount.getCount());
    if (isVcr) {
      assertEquals("Cache hit count", expectedHits, vcrMetrics.blobCacheHitCount.getCount());
    } else {
      //assertEquals("Expected no cache lookups (not VCR)", 0, vcrMetrics.blobCacheLookupCount.getCount());
      assertEquals("Expected no cache hits (not VCR)", 0, vcrMetrics.blobCacheHitCount.getCount());
    }
  }

  /** Test verifying behavior when store not started. */
  @Test
  public void testStoreNotStarted() throws Exception {
    v1TestOnly();
    // Create store and don't start it.
    setupCloudStore(false, true, defaultCacheLimit, false);
    List<StoreKey> keys = Collections.singletonList(getUniqueId(refAccountId, refContainerId, false, partitionId));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    CloudTestUtil.addBlobToMessageSet(messageWriteSet, 10, Utils.Infinite_Time, refAccountId, refContainerId, true,
        false, partitionId, operationTime, isVcr);
    try {
      store.put(messageWriteSet);
      fail("Store put should have failed.");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }
    try {
      store.delete(messageWriteSet.getMessageSetInfo());
      fail("Store delete should have failed.");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }
    try {
      store.findMissingKeys(keys);
      fail("Store findMissingKeys should have failed.");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.Store_Not_Started, e.getErrorCode());
    }
  }

  /** Test verifying exception handling behavior. */
  @Test
  public void testExceptionalDest() throws Exception {
    v1TestOnly();
    CloudDestination exDest = mock(CloudDestination.class);
    when(exDest.uploadBlob(any(BlobId.class), anyLong(), any(), any(InputStream.class))).thenThrow(
        new CloudStorageException("ouch"));
    when(exDest.deleteBlob(any(BlobId.class), anyLong(), anyShort(), any(CloudUpdateValidator.class))).thenThrow(
        new CloudStorageException("ouch"));
    when(exDest.getBlobMetadata(anyList())).thenThrow(new CloudStorageException("ouch"));
    Properties props = new Properties();
    setBasicProperties(props);
    props.setProperty(CloudConfig.VCR_REQUIRE_ENCRYPTION, "false");
    props.setProperty(CloudConfig.CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS,
        TestCloudBlobCryptoAgentFactory.class.getName());
    vcrMetrics = new VcrMetrics(new MetricRegistry());
    CloudBlobStore exStore =
        new CloudBlobStore(new VerifiableProperties(props), partitionId, exDest, clusterMap, vcrMetrics);
    exStore.start();
    List<StoreKey> keys = Collections.singletonList(getUniqueId(refAccountId, refContainerId, false, partitionId));
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    CloudTestUtil.addBlobToMessageSet(messageWriteSet, 10, Utils.Infinite_Time, refAccountId, refContainerId, true,
        false, partitionId, operationTime, isVcr);
    try {
      exStore.put(messageWriteSet);
      fail("Store put should have failed.");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.IOError, e.getErrorCode());
    }
    try {
      exStore.delete(messageWriteSet.getMessageSetInfo());
      fail("Store delete should have failed.");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.IOError, e.getErrorCode());
    }
    try {
      exStore.findMissingKeys(keys);
      fail("Store findMissingKeys should have failed.");
    } catch (StoreException e) {
      assertEquals(StoreErrorCodes.IOError, e.getErrorCode());
    }
  }

  /* Test retry behavior */
  @Test
  public void testExceptionRetry() throws Exception {
    v1TestOnly();
    CloudDestination exDest = mock(CloudDestination.class);
    long retryDelay = 5;
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    BlobId blobId =
        CloudTestUtil.addBlobToMessageSet(messageWriteSet, SMALL_BLOB_SIZE, Utils.Infinite_Time, refAccountId,
            refContainerId, false, false, partitionId, operationTime, isVcr);
    List<StoreKey> keys = Collections.singletonList(blobId);
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, operationTime, Utils.Infinite_Time, 1024, null);
    CloudStorageException retryableException =
        new CloudStorageException("Server unavailable", null, 500, true, retryDelay);
    when(exDest.uploadBlob(any(BlobId.class), anyLong(), any(), any(InputStream.class)))
        // First call, consume inputstream and throw exception
        .thenAnswer((invocation -> {
          consumeStream(invocation);
          throw retryableException;
        }))
        // On second call, consume stream again to make sure we can
        .thenAnswer(invocation -> {
          consumeStream(invocation);
          return true;
        });
    when(exDest.deleteBlob(any(BlobId.class), anyLong(), anyShort(), any(CloudUpdateValidator.class))).thenThrow(
        retryableException).thenReturn(true);
    when(exDest.updateBlobExpiration(any(BlobId.class), anyLong(), any(CloudUpdateValidator.class))).thenThrow(
        retryableException).thenReturn((short) 1);
    when(exDest.getBlobMetadata(anyList())).thenThrow(retryableException)
        .thenReturn(Collections.singletonMap(metadata.getId(), metadata));
    doThrow(retryableException).doNothing().when(exDest).downloadBlob(any(BlobId.class), any());
    Properties props = new Properties();
    setBasicProperties(props);
    props.setProperty(CloudConfig.VCR_REQUIRE_ENCRYPTION, "false");
    props.setProperty(CloudConfig.CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS,
        TestCloudBlobCryptoAgentFactory.class.getName());
    vcrMetrics = new VcrMetrics(new MetricRegistry());
    CloudBlobStore exStore =
        spy(new CloudBlobStore(new VerifiableProperties(props), partitionId, exDest, clusterMap, vcrMetrics));
    exStore.start();

    // Run all operations, they should be retried and succeed second time.
    int expectedCacheLookups = 0, expectedCacheRemoves = 0, expectedRetries = 0;
    // PUT
    exStore.put(messageWriteSet);
    expectedRetries++;
    expectedCacheLookups++;
    // UPDATE_TTL
    exStore.updateTtl(messageWriteSet.getMessageSetInfo());
    expectedRetries++;
    expectedCacheRemoves++;
    expectedCacheLookups += 2;
    // DELETE
    exStore.delete(messageWriteSet.getMessageSetInfo());
    expectedRetries++;
    expectedCacheRemoves++;
    if (isVcr) {
      // no cache lookup in case of delete for frontend.
      expectedCacheLookups += 2;
    }
    // GET
    exStore.get(keys, EnumSet.noneOf(StoreGetOptions.class));
    expectedRetries++;
    exStore.downloadBlob(metadata, blobId, new ByteArrayOutputStream());
    expectedRetries++;
    // Expect retries for all ops, cache lookups for the first three
    assertEquals("Unexpected retry count", expectedRetries, vcrMetrics.retryCount.getCount());
    assertEquals("Unexpected wait time", expectedRetries * retryDelay, vcrMetrics.retryWaitTimeMsec.getCount());
    verifyCacheHits(expectedCacheLookups, 0);
    verify(exStore, times(expectedCacheRemoves)).removeFromCache(eq(blobId.getID()));

    // Rerun the first three, should all skip due to cache hit
    messageWriteSet.resetBuffers();
    int expectedCacheHits = 0;
    try {
      exStore.put(messageWriteSet);
    } catch (StoreException ex) {
      assertEquals(ex.getErrorCode(), StoreErrorCodes.Already_Exist);
    }
    expectedCacheHits++;
    exStore.addToCache(blobId.getID(), (short) 0, CloudBlobStore.BlobState.TTL_UPDATED);
    exStore.updateTtl(messageWriteSet.getMessageSetInfo());
    expectedCacheHits++;
    exStore.addToCache(blobId.getID(), (short) 0, CloudBlobStore.BlobState.DELETED);
    try {
      exStore.delete(messageWriteSet.getMessageSetInfo());
    } catch (StoreException ex) {
      assertEquals(ex.getErrorCode(), StoreErrorCodes.ID_Deleted);
    }
    if (isVcr) {
      expectedCacheHits++;
    }
    verifyCacheHits(expectedCacheLookups + expectedCacheHits, expectedCacheHits);
  }

  /**
   * Consume the input stream passed to the uploadBlob mock invocation.
   * @param invocation
   * @throws IOException
   */
  private void consumeStream(InvocationOnMock invocation) throws IOException {
    long size = invocation.getArgument(1);
    InputStream inputStream = invocation.getArgument(3);
    for (int j = 0; j < size; j++) {
      if (inputStream.read() == -1) {
        throw new EOFException("end of stream");
      }
    }
  }

  /**
   * Test PUT(with TTL) and TtlUpdate record replication.
   * Replication may happen after PUT and after TtlUpdate, or after TtlUpdate only.
   * PUT may already expired, expiration time < upload threshold or expiration time >= upload threshold.
   * @throws Exception
   */
  @Test
  public void testPutWithTtl() throws Exception {
    // Set up remote host
    MockClusterMap clusterMap = new MockClusterMap();
    MockHost remoteHost = getLocalAndRemoteHosts(clusterMap).getSecond();
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    PartitionId partitionId = partitionIds.get(0);
    StoreKeyFactory storeKeyFactory = new BlobIdFactory(clusterMap);
    MockStoreKeyConverterFactory storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    storeKeyConverterFactory.setReturnInputIfAbsent(true);
    MockStoreKeyConverterFactory.MockStoreKeyConverter storeKeyConverter =
        storeKeyConverterFactory.getStoreKeyConverter();
    Transformer transformer = new BlobIdTransformer(storeKeyFactory, storeKeyConverter);
    Map<DataNodeId, MockHost> hosts = new HashMap<>();
    hosts.put(remoteHost.dataNodeId, remoteHost);

    // Generate BlobIds for following PUT.
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    boolean toEncrypt = TestUtils.RANDOM.nextBoolean();
    List<BlobId> blobIdList = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      blobIdList.add(
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
              partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK));
    }

    // Set up VCR
    Properties props = new Properties();
    setBasicProperties(props);
    props.setProperty("clustermap.port", "12300");
    props.setProperty("vcr.ssl.port", "12345");

    ReplicationConfig replicationConfig = new ReplicationConfig(new VerifiableProperties(props));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(props));
    CloudDataNode cloudDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    MockFindTokenHelper findTokenHelper = new MockFindTokenHelper(storeKeyFactory, replicationConfig);
    MockNetworkClient mockNetworkClient =
        (MockNetworkClient) new MockNetworkClientFactory(hosts, clusterMap, 4, findTokenHelper).getNetworkClient();
    MetricRegistry metricRegistry = new MetricRegistry();
    if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_1)) {
      dest =
          new LatchBasedInMemoryCloudDestination(blobIdList, clusterMap);
    } else if (ambryBackupVersion.equals(CloudConfig.AMBRY_BACKUP_VERSION_2)) {
      props.setProperty(AzureCloudConfig.AZURE_NAME_SCHEME_VERSION, "1");
      props.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "PARTITION");
      props.setProperty(CloudConfig.CLOUD_MAX_ATTEMPTS, "1");
      props.setProperty(CloudConfig.CLOUD_RECENT_BLOB_CACHE_LIMIT, String.valueOf(0));
      dest = new AzuriteUtils().getAzuriteClient(props, metricRegistry, clusterMap);
    }

    CloudReplica cloudReplica = new CloudReplica(partitionId, cloudDataNode);
    CloudBlobStore cloudBlobStore =
        new CloudBlobStore(new VerifiableProperties(props), partitionId, dest, clusterMap,
            new VcrMetrics(metricRegistry));
    cloudBlobStore.start();

    // Create ReplicaThread and add RemoteReplicaInfo to it.
    ReplicationMetrics replicationMetrics = new ReplicationMetrics(new MetricRegistry(), Collections.emptyList());
    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", findTokenHelper, clusterMap, new AtomicInteger(0), cloudDataNode,
            mockNetworkClient, replicationConfig, replicationMetrics, null, storeKeyConverter, transformer,
            clusterMap.getMetricRegistry(), false, cloudDataNode.getDatacenterName(), new ResponseHandler(clusterMap),
            new MockTime(), null, null, null);

    for (ReplicaId replica : partitionId.getReplicaIds()) {
      if (replica.getDataNodeId() == remoteHost.dataNodeId) {
        RemoteReplicaInfo remoteReplicaInfo =
            new RemoteReplicaInfo(replica, cloudReplica, cloudBlobStore, new MockFindToken(0, 0), Long.MAX_VALUE,
                SystemTime.getInstance(), new Port(remoteHost.dataNodeId.getPort(), PortType.PLAINTEXT));
        replicaThread.addRemoteReplicaInfo(remoteReplicaInfo);
        break;
      }
    }

    long referenceTime = System.currentTimeMillis();
    // Case 1: Put already expired. Replication happens after Put and after TtlUpdate.
    // Upload to Cloud only after replicating ttlUpdate.
    BlobId id = blobIdList.get(0);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime - 2000, referenceTime - 1000);
    replicaThread.replicate();
    assertFalse("Blob should not exist.", dest.doesBlobExist(id));
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", dest.doesBlobExist(id));

    // Case 2: Put already expired. Replication happens after TtlUpdate.
    // Upload to Cloud only after replicating ttlUpdate.
    id = blobIdList.get(1);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime - 2000, referenceTime - 1000);
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", dest.doesBlobExist(id));

    // Case 3: Put TTL less than cloudConfig.vcrMinTtlDays. Replication happens after Put and after TtlUpdate.
    // Upload to Cloud only after replicating ttlUpdate.
    id = blobIdList.get(2);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays) - 1);
    replicaThread.replicate();
    if (isVcr) {
      assertFalse("Blob should not exist (vcr).", dest.doesBlobExist(id));
    } else {
      assertTrue("Blob should exist (not vcr).", dest.doesBlobExist(id));
    }
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", dest.doesBlobExist(id));

    // Case 4: Put TTL less than cloudConfig.vcrMinTtlDays. Replication happens after TtlUpdate.
    // Upload to Cloud only after replicating ttlUpdate.
    id = blobIdList.get(3);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays) - 1);
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", dest.doesBlobExist(id));

    // Case 5: Put TTL greater than or equals to cloudConfig.vcrMinTtlDays. Replication happens after Put and after TtlUpdate.
    // Upload to Cloud after Put and update ttl after TtlUpdate.
    id = blobIdList.get(4);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays));
    replicaThread.replicate();
    assertTrue(dest.doesBlobExist(id));
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", dest.doesBlobExist(id));

    // Case 6: Put TTL greater than or equals to cloudConfig.vcrMinTtlDays. Replication happens after TtlUpdate.
    // Upload to Cloud after TtlUpdate.
    id = blobIdList.get(5);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays));
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", dest.doesBlobExist(id));

    // Verify expiration time of all blobs.
    Map<String, CloudBlobMetadata> map = dest.getBlobMetadata(blobIdList);
    for (BlobId blobId : blobIdList) {
      assertEquals("Blob ttl should be infinite now.", Utils.Infinite_Time,
          map.get(blobId.toString()).getExpirationTime());
    }
  }

  @Test
  public void testCreateTableEntity() throws ReflectiveOperationException {
    v2TestOnly();
    setupCloudStore(false, false, 0, true);
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();

    /*
      Add some permanent blobs.
      set isVcr = true so that the lifeVersion is 0, else it is -1
     */
    int numBlobs = 10;
    IntStream.range(0,numBlobs).forEach(i -> CloudTestUtil.addBlobToMessageSet(messageWriteSet, 1024,
        Utils.Infinite_Time, refAccountId, refContainerId, false, false, partitionId,
        System.currentTimeMillis(), isVcr));

    // Test constants
    final String CORRUPT_BLOB_IDS = "corruptBlobIds";
    final String ROW_KEY = "localhost";
    final String PROPERTY = "replicaPath";
    final String VALUE = "/mnt/disk/1234";

    // Clear table
    TableClient tableClient = ((AzureCloudDestinationSync) dest).getTableClient(CORRUPT_BLOB_IDS);
    // Don't delete the table, because getTableClient populates its cache with tableClient.
    // If we delete the table, then the cached ref is dangling.
    tableClient.listEntities().forEach(tableEntity -> tableClient.deleteEntity(tableEntity));
    assertEquals(0, tableClient.listEntities().stream().count());

    // Add some rows to the table
    // TableEntity = (partition-key, row-key)
    // Add several times to assert row is added only once
    messageWriteSet.getMessageSetInfo().forEach(messageInfo ->
        IntStream.range(0,3).forEach(i ->
            dest.createTableEntity(CORRUPT_BLOB_IDS,
                new TableEntity(messageInfo.getStoreKey().getID(), ROW_KEY).addProperty(PROPERTY, VALUE))));
    assertEquals(numBlobs, tableClient.listEntities().stream().count());

    // Check rows are as expected
    messageWriteSet.getMessageSetInfo().forEach(messageInfo ->
        assertEquals(VALUE,
            tableClient.getEntity(messageInfo.getStoreKey().getID(), ROW_KEY).getProperties().get(PROPERTY)));

    // Can delete table now
    tableClient.deleteTable();
  }

  /**
   * Test cloud store get method.
   * @throws Exception
   */
  @Test
  public void testStoreGets() throws Exception {
    v1TestOnly();
    testStoreGets(false);
    testStoreGets(true);
  }

  /**
   * Test cloud store get method with the given encryption requirement.
   * @throws Exception
   */
  private void testStoreGets(boolean requireEncryption) throws Exception {
    setupCloudStore(true, requireEncryption, defaultCacheLimit, true);
    // Put blobs with and without expiration and encryption
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 5;
    List<BlobId> blobIds = new ArrayList<>(count);
    Map<BlobId, ByteBuffer> blobIdToUploadedDataMap = new HashMap<>(count);
    for (int j = 0; j < count; j++) {
      long size = Math.abs(random.nextLong()) % 10000;
      blobIds.add(
          CloudTestUtil.addBlobToMessageSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId,
              false, false, partitionId, operationTime, isVcr));
      ByteBuffer uploadedData = messageWriteSet.getBuffers().get(messageWriteSet.getMessageSetInfo().size() - 1);
      blobIdToUploadedDataMap.put(blobIds.get(j), uploadedData);
    }
    store.put(messageWriteSet);

    for (BlobId blobId : blobIdToUploadedDataMap.keySet()) {
      blobIdToUploadedDataMap.get(blobId).flip();
    }

    //test get for a list of blobs that exist
    testGetForExistingBlobs(blobIds, blobIdToUploadedDataMap);

    //test get for one blob that exists
    List<BlobId> singleIdList = Collections.singletonList(blobIds.get(0));
    Map<BlobId, ByteBuffer> singleBlobIdToUploadedDataMap = new HashMap<>(1);
    singleBlobIdToUploadedDataMap.put(blobIds.get(0), blobIdToUploadedDataMap.get(blobIds.get(0)));
    testGetForExistingBlobs(singleIdList, singleBlobIdToUploadedDataMap);

    //test get for one blob that doesnt exist
    BlobId nonExistentId = getUniqueId(refAccountId, refContainerId, false, partitionId);
    singleIdList = Collections.singletonList(nonExistentId);
    testGetWithAtleastOneNonExistentBlob(singleIdList);

    //test get with one blob in list non-existent
    blobIds.add(nonExistentId);
    testGetWithAtleastOneNonExistentBlob(blobIds);
    blobIds.remove(blobIds.remove(blobIds.size() - 1));

    //test get for deleted blob
    MessageInfo deletedMessageInfo = messageWriteSet.getMessageSetInfo().get(0);
    MockMessageWriteSet deleteMockMessageWriteSet = new MockMessageWriteSet();
    deleteMockMessageWriteSet.add(deletedMessageInfo, null);
    store.delete(deleteMockMessageWriteSet.getMessageSetInfo());

    //test get with a deleted blob in blob list to get, but {@code StoreGetOptions.Store_Include_Deleted} not set in get options
    testGetForDeletedBlobWithoutIncludeDeleteOption(blobIds);

    //test get with a deleted blob in blob list to get, and {@code StoreGetOptions.Store_Include_Deleted} set in get options
    testGetForDeletedBlobWithIncludeDeleteOption(blobIds, (BlobId) deletedMessageInfo.getStoreKey());

    blobIds.remove(0);

    //test get for expired blob
    BlobId expiredBlobId = forceUploadExpiredBlob();
    blobIds.add(expiredBlobId);

    //test get with an expired blob in blob list, but {@code StoreGetOptions.Store_Include_Expired} not set in get options
    testGetForDeletedBlobWithoutIncludeExpiredOption(blobIds);

    //test get with a expired blob in blob list to get, and {@code StoreGetOptions.Store_Include_Expired} set in get options
    testGetForDeletedBlobWithIncludeExpiredOption(blobIds, expiredBlobId);
  }

  /**
   * Test cloud store get with a list of blobs that are all valid and previously uploaded in the store
   * @param blobIds list of blob ids to get
   * @param blobIdToUploadedDataMap map of expected blobid to data buffers
   * @throws Exception
   */
  private void testGetForExistingBlobs(List<BlobId> blobIds, Map<BlobId, ByteBuffer> blobIdToUploadedDataMap)
      throws Exception {
    StoreInfo storeInfo = store.get(blobIds, EnumSet.noneOf(StoreGetOptions.class));
    assertEquals("Number of records returned by get should be same as uploaded",
        storeInfo.getMessageReadSetInfo().size(), blobIds.size());
    for (int i = 0; i < storeInfo.getMessageReadSetInfo().size(); i++) {
      MessageInfo messageInfo = storeInfo.getMessageReadSetInfo().get(i);
      if (blobIdToUploadedDataMap.containsKey(messageInfo.getStoreKey())) {
        ByteBuffer uploadedData = blobIdToUploadedDataMap.get(messageInfo.getStoreKey());
        ByteBuffer downloadedData = ByteBuffer.allocate((int) messageInfo.getSize());
        WritableByteChannel writableByteChannel = Channels.newChannel(new ByteBufferOutputStream(downloadedData));
        storeInfo.getMessageReadSet().writeTo(i, writableByteChannel, 0, messageInfo.getSize());
        downloadedData.flip();
        assertEquals(uploadedData, downloadedData);
        break;
      }
    }
  }

  /**
   * Test cloud store get with a list of blobs such that atleast one of the blobs is not present in the store
   * @param blobIds list of blobids to get
   */
  private void testGetWithAtleastOneNonExistentBlob(List<BlobId> blobIds) {
    try {
      store.get(blobIds, EnumSet.noneOf(StoreGetOptions.class));
      fail("get with any non existent id should fail");
    } catch (StoreException e) {
      assertEquals("get with any non existent blob id should throw exception with " + StoreErrorCodes.ID_Not_Found
          + " error code.", e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
    }
  }

  /**
   * Test cloud store get with a list of blobs such that atleast one of them is deleted, but {@code StoreGetOptions.Store_Include_Deleted} is not set in getoptions
   * @param blobIds list of blobids to get
   */
  private void testGetForDeletedBlobWithoutIncludeDeleteOption(List<BlobId> blobIds) {
    try {
      store.get(blobIds, EnumSet.noneOf(StoreGetOptions.class));
      fail("get should fail for a deleted blob, if StoreGetOptions.Store_Include_Deleted is not set in get options");
    } catch (StoreException e) {
      assertEquals(
          "get for deleted blob with with Store_Include_Deleted not set in get options should throw exception with "
              + StoreErrorCodes.ID_Deleted + " error code.", e.getErrorCode(), StoreErrorCodes.ID_Deleted);
    }
  }

  /**
   * Test cloud store get with a list of blobs such that atleast one of them is deleted, and {@code StoreGetOptions.Store_Include_Deleted} is set in getoptions
   * @param blobIds list of blobids to get
   * @param deletedBlobId blobId which is marked as deleted
   * @throws Exception
   */
  private void testGetForDeletedBlobWithIncludeDeleteOption(List<BlobId> blobIds, BlobId deletedBlobId)
      throws Exception {
    StoreInfo storeInfo = store.get(blobIds, EnumSet.of(StoreGetOptions.Store_Include_Deleted));
    for (MessageInfo msgInfo : storeInfo.getMessageReadSetInfo()) {
      if (msgInfo.getStoreKey().equals(deletedBlobId)) {
        return;
      }
    }
    fail("get should be successful for a deleted blob with Store_Include_Deleted set in get options");
  }

  /**
   * Test cloud store get with a list of blobs such that atleast one of them is expired, but {@code StoreGetOptions.Store_Include_Expired} is not set in getoptions
   * @param blobIds list of blobids to get
   */
  private void testGetForDeletedBlobWithoutIncludeExpiredOption(List<BlobId> blobIds) {
    try {
      store.get(blobIds, EnumSet.noneOf(StoreGetOptions.class));
      fail("get should fail for a expired blob, if StoreGetOptions.Store_Include_Expired is not set in get options");
    } catch (StoreException e) {
      assertEquals(
          "get for expired blob with with StoreGetOptions.Store_Include_Expired not set in get options, should throw exception with "
              + StoreErrorCodes.TTL_Expired + " error code.", e.getErrorCode(), StoreErrorCodes.TTL_Expired);
    }
  }

  /**
   * Test cloud store get with a list of blobs such that atleast one of them is expired, and {@code StoreGetOptions.Store_Include_Expired} is set in getoptions
   * @param blobIds list of blobids to get
   * @param expiredBlobId expired blob id
   * @throws Exception
   */
  private void testGetForDeletedBlobWithIncludeExpiredOption(List<BlobId> blobIds, BlobId expiredBlobId)
      throws Exception {
    StoreInfo storeInfo = store.get(blobIds, EnumSet.of(StoreGetOptions.Store_Include_Expired));
    for (MessageInfo msgInfo : storeInfo.getMessageReadSetInfo()) {
      if (msgInfo.getStoreKey().equals(expiredBlobId)) {
        return;
      }
    }
    fail("get should be successful for a expired blob with Store_Include_Expired set in get options");
  }

  /**
   * Force upload an expired blob to cloud destination by directly calling {@code CloudDestination}'s {@code uploadBlob} method to avoid expiry checks during upload.
   * @return Blobid uploaded
   * @throws CloudStorageException if upload fails
   */
  private BlobId forceUploadExpiredBlob() throws CloudStorageException {
    BlobId expiredBlobId = getUniqueId(refAccountId, refContainerId, false, partitionId);
    long size = SMALL_BLOB_SIZE;
    long currentTime = System.currentTimeMillis();
    CloudBlobMetadata expiredBlobMetadata =
        new CloudBlobMetadata(expiredBlobId, currentTime, currentTime - 1, size, null);
    ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) size));
    InputStream inputStream = new ByteBufferInputStream(buffer);
    dest.uploadBlob(expiredBlobId, size, expiredBlobMetadata, inputStream);
    return expiredBlobId;
  }

  /**
   * @return -1 if not vcr. 0 otherwise.
   */
  private short initLifeVersion() {
    return (short) (isVcr ? 0 : -1);
  }
}
