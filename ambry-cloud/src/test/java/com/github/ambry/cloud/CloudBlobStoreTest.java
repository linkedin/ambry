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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.CosmosChangeFeedFindToken;
import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
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
import com.github.ambry.replication.MockConnectionPool;
import com.github.ambry.replication.MockFindToken;
import com.github.ambry.replication.MockFindTokenHelper;
import com.github.ambry.replication.MockHost;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.commons.BlobId.*;
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

  /**
   * Run in both VCR and live serving mode.
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public CloudBlobStoreTest(boolean isVcr) throws Exception {
    this.isVcr = isVcr;
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
      throws Exception {
    Properties properties = new Properties();
    // Required clustermap properties
    setBasicProperties(properties);
    // Require encryption for uploading
    properties.setProperty(CloudConfig.VCR_REQUIRE_ENCRYPTION, Boolean.toString(requireEncryption));
    properties.setProperty(CloudConfig.CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS,
        TestCloudBlobCryptoAgentFactory.class.getName());
    properties.setProperty(CloudConfig.CLOUD_RECENT_BLOB_CACHE_LIMIT, String.valueOf(cacheLimit));
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    dest = inMemoryDestination ? new LatchBasedInMemoryCloudDestination(Collections.emptyList())
        : mock(CloudDestination.class);
    vcrMetrics = new VcrMetrics(new MetricRegistry());
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
    int count = 5;
    int expectedUploads = 0;
    long expectedBytesUploaded = 0;
    int expectedEncryptions = 0;
    for (int j = 0; j < count; j++) {
      long size = Math.abs(random.nextLong()) % 10000;
      // Permanent and encrypted, should be uploaded and not reencrypted
      addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, true);
      expectedUploads++;
      expectedBytesUploaded += size;
      // Permanent and unencrypted
      addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, false);
      expectedUploads++;
      expectedBytesUploaded += size;
      if (requireEncryption) {
        expectedEncryptions++;
      }
    }
    store.put(messageWriteSet);
    LatchBasedInMemoryCloudDestination inMemoryDest = (LatchBasedInMemoryCloudDestination) dest;
    assertEquals("Unexpected blobs count", expectedUploads, inMemoryDest.getBlobsUploaded());
    assertEquals("Unexpected byte count", expectedBytesUploaded, inMemoryDest.getBytesUploaded());
    assertEquals("Unexpected encryption count", expectedEncryptions, vcrMetrics.blobEncryptionCount.getCount());
    verifyCacheHits(expectedUploads, 0);

    // Try to put the same blobs again (e.g. from another replica).
    // If isVcr is true, they should already be cached.
    messageWriteSet.resetBuffers();
    store.put(messageWriteSet);
    int expectedSkips = isVcr ? expectedUploads : 0;
    assertEquals("Unexpected blobs count", expectedUploads, inMemoryDest.getBlobsUploaded());
    assertEquals("Unexpected byte count", expectedBytesUploaded, inMemoryDest.getBytesUploaded());
    assertEquals("Unexpected skipped count", expectedSkips, vcrMetrics.blobUploadSkippedCount.getCount());
    verifyCacheHits(2 * expectedUploads, expectedUploads);
  }

  /** Test the CloudBlobStore delete method. */
  @Test
  public void testStoreDeletes() throws Exception {
    setupCloudStore(false, true, defaultCacheLimit, true);
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 10;
    for (int j = 0; j < count; j++) {
      addBlobToSet(messageWriteSet, SMALL_BLOB_SIZE, Utils.Infinite_Time, refAccountId, refContainerId, true);
    }
    store.delete(messageWriteSet.getMessageSetInfo());
    verify(dest, times(count)).deleteBlob(any(BlobId.class), eq(operationTime));
    verifyCacheHits(count, 0);

    // Call second time.  If isVcr, should be cached causing deletions to be skipped.
    store.delete(messageWriteSet.getMessageSetInfo());
    int expectedCount = isVcr ? count : count * 2;
    verify(dest, times(expectedCount)).deleteBlob(any(BlobId.class), eq(operationTime));
    verifyCacheHits(count * 2, count);
  }

  /** Test the CloudBlobStore updateTtl method. */
  @Test
  public void testStoreTtlUpdates() throws Exception {
    setupCloudStore(false, true, defaultCacheLimit, true);
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 10;
    for (int j = 0; j < count; j++) {
      long expirationTime = Math.abs(random.nextLong());
      addBlobToSet(messageWriteSet, SMALL_BLOB_SIZE, expirationTime, refAccountId, refContainerId, true);
    }
    store.updateTtl(messageWriteSet.getMessageSetInfo());
    verify(dest, times(count)).updateBlobExpiration(any(BlobId.class), anyLong());
    verifyCacheHits(count, 0);

    // Call second time, If isVcr, should be cached causing updates to be skipped.
    store.updateTtl(messageWriteSet.getMessageSetInfo());
    int expectedCount = isVcr ? count : count * 2;
    verify(dest, times(expectedCount)).updateBlobExpiration(any(BlobId.class), anyLong());
    verifyCacheHits(count * 2, count);
  }

  /** Test the CloudBlobStore findMissingKeys method. */
  @Test
  public void testFindMissingKeys() throws Exception {
    setupCloudStore(false, true, defaultCacheLimit, true);
    int count = 10;
    List<StoreKey> keys = new ArrayList<>();
    Map<String, CloudBlobMetadata> metadataMap = new HashMap<>();
    for (int j = 0; j < count; j++) {
      // Blob with metadata
      BlobId existentBlobId = getUniqueId();
      keys.add(existentBlobId);
      metadataMap.put(existentBlobId.getID(),
          new CloudBlobMetadata(existentBlobId, operationTime, Utils.Infinite_Time, 1024,
              CloudBlobMetadata.EncryptionOrigin.ROUTER));
      // Blob without metadata
      BlobId nonexistentBlobId = getUniqueId();
      keys.add(nonexistentBlobId);
    }
    when(dest.getBlobMetadata(anyList())).thenReturn(metadataMap);
    Set<StoreKey> missingKeys = store.findMissingKeys(keys);
    verify(dest).getBlobMetadata(anyList());
    int expectedLookups = keys.size();
    int expectedHits = 0;
    verifyCacheHits(expectedLookups, expectedHits);
    assertEquals("Wrong number of missing keys", count, missingKeys.size());

    if (isVcr) {
      // Add keys to cache and rerun (should be cached)
      for (StoreKey storeKey : keys) {
        store.addToCache(storeKey.getID(), CloudBlobStore.BlobState.CREATED);
      }
      missingKeys = store.findMissingKeys(keys);
      assertTrue("Expected no missing keys", missingKeys.isEmpty());
      expectedLookups += keys.size();
      expectedHits += keys.size();
      verifyCacheHits(expectedLookups, expectedHits);
      // getBlobMetadata should not have been called a second time.
      verify(dest).getBlobMetadata(anyList());
    }
  }

  /** Test the CloudBlobStore findEntriesSince method. */
  @Test
  public void testFindEntriesSince() throws Exception {
    setupCloudStore(false, true, defaultCacheLimit, true);
    long maxTotalSize = 1000000;
    // 1) start with empty token, call find, return some data
    long startTime = System.currentTimeMillis();
    long blobSize = 200000;
    int numBlobsFound = 5;
    List<CloudBlobMetadata> metadataList = generateMetadataList(startTime, blobSize, numBlobsFound);
    CosmosChangeFeedFindToken cosmosChangeFeedFindToken =
        new CosmosChangeFeedFindToken(blobSize * numBlobsFound, "start", "end", 0, numBlobsFound,
            UUID.randomUUID().toString());
    //create a list of 10 blobs with total size less than maxSize, and return it as part of query ChangeFeed
    when(dest.findEntriesSince(anyString(), any(CosmosChangeFeedFindToken.class), anyLong())).thenReturn(
        new FindResult(Collections.emptyList(), cosmosChangeFeedFindToken));
    CosmosChangeFeedFindToken startToken = new CosmosChangeFeedFindToken();
    FindInfo findInfo = store.findEntriesSince(startToken, maxTotalSize);
    CosmosChangeFeedFindToken outputToken = (CosmosChangeFeedFindToken) findInfo.getFindToken();
    assertEquals(blobSize * numBlobsFound, outputToken.getBytesRead());
    assertEquals(numBlobsFound, outputToken.getTotalItems());
    assertEquals(0, outputToken.getIndex());

    // 2) call find with new token, return more data including lastBlob, verify token updated
    startTime += 1000;
    metadataList = generateMetadataList(startTime, blobSize, numBlobsFound);
    cosmosChangeFeedFindToken =
        new CosmosChangeFeedFindToken(blobSize * 2 * numBlobsFound, "start2", "end2", 0, numBlobsFound,
            UUID.randomUUID().toString());
    when(dest.findEntriesSince(anyString(), any(CosmosChangeFeedFindToken.class), anyLong())).thenReturn(
        new FindResult(Collections.emptyList(), cosmosChangeFeedFindToken));
    findInfo = store.findEntriesSince(outputToken, maxTotalSize);
    outputToken = (CosmosChangeFeedFindToken) findInfo.getFindToken();
    assertEquals(blobSize * 2 * numBlobsFound, outputToken.getBytesRead());
    assertEquals(numBlobsFound, outputToken.getTotalItems());
    assertEquals(0, outputToken.getIndex());

    // 3) call find with new token, no more data, verify token unchanged
    metadataList = Collections.emptyList();
    when(dest.findEntriesSince(anyString(), any(CosmosChangeFeedFindToken.class), anyLong())).thenReturn(
        new FindResult(Collections.emptyList(), outputToken));
    findInfo = store.findEntriesSince(outputToken, maxTotalSize);
    assertTrue(findInfo.getMessageEntries().isEmpty());
    FindToken finalToken = findInfo.getFindToken();
    assertEquals(outputToken, finalToken);
  }

  /** Test CloudBlobStore cache eviction. */
  @Test
  public void testCacheEvictionOrder() throws Exception {
    assumeTrue(isVcr);

    // setup store with small cache size
    int cacheSize = 10;
    setupCloudStore(false, false, cacheSize, true);
    // put blobs to fill up cache
    List<StoreKey> blobIdList = new ArrayList<>();
    for (int j = 0; j < cacheSize; j++) {
      blobIdList.add(getUniqueId());
      store.addToCache(blobIdList.get(j).getID(), CloudBlobStore.BlobState.CREATED);
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
      addBlobToSet(messageWriteSet, (BlobId) blobIdList.get(j), SMALL_BLOB_SIZE, Utils.Infinite_Time);
    }
    store.updateTtl(messageWriteSet.getMessageSetInfo());
    expectedLookups += delta;
    // Note: should be cache misses since blobs are still in CREATED state.
    verifyCacheHits(expectedLookups, expectedHits);

    // put 5 more blobs
    for (int j = cacheSize; j < cacheSize + delta; j++) {
      blobIdList.add(getUniqueId());
      store.addToCache(blobIdList.get(j).getID(), CloudBlobStore.BlobState.CREATED);
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
    if (isVcr) {
      assertEquals("Cache lookup count", expectedLookups, vcrMetrics.blobCacheLookupCount.getCount());
      assertEquals("Cache hit count", expectedHits, vcrMetrics.blobCacheHitCount.getCount());
    } else {
      assertEquals("Expected no cache lookups (not VCR)", 0, vcrMetrics.blobCacheLookupCount.getCount());
      assertEquals("Expected no cache hits (not VCR)", 0, vcrMetrics.blobCacheHitCount.getCount());
    }
  }

  /** Test verifying behavior when store not started. */
  @Test
  public void testStoreNotStarted() throws Exception {
    // Create store and don't start it.
    setupCloudStore(false, true, defaultCacheLimit, false);
    List<StoreKey> keys = Collections.singletonList(getUniqueId());
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    addBlobToSet(messageWriteSet, 10, Utils.Infinite_Time, refAccountId, refContainerId, true);
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
    CloudDestination exDest = mock(CloudDestination.class);
    when(exDest.uploadBlob(any(BlobId.class), anyLong(), any(), any(InputStream.class))).thenThrow(
        new CloudStorageException("ouch"));
    when(exDest.deleteBlob(any(BlobId.class), anyLong())).thenThrow(new CloudStorageException("ouch"));
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
    List<StoreKey> keys = Collections.singletonList(getUniqueId());
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    addBlobToSet(messageWriteSet, 10, Utils.Infinite_Time, refAccountId, refContainerId, true);
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
    CloudDestination exDest = mock(CloudDestination.class);
    long retryDelay = 5;
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    BlobId blobId =
        addBlobToSet(messageWriteSet, SMALL_BLOB_SIZE, Utils.Infinite_Time, refAccountId, refContainerId, false);
    List<StoreKey> keys = Collections.singletonList(blobId);
    CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, operationTime, Utils.Infinite_Time, 1024, null);
    CloudStorageException retryableException =
        new CloudStorageException("Server unavailable", null, 500, true, retryDelay);
    when(exDest.uploadBlob(any(BlobId.class), anyLong(), any(), any(InputStream.class))).thenThrow(retryableException)
        .thenReturn(true);
    when(exDest.deleteBlob(any(BlobId.class), anyLong())).thenThrow(retryableException).thenReturn(true);
    when(exDest.updateBlobExpiration(any(BlobId.class), anyLong())).thenThrow(retryableException).thenReturn(true);
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
        new CloudBlobStore(new VerifiableProperties(props), partitionId, exDest, clusterMap, vcrMetrics);
    exStore.start();

    // Run all operations, they should be retried and succeed second time.
    int expectedCacheLookups = 0, expectedRetries = 0;
    exStore.put(messageWriteSet);
    expectedRetries++;
    expectedCacheLookups++;
    exStore.updateTtl(messageWriteSet.getMessageSetInfo());
    expectedRetries++;
    expectedCacheLookups += 2;
    exStore.delete(messageWriteSet.getMessageSetInfo());
    expectedRetries++;
    expectedCacheLookups += 2;
    exStore.get(keys, EnumSet.noneOf(StoreGetOptions.class));
    expectedRetries++;
    exStore.downloadBlob(metadata, blobId, new ByteArrayOutputStream());
    expectedRetries++;
    // Expect retries for all ops, cache lookups for the first three
    assertEquals("Unexpected retry count", expectedRetries, vcrMetrics.retryCount.getCount());
    assertEquals("Unexpected wait time", expectedRetries * retryDelay, vcrMetrics.retryWaitTimeMsec.getCount());
    verifyCacheHits(expectedCacheLookups, 0);

    // Rerun the first three, should all skip due to cache hit
    messageWriteSet.resetBuffers();
    int expectedCacheHits = 0;
    exStore.put(messageWriteSet);
    expectedCacheHits++;
    exStore.updateTtl(messageWriteSet.getMessageSetInfo());
    expectedCacheHits++;
    exStore.delete(messageWriteSet.getMessageSetInfo());
    expectedCacheHits++;
    verifyCacheHits(expectedCacheLookups + expectedCacheHits, expectedCacheHits);
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
    MockConnectionPool connectionPool = new MockConnectionPool(hosts, clusterMap, 4);

    // Generate BlobIds for following PUT.
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    boolean toEncrypt = TestUtils.RANDOM.nextBoolean();
    List<BlobId> blobIdList = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      blobIdList.add(
          new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, accountId,
              containerId, partitionId, toEncrypt, BlobId.BlobDataType.DATACHUNK));
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

    LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(blobIdList);
    CloudReplica cloudReplica = new CloudReplica(partitionId, cloudDataNode);
    CloudBlobStore cloudBlobStore =
        new CloudBlobStore(new VerifiableProperties(props), partitionId, latchBasedInMemoryCloudDestination, clusterMap,
            new VcrMetrics(new MetricRegistry()));
    cloudBlobStore.start();

    // Create ReplicaThread and add RemoteReplicaInfo to it.
    ReplicationMetrics replicationMetrics = new ReplicationMetrics(new MetricRegistry(), Collections.emptyList());
    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", new MockFindTokenHelper(storeKeyFactory, replicationConfig), clusterMap,
            new AtomicInteger(0), cloudDataNode, connectionPool, replicationConfig, replicationMetrics, null,
            storeKeyConverter, transformer, clusterMap.getMetricRegistry(), false, cloudDataNode.getDatacenterName(),
            new ResponseHandler(clusterMap), new MockTime(), null);

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
    assertFalse("Blob should not exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));

    // Case 2: Put already expired. Replication happens after TtlUpdate.
    // Upload to Cloud only after replicating ttlUpdate.
    id = blobIdList.get(1);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime - 2000, referenceTime - 1000);
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));

    // Case 3: Put TTL less than cloudConfig.vcrMinTtlDays. Replication happens after Put and after TtlUpdate.
    // Upload to Cloud only after replicating ttlUpdate.
    id = blobIdList.get(2);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays) - 1);
    replicaThread.replicate();
    if (isVcr) {
      assertFalse("Blob should not exist (vcr).", latchBasedInMemoryCloudDestination.doesBlobExist(id));
    } else {
      assertTrue("Blob should exist (not vcr).", latchBasedInMemoryCloudDestination.doesBlobExist(id));
    }
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));

    // Case 4: Put TTL less than cloudConfig.vcrMinTtlDays. Replication happens after TtlUpdate.
    // Upload to Cloud only after replicating ttlUpdate.
    id = blobIdList.get(3);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays) - 1);
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));

    // Case 5: Put TTL greater than or equals to cloudConfig.vcrMinTtlDays. Replication happens after Put and after TtlUpdate.
    // Upload to Cloud after Put and update ttl after TtlUpdate.
    id = blobIdList.get(4);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays));
    replicaThread.replicate();
    assertTrue(latchBasedInMemoryCloudDestination.doesBlobExist(id));
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));

    // Case 6: Put TTL greater than or equals to cloudConfig.vcrMinTtlDays. Replication happens after TtlUpdate.
    // Upload to Cloud after TtlUpdate.
    id = blobIdList.get(5);
    addPutMessagesToReplicasOfPartition(id, accountId, containerId, partitionId, Collections.singletonList(remoteHost),
        referenceTime, referenceTime + TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays));
    addTtlUpdateMessagesToReplicasOfPartition(partitionId, id, Collections.singletonList(remoteHost),
        Utils.Infinite_Time);
    replicaThread.replicate();
    assertTrue("Blob should exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));

    // Verify expiration time of all blobs.
    Map<String, CloudBlobMetadata> map = latchBasedInMemoryCloudDestination.getBlobMetadata(blobIdList);
    for (BlobId blobId : blobIdList) {
      assertEquals("Blob ttl should be infinite now.", Utils.Infinite_Time,
          map.get(blobId.toString()).getExpirationTime());
    }
  }

  /**
   * Utility method to generate a list of {@link CloudBlobMetadata} with a range of upload times.
   * @param startTime the base time for the upload time range.
   * @param blobSize the blob size.
   * @param count the list size.
   * @return the constructed list.
   */
  private List<CloudBlobMetadata> generateMetadataList(long startTime, long blobSize, int count) {
    List<CloudBlobMetadata> metadataList = new ArrayList<>();
    for (int j = 0; j < count; j++) {
      BlobId blobId = getUniqueId();
      CloudBlobMetadata metadata = new CloudBlobMetadata(blobId, startTime, Utils.Infinite_Time, blobSize,
          CloudBlobMetadata.EncryptionOrigin.NONE);
      metadata.setUploadTime(startTime + j);
      metadata.setLastUpdateTime(startTime + j);
      metadataList.add(metadata);
    }
    return metadataList;
  }

  /**
   * Utility method to generate a BlobId and byte buffer for a blob with specified properties and add them to the specified MessageWriteSet.
   * @param messageWriteSet the {@link MockMessageWriteSet} in which to store the data.
   * @param size the size of the byte buffer.
   * @param expiresAtMs the expiration time.
   * @param accountId the account Id.
   * @param containerId the container Id.
   * @param encrypted the encrypted bit.
   * @return the generated {@link BlobId}.
   */
  private BlobId addBlobToSet(MockMessageWriteSet messageWriteSet, long size, long expiresAtMs, short accountId,
      short containerId, boolean encrypted) {
    BlobId id = getUniqueId(accountId, containerId, encrypted);
    long crc = random.nextLong();
    MessageInfo info = new MessageInfo(id, size, false, true, expiresAtMs, crc, accountId, containerId, operationTime);
    ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) size));
    messageWriteSet.add(info, buffer);
    return id;
  }

  /**
   * Utility method to add a BlobId and generated byte buffer to the specified MessageWriteSet.
   * @param messageWriteSet the {@link MockMessageWriteSet} in which to store the data.
   * @param blobId the blobId to add.
   * @param size the size of the byte buffer.
   * @param expiresAtMs the expiration time.
   */
  private void addBlobToSet(MockMessageWriteSet messageWriteSet, BlobId blobId, long size, long expiresAtMs) {
    long crc = random.nextLong();
    MessageInfo info =
        new MessageInfo(blobId, size, false, true, expiresAtMs, crc, refAccountId, refContainerId, operationTime);
    ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) size));
    messageWriteSet.add(info, buffer);
  }

  /**
   * Utility method to generate a {@link BlobId} for the reference account and container.
   * @return the generated {@link BlobId}.
   */
  private BlobId getUniqueId() {
    return getUniqueId(refAccountId, refContainerId, false);
  }

  /**
   * Utility method to generate a {@link BlobId} with specified account and container.
   * @param accountId the account Id.
   * @param containerId the container Id.
   * @param encrypted the encrypted bit.
   * @return the generated {@link BlobId}.
   */
  private BlobId getUniqueId(short accountId, short containerId, boolean encrypted) {
    byte dataCenterId = 66;
    return new BlobId(BLOB_ID_V6, BlobIdType.NATIVE, dataCenterId, accountId, containerId, partitionId, encrypted,
        BlobDataType.DATACHUNK);
  }

  /**
   * Test cloud store get method.
   * @throws Exception
   */
  @Test
  public void testStoreGets() throws Exception {
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
      blobIds.add(addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, false));
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
    BlobId nonExistentId = getUniqueId(refAccountId, refContainerId, false);
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
    BlobId expiredBlobId = getUniqueId(refAccountId, refContainerId, false);
    long size = SMALL_BLOB_SIZE;
    long currentTime = System.currentTimeMillis();
    CloudBlobMetadata expiredBlobMetadata =
        new CloudBlobMetadata(expiredBlobId, currentTime, currentTime - 1, size, null);
    ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes((int) size));
    InputStream inputStream = new ByteBufferInputStream(buffer);
    dest.uploadBlob(expiredBlobId, size, expiredBlobMetadata, inputStream);
    return expiredBlobId;
  }
}
