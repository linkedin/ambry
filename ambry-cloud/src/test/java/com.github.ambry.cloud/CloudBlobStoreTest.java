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
import com.github.ambry.replication.MockConnectionPool;
import com.github.ambry.replication.MockFindToken;
import com.github.ambry.replication.MockHost;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicaThread;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockMessageWriteSet;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.commons.BlobId.*;
import static com.github.ambry.replication.ReplicationTest.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;


/**
 * Test class testing behavior of CloudBlobStore class.
 */
public class CloudBlobStoreTest {

  private Store store;
  private CloudDestination dest;
  private PartitionId partitionId;
  private VcrMetrics vcrMetrics;
  private Random random = new Random();
  private short refAccountId = 50;
  private short refContainerId = 100;
  private long operationTime = System.currentTimeMillis();

  @Before
  public void setup() throws Exception {
    partitionId = new MockPartitionId();
  }

  /**
   * Setup the cloud blobstore.
   * @param requireEncryption value of requireEncryption flag in CloudConfig.
   * @param start whether to start the store.
   */
  private void setupCloudStore(boolean requireEncryption, boolean start) throws Exception {
    Properties properties = new Properties();
    // Required clustermap properties
    setBasicProperties(properties);
    // Require encryption for uploading
    properties.setProperty(CloudConfig.VCR_REQUIRE_ENCRYPTION, Boolean.toString(requireEncryption));
    properties.setProperty(CloudConfig.CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS,
        TestCloudBlobCryptoAgentFactory.class.getName());
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    dest = mock(CloudDestination.class);
    vcrMetrics = new VcrMetrics(new MetricRegistry());
    store = new CloudBlobStore(verifiableProperties, partitionId, dest, vcrMetrics);
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
  }

  /** Test the CloudBlobStore put method. */
  @Test
  public void testStorePuts() throws Exception {
    testStorePuts(false);
    testStorePuts(true);
  }

  private void testStorePuts(boolean requireEncryption) throws Exception {
    setupCloudStore(requireEncryption, true);
    // Put blobs with and without expiration and encryption
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 5;
    int expectedUploads = 0;
    int expectedEncryptions = 0;
    for (int j = 0; j < count; j++) {
      long size = Math.abs(random.nextLong()) % 10000;
      // Permanent and encrypted, should be uploaded and not reencrypted
      addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, true);
      expectedUploads++;
      // Permanent and unencrypted
      addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, false);
      expectedUploads++;
      if (requireEncryption) {
        expectedEncryptions++;
      }
    }
    store.put(messageWriteSet);
    verify(dest, times(expectedUploads)).uploadBlob(any(BlobId.class), anyLong(), any(CloudBlobMetadata.class),
        any(InputStream.class));
    assertEquals("Unexpected encryption count", expectedEncryptions, vcrMetrics.blobEncryptionCount.getCount());
  }

  /** Test the CloudBlobStore delete method. */
  @Test
  public void testStoreDeletes() throws Exception {
    setupCloudStore(true, true);
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 10;
    for (int j = 0; j < count; j++) {
      long size = 10;
      addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, true);
    }
    store.delete(messageWriteSet);
    verify(dest, times(count)).deleteBlob(any(BlobId.class), eq(operationTime));
  }

  /** Test the CloudBlobStore updateTtl method. */
  @Test
  public void testStoreTtlUpdates() throws Exception {
    setupCloudStore(true, true);
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 10;
    for (int j = 0; j < count; j++) {
      long size = 10;
      long expirationTime = Math.abs(random.nextLong());
      addBlobToSet(messageWriteSet, size, expirationTime, refAccountId, refContainerId, true);
    }
    store.updateTtl(messageWriteSet);
    verify(dest, times(count)).updateBlobExpiration(any(BlobId.class), anyLong());
  }

  /** Test the CloudBlobStore findMissingKeys method. */
  @Test
  public void testFindMissingKeys() throws Exception {
    setupCloudStore(true, true);
    int count = 10;
    List<StoreKey> keys = new ArrayList<>();
    Map<String, CloudBlobMetadata> metadataMap = new HashMap<>();
    for (int j = 0; j < count; j++) {
      // Blob with metadata
      BlobId existentBlobId = getUniqueId();
      keys.add(existentBlobId);
      metadataMap.put(existentBlobId.getID(),
          new CloudBlobMetadata(existentBlobId, operationTime, Utils.Infinite_Time, 1024,
              CloudBlobMetadata.EncryptionOrigin.ROUTER, null, null));
      // Blob without metadata
      BlobId nonexistentBlobId = getUniqueId();
      keys.add(nonexistentBlobId);
    }
    when(dest.getBlobMetadata(anyList())).thenReturn(metadataMap);
    Set<StoreKey> missingKeys = store.findMissingKeys(keys);
    verify(dest).getBlobMetadata(anyList());
    assertEquals("Wrong number of missing keys", count, missingKeys.size());
  }

  /** Test verifying behavior when store not started. */
  @Test
  public void testStoreNotStarted() throws Exception {
    // Create store and don't start it.
    setupCloudStore(true, false);
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
      store.delete(messageWriteSet);
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
    CloudBlobStore exStore = new CloudBlobStore(new VerifiableProperties(props), partitionId, exDest, vcrMetrics);
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
      exStore.delete(messageWriteSet);
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
    CloudReplica cloudReplica = new CloudReplica(cloudConfig, partitionId, cloudDataNode);
    CloudBlobStore cloudBlobStore =
        new CloudBlobStore(new VerifiableProperties(props), partitionId, latchBasedInMemoryCloudDestination,
            new VcrMetrics(new MetricRegistry()));
    cloudBlobStore.start();

    // Prepare RemoteReplicaInfo for ReplicaThread.
    ReplicationMetrics replicationMetrics = new ReplicationMetrics(new MetricRegistry(), Collections.emptyList());
    replicationMetrics.populatePerColoMetrics(Collections.singleton(remoteHost.dataNodeId.getDatacenterName()));
    ReplicaThread replicaThread =
        new ReplicaThread("threadtest", new MockFindToken.MockFindTokenFactory(), clusterMap, new AtomicInteger(0),
            cloudDataNode, connectionPool, replicationConfig, replicationMetrics, null, storeKeyConverter, transformer,
            clusterMap.getMetricRegistry(), false, cloudDataNode.getDatacenterName(), new ResponseHandler(clusterMap),
            new MockTime());

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
    assertFalse("Blob should not exist.", latchBasedInMemoryCloudDestination.doesBlobExist(id));
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
   * Utility method to generate a BlobId and byte buffer for a blob with specified properties and add them to the specified MessageWriteSet.
   * @param messageWriteSet the {@link MockMessageWriteSet} in which to store the data.
   * @param size the size of the byte buffer.
   * @param expiresAtMs the expiration time.
   * @param accountId the account Id.
   * @param containerId the container Id.
   * @param encrypted the encrypted bit.
   * @return the generated {@link BlobId}.
   * @throws StoreException
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
}
