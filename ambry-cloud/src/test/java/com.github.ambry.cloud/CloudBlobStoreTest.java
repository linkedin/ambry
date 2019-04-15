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
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockMessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
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
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.commons.BlobId.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;


/**
 * Test class testing behavior of CloudBlobStore class.
 */
public class CloudBlobStoreTest {

  private final CloudBlobCryptoAgent cryptoAgent = new TestCloudBlobCryptoAgent();
  private Store store;
  private CloudDestination dest;
  private PartitionId partitionId;
  private CloudConfig cloudConfig;
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
   * @param withCryptoAgent whether to set a {@link CloudBlobCryptoAgent} in the cloud store.
   * @param start whether to start the store.
   */
  private void setupCloudStore(boolean requireEncryption, boolean withCryptoAgent, boolean start) throws Exception {
    Properties props = new Properties();
    // Require encryption for uploading
    props.setProperty(CloudConfig.VCR_REQUIRE_ENCRYPTION, Boolean.toString(requireEncryption));
    cloudConfig = new CloudConfig(new VerifiableProperties(props));
    dest = mock(CloudDestination.class);
    vcrMetrics = new VcrMetrics(new MetricRegistry());
    store = new CloudBlobStore(partitionId, cloudConfig, dest, withCryptoAgent ? cryptoAgent : null, vcrMetrics);
    if (start) {
      store.start();
    }
  }

  /** Test the CloudBlobStore put method. */
  @Test
  public void testStorePuts() throws Exception {
    testStorePuts(false, false);
    testStorePuts(false, true);
    testStorePuts(true, false);
    testStorePuts(true, true);
  }

  private void testStorePuts(boolean requireEncryption, boolean withCryptoAgent) throws Exception {
    setupCloudStore(requireEncryption, withCryptoAgent, true);
    // Put blobs with and without expiration and encryption
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 1;
    long expireTime = System.currentTimeMillis() + 10000;
    int expectedUploads = 0;
    int expectedEncryptions = 0;
    int expectedUnencryptedSkips = 0;
    for (int j = 0; j < count; j++) {
      long size = Math.abs(random.nextLong()) % 10000;
      // Permanent and encrypted, should be uploaded and not reencrypted
      addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, true);
      expectedUploads++;
      // Permanent and unencrypted
      addBlobToSet(messageWriteSet, size, Utils.Infinite_Time, refAccountId, refContainerId, false);
      if (withCryptoAgent) {
        expectedUploads++;
        if (requireEncryption) {
          expectedEncryptions++;
        }
      } else {
        if (requireEncryption) {
          expectedUnencryptedSkips++;
        } else {
          expectedUploads++;
        }
      }
      // Short TTL and encrypted, should not be uploaded nor encrypted
      addBlobToSet(messageWriteSet, size, expireTime, refAccountId, refContainerId, true);
    }
    store.put(messageWriteSet);
    verify(dest, times(expectedUploads)).uploadBlob(any(BlobId.class), anyLong(), any(CloudBlobMetadata.class),
        any(InputStream.class));
    assertEquals("Unexpected encryption count", expectedEncryptions, vcrMetrics.blobEncryptionCount.getCount());
    assertEquals("Unexpected skip count", expectedUnencryptedSkips, vcrMetrics.skipUnencryptedBlobsCount.getCount());
  }

  /** Test the CloudBlobStore delete method. */
  @Test
  public void testStoreDeletes() throws Exception {
    setupCloudStore(true, true, true);
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
    setupCloudStore(true, true, true);
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
    setupCloudStore(true, true, true);
    int count = 10;
    List<StoreKey> keys = new ArrayList<>();
    Map<String, CloudBlobMetadata> metadataMap = new HashMap<>();
    for (int j = 0; j < count; j++) {
      // Blob with metadata
      BlobId existentBlobId = getUniqueId();
      keys.add(existentBlobId);
      metadataMap.put(existentBlobId.getID(),
          new CloudBlobMetadata(existentBlobId, operationTime, Utils.Infinite_Time, 1024,
              CloudBlobMetadata.EncryptionOrigin.ROUTER, null));
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
    setupCloudStore(true, true, false);
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
    props.setProperty(CloudConfig.VCR_REQUIRE_ENCRYPTION, "false");
    cloudConfig = new CloudConfig(new VerifiableProperties(props));
    vcrMetrics = new VcrMetrics(new MetricRegistry());
    CloudBlobStore exStore = new CloudBlobStore(partitionId, cloudConfig, exDest, cryptoAgent, vcrMetrics);
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
    MessageInfo info = new MessageInfo(id, size, false, false, expiresAtMs, crc, accountId, containerId, operationTime);
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
