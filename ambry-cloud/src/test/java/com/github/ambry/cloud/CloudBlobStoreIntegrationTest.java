/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockMessageWriteSet;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.cloud.CloudTestUtil.*;
import static org.junit.Assert.*;


/**
 * Integration Test cases for {@link CloudBlobStore}
 * Must supply file azure-test.properties in classpath with valid config property values.
 */
@Ignore
@RunWith(Parameterized.class)
public class CloudBlobStoreIntegrationTest {

  private VerifiableProperties verifiableProperties;
  private CloudBlobStore cloudBlobStore;
  private CloudDestination cloudDestination;
  private Random random = new Random();
  private short accountId = 101;
  private short containerId = 5;
  private long operationTime = System.currentTimeMillis();
  private boolean isVcr;
  private PartitionId partitionId;
  private final String PROPS_FILE_NAME = "azure-test.properties";
  private VcrMetrics vcrMetrics;
  private AzureMetrics azureMetrics;
  private AzureCloudConfig azureCloudConfig;

  /**
   * Run in both VCR and live serving mode.
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Constructor for {@link CloudBlobStoreIntegrationTest}.
   * @param isVcr true if testing for vcr. false otherwise.
   */
  public CloudBlobStoreIntegrationTest(boolean isVcr) {
    this.isVcr = isVcr;
  }

  @Before
  public void setup() throws ReflectiveOperationException {
    Properties testProperties = new Properties();
    try (InputStream input = this.getClass().getClassLoader().getResourceAsStream(PROPS_FILE_NAME)) {
      if (input == null) {
        throw new IllegalStateException("Could not find resource: " + PROPS_FILE_NAME);
      }
      testProperties.load(input);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load properties from resource: " + PROPS_FILE_NAME);
    }
    testProperties.setProperty("clustermap.cluster.name", "Integration-Test");
    testProperties.setProperty("clustermap.datacenter.name", "uswest");
    testProperties.setProperty("clustermap.host.name", "localhost");
    testProperties.setProperty("kms.default.container.key",
        "B374A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");

    testProperties.setProperty(CloudConfig.CLOUD_DELETED_BLOB_RETENTION_DAYS, String.valueOf(1));
    testProperties.setProperty(AzureCloudConfig.AZURE_PURGE_BATCH_SIZE, "10");
    testProperties.setProperty(CloudConfig.CLOUD_IS_VCR, "" + isVcr);
    verifiableProperties = new VerifiableProperties(testProperties);
    azureCloudConfig = new AzureCloudConfig(verifiableProperties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    partitionId = new Partition(666, clusterMapConfig.clusterMapDefaultPartitionClass, PartitionState.READ_WRITE,
        100 * 1024 * 1024 * 1024L);
    ClusterMap clusterMap = new MockClusterMap(false, Collections.singletonList(
        new MockDataNodeId(Collections.singletonList(new Port(6666, PortType.PLAINTEXT)),
            Collections.singletonList("test"), "AzureTest")), 1, Collections.singletonList(partitionId), "AzureTest");
    MetricRegistry registry = new MetricRegistry();
    vcrMetrics = new VcrMetrics(registry);
    azureMetrics = new AzureMetrics(registry);
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, verifiableProperties, registry, clusterMap);
    cloudDestination = cloudDestinationFactory.getCloudDestination();
    cloudBlobStore = new CloudBlobStore(verifiableProperties, partitionId, cloudDestination, clusterMap, vcrMetrics);
    cloudBlobStore.start();
  }

  @After
  public void destroy() throws IOException {
    cleanupPartition(azureCloudConfig, partitionId);
    if (cloudBlobStore != null) {
      cloudBlobStore.shutdown();
    }
    if (cloudDestination != null) {
      cloudDestination.close();
    }
  }

  /** Test {@link CloudBlobStore#put} method. */
  @Test
  public void testPut() throws StoreException {
    // Put blobs with and without expiration and encryption
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    int count = 5;
    int expectedUploads = 0;
    int expectedEncryptions = 0;
    for (int j = 0; j < count; j++) {
      long size = Math.abs(random.nextLong()) % 10000;
      // Permanent and encrypted, should be uploaded and not reencrypted
      addBlobToMessageSet(messageWriteSet, size, Utils.Infinite_Time, accountId, containerId, true, false, partitionId,
          operationTime, isVcr);
      expectedUploads++;
      // Permanent and unencrypted
      addBlobToMessageSet(messageWriteSet, size, Utils.Infinite_Time, accountId, containerId, false, false, partitionId,
          operationTime, isVcr);
      expectedUploads++;
    }
    cloudBlobStore.put(messageWriteSet);
    assertEquals("Unexpected blobs count", expectedUploads, azureMetrics.blobUploadSuccessCount.getCount());
    assertEquals("Unexpected encryption count", expectedEncryptions, vcrMetrics.blobEncryptionCount.getCount());
  }

  /** Test {@link CloudBlobStore#get} method. */
  @Test
  public void testGet() throws StoreException {
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    addBlobToMessageSet(messageWriteSet, Utils.Infinite_Time, accountId, containerId, partitionId, operationTime,
        (short) 2);
    cloudBlobStore.put(messageWriteSet);

    // verify that the blob was uploaded with expected metadata.
    StoreInfo storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected blob id", messageWriteSet.getMessageSetInfo().get(0).getStoreKey(),
        storeInfo.getMessageReadSetInfo().get(0).getStoreKey());
    assertEquals("Unexpected live version", messageWriteSet.getMessageSetInfo().get(0).getLifeVersion(),
        storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertEquals("Unexpected delete status", messageWriteSet.getMessageSetInfo().get(0).isDeleted(),
        storeInfo.getMessageReadSetInfo().get(0).isDeleted());
    assertEquals("Unexpected blob size", messageWriteSet.getMessageSetInfo().get(0).getSize(),
        storeInfo.getMessageReadSetInfo().get(0).getSize());
    assertEquals("Unexpected ttl update status", messageWriteSet.getMessageSetInfo().get(0).isTtlUpdated(),
        storeInfo.getMessageReadSetInfo().get(0).isTtlUpdated());
    assertEquals("Unexpected account id", messageWriteSet.getMessageSetInfo().get(0).getAccountId(),
        storeInfo.getMessageReadSetInfo().get(0).getAccountId());
    assertEquals("Unexpected container id", messageWriteSet.getMessageSetInfo().get(0).getContainerId(),
        storeInfo.getMessageReadSetInfo().get(0).getContainerId());
    assertEquals("Unexpected operation time", messageWriteSet.getMessageSetInfo().get(0).getOperationTimeMs(),
        storeInfo.getMessageReadSetInfo().get(0).getOperationTimeMs());
  }

  /** Test {@link CloudBlobStore#delete} method. */
  @Test
  public void testDelete() throws StoreException {
    if (isVcr) {
      testDeleteFromVcr();
    } else {
      testDeleteFromFrontend();
    }
  }

  /** Test {@link CloudBlobStore#delete} method for vcr. */
  public void testDeleteFromVcr() throws StoreException {
    // First upload a blob with a life version 2
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    addBlobToMessageSet(messageWriteSet, Utils.Infinite_Time, accountId, containerId, partitionId, operationTime,
        (short) 2);
    cloudBlobStore.put(messageWriteSet);

    // verify that the blob was uploaded with expected metadata.
    StoreInfo storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", messageWriteSet.getMessageSetInfo().get(0).getLifeVersion(),
        storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertEquals("Unexpected delete status", messageWriteSet.getMessageSetInfo().get(0).isDeleted(),
        storeInfo.getMessageReadSetInfo().get(0).isDeleted());

    // Now delete with a smaller life version should fail silently without updating the life version.
    MessageInfo messageInfo = messageWriteSet.getMessageSetInfo().get(0);
    MessageInfo deleteMessageInfo =
        new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
            messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
            messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
            messageInfo.getOperationTimeMs(), (short) 1);
    cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", messageWriteSet.getMessageSetInfo().get(0).getLifeVersion(),
        storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertEquals("Unexpected delete status", messageWriteSet.getMessageSetInfo().get(0).isDeleted(),
        storeInfo.getMessageReadSetInfo().get(0).isDeleted());

    // Delete with same life version should pass without changing life version.
    deleteMessageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
        messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
        messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
        messageInfo.getOperationTimeMs(), messageInfo.getLifeVersion());
    cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", messageWriteSet.getMessageSetInfo().get(0).getLifeVersion(),
        storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertTrue("Unexpected delete status", storeInfo.getMessageReadSetInfo().get(0).isDeleted());

    // Deleting a deleted blob with higher life version should update life version.
    deleteMessageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
        messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
        messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
        messageInfo.getOperationTimeMs(), (short) 3);
    cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", 3, storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertTrue("Unexpected delete status", storeInfo.getMessageReadSetInfo().get(0).isDeleted());

    // Deleting again with smaller life version should fail with exception.
    deleteMessageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
        messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
        messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
        messageInfo.getOperationTimeMs(), (short) 1);
    try {
      cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));
      fail("Delete should fail with ID_Deleted StoreException");
    } catch (StoreException ex) {
      assertEquals("Unexpected error code", ex.getErrorCode(), StoreErrorCodes.ID_Deleted);
    }
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", 3, storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertTrue("Unexpected delete status", storeInfo.getMessageReadSetInfo().get(0).isDeleted());

    // Restart cloud blob store to clear cache. Deleting again with smaller life version should fail silently without updating anything.
    cloudBlobStore.shutdown();
    cloudBlobStore.start();
    cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", 3, storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertTrue("Unexpected delete status", storeInfo.getMessageReadSetInfo().get(0).isDeleted());
  }

  /** Test {@link CloudBlobStore#delete} method from frontend. */
  public void testDeleteFromFrontend() throws StoreException {
    // First upload a blob with a life version 2
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    addBlobToMessageSet(messageWriteSet, Utils.Infinite_Time, accountId, containerId, partitionId, operationTime,
        (short) -1);
    cloudBlobStore.put(messageWriteSet);

    // verify that the blob was uploaded with expected metadata.
    StoreInfo storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", 0, storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertEquals("Unexpected delete status", messageWriteSet.getMessageSetInfo().get(0).isDeleted(),
        storeInfo.getMessageReadSetInfo().get(0).isDeleted());

    // Deleting again should fail with ID_Deleted exception.
    MessageInfo messageInfo = messageWriteSet.getMessageSetInfo().get(0);
    MessageInfo deleteMessageInfo =
        new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
            messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
            messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
            messageInfo.getOperationTimeMs(), (short) -1);
    try {
      cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));
    } catch (StoreException ex) {
      assertEquals("Unexpected error code", ex.getErrorCode(), StoreErrorCodes.ID_Deleted);
    }
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", 0, storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertTrue("Unexpected delete status", storeInfo.getMessageReadSetInfo().get(0).isDeleted());

    // Restart cloud blob store to clear cache. Deleting again should still fail with ID_Deleted Store Exception.
    cloudBlobStore.shutdown();
    cloudBlobStore.start();
    deleteMessageInfo = new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
        messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
        messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
        messageInfo.getOperationTimeMs(), (short) 3);
    try {
      cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));
    } catch (StoreException ex) {
      assertEquals("Unexpected error code", ex.getErrorCode(), StoreErrorCodes.ID_Deleted);
    }
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertEquals("Unexpected live version", 3, storeInfo.getMessageReadSetInfo().get(0).getLifeVersion());
    assertTrue("Unexpected delete status", storeInfo.getMessageReadSetInfo().get(0).isDeleted());
  }

  /** Test {@link CloudBlobStore#undelete} method. */
  @Test
  public void testUndelete() throws StoreException {
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    addBlobToMessageSet(messageWriteSet, Utils.Infinite_Time, accountId, containerId, partitionId, operationTime,
        initLifeVersion(isVcr));
    cloudBlobStore.put(messageWriteSet);

    // Attempt to undelete a blob that is not deleted. Should fail silently for vcr and throw exception for frontend.
    MessageInfo messageInfo = messageWriteSet.getMessageSetInfo().get(0);
    MessageInfo undeleteMessageInfo =
        new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
            messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
            messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
            messageInfo.getOperationTimeMs(), (isVcr ? (short) 1 : -1));
    try {
      cloudBlobStore.undelete(undeleteMessageInfo);
      if (!isVcr) {
        fail("Undelete from frontend of a not deleted blob should throw exception.");
      }
    } catch (StoreException ex) {
      if (isVcr) {
        fail("Undelete for the vcr should fail silently");
      }
      assertEquals("Unexpected error message", StoreErrorCodes.ID_Not_Deleted, ex.getErrorCode());
    }

    // delete the blob.
    MessageInfo deleteMessageInfo =
        new MessageInfo(messageInfo.getStoreKey(), messageInfo.getSize(), messageInfo.isDeleted(),
            messageInfo.isTtlUpdated(), messageInfo.isUndeleted(), messageInfo.getExpirationTimeInMs(),
            messageInfo.getCrc(), messageInfo.getAccountId(), messageInfo.getContainerId(),
            messageInfo.getOperationTimeMs(), (short) (isVcr ? 1 : -1));
    cloudBlobStore.delete(Collections.singletonList(deleteMessageInfo));

    // Attempt to undelete should pass
    short lifeVersion = cloudBlobStore.undelete(undeleteMessageInfo);
    assertEquals("Unexpected life version after undelete", lifeVersion, 1);
  }

  /** Test {@link CloudBlobStore#updateTtl} method. */
  @Test
  public void testUpdateTtl() throws StoreException {
    MockMessageWriteSet messageWriteSet = new MockMessageWriteSet();
    StoreConfig storeConfig = new StoreConfig(verifiableProperties);
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    long now = System.currentTimeMillis();
    long expirationTimeMs = now;
    if (isVcr) {
      // vcr doesn't upload a blob that is within CloudConfig#vcrMinTtlDays of expiry.
      expirationTimeMs += Math.max(TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays),
          TimeUnit.SECONDS.toMillis(storeConfig.storeTtlUpdateBufferTimeSeconds));
    } else {
      expirationTimeMs += TimeUnit.SECONDS.toMillis(storeConfig.storeTtlUpdateBufferTimeSeconds);
    }
    expirationTimeMs += 100000;
    addBlobToMessageSet(messageWriteSet, expirationTimeMs, accountId, containerId, partitionId, operationTime,
        (short) -1);
    cloudBlobStore.put(messageWriteSet);

    // verify that the blob was uploaded with expected metadata.
    StoreInfo storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertFalse("Unexpected ttl update status", storeInfo.getMessageReadSetInfo().get(0).isTtlUpdated());
    assertEquals("Unexpected expiration time", expirationTimeMs,
        storeInfo.getMessageReadSetInfo().get(0).getExpirationTimeInMs());

    // Do a ttl update without setting ttl update flag.
    MessageInfo ttlUpdateMessageInfo =
        new MessageInfo(messageWriteSet.getMessageSetInfo().get(0).getStoreKey(), 100, false, true, -1, accountId,
            containerId, now);
    cloudBlobStore.updateTtl(Collections.singletonList(ttlUpdateMessageInfo));
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertTrue("Unexpected ttl update status", storeInfo.getMessageReadSetInfo().get(0).isTtlUpdated());
    assertEquals("Unexpected expiration time", -1, storeInfo.getMessageReadSetInfo().get(0).getExpirationTimeInMs());

    // Do a ttl update on a updated blob. It should fail silently.
    ttlUpdateMessageInfo =
        new MessageInfo(messageWriteSet.getMessageSetInfo().get(0).getStoreKey(), 100, false, true, -1, accountId,
            containerId, now);
    cloudBlobStore.updateTtl(Collections.singletonList(ttlUpdateMessageInfo));
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertTrue("Unexpected ttl update status", storeInfo.getMessageReadSetInfo().get(0).isTtlUpdated());
    assertEquals("Unexpected expiration time", -1, storeInfo.getMessageReadSetInfo().get(0).getExpirationTimeInMs());

    // Clear cache by restarting blob store. Do a ttl update on a updated blob. It should fail silently.
    cloudBlobStore.shutdown();
    cloudBlobStore.start();
    ttlUpdateMessageInfo =
        new MessageInfo(messageWriteSet.getMessageSetInfo().get(0).getStoreKey(), 100, false, true, -1, accountId,
            containerId, now);
    cloudBlobStore.updateTtl(Collections.singletonList(ttlUpdateMessageInfo));
    storeInfo = cloudBlobStore.get(
        messageWriteSet.getMessageSetInfo().stream().map(MessageInfo::getStoreKey).collect(Collectors.toList()),
        EnumSet.allOf(StoreGetOptions.class));
    assertTrue("Unexpected ttl update status", storeInfo.getMessageReadSetInfo().get(0).isTtlUpdated());
    assertEquals("Unexpected expiration time", -1, storeInfo.getMessageReadSetInfo().get(0).getExpirationTimeInMs());

    // Delete the blob.
    cloudBlobStore.delete(Collections.singletonList(ttlUpdateMessageInfo));

    // ttlupdate of a deleted blob should throw ID_Delete Store Exception for frontend and fail silently for vcr.
    try {
      cloudBlobStore.updateTtl(Collections.singletonList(ttlUpdateMessageInfo));
      if (!isVcr) {
        fail("Update ttl of a deleted blob should fail for frontend.");
      }
    } catch (StoreException ex) {
      assertEquals("Unexcpected error code", ex.getErrorCode(), StoreErrorCodes.ID_Deleted);
    }

    // Clear cache by restarting blob store. ttlupdate of a deleted blob should throw ID_Delete Store Exception.
    cloudBlobStore.shutdown();
    cloudBlobStore.start();
    try {
      cloudBlobStore.updateTtl(Collections.singletonList(ttlUpdateMessageInfo));
      if (!isVcr) {
        fail("Update ttl of a deleted blob should fail.");
      }
    } catch (StoreException ex) {
      assertEquals("Unexpected error code", ex.getErrorCode(), StoreErrorCodes.ID_Deleted);
    }
  }
}
