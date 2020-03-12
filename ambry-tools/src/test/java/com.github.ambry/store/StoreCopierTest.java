/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link StoreCopier}
 */
public class StoreCopierTest {

  private static final String STORE_ID = "copier_test";
  private static final DiskIOScheduler DISK_IO_SCHEDULER = new DiskIOScheduler(null);
  private static final StoreKeyFactory STORE_KEY_FACTORY;

  static {
    try {
      STORE_KEY_FACTORY = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static final long STORE_CAPACITY = 1000;
  private static final int PUT_RECORD_SIZE = 53;

  private final File srcDir;
  private final File tgtDir;
  private final StoreCopier storeCopier;
  private final StoreConfig storeConfig;
  private final ClusterMap clusterMap = new MockClusterMap();
  private final Time time = new MockTime();

  private StoreKey permanentPutId;
  private StoreKey putAndTtlUpdatedId;
  private StoreKey temporaryPutId;
  private byte[] putAndTtlUpdatedData;
  private byte[] permanentPutData;
  private byte[] temporaryPutData;
  private long temporaryPutExpiryTimeMs;
  private StoreKey expiredId;
  private StoreKey deletedId;
  private StoreKey putTtlUpdatedAndDeletedId;

  /**
   * Creates temporary directories and sets up some test state.
   * @throws Exception
   */
  public StoreCopierTest() throws Exception {
    srcDir = StoreTestUtils.createTempDirectory("srcDir-" + TestUtils.getRandomString(10));
    tgtDir = StoreTestUtils.createTempDirectory("tgtDir-" + TestUtils.getRandomString(10));
    Properties properties = new Properties();
    properties.setProperty("store.key.factory", MockIdFactory.class.getCanonicalName());
    properties.setProperty("src.store.dir", srcDir.getAbsolutePath());
    properties.setProperty("tgt.store.dir", tgtDir.getAbsolutePath());
    properties.setProperty("store.capacity", Long.toString(STORE_CAPACITY));
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    storeConfig = new StoreConfig(verifiableProperties);
    setupTestState();
    time.sleep(TimeUnit.SECONDS.toMillis(TestUtils.TTL_SECS + 1));
    StoreMetrics metrics = new StoreMetrics(clusterMap.getMetricRegistry());
    storeCopier = new StoreCopier("test_store", srcDir, tgtDir, STORE_CAPACITY, 4 * 1024 * 1024, storeConfig, metrics,
        STORE_KEY_FACTORY, DISK_IO_SCHEDULER, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR, Collections.emptyList(),
        time);
  }

  /**
   * Releases all resources and deletes the temporary directories.
   * @throws IOException
   */
  @After
  public void cleanup() throws IOException {
    storeCopier.close();
    assertTrue(srcDir + " could not be deleted", StoreTestUtils.cleanDirectory(srcDir, true));
    assertTrue(tgtDir + " could not be deleted", StoreTestUtils.cleanDirectory(tgtDir, true));
  }

  /**
   * Tests {@link StoreCopier#copy(FindToken)}.
   * @throws Exception
   */
  @Test
  public void copyTest() throws Exception {
    storeCopier.copy(new StoreFindTokenFactory(STORE_KEY_FACTORY).getNewFindToken());
    storeCopier.close();
    // copy the store descriptor file over
    StoreMetrics storeMetrics = new StoreMetrics(new MetricRegistry());
    Files.copy(new File(srcDir, StoreDescriptor.STORE_DESCRIPTOR_FILENAME).toPath(),
        new File(tgtDir, StoreDescriptor.STORE_DESCRIPTOR_FILENAME).toPath(), StandardCopyOption.REPLACE_EXISTING);
    BlobStore tgt =
        new BlobStore(STORE_ID, storeConfig, null, null, DISK_IO_SCHEDULER, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
            storeMetrics, storeMetrics, tgtDir.getAbsolutePath(), STORE_CAPACITY, STORE_KEY_FACTORY, null, null, time);
    tgt.start();
    try {
      // should not be able to get expired or deleted ids
      StoreKey[] failKeys = {expiredId, deletedId, putTtlUpdatedAndDeletedId};
      for (StoreKey key : failKeys) {
        try {
          tgt.get(Collections.singletonList(key), EnumSet.allOf(StoreGetOptions.class));
          fail("Should have failed to get " + key);
        } catch (StoreException e) {
          assertEquals("Unexpected StoreErrorCode", StoreErrorCodes.ID_Not_Found, e.getErrorCode());
        }
      }
      // should be able to get the non expired, non deleted entries
      Map<StoreKey, Pair<byte[], Long>> successKeys = new HashMap<>();
      successKeys.put(permanentPutId, new Pair<>(permanentPutData, Utils.Infinite_Time));
      successKeys.put(putAndTtlUpdatedId, new Pair<>(putAndTtlUpdatedData, Utils.Infinite_Time));
      successKeys.put(temporaryPutId, new Pair<>(temporaryPutData, temporaryPutExpiryTimeMs));
      for (Map.Entry<StoreKey, Pair<byte[], Long>> entry : successKeys.entrySet()) {
        StoreInfo storeInfo = tgt.get(Collections.singletonList(entry.getKey()), EnumSet.noneOf(StoreGetOptions.class));
        MessageInfo messageInfo = storeInfo.getMessageReadSetInfo().get(0);
        byte[] data = entry.getValue().getFirst();
        assertEquals("Size does not match", data.length, messageInfo.getSize());
        assertEquals("Size does not match", data.length, storeInfo.getMessageReadSet().sizeInBytes(0));
        assertFalse("Should not be deleted or expired", messageInfo.isDeleted() || messageInfo.isExpired());
        assertEquals("Ttl update flag not as expected", putAndTtlUpdatedId.equals(entry.getKey()),
            messageInfo.isTtlUpdated());
        assertEquals("Expiration time does not match", entry.getValue().getSecond().longValue(),
            messageInfo.getExpirationTimeInMs());
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(data.length));
        storeInfo.getMessageReadSet().writeTo(0, channel, 0, data.length);
        assertArrayEquals("Data put does not match data copied", data, channel.getBuffer().array());
      }
    } finally {
      tgt.shutdown();
    }
  }

  /**
   * Sets up some test state required to verify the copy.
   * @throws IOException
   * @throws StoreException
   */
  private void setupTestState() throws IOException, StoreException {
    StoreMetrics metrics = new StoreMetrics(clusterMap.getMetricRegistry());
    long expiryTimeMs = time.milliseconds() + TimeUnit.SECONDS.toMillis(TestUtils.TTL_SECS);
    temporaryPutExpiryTimeMs =
        Utils.getTimeInMsToTheNearestSec(SystemTime.getInstance().milliseconds() + TimeUnit.DAYS.toMillis(1));
    BlobStore src =
        new BlobStore(STORE_ID, storeConfig, null, null, DISK_IO_SCHEDULER, StoreTestUtils.DEFAULT_DISK_SPACE_ALLOCATOR,
            metrics, metrics, srcDir.getAbsolutePath(), STORE_CAPACITY, STORE_KEY_FACTORY, null, null, time);
    src.start();
    try {
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      putAndTtlUpdatedId = new MockId("putAndTtlUpdatedId", accountId, containerId);
      putAndTtlUpdatedData =
          addMessage(src, putAndTtlUpdatedId, expiryTimeMs, false, false, accountId, containerId, time.milliseconds());
      putTtlUpdatedAndDeletedId = new MockId("putTtlUpdatedAndDeletedId", accountId, containerId);
      addMessage(src, putTtlUpdatedAndDeletedId, expiryTimeMs, false, false, accountId, containerId,
          time.milliseconds());
      temporaryPutId = new MockId("temporaryPutId", accountId, containerId);
      temporaryPutData = addMessage(src, temporaryPutId, temporaryPutExpiryTimeMs, false, false, accountId, containerId,
          time.milliseconds());
      deletedId = new MockId("deletedId", accountId, containerId);
      addMessage(src, deletedId, Utils.Infinite_Time, false, false, accountId, containerId, time.milliseconds());
      permanentPutId = new MockId("permanentPutId", accountId, containerId);
      permanentPutData = addMessage(src, permanentPutId, Utils.Infinite_Time, false, false, accountId, containerId,
          time.milliseconds());
      addMessage(src, putAndTtlUpdatedId, Utils.Infinite_Time, false, true, accountId, containerId,
          time.milliseconds());
      addMessage(src, putTtlUpdatedAndDeletedId, Utils.Infinite_Time, false, true, accountId, containerId,
          time.milliseconds());
      addMessage(src, deletedId, Utils.Infinite_Time, true, false, accountId, containerId, time.milliseconds());
      addMessage(src, putTtlUpdatedAndDeletedId, Utils.Infinite_Time, true, true, accountId, containerId,
          time.milliseconds());
      expiredId = new MockId("expiredId", accountId, containerId);
      addMessage(src, expiredId, 0, false, false, accountId, containerId, time.milliseconds());
    } finally {
      src.shutdown();
    }
  }

  /**
   * Adds a message to the given {@code store}.
   * @param store the {@link Store} to add the message to.
   * @param key the {@link StoreKey} associated with the message.
   * @param expiryTimeMs the expiry time associated with the message.
   * @param isDelete {@code true} if this is a delete message, {@code false} otherwise.
   * @param isTtlUpdate {@code true} if this is a ttl update message or if {@code isDelete} is true and the created
   *                    {@link MessageInfo} needs to indicate that a TTL update had occurred, {@code false} otherwise.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operationTime in ms of put or delete
   * @return the message that was written.
   * @throws IOException
   * @throws StoreException
   */
  private byte[] addMessage(Store store, StoreKey key, long expiryTimeMs, boolean isDelete, boolean isTtlUpdate,
      short accountId, short containerId, long operationTimeMs) throws IOException, StoreException {
    int size = PUT_RECORD_SIZE;
    MessageInfo messageInfo =
        new MessageInfo(key, size, isDelete, isTtlUpdate, expiryTimeMs, accountId, containerId, operationTimeMs);
    byte[] data = TestUtils.getRandomBytes(size);
    MessageFormatWriteSet writeSet =
        new MessageFormatWriteSet(new ByteArrayInputStream(data), Collections.singletonList(messageInfo), false);
    if (isDelete) {
      store.delete(writeSet.getMessageSetInfo());
    } else if (isTtlUpdate) {
      store.updateTtl(writeSet.getMessageSetInfo());
    } else {
      store.put(writeSet);
    }
    return data;
  }
}
