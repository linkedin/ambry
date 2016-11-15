/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;


public class BlobStoreTest {

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  class MockMessageWriteSet implements MessageWriteSet {

    public ByteBuffer bufToWrite;
    public List<MessageInfo> info;

    public MockMessageWriteSet(ByteBuffer bufToWrite, List<MessageInfo> info) {
      this.bufToWrite = bufToWrite;
      this.info = info;
    }

    @Override
    public long writeTo(Write writeChannel) throws IOException {
      return writeChannel.appendFrom(bufToWrite);
    }

    @Override
    public List<MessageInfo> getMessageSetInfo() {
      return info;
    }
  }

  @Test
  public void storePutTest() throws IOException {
    MockClusterMap map = null;
    try {
      ScheduledExecutorService scheduler = Utils.newScheduler(4, "thread", false);
      Properties props = new Properties();
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      map = new MockClusterMap();
      DataNodeId dataNodeId1 = map.getDataNodeIds().get(0);
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      List<ReplicaId> replicaIds = map.getReplicaIds(map.getDataNodeId("localhost", dataNodeId1.getPort()));
      String storeId = replicaIds.get(0).getPartitionId().toString();
      StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
      Store store = new BlobStore(storeId, config, scheduler, new DiskIOScheduler(null), metrics,
          replicaIds.get(0).getReplicaPath(), replicaIds.get(0).getCapacityInBytes(), factory,
          new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(), SystemTime.getInstance());
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MessageInfo info1 = new MessageInfo(blobId1, 1000);
      MessageInfo info2 = new MessageInfo(blobId2, 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existance
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(blobId1);
      keys.add(blobId2);
      StoreInfo info = store.get(keys, EnumSet.noneOf(StoreGetOptions.class));
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      Assert.assertEquals(readSet.sizeInBytes(1), 1000);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }

      // put a blob that already exist
      new Random().nextBytes(bufToWrite);
      info1 = new MessageInfo(blobId1, 1000);
      listInfo = new ArrayList<MessageInfo>(1);
      listInfo.add(info1);

      set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      try {
        store.put(set);
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertTrue(e.getErrorCode() == StoreErrorCodes.Already_Exist);
      }
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void storeGetTest() throws IOException {
    MockClusterMap map = null;
    try {
      ScheduledExecutorService scheduler = Utils.newScheduler(4, "thread", false);
      File tempFile = tempFile();
      Properties props = new Properties();
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      map = new MockClusterMap();
      DataNodeId dataNodeId1 = map.getDataNodeIds().get(0);
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      List<ReplicaId> replicaIds = map.getReplicaIds(map.getDataNodeId("localhost", dataNodeId1.getPort()));
      String storeId = replicaIds.get(0).getPartitionId().toString();
      StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
      Store store = new BlobStore(storeId, config, scheduler, new DiskIOScheduler(null), metrics,
          replicaIds.get(0).getReplicaPath(), replicaIds.get(0).getCapacityInBytes(), factory,
          new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(), SystemTime.getInstance());
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MessageInfo info1 = new MessageInfo(blobId1, 1000);
      MessageInfo info2 = new MessageInfo(blobId2, 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);
      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existence
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(blobId1);
      keys.add(blobId2);
      StoreInfo info = store.get(keys, EnumSet.noneOf(StoreGetOptions.class));
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      Assert.assertEquals(readSet.sizeInBytes(1), 1000);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(false, true);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void storeDeleteTest() throws IOException {
    MockClusterMap map = null;
    try {
      ScheduledExecutorService scheduler = Utils.newScheduler(4, "thread", false);
      File tempFile = tempFile();
      Properties props = new Properties();
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      map = new MockClusterMap();
      DataNodeId dataNodeId1 = map.getDataNodeIds().get(0);
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      List<ReplicaId> replicaIds = map.getReplicaIds(map.getDataNodeId("localhost", dataNodeId1.getPort()));
      String storeId = replicaIds.get(0).getPartitionId().toString();
      StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
      Store store = new BlobStore(storeId, config, scheduler, new DiskIOScheduler(null), metrics,
          replicaIds.get(0).getReplicaPath(), replicaIds.get(0).getCapacityInBytes(), factory,
          new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(), SystemTime.getInstance());
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MessageInfo info1 = new MessageInfo(blobId1, 1000);
      MessageInfo info2 = new MessageInfo(blobId2, 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existence
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(blobId1);
      keys.add(blobId2);
      StoreInfo info = store.get(keys, EnumSet.noneOf(StoreGetOptions.class));
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      Assert.assertEquals(readSet.sizeInBytes(1), 1000);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }

      // delete an id
      byte[] bufToDelete = new byte[1000];
      new Random().nextBytes(bufToDelete);

      MessageInfo info3 = new MessageInfo(blobId1, 1000, 1234);
      ArrayList<MessageInfo> listInfo1 = new ArrayList<MessageInfo>(1);
      listInfo1.add(info3);
      MessageWriteSet setToDelete = new MockMessageWriteSet(ByteBuffer.wrap(bufToDelete), listInfo1);
      store.delete(setToDelete);
      ArrayList<StoreKey> keysDeleted = new ArrayList<StoreKey>();
      keysDeleted.add(blobId1);
      try {
        store.get(keysDeleted, EnumSet.noneOf(StoreGetOptions.class));
        Assert.assertEquals(false, true);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      keys.clear();
      keys.add(blobId2);
      store.get(keys, EnumSet.noneOf(StoreGetOptions.class));
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void storeGetDeletedTest() throws IOException {
    MockClusterMap map = null;
    try {
      ScheduledExecutorService scheduler = Utils.newScheduler(4, "thread", false);
      File tempFile = tempFile();
      Properties props = new Properties();
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      map = new MockClusterMap();
      DataNodeId dataNodeId1 = map.getDataNodeIds().get(0);
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      List<ReplicaId> replicaIds = map.getReplicaIds(map.getDataNodeId("localhost", dataNodeId1.getPort()));
      byte[] bufToWrite = new byte[5000];
      new Random().nextBytes(bufToWrite);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");
      MockId blobId4 = new MockId("id4");
      MockId blobId5 = new MockId("id5");
      MessageInfo info1 = new MessageInfo(blobId1, 1000);
      MessageInfo info2 = new MessageInfo(blobId2, 1000);
      MessageInfo info3 = new MessageInfo(blobId3, 1000);
      MessageInfo info4 = new MessageInfo(blobId4, 1000);
      MessageInfo info5 = new MessageInfo(blobId5, 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(5);
      listInfo.add(info1);
      listInfo.add(info2);
      listInfo.add(info3);
      listInfo.add(info4);
      listInfo.add(info5);

      HashMap<Long, MessageInfo> dummyMap = new HashMap<Long, MessageInfo>();
      dummyMap.put(new Long(0), info1);
      dummyMap.put(new Long(1000), info2);
      dummyMap.put(new Long(2000), info3);
      dummyMap.put(new Long(3000), info4);
      dummyMap.put(new Long(4000), info5);

      String storeId = replicaIds.get(0).getPartitionId().toString();
      StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
      Store store = new BlobStore("storeId", config, scheduler, new DiskIOScheduler(null), metrics,
          replicaIds.get(0).getReplicaPath(), replicaIds.get(0).getCapacityInBytes(), factory,
          new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(dummyMap), SystemTime.getInstance());
      store.start();

      // put blobs
      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // get them
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>(5);
      keys.add(blobId1);
      keys.add(blobId2);
      keys.add(blobId3);
      keys.add(blobId4);
      keys.add(blobId5);
      StoreInfo info = store.get(keys, EnumSet.noneOf(StoreGetOptions.class));
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 5);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }
      readSet.writeTo(2, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 2000; i < 3000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 2000]);
      }
      readSet.writeTo(3, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 3000; i < 4000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 3000]);
      }
      readSet.writeTo(4, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 4000; i < 5000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 4000]);
      }

      // delete a few ids
      byte[] bufToDelete = new byte[1000];
      new Random().nextBytes(bufToDelete);

      MessageInfo info1d = new MessageInfo(blobId1, 1000, 1234);
      MessageInfo info4d = new MessageInfo(blobId4, 1000, 1234);
      MessageInfo info3d = new MessageInfo(blobId3, 1000, 1234);
      ArrayList<MessageInfo> listInfo1 = new ArrayList<MessageInfo>(3);
      listInfo1.add(info1d);
      listInfo1.add(info4d);
      listInfo1.add(info3d);
      MessageWriteSet setToDelete = new MockMessageWriteSet(ByteBuffer.wrap(bufToDelete), listInfo1);
      store.delete(setToDelete);

      try {
        info = store.get(keys, EnumSet.of(StoreGetOptions.Store_Include_Deleted));
      } catch (StoreException e) {
        Assert.assertEquals(false, true);
      }

      readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 5);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      Assert.assertEquals(readSet.sizeInBytes(1), 1000);
      Assert.assertEquals(readSet.sizeInBytes(2), 1000);
      Assert.assertEquals(readSet.sizeInBytes(3), 1000);
      Assert.assertEquals(readSet.sizeInBytes(4), 1000);

      output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }
      readSet.writeTo(2, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 2000; i < 3000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 2000]);
      }
      readSet.writeTo(3, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 3000; i < 4000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 3000]);
      }
      readSet.writeTo(4, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 4000; i < 5000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 4000]);
      }
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void storeShutdownTest() throws IOException {
    MockClusterMap map = null;
    try {
      ScheduledExecutorService scheduler = Utils.newScheduler(4, "thread", false);
      Properties props = new Properties();
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      map = new MockClusterMap();
      DataNodeId dataNodeId1 = map.getDataNodeIds().get(0);
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      List<ReplicaId> replicaIds = map.getReplicaIds(map.getDataNodeId("localhost", dataNodeId1.getPort()));
      String storeId = replicaIds.get(0).getPartitionId().toString();
      StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
      Store store = new BlobStore(storeId, config, scheduler, new DiskIOScheduler(null), metrics,
          replicaIds.get(0).getReplicaPath(), replicaIds.get(0).getCapacityInBytes(), factory,
          new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(), SystemTime.getInstance());
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MessageInfo info1 = new MessageInfo(blobId1, 1000);
      MessageInfo info2 = new MessageInfo(blobId2, 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existance
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(blobId1);
      keys.add(blobId2);
      StoreInfo info = store.get(keys, EnumSet.noneOf(StoreGetOptions.class));
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);

      // close store
      store.shutdown();

      try {
        store.get(keys, EnumSet.noneOf(StoreGetOptions.class));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertTrue(e.getErrorCode() == StoreErrorCodes.Store_Not_Started);
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void storeRecoverTest() {

  }

  @Test
  public void storeTTLTest() throws Exception {
    MockClusterMap map = null;
    try {
      ScheduledExecutorService scheduler = Utils.newScheduler(4, "thread", false);
      Properties props = new Properties();
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      map = new MockClusterMap();
      DataNodeId dataNodeId1 = map.getDataNodeIds().get(0);
      MockTime mockTime = new MockTime();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      List<ReplicaId> replicaIds = map.getReplicaIds(dataNodeId1);
      String storeId = replicaIds.get(0).getPartitionId().toString();
      StorageManagerMetrics metrics = new StorageManagerMetrics(new MetricRegistry());
      Store store = new BlobStore(storeId, config, scheduler, new DiskIOScheduler(null), metrics,
          replicaIds.get(0).getReplicaPath(), replicaIds.get(0).getCapacityInBytes(), factory,
          new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(), mockTime);
      store.start();
      byte[] bufToWrite = new byte[1000];
      new Random().nextBytes(bufToWrite);
      MockId blobId = new MockId("id1");
      Long ttl = 60L * Time.MsPerSec;
      MessageInfo messageInfo = new MessageInfo(blobId, 1000, mockTime.currentMilliseconds + ttl);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>();
      listInfo.add(messageInfo);
      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);
      mockTime.sleep(30L * Time.MsPerSec);

      // verify existence
      ArrayList<StoreKey> keyList = new ArrayList<StoreKey>();
      keyList.add(blobId);
      EnumSet<StoreGetOptions> storeGetOptions = EnumSet.noneOf(StoreGetOptions.class);
      StoreInfo storeInfo = store.get(keyList, storeGetOptions);
      MessageReadSet readSet = storeInfo.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 1);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      mockTime.sleep(60L * Time.MsPerSec);
      try {
        storeInfo = store.get(keyList, storeGetOptions);
        Assert.fail("Blob already expired. Should throw exception here.");
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }
}
