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
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;


public class PersistentIndexTest {
  private final File tempDir;
  private final String tempDirStr;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Creates a temporary directory for the store.
   * @throws IOException
   */
  public PersistentIndexTest() throws IOException {
    tempDir = Files.createTempDirectory("storeDir-" + UtilsTest.getRandomString(10)).toFile();
    tempDirStr = tempDir.getAbsolutePath();
    tempDir.deleteOnExit();
  }

  /**
   * Cleans up the temporary directory and deletes it.
   */
  @After
  public void cleanup() {
    File[] files = tempDir.listFiles();
    if (files != null) {
      for (File file : files) {
        assertTrue("The file [" + file.getAbsolutePath() + "] could not be deleted", file.delete());
      }
    }
    assertTrue("The directory [" + tempDir.getAbsolutePath() + "] could not be deleted", tempDir.delete());
  }

  private Offset toOffset(long offset) {
    return new Offset("", offset);
  }

  @Test
  public void testSegmentInfo() throws Exception {
    MockClusterMap map = null;
    try {
      // create a new index
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");
      MockId blobId4 = new MockId("id4");
      MockId blobId5 = new MockId("id5");
      MockId blobId6 = new MockId("id6");
      MockId blobId7 = new MockId("id7");
      MockId blobId8 = new MockId("id8");
      MockId blobId9 = new MockId("id9");

      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      IndexValue value = new IndexValue(1000, toOffset(0), (byte) 0);
      IndexSegment info =
          new IndexSegment(tempDirStr, toOffset(0), factory, blobId1.sizeInBytes(), value.getBytes().capacity(), config,
              new StoreMetrics(tempDirStr, new MetricRegistry()));
      info.addEntry(new IndexEntry(blobId1, value), toOffset(1000));
      value = new IndexValue(1000, toOffset(1000), (byte) 0);
      info.addEntry(new IndexEntry(blobId2, value), toOffset(2000));
      value = new IndexValue(1000, toOffset(2000), (byte) 0);
      info.addEntry(new IndexEntry(blobId3, value), toOffset(3000));
      value = new IndexValue(1000, toOffset(3000), (byte) 0);
      info.addEntry(new IndexEntry(blobId4, value), toOffset(4000));
      value = new IndexValue(1000, toOffset(4000), (byte) 0);
      info.addEntry(new IndexEntry(blobId5, value), toOffset(5000));
      value = new IndexValue(1000, toOffset(5000), (byte) 0);
      info.addEntry(new IndexEntry(blobId6, value), toOffset(6000));
      value = new IndexValue(1000, toOffset(6000), (byte) 0);
      info.addEntry(new IndexEntry(blobId7, value), toOffset(7000));
      value = new IndexValue(1000, toOffset(7000), (byte) 0);
      info.addEntry(new IndexEntry(blobId8, value), toOffset(8000));
      value = new IndexValue(1000, toOffset(8000), (byte) 0);
      info.addEntry(new IndexEntry(blobId9, value), toOffset(9000));

      Assert.assertEquals(info.find(blobId1).getSize(), 1000);
      Assert.assertEquals(info.find(blobId1).getOffset(), toOffset(0));
      Assert.assertEquals(info.find(blobId2).getSize(), 1000);
      Assert.assertEquals(info.find(blobId2).getOffset(), toOffset(1000));
      Assert.assertEquals(info.find(blobId3).getSize(), 1000);
      Assert.assertEquals(info.find(blobId3).getOffset(), toOffset(2000));
      Assert.assertEquals(info.find(blobId4).getSize(), 1000);
      Assert.assertEquals(info.find(blobId4).getOffset(), toOffset(3000));
      Assert.assertEquals(info.find(blobId5).getSize(), 1000);
      Assert.assertEquals(info.find(blobId5).getOffset(), toOffset(4000));
      Assert.assertEquals(info.find(blobId6).getSize(), 1000);
      Assert.assertEquals(info.find(blobId6).getOffset(), toOffset(5000));
      Assert.assertEquals(info.find(blobId7).getSize(), 1000);
      Assert.assertEquals(info.find(blobId7).getOffset(), toOffset(6000));
      Assert.assertEquals(info.find(blobId8).getSize(), 1000);
      Assert.assertEquals(info.find(blobId8).getOffset(), toOffset(7000));

      // test getEntriesSince
      List<MessageInfo> entries = new ArrayList<>();
      info.getEntriesSince(blobId6, new FindEntriesCondition(4000), entries, new AtomicLong(0));
      Assert.assertEquals(entries.get(0).getStoreKey(), blobId7);
      Assert.assertEquals(entries.get(2).getStoreKey(), blobId9);
      Assert.assertEquals(entries.size(), 3);
      entries.clear();
      info.getEntriesSince(blobId1, new FindEntriesCondition(5000), entries, new AtomicLong(0));
      Assert.assertEquals(entries.size(), 5);
      entries.clear();
      info.getEntriesSince(null, new FindEntriesCondition(5000), entries, new AtomicLong(0));
      Assert.assertEquals(entries.size(), 5);
      Assert.assertEquals(entries.get(0).getStoreKey(), blobId1);
      Assert.assertEquals(entries.get(4).getStoreKey(), blobId5);

      info.writeIndexSegmentToFile(toOffset(9000));
      StoreMetrics metrics = new StoreMetrics(info.getFile().getAbsolutePath(), new MetricRegistry());
      Journal journal = new Journal("test", 5, 5);
      IndexSegment infonew = new IndexSegment(info.getFile(), false, factory, config, metrics, journal);
      Assert.assertEquals(infonew.find(blobId1).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId1).getOffset(), toOffset(0));
      Assert.assertEquals(infonew.find(blobId2).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId2).getOffset(), toOffset(1000));
      Assert.assertEquals(infonew.find(blobId3).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId3).getOffset(), toOffset(2000));
      Assert.assertEquals(infonew.find(blobId4).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId4).getOffset(), toOffset(3000));
      Assert.assertEquals(infonew.find(blobId5).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId5).getOffset(), toOffset(4000));
      Assert.assertEquals(infonew.find(blobId6).getSize(), 1000);
      Assert.assertEquals(info.find(blobId6).getOffset(), toOffset(5000));
      Assert.assertEquals(infonew.find(blobId7).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId7).getOffset(), toOffset(6000));
      Assert.assertEquals(infonew.find(blobId8).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId8).getOffset(), toOffset(7000));

      info.map(false);
      Assert.assertEquals(info.find(blobId1).getSize(), 1000);
      Assert.assertEquals(info.find(blobId1).getOffset(), toOffset(0));
      Assert.assertEquals(info.find(blobId2).getSize(), 1000);
      Assert.assertEquals(info.find(blobId2).getOffset(), toOffset(1000));
      Assert.assertEquals(info.find(blobId3).getSize(), 1000);
      Assert.assertEquals(info.find(blobId3).getOffset(), toOffset(2000));
      Assert.assertEquals(info.find(blobId4).getSize(), 1000);
      Assert.assertEquals(info.find(blobId4).getOffset(), toOffset(3000));
      Assert.assertEquals(info.find(blobId5).getSize(), 1000);
      Assert.assertEquals(info.find(blobId5).getOffset(), toOffset(4000));
      Assert.assertEquals(info.find(blobId6).getSize(), 1000);
      Assert.assertEquals(info.find(blobId6).getOffset(), toOffset(5000));
      Assert.assertEquals(info.find(blobId7).getSize(), 1000);
      Assert.assertEquals(info.find(blobId7).getOffset(), toOffset(6000));
      Assert.assertEquals(info.find(blobId8).getSize(), 1000);
      Assert.assertEquals(info.find(blobId8).getOffset(), toOffset(7000));

      // test getEntriesSince
      entries = new ArrayList<>();
      info.getEntriesSince(blobId6, new FindEntriesCondition(5000), entries, new AtomicLong(0));
      Assert.assertEquals(entries.get(0).getStoreKey(), blobId7);
      Assert.assertEquals(entries.get(2).getStoreKey(), blobId9);
      Assert.assertEquals(entries.size(), 3);
      entries.clear();
      info.getEntriesSince(blobId1, new FindEntriesCondition(5000), entries, new AtomicLong(0));
      Assert.assertEquals(entries.size(), 5);
      entries.clear();
      info.getEntriesSince(null, new FindEntriesCondition(5000), entries, new AtomicLong(0));
      Assert.assertEquals(entries.size(), 5);
      Assert.assertEquals(entries.get(0).getStoreKey(), blobId1);
      Assert.assertEquals(entries.get(4).getStoreKey(), blobId5);

      // check invalid cases
      Assert.assertNull(info.find(new MockId("id10")));
      Assert.assertNull(info.find(new MockId("id11")));
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testIndexBasic() throws Exception {
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      Log log = new Log(tempDirStr, 10000, 10000, new StoreMetrics(tempDirStr, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0), flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, toOffset(100), flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, toOffset(300), flags, 12567));
      index.addToIndex(entry1, new FileSpan(toOffset(0), toOffset(100)));
      index.addToIndex(entry2, new FileSpan(toOffset(100), toOffset(300)));
      index.addToIndex(entry3, new FileSpan(toOffset(300), toOffset(600)));
      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), toOffset(0));
      Assert.assertEquals(value2.getOffset(), toOffset(100));
      Assert.assertEquals(value3.getOffset(), toOffset(300));
      scheduler.shutdown();
      scheduler.awaitTermination(2, TimeUnit.MINUTES);
      log.close();
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testIndexRestore() throws Exception {
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      Log log = new Log(tempDirStr, 10000, 10000, new StoreMetrics(tempDirStr, new MetricRegistry()));
      log.appendFrom(ByteBuffer.allocate(5000));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      final MockId blobId1 = new MockId("id1");
      final MockId blobId2 = new MockId("id2");
      final MockId blobId3 = new MockId("id3");
      final MockId blobId4 = new MockId("id4");
      final MockId blobId5 = new MockId("id5");
      final MockId blobId6 = new MockId("id6");
      final MockId blobId7 = new MockId("id7");

      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(3000, toOffset(0), flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(1000, toOffset(3000), flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(1000, toOffset(4000), flags, 12567));
      index.addToIndex(entry1, new FileSpan(toOffset(0), toOffset(3000)));
      index.addToIndex(entry2, new FileSpan(toOffset(3000), toOffset(4000)));
      index.addToIndex(entry3, new FileSpan(toOffset(4000), toOffset(5000)));
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory);

      IndexValue value1 = indexNew.getValue(blobId1);
      IndexValue value2 = indexNew.getValue(blobId2);
      IndexValue value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), toOffset(0));
      Assert.assertEquals(value2.getOffset(), toOffset(3000));
      Assert.assertEquals(value3.getOffset(), toOffset(4000));
      indexNew.close();

      // create a new index, persist, add more entries and fail. ensure new index restore
      // removes extra rows
      Properties props = new Properties();
      props.put("store.data.flush.delay.seconds", "999999");
      config = new StoreConfig(new VerifiableProperties(props));
      indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory);
      indexNew.addToIndex(new IndexEntry(blobId4, new IndexValue(1000, toOffset(5000), 12657)),
          new FileSpan(toOffset(5000), toOffset(6000)));
      indexNew.addToIndex(new IndexEntry(blobId5, new IndexValue(1000, toOffset(6000), 12657)),
          new FileSpan(toOffset(6000), toOffset(7000)));
      try {
        indexNew.close();
        Assert.assertFalse("Should have thrown StoreException since index has new entries compared to log", true);
      } catch (StoreException e) {
        Assert.assertTrue("StoreException thrown as expected ", true);
      }
      indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory);
      value1 = indexNew.getValue(blobId1);
      value2 = indexNew.getValue(blobId2);
      value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), toOffset(0));
      Assert.assertEquals(value2.getOffset(), toOffset(3000));
      Assert.assertEquals(value3.getOffset(), toOffset(4000));
      IndexValue value4 = indexNew.getValue(blobId4);
      IndexValue value5 = indexNew.getValue(blobId5);
      Assert.assertNull(value4);
      Assert.assertNull(value5);
      indexNew.close();

      log.appendFrom(ByteBuffer.allocate(2000));
      indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<>();
          infos.add(new MessageInfo(blobId6, 1000));
          infos.add(new MessageInfo(blobId7, 1000, 12657));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());
      IndexValue value6 = indexNew.getValue(blobId6);
      IndexValue value7 = indexNew.getValue(blobId7);
      Assert.assertEquals(value6.getSize(), 1000);
      Assert.assertEquals(value6.getOffset(), toOffset(5000));
      Assert.assertEquals(value7.getSize(), 1000);
      Assert.assertEquals(value7.getOffset(), toOffset(6000));
      Assert.assertEquals(value7.getExpiresAtMs(), 12657);
      Assert.assertEquals(log.getEndOffset().getOffset(), 7000);
      indexNew.close();

      log.appendFrom(ByteBuffer.allocate(200));
      indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<>();
          infos.add(new MessageInfo(blobId6, 100, true));
          infos.add(new MessageInfo(blobId7, 100, true));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());
      value6 = indexNew.getValue(blobId6);
      value7 = indexNew.getValue(blobId7);
      Assert.assertEquals(value6.isFlagSet(IndexValue.Flags.Delete_Index), true);
      Assert.assertEquals(value7.getExpiresAtMs(), 12657);
      Assert.assertEquals(value6.getSize(), 100);
      Assert.assertEquals(value6.getOriginalMessageOffset(), 5000);
      Assert.assertEquals(value6.getOffset(), toOffset(7000));
      Assert.assertEquals(value7.getSize(), 100);
      Assert.assertEquals(value7.getOriginalMessageOffset(), 6000);
      Assert.assertEquals(value7.getOffset(), toOffset(7100));
      indexNew.stopScheduler();
      indexNew.deleteAll();
      indexNew.close();

      File toModify = new File(tempDirStr, "0_index");
      FileChannel channelToModify = Utils.openChannel(toModify, true);
      channelToModify.truncate(0);
      channelToModify.force(true);
      scheduler = Utils.newScheduler(1, false);

      try {
        MockIndex indexFail = new MockIndex(tempDirStr, scheduler, log, config, factory);
        Assert.assertFalse(true);
      } catch (StoreException e) {
        Assert.assertTrue(true);
      }

      byte[] salt = new byte[1];
      salt[0] = 1;
      channelToModify.write(ByteBuffer.wrap(salt));  // write version 1

      try {
        MockIndex indexReadFail = new MockIndex(tempDirStr, scheduler, log, config, factory);
        Assert.assertFalse(true);
      } catch (StoreException e) {
        Assert.assertTrue(true);
      }

      channelToModify.truncate(0);
      byte[] addOnlyVersion = new byte[1];
      addOnlyVersion[0] = 0;
      channelToModify.write(ByteBuffer.wrap(addOnlyVersion));

      try {
        MockIndex indexEmptyLine = new MockIndex(tempDirStr, scheduler, log, config, factory);
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertTrue(true);
      }

      toModify.delete();

      indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<>();
          infos.add(new MessageInfo(blobId1, 1000));
          infos.add(new MessageInfo(blobId2, 1000, 12657));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());
      value4 = indexNew.getValue(blobId1);
      value5 = indexNew.getValue(blobId2);
      Assert.assertEquals(value4.getSize(), 1000);
      Assert.assertEquals(value4.getOffset(), toOffset(0));
      Assert.assertEquals(value5.getSize(), 1000);
      Assert.assertEquals(value5.getOffset(), toOffset(1000));
      Assert.assertEquals(value5.getExpiresAtMs(), 12657);

      // check error state. this scenario would populate the index but the contents would fail to be parsed

      indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<>();
          infos.add(new MessageInfo(blobId4, 100));
          infos.add(new MessageInfo(blobId4, 100, true));
          infos.add(new MessageInfo(blobId5, 100, Utils.Infinite_Time));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());

      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.getSize(), 100);
      Assert.assertEquals(value4.getOffset(), toOffset(100));
      Assert.assertEquals(value5.getSize(), 100);
      Assert.assertEquals(value5.getOffset(), toOffset(200));

      log.close();
      scheduler.shutdown();
      scheduler.awaitTermination(2, TimeUnit.MINUTES);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testIndexBatch() throws Exception {
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      Log log = new Log(tempDirStr, 10000, 10000, new StoreMetrics(tempDirStr, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0), flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, toOffset(100), flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, toOffset(300), flags, 12567));
      ArrayList<IndexEntry> list = new ArrayList<>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(toOffset(0), toOffset(600)));
      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), toOffset(0));
      Assert.assertEquals(value2.getOffset(), toOffset(100));
      Assert.assertEquals(value3.getOffset(), toOffset(300));

      MockId blobId4 = new MockId("id4");

      IndexValue value4 = index.getValue(blobId4);
      try {
        index.addToIndex(new IndexEntry(blobId4, value4), new FileSpan(toOffset(500), toOffset(600)));
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testIndexRead() throws Exception {
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      Log log = new Log(tempDirStr, 10000, 10000, new StoreMetrics(tempDirStr, new MetricRegistry()));
      log.appendFrom(ByteBuffer.allocate(10000));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0)));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, toOffset(100)));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, toOffset(300), 0));
      ArrayList<IndexEntry> list = new ArrayList<>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(toOffset(0), toOffset(600)));
      // simple read
      BlobReadOptions readOptions = index.getBlobReadInfo(blobId1, EnumSet.noneOf(StoreGetOptions.class));
      Assert.assertEquals(readOptions.getOffset(), 0);
      Assert.assertEquals(readOptions.getSize(), 100);
      Assert.assertEquals(readOptions.getExpiresAtMs(), -1);

      // read missing item
      try {
        index.getBlobReadInfo(new MockId("id4"), EnumSet.noneOf(StoreGetOptions.class));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(blobId2, new FileSpan(toOffset(600), toOffset(700)));
      try {
        index.getBlobReadInfo(blobId2, EnumSet.noneOf(StoreGetOptions.class));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      // read ttl expired item
      try {
        index.getBlobReadInfo(blobId3, EnumSet.noneOf(StoreGetOptions.class));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }
      //  read ttl expired item with include expired flag
      BlobReadOptions blobOptionsForExpired =
          index.getBlobReadInfo(blobId3, EnumSet.of(StoreGetOptions.Store_Include_Expired));
      Assert.assertEquals(blobOptionsForExpired.getOffset(), 300);
      Assert.assertEquals(blobOptionsForExpired.getSize(), 300);

      // try to delete or update a missing blob
      try {
        index.markAsDeleted(new MockId("id5"), new FileSpan(toOffset(800), toOffset(900)));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testMissingEntries() throws Exception {
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      Log log = new Log(tempDirStr, 10000, 10000, new StoreMetrics(tempDirStr, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0)));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, toOffset(100)));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, toOffset(300)));
      ArrayList<IndexEntry> list = new ArrayList<>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(toOffset(0), toOffset(600)));
      ArrayList<StoreKey> keys = new ArrayList<>();
      StoreKey key1 = new MockId("id4");
      keys.add(key1);
      keys.add(blobId1);
      keys.add(blobId2);
      Set<StoreKey> missing = index.findMissingKeys(keys);
      Assert.assertEquals(missing.size(), 1);
      StoreKey missingKeys = missing.iterator().next();
      Assert.assertArrayEquals(missingKeys.toBytes(), key1.toBytes());
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  //@Test
  public void testRollingIndex() throws IOException {
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      Log log = new Log(tempDirStr, 6900, 6900, new StoreMetrics(tempDirStr, new MetricRegistry()));
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "200");
      props.setProperty("store.data.flush.interval.seconds", "1");
      props.setProperty("store.data.flush.delay.seconds", "1");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      ByteBuffer buffer = ByteBuffer.allocate(6900);
      log.appendFrom(buffer);
      MockId blobId1 = new MockId("id01");
      MockId blobId2 = new MockId("id02");
      MockId blobId3 = new MockId("id03");
      MockId blobId4 = new MockId("id04");
      MockId blobId5 = new MockId("id05");
      MockId blobId6 = new MockId("id06");
      MockId blobId7 = new MockId("id07");
      MockId blobId8 = new MockId("id08");
      MockId blobId9 = new MockId("id09");
      MockId blobId10 = new MockId("id10");
      MockId blobId11 = new MockId("id11");
      MockId blobId12 = new MockId("id12");
      MockId blobId13 = new MockId("id13");
      MockId blobId14 = new MockId("id14");
      MockId blobId15 = new MockId("id15");
      MockId blobId16 = new MockId("id16");
      MockId blobId17 = new MockId("id17");
      MockId blobId18 = new MockId("id18");
      MockId blobId19 = new MockId("id19");
      MockId blobId20 = new MockId("id20");
      MockId blobId21 = new MockId("id21");
      MockId blobId22 = new MockId("id22");
      MockId blobId23 = new MockId("id23");
      MockId blobId24 = new MockId("id24");

      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0)));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, toOffset(100)));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, toOffset(300)));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(300, toOffset(600)));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(300, toOffset(900)));

      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(300, toOffset(1200)));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(300, toOffset(1500)));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(300, toOffset(1800)));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(300, toOffset(2100)));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(300, toOffset(2400)));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(300, toOffset(2700)));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(300, toOffset(3000)));

      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(300, toOffset(3300)));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(300, toOffset(3600)));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(300, toOffset(3900)));
      IndexEntry entry16 = new IndexEntry(blobId16, new IndexValue(300, toOffset(4200)));
      IndexEntry entry17 = new IndexEntry(blobId17, new IndexValue(300, toOffset(4500)));
      IndexEntry entry18 = new IndexEntry(blobId18, new IndexValue(300, toOffset(4800)));
      IndexEntry entry19 = new IndexEntry(blobId19, new IndexValue(300, toOffset(5100)));

      IndexEntry entry20 = new IndexEntry(blobId20, new IndexValue(300, toOffset(5400)));
      IndexEntry entry21 = new IndexEntry(blobId21, new IndexValue(300, toOffset(5700)));
      IndexEntry entry22 = new IndexEntry(blobId22, new IndexValue(300, toOffset(6000)));
      IndexEntry entry23 = new IndexEntry(blobId23, new IndexValue(300, toOffset(6300)));
      IndexEntry entry24 = new IndexEntry(blobId24, new IndexValue(300, toOffset(6600)));

      ArrayList<IndexEntry> list = new ArrayList<>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(toOffset(0), toOffset(600)));
      list.clear();
      list.add(entry4);
      list.add(entry5);
      list.add(entry6);
      list.add(entry7);
      list.add(entry8);
      index.addToIndex(list, new FileSpan(toOffset(600), toOffset(2100)));
      list.clear();
      list.add(entry9);
      list.add(entry10);
      list.add(entry11);
      list.add(entry12);
      index.addToIndex(list, new FileSpan(toOffset(2100), toOffset(3300)));
      list.clear();
      list.add(entry13);
      list.add(entry14);
      list.add(entry15);
      list.add(entry16);
      index.addToIndex(list, new FileSpan(toOffset(3300), toOffset(4500)));
      list.clear();
      list.add(entry17);
      list.add(entry18);
      list.add(entry19);
      index.addToIndex(list, new FileSpan(toOffset(4500), toOffset(5400)));
      list.clear();
      list.add(entry20);
      list.add(entry21);
      list.add(entry22);
      list.add(entry23);
      list.add(entry24);
      index.addToIndex(list, new FileSpan(toOffset(5400), toOffset(6900)));

      // search
      Assert.assertEquals(index.findKey(blobId20).getOffset(), toOffset(5400));
      Assert.assertEquals(index.findKey(blobId21).getOffset(), toOffset(5700));
      Thread.sleep(3000);

      Assert.assertEquals(index.findKey(blobId1).getOffset(), toOffset(0));
      Assert.assertEquals(index.findKey(blobId2).getOffset(), toOffset(100));

      index.close();
      MockIndex indexNew = new MockIndex(tempDirStr, scheduler, log, config, factory);
      Assert.assertEquals(indexNew.findKey(blobId1).getOffset(), toOffset(0));
      Assert.assertEquals(indexNew.findKey(blobId2).getOffset(), toOffset(100));
    } catch (Exception e) {
      e.printStackTrace();
      org.junit.Assert.assertTrue(false);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testExistsWithFileSpan() throws Exception {
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      Log log = new Log(tempDirStr, 2700, 2700, new StoreMetrics(tempDirStr, new MetricRegistry()));
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "200");
      props.setProperty("store.data.flush.interval.seconds", "1");
      props.setProperty("store.data.flush.delay.seconds", "1");
      props.setProperty("store.index.max.number.of.inmem.elements", "5");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      ByteBuffer buffer = ByteBuffer.allocate(2700);
      log.appendFrom(buffer);
      MockId blobId1 = new MockId("id01");
      MockId blobId2 = new MockId("id02");
      MockId blobId3 = new MockId("id03");
      MockId blobId4 = new MockId("id04");
      MockId blobId5 = new MockId("id05");
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0)));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, toOffset(100)));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, toOffset(300)));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(300, toOffset(600)));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(300, toOffset(900)));

      MockId blobId6 = new MockId("id06");
      MockId blobId7 = new MockId("id07");
      MockId blobId8 = new MockId("id08");
      MockId blobId9 = new MockId("id09");
      MockId blobId10 = new MockId("id10");
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, toOffset(1200)));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(200, toOffset(1300)));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(300, toOffset(1500)));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(300, toOffset(1800)));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(300, toOffset(2100)));
      IndexEntry entry2d = new IndexEntry(blobId2, new IndexValue(100, toOffset(2400), (byte) 1, -1));

      ArrayList<IndexEntry> list = new ArrayList<>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(toOffset(0), toOffset(600)));
      list.clear();
      list.add(entry4);
      list.add(entry5);
      index.addToIndex(list, new FileSpan(toOffset(600), toOffset(1200)));
      list.clear();

      list.clear();
      list.add(entry6);
      list.add(entry7);
      list.add(entry8);
      index.addToIndex(list, new FileSpan(toOffset(1200), toOffset(1800)));
      list.clear();
      list.add(entry9);
      list.add(entry10);
      list.add(entry2d);
      index.addToIndex(list, new FileSpan(toOffset(1800), toOffset(2700)));
      list.clear();

      // Index looks as follows:
      // offsets:  0               1200             2400
      // segments: [1, 2, 3, 4, 5] [6, 7, 8, 9, 10] [2d]

      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(toOffset(0), toOffset(200))));
      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(toOffset(101), toOffset(500))));
      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(toOffset(700), toOffset(1200))));
      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(toOffset(1000), toOffset(1500))));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(toOffset(1200), toOffset(2400))));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(toOffset(1201), toOffset(2000))));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(toOffset(1600), toOffset(2200))));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(toOffset(3000), toOffset(4000))));

      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(toOffset(0), toOffset(200))));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(toOffset(101), toOffset(500))));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(toOffset(700), toOffset(1200))));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(toOffset(1000), toOffset(1500))));
      Assert.assertFalse(index.findKey(blobId2, new FileSpan(toOffset(1000), toOffset(1500)))
          .isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertNull(index.findKey(blobId2, new FileSpan(toOffset(1201), toOffset(2000))));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(toOffset(1200), toOffset(2400))));
      Assert.assertNull(index.findKey(blobId2, new FileSpan(toOffset(1600), toOffset(2200))));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(toOffset(2600), toOffset(2900))));
      Assert.assertTrue(index.findKey(blobId2, new FileSpan(toOffset(2600), toOffset(2900)))
          .isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertTrue(index.findKey(blobId2, new FileSpan(toOffset(1600), toOffset(2900)))
          .isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertTrue(
          index.findKey(blobId2, new FileSpan(toOffset(600), toOffset(2900))).isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(toOffset(3000), toOffset(4000))));

      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(toOffset(0), toOffset(200))));
      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(toOffset(101), toOffset(500))));
      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(toOffset(700), toOffset(1200))));
      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(toOffset(1000), toOffset(1500))));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(toOffset(1200), toOffset(2400))));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(toOffset(1201), toOffset(2000))));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(toOffset(1600), toOffset(2200))));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(toOffset(3000), toOffset(4000))));

      Assert.assertNull(index.findKey(blobId6, new FileSpan(toOffset(0), toOffset(200))));
      Assert.assertNull(index.findKey(blobId6, new FileSpan(toOffset(101), toOffset(500))));
      Assert.assertNull(index.findKey(blobId6, new FileSpan(toOffset(700), toOffset(1199))));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(toOffset(1000), toOffset(1400))));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(toOffset(500), toOffset(1600))));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(toOffset(1200), toOffset(2400))));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(toOffset(1600), toOffset(2200))));
      Assert.assertNull(index.findKey(blobId6, new FileSpan(toOffset(3000), toOffset(4000))));

      Assert.assertNull(index.findKey(blobId9, new FileSpan(toOffset(0), toOffset(200))));
      Assert.assertNull(index.findKey(blobId9, new FileSpan(toOffset(101), toOffset(500))));
      Assert.assertNull(index.findKey(blobId9, new FileSpan(toOffset(700), toOffset(1199))));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(toOffset(1000), toOffset(1400))));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(toOffset(500), toOffset(1600))));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(toOffset(1200), toOffset(2400))));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(toOffset(1600), toOffset(2200))));
      Assert.assertNull(index.findKey(blobId9, new FileSpan(toOffset(3000), toOffset(4000))));

      Assert.assertNull(index.findKey(blobId10, new FileSpan(toOffset(0), toOffset(200))));
      Assert.assertNull(index.findKey(blobId10, new FileSpan(toOffset(101), toOffset(500))));
      Assert.assertNull(index.findKey(blobId10, new FileSpan(toOffset(700), toOffset(1199))));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(toOffset(1000), toOffset(1400))));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(toOffset(500), toOffset(1600))));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(toOffset(1200), toOffset(2400))));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(toOffset(1600), toOffset(2200))));
      Assert.assertNull(index.findKey(blobId10, new FileSpan(toOffset(3000), toOffset(4000))));

      index.close();
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testFindEntries() throws Exception {
    // provide empty token and ensure we get everything till max
    StoreFindToken token = new StoreFindToken();
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      StoreMetrics metrics = new StoreMetrics(tempDirStr, new MetricRegistry());
      Log log = new Log(tempDirStr, 10000, 10000, metrics);
      log.appendFrom(ByteBuffer.allocate(1600));
      Properties props = new Properties();
      props.put("store.index.max.number.of.inmem.elements", "5");
      props.put("store.max.number.of.entries.to.return.for.find", "12");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      FindInfo infoempty = index.findEntriesSince(token, 1000);
      Assert.assertEquals(infoempty.getMessageEntries().size(), 0);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");
      MockId blobId4 = new MockId("id4");
      MockId blobId5 = new MockId("id5");
      MockId blobId6 = new MockId("id6");
      MockId blobId7 = new MockId("id7");
      MockId blobId8 = new MockId("id8");
      MockId blobId9 = new MockId("id9");
      MockId blobId10 = new MockId("id10");
      MockId blobId11 = new MockId("id11");
      MockId blobId12 = new MockId("id12");
      MockId blobId13 = new MockId("id13");
      MockId blobId14 = new MockId("id14");
      MockId blobId15 = new MockId("id15");

      byte flags = 0;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0), flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(100, toOffset(100), flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(100, toOffset(200), flags, 12567));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(100, toOffset(300), flags, 12567));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(100, toOffset(400), flags, 12567));
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, toOffset(500), flags, 12567));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(100, toOffset(600), flags, 12567));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(100, toOffset(700), flags, 12567));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(100, toOffset(800), flags, 12567));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(100, toOffset(900), flags, 12567));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(100, toOffset(1000), flags, 12567));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(100, toOffset(1100), flags, 12567));
      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(100, toOffset(1200), flags, 12567));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(100, toOffset(1300), flags, 12567));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(100, toOffset(1400), flags, 12567));

      index.addToIndex(entry1, new FileSpan(toOffset(0), toOffset(100)));
      index.addToIndex(entry2, new FileSpan(toOffset(100), toOffset(200)));
      index.addToIndex(entry3, new FileSpan(toOffset(200), toOffset(300)));
      index.addToIndex(entry4, new FileSpan(toOffset(300), toOffset(400)));
      index.addToIndex(entry5, new FileSpan(toOffset(400), toOffset(500)));
      index.addToIndex(entry6, new FileSpan(toOffset(500), toOffset(600)));
      index.addToIndex(entry7, new FileSpan(toOffset(600), toOffset(700)));
      index.addToIndex(entry8, new FileSpan(toOffset(700), toOffset(800)));
      index.addToIndex(entry9, new FileSpan(toOffset(800), toOffset(900)));
      index.addToIndex(entry10, new FileSpan(toOffset(900), toOffset(1000)));
      index.addToIndex(entry11, new FileSpan(toOffset(1000), toOffset(1100)));
      index.addToIndex(entry12, new FileSpan(toOffset(1100), toOffset(1200)));
      index.addToIndex(entry13, new FileSpan(toOffset(1200), toOffset(1300)));
      index.addToIndex(entry14, new FileSpan(toOffset(1300), toOffset(1400)));
      index.addToIndex(entry15, new FileSpan(toOffset(1400), toOffset(1500)));
      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), toOffset(0));
      Assert.assertEquals(value2.getOffset(), toOffset(100));
      Assert.assertEquals(value3.getOffset(), toOffset(200));

      index.markAsDeleted(blobId1, new FileSpan(toOffset(1500), toOffset(1600)));
      index.close();
      index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      FindInfo info = index.findEntriesSince(token, 1200);
      List<MessageInfo> messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId1);
      Assert.assertEquals(messageEntries.get(0).getSize(), 100);
      Assert.assertEquals(messageEntries.get(0).getExpirationTimeInMs(), 12345);
      Assert.assertEquals(messageEntries.get(0).isDeleted(), true);
      Assert.assertEquals(messageEntries.size(), 12);
      Assert.assertEquals(messageEntries.get(messageEntries.size() - 1).getStoreKey(), blobId12);

      FindInfo info1 = index.findEntriesSince(info.getFindToken(), 400);
      messageEntries = info1.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 4);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId13);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId15);
      Assert.assertEquals(messageEntries.get(3).getStoreKey(), blobId1);

      index.close();
      props = new Properties();
      config = new StoreConfig(new VerifiableProperties(props));
      index = new MockIndex(tempDirStr, scheduler, log, config, factory);

      StoreFindToken token2 = new StoreFindToken();
      FindInfo info2 = index.findEntriesSince(token2, 300);
      messageEntries = info2.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 3);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId1);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId3);
      info2 = index.findEntriesSince(info2.getFindToken(), 300);
      messageEntries = info2.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 3);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId4);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId6);
      info2 = index.findEntriesSince(info2.getFindToken(), 300);
      messageEntries = info2.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 3);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId7);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId9);
      info2 = index.findEntriesSince(info2.getFindToken(), 300);
      messageEntries = info2.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 3);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId10);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId12);
      info2 = index.findEntriesSince(info2.getFindToken(), 300);
      messageEntries = info2.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 3);
      Assert.assertEquals(messageEntries.get(1).getStoreKey(), blobId14);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId15);
      info2 = index.findEntriesSince(info2.getFindToken(), 300);
      messageEntries = info2.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 1);
      info2 = index.findEntriesSince(info2.getFindToken(), 300);
      messageEntries = info2.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 0);
      scheduler.shutdown();
      scheduler.awaitTermination(2, TimeUnit.MINUTES);
      log.close();
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  /* Additional tests for findEntriesSince with the mock journal. Ensure token gets reset correctly and entries are
   * read correctly and from segments or the journal as expected. Also test out boundary conditions and that we never
   * read from the latest segment.
   */
  @Test
  public void testFindEntriesAdditional() throws Exception {
    // provide token referencing an offset from before
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      StoreMetrics metrics = new StoreMetrics(tempDirStr, new MetricRegistry());
      Log log = new Log(tempDirStr, 10000, 10000, metrics);
      Properties props = new Properties();
      props.put("store.index.max.number.of.inmem.elements", "5");
      props.put("store.max.number.of.entries.to.return.for.find", "12");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory,
          new MockJournal(tempDirStr, 2 * config.storeIndexMaxNumberOfInmemElements,
              config.storeMaxNumberOfEntriesToReturnFromJournal));
      MockJournal journal = (MockJournal) index.getJournal();

      MockId blobId1 = new MockId("id01");
      MockId blobId2 = new MockId("id02");
      MockId blobId3 = new MockId("id03");
      MockId blobId4 = new MockId("id04");
      MockId blobId5 = new MockId("id05");
      MockId blobId6 = new MockId("id06");
      MockId blobId7 = new MockId("id07");
      MockId blobId8 = new MockId("id08");
      MockId blobId9 = new MockId("id09");
      MockId blobId10 = new MockId("id10");
      MockId blobId11 = new MockId("id11");
      MockId blobId12 = new MockId("id12");
      MockId blobId13 = new MockId("id13");
      MockId blobId14 = new MockId("id14");
      MockId blobId15 = new MockId("id15");
      MockId blobId16 = new MockId("id16");
      MockId blobId17 = new MockId("id17");
      MockId blobId18 = new MockId("id18");
      MockId blobId19 = new MockId("id19");
      MockId blobId20 = new MockId("id20");
      MockId blobId21 = new MockId("id21");
      MockId blobId22 = new MockId("id22");
      MockId blobId23 = new MockId("id23");
      MockId blobId24 = new MockId("id24");
      MockId blobId25 = new MockId("id25");

      byte flags = 0;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0), flags, 12345));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(100, toOffset(100), flags, 12567));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(100, toOffset(200), flags, 12567));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(100, toOffset(300), flags, 12567));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(100, toOffset(400), flags, 12567));
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, toOffset(500), flags, 12567));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(100, toOffset(600), flags, 12567));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(100, toOffset(700), flags, 12567));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(100, toOffset(800), flags, 12567));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(100, toOffset(900), flags, 12567));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(100, toOffset(1000), flags, 12567));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(100, toOffset(1100), flags, 12567));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(100, toOffset(1200), flags, 12567));
      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(100, toOffset(1300), flags, 12567));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(100, toOffset(1400), flags, 12567));
      IndexEntry entry16 = new IndexEntry(blobId16, new IndexValue(100, toOffset(1500), flags, 12567));
      IndexEntry entry18 = new IndexEntry(blobId18, new IndexValue(100, toOffset(1600), flags, 12567));
      IndexEntry entry17 = new IndexEntry(blobId17, new IndexValue(100, toOffset(1700), flags, 12567));
      IndexEntry entry19 = new IndexEntry(blobId19, new IndexValue(100, toOffset(1800), flags, 12567));
      IndexEntry entry20 = new IndexEntry(blobId20, new IndexValue(100, toOffset(1900), flags, 12567));
      IndexEntry entry21 = new IndexEntry(blobId21, new IndexValue(100, toOffset(2000), flags, 12567));
      IndexEntry entry22 = new IndexEntry(blobId22, new IndexValue(100, toOffset(2100), flags, 12567));
      IndexEntry entry24 = new IndexEntry(blobId24, new IndexValue(100, toOffset(2200), flags, 12567));
      IndexEntry entry23 = new IndexEntry(blobId23, new IndexValue(100, toOffset(2300), flags, 12567));

      // Add some delete entries
      flags = 1;
      IndexEntry entry21d = new IndexEntry(blobId21, new IndexValue(100, toOffset(2400), flags, 12567));
      IndexEntry entry22d = new IndexEntry(blobId22, new IndexValue(100, toOffset(2500), flags, 12567));
      IndexEntry entry24d = new IndexEntry(blobId24, new IndexValue(100, toOffset(2600), flags, 12567));
      IndexEntry entry23d = new IndexEntry(blobId23, new IndexValue(100, toOffset(2700), flags, 12567));

      flags = 0;
      IndexEntry entry25 = new IndexEntry(blobId25, new IndexValue(100, toOffset(2800), flags, 12567));

      long entrySize = entry1.getValue().getSize();

      /* Ensure older tokens are reset, and no entries are returned when the store just started.
       * (when the index is created for the first time, it is similar to an unclean shutdown, so
       * this tests token getting reset in the wake of an unclean shutdown case) */
      StoreFindToken token = new StoreFindToken(blobId1, toOffset(1000), new UUID(0, 0));
      FindInfo info = index.findEntriesSince(token, 500);
      List<MessageInfo> mEntries = info.getMessageEntries();
      Assert.assertEquals(mEntries.size(), 0);

      /* Test the case where an entry is added to the index, but not yet to the journal.
      There can only be at most one such entry. The index ensures an entry is added to
      the journal before the next entry is added to a segment. */

      journal.pause();
      log.appendFrom(ByteBuffer.allocate(100));
      index.addToIndex(entry1, new FileSpan(toOffset(0), toOffset(100)));

      // Index_0:      [1]         // sorted on keys
      // Journal:      [ ]         // sorted on offsets
      // token before: j           // j means offset (journal) based token
      // token after : j           // i means index based offset
      token = new StoreFindToken();
      info = index.findEntriesSince(token, 100 * entrySize);
      mEntries = info.getMessageEntries();
      Assert.assertEquals(mEntries.size(), 0);

      /* Ensure we get the entry after it gets into the journal */
      journal.resume();
      token = new StoreFindToken();
      info = index.findEntriesSince(token, 100 * entrySize);
      mEntries = info.getMessageEntries();
      Assert.assertEquals(mEntries.size(), 1);
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId1);

      log.appendFrom(ByteBuffer.allocate(400));
      index.addToIndex(entry3, new FileSpan(toOffset(100), toOffset(200)));
      index.addToIndex(entry2, new FileSpan(toOffset(200), toOffset(300)));
      index.addToIndex(entry5, new FileSpan(toOffset(300), toOffset(400)));
      index.addToIndex(entry4, new FileSpan(toOffset(400), toOffset(500)));

      /* Ensure that a token from a previous session with a key that is beyond the logEndOffsetOnStartup gets reset
         correctly - meaning findEntriesSince gets keys starting and *including* the key at logEndOffsetOnStartup. */

      // Index_0:      [1 2 3 4 5]           // sorted on keys
      // Journal:      [1 3 2 5 4]           // sorted on offsets
      // token before:                    i  // i means index based token
      // token after :          j            // j means offset (journal) based token

      token = new StoreFindToken(blobId1, toOffset(1000), new UUID(0, 0));
      info = index.findEntriesSince(token, 5 * entrySize);
      mEntries = info.getMessageEntries();
      // Ensure that we got all the keys from the beginning and ordered by offset
      Assert.assertEquals(mEntries.size(), 5);
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId1);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId3);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId2);
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId5);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId4);

      // Add more entries to the index to create two new segments and push out the first 5 from the journal.
      log.appendFrom(ByteBuffer.allocate(1000));
      index.addToIndex(entry6, new FileSpan(toOffset(500), toOffset(600)));
      index.addToIndex(entry8, new FileSpan(toOffset(600), toOffset(700)));
      index.addToIndex(entry9, new FileSpan(toOffset(700), toOffset(800)));
      index.addToIndex(entry7, new FileSpan(toOffset(800), toOffset(900)));
      index.addToIndex(entry10, new FileSpan(toOffset(900), toOffset(1000)));
      index.addToIndex(entry12, new FileSpan(toOffset(1000), toOffset(1100)));
      index.addToIndex(entry11, new FileSpan(toOffset(1100), toOffset(1200)));
      index.addToIndex(entry14, new FileSpan(toOffset(1200), toOffset(1300)));
      index.addToIndex(entry13, new FileSpan(toOffset(1300), toOffset(1400)));
      index.addToIndex(entry15, new FileSpan(toOffset(1400), toOffset(1500)));

      // Get the next 5 entries. Tests the case where the token has a journal offset which is now no longer
      // present in the journal. The entries will be read from the corresponding index segment.
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15]
      // journal:                  [6 8 9 7 10   12 11 14 13 15]
      // token before:          j
      // token after:           i
      token = (StoreFindToken) info.getFindToken();
      info = index.findEntriesSince(token, 5 * entrySize);
      mEntries = info.getMessageEntries();
      Assert.assertEquals(mEntries.size(), 5);
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId1);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId2);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId3);
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId4);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId5);

      // Get the next 5 entries. Tests the case where the token is index based and has the last key of the last but one
      // segment. The entries will be read from the journal.
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15]
      // journal:                  [6 8 9 7 10   12 11 14 13 15]
      // token before:          i
      // token after:                        j
      token = (StoreFindToken) info.getFindToken();
      info = index.findEntriesSince(token, 5 * entrySize);
      mEntries = info.getMessageEntries();
      Assert.assertEquals(5, mEntries.size());
      // Ensure that they came from the journal (by verifying they are ordered by offsets)
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId6);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId8);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId9);
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId7);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId10);

      // Create a token positioned at the last key in the last but one segment.
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15]
      // journal:                  [6 8 9 7 10   12 11 14 13 15]
      // token before:  j
      // token after:           i
      token = new StoreFindToken();
      info = index.findEntriesSince(token, 5 * entrySize);

      // Add more entries to the index to create 4 segments.
      log.appendFrom(ByteBuffer.allocate(200));
      index.addToIndex(entry16, new FileSpan(toOffset(1500), toOffset(1600)));
      index.addToIndex(entry18, new FileSpan(toOffset(1600), toOffset(1700)));

      // Get the next 7 entries. Tests the case where keys are read from a segment and then from the journal.
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15] [16 18]
      // journal:                      [9 7 10   12 11 14 13 15   16 18]
      // token before:          i
      // token after:                                j
      token = (StoreFindToken) info.getFindToken();
      info = index.findEntriesSince(token, 7 * entrySize);
      mEntries = info.getMessageEntries();
      Assert.assertEquals(7, mEntries.size());

      // Ensure the first 5 are ordered by keys
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId6);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId7);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId8);
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId9);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId10);
      // Ensure the last 2 are ordered by offset
      Assert.assertEquals(mEntries.get(5).getStoreKey(), blobId12);
      Assert.assertEquals(mEntries.get(6).getStoreKey(), blobId11);

      long newLogEndOffset = entry24.getValue().getOffset().getOffset() + entry24.getValue().getSize();
      log.appendFrom(ByteBuffer.allocate((int) (newLogEndOffset - entry17.getValue().getOffset().getOffset())));
      ArrayList<IndexEntry> indexEntries = new ArrayList<>();
      indexEntries.add(entry17);
      indexEntries.add(entry19);
      indexEntries.add(entry20);
      indexEntries.add(entry21);
      indexEntries.add(entry22);
      indexEntries.add(entry24);
      index.addToIndex(indexEntries, new FileSpan(entry17.getValue().getOffset(), toOffset(newLogEndOffset)));

      /* Test batched add, and that keys are read from the segment and then from the journal for the latest ones. */
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15] [16 17 18 19 20] [21 22 24]
      // journal:                                        [13 15   16 18 17 19 20   21 22 24]
      // token before:                               j
      // token after:                                                    j
      token = (StoreFindToken) info.getFindToken();
      info = index.findEntriesSince(token, 100 * entrySize);
      mEntries = info.getMessageEntries();
      // This should get us all entries from the segment not completely in journal, ([11 - 15]) +
      // rest of the entries from the journal
      Assert.assertEquals(13, mEntries.size());
      // First 5 from the index
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId11);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId12);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId13);
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId14);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId15);
      // Last 3 from the journal
      Assert.assertEquals(mEntries.get(5).getStoreKey(), blobId16);
      Assert.assertEquals(mEntries.get(6).getStoreKey(), blobId18);
      Assert.assertEquals(mEntries.get(7).getStoreKey(), blobId17);
      Assert.assertEquals(mEntries.get(8).getStoreKey(), blobId19);
      Assert.assertEquals(mEntries.get(9).getStoreKey(), blobId20);
      Assert.assertEquals(mEntries.get(10).getStoreKey(), blobId21);
      Assert.assertEquals(mEntries.get(11).getStoreKey(), blobId22);
      Assert.assertEquals(mEntries.get(12).getStoreKey(), blobId24);

      /* Position the index at the middle of some segment. Ensure keys are read from multiple segments up to what is
         asked.*/
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15] [16 17 18 19 20] [21 22 24]
      // journal:                                        [13 15   16 18 17 19 20   21 22 24]
      // token before:                i
      // token after:                                i

      token = new StoreFindToken();
      info = index.findEntriesSince(token, 7 * entrySize);
      token = (StoreFindToken) info.getFindToken();
      info = index.findEntriesSince(token, 5 * entrySize);
      mEntries = info.getMessageEntries();
      Assert.assertEquals(5, mEntries.size());
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId8);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId9);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId10);
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId11);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId12);

      // Get the next entries. Ensure that the journal marks the end of the entries even if it is not up-to-date.
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15] [16 17 18 19 20] [21 22 23 24]
      // journal:                                        [13 15   16 18 17 19 20   21 22 24]
      // token before:                               i
      // token after:                                                                     j

      journal.pause();
      newLogEndOffset = entry23.getValue().getOffset().getOffset() + entry23.getValue().getSize();
      log.appendFrom(ByteBuffer.allocate((int) (entry23.getValue().getSize())));
      index.addToIndex(entry23, new FileSpan(entry23.getValue().getOffset(), toOffset(newLogEndOffset)));
      token = (StoreFindToken) info.getFindToken();
      info = index.findEntriesSince(token, 100 * entrySize);
      mEntries = info.getMessageEntries();
      journal.resume();

      // This should get us entries from the last segment not in journal ([13 - 15]) +
      // rest of the entries from the journal
      Assert.assertEquals(11, mEntries.size());
      // First 3 from the index
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId13);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId14);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId15);
      // Last 3 from the journal
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId16);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId18);
      Assert.assertEquals(mEntries.get(5).getStoreKey(), blobId17);
      Assert.assertEquals(mEntries.get(6).getStoreKey(), blobId19);
      Assert.assertEquals(mEntries.get(7).getStoreKey(), blobId20);
      Assert.assertEquals(mEntries.get(8).getStoreKey(), blobId21);
      Assert.assertEquals(mEntries.get(9).getStoreKey(), blobId22);
      Assert.assertEquals(mEntries.get(10).getStoreKey(), blobId24);

      // Add more entries to test the case where deletes happen to keys in the latest segment.
      // The journal keeps both entries, but the segment keeps only the delete entry.
      newLogEndOffset = entry25.getValue().getOffset().getOffset() + entry25.getValue().getSize();
      log.appendFrom(ByteBuffer.allocate((int) (newLogEndOffset - entry21d.getValue().getOffset().getOffset())));
      indexEntries.clear();
      indexEntries.add(entry21d);
      indexEntries.add(entry22d);
      indexEntries.add(entry24d);
      indexEntries.add(entry23d);
      indexEntries.add(entry25);
      index.addToIndex(indexEntries, new FileSpan(entry21d.getValue().getOffset(), toOffset(newLogEndOffset)));

      // Ensure that the journal still contains the latest segment's start offset.
      Assert.assertTrue(journal.getFirstOffset().compareTo(index.getLastSegment().getStartOffset()) <= 0);

      // Get the next entries. Ensure that entries for the last segment are obtained from the journal.
      // Index:        [1 2 3 4 5] [6 7 8 9 10] [11 12 13 14 15] [16 17 18 19 20] [21d 22d 23d 24d 25]
      // journal:                                                                 [21 22 24 23 21d 22d 24d 23d 25]
      // token before:                                                i
      // token after:                                                                                           j

      token = new StoreFindToken();
      info = index.findEntriesSince(token, 17 * entrySize); // position in the middle of the last but one segment.
      token = (StoreFindToken) info.getFindToken();
      info = index.findEntriesSince(token, 100 * entrySize);
      mEntries = info.getMessageEntries();
      Assert.assertEquals(8, mEntries.size()); // All keys are deleted so shouldn't be returned.

      // from the index
      Assert.assertEquals(mEntries.get(0).getStoreKey(), blobId18);
      Assert.assertEquals(mEntries.get(1).getStoreKey(), blobId19);
      Assert.assertEquals(mEntries.get(2).getStoreKey(), blobId20);
      // from the journal
      Assert.assertEquals(mEntries.get(3).getStoreKey(), blobId21);
      Assert.assertEquals(mEntries.get(4).getStoreKey(), blobId22);
      Assert.assertEquals(mEntries.get(5).getStoreKey(), blobId24);
      Assert.assertEquals(mEntries.get(6).getStoreKey(), blobId23);
      Assert.assertEquals(mEntries.get(7).getStoreKey(), blobId25);

      scheduler.shutdown();
      scheduler.awaitTermination(2, TimeUnit.MINUTES);
      log.close();
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testFindDeletedEntries() throws Exception {
    // provide empty token and ensure we get everything till max
    StoreFindToken token = new StoreFindToken();
    MockClusterMap map = null;
    try {
      for (File c : tempDir.listFiles()) {
        c.delete();
      }
      ScheduledExecutorService scheduler = Utils.newScheduler(1, false);
      StoreMetrics metrics = new StoreMetrics(tempDirStr, new MetricRegistry());
      Log log = new Log(tempDirStr, 10000, 10000, metrics);
      log.appendFrom(ByteBuffer.allocate(2900));
      Properties props = new Properties();
      props.put("store.index.max.number.of.inmem.elements", "5");
      props.put("store.max.number.of.entries.to.return.for.find", "12");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      FindInfo infoempty = index.findDeletedEntriesSince(token, 1000, SystemTime.getInstance().milliseconds() / 1000);
      Assert.assertEquals(infoempty.getMessageEntries().size(), 0);
      MockId blobId1 = new MockId("id01");
      MockId blobId2 = new MockId("id02");
      MockId blobId3 = new MockId("id03");
      MockId blobId4 = new MockId("id04");
      MockId blobId5 = new MockId("id05");
      MockId blobId6 = new MockId("id06");
      MockId blobId7 = new MockId("id07");
      MockId blobId8 = new MockId("id08");
      MockId blobId9 = new MockId("id09");
      MockId blobId10 = new MockId("id10");
      MockId blobId11 = new MockId("id11");
      MockId blobId12 = new MockId("id12");
      MockId blobId13 = new MockId("id13");
      MockId blobId14 = new MockId("id14");
      MockId blobId15 = new MockId("id15");
      MockId blobId16 = new MockId("id16");
      MockId blobId17 = new MockId("id17");
      MockId blobId18 = new MockId("id18");
      MockId blobId19 = new MockId("id19");

      byte flags = 0;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, toOffset(0), flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(100, toOffset(100), flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(100, toOffset(200), flags, 12567));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(100, toOffset(300), flags, 12567));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(100, toOffset(500), flags, 12567));
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, toOffset(600), flags, 12567));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(100, toOffset(700), flags, 12567));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(100, toOffset(800), flags, 12567));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(100, toOffset(900), flags, 12567));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(100, toOffset(1100), flags, 12567));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(100, toOffset(1200), flags, 12567));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(100, toOffset(1500), flags, 12567));
      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(100, toOffset(1600), flags, 12567));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(100, toOffset(1700), flags, 12567));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(100, toOffset(1900), flags, 12567));
      IndexEntry entry16 = new IndexEntry(blobId16, new IndexValue(100, toOffset(2300), flags, 12567));
      IndexEntry entry17 = new IndexEntry(blobId17, new IndexValue(100, toOffset(2400), flags, 12567));
      IndexEntry entry18 = new IndexEntry(blobId18, new IndexValue(100, toOffset(2500), flags, 12567));
      IndexEntry entry19 = new IndexEntry(blobId19, new IndexValue(100, toOffset(2800), flags, 12567));

      //segment 1
      index.addToIndex(entry1, new FileSpan(toOffset(0), toOffset(100)));
      index.addToIndex(entry2, new FileSpan(toOffset(100), toOffset(200)));
      index.addToIndex(entry3, new FileSpan(toOffset(200), toOffset(300)));
      index.addToIndex(entry4, new FileSpan(toOffset(300), toOffset(400)));
      index.markAsDeleted(blobId2, new FileSpan(toOffset(400), toOffset(500)));
      index.addToIndex(entry5, new FileSpan(toOffset(500), toOffset(600)));
      //Segment 1: [1 2d 3 4 5]

      //segment 2
      index.addToIndex(entry6, new FileSpan(toOffset(600), toOffset(700)));
      index.addToIndex(entry7, new FileSpan(toOffset(700), toOffset(800)));
      index.addToIndex(entry8, new FileSpan(toOffset(800), toOffset(900)));
      index.addToIndex(entry9, new FileSpan(toOffset(900), toOffset(1000)));
      index.markAsDeleted(blobId1, new FileSpan(toOffset(1000), toOffset(1100)));
      //Segment 2: [1d 6 7 8 9]

      //segment 3
      index.addToIndex(entry10, new FileSpan(toOffset(1100), toOffset(1200)));
      index.addToIndex(entry11, new FileSpan(toOffset(1200), toOffset(1300)));
      index.markAsDeleted(blobId10, new FileSpan(toOffset(1300), toOffset(1400)));
      index.markAsDeleted(blobId6, new FileSpan(toOffset(1400), toOffset(1500)));
      index.addToIndex(entry12, new FileSpan(toOffset(1500), toOffset(1600)));
      index.addToIndex(entry13, new FileSpan(toOffset(1600), toOffset(1700)));
      //Segment 3: [6d 10d 11 12 13]

      //segment 4
      index.addToIndex(entry14, new FileSpan(toOffset(1700), toOffset(1800)));
      index.markAsDeleted(blobId4, new FileSpan(toOffset(1800), toOffset(1900))); // <- earliest journal entry.
      index.addToIndex(entry15, new FileSpan(toOffset(1900), toOffset(2000)));
      index.markAsDeleted(blobId12, new FileSpan(toOffset(2000), toOffset(2100)));
      index.markAsDeleted(blobId15, new FileSpan(toOffset(2100), toOffset(2200)));
      index.markAsDeleted(blobId7, new FileSpan(toOffset(2200), toOffset(2300)));
      //Segment 4: [4d 7d 12d 14 15d]

      //segment 5
      index.addToIndex(entry16, new FileSpan(toOffset(2300), toOffset(2400)));
      index.addToIndex(entry17, new FileSpan(toOffset(2400), toOffset(2500)));
      index.addToIndex(entry18, new FileSpan(toOffset(2500), toOffset(2600)));
      index.markAsDeleted(blobId18, new FileSpan(toOffset(2600), toOffset(2700)));
      index.markAsDeleted(blobId17, new FileSpan(toOffset(2700), toOffset(2800)));
      index.addToIndex(entry19, new FileSpan(toOffset(2800), toOffset(2900)));
      //segment 5: [16 17d 18d 19]

      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), toOffset(1000));
      Assert.assertEquals(value1.getOriginalMessageOffset(), 0);
      Assert.assertEquals(value2.getOffset(), toOffset(400));
      Assert.assertEquals(value2.getOriginalMessageOffset(), 100);
      Assert.assertEquals(value3.getOffset(), toOffset(200));
      Assert.assertEquals(value3.getOriginalMessageOffset(), 200);

      index.close();
      index = new MockIndex(tempDirStr, scheduler, log, config, factory);

      //Segment 1: [*1* 2d 3 4 5]
      //Segment 2: [1d 6 7 8 9]
      //Segment 3: [6d 10d 11 12 13]
      //Segment 4: [4d 7d 12d 14 15d]
      //segment 5: [16 17d 18d 19]
      FindInfo info = index.findDeletedEntriesSince(token, 500, SystemTime.getInstance().milliseconds() / 1000);
      List<MessageInfo> messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 1);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId2);
      Assert.assertEquals(messageEntries.get(0).getSize(), 100);
      Assert.assertEquals(messageEntries.get(0).getExpirationTimeInMs(), 12567);
      Assert.assertEquals(messageEntries.get(0).isDeleted(), true);
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getStoreKey(), blobId5);

      //Segment 1: [1 2d 3 4 *5*]
      //Segment 2: [1d 6 7 8 9]
      //Segment 3: [6d 10d 11 12 13]
      //Segment 4: [4d 7d 12d 14 15d]
      //segment 5: [16 17d 18d 19]
      info = index.findDeletedEntriesSince(info.getFindToken(), 200, SystemTime.getInstance().milliseconds() / 1000);
      messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 1);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId1);
      Assert.assertEquals(messageEntries.get(0).getSize(), 100);
      Assert.assertEquals(messageEntries.get(0).getExpirationTimeInMs(), 12345);
      Assert.assertEquals(messageEntries.get(0).isDeleted(), true);
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getStoreKey(), blobId6);

      //Segment 1: [1 2d 3 4 5]
      //Segment 2: [1d *6* 7 8 9]
      //Segment 3: [6d 10d 11 12 13]
      //Segment 4: [4d 7d 12d 14 15d]
      //segment 5: [16 17d 18d 19]
      info = index.findDeletedEntriesSince(info.getFindToken(), 300, SystemTime.getInstance().milliseconds() / 1000);
      messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 0);
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getStoreKey(), blobId9);

      //Segment 1: [1 2d 3 4 5]
      //Segment 2: [1d 6 7 8 *9*]
      //Segment 3: [6d 10d 11 12 13]
      //Segment 4: [4d 7d 12d 14 15d]
      //segment 5: [16 17d 18d 19]
      info = index.findDeletedEntriesSince(info.getFindToken(), 700, SystemTime.getInstance().milliseconds() / 1000);
      messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 4);
      // in the order in the segment
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId6);
      Assert.assertEquals(messageEntries.get(1).getStoreKey(), blobId10);
      // these come from the next segment
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId4);
      Assert.assertEquals(messageEntries.get(3).getStoreKey(), blobId7);
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getStoreKey(), blobId7);

      //Segment 1: [1 2d 3 4 5]
      //Segment 2: [1d 6 7 8 9]
      //Segment 3: [6d 10d 11 12 13]
      //Segment 4: [4d *7d* 12d 14 15d]
      //segment 5: [16 17d 18d 19] : in the journal 18d comes before 17d. In messageEntries, 17 comes before 18
      // as we would have had ...17 18 18d 17d... from the journal, and then we lookup and update delete status of
      // keys in that order when filtering out non-delete entries.
      info = index.findDeletedEntriesSince(info.getFindToken(), 500, SystemTime.getInstance().milliseconds() / 1000);
      messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 3);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId12);
      Assert.assertEquals(messageEntries.get(1).getStoreKey(), blobId15);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId17);
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getStoreKey(), null);
      // journal based token with offset of blobid17
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getOffset(), toOffset(2400));

      //Segment 1: [1 2d 3 4 5]
      //Segment 2: [1d 6 7 8 9]
      //Segment 3: [6d 10d 11 12 13]
      //Segment 4: [4d 7d 12d 14 15d]
      //segment 5: [16 17d 18d 19] : in the journal 18d comes before 17d.
      //journal: .... 17 18 18d 17d
      //this time we read from the journal, so 18d will come before 17d.
      info = index.findDeletedEntriesSince(info.getFindToken(), 700, SystemTime.getInstance().milliseconds() / 1000);
      messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 2);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId18);
      Assert.assertEquals(messageEntries.get(1).getStoreKey(), blobId17);

      //Test end time

      index.close();
      index = new MockIndex(tempDirStr, scheduler, log, config, factory);

      AtomicLong beforeSegment5LastModification = new AtomicLong(index.getLastSegment().getLastModifiedTime());
      // Wait long enough for the current time in seconds to be greater than the above time. In the future we
      // should mock time to do this in a cleaner way.
      Thread.sleep(1000);

      log.appendFrom(ByteBuffer.allocate(200));
      index.markAsDeleted(blobId16, new FileSpan(toOffset(2900), toOffset(3000)));
      index.markAsDeleted(blobId5, new FileSpan(toOffset(3000), toOffset(3100)));
      index.close();
      index = new MockIndex(tempDirStr, scheduler, log, config, factory);
      //Segment 1: [1 2d 3 4 5]
      //Segment 2: [1d 6 7 8 9]
      //Segment 3: [6d 10d 11 12 13]
      //Segment 4: [4d 7d 12d 14 15d] // lastmodified time < beforeSegment5LastModification
      //segment 5: [5d 16d 17d 18 19d] //lastmodified time > beforeSegment5LastModification
      info = index.findDeletedEntriesSince(new StoreFindToken(), 4000, beforeSegment5LastModification.get());
      messageEntries = info.getMessageEntries();
      Assert.assertEquals(messageEntries.size(), 8);
      Assert.assertEquals(messageEntries.get(0).getStoreKey(), blobId2);
      Assert.assertEquals(messageEntries.get(1).getStoreKey(), blobId1);
      Assert.assertEquals(messageEntries.get(2).getStoreKey(), blobId6);
      Assert.assertEquals(messageEntries.get(3).getStoreKey(), blobId10);
      Assert.assertEquals(messageEntries.get(4).getStoreKey(), blobId4);
      Assert.assertEquals(messageEntries.get(5).getStoreKey(), blobId7);
      Assert.assertEquals(messageEntries.get(6).getStoreKey(), blobId12);
      Assert.assertEquals(messageEntries.get(7).getStoreKey(), blobId15);
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getStoreKey(), blobId15);

      // just close and open again to execute the shutdown logic once again.
      index.close();
      index = new MockIndex(tempDirStr, scheduler, log, config, factory);

      // close and cleanup for good.
      index.close();
      scheduler.shutdown();
      scheduler.awaitTermination(2, TimeUnit.MINUTES);
      log.close();
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }
}

class MockIndex extends PersistentIndex {

  public MockIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory, MessageStoreHardDelete messageStoreHardDelete, Time time) throws StoreException {
    super(datadir, scheduler, log, config, factory, new DummyMessageStoreRecovery(), messageStoreHardDelete,
        new StoreMetrics(datadir, new MetricRegistry()), time);
  }

  public MockIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory) throws StoreException {
    this(datadir, scheduler, log, config, factory, new DummyMessageStoreHardDelete(), SystemTime.getInstance());
  }

  public MockIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory, Journal journal) throws StoreException {
    super(datadir, scheduler, log, config, factory, new DummyMessageStoreRecovery(), new DummyMessageStoreHardDelete(),
        new StoreMetrics(datadir, new MetricRegistry()), journal, SystemTime.getInstance());
  }

  public void setHardDeleteRunningStatus(boolean status) {
    super.hardDeleter.running.set(status);
  }

  public boolean hardDelete() throws StoreException {
    return super.hardDeleter.hardDelete();
  }

  public void persistAndAdvanceStartTokenSafeToPersist() {
    super.hardDeleter.preLogFlush();
    // no flushing to do.
    super.hardDeleter.postLogFlush();
  }

  public void pruneHardDeleteRecoveryRange() {
    super.hardDeleter.pruneHardDeleteRecoveryRange();
  }

  public void performHardDeleteRecovery() throws StoreException {
    super.hardDeleter.performRecovery();
  }

  public MockIndex(String datadir, ScheduledExecutorService scheduler, Log log, StoreConfig config,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete cleanup) throws StoreException {
    super(datadir, scheduler, log, config, factory, recovery, cleanup, new StoreMetrics(datadir, new MetricRegistry()),
        SystemTime.getInstance());
  }

  IndexValue getValue(StoreKey key) throws StoreException {
    return findKey(key);
  }

  public void deleteAll() {
    indexes.clear();
  }

  public void stopScheduler() throws InterruptedException {
    scheduler.shutdown();
    scheduler.awaitTermination(2, TimeUnit.MINUTES);
  }

  public Journal getJournal() {
    return super.journal;
  }

  public IndexSegment getLastSegment() {
    return super.indexes.lastEntry().getValue();
  }
}

