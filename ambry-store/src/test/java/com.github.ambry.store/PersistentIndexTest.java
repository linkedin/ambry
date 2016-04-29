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
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class PersistentIndexTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Create a temporary file
   */
  File tempFile()
      throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  class MockIndex extends PersistentIndex {
    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config, StoreKeyFactory factory,
        MessageStoreHardDelete messageStoreHardDelete, Time time)
        throws StoreException {
      super(datadir, scheduler, log, config, factory, new DummyMessageStoreRecovery(), messageStoreHardDelete,
          new StoreMetrics(datadir, new MetricRegistry()), time);
    }

    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config, StoreKeyFactory factory)
        throws StoreException {
      this(datadir, scheduler, log, config, factory, new DummyMessageStoreHardDelete(), SystemTime.getInstance());
    }

    public void setHardDeleteRunningStatus(boolean status) {
      super.hardDeleter.running.set(status);
    }

    public boolean hardDelete()
        throws StoreException {
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

    public void performHardDeleteRecovery()
        throws StoreException {
      super.hardDeleter.performRecovery();
    }

    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config, StoreKeyFactory factory,
        MessageStoreRecovery recovery, MessageStoreHardDelete cleanup)
        throws StoreException {
      super(datadir, scheduler, log, config, factory, recovery, cleanup,
          new StoreMetrics(datadir, new MetricRegistry()), SystemTime.getInstance());
    }

    IndexValue getValue(StoreKey key)
        throws StoreException {
      return findKey(key);
    }

    public void deleteAll() {
      indexes.clear();
    }

    public void stopScheduler() {
      scheduler.shutdown();
    }

    public boolean isEmpty() {
      return indexes.size() == 0;
    }

    public Journal getJournal() {
      return super.journal;
    }

    public IndexSegment getLastSegment() {
      return super.indexes.lastEntry().getValue();
    }
  }

  @Test
  public void testSegmentInfo()
      throws IOException {
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
      IndexSegment info = new IndexSegment(tempFile().getParent(), 0, factory, blobId1.sizeInBytes(),
          IndexValue.Index_Value_Size_In_Bytes, config, new StoreMetrics(tempFile().getParent(), new MetricRegistry()));
      IndexValue value = new IndexValue(1000, 0, (byte) 0);
      info.addEntry(new IndexEntry(blobId1, value), 1000);
      value = new IndexValue(1000, 1000, (byte) 0);
      info.addEntry(new IndexEntry(blobId2, value), 2000);
      value = new IndexValue(1000, 2000, (byte) 0);
      info.addEntry(new IndexEntry(blobId3, value), 3000);
      value = new IndexValue(1000, 3000, (byte) 0);
      info.addEntry(new IndexEntry(blobId4, value), 4000);
      value = new IndexValue(1000, 4000, (byte) 0);
      info.addEntry(new IndexEntry(blobId5, value), 5000);
      value = new IndexValue(1000, 5000, (byte) 0);
      info.addEntry(new IndexEntry(blobId6, value), 6000);
      value = new IndexValue(1000, 6000, (byte) 0);
      info.addEntry(new IndexEntry(blobId7, value), 7000);
      value = new IndexValue(1000, 7000, (byte) 0);
      info.addEntry(new IndexEntry(blobId8, value), 8000);
      value = new IndexValue(1000, 8000, (byte) 0);
      info.addEntry(new IndexEntry(blobId9, value), 9000);

      Assert.assertEquals(info.find(blobId1).getSize(), 1000);
      Assert.assertEquals(info.find(blobId1).getOffset(), 0);
      Assert.assertEquals(info.find(blobId2).getSize(), 1000);
      Assert.assertEquals(info.find(blobId2).getOffset(), 1000);
      Assert.assertEquals(info.find(blobId3).getSize(), 1000);
      Assert.assertEquals(info.find(blobId3).getOffset(), 2000);
      Assert.assertEquals(info.find(blobId4).getSize(), 1000);
      Assert.assertEquals(info.find(blobId4).getOffset(), 3000);
      Assert.assertEquals(info.find(blobId5).getSize(), 1000);
      Assert.assertEquals(info.find(blobId5).getOffset(), 4000);
      Assert.assertEquals(info.find(blobId6).getSize(), 1000);
      Assert.assertEquals(info.find(blobId6).getOffset(), 5000);
      Assert.assertEquals(info.find(blobId7).getSize(), 1000);
      Assert.assertEquals(info.find(blobId7).getOffset(), 6000);
      Assert.assertEquals(info.find(blobId8).getSize(), 1000);
      Assert.assertEquals(info.find(blobId8).getOffset(), 7000);

      // test getEntriesSince
      List<MessageInfo> entries = new ArrayList<MessageInfo>();
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

      info.writeIndexToFile(9000);
      StoreMetrics metrics = new StoreMetrics(info.getFile().getAbsolutePath(), new MetricRegistry());
      InMemoryJournal journal = new InMemoryJournal("test", 5, 5);
      IndexSegment infonew = new IndexSegment(info.getFile(), false, factory, config, metrics, journal);
      Assert.assertEquals(infonew.find(blobId1).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId1).getOffset(), 0);
      Assert.assertEquals(infonew.find(blobId2).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId2).getOffset(), 1000);
      Assert.assertEquals(infonew.find(blobId3).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId3).getOffset(), 2000);
      Assert.assertEquals(infonew.find(blobId4).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId4).getOffset(), 3000);
      Assert.assertEquals(infonew.find(blobId5).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId5).getOffset(), 4000);
      Assert.assertEquals(infonew.find(blobId6).getSize(), 1000);
      Assert.assertEquals(info.find(blobId6).getOffset(), 5000);
      Assert.assertEquals(infonew.find(blobId7).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId7).getOffset(), 6000);
      Assert.assertEquals(infonew.find(blobId8).getSize(), 1000);
      Assert.assertEquals(infonew.find(blobId8).getOffset(), 7000);

      info.map(false);
      Assert.assertEquals(info.find(blobId1).getSize(), 1000);
      Assert.assertEquals(info.find(blobId1).getOffset(), 0);
      Assert.assertEquals(info.find(blobId2).getSize(), 1000);
      Assert.assertEquals(info.find(blobId2).getOffset(), 1000);
      Assert.assertEquals(info.find(blobId3).getSize(), 1000);
      Assert.assertEquals(info.find(blobId3).getOffset(), 2000);
      Assert.assertEquals(info.find(blobId4).getSize(), 1000);
      Assert.assertEquals(info.find(blobId4).getOffset(), 3000);
      Assert.assertEquals(info.find(blobId5).getSize(), 1000);
      Assert.assertEquals(info.find(blobId5).getOffset(), 4000);
      Assert.assertEquals(info.find(blobId6).getSize(), 1000);
      Assert.assertEquals(info.find(blobId6).getOffset(), 5000);
      Assert.assertEquals(info.find(blobId7).getSize(), 1000);
      Assert.assertEquals(info.find(blobId7).getOffset(), 6000);
      Assert.assertEquals(info.find(blobId8).getSize(), 1000);
      Assert.assertEquals(info.find(blobId8).getOffset(), 7000);

      // test getEntriesSince
      entries = new ArrayList<MessageInfo>();
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
    } catch (Exception e) {
      Assert.assertTrue(false);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testIndexBasic()
      throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 10000, new StoreMetrics(logFile, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0, flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, 100, flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, 300, flags, 12567));
      index.addToIndex(entry1, new FileSpan(0, 100));
      index.addToIndex(entry2, new FileSpan(100, 300));
      index.addToIndex(entry3, new FileSpan(300, 600));
      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 100);
      Assert.assertEquals(value3.getOffset(), 300);
      indexFile.delete();
      scheduler.shutdown();
      log.close();
    } catch (Exception e) {
      org.junit.Assert.assertEquals(false, true);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testIndexRestore()
      throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 10000, new StoreMetrics(logFile, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      final MockId blobId1 = new MockId("id1");
      final MockId blobId2 = new MockId("id2");
      final MockId blobId3 = new MockId("id3");
      final MockId blobId4 = new MockId("id4");
      final MockId blobId5 = new MockId("id5");
      final MockId blobId6 = new MockId("id6");
      final MockId blobId7 = new MockId("id7");

      ByteBuffer buffer = ByteBuffer.allocate(7000);
      log.appendFrom(buffer);
      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(3000, 0, flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(1000, 3000, flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(1000, 4000, flags, 12567));
      index.addToIndex(entry1, new FileSpan(0, 3000));
      index.addToIndex(entry2, new FileSpan(3000, 4000));
      index.addToIndex(entry3, new FileSpan(4000, 5000));
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile, scheduler, log, config, factory);

      IndexValue value1 = indexNew.getValue(blobId1);
      IndexValue value2 = indexNew.getValue(blobId2);
      IndexValue value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 3000);
      Assert.assertEquals(value3.getOffset(), 4000);
      indexNew.close();

      // create a new index, persist, add more entries and fail. ensure new index restore
      // removes extra rows
      Properties props = new Properties();
      props.put("store.data.flush.delay.seconds", "999999");
      config = new StoreConfig(new VerifiableProperties(props));
      indexNew = new MockIndex(logFile, scheduler, log, config, factory);
      indexNew.addToIndex(new IndexEntry(blobId4, new IndexValue(1000, 5000, 12657)), new FileSpan(5000, 6000));
      indexNew.addToIndex(new IndexEntry(blobId5, new IndexValue(1000, 6000, 12657)), new FileSpan(6000, 7000));
      try {
        indexNew.close();
        Assert.assertFalse("Should have thrown StoreException since index has new entires compared to log", true);
      } catch (StoreException e) {
        Assert.assertTrue("StoreException thrown as expected ", true);
      }
      indexNew = new MockIndex(logFile, scheduler, log, config, factory);
      value1 = indexNew.getValue(blobId1);
      value2 = indexNew.getValue(blobId2);
      value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 3000);
      Assert.assertEquals(value3.getOffset(), 4000);
      IndexValue value4 = indexNew.getValue(blobId4);
      IndexValue value5 = indexNew.getValue(blobId5);
      Assert.assertNull(value4);
      Assert.assertNull(value5);
      indexNew.close();

      buffer = ByteBuffer.allocate(2000);
      log.appendFrom(buffer);
      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId6, 1000));
          infos.add(new MessageInfo(blobId7, 1000, 12657));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());
      IndexValue value6 = indexNew.getValue(blobId6);
      IndexValue value7 = indexNew.getValue(blobId7);
      Assert.assertEquals(value6.getSize(), 1000);
      Assert.assertEquals(value6.getOffset(), 5000);
      Assert.assertEquals(value7.getSize(), 1000);
      Assert.assertEquals(value7.getOffset(), 6000);
      Assert.assertEquals(value7.getTimeToLiveInMs(), 12657);
      Assert.assertEquals(log.getLogEndOffset(), 7000);
      indexNew.close();

      buffer = ByteBuffer.allocate(1000);
      log.appendFrom(buffer);
      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId6, 100, true));
          infos.add(new MessageInfo(blobId7, 100, true));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());
      value6 = indexNew.getValue(blobId6);
      value7 = indexNew.getValue(blobId7);
      Assert.assertEquals(value6.isFlagSet(IndexValue.Flags.Delete_Index), true);
      Assert.assertEquals(value7.getTimeToLiveInMs(), 12657);
      Assert.assertEquals(value6.getSize(), 100);
      Assert.assertEquals(value6.getOriginalMessageOffset(), 5000);
      Assert.assertEquals(value6.getOffset(), 7000);
      Assert.assertEquals(value7.getSize(), 100);
      Assert.assertEquals(value7.getOriginalMessageOffset(), 6000);
      Assert.assertEquals(value7.getOffset(), 7100);
      indexNew.stopScheduler();
      indexNew.deleteAll();
      indexNew.close();

      File toModify = new File(logFile, "0_index");
      FileChannel channelToModify = Utils.openChannel(toModify, true);
      channelToModify.truncate(0);
      channelToModify.force(true);
      scheduler.startup();

      try {
        MockIndex indexFail = new MockIndex(logFile, scheduler, log, config, factory);
        Assert.assertFalse(true);
      } catch (StoreException e) {
        Assert.assertTrue(true);
      }

      byte[] salt = new byte[1];
      salt[0] = 1;
      channelToModify.write(ByteBuffer.wrap(salt));  // write version 1

      try {
        MockIndex indexReadFail = new MockIndex(logFile, scheduler, log, config, factory);
        Assert.assertFalse(true);
      } catch (StoreException e) {
        Assert.assertTrue(true);
      }

      channelToModify.truncate(0);
      byte[] addOnlyVersion = new byte[1];
      addOnlyVersion[0] = 0;
      channelToModify.write(ByteBuffer.wrap(addOnlyVersion));

      try {
        MockIndex indexEmptyLine = new MockIndex(logFile, scheduler, log, config, factory);
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertTrue(true);
      }

      toModify.delete();

      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId1, 1000));
          infos.add(new MessageInfo(blobId2, 1000, 12657));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());
      value4 = indexNew.getValue(blobId1);
      value5 = indexNew.getValue(blobId2);
      Assert.assertEquals(value4.getSize(), 1000);
      Assert.assertEquals(value4.getOffset(), 0);
      Assert.assertEquals(value5.getSize(), 1000);
      Assert.assertEquals(value5.getOffset(), 1000);
      Assert.assertEquals(value5.getTimeToLiveInMs(), 12657);

      // check error state. this scenario would populate the index but the contents would fail to be parsed

      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId4, 100, true));
          infos.add(new MessageInfo(blobId5, 100, Utils.Infinite_Time));
          return infos;
        }
      }, new DummyMessageStoreHardDelete());

      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.getSize(), 100);
      Assert.assertEquals(value4.getOffset(), 0);
      Assert.assertEquals(value5.getSize(), 100);
      Assert.assertEquals(value5.getOffset(), 100);

      log.close();
      scheduler.shutdown();
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
  public void testIndexBatch()
      throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 10000, new StoreMetrics(logFile, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0, flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, 100, flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, 300, flags, 12567));
      ArrayList<IndexEntry> list = new ArrayList<IndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(0, 600));
      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 100);
      Assert.assertEquals(value3.getOffset(), 300);

      MockId blobId4 = new MockId("id4");

      IndexValue value4 = index.getValue(blobId4);
      try {
        index.addToIndex(new IndexEntry(blobId4, value4), new FileSpan(500, 600));
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
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
  public void testIndexRead()
      throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log log = new Log(logFile, 10000, new StoreMetrics(logFile, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, 100));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, 300, 0));
      ArrayList<IndexEntry> list = new ArrayList<IndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(0, 600));
      // simple read
      BlobReadOptions readOptions = index.getBlobReadInfo(blobId1, EnumSet.noneOf(StoreGetOptions.class));
      Assert.assertEquals(readOptions.getOffset(), 0);
      Assert.assertEquals(readOptions.getSize(), 100);
      Assert.assertEquals(readOptions.getTTL(), -1);

      // read missing item
      try {
        index.getBlobReadInfo(new MockId("id4"), EnumSet.noneOf(StoreGetOptions.class));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(blobId2, new FileSpan(600, 700));
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
        index.markAsDeleted(new MockId("id5"), new FileSpan(800, 900));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
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
  public void testMissingEntries()
      throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log log = new Log(logFile, 10000, new StoreMetrics(logFile, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, 100));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, 300));
      ArrayList<IndexEntry> list = new ArrayList<IndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(0, 600));
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      StoreKey key1 = new MockId("id4");
      keys.add(key1);
      keys.add(blobId1);
      keys.add(blobId2);
      Set<StoreKey> missing = index.findMissingKeys(keys);
      Assert.assertEquals(missing.size(), 1);
      StoreKey missingKeys = missing.iterator().next();
      Assert.assertArrayEquals(missingKeys.toBytes(), key1.toBytes());
    } catch (Exception e) {
      Assert.assertTrue(false);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testRollingIndex() {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 6900, new StoreMetrics(logFile, new MetricRegistry()));
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "200");
      props.setProperty("store.data.flush.interval.seconds", "1");
      props.setProperty("store.data.flush.delay.seconds", "1");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
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

      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, 100));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, 300));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(300, 600));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(300, 900));

      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(300, 1200));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(300, 1500));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(300, 1800));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(300, 2100));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(300, 2400));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(300, 2700));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(300, 3000));

      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(300, 3300));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(300, 3600));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(300, 3900));
      IndexEntry entry16 = new IndexEntry(blobId16, new IndexValue(300, 4200));
      IndexEntry entry17 = new IndexEntry(blobId17, new IndexValue(300, 4500));
      IndexEntry entry18 = new IndexEntry(blobId18, new IndexValue(300, 4800));
      IndexEntry entry19 = new IndexEntry(blobId19, new IndexValue(300, 5100));

      IndexEntry entry20 = new IndexEntry(blobId20, new IndexValue(300, 5400));
      IndexEntry entry21 = new IndexEntry(blobId21, new IndexValue(300, 5700));
      IndexEntry entry22 = new IndexEntry(blobId22, new IndexValue(300, 6000));
      IndexEntry entry23 = new IndexEntry(blobId23, new IndexValue(300, 6300));
      IndexEntry entry24 = new IndexEntry(blobId24, new IndexValue(300, 6600));

      ArrayList<IndexEntry> list = new ArrayList<IndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(0, 600));
      list.clear();
      list.add(entry4);
      list.add(entry5);
      list.add(entry6);
      list.add(entry7);
      list.add(entry8);
      index.addToIndex(list, new FileSpan(600, 2100));
      list.clear();
      list.add(entry9);
      list.add(entry10);
      list.add(entry11);
      list.add(entry12);
      index.addToIndex(list, new FileSpan(2100, 3300));
      list.clear();
      list.add(entry13);
      list.add(entry14);
      list.add(entry15);
      list.add(entry16);
      index.addToIndex(list, new FileSpan(3300, 4500));
      list.clear();
      list.add(entry17);
      list.add(entry18);
      list.add(entry19);
      index.addToIndex(list, new FileSpan(4500, 5400));
      list.clear();
      list.add(entry20);
      list.add(entry21);
      list.add(entry22);
      list.add(entry23);
      list.add(entry24);
      index.addToIndex(list, new FileSpan(5400, 6900));

      // search
      Assert.assertEquals(index.findKey(blobId20).getOffset(), 5400);
      Assert.assertEquals(index.findKey(blobId21).getOffset(), 5700);
      Thread.sleep(3000);

      Assert.assertEquals(index.findKey(blobId1).getOffset(), 0);
      Assert.assertEquals(index.findKey(blobId2).getOffset(), 100);

      index.close();
      MockIndex indexNew = new MockIndex(logFile, scheduler, log, config, factory);
      Assert.assertEquals(indexNew.findKey(blobId1).getOffset(), 0);
      Assert.assertEquals(indexNew.findKey(blobId2).getOffset(), 100);
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
  public void testExistsWithFileSpan() {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 2700, new StoreMetrics(logFile, new MetricRegistry()));
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "200");
      props.setProperty("store.data.flush.interval.seconds", "1");
      props.setProperty("store.data.flush.delay.seconds", "1");
      props.setProperty("store.index.max.number.of.inmem.elements", "5");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      ByteBuffer buffer = ByteBuffer.allocate(2700);
      log.appendFrom(buffer);
      MockId blobId1 = new MockId("id01");
      MockId blobId2 = new MockId("id02");
      MockId blobId3 = new MockId("id03");
      MockId blobId4 = new MockId("id04");
      MockId blobId5 = new MockId("id05");
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, 100));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, 300));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(300, 600));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(300, 900));

      MockId blobId6 = new MockId("id06");
      MockId blobId7 = new MockId("id07");
      MockId blobId8 = new MockId("id08");
      MockId blobId9 = new MockId("id09");
      MockId blobId10 = new MockId("id10");
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, 1200));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(200, 1300));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(300, 1500));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(300, 1800));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(300, 2100));
      IndexEntry entry2d = new IndexEntry(blobId2, new IndexValue(100, 2400, (byte) 1, -1));

      ArrayList<IndexEntry> list = new ArrayList<IndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(0, 600));
      list.clear();
      list.add(entry4);
      list.add(entry5);
      index.addToIndex(list, new FileSpan(600, 1200));
      list.clear();

      list.clear();
      list.add(entry6);
      list.add(entry7);
      list.add(entry8);
      index.addToIndex(list, new FileSpan(1200, 1800));
      list.clear();
      list.add(entry9);
      list.add(entry10);
      list.add(entry2d);
      index.addToIndex(list, new FileSpan(1800, 2700));
      list.clear();

      // Index looks as follows:
      // offsets:  0               1200             2400
      // segments: [1, 2, 3, 4, 5] [6, 7, 8, 9, 10] [2d]

      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(0, 200)));
      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(101, 500)));
      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(700, 1200)));
      Assert.assertNotNull(index.findKey(blobId1, new FileSpan(1000, 1500)));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(1200, 2400)));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(1201, 2000)));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(1600, 2200)));
      Assert.assertNull(index.findKey(blobId1, new FileSpan(3000, 4000)));

      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(0, 200)));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(101, 500)));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(700, 1200)));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(1000, 1500)));
      Assert.assertFalse(index.findKey(blobId2, new FileSpan(1000, 1500)).isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertNull(index.findKey(blobId2, new FileSpan(1201, 2000)));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(1200, 2400)));
      Assert.assertNull(index.findKey(blobId2, new FileSpan(1600, 2200)));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(2600, 2900)));
      Assert.assertTrue(index.findKey(blobId2, new FileSpan(2600, 2900)).isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertTrue(index.findKey(blobId2, new FileSpan(1600, 2900)).isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertTrue(index.findKey(blobId2, new FileSpan(600, 2900)).isFlagSet(IndexValue.Flags.Delete_Index));
      Assert.assertNotNull(index.findKey(blobId2, new FileSpan(3000, 4000)));

      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(0, 200)));
      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(101, 500)));
      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(700, 1200)));
      Assert.assertNotNull(index.findKey(blobId5, new FileSpan(1000, 1500)));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(1200, 2400)));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(1201, 2000)));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(1600, 2200)));
      Assert.assertNull(index.findKey(blobId5, new FileSpan(3000, 4000)));

      Assert.assertNull(index.findKey(blobId6, new FileSpan(0, 200)));
      Assert.assertNull(index.findKey(blobId6, new FileSpan(101, 500)));
      Assert.assertNull(index.findKey(blobId6, new FileSpan(700, 1199)));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(1000, 1400)));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(500, 1600)));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(1200, 2400)));
      Assert.assertNotNull(index.findKey(blobId6, new FileSpan(1600, 2200)));
      Assert.assertNull(index.findKey(blobId6, new FileSpan(3000, 4000)));

      Assert.assertNull(index.findKey(blobId9, new FileSpan(0, 200)));
      Assert.assertNull(index.findKey(blobId9, new FileSpan(101, 500)));
      Assert.assertNull(index.findKey(blobId9, new FileSpan(700, 1199)));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(1000, 1400)));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(500, 1600)));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(1200, 2400)));
      Assert.assertNotNull(index.findKey(blobId9, new FileSpan(1600, 2200)));
      Assert.assertNull(index.findKey(blobId9, new FileSpan(3000, 4000)));

      Assert.assertNull(index.findKey(blobId10, new FileSpan(0, 200)));
      Assert.assertNull(index.findKey(blobId10, new FileSpan(101, 500)));
      Assert.assertNull(index.findKey(blobId10, new FileSpan(700, 1199)));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(1000, 1400)));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(500, 1600)));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(1200, 2400)));
      Assert.assertNotNull(index.findKey(blobId10, new FileSpan(1600, 2200)));
      Assert.assertNull(index.findKey(blobId10, new FileSpan(3000, 4000)));

      index.close();
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
  public void testFindEntries() {
    // provide empty token and ensure we get everything till max
    StoreFindToken token = new StoreFindToken();
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      StoreMetrics metrics = new StoreMetrics(tempFile().getParent(), new MetricRegistry());
      Log log = new Log(logFile, 10000, metrics);
      Properties props = new Properties();
      props.put("store.index.max.number.of.inmem.elements", "5");
      props.put("store.max.number.of.entries.to.return.for.find", "12");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
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
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0, flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(100, 100, flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(100, 200, flags, 12567));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(100, 300, flags, 12567));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(100, 400, flags, 12567));
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, 500, flags, 12567));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(100, 600, flags, 12567));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(100, 700, flags, 12567));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(100, 800, flags, 12567));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(100, 900, flags, 12567));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(100, 1000, flags, 12567));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(100, 1100, flags, 12567));
      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(100, 1200, flags, 12567));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(100, 1300, flags, 12567));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(100, 1400, flags, 12567));

      index.addToIndex(entry1, new FileSpan(0, 100));
      index.addToIndex(entry2, new FileSpan(100, 200));
      index.addToIndex(entry3, new FileSpan(200, 300));
      index.addToIndex(entry4, new FileSpan(300, 400));
      index.addToIndex(entry5, new FileSpan(400, 500));
      index.addToIndex(entry6, new FileSpan(500, 600));
      index.addToIndex(entry7, new FileSpan(600, 700));
      index.addToIndex(entry8, new FileSpan(700, 800));
      index.addToIndex(entry9, new FileSpan(800, 900));
      index.addToIndex(entry10, new FileSpan(900, 1000));
      index.addToIndex(entry11, new FileSpan(1000, 1100));
      index.addToIndex(entry12, new FileSpan(1100, 1200));
      index.addToIndex(entry13, new FileSpan(1200, 1300));
      index.addToIndex(entry14, new FileSpan(1300, 1400));
      index.addToIndex(entry15, new FileSpan(1400, 1500));
      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 100);
      Assert.assertEquals(value3.getOffset(), 200);

      index.markAsDeleted(blobId1, new FileSpan(1500, 1600));
      ByteBuffer buffer = ByteBuffer.allocate(1600);
      log.appendFrom(buffer);
      index.close();
      index = new MockIndex(logFile, scheduler, log, config, factory);
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
      index = new MockIndex(logFile, scheduler, log, config, factory);

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
      indexFile.delete();
      scheduler.shutdown();
      log.close();
    } catch (Exception e) {
      e.printStackTrace();
      org.junit.Assert.assertEquals(false, true);
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
  public void testFindEntriesAdditional() {
    // provide token referencing an offset from before
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      StoreMetrics metrics = new StoreMetrics(tempFile().getParent(), new MetricRegistry());
      Log log = new Log(logFile, 10000, metrics);
      Properties props = new Properties();
      props.put("store.index.max.number.of.inmem.elements", "5");
      props.put("store.max.number.of.entries.to.return.for.find", "12");
      props.put("store.journal.factory", "com.github.ambry.store.MockJournalFactory");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
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
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0, flags, 12345));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(100, 100, flags, 12567));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(100, 200, flags, 12567));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(100, 300, flags, 12567));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(100, 400, flags, 12567));
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, 500, flags, 12567));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(100, 600, flags, 12567));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(100, 700, flags, 12567));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(100, 800, flags, 12567));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(100, 900, flags, 12567));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(100, 1000, flags, 12567));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(100, 1100, flags, 12567));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(100, 1200, flags, 12567));
      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(100, 1300, flags, 12567));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(100, 1400, flags, 12567));
      IndexEntry entry16 = new IndexEntry(blobId16, new IndexValue(100, 1500, flags, 12567));
      IndexEntry entry18 = new IndexEntry(blobId18, new IndexValue(100, 1600, flags, 12567));
      IndexEntry entry17 = new IndexEntry(blobId17, new IndexValue(100, 1700, flags, 12567));
      IndexEntry entry19 = new IndexEntry(blobId19, new IndexValue(100, 1800, flags, 12567));
      IndexEntry entry20 = new IndexEntry(blobId20, new IndexValue(100, 1900, flags, 12567));
      IndexEntry entry21 = new IndexEntry(blobId21, new IndexValue(100, 2000, flags, 12567));
      IndexEntry entry22 = new IndexEntry(blobId22, new IndexValue(100, 2100, flags, 12567));
      IndexEntry entry24 = new IndexEntry(blobId24, new IndexValue(100, 2200, flags, 12567));
      IndexEntry entry23 = new IndexEntry(blobId23, new IndexValue(100, 2300, flags, 12567));

      // Add some delete entries
      flags = 1;
      IndexEntry entry21d = new IndexEntry(blobId21, new IndexValue(100, 2400, flags, 12567));
      IndexEntry entry22d = new IndexEntry(blobId22, new IndexValue(100, 2500, flags, 12567));
      IndexEntry entry24d = new IndexEntry(blobId24, new IndexValue(100, 2600, flags, 12567));
      IndexEntry entry23d = new IndexEntry(blobId23, new IndexValue(100, 2700, flags, 12567));

      flags = 0;
      IndexEntry entry25 = new IndexEntry(blobId25, new IndexValue(100, 2800, flags, 12567));

      long entrySize = entry1.getValue().getSize();

      /* Ensure older tokens are reset, and no entries are returned when the store just started.
       * (when the index is created for the first time, it is similar to an unclean shutdown, so
       * this tests token getting reset in the wake of an unclean shutdown case) */
      StoreFindToken token = new StoreFindToken(blobId1, 1000, new UUID(0, 0));
      FindInfo info = index.findEntriesSince(token, 500);
      List<MessageInfo> mEntries = info.getMessageEntries();
      Assert.assertEquals(mEntries.size(), 0);

      /* Test the case where an entry is added to the index, but not yet to the journal.
      There can only be at most one such entry. The index ensures an entry is added to
      the journal before the next entry is added to a segment. */

      journal.pause();
      index.addToIndex(entry1, new FileSpan(0, 100));

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

      index.addToIndex(entry3, new FileSpan(100, 200));
      index.addToIndex(entry2, new FileSpan(200, 300));
      index.addToIndex(entry5, new FileSpan(300, 400));
      index.addToIndex(entry4, new FileSpan(400, 500));

      /* Ensure that a token from a previous session with a key that is beyond the logEndOffsetOnStartup gets reset
         correctly - meaning findEntriesSince gets keys starting and *including* the key at logEndOffsetOnStartup. */

      // Index_0:      [1 2 3 4 5]           // sorted on keys
      // Journal:      [1 3 2 5 4]           // sorted on offsets
      // token before:                    i  // i means index based token
      // token after :          j            // j means offset (journal) based token

      token = new StoreFindToken(blobId1, 1000, new UUID(0, 0));
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
      index.addToIndex(entry6, new FileSpan(500, 600));
      index.addToIndex(entry8, new FileSpan(600, 700));
      index.addToIndex(entry9, new FileSpan(700, 800));
      index.addToIndex(entry7, new FileSpan(800, 900));
      index.addToIndex(entry10, new FileSpan(900, 1000));
      index.addToIndex(entry12, new FileSpan(1000, 1100));
      index.addToIndex(entry11, new FileSpan(1100, 1200));
      index.addToIndex(entry14, new FileSpan(1200, 1300));
      index.addToIndex(entry13, new FileSpan(1300, 1400));
      index.addToIndex(entry15, new FileSpan(1400, 1500));

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
      index.addToIndex(entry16, new FileSpan(1500, 1600));
      index.addToIndex(entry18, new FileSpan(1600, 1700));

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

      ArrayList indexEntries = new ArrayList<IndexEntry>();
      indexEntries.add(entry17);
      indexEntries.add(entry19);
      indexEntries.add(entry20);
      indexEntries.add(entry21);
      indexEntries.add(entry22);
      indexEntries.add(entry24);
      index.addToIndex(indexEntries,
          new FileSpan(entry17.getValue().getOffset(), entry24.getValue().getOffset() + entry24.getValue().getSize()));

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
      index.addToIndex(entry23,
          new FileSpan(entry23.getValue().getOffset(), entry23.getValue().getOffset() + entry23.getValue().getSize()));
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
      indexEntries.clear();
      indexEntries.add(entry21d);
      indexEntries.add(entry22d);
      indexEntries.add(entry24d);
      indexEntries.add(entry23d);
      indexEntries.add(entry25);
      index.addToIndex(indexEntries,
          new FileSpan(entry21d.getValue().getOffset(), entry25.getValue().getOffset() + entry25.getValue().getSize()));

      // Ensure that the journal still contains the latest segment's start offset.
      Assert.assertTrue(journal.getFirstOffset() <= index.getLastSegment().getStartOffset());

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

      indexFile.delete();
      scheduler.shutdown();
      log.close();
    } catch (Exception e) {
      e.printStackTrace();
      org.junit.Assert.assertEquals(false, true);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testFindDeletedEntries() {
    // provide empty token and ensure we get everything till max
    StoreFindToken token = new StoreFindToken();
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      StoreMetrics metrics = new StoreMetrics(tempFile().getParent(), new MetricRegistry());
      Log log = new Log(logFile, 10000, metrics);
      Properties props = new Properties();
      props.put("store.index.max.number.of.inmem.elements", "5");
      props.put("store.max.number.of.entries.to.return.for.find", "12");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
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
      MockId blobId20 = new MockId("id20");

      byte flags = 0;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0, flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(100, 100, flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(100, 200, flags, 12567));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(100, 300, flags, 12567));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(100, 500, flags, 12567));
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, 600, flags, 12567));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(100, 700, flags, 12567));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(100, 800, flags, 12567));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(100, 900, flags, 12567));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(100, 1100, flags, 12567));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(100, 1200, flags, 12567));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(100, 1500, flags, 12567));
      IndexEntry entry13 = new IndexEntry(blobId13, new IndexValue(100, 1600, flags, 12567));
      IndexEntry entry14 = new IndexEntry(blobId14, new IndexValue(100, 1700, flags, 12567));
      IndexEntry entry15 = new IndexEntry(blobId15, new IndexValue(100, 1900, flags, 12567));
      IndexEntry entry16 = new IndexEntry(blobId16, new IndexValue(100, 2300, flags, 12567));
      IndexEntry entry17 = new IndexEntry(blobId17, new IndexValue(100, 2400, flags, 12567));
      IndexEntry entry18 = new IndexEntry(blobId18, new IndexValue(100, 2500, flags, 12567));
      IndexEntry entry19 = new IndexEntry(blobId19, new IndexValue(100, 2800, flags, 12567));

      //segment 1
      index.addToIndex(entry1, new FileSpan(0, 100));
      index.addToIndex(entry2, new FileSpan(100, 200));
      index.addToIndex(entry3, new FileSpan(200, 300));
      index.addToIndex(entry4, new FileSpan(300, 400));
      index.markAsDeleted(blobId2, new FileSpan(400, 500));
      index.addToIndex(entry5, new FileSpan(500, 600));
      //Segment 1: [1 2d 3 4 5]

      //segment 2
      index.addToIndex(entry6, new FileSpan(600, 700));
      index.addToIndex(entry7, new FileSpan(700, 800));
      index.addToIndex(entry8, new FileSpan(800, 900));
      index.addToIndex(entry9, new FileSpan(900, 1000));
      index.markAsDeleted(blobId1, new FileSpan(1000, 1100));
      //Segment 2: [1d 6 7 8 9]

      //segment 3
      index.addToIndex(entry10, new FileSpan(1100, 1200));
      index.addToIndex(entry11, new FileSpan(1200, 1300));
      index.markAsDeleted(blobId10, new FileSpan(1300, 1400));
      index.markAsDeleted(blobId6, new FileSpan(1400, 1500));
      index.addToIndex(entry12, new FileSpan(1500, 1600));
      index.addToIndex(entry13, new FileSpan(1600, 1700));
      //Segment 3: [6d 10d 11 12 13]

      //segment 4
      index.addToIndex(entry14, new FileSpan(1700, 1800));
      index.markAsDeleted(blobId4, new FileSpan(1800, 1900)); // <- earliest journal entry.
      index.addToIndex(entry15, new FileSpan(1900, 2000));
      index.markAsDeleted(blobId12, new FileSpan(2000, 2100));
      index.markAsDeleted(blobId15, new FileSpan(2100, 2200));
      index.markAsDeleted(blobId7, new FileSpan(2200, 2300));
      //Segment 4: [4d 7d 12d 14 15d]

      //segment 5
      index.addToIndex(entry16, new FileSpan(2300, 2400));
      index.addToIndex(entry17, new FileSpan(2400, 2500));
      index.addToIndex(entry18, new FileSpan(2500, 2600));
      index.markAsDeleted(blobId18, new FileSpan(2600, 2700));
      index.markAsDeleted(blobId17, new FileSpan(2700, 2800));
      index.addToIndex(entry19, new FileSpan(2800, 2900));
      //segment 5: [16 17d 18d 19]

      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value1.getOriginalMessageOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 400);
      Assert.assertEquals(value2.getOriginalMessageOffset(), 100);
      Assert.assertEquals(value3.getOffset(), 200);
      Assert.assertEquals(value3.getOriginalMessageOffset(), 200);

      ByteBuffer buffer = ByteBuffer.allocate(2900);
      log.appendFrom(buffer);
      index.close();
      index = new MockIndex(logFile, scheduler, log, config, factory);

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
      Assert.assertEquals(((StoreFindToken) info.getFindToken()).getOffset(), 2400);

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
      index = new MockIndex(logFile, scheduler, log, config, factory);

      AtomicLong beforeSegment5LastModification = new AtomicLong(index.getLastSegment().getLastModifiedTime());
      // Wait long enough for the current time in seconds to be greater than the above time. In the future we
      // should mock time to do this in a cleaner way.
      Thread.sleep(1000);

      index.markAsDeleted(blobId16, new FileSpan(2900, 3000));
      index.markAsDeleted(blobId5, new FileSpan(3000, 3100));
      buffer = ByteBuffer.allocate(200);
      log.appendFrom(buffer);
      index.close();
      index = new MockIndex(logFile, scheduler, log, config, factory);
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
      index = new MockIndex(logFile, scheduler, log, config, factory);

      // close and cleanup for good.
      index.close();
      indexFile.delete();
      scheduler.shutdown();
      log.close();
    } catch (Exception e) {
      e.printStackTrace();
      org.junit.Assert.assertEquals(false, true);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }

  @Test
  public void testHardDelete() {
    // Create a mock index with regular log.
    // perform puts to the index.
    // perform deletes to the index.
    // call hardDelete() explicitly (set the thread frequency time to a large number if required) -
    // that will do findDeleted, prune and performHardDelete()
    // perform will need a messageStoreHardDelete implementation that just returns something
    // that will be written back. - need to implement that.
    // disable index persistor if that can be done.
    // call persist cleanup token and close the index. Reopen it to execute recovery path.
    // perform hard deletes upto some point.
    // perform more deletes.
    // do recovery.

    class HardDeleteTestHelper implements MessageStoreHardDelete {
      private long nextOffset;
      private long sizeOfEntry;
      private MockIndex index;
      private Log log;
      HashMap<Long, MessageInfo> offsetMap;

      HardDeleteTestHelper(long offset, long size) {
        nextOffset = offset;
        sizeOfEntry = size;
        offsetMap = new HashMap<Long, MessageInfo>();
      }

      void setIndex(MockIndex index, Log log) {
        this.index = index;
        this.log = log;
      }

      void add(MockId id)
          throws IOException, StoreException {
        index.addToIndex(new IndexEntry(id, new IndexValue(sizeOfEntry, nextOffset, (byte) 0, 12345)),
            new FileSpan(nextOffset, nextOffset + sizeOfEntry));
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) sizeOfEntry);
        log.appendFrom(byteBuffer);
        offsetMap.put(nextOffset, new MessageInfo(id, sizeOfEntry));
        nextOffset += sizeOfEntry;
      }

      void delete(MockId id)
          throws IOException, StoreException {
        index.markAsDeleted(id, new FileSpan(nextOffset, nextOffset + sizeOfEntry));
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) sizeOfEntry);
        log.appendFrom(byteBuffer);
        nextOffset += sizeOfEntry;
      }

      @Override
      public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory,
          List<byte[]> recoveryInfoList) {
        class MockMessageStoreHardDeleteIterator implements Iterator<HardDeleteInfo> {
          int count;
          MessageReadSet readSet;

          MockMessageStoreHardDeleteIterator(MessageReadSet readSet) {
            this.readSet = readSet;
            this.count = readSet.count();
          }

          @Override
          public boolean hasNext() {
            return count > 0;
          }

          @Override
          public HardDeleteInfo next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            --count;
            ByteBuffer buf = ByteBuffer.allocate((int) sizeOfEntry);
            byte[] recoveryInfo = new byte[100];
            Arrays.fill(recoveryInfo, (byte) 0);
            ByteBufferInputStream stream = new ByteBufferInputStream(buf);
            ReadableByteChannel channel = Channels.newChannel(stream);
            HardDeleteInfo hardDeleteInfo = new HardDeleteInfo(channel, 100, 100, recoveryInfo);
            return hardDeleteInfo;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
        return new MockMessageStoreHardDeleteIterator(readSet);
      }

      @Override
      public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory factory) {
        return offsetMap.get(offset);
      }
    }

    StoreFindToken token = new StoreFindToken();
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles()) {
        c.delete();
      }
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 10000, new StoreMetrics(logFile, new MetricRegistry()));
      Properties props = new Properties();
      // the test will set the tokens, so disable the index persistor.
      props.setProperty("store.data.flush.interval.seconds", "3600");
      props.setProperty("store.deleted.message.retention.days", "1");
      props.setProperty("store.index.max.number.of.inmem.elements", "2");
      // the following determines the number of entries that will be fetched at most. We need this to test the
      // case where the endToken does not reach the journal.
      props.setProperty("store.hard.delete.bytes.per.sec", "40");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockTime time = new MockTime(SystemTime.getInstance().milliseconds());

      HardDeleteTestHelper helper = new HardDeleteTestHelper(0, 200);
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory, helper, time);
      helper.setIndex(index, log);
      // Setting this below will not enable the hard delete thread. This being a unit test, the methods
      // are going to be called directly. We simply want to set the running flag to avoid those methods
      // from bailing out prematurely.
      index.setHardDeleteRunningStatus(true);

      MockId blobId01 = new MockId("id01");
      MockId blobId02 = new MockId("id02");
      MockId blobId03 = new MockId("id03");
      MockId blobId04 = new MockId("id04");
      MockId blobId05 = new MockId("id05");
      MockId blobId06 = new MockId("id06");
      MockId blobId07 = new MockId("id07");
      MockId blobId08 = new MockId("id08");
      MockId blobId09 = new MockId("id09");
      MockId blobId10 = new MockId("id10");

      helper.add(blobId01);
      helper.add(blobId02);
      helper.add(blobId03);
      helper.add(blobId04);

      helper.delete(blobId03);

      helper.add(blobId05);
      helper.add(blobId06);
      helper.add(blobId07);

      helper.delete(blobId02);
      helper.delete(blobId06);

      helper.add(blobId08);
      helper.add(blobId09);
      helper.add(blobId10);

      helper.delete(blobId10);
      helper.delete(blobId01);
      helper.delete(blobId08);

      // Let enough time to pass so that the above records become eligible for hard deletes.
      time.currentMilliseconds = time.currentMilliseconds + 2 * Time.SecsPerDay * Time.MsPerSec;

      // The first * shows where startTokenSafeToPersist is
      // The second * shows where startToken/endToken are before the operations.
      // Note since we are only depicting values before and after hardDelete() is done, startToken and endToken
      // will be the same.

      // indexes: **[1 2] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      boolean tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = endToken = 2.

      // call into the log flush hooks so that startTokenSafeToPersist = startToken = 2.
      index.persistAndAdvanceStartTokenSafeToPersist();

      // indexes: [1 2**] [3 4] [3d 5] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = endToken = 4.

      // indexes: [1 2*] [3 4*] [3d 5] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = 5, endToken = 5, startTokenSafeToPersist = 2

      // indexes: [1 2*] [3 4] [3d 5*] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      index.persistAndAdvanceStartTokenSafeToPersist();
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);
      // startToken = 7, endToken = 7, starttokenSafeToPersist = 5
      // 3d just got pruned.

      // indexes: [1 2] [3 4] [3d 5*] [6 7*] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d*] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d] [8 9*] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8]
      index.persistAndAdvanceStartTokenSafeToPersist();
      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d* 1d 8]

      tokenMovedForward = index.hardDelete();
      Assert.assertTrue(tokenMovedForward);

      // indexes: [1 2] [3 4] [3d 5*] [6 7] [2d 6d] [8 9] [1d 10d] [8]
      // journal:                                       [10 10d 1d 8*]
      // All caught up, so token should not have moved forward.
      tokenMovedForward = index.hardDelete();
      Assert.assertFalse(tokenMovedForward);

      index.persistAndAdvanceStartTokenSafeToPersist();

      // directly prune the recovery range completely (which should happen since we flushed till the endToken).
      index.pruneHardDeleteRecoveryRange(); //

      // Test recovery - this tests reading from the persisted token, filling up the hard delete recovery range and
      // then actually redoing the hard deletes on the range.
      index.performHardDeleteRecovery();

      index.close();
    } catch (Exception e) {
      e.printStackTrace();
      org.junit.Assert.assertEquals(false, true);
    }
  }
}
