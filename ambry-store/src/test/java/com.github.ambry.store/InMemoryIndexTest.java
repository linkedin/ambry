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
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


public class InMemoryIndexTest {

  private StoreKeyFactory factory;
  private MockClusterMap map;

  /**
   * Create a temporary file
   */
  File tempFile()
      throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  public InMemoryIndexTest()
      throws InstantiationException, IOException {
    map = new MockClusterMap();
    try {
      factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
    } catch (Exception e) {
      throw new InstantiationException();
    }
  }

  class MockIndex extends InMemoryIndex {
    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreKeyFactory factory)
        throws StoreException {
      super(datadir, scheduler, log, new StoreConfig(new VerifiableProperties(new Properties())), factory,
          new DummyMessageStoreRecovery(), new MetricRegistry(), SystemTime.getInstance());
    }

    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreKeyFactory factory, StoreConfig config,
        MessageStoreRecovery messageRecovery)
        throws StoreException {
      super(datadir, scheduler, log, config, factory, messageRecovery, new MetricRegistry(), SystemTime.getInstance());
    }

    IndexValue getValue(StoreKey key) {
      return index.get(key);
    }

    public void deleteAll() {
      index.clear();
    }

    public void stopScheduler() {
      scheduler.shutdown();
    }

    public boolean isEmpty() {
      return index.size() == 0;
    }
  }

  @Test
  public void testIndexBasic()
      throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log log = new Log(logFile, 1000, new StoreMetrics(logFile, new MetricRegistry()));
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0, flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(200, 100, flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(300, 200, flags, 12567));
      index.addToIndex(entry1, new FileSpan(0, 100));
      index.addToIndex(entry2, new FileSpan(100, 200));
      index.addToIndex(entry3, new FileSpan(200, 300));
      IndexValue value1 = index.getValue(blobId1);
      IndexValue value2 = index.getValue(blobId2);
      IndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 100);
      Assert.assertEquals(value3.getOffset(), 200);
      indexFile.delete();
      scheduler.shutdown();
      log.close();
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexRestore()
      throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 7000, new StoreMetrics(logFile, new MetricRegistry()));
      Properties props = new Properties();
      props.put("store.data.flush.delay.seconds", "999999");
      MockIndex index =
          new MockIndex(logFile, scheduler, log, factory, new StoreConfig(new VerifiableProperties(props)),
              new DummyMessageStoreRecovery());
      final MockId blobId1 = new MockId("id1");
      final MockId blobId2 = new MockId("id2");
      final MockId blobId3 = new MockId("id3");
      final MockId blobId4 = new MockId("id4");
      final MockId blobId5 = new MockId("id5");
      ByteBuffer buffer = ByteBuffer.allocate(3000);
      log.appendFrom(buffer);

      byte flags = 3;
      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(1000, 0, flags, 12345));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(1000, 1000, flags, 12567));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(1000, 2000, flags, 12567));
      index.addToIndex(entry1, new FileSpan(0, 1000));
      index.addToIndex(entry2, new FileSpan(1000, 2000));
      index.addToIndex(entry3, new FileSpan(2000, 3000));
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew =
          new MockIndex(logFile, scheduler, log, factory, new StoreConfig(new VerifiableProperties(props)),
              new DummyMessageStoreRecovery());

      IndexValue value1 = indexNew.getValue(blobId1);
      IndexValue value2 = indexNew.getValue(blobId2);
      IndexValue value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 1000);
      Assert.assertEquals(value3.getOffset(), 2000);
      indexNew.close();

      indexNew = new MockIndex(logFile, scheduler, log, factory, new StoreConfig(new VerifiableProperties(props)),
          new DummyMessageStoreRecovery());

      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(1000, 3000, flags, 12567));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(1000, 4000, flags, 12567));
      index.addToIndex(entry4, new FileSpan(3000, 4000));
      index.addToIndex(entry5, new FileSpan(4000, 5000));
      indexNew.close();
      indexNew = new MockIndex(logFile, scheduler, log, factory, new StoreConfig(new VerifiableProperties(props)),
          new DummyMessageStoreRecovery());
      value1 = indexNew.getValue(blobId1);
      value2 = indexNew.getValue(blobId2);
      value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 1000);
      Assert.assertEquals(value3.getOffset(), 2000);
      IndexValue value4 = indexNew.getValue(blobId4);
      IndexValue value5 = indexNew.getValue(blobId5);
      Assert.assertNull(value4);
      Assert.assertNull(value5);
      indexNew.close();

      buffer = ByteBuffer.allocate(2000);
      log.appendFrom(buffer);

      indexNew = new MockIndex(logFile, scheduler, log, factory, new StoreConfig(new VerifiableProperties(props)),
          new MessageStoreRecovery() {
            @Override
            public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
                throws IOException {
              List<MessageInfo> infos = new ArrayList<MessageInfo>();
              infos.add(new MessageInfo(blobId4, 1000));
              infos.add(new MessageInfo(blobId5, 1000, 12657));
              return infos;
            }
          });
      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.getSize(), 1000);
      Assert.assertEquals(value5.getSize(), 1000);
      Assert.assertEquals(value5.getTimeToLiveInMs(), 12657);
      indexNew.close();
      buffer = ByteBuffer.allocate(2000);
      log.appendFrom(buffer);

      indexNew = new MockIndex(logFile, scheduler, log, factory, new StoreConfig(new VerifiableProperties(props)),
          new MessageStoreRecovery() {
            @Override
            public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
                throws IOException {
              List<MessageInfo> infos = new ArrayList<MessageInfo>();
              infos.add(new MessageInfo(blobId4, 1000, true));
              infos.add(new MessageInfo(blobId5, 1000, true, Utils.Infinite_Time));
              return infos;
            }
          });
      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.isFlagSet(IndexValue.Flags.Delete_Index), true);
      Assert.assertEquals(value5.getTimeToLiveInMs(), 12657);
      indexNew.stopScheduler();
      indexNew.deleteAll();
      indexNew.close();

      // truncate and make an empty index and ensure we fail index creation
      File toModify = new File(logFile, "index_current");
      FileChannel channelToModify = Utils.openChannel(toModify, true);
      channelToModify.truncate(0);
      channelToModify.force(true);
      scheduler.startup();

      try {
        MockIndex indexFail = new MockIndex(logFile, scheduler, log, factory);
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.Index_Creation_Failure);
      }

      byte[] salt = new byte[2];
      salt[0] = 0;
      salt[1] = 1;
      channelToModify.write(ByteBuffer.wrap(salt));  // write version 1

      MockIndex indexReadFail = new MockIndex(logFile, scheduler, log, factory);
      Assert.assertTrue(indexReadFail.isEmpty());

      channelToModify.truncate(0);
      channelToModify.force(true);
      byte[] addOnlyVersion = new byte[2];
      addOnlyVersion[0] = 0;
      addOnlyVersion[1] = 0;
      channelToModify.write(ByteBuffer.wrap(addOnlyVersion));

      try {
        MockIndex indexEmptyLine = new MockIndex(logFile, scheduler, log, factory);
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.Index_Creation_Failure);
      }

      log.close();
      scheduler.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexBatch()
      throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log log = new Log(logFile, 1000, new StoreMetrics(logFile, new MetricRegistry()));
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
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

      IndexValue value4 = index.getValue(new MockId("id4"));
      try {
        index.addToIndex(new IndexEntry(new MockId("id5"), value4), new FileSpan(500, 600));
        Assert.assertTrue(false);
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
    } catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexRead()
      throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log log = new Log(logFile, 1000, new StoreMetrics(logFile, new MetricRegistry()));
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
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
      BlobReadOptions readOptions = index.getBlobReadInfo(blobId1);
      Assert.assertEquals(readOptions.getOffset(), 0);
      Assert.assertEquals(readOptions.getSize(), 100);
      Assert.assertEquals(readOptions.getTTL(), -1);

      // read missing item
      try {
        index.getBlobReadInfo(new MockId("id4"));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(blobId2, new FileSpan(600, 700));
      try {
        index.getBlobReadInfo(blobId2);
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      // read ttl expired item
      try {
        index.getBlobReadInfo(blobId3);
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }

      // try to delete or update a missing blob
      try {
        index.markAsDeleted(new MockId("id5"), new FileSpan(800, 900));
        Assert.assertTrue(false);
      } catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testMissingEntries()
      throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log log = new Log(logFile, 1000, new StoreMetrics(logFile, new MetricRegistry()));
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
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
      MockId missingId = new MockId("id4");
      keys.add(missingId);
      keys.add(blobId1);
      keys.add(blobId2);
      Set<StoreKey> missing = index.findMissingKeys(keys);
      Assert.assertEquals(missing.size(), 1);
      StoreKey missingKey = missing.iterator().next();
      Assert.assertArrayEquals(missingKey.toBytes(), missingId.toBytes());
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testFindEntries() {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile, 1000, new StoreMetrics(logFile, new MetricRegistry()));
      Properties props = new Properties();
      props.put("store.index.max.number.of.inmem.elements", "5");
      props.put("store.max.number.of.entries.to.return.for.find", "12");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      MockIndex index = new MockIndex(logFile, scheduler, log, factory, config, new DummyMessageStoreRecovery());
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

      IndexEntry entry1 = new IndexEntry(blobId1, new IndexValue(100, 0));
      IndexEntry entry2 = new IndexEntry(blobId2, new IndexValue(100, 100));
      IndexEntry entry3 = new IndexEntry(blobId3, new IndexValue(100, 200));
      IndexEntry entry4 = new IndexEntry(blobId4, new IndexValue(100, 300));
      IndexEntry entry5 = new IndexEntry(blobId5, new IndexValue(100, 400));
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, 500));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(100, 600));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(100, 700));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(100, 800));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(100, 900));
      IndexEntry entry11 = new IndexEntry(blobId11, new IndexValue(100, 1000));
      IndexEntry entry12 = new IndexEntry(blobId12, new IndexValue(100, 1100));

      ArrayList<IndexEntry> list = new ArrayList<IndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      list.add(entry4);
      list.add(entry5);
      list.add(entry6);
      list.add(entry7);
      list.add(entry8);
      list.add(entry9);
      list.add(entry10);
      list.add(entry11);
      list.add(entry12);

      index.addToIndex(list, new FileSpan(0, 1200));
      StoreFindToken token = new StoreFindToken(900, null);
      FindInfo info1 = index.findEntriesSince(token, 100000);
      StoreFindToken newToken = (StoreFindToken) info1.getFindToken();
      newToken.setOffset(900);
      info1 = index.findEntriesSince(newToken, 100000);
      List<MessageInfo> entries = info1.getMessageEntries();
      Assert.assertEquals(entries.size(), 2);
      Assert.assertEquals(entries.get(0).getStoreKey(), blobId11);
      Assert.assertEquals(entries.get(1).getStoreKey(), blobId12);
      info1 = index.findEntriesSince(info1.getFindToken(), 100000);
      Assert.assertEquals(info1.getMessageEntries().size(), 0);
      token = new StoreFindToken();
      info1 = index.findEntriesSince(token, 100000);
      Assert.assertEquals(info1.getMessageEntries().size(), 12);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }
}
