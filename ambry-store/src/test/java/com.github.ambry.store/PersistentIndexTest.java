package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import java.util.EnumSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


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
    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config, StoreKeyFactory factory)
        throws StoreException {
      super(datadir, scheduler, log, config, factory, new DummyMessageStoreRecovery(),
          new StoreMetrics(datadir, new MetricRegistry()));
    }

    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config, StoreKeyFactory factory,
        MessageStoreRecovery recovery)
        throws StoreException {
      super(datadir, scheduler, log, config, factory, recovery, new StoreMetrics(datadir, new MetricRegistry()));
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
      info.getEntriesSince(blobId6, 4000, entries, new AtomicLong(0));
      Assert.assertEquals(entries.get(0).getStoreKey(), blobId7);
      Assert.assertEquals(entries.get(2).getStoreKey(), blobId9);
      Assert.assertEquals(entries.size(), 3);
      entries.clear();
      info.getEntriesSince(blobId1, 5000, entries, new AtomicLong(0));
      Assert.assertEquals(entries.size(), 5);
      entries.clear();
      info.getEntriesSince(null, 5000, entries, new AtomicLong(0));
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
      info.getEntriesSince(blobId6, 5000, entries, new AtomicLong(0));
      Assert.assertEquals(entries.get(0).getStoreKey(), blobId7);
      Assert.assertEquals(entries.get(2).getStoreKey(), blobId9);
      Assert.assertEquals(entries.size(), 3);
      entries.clear();
      info.getEntriesSince(blobId1, 5000, entries, new AtomicLong(0));
      Assert.assertEquals(entries.size(), 5);
      entries.clear();
      info.getEntriesSince(null, 5000, entries, new AtomicLong(0));
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

 /* @Test
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
      Log log = new Log(logFile, 8000, new StoreMetrics(logFile, new MetricRegistry()));
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      final MockId blobId1 = new MockId("id1");
      final MockId blobId2 = new MockId("id2");
      final MockId blobId3 = new MockId("id3");
      final MockId blobId4 = new MockId("id4");
      final MockId blobId5 = new MockId("id5");

      ByteBuffer buffer = ByteBuffer.allocate(6000);
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
      indexNew.close();
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
          infos.add(new MessageInfo(blobId4, 1000));
          infos.add(new MessageInfo(blobId5, 1000, 12657));
          return infos;
        }
      });
      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.getSize(), 1000);
      Assert.assertEquals(value4.getOffset(), 5000);
      Assert.assertEquals(value5.getSize(), 1000);
      Assert.assertEquals(value5.getOffset(), 6000);
      Assert.assertEquals(value5.getTimeToLiveInMs(), 12657);
      Assert.assertEquals(log.getLogEndOffset(), 7000);
      indexNew.close();

      buffer = ByteBuffer.allocate(1000);
      log.appendFrom(buffer);
      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
            throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId4, 100, true));
          infos.add(new MessageInfo(blobId5, 100, true));
          return infos;
        }
      });
      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.isFlagSet(IndexValue.Flags.Delete_Index), true);
      Assert.assertEquals(value5.getTimeToLiveInMs(), 12657);
      Assert.assertEquals(value4.getSize(), 100);
      Assert.assertEquals(value4.getOriginalMessageOffset(), 5000);
      Assert.assertEquals(value4.getOffset(), 7000);
      Assert.assertEquals(value5.getSize(), 100);
      Assert.assertEquals(value5.getOriginalMessageOffset(), 6000);
      Assert.assertEquals(value5.getOffset(), 7100);
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
      });
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
      });

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
  }*/

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

  /*@Test
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
      Log log = new Log(logFile, 2400, new StoreMetrics(logFile, new MetricRegistry()));
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "200");
      props.setProperty("store.data.flush.interval.seconds", "1");
      props.setProperty("store.data.flush.delay.seconds", "1");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      ByteBuffer buffer = ByteBuffer.allocate(1200);
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

      MockIndex index1 = new MockIndex(logFile, scheduler, log, config, factory);
      ByteBuffer buffer1 = ByteBuffer.allocate(1200);
      log.appendFrom(buffer1);
      MockId blobId6 = new MockId("id01");
      MockId blobId7 = new MockId("id02");
      MockId blobId8 = new MockId("id03");
      MockId blobId9 = new MockId("id04");
      MockId blobId10 = new MockId("id05");
      IndexEntry entry6 = new IndexEntry(blobId6, new IndexValue(100, 1200));
      IndexEntry entry7 = new IndexEntry(blobId7, new IndexValue(200, 1300));
      IndexEntry entry8 = new IndexEntry(blobId8, new IndexValue(300, 1500));
      IndexEntry entry9 = new IndexEntry(blobId9, new IndexValue(300, 1800));
      IndexEntry entry10 = new IndexEntry(blobId10, new IndexValue(300, 2100));

      list.clear();
      list.add(entry6);
      list.add(entry7);
      list.add(entry8);
      index1.addToIndex(list, new FileSpan(1200, 1800));
      list.clear();
      list.add(entry9);
      list.add(entry10);
      index1.addToIndex(list, new FileSpan(1800, 2400));
      list.clear();

      Assert.assertTrue(index.exists(blobId1, new FileSpan(0,200)));
      Assert.assertTrue(index.exists(blobId1, new FileSpan(101, 200)));
      Assert.assertFalse(index.exists(blobId5, new FileSpan(1201, 2000)));

      Assert.assertTrue(index.exists(blobId2, new FileSpan(0,1000)));
      Assert.assertTrue(index.exists(blobId2, new FileSpan(0,100)));
      Assert.assertTrue(index.exists(blobId2, new FileSpan(0,101)));
      Assert.assertTrue(index.exists(blobId2, new FileSpan(0,99)));
      Assert.assertTrue(index.exists(blobId2, new FileSpan(150, 300)));
      Assert.assertTrue(index.exists(blobId2, new FileSpan(0,99)));
      Assert.assertTrue(index.exists(blobId2, new FileSpan(1000,2000)));
      Assert.assertFalse(index.exists(blobId5, new FileSpan(1300, 1500)));

      Assert.assertTrue(index.exists(blobId3, new FileSpan(0, 299)));
      Assert.assertTrue(index.exists(blobId3, new FileSpan(200,400)));
      Assert.assertTrue(index.exists(blobId3, new FileSpan(300,400)));
      Assert.assertTrue(index.exists(blobId3, new FileSpan(100, 300)));
      Assert.assertTrue(index.exists(blobId3, new FileSpan(300,600)));
      Assert.assertTrue(index.exists(blobId3, new FileSpan(300,800)));
      Assert.assertTrue(index.exists(blobId3, new FileSpan(0,600)));
      Assert.assertTrue(index.exists(blobId3, new FileSpan(0,1000)));
      Assert.assertFalse(index.exists(blobId5, new FileSpan(1400, 1500)));

      Assert.assertTrue(index.exists(blobId5, new FileSpan(0,400)));
      Assert.assertTrue(index.exists(blobId5, new FileSpan(200, 400)));
      Assert.assertTrue(index.exists(blobId5, new FileSpan(300, 400)));
      Assert.assertTrue(index.exists(blobId5, new FileSpan(300, 1000)));
      Assert.assertTrue(index.exists(blobId5, new FileSpan(900,1200)));
      Assert.assertTrue(index.exists(blobId5, new FileSpan(900, 1000)));
      Assert.assertTrue(index.exists(blobId5, new FileSpan(700, 900)));
      Assert.assertTrue(index.exists(blobId5, new FileSpan(0,1200)));
      Assert.assertFalse(index.exists(blobId5, new FileSpan(1201, 1500)));
      index.close();
    } catch (Exception e) {
      e.printStackTrace();
      org.junit.Assert.assertTrue(false);
    } finally {
      if (map != null) {
        map.cleanup();
      }
    }
  }      */

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

    // provide token with offset that is in journal

    // provide token with offset that is not in journal

    // provide token with key

  }
}
