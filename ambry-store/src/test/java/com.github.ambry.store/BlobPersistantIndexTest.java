package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
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

public class BlobPersistantIndexTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  class MockIndex extends BlobPersistentIndex {
    public MockIndex(String datadir,
                     Scheduler scheduler,
                     Log log,
                     StoreConfig config,
                     StoreKeyFactory factory) throws StoreException {
      super(datadir, scheduler, log, config, factory, new DummyMessageStoreRecovery());
    }

    public MockIndex(String datadir,
                     Scheduler scheduler,
                     Log log,
                     StoreConfig config,
                     StoreKeyFactory factory,
                     MessageStoreRecovery recovery) throws StoreException {
      super(datadir, scheduler, log, config, factory, recovery);
    }

    BlobIndexValue getValue(StoreKey key) throws StoreException {
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
  public void testIndexInfo() throws IOException {
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
      IndexSegmentInfo info = new IndexSegmentInfo(tempFile().getParent(),
                                                   0,
                                                   factory,
                                                   blobId1.sizeInBytes(),
                                                   BlobIndexValue.Index_Value_Size_In_Bytes,
                                                   config);
      BlobIndexValue value = new BlobIndexValue(1000, 0, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId1, value), 1000);
      value = new BlobIndexValue(1000, 1000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId2, value), 2000);
      value = new BlobIndexValue(1000, 2000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId3, value), 3000);
      value = new BlobIndexValue(1000, 3000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId4, value), 4000);
      value = new BlobIndexValue(1000, 4000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId5, value), 5000);
      value = new BlobIndexValue(1000, 5000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId6, value), 6000);
      value = new BlobIndexValue(1000, 6000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId7, value), 7000);
      value = new BlobIndexValue(1000, 7000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId8, value), 8000);
      value = new BlobIndexValue(1000, 8000, (byte)0);
      info.addEntry(new BlobIndexEntry(blobId9, value), 9000);

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

      info.writeIndexToFile(9000);
      IndexSegmentInfo infonew = new IndexSegmentInfo(info.getFile(), false, factory, config);
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
      // check invalid cases
      Assert.assertNull(info.find(new MockId("id10")));
      Assert.assertNull(info.find(new MockId("id11")));
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
    finally {
      if (map != null)
        map.cleanup();
    }
  }

  @Test
  public void testIndexBasic() throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles())
        c.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics, 10000);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 1000, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 2000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 3000, flags, 12567));
      index.addToIndex(entry1, 3000);
      index.addToIndex(entry2, 4000);
      index.addToIndex(entry3, 5000);
      BlobIndexValue value1 = index.getValue(blobId1);
      BlobIndexValue value2 = index.getValue(blobId2);
      BlobIndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);
      indexFile.delete();
      scheduler.shutdown();
      log.close();
    }
    catch (Exception e) {
      org.junit.Assert.assertEquals(false, true);
    }
    finally {
      if (map != null)
        map.cleanup();
    }
  }


  @Test
  public void testIndexRestore() throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles())
        c.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics, 7000);
      log.setLogEndOffset(5000);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      final MockId blobId1 = new MockId("id1");
      final MockId blobId2 = new MockId("id2");
      final MockId blobId3 = new MockId("id3");
      final MockId blobId4 = new MockId("id4");
      final MockId blobId5 = new MockId("id5");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(3000, 0, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(1000, 3000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(1000, 4000, flags, 12567));
      index.addToIndex(entry1, 3000);
      index.addToIndex(entry2, 4000);
      index.addToIndex(entry3, 5000);
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile, scheduler, log, config, factory);

      BlobIndexValue value1 = indexNew.getValue(blobId1);
      BlobIndexValue value2 = indexNew.getValue(blobId2);
      BlobIndexValue value3 = indexNew.getValue(blobId3);
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
      log.setLogEndOffset(5000);
      indexNew.addToIndex(new BlobIndexEntry(blobId4, new BlobIndexValue(1000, 5000, 12657)), 6000);
      indexNew.addToIndex(new BlobIndexEntry(blobId5, new BlobIndexValue(1000, 6000, 12657)), 7000);
      indexNew.close();
      indexNew = new MockIndex(logFile, scheduler, log, config, factory);
      value1 = indexNew.getValue(blobId1);
      value2 = indexNew.getValue(blobId2);
      value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 3000);
      Assert.assertEquals(value3.getOffset(), 4000);
      BlobIndexValue value4 = indexNew.getValue(blobId4);
      BlobIndexValue value5 = indexNew.getValue(blobId5);
      Assert.assertNull(value4);
      Assert.assertNull(value5);
      indexNew.close();

      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory) throws IOException {
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
      log.setLogEndOffset(7000);
      indexNew.close();

      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory) throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId4, 100, true));
          infos.add(new MessageInfo(blobId5, 100, BlobIndexValue.TTL_Infinite));
          return infos;
        }
      });
      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.isFlagSet(BlobIndexValue.Flags.Delete_Index), true);
      Assert.assertEquals(value5.getTimeToLiveInMs(), BlobIndexValue.TTL_Infinite);
      Assert.assertEquals(value4.getSize(), 1000);
      Assert.assertEquals(value4.getOffset(), 5000);
      Assert.assertEquals(value5.getSize(), 1000);
      Assert.assertEquals(value5.getOffset(), 6000);
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
      }
      catch (StoreException e) {
        Assert.assertTrue(true);
      }

      byte[] salt = new byte[1];
      salt[0] = 1;
      channelToModify.write(ByteBuffer.wrap(salt));  // write version 1

      try {
        MockIndex indexReadFail = new MockIndex(logFile, scheduler, log, config, factory);
        Assert.assertFalse(true);
      }
      catch (StoreException e) {
        Assert.assertTrue(true);
      }

      channelToModify.truncate(0);
      byte[] addOnlyVersion = new byte[1];
      addOnlyVersion[0] = 0;
      channelToModify.write(ByteBuffer.wrap(addOnlyVersion));

      try {
        MockIndex indexEmptyLine = new MockIndex(logFile, scheduler, log, config, factory);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertTrue(true);
      }

      toModify.delete();

      indexNew = new MockIndex(logFile, scheduler, log, config, factory, new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory) throws IOException {
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
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory) throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId4, 100, true));
          infos.add(new MessageInfo(blobId5, 100, BlobIndexValue.TTL_Infinite));
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
    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
    finally {
      if (map != null)
        map.cleanup();
    }
  }

  @Test
  public void testIndexBatch() throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles())
        c.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics, 10000);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 1000, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 2000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 3000, flags, 12567));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 5000);
      BlobIndexValue value1 = index.getValue(blobId1);
      BlobIndexValue value2 = index.getValue(blobId2);
      BlobIndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);

      MockId blobId4 = new MockId("id4");

      BlobIndexValue value4 = index.getValue(blobId4);
      try {
        index.addToIndex(new BlobIndexEntry(blobId4, value4), 4000);
        Assert.assertTrue(false);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
    finally {
      if (map != null)
        map.cleanup();
    }
  }

  @Test
  public void testIndexRead() throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles())
        c.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics, 10000);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 1000));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 2000));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 3000, System.currentTimeMillis()));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 5000);
      // simple read
      BlobReadOptions readOptions = index.getBlobReadInfo(blobId1);
      Assert.assertEquals(readOptions.getOffset(), 1000);
      Assert.assertEquals(readOptions.getSize(), 100);
      Assert.assertEquals(readOptions.getTTL(), -1);

      // read missing item
      try {
        index.getBlobReadInfo(new MockId("id4"));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(blobId2, 6000);
      try {
        index.getBlobReadInfo(blobId2);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      // read ttl expired item
      index.updateTTL(blobId1, 1234, 7000);
      try {
        index.getBlobReadInfo(blobId1);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }

      // try to delete or update a missing blob
      try {
        index.markAsDeleted(new MockId("id5"), 8000);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);

      }
      try {
        index.updateTTL(new MockId("id6"), 1234, 9000);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
    finally {
      if (map != null)
        map.cleanup();
    }
  }

  @Test
  public void testMissingEntries() throws IOException {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles())
        c.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics, 10000);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
      MockId blobId1 = new MockId("id1");
      MockId blobId2 = new MockId("id2");
      MockId blobId3 = new MockId("id3");

      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 1000));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 2000));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 3000));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 5000);
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      StoreKey key1 = new MockId("id4");
      keys.add(key1);
      keys.add(blobId1);
      keys.add(blobId2);
      List<StoreKey> missing = index.findMissingEntries(keys);
      Assert.assertEquals(missing.size(), 1);
      Assert.assertArrayEquals(missing.get(0).toBytes(), key1.toBytes());
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
    finally {
      if (map != null)
        map.cleanup();
    }
  }

  @Test
  public void testRollingIndex() {
    MockClusterMap map = null;
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile);
      for (File c : indexFile.listFiles())
        c.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics, 30000);
      log.setLogEndOffset(30000);
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "200");
      props.setProperty("store.data.flush.interval.seconds", "1");
      props.setProperty("store.data.flush.delay.seconds", "1");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      map = new MockClusterMap();
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
      MockIndex index = new MockIndex(logFile, scheduler, log, config, factory);
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

      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 1000));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 2000));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 3000));
      BlobIndexEntry entry4 = new BlobIndexEntry(blobId4, new BlobIndexValue(300, 4000));
      BlobIndexEntry entry5 = new BlobIndexEntry(blobId5, new BlobIndexValue(300, 5000));

      BlobIndexEntry entry6 = new BlobIndexEntry(blobId6, new BlobIndexValue(300, 6000));
      BlobIndexEntry entry7 = new BlobIndexEntry(blobId7, new BlobIndexValue(300, 7000));
      BlobIndexEntry entry8 = new BlobIndexEntry(blobId8, new BlobIndexValue(300, 8000));
      BlobIndexEntry entry9 = new BlobIndexEntry(blobId9, new BlobIndexValue(300, 9000));
      BlobIndexEntry entry10 = new BlobIndexEntry(blobId10, new BlobIndexValue(300, 10000));
      BlobIndexEntry entry11 = new BlobIndexEntry(blobId11, new BlobIndexValue(300, 11000));
      BlobIndexEntry entry12 = new BlobIndexEntry(blobId12, new BlobIndexValue(300, 12000));

      BlobIndexEntry entry13 = new BlobIndexEntry(blobId13, new BlobIndexValue(300, 13000));
      BlobIndexEntry entry14 = new BlobIndexEntry(blobId14, new BlobIndexValue(300, 14000));
      BlobIndexEntry entry15 = new BlobIndexEntry(blobId15, new BlobIndexValue(300, 15000));
      BlobIndexEntry entry16 = new BlobIndexEntry(blobId16, new BlobIndexValue(300, 16000));
      BlobIndexEntry entry17 = new BlobIndexEntry(blobId17, new BlobIndexValue(300, 17000));
      BlobIndexEntry entry18 = new BlobIndexEntry(blobId18, new BlobIndexValue(300, 18000));
      BlobIndexEntry entry19 = new BlobIndexEntry(blobId19, new BlobIndexValue(300, 19000));

      BlobIndexEntry entry20 = new BlobIndexEntry(blobId20, new BlobIndexValue(300, 20000));
      BlobIndexEntry entry21 = new BlobIndexEntry(blobId21, new BlobIndexValue(300, 21000));
      BlobIndexEntry entry22 = new BlobIndexEntry(blobId22, new BlobIndexValue(300, 22000));
      BlobIndexEntry entry23 = new BlobIndexEntry(blobId23, new BlobIndexValue(300, 23000));
      BlobIndexEntry entry24 = new BlobIndexEntry(blobId24, new BlobIndexValue(300, 24000));

      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 3000);
      list.clear();
      list.add(entry4);
      list.add(entry5);
      list.add(entry6);
      list.add(entry7);
      list.add(entry8);
      index.addToIndex(list, 8000);
      list.clear();
      list.add(entry9);
      list.add(entry10);
      list.add(entry11);
      list.add(entry12);
      index.addToIndex(list, 12000);
      list.clear();
      list.add(entry13);
      list.add(entry14);
      list.add(entry15);
      list.add(entry16);
      index.addToIndex(list, 16000);
      list.clear();
      list.add(entry17);
      list.add(entry18);
      list.add(entry19);
      index.addToIndex(list, 19000);
      list.clear();
      list.add(entry20);
      list.add(entry21);
      list.add(entry22);
      list.add(entry23);
      list.add(entry24);
      index.addToIndex(list, 24000);

      // search
      Assert.assertEquals(index.findKey(blobId20).getOffset(), 20000);
      Assert.assertEquals(index.findKey(blobId21).getOffset(), 21000);
      Thread.sleep(3000);

      Assert.assertEquals(index.findKey(blobId1).getOffset(), 1000);
      Assert.assertEquals(index.findKey(blobId2).getOffset(), 2000);

      index.close();
      MockIndex indexNew = new MockIndex(logFile, scheduler, log, config, factory);
      Assert.assertEquals(indexNew.findKey(blobId1).getOffset(), 1000);
      Assert.assertEquals(indexNew.findKey(blobId2).getOffset(), 2000);
    }
    catch (Exception e) {
      org.junit.Assert.assertTrue(false);
    }
    finally {
      if (map != null)
        map.cleanup();
    }
  }
}
