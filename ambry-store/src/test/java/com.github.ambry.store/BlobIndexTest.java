package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class BlobIndexTest {

  private StoreKeyFactory factory;
  private MockClusterMap map;

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  public BlobIndexTest() throws InstantiationException {
    map = new MockClusterMap();
    try {
      factory = Utils.getObj("com.github.ambry.store.MockIdFactory");
    }
    catch (Exception e) {
      throw new InstantiationException();
    }
  }

  class MockIndex extends BlobIndex {
    public MockIndex(String datadir,
                     Scheduler scheduler,
                     Log log,
                     StoreKeyFactory factory) throws StoreException {
      super(datadir,
            scheduler,
            log,
            new StoreConfig(new VerifiableProperties(new Properties())),
            factory,
            new DummyMessageStoreRecovery(),
            new MetricRegistry());
    }

    public MockIndex(String datadir,
                     Scheduler scheduler,
                     Log log,
                     StoreKeyFactory factory,
                     StoreConfig config,
                     MessageStoreRecovery messageRecovery) throws StoreException {
      super(datadir, scheduler, log, config, factory, messageRecovery, new MetricRegistry());
    }

    BlobIndexValue getValue(StoreKey key) {
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
  public void testIndexBasic() throws IOException {
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
      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 0, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 100, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 200, flags, 12567));
      index.addToIndex(entry1, new FileSpan(0, 100));
      index.addToIndex(entry2, new FileSpan(100, 200));
      index.addToIndex(entry3, new FileSpan(200, 300));
      BlobIndexValue value1 = index.getValue(blobId1);
      BlobIndexValue value2 = index.getValue(blobId2);
      BlobIndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 100);
      Assert.assertEquals(value3.getOffset(), 200);
      indexFile.delete();
      scheduler.shutdown();
      log.close();
    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexRestore() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Log log = new Log(logFile, 5000, new StoreMetrics(logFile, new MetricRegistry()));
      log.setLogEndOffset(3000);
      Properties props = new Properties();
      props.put("store.data.flush.delay.seconds", "999999");
      MockIndex index = new MockIndex(logFile,
                                      scheduler,
                                      log,
                                      factory,
                                      new StoreConfig(new VerifiableProperties(props)),
                                      new DummyMessageStoreRecovery());
      final MockId blobId1 = new MockId("id1");
      final MockId blobId2 = new MockId("id2");
      final MockId blobId3 = new MockId("id3");
      final MockId blobId4 = new MockId("id4");
      final MockId blobId5 = new MockId("id5");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(1000, 0, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(1000, 1000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(1000, 2000, flags, 12567));
      index.addToIndex(entry1, new FileSpan(0, 1000));
      index.addToIndex(entry2, new FileSpan(1000, 2000));
      index.addToIndex(entry3, new FileSpan(2000, 3000));
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile,
                                         scheduler,
                                         log,
                                         factory,
                                         new StoreConfig(new VerifiableProperties(props)),
                                         new DummyMessageStoreRecovery());

      BlobIndexValue value1 = indexNew.getValue(blobId1);
      BlobIndexValue value2 = indexNew.getValue(blobId2);
      BlobIndexValue value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 1000);
      Assert.assertEquals(value3.getOffset(), 2000);
      indexNew.close();

      indexNew = new MockIndex(logFile,
                               scheduler,
                               log,
                               factory,
                               new StoreConfig(new VerifiableProperties(props)),
                               new DummyMessageStoreRecovery());

      BlobIndexEntry entry4 = new BlobIndexEntry(blobId4, new BlobIndexValue(1000, 3000, flags, 12567));
      BlobIndexEntry entry5 = new BlobIndexEntry(blobId5, new BlobIndexValue(1000, 4000, flags, 12567));
      index.addToIndex(entry4, new FileSpan(3000, 4000));
      index.addToIndex(entry5, new FileSpan(4000, 5000));
      indexNew.close();
      indexNew = new MockIndex(logFile,
                               scheduler,
                               log,
                               factory,
                               new StoreConfig(new VerifiableProperties(props)),
                               new DummyMessageStoreRecovery());
      value1 = indexNew.getValue(blobId1);
      value2 = indexNew.getValue(blobId2);
      value3 = indexNew.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 1000);
      Assert.assertEquals(value3.getOffset(), 2000);
      BlobIndexValue value4 = indexNew.getValue(blobId4);
      BlobIndexValue value5 = indexNew.getValue(blobId5);
      Assert.assertNull(value4);
      Assert.assertNull(value5);
      indexNew.close();

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
      log.setLogEndOffset(5000);
      indexNew.close();

      indexNew = new MockIndex(logFile, scheduler, log, factory, new StoreConfig(new VerifiableProperties(props)),
              new MessageStoreRecovery() {
        @Override
        public List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory)
                throws IOException {
          List<MessageInfo> infos = new ArrayList<MessageInfo>();
          infos.add(new MessageInfo(blobId4, 1000, true));
          infos.add(new MessageInfo(blobId5, 1000, BlobIndexValue.TTL_Infinite));
          return infos;
        }
      });
      value4 = indexNew.getValue(blobId4);
      value5 = indexNew.getValue(blobId5);
      Assert.assertEquals(value4.isFlagSet(BlobIndexValue.Flags.Delete_Index), true);
      Assert.assertEquals(value5.getTimeToLiveInMs(), BlobIndexValue.TTL_Infinite);
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
      }
      catch (StoreException e) {
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
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.Index_Creation_Failure);
      }

      log.close();
      scheduler.shutdown();

    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexBatch() throws IOException {
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
      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 0, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 100, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 300, flags, 12567));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(0, 600));
      BlobIndexValue value1 = index.getValue(blobId1);
      BlobIndexValue value2 = index.getValue(blobId2);
      BlobIndexValue value3 = index.getValue(blobId3);
      Assert.assertEquals(value1.getOffset(), 0);
      Assert.assertEquals(value2.getOffset(), 100);
      Assert.assertEquals(value3.getOffset(), 300);

      BlobIndexValue value4 = index.getValue(new MockId("id4"));
      try {
        index.addToIndex(new BlobIndexEntry(new MockId("id5"), value4), new FileSpan(500, 600));
        Assert.assertTrue(false);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue(true);
      }
    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexRead() throws IOException {
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

      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 0));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 100));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 300, System.currentTimeMillis()));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
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
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(blobId2, new FileSpan(600, 700));
      try {
        index.getBlobReadInfo(blobId2);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      // read ttl expired item
      index.updateTTL(blobId1, 1234, new FileSpan(700, 800));
      try {
        index.getBlobReadInfo(blobId1);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }

      // try to delete or update a missing blob
      try {
        index.markAsDeleted(new MockId("id5"), new FileSpan(800, 900));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);

      }
      try {
        index.updateTTL(new MockId("id6"), 1234, new FileSpan(900, 1000));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testMissingEntries() throws IOException {
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

      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 0));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 100));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 300));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, new FileSpan(0, 600));
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      MockId missingId = new MockId("id4");
      keys.add(missingId);
      keys.add(blobId1);
      keys.add(blobId2);
      List<StoreKey> missing = index.findMissingEntries(keys);
      Assert.assertEquals(missing.size(), 1);
      Assert.assertArrayEquals(missing.get(0).toBytes(), missingId.toBytes());
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

}
