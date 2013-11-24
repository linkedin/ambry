package com.github.ambry.store;

import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BlobIndexTest {

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  class MockIndex extends BlobIndex {
    public MockIndex(String datadir, Scheduler scheduler, Log log) throws IndexCreationException {
      super(datadir, scheduler, log);
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
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000, flags, 12345));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000, flags, 12567));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000, flags, 12567));
      index.AddToIndex(entry1, 3000);
      index.AddToIndex(entry2, 4000);
      index.AddToIndex(entry3, 5000);
      BlobIndex.BlobIndexValue value1 = index.getValue(factory.getStoreKey(blobId1));
      BlobIndex.BlobIndexValue value2 = index.getValue(factory.getStoreKey(blobId2));
      BlobIndex.BlobIndexValue value3 = index.getValue(factory.getStoreKey(blobId3));
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);
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
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000, flags, 12345));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000, flags, 12567));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000, flags, 12567));
      index.AddToIndex(entry1, 3000);
      index.AddToIndex(entry2, 4000);
      index.AddToIndex(entry3, 5000);
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile, scheduler, log);

      BlobIndex.BlobIndexValue value1 = indexNew.getValue(factory.getStoreKey(blobId1));
      BlobIndex.BlobIndexValue value2 = indexNew.getValue(factory.getStoreKey(blobId2));
      BlobIndex.BlobIndexValue value3 = indexNew.getValue(factory.getStoreKey(blobId3));
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);
      indexNew.stopScheduler();
      indexNew.deleteAll();
      indexNew.close();

      File toModify = new File(logFile, "index_current");
      FileChannel channelToModify = Utils.openChannel(toModify, true);
      channelToModify.truncate(0);
      channelToModify.force(true);
      scheduler.startup();

      MockIndex indexFail = new MockIndex(logFile, scheduler, log);
      Assert.assertTrue(indexFail.isEmpty());
      indexFail.close();

      byte[] salt = new byte[1];
      salt[0] = 1;
      channelToModify.write(ByteBuffer.wrap(salt));  // write version 1

      MockIndex indexReadFail = new MockIndex(logFile, scheduler, log);
      Assert.assertTrue(indexReadFail.isEmpty());
      indexReadFail.close();

      channelToModify.truncate(0);
      byte[] addOnlyVersion = new byte[1];
      addOnlyVersion[0] = 0;
      channelToModify.write(ByteBuffer.wrap(addOnlyVersion));

      MockIndex indexEmptyLine = new MockIndex(logFile, scheduler, log);
      Assert.assertTrue(indexEmptyLine.isEmpty());
      indexEmptyLine.close();

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
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000, flags, 12345));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000, flags, 12567));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000, flags, 12567));
      ArrayList<BlobIndex.BlobIndexEntry> list = new ArrayList<BlobIndex.BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.AddToIndex(list, 5000);
      BlobIndex.BlobIndexValue value1 = index.getValue(factory.getStoreKey(blobId1));
      BlobIndex.BlobIndexValue value2 = index.getValue(factory.getStoreKey(blobId2));
      BlobIndex.BlobIndexValue value3 = index.getValue(factory.getStoreKey(blobId3));
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);

      BlobIndex.BlobIndexValue value4 = index.getValue(factory.getStoreKey("id4"));
      try {
        index.AddToIndex(new BlobIndex.BlobIndexEntry(factory.getStoreKey("id4"), value4), 4000);
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
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000, System.currentTimeMillis()));
      ArrayList<BlobIndex.BlobIndexEntry> list = new ArrayList<BlobIndex.BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.AddToIndex(list, 5000);
      // simple read
      BlobReadOptions readOptions = index.getBlobReadInfo(factory.getStoreKey(blobId1));
      Assert.assertEquals(readOptions.getOffset(), 1000);
      Assert.assertEquals(readOptions.getSize(), 100);
      Assert.assertEquals(readOptions.getTTL(), -1);

      // read missing item
      try {
        index.getBlobReadInfo(factory.getStoreKey("id4"));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(factory.getStoreKey(blobId2), 6000);
      try {
        index.getBlobReadInfo(factory.getStoreKey(blobId2));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      // read ttl expired item
      index.updateTTL(factory.getStoreKey(blobId1), 1234, 7000);
      try {
        index.getBlobReadInfo(factory.getStoreKey(blobId1));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }

      // try to delete or update a missing blob
      try {
        index.markAsDeleted(factory.getStoreKey("id4"), 8000);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);

      }
      try {
        index.updateTTL(factory.getStoreKey("id5"), 1234, 9000);
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
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getStoreKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000));
      ArrayList<BlobIndex.BlobIndexEntry> list = new ArrayList<BlobIndex.BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.AddToIndex(list, 5000);
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      StoreKey key1 = factory.getStoreKey("id4");
      StoreKey key2 = factory.getStoreKey(blobId1);
      StoreKey key3 = factory.getStoreKey(blobId2);
      keys.add(key1);
      keys.add(key2);
      keys.add(key3);
      List<StoreKey> missing = index.findMissingEntries(keys);
      Assert.assertEquals(missing.size(), 1);
      Assert.assertEquals(missing.get(0).toBytes(), factory.getStoreKey("id4").toBytes());
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

}
