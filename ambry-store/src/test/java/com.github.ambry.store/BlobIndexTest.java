package com.github.ambry.store;

import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.shared.BlobId;
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
    public MockIndex(String datadir, Scheduler scheduler, Log log) throws StoreException {
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
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(new BlobId(blobId1),
              new BlobIndexValue(100, 1000, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(new BlobId(blobId2),
              new BlobIndexValue(200, 2000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(new BlobId(blobId3),
              new BlobIndexValue(300, 3000, flags, 12567));
      index.addToIndex(entry1, 3000);
      index.addToIndex(entry2, 4000);
      index.addToIndex(entry3, 5000);
      BlobIndexValue value1 = index.getValue(new BlobId(blobId1));
      BlobIndexValue value2 = index.getValue(new BlobId(blobId2));
      BlobIndexValue value3 = index.getValue(new BlobId(blobId3));
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
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(new BlobId(blobId1),
              new BlobIndexValue(100, 1000, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(new BlobId(blobId2),
              new BlobIndexValue(200, 2000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(new BlobId(blobId3),
              new BlobIndexValue(300, 3000, flags, 12567));
      index.addToIndex(entry1, 3000);
      index.addToIndex(entry2, 4000);
      index.addToIndex(entry3, 5000);
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile, scheduler, log);

      BlobIndexValue value1 = indexNew.getValue(new BlobId(blobId1));
      BlobIndexValue value2 = indexNew.getValue(new BlobId(blobId2));
      BlobIndexValue value3 = indexNew.getValue(new BlobId(blobId3));
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
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(new BlobId(blobId1),
              new BlobIndexValue(100, 1000, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(new BlobId(blobId2),
              new BlobIndexValue(200, 2000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(new BlobId(blobId3),
              new BlobIndexValue(300, 3000, flags, 12567));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 5000);
      BlobIndexValue value1 = index.getValue(new BlobId(blobId1));
      BlobIndexValue value2 = index.getValue(new BlobId(blobId2));
      BlobIndexValue value3 = index.getValue(new BlobId(blobId3));
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);

      BlobIndexValue value4 = index.getValue(new BlobId("id4"));
      try {
        index.addToIndex(new BlobIndexEntry(new BlobId("id4"), value4), 4000);
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
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      BlobIndexEntry entry1 = new BlobIndexEntry(new BlobId(blobId1),
              new BlobIndexValue(100, 1000));
      BlobIndexEntry entry2 = new BlobIndexEntry(new BlobId(blobId2),
              new BlobIndexValue(200, 2000));
      BlobIndexEntry entry3 = new BlobIndexEntry(new BlobId(blobId3),
              new BlobIndexValue(300, 3000, System.currentTimeMillis()));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 5000);
      // simple read
      BlobReadOptions readOptions = index.getBlobReadInfo(new BlobId(blobId1));
      Assert.assertEquals(readOptions.getOffset(), 1000);
      Assert.assertEquals(readOptions.getSize(), 100);
      Assert.assertEquals(readOptions.getTTL(), -1);

      // read missing item
      try {
        index.getBlobReadInfo(new BlobId("id4"));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(new BlobId(blobId2), 6000);
      try {
        index.getBlobReadInfo(new BlobId(blobId2));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      // read ttl expired item
      index.updateTTL(new BlobId(blobId1), 1234, 7000);
      try {
        index.getBlobReadInfo(new BlobId(blobId1));
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }

      // try to delete or update a missing blob
      try {
        index.markAsDeleted(new BlobId("id4"), 8000);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);

      }
      try {
        index.updateTTL(new BlobId("id5"), 1234, 9000);
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
      StoreMetrics metrics = new StoreMetrics("test", registry);
      Log log = new Log(logFile, metrics);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      BlobIndexEntry entry1 = new BlobIndexEntry(new BlobId(blobId1),
              new BlobIndexValue(100, 1000));
      BlobIndexEntry entry2 = new BlobIndexEntry(new BlobId(blobId2),
              new BlobIndexValue(200, 2000));
      BlobIndexEntry entry3 = new BlobIndexEntry(new BlobId(blobId3),
              new BlobIndexValue(300, 3000));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 5000);
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      StoreKey key1 = new BlobId("id4");
      StoreKey key2 = new BlobId(blobId1);
      StoreKey key3 = new BlobId(blobId2);
      keys.add(key1);
      keys.add(key2);
      keys.add(key3);
      List<StoreKey> missing = index.findMissingEntries(keys);
      Assert.assertEquals(missing.size(), 1);
      Assert.assertArrayEquals(missing.get(0).toBytes(), new BlobId("id4").toBytes());
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

}
