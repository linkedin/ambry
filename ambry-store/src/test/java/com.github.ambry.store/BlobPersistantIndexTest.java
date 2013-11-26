package com.github.ambry.store;


import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.shared.BlobId;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import junit.framework.Assert;
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

  class MockIndex extends BlobPersistantIndex {
    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config) throws StoreException {
      super(datadir, scheduler, log, config);
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
    try {
      // create a new index
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");
      IndexInfo info = new IndexInfo(tempFile().getParent(), 0, factory, 5, BlobIndexValue.Index_Value_Size_In_Bytes);
      BlobIndexValue value = new BlobIndexValue(1000, 0, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id1"), value), 1000);
      value = new BlobIndexValue(1000, 1000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id2"), value), 2000);
      value = new BlobIndexValue(1000, 2000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id3"), value), 3000);
      value = new BlobIndexValue(1000, 3000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id4"), value), 4000);
      value = new BlobIndexValue(1000, 4000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id5"), value), 5000);
      value = new BlobIndexValue(1000, 5000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id6"), value), 6000);
      value = new BlobIndexValue(1000, 6000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id7"), value), 7000);
      value = new BlobIndexValue(1000, 7000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id8"), value), 8000);
      value = new BlobIndexValue(1000, 8000, (byte)0);
      info.AddEntry(new BlobIndexEntry(new BlobId("id9"), value), 9000);

      Assert.assertEquals(info.find(new BlobId("id1")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id1")).getOffset(), 0);
      Assert.assertEquals(info.find(new BlobId("id2")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id2")).getOffset(), 1000);
      Assert.assertEquals(info.find(new BlobId("id3")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id3")).getOffset(), 2000);
      Assert.assertEquals(info.find(new BlobId("id4")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id4")).getOffset(), 3000);
      Assert.assertEquals(info.find(new BlobId("id5")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id5")).getOffset(), 4000);
      Assert.assertEquals(info.find(new BlobId("id6")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id6")).getOffset(), 5000);
      Assert.assertEquals(info.find(new BlobId("id7")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id7")).getOffset(), 6000);
      Assert.assertEquals(info.find(new BlobId("id8")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id8")).getOffset(), 7000);

      info.writeIndexToFile(3000);
      IndexInfo infonew = new IndexInfo(info.getFile(), false, factory);
      Assert.assertEquals(infonew.find(new BlobId("id1")).getSize(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id1")).getOffset(), 0);
      Assert.assertEquals(infonew.find(new BlobId("id2")).getSize(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id2")).getOffset(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id3")).getSize(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id3")).getOffset(), 2000);
      Assert.assertEquals(infonew.find(new BlobId("id4")).getSize(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id4")).getOffset(), 3000);
      Assert.assertEquals(infonew.find(new BlobId("id5")).getSize(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id5")).getOffset(), 4000);
      Assert.assertEquals(infonew.find(new BlobId("id6")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id6")).getOffset(), 5000);
      Assert.assertEquals(infonew.find(new BlobId("id7")).getSize(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id7")).getOffset(), 6000);
      Assert.assertEquals(infonew.find(new BlobId("id8")).getSize(), 1000);
      Assert.assertEquals(infonew.find(new BlobId("id8")).getOffset(), 7000);

      info.map(false);
      Assert.assertEquals(info.find(new BlobId("id1")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id1")).getOffset(), 0);
      Assert.assertEquals(info.find(new BlobId("id2")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id2")).getOffset(), 1000);
      Assert.assertEquals(info.find(new BlobId("id3")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id3")).getOffset(), 2000);
      Assert.assertEquals(info.find(new BlobId("id4")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id4")).getOffset(), 3000);
      Assert.assertEquals(info.find(new BlobId("id5")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id5")).getOffset(), 4000);
      Assert.assertEquals(info.find(new BlobId("id6")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id6")).getOffset(), 5000);
      Assert.assertEquals(info.find(new BlobId("id7")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id7")).getOffset(), 6000);
      Assert.assertEquals(info.find(new BlobId("id8")).getSize(), 1000);
      Assert.assertEquals(info.find(new BlobId("id8")).getOffset(), 7000);
      // check invalid cases
      Assert.assertNull(info.find(new BlobId("id122")));
      Assert.assertNull(info.find(new BlobId("id122")));
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testIndexBasic() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "0_index");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      MockIndex index = new MockIndex(logFile, scheduler, log, config);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(new BlobId(blobId1),
              new BlobIndexValue(100, 1000, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(new BlobId(blobId2),
              new BlobIndexValue(200, 2000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(new BlobId(blobId3),
              new BlobIndexValue(300, 3000, flags, 12567));
      index.AddToIndex(entry1, 3000);
      index.AddToIndex(entry2, 4000);
      index.AddToIndex(entry3, 5000);
      BlobIndexValue value1 = index.getValue(new BlobId(blobId1));
      BlobIndexValue value2 = index.getValue(new BlobId(blobId2));
      BlobIndexValue value3 = index.getValue(new BlobId(blobId3));
      org.junit.Assert.assertEquals(value1.getOffset(), 1000);
      org.junit.Assert.assertEquals(value2.getOffset(), 2000);
      org.junit.Assert.assertEquals(value3.getOffset(), 3000);
      indexFile.delete();
      scheduler.shutdown();
      log.close();
    }
    catch (Exception e) {
      org.junit.Assert.assertEquals(false, true);
    }
  }


  @Test
  public void testIndexRestore() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "0_index");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      MockIndex index = new MockIndex(logFile, scheduler, log, config);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(new BlobId(blobId1),
              new BlobIndexValue(100, 0, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(new BlobId(blobId2),
              new BlobIndexValue(200, 1000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(new BlobId(blobId3),
              new BlobIndexValue(300, 2000, flags, 12567));
      index.AddToIndex(entry1, 3000);
      index.AddToIndex(entry2, 4000);
      index.AddToIndex(entry3, 5000);
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile, scheduler, log, config);

      BlobIndexValue value1 = indexNew.getValue(new BlobId(blobId1));
      BlobIndexValue value2 = indexNew.getValue(new BlobId(blobId2));
      BlobIndexValue value3 = indexNew.getValue(new BlobId(blobId3));
      org.junit.Assert.assertEquals(value1.getOffset(), 0);
      org.junit.Assert.assertEquals(value2.getOffset(), 1000);
      org.junit.Assert.assertEquals(value3.getOffset(), 2000);
      indexNew.stopScheduler();
      indexNew.deleteAll();
      indexNew.close();

      File toModify = new File(logFile, "0_index");
      FileChannel channelToModify = Utils.openChannel(toModify, true);
      channelToModify.truncate(0);
      channelToModify.force(true);
      scheduler.startup();

      try {
        MockIndex indexFail = new MockIndex(logFile, scheduler, log, config);
        Assert.assertFalse(true);
      }
      catch (StoreException e) {
        Assert.assertTrue(true);
      }

      byte[] salt = new byte[1];
      salt[0] = 1;
      channelToModify.write(ByteBuffer.wrap(salt));  // write version 1

      try {
        MockIndex indexReadFail = new MockIndex(logFile, scheduler, log, config);
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
        MockIndex indexEmptyLine = new MockIndex(logFile, scheduler, log, config);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertTrue(true);
      }

      log.close();
      scheduler.shutdown();
    }
    catch (Exception e) {
      org.junit.Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexBatch() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "0_index");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      MockIndex index = new MockIndex(logFile, scheduler, log, config);
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
      index.AddToIndex(list, 5000);
      BlobIndexValue value1 = index.getValue(new BlobId(blobId1));
      BlobIndexValue value2 = index.getValue(new BlobId(blobId2));
      BlobIndexValue value3 = index.getValue(new BlobId(blobId3));
      org.junit.Assert.assertEquals(value1.getOffset(), 1000);
      org.junit.Assert.assertEquals(value2.getOffset(), 2000);
      org.junit.Assert.assertEquals(value3.getOffset(), 3000);

      BlobIndexValue value4 = index.getValue(new BlobId("id4"));
      try {
        index.AddToIndex(new BlobIndexEntry(new BlobId("id4"), value4), 4000);
        org.junit.Assert.assertTrue(false);
      }
      catch (IllegalArgumentException e) {
        org.junit.Assert.assertTrue(true);
      }
    }
    catch (Exception e) {
      org.junit.Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexRead() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "0_index");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      MockIndex index = new MockIndex(logFile, scheduler, log, config);
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
      index.AddToIndex(list, 5000);
      // simple read
      BlobReadOptions readOptions = index.getBlobReadInfo(new BlobId(blobId1));
      org.junit.Assert.assertEquals(readOptions.getOffset(), 1000);
      org.junit.Assert.assertEquals(readOptions.getSize(), 100);
      org.junit.Assert.assertEquals(readOptions.getTTL(), -1);

      // read missing item
      try {
        index.getBlobReadInfo(new BlobId("id4"));
        org.junit.Assert.assertTrue(false);
      }
      catch (StoreException e) {
        org.junit.Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }

      // read deleted item
      index.markAsDeleted(new BlobId(blobId2), 6000);
      try {
        index.getBlobReadInfo(new BlobId(blobId2));
        org.junit.Assert.assertTrue(false);
      }
      catch (StoreException e) {
        org.junit.Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      // read ttl expired item
      index.updateTTL(new BlobId(blobId1), 1234, 7000);
      try {
        index.getBlobReadInfo(new BlobId(blobId1));
        org.junit.Assert.assertTrue(false);
      }
      catch (StoreException e) {
        org.junit.Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }

      // try to delete or update a missing blob
      try {
        index.markAsDeleted(new BlobId("id4"), 8000);
        org.junit.Assert.assertTrue(false);
      }
      catch (StoreException e) {
        org.junit.Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);

      }
      try {
        index.updateTTL(new BlobId("id5"), 1234, 9000);
        org.junit.Assert.assertTrue(false);
      }
      catch (StoreException e) {
        org.junit.Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);
      }
    }
    catch (Exception e) {
      org.junit.Assert.assertTrue(false);
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
      StoreConfig config = new StoreConfig(new VerifiableProperties(new Properties()));
      MockIndex index = new MockIndex(logFile, scheduler, log, config);
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
      index.AddToIndex(list, 5000);
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      StoreKey key1 = new BlobId("id4");
      StoreKey key2 = new BlobId(blobId1);
      StoreKey key3 = new BlobId(blobId2);
      keys.add(key1);
      keys.add(key2);
      keys.add(key3);
      List<StoreKey> missing = index.findMissingEntries(keys);
      org.junit.Assert.assertEquals(missing.size(), 1);
      org.junit.Assert.assertArrayEquals(missing.get(0).toBytes(), new BlobId("id4").toBytes());
    }
    catch (Exception e) {
      org.junit.Assert.assertTrue(false);
    }
  }

  @Test
  public void testRollingIndex() {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(logFile, metrics);
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "500");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      MockIndex index = new MockIndex(logFile, scheduler, log, config);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";

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
      index.AddToIndex(list, 5000);
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      StoreKey key1 = new BlobId("id4");
      StoreKey key2 = new BlobId(blobId1);
      StoreKey key3 = new BlobId(blobId2);
      keys.add(key1);
      keys.add(key2);
      keys.add(key3);
      List<StoreKey> missing = index.findMissingEntries(keys);
      org.junit.Assert.assertEquals(missing.size(), 1);
      org.junit.Assert.assertArrayEquals(missing.get(0).toBytes(), new BlobId("id4").toBytes());
    }
    catch (Exception e) {
      org.junit.Assert.assertTrue(false);
    }
  }
}
