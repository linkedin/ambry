package com.github.ambry.store;

import com.github.ambry.MockSharedUtils;
import com.github.ambry.clustermap.MockClusterMap;
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
    map = MockSharedUtils.getMockClusterMap();
    try {
      factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory", new Object[]{map});
    }
    catch (Exception e) {
      throw new InstantiationException();
    }
  }

  class MockIndex extends BlobIndex {
    public MockIndex(String datadir, Scheduler scheduler, Log log, StoreKeyFactory factory) throws StoreException {
      super(datadir, scheduler, log, factory);
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

  public void shutdown() {
    map.shutdown();
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
      Log log = new Log(logFile, metrics, 10000);
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
      BlobId blobId1 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId2 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId3 = new BlobId(MockSharedUtils.getMockPartitionId());

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
      Log log = new Log(logFile, metrics, 10000);
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
      BlobId blobId1 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId2 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId3 = new BlobId(MockSharedUtils.getMockPartitionId());

      byte flags = 3;
      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 1000, flags, 12345));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 2000, flags, 12567));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 3000, flags, 12567));
      index.addToIndex(entry1, 3000);
      index.addToIndex(entry2, 4000);
      index.addToIndex(entry3, 5000);
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile, scheduler, log, factory);

      BlobIndexValue value1 = indexNew.getValue(blobId1);
      BlobIndexValue value2 = indexNew.getValue(blobId2);
      BlobIndexValue value3 = indexNew.getValue(blobId3);
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

      MockIndex indexFail = new MockIndex(logFile, scheduler, log, factory);
      Assert.assertTrue(indexFail.isEmpty());
      indexFail.close();

      byte[] salt = new byte[1];
      salt[0] = 1;
      channelToModify.write(ByteBuffer.wrap(salt));  // write version 1

      MockIndex indexReadFail = new MockIndex(logFile, scheduler, log, factory);
      Assert.assertTrue(indexReadFail.isEmpty());
      indexReadFail.close();

      channelToModify.truncate(0);
      byte[] addOnlyVersion = new byte[1];
      addOnlyVersion[0] = 0;
      channelToModify.write(ByteBuffer.wrap(addOnlyVersion));

      MockIndex indexEmptyLine = new MockIndex(logFile, scheduler, log, factory);
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
      Log log = new Log(logFile, metrics, 10000);
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
      BlobId blobId1 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId2 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId3 = new BlobId(MockSharedUtils.getMockPartitionId());

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

      BlobIndexValue value4 = index.getValue(new BlobId(MockSharedUtils.getMockPartitionId()));
      try {
        index.addToIndex(new BlobIndexEntry(new BlobId(MockSharedUtils.getMockPartitionId()), value4), 4000);
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
      Log log = new Log(logFile, metrics, 10000);
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
      BlobId blobId1 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId2 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId3 = new BlobId(MockSharedUtils.getMockPartitionId());

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
        index.getBlobReadInfo(new BlobId(MockSharedUtils.getMockPartitionId()));
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
        index.markAsDeleted(new BlobId(MockSharedUtils.getMockPartitionId()), 8000);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Not_Found);

      }
      try {
        index.updateTTL(new BlobId(MockSharedUtils.getMockPartitionId()), 1234, 9000);
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
      Log log = new Log(logFile, metrics, 10000);
      MockIndex index = new MockIndex(logFile, scheduler, log, factory);
      BlobId blobId1 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId2 = new BlobId(MockSharedUtils.getMockPartitionId());
      BlobId blobId3 = new BlobId(MockSharedUtils.getMockPartitionId());

      BlobIndexEntry entry1 = new BlobIndexEntry(blobId1, new BlobIndexValue(100, 1000));
      BlobIndexEntry entry2 = new BlobIndexEntry(blobId2, new BlobIndexValue(200, 2000));
      BlobIndexEntry entry3 = new BlobIndexEntry(blobId3, new BlobIndexValue(300, 3000));
      ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.addToIndex(list, 5000);
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      BlobId missingId = new BlobId(MockSharedUtils.getMockPartitionId());
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
