package com.github.ambry.store;

import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

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
  }

  @Test
  public void testIndexBasic() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile, "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000, flags, 12345));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000, flags, 12567));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000, flags, 12567));
      index.AddToIndex(entry1, 3000);
      index.AddToIndex(entry2, 4000);
      index.AddToIndex(entry3, 5000);
      BlobIndex.BlobIndexValue value1 = index.getValue(factory.getKey(blobId1));
      BlobIndex.BlobIndexValue value2 = index.getValue(factory.getKey(blobId2));
      BlobIndex.BlobIndexValue value3 = index.getValue(factory.getKey(blobId3));
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);
    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }


  @Test
  public void testIndexRestore() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile + "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000, flags, 12345));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000, flags, 12567));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000, flags, 12567));
      index.AddToIndex(entry1, 3000);
      index.AddToIndex(entry2, 4000);
      index.AddToIndex(entry3, 5000);
      index.close();

      // create a new index and ensure the index is restored
      MockIndex indexNew = new MockIndex(logFile, scheduler, log);

      BlobIndex.BlobIndexValue value1 = indexNew.getValue(factory.getKey(blobId1));
      BlobIndex.BlobIndexValue value2 = indexNew.getValue(factory.getKey(blobId2));
      BlobIndex.BlobIndexValue value3 = indexNew.getValue(factory.getKey(blobId3));
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);
      File toDelete = new File(logFile + "index_current");
      toDelete.delete();
    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void testIndexBatch() throws IOException {
    try {
      String logFile = tempFile().getParent();
      File indexFile = new File(logFile + "index_current");
      indexFile.delete();
      Scheduler scheduler = new Scheduler(1, false);
      scheduler.startup();
      Log log = new Log(logFile);
      MockIndex index = new MockIndex(logFile, scheduler, log);
      String blobId1 = "id1";
      String blobId2 = "id2";
      String blobId3 = "id3";
      StoreKeyFactory  factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");

      byte flags = 3;
      BlobIndex.BlobIndexEntry entry1 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId1),
              new BlobIndex.BlobIndexValue(100, 1000, flags, 12345));
      BlobIndex.BlobIndexEntry entry2 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId2),
              new BlobIndex.BlobIndexValue(200, 2000, flags, 12567));
      BlobIndex.BlobIndexEntry entry3 = new BlobIndex.BlobIndexEntry(factory.getKey(blobId3),
              new BlobIndex.BlobIndexValue(300, 3000, flags, 12567));
      ArrayList<BlobIndex.BlobIndexEntry> list = new ArrayList<BlobIndex.BlobIndexEntry>();
      list.add(entry1);
      list.add(entry2);
      list.add(entry3);
      index.AddToIndex(list, 5000);
      BlobIndex.BlobIndexValue value1 = index.getValue(factory.getKey(blobId1));
      BlobIndex.BlobIndexValue value2 = index.getValue(factory.getKey(blobId2));
      BlobIndex.BlobIndexValue value3 = index.getValue(factory.getKey(blobId3));
      Assert.assertEquals(value1.getOffset(), 1000);
      Assert.assertEquals(value2.getOffset(), 2000);
      Assert.assertEquals(value3.getOffset(), 3000);
    }
    catch (Exception e) {
      Assert.assertEquals(false, true);
    }
  }
}
