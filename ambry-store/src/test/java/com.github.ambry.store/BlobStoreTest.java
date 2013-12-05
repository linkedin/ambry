package com.github.ambry.store;


import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.shared.BlobId;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Scheduler;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class BlobStoreTest {

  /**
   * Create a temporary file
   */
  File tempFile() throws IOException {
    File f = File.createTempFile("ambry", ".tmp");
    f.deleteOnExit();
    return f;
  }

  class MockMessageWriteSet implements MessageWriteSet {

    public ByteBuffer bufToWrite;
    public List<MessageInfo> info;

    public MockMessageWriteSet(ByteBuffer bufToWrite, List<MessageInfo> info) {
      this.bufToWrite = bufToWrite;
      this.info = info;
    }

    @Override
    public long writeTo(Write writeChannel) throws IOException {
      return writeChannel.appendFrom(bufToWrite);
    }

    @Override
    public List<MessageInfo> getMessageSetInfo() {
      return info;
    }
  }

  @Test
  public void storePutTest() throws IOException {
    try {
      Scheduler scheduler = new Scheduler(4, "thread", false);
      scheduler.startup();
      File tempFile = tempFile();
      File indexFile = new File(tempFile.getParent());
      for (File c : indexFile.listFiles())
        c.delete();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      Properties props = new Properties();
      props.setProperty("store.data.dir", tempFile.getParent());
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      MetricsRegistryMap registryMap = new MetricsRegistryMap("Test");
      Store store = new BlobStore(config, scheduler, registryMap);
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MessageInfo info1 = new MessageInfo(new BlobId("id1"), 1000);
      MessageInfo info2 = new MessageInfo(new BlobId("id2"), 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existance
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(new BlobId("id1"));
      keys.add(new BlobId("id2"));
      StoreInfo info = store.get(keys);
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      Assert.assertEquals(readSet.sizeInBytes(1), 1000);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }

      // put a blob that already exist
      new Random().nextBytes(bufToWrite);
      info1 = new MessageInfo(new BlobId("id1"), 1000);
      listInfo = new ArrayList<MessageInfo>(1);
      listInfo.add(info1);

      set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      try {
        store.put(set);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertTrue(e.getErrorCode() == StoreErrorCodes.Already_Exist);
      }
    }
    catch (StoreException e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void storeGetTest() throws IOException {
    try {
      Scheduler scheduler = new Scheduler(4, "thread", false);
      scheduler.startup();
      File tempFile = tempFile();
      File indexFile = new File(tempFile.getParent());
      for (File c : indexFile.listFiles())
        c.delete();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      Properties props = new Properties();
      props.setProperty("store.data.dir", tempFile.getParent());
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      MetricsRegistryMap registryMap = new MetricsRegistryMap("Test");
      Store store = new BlobStore(config, scheduler, registryMap);
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MessageInfo info1 = new MessageInfo(new BlobId("id1"), 1000);
      MessageInfo info2 = new MessageInfo(new BlobId("id2"), 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existance
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(new BlobId("id1"));
      keys.add(new BlobId("id2"));
      StoreInfo info = store.get(keys);
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      Assert.assertEquals(readSet.sizeInBytes(1), 1000);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }
    }
    catch (StoreException e) {
      Assert.assertEquals(false, true);
    }

  }

  @Test
  public void storeDeleteTest() throws IOException {
    try {
      Scheduler scheduler = new Scheduler(4, "thread", false);
      scheduler.startup();
      File tempFile = tempFile();
      File indexFile = new File(tempFile.getParent());
      for (File c : indexFile.listFiles())
        c.delete();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      Properties props = new Properties();
      props.setProperty("store.data.dir", tempFile.getParent());
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      MetricsRegistryMap registryMap = new MetricsRegistryMap("Test");
      Store store = new BlobStore(config, scheduler, registryMap);
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MessageInfo info1 = new MessageInfo(new BlobId("id1"), 1000);
      MessageInfo info2 = new MessageInfo(new BlobId("id2"), 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existance
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(new BlobId("id1"));
      keys.add(new BlobId("id2"));
      StoreInfo info = store.get(keys);
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);
      Assert.assertEquals(readSet.sizeInBytes(0), 1000);
      Assert.assertEquals(readSet.sizeInBytes(1), 1000);
      byte[] output = new byte[1000];
      readSet.writeTo(0, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i]);
      }
      readSet.writeTo(1, Channels.newChannel(new ByteBufferOutputStream(ByteBuffer.wrap(output))), 0, 1000);
      for (int i = 1000; i < 2000; i++) {
        Assert.assertEquals(bufToWrite[i], output[i - 1000]);
      }

      // delete an id
      byte[] bufToDelete = new byte[1000];
      new Random().nextBytes(bufToDelete);

      MessageInfo info3 = new MessageInfo(new BlobId("id1"), 1000, 1234);
      ArrayList<MessageInfo> listInfo1 = new ArrayList<MessageInfo>(1);
      listInfo1.add(info3);
      MessageWriteSet setToDelete = new MockMessageWriteSet(ByteBuffer.wrap(bufToDelete), listInfo1);
      store.delete(setToDelete);
      ArrayList<StoreKey> keysDeleted = new ArrayList<StoreKey>();
      keysDeleted.add(new BlobId("id1"));
      try {
        store.get(keysDeleted);
        Assert.assertEquals(false, true);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.ID_Deleted);
      }
      keysDeleted.clear();
      keysDeleted.add(new BlobId("id2"));
      store.get(keysDeleted);
      Assert.assertEquals(true, true);
    }
    catch (StoreException e) {
      Assert.assertEquals(false, true);
    }
  }

  @Test
  public void storeUpdateTTLTest() throws IOException {
    try {
      Scheduler scheduler = new Scheduler(4, "thread", false);
      scheduler.startup();
      File tempFile = tempFile();
      File indexFile = new File(tempFile.getParent());
      for (File c : indexFile.listFiles())
        c.delete();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      Properties props = new Properties();
      props.setProperty("store.data.dir", tempFile.getParent());
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      MetricsRegistryMap registryMap = new MetricsRegistryMap("Test");
      Store store = new BlobStore(config, scheduler, registryMap);
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MessageInfo info1 = new MessageInfo(new BlobId("id1"), 1000);
      MessageInfo info2 = new MessageInfo(new BlobId("id2"), 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existance
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(new BlobId("id1"));
      keys.add(new BlobId("id2"));
      StoreInfo info = store.get(keys);
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);

      // update TTL
      byte[] bufToUpdateTTL = new byte[1000];
      new Random().nextBytes(bufToUpdateTTL);

      MessageInfo info3 = new MessageInfo(new BlobId("id1"), 1000, 1234);
      ArrayList<MessageInfo> listInfo1 = new ArrayList<MessageInfo>(1);
      listInfo1.add(info3);
      MessageWriteSet setToUpdateTTL = new MockMessageWriteSet(ByteBuffer.wrap(bufToUpdateTTL), listInfo1);
      store.updateTTL(setToUpdateTTL);
      ArrayList<StoreKey> keysUpdated = new ArrayList<StoreKey>();
      keysUpdated.add(new BlobId("id1"));
      try {
        store.get(keysUpdated);
        Assert.assertEquals(false, true);
      }
      catch (StoreException e) {
        Assert.assertEquals(e.getErrorCode(), StoreErrorCodes.TTL_Expired);
      }
      keysUpdated.clear();
      keysUpdated.add(new BlobId("id2"));
      store.get(keysUpdated);
      Assert.assertEquals(true, true);

    }
    catch (StoreException e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void storeShutdownTest() throws IOException {
    try {
      Scheduler scheduler = new Scheduler(4, "thread", false);
      scheduler.startup();
      File tempFile = tempFile();
      File indexFile = new File(tempFile.getParent());
      for (File c : indexFile.listFiles())
        c.delete();
      RandomAccessFile randomFile = new RandomAccessFile(tempFile.getParent() + File.separator + "log_current", "rw");
      // preallocate file
      randomFile.setLength(5000);
      Properties props = new Properties();
      props.setProperty("store.data.dir", tempFile.getParent());
      VerifiableProperties verifyProperty = new VerifiableProperties(props);
      verifyProperty.verify();
      StoreConfig config = new StoreConfig(verifyProperty);
      MetricsRegistryMap registryMap = new MetricsRegistryMap("Test");
      Store store = new BlobStore(config, scheduler, registryMap);
      store.start();
      byte[] bufToWrite = new byte[2000];
      new Random().nextBytes(bufToWrite);
      MessageInfo info1 = new MessageInfo(new BlobId("id1"), 1000);
      MessageInfo info2 = new MessageInfo(new BlobId("id2"), 1000);
      ArrayList<MessageInfo> listInfo = new ArrayList<MessageInfo>(2);
      listInfo.add(info1);
      listInfo.add(info2);

      MessageWriteSet set = new MockMessageWriteSet(ByteBuffer.wrap(bufToWrite), listInfo);
      store.put(set);

      // verify existance
      ArrayList<StoreKey> keys = new ArrayList<StoreKey>();
      keys.add(new BlobId("id1"));
      keys.add(new BlobId("id2"));
      StoreInfo info = store.get(keys);
      MessageReadSet readSet = info.getMessageReadSet();
      Assert.assertEquals(readSet.count(), 2);

      // close store
      store.shutdown();

      try {
        store.get(keys);
        Assert.assertTrue(false);
      }
      catch (StoreException e) {
        Assert.assertTrue(e.getErrorCode() == StoreErrorCodes.Store_Not_Started);
      }

    }
    catch (StoreException e) {
      Assert.assertTrue(false);
    }
  }
}
