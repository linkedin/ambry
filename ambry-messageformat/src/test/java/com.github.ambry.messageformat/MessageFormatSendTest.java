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
package com.github.ambry.messageformat;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Crc32;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class MessageFormatSendTest {

  class MockMessageReadSet implements MessageReadSet {

    ArrayList<ByteBuffer> buffers;
    ArrayList<StoreKey> keys;

    public MockMessageReadSet(ArrayList<ByteBuffer> buffers, ArrayList<StoreKey> keys) {
      this.buffers = buffers;
      this.keys = keys;
    }

    @Override
    public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
      buffers.get(index).position((int) relativeOffset);
      buffers.get(index).limit((int) Math.min(buffers.get(index).limit(), relativeOffset + maxSize));
      int written = channel.write(buffers.get(index));
      buffers.get(index).clear();
      return written;
    }

    @Override
    public int count() {
      return buffers.size();
    }

    @Override
    public long sizeInBytes(int index) {
      return buffers.get(index).remaining();
    }

    @Override
    public StoreKey getKeyAt(int index) {
      return keys.get(index);
    }
  }

  @Test
  public void sendWriteTest() throws IOException, MessageFormatException {
    try {
      // create one buffer of size 1004

      // add header,system metadata, user metadata and data to the buffers
      ByteBuffer buf1 = ByteBuffer.allocate(1010);
      // fill header
      buf1.putShort((short) 1);                    // version
      buf1.putLong(950);                          // total size
      // put relative offsets
      buf1.putInt(60);                           // blob property relative offset
      buf1.putInt(-1);                           // delete relative offset
      buf1.putInt(81);                           // user metadata relative offset
      buf1.putInt(191);                          // data relative offset
      Crc32 crc = new Crc32();
      crc.update(buf1.array(), 0, buf1.position());
      buf1.putLong(crc.getValue());                          // crc
      String id = new String("012345678910123456789012");     // blob id
      buf1.putShort((short) id.length());
      buf1.put(id.getBytes());

      buf1.putShort((short) 1); // blob property version
      String attribute1 = "ttl";
      String attribute2 = "del";
      buf1.put(attribute1.getBytes()); // ttl name
      buf1.putLong(12345);             // ttl value
      buf1.put(attribute2.getBytes()); // delete name
      byte b = 1;
      buf1.put(b);      // delete flag
      buf1.putInt(456); //crc

      buf1.putShort((short) 1); // user metadata version
      buf1.putInt(100);
      byte[] usermetadata = new byte[100];
      new Random().nextBytes(usermetadata);
      buf1.put(usermetadata);
      buf1.putInt(123);

      buf1.putShort((short) 0); // blob version
      buf1.putLong(805);       // blob size
      byte[] data = new byte[805];         // blob
      new Random().nextBytes(data);
      buf1.put(data);
      buf1.putInt(123);                    // blob crc
      buf1.flip();

      ArrayList<ByteBuffer> listbuf = new ArrayList<ByteBuffer>();
      listbuf.add(buf1);
      ArrayList<StoreKey> storeKeys = new ArrayList<StoreKey>();
      storeKeys.add(new MockId("012345678910123456789012"));
      MessageReadSet readSet = new MockMessageReadSet(listbuf, storeKeys);

      MetricRegistry registry = new MetricRegistry();
      MessageFormatMetrics metrics = new MessageFormatMetrics(registry);
      // get all
      MessageFormatSend send = new MessageFormatSend(readSet, MessageFormatFlags.All, metrics, new MockIdFactory());
      Assert.assertEquals(send.sizeInBytes(), 1010);
      ByteBuffer bufresult = ByteBuffer.allocate(1010);
      WritableByteChannel channel1 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
      while (!send.isSendComplete()) {
        send.writeTo(channel1);
      }
      Assert.assertArrayEquals(buf1.array(), bufresult.array());

      // get blob
      MessageFormatSend send1 = new MessageFormatSend(readSet, MessageFormatFlags.Blob, metrics, new MockIdFactory());
      Assert.assertEquals(send1.sizeInBytes(), 819);
      bufresult.clear();
      WritableByteChannel channel2 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
      while (!send1.isSendComplete()) {
        send1.writeTo(channel2);
      }

      for (int i = 10; i < 815; i++) {
        Assert.assertEquals(data[i - 10], bufresult.array()[i]);
      }

      // get user metadata
      MessageFormatSend send2 =
          new MessageFormatSend(readSet, MessageFormatFlags.BlobUserMetadata, metrics, new MockIdFactory());
      Assert.assertEquals(send2.sizeInBytes(), 110);
      bufresult.clear();
      WritableByteChannel channel3 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
      while (!send2.isSendComplete()) {
        send2.writeTo(channel2);
      }

      bufresult.flip();
      verifyBlobUserMetadata(usermetadata, bufresult);

      // get blob properties
      MessageFormatSend send3 =
          new MessageFormatSend(readSet, MessageFormatFlags.BlobProperties, metrics, new MockIdFactory());
      Assert.assertEquals(send3.sizeInBytes(), 21);
      bufresult.clear();
      WritableByteChannel channel4 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
      while (!send3.isSendComplete()) {
        send3.writeTo(channel4);
      }

      bufresult.flip();
      verifyBlobProperties(bufresult);

      // get blob info
      MessageFormatSend send4 =
          new MessageFormatSend(readSet, MessageFormatFlags.BlobInfo, metrics, new MockIdFactory());
      Assert.assertEquals(send4.sizeInBytes(), 110 + 21);
      bufresult.clear();
      WritableByteChannel channel5 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
      while (!send4.isSendComplete()) {
        send4.writeTo(channel5);
      }

      bufresult.flip();
      verifyBlobProperties(bufresult);
      verifyBlobUserMetadata(usermetadata, bufresult);
    } catch (MessageFormatException e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }

  private void verifyBlobProperties(ByteBuffer bufresult) {
    Assert.assertEquals(bufresult.getShort(), (short) 1);
    byte[] attributes = new byte[3];
    bufresult.get(attributes);
    Assert.assertEquals("ttl", new String(attributes));
    Assert.assertEquals(12345, bufresult.getLong());
    bufresult.get(attributes);
    Assert.assertEquals("del", new String(attributes));
    Assert.assertEquals(1, bufresult.get());
    Assert.assertEquals(456, bufresult.getInt());
  }

  private void verifyBlobUserMetadata(byte[] usermetadata, ByteBuffer result) {
    // version
    Assert.assertEquals(result.getShort(), 1);
    // size
    Assert.assertEquals(result.getInt(), 100);
    // content
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(usermetadata[i], result.get());
    }
    // crc
    Assert.assertEquals(result.getInt(), 123);
  }

  @Test
  public void sendWriteTestWithBadId() throws IOException, MessageFormatException {
    try {
      // create one buffer of size 1004

      // add header,system metadata, user metadata and data to the buffers
      ByteBuffer buf1 = ByteBuffer.allocate(1010);
      // fill header
      buf1.putShort((short) 1);                    // version
      buf1.putLong(950);                          // total size
      // put relative offsets
      buf1.putInt(60);                           // blob property relative offset
      buf1.putInt(-1);                           // delete relative offset
      buf1.putInt(81);                           // user metadata relative offset
      buf1.putInt(191);                          // data relative offset
      Crc32 crc = new Crc32();
      crc.update(buf1.array(), 0, buf1.position());
      buf1.putLong(crc.getValue());                          // crc
      String id = new String("012345678910123456789012");     // blob id
      buf1.putShort((short) id.length());
      buf1.put(id.getBytes());

      buf1.putShort((short) 1); // blob property version
      String attribute1 = "ttl";
      String attribute2 = "del";
      buf1.put(attribute1.getBytes()); // ttl name
      buf1.putLong(12345);             // ttl value
      buf1.put(attribute2.getBytes()); // delete name
      byte b = 1;
      buf1.put(b);      // delete flag
      buf1.putInt(456); //crc

      buf1.putShort((short) 1); // user metadata version
      buf1.putInt(100);
      byte[] usermetadata = new byte[100];
      new Random().nextBytes(usermetadata);
      buf1.put(usermetadata);
      buf1.putInt(123);

      buf1.putShort((short) 0); // blob version
      buf1.putLong(805);       // blob size
      byte[] data = new byte[805];         // blob
      new Random().nextBytes(data);
      buf1.put(data);
      buf1.putInt(123);                    // blob crc
      buf1.flip();

      ArrayList<ByteBuffer> listbuf = new ArrayList<ByteBuffer>();
      listbuf.add(buf1);
      ArrayList<StoreKey> storeKeys = new ArrayList<StoreKey>();
      storeKeys.add(new MockId("012345678910123223233456789012"));
      MessageReadSet readSet = new MockMessageReadSet(listbuf, storeKeys);

      MetricRegistry registry = new MetricRegistry();
      MessageFormatMetrics metrics = new MessageFormatMetrics(registry);
      // get all
      MessageFormatSend send = new MessageFormatSend(readSet, MessageFormatFlags.All, metrics, new MockIdFactory());
      Assert.assertEquals(send.sizeInBytes(), 1010);
      ByteBuffer bufresult = ByteBuffer.allocate(1010);
      WritableByteChannel channel1 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
      while (!send.isSendComplete()) {
        send.writeTo(channel1);
      }
      Assert.assertArrayEquals(buf1.array(), bufresult.array());
      try {
        // get blob
        MessageFormatSend send1 = new MessageFormatSend(readSet, MessageFormatFlags.Blob, metrics, new MockIdFactory());
        Assert.assertTrue(false);
      } catch (MessageFormatException e) {
        Assert.assertTrue(e.getErrorCode() == MessageFormatErrorCodes.Store_Key_Id_MisMatch);
      }
    } catch (MessageFormatException e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }

  @Test
  public void messageReadSetIndexInputStreamTest() {
    try {
      ArrayList<ByteBuffer> listbuf = new ArrayList<ByteBuffer>();
      byte[] buf1 = new byte[1024];
      byte[] buf2 = new byte[2048];
      byte[] buf3 = new byte[4096];
      new Random().nextBytes(buf1);
      new Random().nextBytes(buf2);
      new Random().nextBytes(buf3);
      listbuf.add(ByteBuffer.wrap(buf1));
      listbuf.add(ByteBuffer.wrap(buf2));
      listbuf.add(ByteBuffer.wrap(buf3));
      ArrayList<StoreKey> storeKeys = new ArrayList<StoreKey>();
      storeKeys.add(new MockId("012345678910123223233456789012"));
      storeKeys.add(new MockId("012345678910123223233456789013"));
      storeKeys.add(new MockId("012345678910123223233456789014"));
      MessageReadSet readSet = new MockMessageReadSet(listbuf, storeKeys);
      MessageReadSetIndexInputStream stream1 = new MessageReadSetIndexInputStream(readSet, 0, 0);
      byte[] buf1Output = new byte[1024];
      stream1.read(buf1Output, 0, 1024);
      Assert.assertArrayEquals(buf1Output, buf1);
      MessageReadSetIndexInputStream stream2 = new MessageReadSetIndexInputStream(readSet, 1, 1024);
      byte[] buf2Output = new byte[1024];
      stream2.read(buf2Output, 0, 1024);
      for (int i = 0; i < 1024; i++) {
        Assert.assertEquals(buf2Output[i], buf2[i + 1024]);
      }
      MessageReadSetIndexInputStream stream3 = new MessageReadSetIndexInputStream(readSet, 2, 2048);
      byte[] buf3Output = new byte[2048];
      stream3.read(buf3Output, 0, 2048);
      for (int i = 0; i < 2048; i++) {
        Assert.assertEquals(buf3Output[i], buf3[i + 2048]);
      }
      try {
        stream3.read(buf3Output, 0, 1024);
        Assert.assertTrue(false);
      } catch (IOException e) {
        Assert.assertTrue(true);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(true, false);
    }
  }
}
