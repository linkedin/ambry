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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.TestUtils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class MessageFormatSendTest {
  private final String putFormat;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{PutMessageFormatInputStream.class.getSimpleName()}, {PutMessageFormatBlobV1InputStream.class.getSimpleName()}});
  }

  public MessageFormatSendTest(String putFormat) {
    this.putFormat = putFormat;
  }

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
  public void sendWriteTest() throws MessageFormatException, IOException {
    if (putFormat.equals(PutMessageFormatInputStream.class.getSimpleName())) {
      ByteBuffer encryptionKey = ByteBuffer.wrap(TestUtils.getRandomBytes(256));
      MessageFormatRecord.HEADER_VERSION_TO_USE = MessageFormatRecord.Message_Header_Version_V1;
      doSendWriteTest(null, null);
      doSendWriteTest(encryptionKey.duplicate(), null);
      MessageFormatRecord.HEADER_VERSION_TO_USE = MessageFormatRecord.Message_Header_Version_V2;
      doSendWriteTest(null, null);
      doSendWriteTest(ByteBuffer.allocate(0), ByteBuffer.allocate(0));
      doSendWriteTest(encryptionKey.duplicate(), encryptionKey.duplicate());
      MessageFormatRecord.HEADER_VERSION_TO_USE = MessageFormatRecord.Message_Header_Version_V1;
    } else {
      doSendWriteTest(null, null);
    }
  }

  private void doSendWriteTest(ByteBuffer encryptionKey, ByteBuffer expectedEncryptionKey)
      throws MessageFormatException, IOException {
    String serviceId = "serviceId";
    String ownerId = "owner";
    String contentType = "bin";
    short accountId = 10;
    short containerId = 2;

    byte[] blob = TestUtils.getRandomBytes(10000);
    byte[] userMetadata = TestUtils.getRandomBytes(2000);
    StoreKey storeKey = new MockId("012345678910123456789012");
    BlobProperties properties =
        new BlobProperties(blob.length, serviceId, ownerId, contentType, false, 100, accountId, containerId,
            encryptionKey != null);
    MessageFormatInputStream putStream;
    if (putFormat.equals(PutMessageFormatInputStream.class.getSimpleName())) {
      putStream = new PutMessageFormatInputStream(storeKey, encryptionKey, properties, ByteBuffer.wrap(userMetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), blob.length, BlobType.DataBlob);
    } else {
      putStream = new PutMessageFormatBlobV1InputStream(storeKey, properties, ByteBuffer.wrap(userMetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), blob.length, BlobType.DataBlob);
    }
    ByteBuffer buf1 = ByteBuffer.allocate((int) putStream.getSize());

    putStream.read(buf1.array());
    ArrayList<ByteBuffer> listbuf = new ArrayList<ByteBuffer>();
    listbuf.add(buf1);
    ArrayList<StoreKey> storeKeys = new ArrayList<StoreKey>();
    storeKeys.add(storeKey);
    MessageReadSet readSet = new MockMessageReadSet(listbuf, storeKeys);

    MetricRegistry registry = new MetricRegistry();
    MessageFormatMetrics metrics = new MessageFormatMetrics(registry);
    // get all
    MessageFormatSend send = new MessageFormatSend(readSet, MessageFormatFlags.All, metrics, new MockIdFactory());
    Assert.assertEquals(send.sizeInBytes(), putStream.getSize());
    Assert.assertEquals(1, send.getMessageMetadataList().size());
    Assert.assertEquals(null, send.getMessageMetadataList().get(0));
    ByteBuffer bufresult = ByteBuffer.allocate((int) putStream.getSize());
    WritableByteChannel channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }
    Assert.assertArrayEquals(buf1.array(), bufresult.array());

    // get blob
    send = new MessageFormatSend(readSet, MessageFormatFlags.Blob, metrics, new MockIdFactory());
    long blobRecordSize = putFormat.equals(PutMessageFormatInputStream.class.getSimpleName())
        ? MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blob.length)
        : MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blob.length);
    Assert.assertEquals(send.sizeInBytes(), blobRecordSize);
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    for (int i = 0; i < blob.length; i++) {
      Assert.assertEquals(blob[i],
          bufresult.array()[i + (int) blobRecordSize - MessageFormatRecord.Crc_Size - blob.length]);
    }

    if (expectedEncryptionKey == null) {
      Assert.assertEquals(null, send.getMessageMetadataList().get(0));
    } else {
      Assert.assertEquals(expectedEncryptionKey, send.getMessageMetadataList().get(0).getEncryptionKey());
    }

    // get user metadata
    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobUserMetadata, metrics, new MockIdFactory());
    long userMetadataRecordSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(userMetadata));
    Assert.assertEquals(send.sizeInBytes(), userMetadataRecordSize);
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    bufresult.flip();
    // read off the header.
    for (int i = 0; i < userMetadataRecordSize - MessageFormatRecord.Crc_Size - userMetadata.length; i++) {
      bufresult.get();
    }

    verifyBlobUserMetadata(userMetadata, bufresult);
    if (expectedEncryptionKey == null) {
      Assert.assertEquals(null, send.getMessageMetadataList().get(0));
    } else {
      Assert.assertEquals(expectedEncryptionKey, send.getMessageMetadataList().get(0).getEncryptionKey());
    }

    // get blob properties
    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobProperties, metrics, new MockIdFactory());
    long blobPropertiesRecordSize =
        MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(properties);
    Assert.assertEquals(send.sizeInBytes(), blobPropertiesRecordSize);
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    bufresult.flip();
    // read off the header.
    for (int i = 0;
        i < blobPropertiesRecordSize - MessageFormatRecord.Crc_Size - BlobPropertiesSerDe.getBlobPropertiesSerDeSize(
            properties); i++) {
      bufresult.get();
    }

    verifyBlobProperties(properties,
        BlobPropertiesSerDe.getBlobPropertiesFromStream(new DataInputStream(new ByteBufferInputStream(bufresult))));
    Assert.assertEquals(null, send.getMessageMetadataList().get(0));

    // get blob info
    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobInfo, metrics, new MockIdFactory());
    Assert.assertEquals(send.sizeInBytes(), blobPropertiesRecordSize + userMetadataRecordSize);
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    bufresult.flip();
    for (int i = 0;
        i < blobPropertiesRecordSize - MessageFormatRecord.Crc_Size - BlobPropertiesSerDe.getBlobPropertiesSerDeSize(
            properties); i++) {
      bufresult.get();
    }
    verifyBlobProperties(properties,
        BlobPropertiesSerDe.getBlobPropertiesFromStream(new DataInputStream(new ByteBufferInputStream(bufresult))));
    for (int i = 0; i < userMetadataRecordSize - userMetadata.length; i++) {
      bufresult.get();
    }
    verifyBlobUserMetadata(userMetadata, bufresult);
    if (expectedEncryptionKey == null) {
      Assert.assertEquals(null, send.getMessageMetadataList().get(0));
    } else {
      Assert.assertEquals(expectedEncryptionKey, send.getMessageMetadataList().get(0).getEncryptionKey());
    }
  }

  @Test
  public void sendWriteTestWithBadId() throws IOException, MessageFormatException {
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

  private void verifyBlobUserMetadata(byte[] usermetadata, ByteBuffer result) {
    for (int i = 0; i < usermetadata.length; i++) {
      Assert.assertEquals(usermetadata[i], result.get());
    }
  }

  /**
   * Verifies that the given two {@link BlobProperties} have the same fields
   */
  private void verifyBlobProperties(BlobProperties a, BlobProperties b) {
    Assert.assertTrue(
        a.getServiceId().equals(b.getServiceId()) && a.getOwnerId().equals(b.getOwnerId()) && a.getContentType()
            .equals(b.getContentType()) && a.isPrivate() == b.isPrivate()
            && a.getTimeToLiveInSeconds() == b.getTimeToLiveInSeconds()
            && a.getCreationTimeInMs() == b.getCreationTimeInMs() && a.getAccountId() == b.getAccountId()
            && a.getContainerId() == b.getContainerId());
  }
}
