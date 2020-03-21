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
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MockId;
import com.github.ambry.store.MockIdFactory;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class MessageFormatSendTest {
  private final String putFormat;
  private static short messageFormatHeaderVersionSaved;

  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  @BeforeClass
  public static void saveMessageFormatHeaderVersionToUse() {
    messageFormatHeaderVersionSaved = MessageFormatRecord.headerVersionToUse;
  }

  @After
  public void resetMessageFormatHeaderVersionToUse() {
    MessageFormatRecord.headerVersionToUse = messageFormatHeaderVersionSaved;
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{PutMessageFormatInputStream.class.getSimpleName()},
        {PutMessageFormatBlobV1InputStream.class.getSimpleName()}});
  }

  public MessageFormatSendTest(String putFormat) {
    this.putFormat = putFormat;
  }

  class MockMessageReadSet implements MessageReadSet {

    ArrayList<ByteBuffer> buffers;
    ArrayList<StoreKey> keys;
    private long prefetchRelativeOffset;
    private long preFetchSize;

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
    public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {

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

    @Override
    public void doPrefetch(int index, long relativeOffset, long size) {
      this.prefetchRelativeOffset = relativeOffset;
      this.preFetchSize = size;
    }

    /**
     * Check if prefetched offset and size are correct.
     */
    boolean isPrefetchInfoCorrect(long relativeOffset, long size) {
      return (relativeOffset == this.prefetchRelativeOffset && size == this.preFetchSize);
    }
  }

  /**
   * Tests cases involving single messages across different header versions and with and without encryption keys
   */
  @Test
  public void sendWriteSingleMessageTest() throws Exception {
    if (putFormat.equals(PutMessageFormatInputStream.class.getSimpleName())) {
      ByteBuffer encryptionKey = ByteBuffer.wrap(TestUtils.getRandomBytes(256));
      MessageFormatRecord.headerVersionToUse = MessageFormatRecord.Message_Header_Version_V1;
      doSendWriteSingleMessageTest(null, null, false);
      doSendWriteSingleMessageTest(encryptionKey.duplicate(), null, false);
      // with store data prefetch
      doSendWriteSingleMessageTest(null, null, true);
      doSendWriteSingleMessageTest(encryptionKey.duplicate(), null, true);

      MessageFormatRecord.headerVersionToUse = MessageFormatRecord.Message_Header_Version_V2;
      doSendWriteSingleMessageTest(null, null, false);
      doSendWriteSingleMessageTest(ByteBuffer.allocate(0), ByteBuffer.allocate(0), false);
      doSendWriteSingleMessageTest(encryptionKey.duplicate(), encryptionKey.duplicate(), false);

      MessageFormatRecord.headerVersionToUse = MessageFormatRecord.Message_Header_Version_V3;
      doSendWriteSingleMessageTest(null, null, false);
      doSendWriteSingleMessageTest(ByteBuffer.allocate(0), ByteBuffer.allocate(0), false);
      doSendWriteSingleMessageTest(encryptionKey.duplicate(), encryptionKey.duplicate(), false);
      // with store data prefetch
      doSendWriteSingleMessageTest(null, null, true);
      doSendWriteSingleMessageTest(ByteBuffer.allocate(0), ByteBuffer.allocate(0), true);
      doSendWriteSingleMessageTest(encryptionKey.duplicate(), encryptionKey.duplicate(), true);
    } else {
      doSendWriteSingleMessageTest(null, null, false);
      doSendWriteSingleMessageTest(null, null, true);
    }
  }

  /**
   * Helper method for testing single message sends.
   * @param encryptionKey the encryption key to include in the message while writing it.
   * @param expectedEncryptionKey the key expected when reading the sent message.
   * @param enableDataPrefetch enable data prefetch.
   */
  private void doSendWriteSingleMessageTest(ByteBuffer encryptionKey, ByteBuffer expectedEncryptionKey,
      boolean enableDataPrefetch) throws Exception {
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
            encryptionKey != null, null);
    MessageFormatInputStream putStream;
    MessageFormatRecord.MessageHeader_Format header;
    if (putFormat.equals(PutMessageFormatInputStream.class.getSimpleName())) {
      header = getHeader(
          new PutMessageFormatInputStream(storeKey, encryptionKey == null ? null : encryptionKey.duplicate(),
              properties, ByteBuffer.wrap(userMetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)), blob.length,
              BlobType.DataBlob));
      putStream = new PutMessageFormatInputStream(storeKey, encryptionKey, properties, ByteBuffer.wrap(userMetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), blob.length, BlobType.DataBlob);
    } else {
      header = getHeader(new PutMessageFormatBlobV1InputStream(storeKey, properties, ByteBuffer.wrap(userMetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), blob.length, BlobType.DataBlob));
      putStream = new PutMessageFormatBlobV1InputStream(storeKey, properties, ByteBuffer.wrap(userMetadata),
          new ByteBufferInputStream(ByteBuffer.wrap(blob)), blob.length, BlobType.DataBlob);
    }

    ByteBuffer buf1 = ByteBuffer.allocate((int) putStream.getSize());

    putStream.read(buf1.array());
    ArrayList<ByteBuffer> listbuf = new ArrayList<ByteBuffer>();
    listbuf.add(buf1);
    ArrayList<StoreKey> storeKeys = new ArrayList<StoreKey>();
    storeKeys.add(storeKey);
    MockMessageReadSet readSet = new MockMessageReadSet(listbuf, storeKeys);

    MetricRegistry registry = new MetricRegistry();
    MessageFormatMetrics metrics = new MessageFormatMetrics(registry);
    // get all
    MessageFormatSend send =
        new MessageFormatSend(readSet, MessageFormatFlags.All, metrics, new MockIdFactory(), enableDataPrefetch);
    Assert.assertEquals(send.sizeInBytes(), putStream.getSize());
    Assert.assertEquals(1, send.getMessageMetadataList().size());
    Assert.assertEquals(null, send.getMessageMetadataList().get(0));
    ByteBuffer bufresult = ByteBuffer.allocate((int) putStream.getSize());
    WritableByteChannel channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }
    Assert.assertArrayEquals(buf1.array(), bufresult.array());
    if (enableDataPrefetch) {
      Assert.assertTrue(readSet.isPrefetchInfoCorrect(0, readSet.sizeInBytes(0)));
    }

    // get blob
    send = new MessageFormatSend(readSet, MessageFormatFlags.Blob, metrics, new MockIdFactory(), enableDataPrefetch);
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
    if (enableDataPrefetch) {
      Assert.assertTrue(readSet.isPrefetchInfoCorrect(header.getBlobRecordRelativeOffset(), blobRecordSize));
    }

    // get user metadata
    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobUserMetadata, metrics, new MockIdFactory(),
        enableDataPrefetch);
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
    if (enableDataPrefetch) {
      Assert.assertTrue(readSet.isPrefetchInfoCorrect(header.getUserMetadataRecordRelativeOffset(),
          header.getUserMetadataRecordSize()));
    }

    // get blob properties
    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobProperties, metrics, new MockIdFactory(),
        enableDataPrefetch);
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
    if (enableDataPrefetch) {
      Assert.assertTrue(readSet.isPrefetchInfoCorrect(header.getBlobPropertiesRecordRelativeOffset(),
          header.getBlobPropertiesRecordSize()));
    }

    // get blob info
    send =
        new MessageFormatSend(readSet, MessageFormatFlags.BlobInfo, metrics, new MockIdFactory(), enableDataPrefetch);
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
    if (enableDataPrefetch) {
      Assert.assertTrue(readSet.isPrefetchInfoCorrect(header.getBlobPropertiesRecordRelativeOffset(),
          header.getBlobPropertiesRecordSize() + header.getUserMetadataRecordSize()));
    }
  }

  /**
   * Tests involving multiple messages in a single Send involving different combinations of header format versions, put
   * formats and encryption keys.
   */
  @Test
  public void sendWriteCompositeMessagesTest() throws Exception {
    short savedVersion = MessageFormatRecord.headerVersionToUse;
    if (!putFormat.equals(PutMessageFormatInputStream.class.getSimpleName())) {
      return;
    }

    String putFormat1 = PutMessageFormatBlobV1InputStream.class.getSimpleName();
    String putFormat2 = PutMessageFormatInputStream.class.getSimpleName();
    short headerFormatV1 = MessageFormatRecord.Message_Header_Version_V1;
    short headerFormatV2 = MessageFormatRecord.Message_Header_Version_V2;
    short headerFormatV3 = MessageFormatRecord.Message_Header_Version_V3;

    byte[][] blob = {TestUtils.getRandomBytes(1000), TestUtils.getRandomBytes(2000), TestUtils.getRandomBytes(10000),
        TestUtils.getRandomBytes(20000), TestUtils.getRandomBytes(40000)};
    byte[][] userMetadata =
        {TestUtils.getRandomBytes(200), TestUtils.getRandomBytes(400), TestUtils.getRandomBytes(2000),
            TestUtils.getRandomBytes(4000), TestUtils.getRandomBytes(8000)};
    StoreKey[] storeKeys = {new MockId("64"), new MockId("32"), new MockId("16"), new MockId("08"), new MockId("04")};
    ByteBuffer[] encryptionKeys =
        {ByteBuffer.wrap(TestUtils.getRandomBytes(64)), ByteBuffer.wrap(TestUtils.getRandomBytes(128)),
            ByteBuffer.wrap(TestUtils.getRandomBytes(256)), ByteBuffer.wrap(TestUtils.getRandomBytes(512)),
            ByteBuffer.wrap(TestUtils.getRandomBytes(1024))};
    String putFormat1s[] = {putFormat1, putFormat1, putFormat1, putFormat1, putFormat1};
    String putFormat2s[] = {putFormat2, putFormat2, putFormat2, putFormat2, putFormat2};
    String putFormatComposite1[] = {putFormat1, putFormat2, putFormat2, putFormat2, putFormat1};
    String putFormatComposite2[] = {putFormat2, putFormat1, putFormat1, putFormat2, putFormat2};

    short headerFormat1s[] = {headerFormatV1, headerFormatV1, headerFormatV1, headerFormatV1, headerFormatV1};
    short headerFormat2s[] = {headerFormatV2, headerFormatV2, headerFormatV2, headerFormatV2, headerFormatV2};
    short headerFormat3s[] = {headerFormatV3, headerFormatV3, headerFormatV3, headerFormatV3, headerFormatV3};
    short headerFormatComposite1[] = {headerFormatV1, headerFormatV2, headerFormatV2, headerFormatV1, headerFormatV1};
    short headerFormatComposite2[] = {headerFormatV2, headerFormatV1, headerFormatV1, headerFormatV2, headerFormatV2};
    short headerFormatComposite3[] = {headerFormatV3, headerFormatV2, headerFormatV2, headerFormatV3, headerFormatV3};

    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormat1s, headerFormat1s);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormat2s, headerFormat1s);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormat2s, headerFormat2s);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormat2s, headerFormat3s);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormat2s,
        headerFormatComposite1);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormat2s,
        headerFormatComposite2);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormat2s,
        headerFormatComposite3);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormatComposite1,
        headerFormatComposite1);
    doSendWriteCompositeMessagesTest(blob, userMetadata, storeKeys, encryptionKeys, putFormatComposite2,
        headerFormatComposite2);

    MessageFormatRecord.headerVersionToUse = savedVersion;
  }

  /**
   * Helper method to test multiple messages in a single Send involving different combinations of header format
   * versions, put formats and encryption keys.
   * @param blob the array of blob records for the messages.
   * @param userMetadata the array of userMetadata for the messages.
   * @param storeKeys the array of store keys for the messages.
   * @param encryptionKeys the array of encryption keys for the messages.
   * @param putFormats the array of Put Format class names to use to create the message streams.
   * @param headerVersions the array of Message Header versions to use for the messages.
   */
  private void doSendWriteCompositeMessagesTest(byte[][] blob, byte[][] userMetadata, StoreKey[] storeKeys,
      ByteBuffer[] encryptionKeys, String[] putFormats, short[] headerVersions)
      throws MessageFormatException, IOException {
    String serviceIdPrefix = "serviceId";
    String ownerIdPrefix = "owner";
    String contentTypePrefix = "bin";
    short accountIdBase = 10;
    short containerIdBase = 2;
    BlobProperties[] properties = new BlobProperties[5];
    for (int i = 0; i < 5; i++) {
      properties[i] =
          new BlobProperties(blob[i].length, serviceIdPrefix + i, ownerIdPrefix + i, contentTypePrefix + i, false, 100,
              (short) (accountIdBase + i), (short) (containerIdBase + i), encryptionKeys[i] != null, null);
    }

    MessageFormatInputStream[] putStreams = new MessageFormatInputStream[5];
    for (int i = 0; i < 5; i++) {
      MessageFormatRecord.headerVersionToUse = headerVersions[i];
      if (putFormats[i].equals(PutMessageFormatInputStream.class.getSimpleName())) {
        putStreams[i] =
            new PutMessageFormatInputStream(storeKeys[i], (ByteBuffer) encryptionKeys[i].rewind(), properties[i],
                ByteBuffer.wrap(userMetadata[i]), new ByteBufferInputStream(ByteBuffer.wrap(blob[i])), blob[i].length,
                BlobType.DataBlob);
      } else {
        putStreams[i] =
            new PutMessageFormatBlobV1InputStream(storeKeys[i], properties[i], ByteBuffer.wrap(userMetadata[i]),
                new ByteBufferInputStream(ByteBuffer.wrap(blob[i])), blob[i].length, BlobType.DataBlob);
      }
    }

    int totalStreamSize = (int) Arrays.stream(putStreams).mapToLong(MessageFormatInputStream::getSize).sum();
    ByteBuffer compositeBuf = ByteBuffer.allocate(totalStreamSize);

    ArrayList<ByteBuffer> listbuf = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ByteBuffer buf = ByteBuffer.allocate((int) putStreams[i].getSize());
      putStreams[i].read(buf.array());
      compositeBuf.put(buf.array());
      listbuf.add(buf);
    }
    MessageReadSet readSet = new MockMessageReadSet(listbuf, new ArrayList<>(Arrays.asList(storeKeys)));

    MetricRegistry registry = new MetricRegistry();
    MessageFormatMetrics metrics = new MessageFormatMetrics(registry);

    // get all
    MessageFormatSend send =
        new MessageFormatSend(readSet, MessageFormatFlags.All, metrics, new MockIdFactory(), false);
    Assert.assertEquals(send.sizeInBytes(), totalStreamSize);
    Assert.assertEquals(5, send.getMessageMetadataList().size());
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(null, send.getMessageMetadataList().get(i));
    }
    ByteBuffer bufresult = ByteBuffer.allocate(totalStreamSize);
    WritableByteChannel channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }
    Assert.assertArrayEquals(compositeBuf.array(), bufresult.array());

    // get blob
    send = new MessageFormatSend(readSet, MessageFormatFlags.Blob, metrics, new MockIdFactory(), false);
    int blobRecordSizes[] = new int[5];
    for (int i = 0; i < 5; i++) {
      blobRecordSizes[i] = (int) (putFormats[i].equals(PutMessageFormatInputStream.class.getSimpleName())
          ? MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blob[i].length)
          : MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blob[i].length));
    }
    Assert.assertEquals(send.sizeInBytes(), (long) Arrays.stream(blobRecordSizes).sum());
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    int startOffset = 0;
    for (int i = 0; i < 5; i++) {
      DeserializedBlob deserializedBlob = MessageFormatRecord.deserializeAndGetBlobWithVersion(
          new ByteArrayInputStream(bufresult.array(), startOffset, blobRecordSizes[i]));
      Assert.assertEquals(
          putFormats[i].equals(PutMessageFormatInputStream.class.getSimpleName()) ? MessageFormatRecord.Blob_Version_V2
              : MessageFormatRecord.Blob_Version_V1, deserializedBlob.getVersion());
      Assert.assertEquals(BlobType.DataBlob, deserializedBlob.getBlobData().getBlobType());
      Assert.assertEquals(blob[i].length, deserializedBlob.getBlobData().getSize());
      byte[] readBlob = new byte[blob[i].length];
      deserializedBlob.getBlobData().content().readBytes(readBlob);
      Assert.assertArrayEquals(blob[i], readBlob);
      deserializedBlob.getBlobData().release();

      if (headerVersions[i] == MessageFormatRecord.Message_Header_Version_V1) {
        Assert.assertEquals(null, send.getMessageMetadataList().get(i));
      } else {
        Assert.assertEquals(encryptionKeys[i].rewind(), send.getMessageMetadataList().get(i).getEncryptionKey());
      }
      startOffset += blobRecordSizes[i];
    }

    // get user metadata
    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobUserMetadata, metrics, new MockIdFactory(), false);
    int userMetadataSizes[] = new int[5];
    for (int i = 0; i < 5; i++) {
      userMetadataSizes[i] =
          MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(userMetadata[i]));
    }
    Assert.assertEquals(send.sizeInBytes(), (long) Arrays.stream(userMetadataSizes).sum());
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    startOffset = 0;
    for (int i = 0; i < 5; i++) {
      DeserializedUserMetadata deserializedUserMetadata = MessageFormatRecord.deserializeAndGetUserMetadataWithVersion(
          new ByteArrayInputStream(bufresult.array(), startOffset, userMetadataSizes[i]));
      Assert.assertEquals(MessageFormatRecord.UserMetadata_Version_V1, deserializedUserMetadata.getVersion());
      verifyBlobUserMetadata(userMetadata[i], deserializedUserMetadata.getUserMetadata());

      if (headerVersions[i] == MessageFormatRecord.Message_Header_Version_V1) {
        Assert.assertEquals(null, send.getMessageMetadataList().get(i));
      } else {
        Assert.assertEquals(encryptionKeys[i].rewind(), send.getMessageMetadataList().get(i).getEncryptionKey());
      }

      startOffset += userMetadataSizes[i];
    }

    // get blob properties
    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobProperties, metrics, new MockIdFactory(), false);
    int blobPropertiesSizes[] = new int[5];
    for (int i = 0; i < 5; i++) {
      blobPropertiesSizes[i] = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(properties[i]);
    }
    Assert.assertEquals(send.sizeInBytes(), (long) Arrays.stream(blobPropertiesSizes).sum());
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    startOffset = 0;
    for (int i = 0; i < 5; i++) {
      DeserializedBlobProperties deserializedBlobProperties =
          MessageFormatRecord.deserializeAndGetBlobPropertiesWithVersion(
              new ByteArrayInputStream(bufresult.array(), startOffset, blobPropertiesSizes[i]));
      Assert.assertEquals(MessageFormatRecord.BlobProperties_Version_V1, deserializedBlobProperties.getVersion());
      verifyBlobProperties(properties[i], deserializedBlobProperties.getBlobProperties());

      Assert.assertEquals(null, send.getMessageMetadataList().get(i));

      startOffset += blobPropertiesSizes[i];
    }

    // get blob info

    send = new MessageFormatSend(readSet, MessageFormatFlags.BlobInfo, metrics, new MockIdFactory(), false);
    int blobInfoSizes[] = new int[5];
    for (int i = 0; i < 5; i++) {
      blobInfoSizes[i] = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(properties[i])
          + MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(userMetadata[i]));
    }
    Assert.assertEquals(send.sizeInBytes(), (long) Arrays.stream(blobInfoSizes).sum());
    bufresult.clear();
    channel = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel);
    }

    startOffset = 0;
    for (int i = 0; i < 5; i++) {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bufresult.array(), startOffset, blobInfoSizes[i]);
      DeserializedBlobProperties deserializedBlobProperties =
          MessageFormatRecord.deserializeAndGetBlobPropertiesWithVersion(inputStream);
      DeserializedUserMetadata deserializedUserMetadata =
          MessageFormatRecord.deserializeAndGetUserMetadataWithVersion(inputStream);

      Assert.assertEquals(MessageFormatRecord.BlobProperties_Version_V1, deserializedBlobProperties.getVersion());
      verifyBlobProperties(properties[i], deserializedBlobProperties.getBlobProperties());

      Assert.assertEquals(MessageFormatRecord.UserMetadata_Version_V1, deserializedUserMetadata.getVersion());
      verifyBlobUserMetadata(userMetadata[i], deserializedUserMetadata.getUserMetadata());

      if (headerVersions[i] == MessageFormatRecord.Message_Header_Version_V1) {
        Assert.assertEquals(null, send.getMessageMetadataList().get(i));
      } else {
        Assert.assertEquals(encryptionKeys[i].rewind(), send.getMessageMetadataList().get(i).getEncryptionKey());
      }

      startOffset += blobInfoSizes[i];
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
    MessageFormatSend send =
        new MessageFormatSend(readSet, MessageFormatFlags.All, metrics, new MockIdFactory(), false);
    Assert.assertEquals(send.sizeInBytes(), 1010);
    ByteBuffer bufresult = ByteBuffer.allocate(1010);
    WritableByteChannel channel1 = Channels.newChannel(new ByteBufferOutputStream(bufresult));
    while (!send.isSendComplete()) {
      send.writeTo(channel1);
    }
    Assert.assertArrayEquals(buf1.array(), bufresult.array());
    try {
      // get blob
      MessageFormatSend send1 =
          new MessageFormatSend(readSet, MessageFormatFlags.Blob, metrics, new MockIdFactory(), false);
      Assert.assertTrue(false);
    } catch (MessageFormatException e) {
      Assert.assertTrue(e.getErrorCode() == MessageFormatErrorCodes.Store_Key_Id_MisMatch);
    }
  }

  /**
   * Test {@link MessageReadSetIndexInputStream} with different offsets and lengths.
   */
  @Test
  public void messageReadSetIndexInputStreamTest() throws Exception {
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
    Assert.assertEquals("Number of bytes read doesn't match", 1024, stream1.read(buf1Output, 0, 1024));
    Assert.assertArrayEquals(buf1Output, buf1);
    MessageReadSetIndexInputStream stream2 = new MessageReadSetIndexInputStream(readSet, 1, 1024);
    byte[] buf2Output = new byte[1025];
    Assert.assertEquals("Number of bytes read doesn't match", 512, stream2.read(buf2Output, 0, 512));
    Assert.assertEquals("Number of bytes read doesn't match", 512, stream2.read(buf2Output, 512, 513));
    for (int i = 0; i < 1024; i++) {
      Assert.assertEquals(buf2Output[i], buf2[i + 1024]);
    }
    MessageReadSetIndexInputStream stream3 = new MessageReadSetIndexInputStream(readSet, 2, 2048);
    byte[] buf3Output = new byte[2048];
    for (int i = 0; i < 2048; i++) {
      Assert.assertEquals((byte) stream3.read(), buf3[i + 2048]);
    }
    Assert.assertEquals("Should return -1 if no more data available", -1, stream3.read(buf3Output, 0, 1));
    Assert.assertEquals("Should return -1 if no more data available", -1, stream3.read());
  }

  /**
   * Test Exceptions cases for {@link MessageReadSetIndexInputStream}
   * IndexOutOfBoundsException should be thrown if offset or length is invalid.
   * 0 is expected if length requested is 0.
   * -1 is expected if no more data available.
   */
  @Test
  public void messageReadSetIndexInputStreamTestException() throws Exception {
    ArrayList<ByteBuffer> listBuf = new ArrayList<ByteBuffer>();
    byte[] buf = new byte[1024];
    new Random().nextBytes(buf);
    listBuf.add(ByteBuffer.wrap(buf));
    ArrayList<StoreKey> storeKeys = new ArrayList<StoreKey>();
    storeKeys.add(new MockId("012345678910123223233456789012"));
    MessageReadSet readSet = new MockMessageReadSet(listBuf, storeKeys);
    MessageReadSetIndexInputStream stream = new MessageReadSetIndexInputStream(readSet, 0, 0);
    byte[] bufOutput = new byte[1024];
    Assert.assertEquals("Should return 0 if length requested is 0", 0, stream.read(bufOutput, 0, 0));
    Assert.assertEquals("Should return 0 if length requested is 0", 0, stream.read(bufOutput, 1, 0));
    try {
      stream.read(bufOutput, -1, 10);
      Assert.fail("IndexOutOfBoundsException is expected.");
    } catch (IndexOutOfBoundsException e) {
    }
    try {
      stream.read(bufOutput, 0, -1);
      Assert.fail("IndexOutOfBoundsException is expected.");
    } catch (IndexOutOfBoundsException e) {
    }
    try {
      stream.read(bufOutput, 1, 1024);
      Assert.fail("IndexOutOfBoundsException is expected.");
    } catch (IndexOutOfBoundsException e) {
    }
    stream.read(bufOutput, 0, 1024);
    Assert.assertArrayEquals("Output doesn't match", bufOutput, buf);
    Assert.assertEquals("Should return -1 if no more data", -1, stream.read());
    Assert.assertEquals("Should return -1 if no more data", -1, stream.read(bufOutput, 0, 1));
  }

  private void verifyBlobUserMetadata(byte[] userMetadata, ByteBuffer result) {
    for (int i = 0; i < userMetadata.length; i++) {
      Assert.assertEquals(userMetadata[i], result.get());
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

  /**
   * Get blob header info from input stream.
   */
  private MessageFormatRecord.MessageHeader_Format getHeader(MessageFormatInputStream putStream) throws Exception {
    DataInputStream dStream = new DataInputStream(putStream);
    short headerVersion = dStream.readShort();
    ByteBuffer headerBuf = ByteBuffer.allocate(MessageFormatRecord.getHeaderSizeForVersion(headerVersion));
    headerBuf.putShort(headerVersion);
    Assert.assertEquals("Did not read header correctly", headerBuf.capacity() - Short.BYTES,
        dStream.read(headerBuf.array(), Short.BYTES, headerBuf.capacity() - Short.BYTES));
    headerBuf.rewind();
    return MessageFormatRecord.getMessageHeader(headerVersion, headerBuf);
  }
}
