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
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockId;
import com.github.ambry.store.MockIdFactory;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.messageformat.MessageFormatRecord.*;
import static com.github.ambry.utils.TestUtils.*;


@RunWith(Parameterized.class)
public class MessageSievingInputStreamTest {
  private enum TransformerOptions {
    Validate, KeyConvert,
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{EnumSet.noneOf(TransformerOptions.class)}, {EnumSet.of(TransformerOptions.Validate)},
            {EnumSet.of(TransformerOptions.KeyConvert)}, {EnumSet.allOf(TransformerOptions.class)}});
  }

  private static short messageFormatHeaderVersionSaved;
  private final List<Transformer> transformers = new ArrayList<>();
  private final EnumSet<TransformerOptions> options;
  private final StoreKeyFactory storeKeyFactory;
  private final RandomKeyConverter randomKeyConverter;
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @BeforeClass
  public static void saveMessageFormatHeaderVersionToUse() {
    messageFormatHeaderVersionSaved = MessageFormatRecord.headerVersionToUse;
  }

  @After
  public void resetMessageFormatHeaderVersionToUse() {
    MessageFormatRecord.headerVersionToUse = messageFormatHeaderVersionSaved;
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  public MessageSievingInputStreamTest(EnumSet<TransformerOptions> options) throws Exception {
    this.options = options;
    this.storeKeyFactory = new MockIdFactory();
    this.randomKeyConverter = new RandomKeyConverter();
    if (options.contains(TransformerOptions.Validate)) {
      transformers.add(new ValidatingTransformer(storeKeyFactory, randomKeyConverter));
    }
    if (options.contains(TransformerOptions.KeyConvert)) {
      transformers.add(new ValidatingKeyConvertingTransformer(storeKeyFactory, randomKeyConverter));
    }
  }

  /**
   * If there are no messages in the Message info list, the returned stream should be empty.
   */
  @Test
  public void testEmptyMsgInfoList() throws Exception {
    MessageSievingInputStream sievedStream =
        new MessageSievingInputStream(null, Collections.emptyList(), Collections.emptyList(), new MetricRegistry());
    Assert.assertFalse(sievedStream.hasInvalidMessages());
    Assert.assertEquals(0, sievedStream.getSize());
    Assert.assertEquals(0, sievedStream.getValidMessageInfoList().size());
  }

  /**
   * Test the case where all messages are valid.
   */
  @Test
  public void testValidBlobsAgainstCorruption() throws Exception {
    testValidBlobs(Blob_Version_V1, BlobType.DataBlob, Message_Header_Version_V1);
    testValidBlobs(Blob_Version_V1, BlobType.DataBlob, Message_Header_Version_V2);
    testValidBlobs(Blob_Version_V2, BlobType.DataBlob, Message_Header_Version_V2);
    testValidBlobs(Blob_Version_V2, BlobType.MetadataBlob, Message_Header_Version_V2);
    testValidBlobs(Blob_Version_V2, BlobType.DataBlob, Message_Header_Version_V3);
    testValidBlobs(Blob_Version_V2, BlobType.MetadataBlob, Message_Header_Version_V3);
  }

  private void testValidBlobs(short blobVersion, BlobType blobType, short headerVersionToUse) throws Exception {
    // MessageSievingInputStream contains put records for 3 valid blobs
    // id1(put record for valid blob), id2(put record for valid blob) and id3(put record for valid blob)

    MessageFormatRecord.headerVersionToUse = headerVersionToUse;
    byte[] encryptionKey = new byte[100];
    RANDOM.nextBytes(encryptionKey);
    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    short accountId1 = Utils.getRandomShort(RANDOM);
    short containerId1 = Utils.getRandomShort(RANDOM);
    BlobProperties prop1 = new BlobProperties(10, "servid1", accountId1, containerId1, false);
    byte[] usermetadata1 = new byte[1000];
    RANDOM.nextBytes(usermetadata1);
    int blobContentSize = 2000;
    byte[] data1 = new byte[blobContentSize];
    RANDOM.nextBytes(data1);
    long blobSize;
    if (blobType == BlobType.DataBlob) {
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data1 = byteBufferBlob.array();
      blobContentSize = data1.length;
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    }

    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    // For Blob_Version_V2, encryption key is null.
    MessageFormatInputStream messageFormatStream1 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key1, null, prop1,
            ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                blobContentSize, blobType);

    MessageInfo msgInfo1 =
        new MessageInfo(key1, messageFormatStream1.getSize(), accountId1, containerId1, prop1.getCreationTimeInMs());

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    short accountId2 = Utils.getRandomShort(RANDOM);
    short containerId2 = Utils.getRandomShort(RANDOM);
    BlobProperties prop2 = new BlobProperties(10, "servid2", accountId2, containerId2, false);
    byte[] usermetadata2 = new byte[1000];
    RANDOM.nextBytes(usermetadata2);
    blobContentSize = 2000;
    byte[] data2 = new byte[blobContentSize];
    RANDOM.nextBytes(data2);
    if (blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data2 = byteBufferBlob.array();
      blobContentSize = data2.length;
    }
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    // For Blob_Version_V2, encryption key is non-null.
    MessageFormatInputStream messageFormatStream2 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key2, ByteBuffer.wrap(encryptionKey), prop2,
            ByteBuffer.wrap(usermetadata2), stream2, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2,
                blobContentSize, blobType);

    MessageInfo msgInfo2 =
        new MessageInfo(key2, messageFormatStream2.getSize(), accountId2, containerId2, prop2.getCreationTimeInMs());

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    short accountId3 = Utils.getRandomShort(RANDOM);
    short containerId3 = Utils.getRandomShort(RANDOM);
    BlobProperties prop3 = new BlobProperties(10, "servid3", accountId3, containerId3, false);
    byte[] usermetadata3 = new byte[1000];
    RANDOM.nextBytes(usermetadata3);
    blobContentSize = 2000;
    byte[] data3 = new byte[blobContentSize];
    RANDOM.nextBytes(data3);
    if (blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data3 = byteBufferBlob.array();
      blobContentSize = data3.length;
    }
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    // For Blob_Version_V2, encryption key is null.
    MessageFormatInputStream messageFormatStream3 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key3, null, prop3,
            ByteBuffer.wrap(usermetadata3), stream3, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3,
                blobContentSize, blobType);

    MessageInfo msgInfo3 =
        new MessageInfo(key3, messageFormatStream3.getSize(), accountId3, containerId3, prop3.getCreationTimeInMs());

    MessageInfo msgInfo4 = null;
    MessageFormatInputStream messageFormatStream4 = null;
    MessageInfo msgInfo5 = null;
    MessageFormatInputStream messageFormatStream5 = null;
    // create message stream for blob 4. Header version 2, with encryption key.
    StoreKey key4 = new MockId("id4");
    short accountId4 = Utils.getRandomShort(RANDOM);
    short containerId4 = Utils.getRandomShort(RANDOM);
    BlobProperties prop4 = new BlobProperties(10, "servid4", accountId4, containerId4, false);
    byte[] usermetadata4 = new byte[1000];
    RANDOM.nextBytes(usermetadata4);
    blobContentSize = 2000;
    byte[] data4 = new byte[blobContentSize];
    RANDOM.nextBytes(data4);
    if (blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data4 = byteBufferBlob.array();
      blobContentSize = data4.length;
    }

    if (blobVersion == Blob_Version_V2) {
      ByteBufferInputStream stream4 = new ByteBufferInputStream(ByteBuffer.wrap(data4));
      MessageFormatRecord.headerVersionToUse = Message_Header_Version_V2;
      // encryption key is non-null.
      messageFormatStream4 =
          new PutMessageFormatInputStream(key4, ByteBuffer.wrap(encryptionKey), prop4, ByteBuffer.wrap(usermetadata4),
              stream4, blobContentSize, blobType);

      msgInfo4 =
          new MessageInfo(key4, messageFormatStream4.getSize(), accountId4, containerId4, prop4.getCreationTimeInMs());
    }

    // create message stream for blob 5. Header version 2, without encryption key.
    StoreKey key5 = new MockId("id5");
    short accountId5 = Utils.getRandomShort(RANDOM);
    short containerId5 = Utils.getRandomShort(RANDOM);
    BlobProperties prop5 = new BlobProperties(10, "servid5", accountId5, containerId5, false);
    byte[] usermetadata5 = new byte[1000];
    RANDOM.nextBytes(usermetadata5);
    blobContentSize = 2000;
    byte[] data5 = new byte[blobContentSize];
    RANDOM.nextBytes(data5);
    if (blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data5 = byteBufferBlob.array();
      blobContentSize = data5.length;
    }
    if (blobVersion == Blob_Version_V2) {
      ByteBufferInputStream stream5 = new ByteBufferInputStream(ByteBuffer.wrap(data5));
      MessageFormatRecord.headerVersionToUse = Message_Header_Version_V2;
      // encryption key is null.
      messageFormatStream5 =
          new PutMessageFormatInputStream(key5, null, prop5, ByteBuffer.wrap(usermetadata5), stream5, blobContentSize,
              blobType);

      msgInfo5 =
          new MessageInfo(key5, messageFormatStream5.getSize(), accountId5, containerId5, prop5.getCreationTimeInMs());
    }

    //create input stream for all blob messages together
    byte[] totalMessageStreamContent =
        new byte[(int) (messageFormatStream1.getSize() + messageFormatStream2.getSize() + messageFormatStream3.getSize()
            + (blobVersion == Blob_Version_V2 ? messageFormatStream4.getSize() + messageFormatStream5.getSize() : 0))];
    messageFormatStream1.read(totalMessageStreamContent, 0, (int) messageFormatStream1.getSize());
    messageFormatStream2.read(totalMessageStreamContent, (int) messageFormatStream1.getSize(),
        (int) messageFormatStream2.getSize());
    messageFormatStream3.read(totalMessageStreamContent,
        (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
        (int) messageFormatStream3.getSize());

    if (blobVersion == Blob_Version_V2) {
      messageFormatStream4.read(totalMessageStreamContent,
          (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
              + (int) messageFormatStream3.getSize(), (int) messageFormatStream4.getSize());
      messageFormatStream5.read(totalMessageStreamContent,
          (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
              + (int) messageFormatStream3.getSize() + (int) messageFormatStream4.getSize(),
          (int) messageFormatStream5.getSize());
    }

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageStreamContent));

    List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);
    if (blobVersion == Blob_Version_V2) {
      msgInfoList.add(msgInfo4);
      msgInfoList.add(msgInfo5);
    }

    MessageSievingInputStream sievedStream =
        new MessageSievingInputStream(inputStream, msgInfoList, transformers, new MetricRegistry());

    Map<StoreKey, StoreKey> convertedMap = randomKeyConverter.convert(Arrays.asList(key1, key2, key3));
    Map<StoreKey, StoreKey> convertedMapExtra = randomKeyConverter.convert(Arrays.asList(key4, key5));
    int headerSizeV1 = MessageHeader_Format_V1.getHeaderSize();
    int headerSizeV2 = MessageHeader_Format_V2.getHeaderSize();
    int blobPropertiesRecordSize = BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize = UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));

    int totalHeadSize =
        3 * (headerVersionToUse == MessageFormatRecord.Message_Header_Version_V1 ? headerSizeV1 : headerSizeV2);
    int totalEncryptionRecordSize = 0;
    int totalBlobPropertiesSize = 3 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 3 * userMetadataSize;
    int totalBlobSize = 3 * (int) blobSize;
    int totalKeySize = (options.contains(TransformerOptions.KeyConvert) ? convertedMap.values() : convertedMap.keySet())
        .stream()
        .mapToInt(StoreKey::sizeInBytes)
        .sum();
    int encryptionRecordSize =
        BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(ByteBuffer.wrap(encryptionKey));

    if (blobVersion == Blob_Version_V2) {
      totalHeadSize += 2 * headerSizeV2;
      // stream 2 and stream 4 have encryption keys.
      totalEncryptionRecordSize += 2 * encryptionRecordSize;
      totalBlobPropertiesSize += 2 * blobPropertiesRecordSize;
      totalUserMetadataSize += 2 * userMetadataSize;
      totalBlobSize += 2 * (int) blobSize;
      totalKeySize += (options.contains(TransformerOptions.KeyConvert) ? convertedMapExtra.values()
          : convertedMapExtra.keySet()).stream().mapToInt(StoreKey::sizeInBytes).sum();
    }

    Assert.assertFalse(sievedStream.hasInvalidMessages());

    if (!options.isEmpty()) {
      Assert.assertEquals(options.isEmpty() ? totalMessageStreamContent.length
          : totalHeadSize + totalEncryptionRecordSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize
              + totalKeySize, sievedStream.getSize());

      Assert.assertEquals((int) sievedStream.getValidMessageInfoList().stream().mapToLong(MessageInfo::getSize).sum(),
          sievedStream.getSize());

      verifySievedTransformedMessage(sievedStream,
          options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key1) : key1, "servid1", accountId1,
          containerId1, null, usermetadata1, data1, blobVersion, blobType);

      verifySievedTransformedMessage(sievedStream,
          options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key2) : key2, "servid2", accountId2,
          containerId2, blobVersion == Blob_Version_V2 ? encryptionKey : null, usermetadata2, data2, blobVersion,
          blobType);

      verifySievedTransformedMessage(sievedStream,
          options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key3) : key3, "servid3", accountId3,
          containerId3, null, usermetadata3, data3, blobVersion, blobType);

      if (blobVersion == Blob_Version_V2) {
        verifySievedTransformedMessage(sievedStream,
            options.contains(TransformerOptions.KeyConvert) ? convertedMapExtra.get(key4) : key4, "servid4", accountId4,
            containerId4, encryptionKey, usermetadata4, data4, blobVersion, blobType);

        verifySievedTransformedMessage(sievedStream,
            options.contains(TransformerOptions.KeyConvert) ? convertedMapExtra.get(key5) : key5, "servid5", accountId5,
            containerId5, null, usermetadata5, data5, blobVersion, blobType);
      }
    } else {
      Assert.assertEquals(totalMessageStreamContent.length, sievedStream.getSize());
      byte[] sievedBytes = Utils.readBytesFromStream(sievedStream, sievedStream.getSize());
      Assert.assertArrayEquals(totalMessageStreamContent, sievedBytes);
    }
    Assert.assertEquals(-1, sievedStream.read());
  }

  /**
   * Test the case where there are corrupt messages, messages that are deleted and messages that are expired.
   */
  @Test
  public void testInValidDeletedAndExpiredBlobsAgainstCorruption() throws Exception {
    testInValidDeletedAndExpiredBlobs(Blob_Version_V1, BlobType.DataBlob,
        MessageFormatRecord.Message_Header_Version_V1);
    testInValidDeletedAndExpiredBlobs(Blob_Version_V2, BlobType.DataBlob,
        MessageFormatRecord.Message_Header_Version_V2);
    testInValidDeletedAndExpiredBlobs(Blob_Version_V2, BlobType.MetadataBlob,
        MessageFormatRecord.Message_Header_Version_V2);
    testInValidDeletedAndExpiredBlobs(Blob_Version_V2, BlobType.DataBlob,
        MessageFormatRecord.Message_Header_Version_V3);
    testInValidDeletedAndExpiredBlobs(Blob_Version_V2, BlobType.MetadataBlob,
        MessageFormatRecord.Message_Header_Version_V3);
  }

  private void testInValidDeletedAndExpiredBlobs(short blobVersion, BlobType blobType, short headerVersionToUse)
      throws Exception {
    MessageFormatRecord.headerVersionToUse = headerVersionToUse;

    // MessageSievingInputStream contains put records for 2 valid blobs, 1 corrupt blob, 1 deleted blob and 1 expired.
    // id1(put record for valid blob), id2(corrupt), id3(put record of deleted blob), id4(put record of expired blob)
    // and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    short accountId1 = Utils.getRandomShort(RANDOM);
    short containerId1 = Utils.getRandomShort(RANDOM);
    BlobProperties prop1 = new BlobProperties(10, "servid1", accountId1, containerId1, false);
    byte[] encryptionKey1 = new byte[100];
    RANDOM.nextBytes(encryptionKey1);
    byte[] usermetadata1 = new byte[1000];
    RANDOM.nextBytes(usermetadata1);
    int blobContentSize = 2000;
    byte[] data1 = new byte[blobContentSize];
    RANDOM.nextBytes(data1);
    long blobSize = -1;
    if (blobType == BlobType.DataBlob) {
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data1 = byteBufferBlob.array();
      blobContentSize = data1.length;
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    }
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key1, ByteBuffer.wrap(encryptionKey1), prop1,
            ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                blobContentSize, blobType);

    MessageInfo msgInfo1 =
        new MessageInfo(key1, messageFormatStream1.getSize(), accountId1, containerId1, prop1.getCreationTimeInMs());

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    short accountId2 = Utils.getRandomShort(RANDOM);
    short containerId2 = Utils.getRandomShort(RANDOM);
    BlobProperties prop2 = new BlobProperties(10, "servid2", accountId2, containerId2, false);
    byte[] encryptionKey2 = new byte[100];
    RANDOM.nextBytes(encryptionKey2);
    byte[] usermetadata2 = new byte[1000];
    RANDOM.nextBytes(usermetadata2);
    blobContentSize = 2000;
    byte[] data2 = new byte[blobContentSize];
    RANDOM.nextBytes(data2);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data2 = byteBufferBlob.array();
      blobContentSize = data2.length;
    }
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream2 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key2, ByteBuffer.wrap(encryptionKey2), prop2,
            ByteBuffer.wrap(usermetadata2), stream2, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2,
                blobContentSize, blobType);

    MessageInfo msgInfo2 =
        new MessageInfo(key2, messageFormatStream2.getSize(), accountId2, containerId2, prop2.getCreationTimeInMs());

    // corrupt the message stream but make sure this header version is still valid
    byte[] corruptMessageStream2 = new byte[(int) messageFormatStream2.getSize()];
    RANDOM.nextBytes(corruptMessageStream2);
    corruptMessageStream2[0] = (byte) 0;
    corruptMessageStream2[1] = (byte) headerVersionToUse;

    InputStream corruptStream2 = new ByteBufferInputStream(ByteBuffer.wrap(corruptMessageStream2));

    // create message stream for blob 3 that is deleted.
    StoreKey key3 = new MockId("id3");
    short accountId3 = Utils.getRandomShort(RANDOM);
    short containerId3 = Utils.getRandomShort(RANDOM);
    BlobProperties prop3 = new BlobProperties(10, "servid3", accountId3, containerId3, false);
    byte[] usermetadata3 = new byte[1000];
    RANDOM.nextBytes(usermetadata3);
    blobContentSize = 2000;
    byte[] data3 = new byte[blobContentSize];
    RANDOM.nextBytes(data3);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data3 = byteBufferBlob.array();
      blobContentSize = data3.length;
    }
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key3, null, prop3,
            ByteBuffer.wrap(usermetadata3), stream3, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3,
                blobContentSize, blobType);

    // MessageInfo marks this blob as deleted.
    MessageInfo msgInfo3 =
        new MessageInfo(key3, messageFormatStream3.getSize(), true, false, Utils.Infinite_Time, accountId3,
            containerId3, prop3.getCreationTimeInMs());

    // create message stream for blob 4 that is expired.
    StoreKey key4 = new MockId("id4");
    short accountId4 = Utils.getRandomShort(RANDOM);
    short containerId4 = Utils.getRandomShort(RANDOM);
    BlobProperties prop4 = new BlobProperties(10, "servid4", accountId4, containerId4, false);
    byte[] usermetadata4 = new byte[1000];
    RANDOM.nextBytes(usermetadata4);
    blobContentSize = 2000;
    byte[] data4 = new byte[blobContentSize];
    RANDOM.nextBytes(data4);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data4 = byteBufferBlob.array();
      blobContentSize = data4.length;
    }
    ByteBufferInputStream stream4 = new ByteBufferInputStream(ByteBuffer.wrap(data4));

    MessageFormatInputStream messageFormatStream4 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key4, null, prop4,
            ByteBuffer.wrap(usermetadata4), stream4, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key4, prop4, ByteBuffer.wrap(usermetadata4), stream4,
                blobContentSize, blobType);

    // MessageInfo marks this as already expired (the third field is an absolute expiresAt time).
    MessageInfo msgInfo4 =
        new MessageInfo(key4, messageFormatStream4.getSize(), 1, accountId4, containerId4, prop4.getCreationTimeInMs());

    // create message stream for blob 5
    StoreKey key5 = new MockId("id5");
    short accountId5 = Utils.getRandomShort(RANDOM);
    short containerId5 = Utils.getRandomShort(RANDOM);
    BlobProperties prop5 = new BlobProperties(10, "servid5", accountId5, containerId5, false);
    byte[] encryptionKey5 = new byte[100];
    RANDOM.nextBytes(encryptionKey5);
    byte[] usermetadata5 = new byte[1000];
    RANDOM.nextBytes(usermetadata5);
    blobContentSize = 2000;
    byte[] data5 = new byte[blobContentSize];
    RANDOM.nextBytes(data5);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data5 = byteBufferBlob.array();
      blobContentSize = data5.length;
    }
    ByteBufferInputStream stream5 = new ByteBufferInputStream(ByteBuffer.wrap(data5));

    MessageFormatInputStream messageFormatStream5 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key5, ByteBuffer.wrap(encryptionKey5), prop5,
            ByteBuffer.wrap(usermetadata5), stream5, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key5, prop5, ByteBuffer.wrap(usermetadata5), stream5,
                blobContentSize, blobType);

    MessageInfo msgInfo5 =
        new MessageInfo(key5, messageFormatStream5.getSize(), accountId5, containerId5, prop5.getCreationTimeInMs());

    // create message stream for blob 6
    StoreKey key6 = new MockId("id6");
    short accountId6 = Utils.getRandomShort(RANDOM);
    short containerId6 = Utils.getRandomShort(RANDOM);
    BlobProperties prop6 = new BlobProperties(10, "servid6", accountId6, containerId6, false);
    byte[] encryptionKey6 = new byte[100];
    RANDOM.nextBytes(encryptionKey6);
    byte[] usermetadata6 = new byte[1000];
    RANDOM.nextBytes(usermetadata6);
    blobContentSize = 2000;
    byte[] data6 = new byte[blobContentSize];
    RANDOM.nextBytes(data6);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data2 = byteBufferBlob.array();
      blobContentSize = data6.length;
    }
    ByteBufferInputStream stream6 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream6 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key6, ByteBuffer.wrap(encryptionKey6), prop6,
            ByteBuffer.wrap(usermetadata6), stream6, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key6, prop6, ByteBuffer.wrap(usermetadata6), stream6,
                blobContentSize, blobType);

    MessageInfo msgInfo6 =
        new MessageInfo(key6, messageFormatStream6.getSize(), accountId6, containerId6, prop6.getCreationTimeInMs());

    // corrupt the message stream but make sure this header version is not valid
    byte[] corruptMessageStream6 = new byte[(int) messageFormatStream6.getSize()];
    RANDOM.nextBytes(corruptMessageStream6);
    corruptMessageStream6[1] = (byte) 100;

    InputStream corruptStream6 = new ByteBufferInputStream(ByteBuffer.wrap(corruptMessageStream6));

    //create input stream for all blob messages together
    byte[] totalMessageStreamContent =
        new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
            + (int) messageFormatStream3.getSize() + (int) messageFormatStream4.getSize()
            + (int) messageFormatStream5.getSize() + (int) messageFormatStream6.getSize()];
    messageFormatStream1.read(totalMessageStreamContent, 0, (int) messageFormatStream1.getSize());
    corruptStream2.read(totalMessageStreamContent, (int) messageFormatStream1.getSize(),
        (int) messageFormatStream2.getSize());
    messageFormatStream3.read(totalMessageStreamContent,
        (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
        (int) messageFormatStream3.getSize());
    messageFormatStream4.read(totalMessageStreamContent,
        (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
            + (int) messageFormatStream3.getSize(), (int) messageFormatStream4.getSize());
    messageFormatStream5.read(totalMessageStreamContent,
        (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
            + (int) messageFormatStream3.getSize() + (int) messageFormatStream4.getSize(),
        (int) messageFormatStream5.getSize());
    corruptStream6.read(totalMessageStreamContent,
        (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
            + (int) messageFormatStream3.getSize() + (int) messageFormatStream4.getSize()
            + (int) messageFormatStream5.getSize(), (int) messageFormatStream6.getSize());

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageStreamContent));

    List<MessageInfo> msgInfoList = new ArrayList<>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);
    msgInfoList.add(msgInfo4);
    msgInfoList.add(msgInfo5);
    msgInfoList.add(msgInfo6);

    MessageSievingInputStream sievedStream =
        new MessageSievingInputStream(inputStream, msgInfoList, transformers, new MetricRegistry());

    Map<StoreKey, StoreKey> convertedMap =
        randomKeyConverter.convert(Arrays.asList(key1, key2, key3, key4, key5, key6));

    int headerSize = getHeaderSizeForVersion(headerVersionToUse);
    int blobPropertiesRecordSize = BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize = UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));

    int totalHeadSize = 2 * headerSize;
    int totalBlobPropertiesSize = 2 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 2 * userMetadataSize;
    int totalBlobSize = 2 * (int) blobSize;
    int totalKeySize =
        options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key1).sizeInBytes() + convertedMap.get(key5)
            .sizeInBytes() : key1.sizeInBytes() + key5.sizeInBytes();
    int totalEncryptionRecordSize = blobVersion > Blob_Version_V1 ?
        BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(ByteBuffer.wrap(encryptionKey1))
            + BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(ByteBuffer.wrap(encryptionKey5)) : 0;

    if (!options.isEmpty()) {
      Assert.assertTrue(sievedStream.hasInvalidMessages());
      Assert.assertEquals((int) sievedStream.getValidMessageInfoList().stream().mapToLong(MessageInfo::getSize).sum(),
          sievedStream.getSize());

      Assert.assertEquals(totalHeadSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize + totalKeySize
          + totalEncryptionRecordSize, sievedStream.getSize());

      verifySievedTransformedMessage(sievedStream,
          options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key1) : key1, "servid1", accountId1,
          containerId1, blobVersion > Blob_Version_V1 ? encryptionKey1 : null, usermetadata1, data1, blobVersion,
          blobType);

      verifySievedTransformedMessage(sievedStream,
          options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key5) : key5, "servid5", accountId5,
          containerId5, blobVersion > Blob_Version_V1 ? encryptionKey5 : null, usermetadata5, data5, blobVersion,
          blobType);
    } else {
      // even if there are no transformers, deleted and expired messages should be dropped by the MessageSievingInputStream.
      byte[] expectedBytes = new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
          + (int) messageFormatStream5.getSize() + (int) messageFormatStream6.getSize()];
      System.arraycopy(totalMessageStreamContent, 0, expectedBytes, 0,
          (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize());
      System.arraycopy(totalMessageStreamContent,
          totalMessageStreamContent.length - (int) messageFormatStream5.getSize()
              - (int) messageFormatStream6.getSize(), expectedBytes,
          (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
          (int) messageFormatStream5.getSize() + (int) messageFormatStream6.getSize());
      Assert.assertEquals(expectedBytes.length, sievedStream.getSize());
      byte[] sievedBytes = Utils.readBytesFromStream(sievedStream, sievedStream.getSize());
      Assert.assertArrayEquals(expectedBytes, sievedBytes);
    }

    Assert.assertEquals(-1, sievedStream.read());
  }

  /**
   * Test the case where there are delete records in messages.
   */
  @Test
  public void testDeletedRecordsAgainstCorruption() throws Exception {
    testDeletedRecords(Blob_Version_V1, BlobType.DataBlob);
    testDeletedRecords(Blob_Version_V2, BlobType.DataBlob);
    testDeletedRecords(Blob_Version_V2, BlobType.MetadataBlob);
  }

  private void testDeletedRecords(short blobVersion, BlobType blobType) throws Exception {
    // MessageSievingInputStream contains put records for 2 valid blobs and 1 delete record
    // id1(put record for valid blob), id2(delete record) and id3(put record for valid blob)
    ArrayList<Short> versions = new ArrayList<>();
    versions.add(Message_Header_Version_V1);
    if (blobVersion != Blob_Version_V1) {
      versions.add(Message_Header_Version_V2);
      versions.add(Message_Header_Version_V3);
    }

    try {
      for (short version : versions) {
        headerVersionToUse = version;
        // create message stream for blob 1
        StoreKey key1 = new MockId("id1");
        short accountId = Utils.getRandomShort(RANDOM);
        short containerId = Utils.getRandomShort(RANDOM);
        BlobProperties prop1 = new BlobProperties(10, "servid1", accountId, containerId, false);
        byte[] encryptionKey1 = new byte[100];
        RANDOM.nextBytes(encryptionKey1);
        byte[] usermetadata1 = new byte[1000];
        RANDOM.nextBytes(usermetadata1);
        int blobContentSize = 2000;
        byte[] data1 = new byte[blobContentSize];
        RANDOM.nextBytes(data1);
        if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
          ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
          data1 = byteBufferBlob.array();
          blobContentSize = data1.length;
        }
        ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

        MessageFormatInputStream messageFormatStream1 =
            (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key1, ByteBuffer.wrap(encryptionKey1),
                prop1, ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
                : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                    blobContentSize, blobType);
        MessageInfo msgInfo1 =
            new MessageInfo(key1, messageFormatStream1.getSize(), accountId, containerId, prop1.getCreationTimeInMs());

        // create message stream for blob 2 and mark it as deleted
        StoreKey key2 = new MockId("id2");
        accountId = Utils.getRandomShort(RANDOM);
        containerId = Utils.getRandomShort(RANDOM);
        long deletionTimeMs = SystemTime.getInstance().milliseconds() + RANDOM.nextInt();
        MessageFormatInputStream messageFormatStream2 =
            new DeleteMessageFormatInputStream(key2, accountId, containerId, deletionTimeMs);

        MessageInfo msgInfo2 =
            new MessageInfo(key2, messageFormatStream2.getSize(), accountId, containerId, deletionTimeMs);

        // create message stream for blob 3
        StoreKey key3 = new MockId("id3");
        accountId = Utils.getRandomShort(RANDOM);
        containerId = Utils.getRandomShort(RANDOM);
        BlobProperties prop3 = new BlobProperties(10, "servid3", accountId, containerId, false);
        byte[] encryptionKey3 = new byte[100];
        RANDOM.nextBytes(encryptionKey3);
        byte[] usermetadata3 = new byte[1000];
        RANDOM.nextBytes(usermetadata3);
        blobContentSize = 2000;
        byte[] data3 = new byte[blobContentSize];
        RANDOM.nextBytes(data3);
        if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
          ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
          data3 = byteBufferBlob.array();
          blobContentSize = data3.length;
        }
        ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

        MessageFormatInputStream messageFormatStream3 =
            (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key3, ByteBuffer.wrap(encryptionKey3),
                prop3, ByteBuffer.wrap(usermetadata3), stream3, blobContentSize, blobType)
                : new PutMessageFormatBlobV1InputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3,
                    blobContentSize, blobType);

        MessageInfo msgInfo3 =
            new MessageInfo(key3, messageFormatStream3.getSize(), accountId, containerId, prop3.getCreationTimeInMs());

        //create input stream for all blob messages together
        byte[] totalMessageContent =
            new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
                + (int) messageFormatStream3.getSize()];
        messageFormatStream1.read(totalMessageContent, 0, (int) messageFormatStream1.getSize());
        messageFormatStream2.read(totalMessageContent, (int) messageFormatStream1.getSize(),
            (int) messageFormatStream2.getSize());
        messageFormatStream3.read(totalMessageContent,
            (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
            (int) messageFormatStream3.getSize());

        InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageContent));

        List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
        msgInfoList.add(msgInfo1);
        msgInfoList.add(msgInfo2);
        msgInfoList.add(msgInfo3);

        new MessageSievingInputStream(inputStream, msgInfoList, transformers, new MetricRegistry());
        if (!options.isEmpty()) {
          Assert.fail("IOException should have been thrown due to delete record ");
        }
      }
    } catch (IOException e) {
      if (options.isEmpty()) {
        Assert.fail("No exceptions should have occurred");
      }
    }
    headerVersionToUse = Message_Header_Version_V1;
  }

  /**
   * Test the case where there are deprecated messages.
   * @throws Exception
   */
  @Test
  public void testDeprecatedMsgTransformation() throws Exception {
    testDeprecatedMsg(Blob_Version_V1, BlobType.DataBlob, MessageFormatRecord.Message_Header_Version_V1);
    testDeprecatedMsg(Blob_Version_V2, BlobType.DataBlob, MessageFormatRecord.Message_Header_Version_V2);
    testDeprecatedMsg(Blob_Version_V2, BlobType.MetadataBlob, MessageFormatRecord.Message_Header_Version_V2);
    testDeprecatedMsg(Blob_Version_V2, BlobType.DataBlob, MessageFormatRecord.Message_Header_Version_V3);
    testDeprecatedMsg(Blob_Version_V2, BlobType.MetadataBlob, MessageFormatRecord.Message_Header_Version_V3);
  }

  private void testDeprecatedMsg(short blobVersion, BlobType blobType, short headerVersionToUse) throws Exception {
    MessageFormatRecord.headerVersionToUse = headerVersionToUse;

    // MessageSievingInputStream contains put records for 2 valid blobs and 1 deprecated blob
    // id1(put record for valid blob), id2(deprecated) and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    short accountId1 = Utils.getRandomShort(RANDOM);
    short containerId1 = Utils.getRandomShort(RANDOM);
    BlobProperties prop1 = new BlobProperties(10, "servid1", accountId1, containerId1, false);
    byte[] encryptionKey1 = new byte[100];
    RANDOM.nextBytes(encryptionKey1);
    byte[] usermetadata1 = new byte[1000];
    RANDOM.nextBytes(usermetadata1);
    int blobContentSize = 2000;
    byte[] data1 = new byte[blobContentSize];
    RANDOM.nextBytes(data1);
    long blobSize = -1;
    if (blobType == BlobType.DataBlob) {
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data1 = byteBufferBlob.array();
      blobContentSize = data1.length;
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    }
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key1, ByteBuffer.wrap(encryptionKey1), prop1,
            ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                blobContentSize, blobType);

    MessageInfo msgInfo1 =
        new MessageInfo(key1, messageFormatStream1.getSize(), accountId1, containerId1, prop1.getCreationTimeInMs());

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    short accountId2 = Utils.getRandomShort(RANDOM);
    short containerId2 = Utils.getRandomShort(RANDOM);
    BlobProperties prop2 = new BlobProperties(10, "servid2", accountId2, containerId2, false);
    byte[] encryptionKey2 = new byte[100];
    RANDOM.nextBytes(encryptionKey2);
    byte[] usermetadata2 = new byte[1000];
    RANDOM.nextBytes(usermetadata2);
    blobContentSize = 2000;
    byte[] data2 = new byte[blobContentSize];
    RANDOM.nextBytes(data2);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data2 = byteBufferBlob.array();
      blobContentSize = data2.length;
    }
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream2 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key2, ByteBuffer.wrap(encryptionKey2), prop2,
            ByteBuffer.wrap(usermetadata2), stream2, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2,
                blobContentSize, blobType);

    MessageInfo msgInfo2 =
        new MessageInfo(key2, messageFormatStream2.getSize(), accountId2, containerId2, prop2.getCreationTimeInMs());

    // Add the key for the second message to the discardable ones.
    randomKeyConverter.addInvalids(Collections.singletonList(key2));

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    short accountId3 = Utils.getRandomShort(RANDOM);
    short containerId3 = Utils.getRandomShort(RANDOM);
    BlobProperties prop3 = new BlobProperties(10, "servid3", accountId3, containerId3, false);
    byte[] encryptionKey3 = new byte[100];
    RANDOM.nextBytes(encryptionKey3);
    byte[] usermetadata3 = new byte[1000];
    RANDOM.nextBytes(usermetadata3);
    blobContentSize = 2000;
    byte[] data3 = new byte[blobContentSize];
    RANDOM.nextBytes(data3);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data3 = byteBufferBlob.array();
      blobContentSize = data3.length;
    }
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key3, ByteBuffer.wrap(encryptionKey3), prop3,
            ByteBuffer.wrap(usermetadata3), stream3, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3,
                blobContentSize, blobType);

    MessageInfo msgInfo3 =
        new MessageInfo(key3, messageFormatStream3.getSize(), accountId3, containerId3, prop3.getCreationTimeInMs());

    //create input stream for all blob messages together
    byte[] totalMessageStreamContent =
        new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
            + (int) messageFormatStream3.getSize()];
    messageFormatStream1.read(totalMessageStreamContent, 0, (int) messageFormatStream1.getSize());
    messageFormatStream2.read(totalMessageStreamContent, (int) messageFormatStream1.getSize(),
        (int) messageFormatStream2.getSize());
    messageFormatStream3.read(totalMessageStreamContent,
        (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
        (int) messageFormatStream3.getSize());

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageStreamContent));

    List<MessageInfo> msgInfoList = new ArrayList<>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);

    MessageSievingInputStream sievedStream =
        new MessageSievingInputStream(inputStream, msgInfoList, transformers, new MetricRegistry());

    Map<StoreKey, StoreKey> convertedMap = randomKeyConverter.convert(Arrays.asList(key1, key2, key3));

    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(headerVersionToUse);
    int blobPropertiesRecordSize = BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize = UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));

    int totalHeadSize = (options.contains(TransformerOptions.KeyConvert) ? 2 : 3) * headerSize;
    int totalBlobPropertiesSize = (options.contains(TransformerOptions.KeyConvert) ? 2 : 3) * blobPropertiesRecordSize;
    int totalUserMetadataSize = (options.contains(TransformerOptions.KeyConvert) ? 2 : 3) * userMetadataSize;
    int totalBlobSize = (options.contains(TransformerOptions.KeyConvert) ? 2 : 3) * (int) blobSize;
    int totalKeySize =
        options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key1).sizeInBytes() + convertedMap.get(key3)
            .sizeInBytes() : key1.sizeInBytes() + key2.sizeInBytes() + key3.sizeInBytes();
    int totalEncryptionRecordSize = blobVersion > Blob_Version_V1 ?
        BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(ByteBuffer.wrap(encryptionKey1)) + (
            options.contains(TransformerOptions.KeyConvert) ? 0
                : BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(ByteBuffer.wrap(encryptionKey2)))
            + BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(ByteBuffer.wrap(encryptionKey3)) : 0;

    if (!options.isEmpty()) {
      if (options.contains(TransformerOptions.KeyConvert)) {
        Assert.assertTrue(sievedStream.hasDeprecatedMessages());
      }
      Assert.assertEquals((int) sievedStream.getValidMessageInfoList().stream().mapToLong(MessageInfo::getSize).sum(),
          sievedStream.getSize());

      Assert.assertEquals(options.isEmpty() ? totalMessageStreamContent.length
          : totalHeadSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize + totalKeySize
              + totalEncryptionRecordSize, sievedStream.getSize());

      verifySievedTransformedMessage(sievedStream,
          options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key1) : key1, "servid1", accountId1,
          containerId1, blobVersion > Blob_Version_V1 ? encryptionKey1 : null, usermetadata1, data1, blobVersion,
          blobType);

      if (!options.contains(TransformerOptions.KeyConvert)) {
        verifySievedTransformedMessage(sievedStream, key2, "servid2", accountId2, containerId2,
            blobVersion > Blob_Version_V1 ? encryptionKey2 : null, usermetadata2, data2, blobVersion, blobType);
      }

      verifySievedTransformedMessage(sievedStream,
          options.contains(TransformerOptions.KeyConvert) ? convertedMap.get(key3) : key3, "servid3", accountId3,
          containerId3, blobVersion > Blob_Version_V1 ? encryptionKey3 : null, usermetadata3, data3, blobVersion,
          blobType);
    } else {
      Assert.assertEquals(totalMessageStreamContent.length, sievedStream.getSize());
      byte[] sievedBytes = Utils.readBytesFromStream(sievedStream, sievedStream.getSize());
      Assert.assertArrayEquals(totalMessageStreamContent, sievedBytes);
    }
    Assert.assertEquals(-1, sievedStream.read());
  }

  private void verifySievedTransformedMessage(MessageSievingInputStream sievedStream, StoreKey key, String serviceId,
      short accountId, short containerId, byte[] encryptionKey, byte[] usermetadata, byte[] data, short blobVersion,
      BlobType blobType) throws Exception {

    byte[] headerVersion = new byte[Version_Field_Size_In_Bytes];
    sievedStream.read(headerVersion, 0, Version_Field_Size_In_Bytes);
    short version = ByteBuffer.wrap(headerVersion).getShort();
    if (!isValidHeaderVersion(version)) {
      throw new MessageFormatException("Header version not supported " + version, MessageFormatErrorCodes.Data_Corrupt);
    }
    int headerSize = getHeaderSizeForVersion(version);
    byte[] headerArr = new byte[headerSize];
    ByteBuffer headerBuffer = ByteBuffer.wrap(headerArr, 0, headerSize);
    headerBuffer.putShort(version);
    sievedStream.read(headerArr, Version_Field_Size_In_Bytes, headerSize - Version_Field_Size_In_Bytes);
    MessageHeader_Format header = getMessageHeader(version, headerBuffer);
    byte[] keyInStreamBytes = ((new MockIdFactory()).getStoreKey(new DataInputStream(sievedStream))).toBytes();
    Assert.assertArrayEquals(key.toBytes(), keyInStreamBytes);
    Assert.assertTrue(header.isPutRecord());
    ByteBuffer encryptionKeyInStream =
        header.hasEncryptionKeyRecord() ? deserializeBlobEncryptionKey(sievedStream) : null;
    BlobProperties propsFromStream = deserializeBlobProperties(sievedStream);
    ByteBuffer userMetadataFromStream = deserializeUserMetadata(sievedStream);
    BlobData blobDataFromStream = deserializeBlob(sievedStream);
    Assert.assertEquals(encryptionKey == null, encryptionKeyInStream == null);
    if (encryptionKey != null) {
      Assert.assertArrayEquals(encryptionKey, encryptionKeyInStream.array());
    }
    Assert.assertEquals(serviceId, propsFromStream.getServiceId());
    Assert.assertEquals(accountId, propsFromStream.getAccountId());
    Assert.assertEquals(containerId, propsFromStream.getContainerId());
    Assert.assertEquals(ByteBuffer.wrap(usermetadata), userMetadataFromStream);
    Assert.assertEquals(blobType, blobDataFromStream.getBlobType());
    ByteBuf byteBuf = blobDataFromStream.content();
    try {
      Assert.assertEquals(Unpooled.wrappedBuffer(data), byteBuf);
    } finally {
      byteBuf.release();
    }
  }
}

/**
 * An implementation of the {@link StoreKeyConverter} interface that generates random key mappings for the given set of
 * keys. The mappings are deterministic (a key will always be mapped to the same converted keys). The converted keys can
 * be of length less than, equal to, or greater than the size of the input key. Additionally, a set of "invalid" keys
 * can be provided while instantiating, and for these keys a null mapping will be provided.
 */
class RandomKeyConverter implements StoreKeyConverter {

  Collection<? extends StoreKey> invalids = Collections.emptyList();
  Map<StoreKey, StoreKey> onceConverted = new HashMap<>();

  /**
   * Add invalid mappings.
   * @param invalids the keys for which no mapping will be generated during conversion.
   */
  public void addInvalids(Collection<? extends StoreKey> invalids) {
    this.invalids = invalids;
  }

  @Override
  public Map<StoreKey, StoreKey> convert(Collection<? extends StoreKey> input) {
    Map<StoreKey, StoreKey> output = new HashMap<>();
    input.forEach(inKey -> {
      if (onceConverted.containsKey(inKey)) {
        output.put(inKey, onceConverted.get(inKey));
      } else {
        StoreKey replaceMent = invalids.contains(inKey) ? null : new MockId(
            inKey.getID().substring(0, inKey.getID().length() / 2) + Integer.toString(RANDOM.nextInt(1000)));
        onceConverted.put(inKey, replaceMent);
        output.put(inKey, replaceMent);
      }
    });
    return output;
  }

  @Override
  public StoreKey getConverted(StoreKey storeKey) {
    return onceConverted.get(storeKey);
  }

  @Override
  public void dropCache() {
    onceConverted = null;
  }
}

/**
 * A {@link Transformer} implementation that does random key conversions, used for testing.
 */
class ValidatingKeyConvertingTransformer implements Transformer {
  private final StoreKeyFactory storeKeyFactory;
  private final StoreKeyConverter storeKeyConverter;

  ValidatingKeyConvertingTransformer(StoreKeyFactory storeKeyFactory, StoreKeyConverter storeKeyConverter) {
    this.storeKeyFactory = storeKeyFactory;
    this.storeKeyConverter = storeKeyConverter;
  }

  @Override
  public TransformationOutput transform(Message message) {
    ByteBuffer encryptionKey;
    BlobProperties props;
    ByteBuffer metadata;
    BlobData blobData;
    MessageInfo msgInfo = message.getMessageInfo();
    InputStream msgStream = message.getStream();
    TransformationOutput transformationOutput;
    try {
      // Read header
      ByteBuffer headerVersion = ByteBuffer.allocate(Version_Field_Size_In_Bytes);
      msgStream.read(headerVersion.array());
      short version = headerVersion.getShort();
      if (!isValidHeaderVersion(version)) {
        throw new MessageFormatException("Header version not supported " + version,
            MessageFormatErrorCodes.Data_Corrupt);
      }
      int headerSize = getHeaderSizeForVersion(version);
      ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize);
      headerBuffer.put(headerVersion.array());
      msgStream.read(headerBuffer.array(), Version_Field_Size_In_Bytes, headerSize - Version_Field_Size_In_Bytes);
      headerBuffer.rewind();
      MessageHeader_Format header = getMessageHeader(version, headerBuffer);
      header.verifyHeader();
      StoreKey originalKey = storeKeyFactory.getStoreKey(new DataInputStream(msgStream));
      if (header.isPutRecord()) {
        encryptionKey = header.hasEncryptionKeyRecord() ? deserializeBlobEncryptionKey(msgStream) : null;
        props = deserializeBlobProperties(msgStream);
        metadata = deserializeUserMetadata(msgStream);
        blobData = deserializeBlob(msgStream);
      } else {
        throw new IllegalArgumentException("Message cannot be a deleted record ");
      }
      if (msgInfo.getStoreKey().equals(originalKey)) {
        StoreKey newKey = storeKeyConverter.convert(Collections.singletonList(originalKey)).get(originalKey);
        if (newKey == null) {
          System.out.println("No mapping for the given key, transformed message will be null");
          transformationOutput = new TransformationOutput((Message) null);
        } else {
          MessageInfo transformedMsgInfo;
          PutMessageFormatInputStream transformedStream =
              new PutMessageFormatInputStream(newKey, encryptionKey, props, metadata,
                  new ByteBufInputStream(blobData.content(), true), blobData.getSize(), blobData.getBlobType(),
                  msgInfo.getLifeVersion());
          transformedMsgInfo =
              new MessageInfo(newKey, transformedStream.getSize(), msgInfo.isDeleted(), msgInfo.isTtlUpdated(), false,
                  msgInfo.getExpirationTimeInMs(), msgInfo.getCrc(), msgInfo.getAccountId(), msgInfo.getContainerId(),
                  msgInfo.getOperationTimeMs(), msgInfo.getLifeVersion());
          transformationOutput = new TransformationOutput(new Message(transformedMsgInfo, transformedStream));
        }
      } else {
        throw new IllegalStateException(
            "StoreKey in log " + originalKey + " failed to match store key from Index " + msgInfo.getStoreKey());
      }
    } catch (Exception e) {
      transformationOutput = new TransformationOutput(e);
    }
    return transformationOutput;
  }

  @Override
  public void warmup(List<MessageInfo> messageInfos) throws Exception {
    //no-op
  }
}
