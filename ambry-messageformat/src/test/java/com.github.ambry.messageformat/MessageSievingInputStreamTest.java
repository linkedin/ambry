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
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


public class MessageSievingInputStreamTest {
  private static short messageFormatHeaderVersionSaved;

  @BeforeClass
  public static void saveMessageFormatHeaderVersionToUse() {
    messageFormatHeaderVersionSaved = MessageFormatRecord.headerVersionToUse;
  }

  @After
  public void resetMessageFormatHeaderVersionToUse() {
    MessageFormatRecord.headerVersionToUse = messageFormatHeaderVersionSaved;
  }

  @Test
  public void testValidBlobsAgainstCorruption() throws IOException, MessageFormatException {
    testValidBlobs(Blob_Version_V1, BlobType.DataBlob);
    testValidBlobs(Blob_Version_V2, BlobType.DataBlob);
    testValidBlobs(Blob_Version_V2, BlobType.MetadataBlob);
  }

  public void testValidBlobs(short blobVersion, BlobType blobType) throws IOException, MessageFormatException {

    // MessageSievingInputStream contains put records for 3 valid blobs
    // id1(put record for valid blob), id2(put record for valid blob) and id3(put record for valid blob)

    headerVersionToUse = Message_Header_Version_V1;
    byte[] encryptionKey = new byte[100];
    TestUtils.RANDOM.nextBytes(encryptionKey);
    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    short accountId1 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId1 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop1 = new BlobProperties(10, "servid1", accountId1, containerId1, false);
    byte[] usermetadata1 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata1);
    int blobContentSize = 2000;
    byte[] data1 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data1);
    long blobSize = -1;
    if (blobVersion == Blob_Version_V1) {
      blobSize = Blob_Format_V1.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == Blob_Version_V2 && blobType == BlobType.DataBlob) {
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data1 = byteBufferBlob.array();
      blobContentSize = data1.length;
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    }

    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key1, ByteBuffer.wrap(encryptionKey), prop1,
            ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                blobContentSize, blobType);

    MessageInfo msgInfo1 =
        new MessageInfo(key1, messageFormatStream1.getSize(), accountId1, containerId1, prop1.getCreationTimeInMs());

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    short accountId2 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId2 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop2 = new BlobProperties(10, "servid2", accountId2, containerId2, false);
    byte[] usermetadata2 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata2);
    blobContentSize = 2000;
    byte[] data2 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data2);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data2 = byteBufferBlob.array();
      blobContentSize = data2.length;
    }
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream2 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key2, ByteBuffer.wrap(encryptionKey), prop2,
            ByteBuffer.wrap(usermetadata2), stream2, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2,
                blobContentSize, blobType);

    MessageInfo msgInfo2 =
        new MessageInfo(key2, messageFormatStream2.getSize(), accountId2, containerId2, prop2.getCreationTimeInMs());

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    short accountId3 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId3 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop3 = new BlobProperties(10, "servid3", accountId3, containerId3, false);
    byte[] usermetadata3 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata3);
    blobContentSize = 2000;
    byte[] data3 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data3);
    if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data3 = byteBufferBlob.array();
      blobContentSize = data3.length;
    }
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        (blobVersion == Blob_Version_V2) ? new PutMessageFormatInputStream(key3, ByteBuffer.wrap(encryptionKey), prop3,
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
    short accountId4 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId4 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop4 = new BlobProperties(10, "servid4", accountId4, containerId4, false);
    byte[] usermetadata4 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata4);
    blobContentSize = 2000;
    byte[] data4 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data4);
    if (blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data4 = byteBufferBlob.array();
      blobContentSize = data4.length;
    }

    if (blobVersion == Blob_Version_V2) {
      ByteBufferInputStream stream4 = new ByteBufferInputStream(ByteBuffer.wrap(data4));
      headerVersionToUse = Message_Header_Version_V2;
      messageFormatStream4 =
          new PutMessageFormatInputStream(key4, ByteBuffer.wrap(encryptionKey), prop4, ByteBuffer.wrap(usermetadata4),
              stream4, blobContentSize, blobType);

      msgInfo4 =
          new MessageInfo(key4, messageFormatStream4.getSize(), accountId4, containerId4, prop4.getCreationTimeInMs());
    }

    // create message stream for blob 5. Header version 2, without encryption key.
    StoreKey key5 = new MockId("id5");
    short accountId5 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId5 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop5 = new BlobProperties(10, "servid5", accountId5, containerId5, false);
    byte[] usermetadata5 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata5);
    blobContentSize = 2000;
    byte[] data5 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data5);
    if (blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data5 = byteBufferBlob.array();
      blobContentSize = data5.length;
    }
    if (blobVersion == Blob_Version_V2) {
      ByteBufferInputStream stream5 = new ByteBufferInputStream(ByteBuffer.wrap(data5));
      headerVersionToUse = Message_Header_Version_V2;
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

    MessageSievingInputStream validMessageDetectionInputStream =
        new MessageSievingInputStream(inputStream, msgInfoList, new MockIdFactory(), new MetricRegistry());

    int headerSizeV1 = MessageHeader_Format_V1.getHeaderSize();
    int headerSizeV2 = MessageHeader_Format_V2.getHeaderSize();
    int blobPropertiesRecordSize = BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize = UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));

    int totalHeadSize = 3 * headerSizeV1;
    int totalEncryptionRecordSize = 0;
    int totalBlobPropertiesSize = 3 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 3 * userMetadataSize;
    int totalBlobSize = 3 * (int) blobSize;
    int totalKeySize = key1.sizeInBytes() + key2.sizeInBytes() + key3.sizeInBytes();
    int encryptionRecordSize =
        BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(ByteBuffer.wrap(encryptionKey));

    if (blobVersion == Blob_Version_V2) {
      totalHeadSize += 2 * headerSizeV2;
      totalEncryptionRecordSize += encryptionRecordSize;
      totalBlobPropertiesSize += 2 * blobPropertiesRecordSize;
      totalUserMetadataSize += 2 * userMetadataSize;
      totalBlobSize += 2 * (int) blobSize;
      totalKeySize += key4.sizeInBytes() + key5.sizeInBytes();
    }

    Assert.assertEquals(validMessageDetectionInputStream.getSize(),
        totalHeadSize + totalEncryptionRecordSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize
            + totalKeySize);

    Assert.assertEquals(validMessageDetectionInputStream.getSize(),
        messageFormatStream1.getSize() + messageFormatStream2.getSize() + messageFormatStream3.getSize() + (
            blobVersion == Blob_Version_V2 ? messageFormatStream4.getSize() + messageFormatStream5.getSize() : 0));

    Assert.assertEquals(true, verifyBlob(validMessageDetectionInputStream, Message_Header_Version_V1, headerSizeV1, 0,
        blobPropertiesRecordSize, userMetadataSize, (int) blobSize, key1, 10, "servid1", accountId1, containerId1, null,
        usermetadata1, data1, blobVersion, blobType));

    Assert.assertEquals(true, verifyBlob(validMessageDetectionInputStream, Message_Header_Version_V1, headerSizeV1, 0,
        blobPropertiesRecordSize, userMetadataSize, (int) blobSize, key2, 10, "servid2", accountId2, containerId2, null,
        usermetadata2, data2, blobVersion, blobType));

    Assert.assertEquals(true, verifyBlob(validMessageDetectionInputStream, Message_Header_Version_V1, headerSizeV1, 0,
        blobPropertiesRecordSize, userMetadataSize, (int) blobSize, key3, 10, "servid3", accountId3, containerId3, null,
        usermetadata3, data3, blobVersion, blobType));

    if (blobVersion == Blob_Version_V2) {
      Assert.assertEquals(true,
          verifyBlob(validMessageDetectionInputStream, Message_Header_Version_V2, headerSizeV2, encryptionRecordSize,
              blobPropertiesRecordSize, userMetadataSize, (int) blobSize, key4, 10, "servid4", accountId4, containerId4,
              encryptionKey, usermetadata4, data4, blobVersion, blobType));

      Assert.assertEquals(true, verifyBlob(validMessageDetectionInputStream, Message_Header_Version_V2, headerSizeV2, 0,
          blobPropertiesRecordSize, userMetadataSize, (int) blobSize, key5, 10, "servid5", accountId5, containerId5,
          null, usermetadata5, data5, blobVersion, blobType));
    }
  }

  @Test
  public void testInValidBlobsAgainstCorruption() throws IOException, MessageFormatException {
    testInValidBlobs(Blob_Version_V1, BlobType.DataBlob);
    testInValidBlobs(Blob_Version_V2, BlobType.DataBlob);
    testInValidBlobs(Blob_Version_V2, BlobType.MetadataBlob);
  }

  private void testInValidBlobs(short blobVersion, BlobType blobType) throws IOException, MessageFormatException {
    headerVersionToUse = Message_Header_Version_V1;

    // MessageSievingInputStream contains put records for 2 valid blobs and 1 corrupt blob
    // id1(put record for valid blob), id2(corrupt) and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    short accountId1 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId1 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop1 = new BlobProperties(10, "servid1", accountId1, containerId1, false);
    byte[] encryptionKey1 = new byte[100];
    TestUtils.RANDOM.nextBytes(encryptionKey1);
    byte[] usermetadata1 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata1);
    int blobContentSize = 2000;
    byte[] data1 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data1);
    long blobSize = -1;
    if (blobVersion == Blob_Version_V1) {
      blobSize = Blob_Format_V1.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == Blob_Version_V2 && blobType == BlobType.DataBlob) {
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data1 = byteBufferBlob.array();
      blobContentSize = data1.length;
      blobSize = (int) Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else {
      Assert.fail("Illegal blob version " + blobVersion + " and type " + blobType);
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
    short accountId2 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId2 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop2 = new BlobProperties(10, "servid2", accountId2, containerId2, false);
    byte[] encryptionKey2 = new byte[100];
    TestUtils.RANDOM.nextBytes(encryptionKey2);
    byte[] usermetadata2 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata2);
    blobContentSize = 2000;
    byte[] data2 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data2);
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

    // corrupt the message stream
    byte[] corruptMessageStream = new byte[(int) messageFormatStream2.getSize()];
    TestUtils.RANDOM.nextBytes(corruptMessageStream);

    InputStream corruptStream = new ByteBufferInputStream(ByteBuffer.wrap(corruptMessageStream));

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    short accountId3 = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId3 = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties prop3 = new BlobProperties(10, "servid3", accountId3, containerId3, false);
    byte[] encryptionKey3 = new byte[100];
    TestUtils.RANDOM.nextBytes(encryptionKey3);
    byte[] usermetadata3 = new byte[1000];
    TestUtils.RANDOM.nextBytes(usermetadata3);
    blobContentSize = 2000;
    byte[] data3 = new byte[blobContentSize];
    TestUtils.RANDOM.nextBytes(data3);
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
    corruptStream.read(totalMessageStreamContent, (int) messageFormatStream1.getSize(),
        (int) messageFormatStream2.getSize());
    messageFormatStream3.read(totalMessageStreamContent,
        (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
        (int) messageFormatStream3.getSize());

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageStreamContent));

    List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);

    MessageSievingInputStream validMessageDetectionInputStream =
        new MessageSievingInputStream(inputStream, msgInfoList, new MockIdFactory(), new MetricRegistry());

    int headerSize = MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize = BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize = UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));

    int totalHeadSize = 2 * headerSize;
    int totalBlobPropertiesSize = 2 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 2 * userMetadataSize;
    int totalBlobSize = 2 * (int) blobSize;
    int totalKeySize = key1.sizeInBytes() + key3.sizeInBytes();

    Assert.assertEquals(validMessageDetectionInputStream.getSize(),
        totalHeadSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize + totalKeySize);

    Assert.assertEquals(validMessageDetectionInputStream.getSize(),
        messageFormatStream1.getSize() + messageFormatStream3.getSize());

    Assert.assertEquals(true,
        verifyBlob(validMessageDetectionInputStream, Message_Header_Version_V1, headerSize, 0, blobPropertiesRecordSize,
            userMetadataSize, (int) blobSize, key1, 10, "servid1", accountId1, containerId1, null, usermetadata1, data1,
            blobVersion, blobType));

    Assert.assertEquals(true,
        verifyBlob(validMessageDetectionInputStream, Message_Header_Version_V1, headerSize, 0, blobPropertiesRecordSize,
            userMetadataSize, (int) blobSize, key3, 10, "servid3", accountId3, containerId3, null, usermetadata3, data3,
            blobVersion, blobType));
  }

  @Test
  public void testDeletedBlobsAgainstCorruption() throws IOException, MessageFormatException {
    testDeletedBlobs(Blob_Version_V1, BlobType.DataBlob);
    testDeletedBlobs(Blob_Version_V2, BlobType.DataBlob);
    testDeletedBlobs(Blob_Version_V2, BlobType.MetadataBlob);
  }

  private void testDeletedBlobs(short blobVersion, BlobType blobType) throws IOException, MessageFormatException {
    // MessageSievingInputStream contains put records for 2 valid blobs and 1 deleted blob
    // id1(put record for valid blob), id2(delete record) and id3(put record for valid blob)
    ArrayList<Short> versions = new ArrayList<>();
    versions.add(Message_Header_Version_V1);
    if (blobVersion != Blob_Version_V1) {
      versions.add(Message_Header_Version_V2);
    }

    try {
      for (short version : versions) {
        headerVersionToUse = version;
        // create message stream for blob 1
        StoreKey key1 = new MockId("id1");
        short accountId = Utils.getRandomShort(TestUtils.RANDOM);
        short containerId = Utils.getRandomShort(TestUtils.RANDOM);
        BlobProperties prop1 = new BlobProperties(10, "servid1", accountId, containerId, false);
        byte[] encryptionKey1 = new byte[100];
        TestUtils.RANDOM.nextBytes(encryptionKey1);
        byte[] usermetadata1 = new byte[1000];
        TestUtils.RANDOM.nextBytes(usermetadata1);
        int blobContentSize = 2000;
        byte[] data1 = new byte[blobContentSize];
        TestUtils.RANDOM.nextBytes(data1);
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
        accountId = Utils.getRandomShort(TestUtils.RANDOM);
        containerId = Utils.getRandomShort(TestUtils.RANDOM);
        long deletionTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
        MessageFormatInputStream messageFormatStream2 =
            new DeleteMessageFormatInputStream(key2, accountId, containerId, deletionTimeMs);

        MessageInfo msgInfo2 =
            new MessageInfo(key2, messageFormatStream2.getSize(), accountId, containerId, deletionTimeMs);

        // create message stream for blob 3
        StoreKey key3 = new MockId("id3");
        accountId = Utils.getRandomShort(TestUtils.RANDOM);
        containerId = Utils.getRandomShort(TestUtils.RANDOM);
        BlobProperties prop3 = new BlobProperties(10, "servid3", accountId, containerId, false);
        byte[] encryptionKey3 = new byte[100];
        TestUtils.RANDOM.nextBytes(encryptionKey3);
        byte[] usermetadata3 = new byte[1000];
        TestUtils.RANDOM.nextBytes(usermetadata3);
        blobContentSize = 2000;
        byte[] data3 = new byte[blobContentSize];
        TestUtils.RANDOM.nextBytes(data3);
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

        MessageSievingInputStream validMessageDetectionInputStream =
            new MessageSievingInputStream(inputStream, msgInfoList, new MockIdFactory(), new MetricRegistry());
        Assert.fail("IllegalStateException should have been thrown due to delete record ");
      }
    } catch (IllegalStateException e) {
      Assert.assertTrue("IllegalStateException thrown as expected ", true);
    }
    headerVersionToUse = Message_Header_Version_V1;
  }

  private boolean verifyBlob(MessageSievingInputStream validMessageDetectionInputStream, short headerVersion,
      int headerSize, int blobEncryptionRecordSize, int blobPropertiesRecordSize, int userMetadataSize, int blobSize,
      StoreKey key, int blobPropertiesSize, String serviceId, short accountId, short containerId, byte[] encryptionKey,
      byte[] usermetadata, byte[] data, short blobVersion, BlobType blobType) throws IOException {
    // verify header
    byte[] headerOutput = new byte[headerSize];
    validMessageDetectionInputStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(headerVersion, headerBuf.getShort());
    Assert.assertEquals(blobEncryptionRecordSize + blobPropertiesRecordSize + userMetadataSize + blobSize,
        headerBuf.getLong());
    if (headerVersion == Message_Header_Version_V2) {
      if (blobEncryptionRecordSize == 0) {
        Assert.assertEquals(Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
      } else {
        Assert.assertEquals(headerSize + key.sizeInBytes(), headerBuf.getInt());
      }
    }
    Assert.assertEquals(headerSize + key.sizeInBytes() + blobEncryptionRecordSize, headerBuf.getInt());
    Assert.assertEquals(Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Assert.assertEquals(headerSize + key.sizeInBytes() + blobEncryptionRecordSize + blobPropertiesRecordSize,
        headerBuf.getInt());
    Assert.assertEquals(
        headerSize + key.sizeInBytes() + blobEncryptionRecordSize + blobPropertiesRecordSize + userMetadataSize,
        headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - Crc_Size);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    validMessageDetectionInputStream.read(handleOutput);
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());

    if (headerVersion == Message_Header_Version_V2 && blobEncryptionRecordSize != 0) {
      // verify blob encryption record
      byte[] blobEncryptionOutput = new byte[blobEncryptionRecordSize];
      ByteBuffer blobEncryptionBuf = ByteBuffer.wrap(blobEncryptionOutput);
      validMessageDetectionInputStream.read(blobEncryptionOutput);
      Assert.assertEquals(blobEncryptionBuf.getShort(), Blob_Encryption_Key_V1);
      Assert.assertEquals(blobEncryptionBuf.getInt(), encryptionKey.length);
      dest = new byte[encryptionKey.length];
      blobEncryptionBuf.get(dest);
      Assert.assertArrayEquals(dest, encryptionKey);
      crc = new Crc32();
      crc.update(blobEncryptionOutput, 0, blobEncryptionRecordSize - Crc_Size);
      Assert.assertEquals(crc.getValue(), blobEncryptionBuf.getLong());
    }

    // verify blob properties
    byte[] blobPropertiesOutput = new byte[blobPropertiesRecordSize];
    ByteBuffer blobPropertiesBuf = ByteBuffer.wrap(blobPropertiesOutput);
    validMessageDetectionInputStream.read(blobPropertiesOutput);
    Assert.assertEquals(blobPropertiesBuf.getShort(), 1);
    BlobProperties propOutput = BlobPropertiesSerDe.getBlobPropertiesFromStream(
        new DataInputStream(new ByteBufferInputStream(blobPropertiesBuf)));
    Assert.assertEquals(blobPropertiesSize, propOutput.getBlobSize());
    Assert.assertEquals(serviceId, propOutput.getServiceId());
    Assert.assertEquals("AccountId mismatch", accountId, propOutput.getAccountId());
    Assert.assertEquals("ContainerId mismatch", containerId, propOutput.getContainerId());
    crc = new Crc32();
    crc.update(blobPropertiesOutput, 0, blobPropertiesRecordSize - Crc_Size);
    Assert.assertEquals(crc.getValue(), blobPropertiesBuf.getLong());

    // verify user metadata
    byte[] userMetadataOutput = new byte[userMetadataSize];
    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadataOutput);
    validMessageDetectionInputStream.read(userMetadataOutput);
    Assert.assertEquals(userMetadataBuf.getShort(), 1);
    Assert.assertEquals(userMetadataBuf.getInt(), usermetadata.length);
    dest = new byte[usermetadata.length];
    userMetadataBuf.get(dest);
    Assert.assertArrayEquals(dest, usermetadata);
    crc = new Crc32();
    crc.update(userMetadataOutput, 0, userMetadataSize - Crc_Size);
    Assert.assertEquals(crc.getValue(), userMetadataBuf.getLong());

    // verify blob
    CrcInputStream crcstream = new CrcInputStream(validMessageDetectionInputStream);
    DataInputStream streamData = new DataInputStream(crcstream);
    Assert.assertEquals(streamData.readShort(), blobVersion);
    if (blobVersion == Blob_Version_V2) {
      Assert.assertEquals(blobType.ordinal(), streamData.readShort());
    }
    Assert.assertEquals(streamData.readLong(), data.length);
    for (int i = 0; i < data.length; i++) {
      Assert.assertEquals((byte) streamData.read(), data[i]);
    }
    long crcVal = crcstream.getValue();
    Assert.assertEquals(crcVal, streamData.readLong());
    return true;
  }
}
