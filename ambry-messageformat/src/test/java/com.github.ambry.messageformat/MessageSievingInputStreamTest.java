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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class MessageSievingInputStreamTest {

  @Test
  public void testValidBlobsAgainstCorruption()
      throws IOException, MessageFormatException {
    testValidBlobs(MessageFormatRecord.Blob_Version_V1, BlobType.DataBlob);
    testValidBlobs(MessageFormatRecord.Blob_Version_V2, BlobType.DataBlob);
    testValidBlobs(MessageFormatRecord.Blob_Version_V2, BlobType.MetadataBlob);
  }

  public void testValidBlobs(short blobVersion, BlobType blobType)
      throws IOException, MessageFormatException {

    // MessageSievingInputStream contains put records for 3 valid blobs
    // id1(put record for valid blob), id2(put record for valid blob) and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    BlobProperties prop1 = new BlobProperties(10, "servid1");
    byte[] usermetadata1 = new byte[1000];
    new Random().nextBytes(usermetadata1);
    int blobContentSize = 2000;
    byte[] data1 = new byte[blobContentSize];
    new Random().nextBytes(data1);
    long blobSize = -1;
    if (blobVersion == MessageFormatRecord.Blob_Version_V1) {
      blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.DataBlob) {
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data1 = byteBufferBlob.array();
      blobContentSize = data1.length;
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    }

    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key1, prop1,
            ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                blobContentSize, blobType);

    MessageInfo msgInfo1 = new MessageInfo(key1, messageFormatStream1.getSize(), false, -1);

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    BlobProperties prop2 = new BlobProperties(10, "servid2");
    byte[] usermetadata2 = new byte[1000];
    new Random().nextBytes(usermetadata2);
    blobContentSize = 2000;
    byte[] data2 = new byte[blobContentSize];
    new Random().nextBytes(data2);
    if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data2 = byteBufferBlob.array();
      blobContentSize = data2.length;
    }
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream2 =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key2, prop2,
            ByteBuffer.wrap(usermetadata2), stream2, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2,
                blobContentSize, blobType);

    MessageInfo msgInfo2 = new MessageInfo(key2, messageFormatStream2.getSize(), false, -1);

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    BlobProperties prop3 = new BlobProperties(10, "servid3");
    byte[] usermetadata3 = new byte[1000];
    new Random().nextBytes(usermetadata3);
    blobContentSize = 2000;
    byte[] data3 = new byte[blobContentSize];
    new Random().nextBytes(data3);
    if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data3 = byteBufferBlob.array();
      blobContentSize = data3.length;
    }
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key3, prop3,
            ByteBuffer.wrap(usermetadata3), stream3, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3,
                blobContentSize, blobType);

    MessageInfo msgInfo3 = new MessageInfo(key3, messageFormatStream3.getSize(), false, -1);

    //create input stream for all blob messages together
    byte[] totalMessageStreamContent =
        new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
            + (int) messageFormatStream3.getSize()];
    messageFormatStream1.read(totalMessageStreamContent, 0, (int) messageFormatStream1.getSize());
    messageFormatStream2
        .read(totalMessageStreamContent, (int) messageFormatStream1.getSize(), (int) messageFormatStream2.getSize());
    messageFormatStream3
        .read(totalMessageStreamContent, (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
            (int) messageFormatStream3.getSize());

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageStreamContent));

    List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);

    MessageSievingInputStream validMessageDetectionInputStream =
        new MessageSievingInputStream(inputStream, msgInfoList, new MockIdFactory(), new MetricRegistry());

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));

    int totalHeadSize = 3 * headerSize;
    int totalBlobPropertiesSize = 3 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 3 * userMetadataSize;
    int totalBlobSize = 3 * (int) blobSize;
    int totalKeySize = key1.sizeInBytes() + key2.sizeInBytes() + key3.sizeInBytes();

    Assert.assertEquals(validMessageDetectionInputStream.getSize(),
        totalHeadSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize + totalKeySize);

    Assert.assertEquals(validMessageDetectionInputStream.getSize(),
        messageFormatStream1.getSize() + messageFormatStream2.getSize() + messageFormatStream3.getSize());

    Assert.assertEquals(true,
        verifyBlob(validMessageDetectionInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key1, 10, "servid1", usermetadata1, data1, blobVersion, blobType));

    Assert.assertEquals(true,
        verifyBlob(validMessageDetectionInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key2, 10, "servid2", usermetadata2, data2, blobVersion, blobType));

    Assert.assertEquals(true,
        verifyBlob(validMessageDetectionInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key3, 10, "servid3", usermetadata3, data3, blobVersion, blobType));
  }

  @Test
  public void testInValidBlobsAgainstCorruption()
      throws IOException, MessageFormatException {
    testInValidBlobs(MessageFormatRecord.Blob_Version_V1, BlobType.DataBlob);
    testInValidBlobs(MessageFormatRecord.Blob_Version_V2, BlobType.DataBlob);
    testInValidBlobs(MessageFormatRecord.Blob_Version_V2, BlobType.MetadataBlob);
  }

  private void testInValidBlobs(short blobVersion, BlobType blobType)
      throws IOException, MessageFormatException {

    // MessageSievingInputStream contains put records for 2 valid blobs and 1 corrupt blob
    // id1(put record for valid blob), id2(corrupt) and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    BlobProperties prop1 = new BlobProperties(10, "servid1");
    byte[] usermetadata1 = new byte[1000];
    new Random().nextBytes(usermetadata1);
    int blobContentSize = 2000;
    byte[] data1 = new byte[blobContentSize];
    new Random().nextBytes(data1);
    long blobSize = -1;
    if (blobVersion == MessageFormatRecord.Blob_Version_V1) {
      blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.DataBlob) {
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data1 = byteBufferBlob.array();
      blobContentSize = data1.length;
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else {
      Assert.fail("Illegal blob version " + blobVersion + " and type " + blobType);
    }
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key1, prop1,
            ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                blobContentSize, blobType);

    MessageInfo msgInfo1 = new MessageInfo(key1, messageFormatStream1.getSize(), false, -1);

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    BlobProperties prop2 = new BlobProperties(10, "servid2");
    byte[] usermetadata2 = new byte[1000];
    new Random().nextBytes(usermetadata2);
    blobContentSize = 2000;
    byte[] data2 = new byte[blobContentSize];
    new Random().nextBytes(data2);
    if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data2 = byteBufferBlob.array();
      blobContentSize = data2.length;
    }
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream2 =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key2, prop2,
            ByteBuffer.wrap(usermetadata2), stream2, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2,
                blobContentSize, blobType);

    MessageInfo msgInfo2 = new MessageInfo(key2, messageFormatStream2.getSize(), false, -1);

    // corrupt the message stream
    byte[] corruptMessageStream = new byte[(int) messageFormatStream2.getSize()];
    new Random().nextBytes(corruptMessageStream);

    InputStream corruptStream = new ByteBufferInputStream(ByteBuffer.wrap(corruptMessageStream));

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    BlobProperties prop3 = new BlobProperties(10, "servid3");
    byte[] usermetadata3 = new byte[1000];
    new Random().nextBytes(usermetadata3);
    blobContentSize = 2000;
    byte[] data3 = new byte[blobContentSize];
    new Random().nextBytes(data3);
    if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data3 = byteBufferBlob.array();
      blobContentSize = data3.length;
    }
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key3, prop3,
            ByteBuffer.wrap(usermetadata3), stream3, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3,
                blobContentSize, blobType);

    MessageInfo msgInfo3 = new MessageInfo(key3, messageFormatStream3.getSize(), false, -1);

    //create input stream for all blob messages together
    byte[] totalMessageStreamContent =
        new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
            + (int) messageFormatStream3.getSize()];
    messageFormatStream1.read(totalMessageStreamContent, 0, (int) messageFormatStream1.getSize());
    corruptStream
        .read(totalMessageStreamContent, (int) messageFormatStream1.getSize(), (int) messageFormatStream2.getSize());
    messageFormatStream3
        .read(totalMessageStreamContent, (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
            (int) messageFormatStream3.getSize());

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageStreamContent));

    List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);

    MessageSievingInputStream validMessageDetectionInputStream =
        new MessageSievingInputStream(inputStream, msgInfoList, new MockIdFactory(), new MetricRegistry());

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));

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
        verifyBlob(validMessageDetectionInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key1, 10, "servid1", usermetadata1, data1, blobVersion, blobType));

    Assert.assertEquals(true,
        verifyBlob(validMessageDetectionInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key3, 10, "servid3", usermetadata3, data3, blobVersion, blobType));
  }

  @Test
  public void testDeletedBlobsAgainstCorruption()
      throws IOException, MessageFormatException {
    testDeletedBlobs(MessageFormatRecord.Blob_Version_V1, BlobType.DataBlob);
    testDeletedBlobs(MessageFormatRecord.Blob_Version_V2, BlobType.DataBlob);
    testDeletedBlobs(MessageFormatRecord.Blob_Version_V2, BlobType.MetadataBlob);
  }

  private void testDeletedBlobs(short blobVersion, BlobType blobType)
      throws IOException, MessageFormatException {

    // MessageSievingInputStream contains put records for 2 valid blobs and 1 deleted blob
    // id1(put record for valid blob), id2(delete record) and id3(put record for valid blob)

    try {
      // create message stream for blob 1
      StoreKey key1 = new MockId("id1");
      BlobProperties prop1 = new BlobProperties(10, "servid1");
      byte[] usermetadata1 = new byte[1000];
      new Random().nextBytes(usermetadata1);
      int blobContentSize = 2000;
      byte[] data1 = new byte[blobContentSize];
      new Random().nextBytes(data1);
      if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
        ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
        data1 = byteBufferBlob.array();
        blobContentSize = data1.length;
      }
      ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

      MessageFormatInputStream messageFormatStream1 =
          (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key1, prop1,
              ByteBuffer.wrap(usermetadata1), stream1, blobContentSize, blobType)
              : new PutMessageFormatBlobV1InputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1,
                  blobContentSize, blobType);

      MessageInfo msgInfo1 = new MessageInfo(key1, messageFormatStream1.getSize(), false, -1);

      // create message stream for blob 2 and mark it as deleted
      StoreKey key2 = new MockId("id2");
      MessageFormatInputStream messageFormatStream2 = new DeleteMessageFormatInputStream(key2);

      MessageInfo msgInfo2 = new MessageInfo(key2, messageFormatStream2.getSize(), true, -1);

      // create message stream for blob 3
      StoreKey key3 = new MockId("id3");
      BlobProperties prop3 = new BlobProperties(10, "servid3");
      byte[] usermetadata3 = new byte[1000];
      new Random().nextBytes(usermetadata3);
      blobContentSize = 2000;
      byte[] data3 = new byte[blobContentSize];
      new Random().nextBytes(data3);
      if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
        ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
        data3 = byteBufferBlob.array();
        blobContentSize = data3.length;
      }
      ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

      MessageFormatInputStream messageFormatStream3 =
          (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key3, prop3,
              ByteBuffer.wrap(usermetadata3), stream3, blobContentSize, blobType)
              : new PutMessageFormatBlobV1InputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3,
                  blobContentSize, blobType);

      MessageInfo msgInfo3 = new MessageInfo(key3, messageFormatStream3.getSize(), false, -1);

      //create input stream for all blob messages together
      byte[] totalMessageContent = new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
          + (int) messageFormatStream3.getSize()];
      messageFormatStream1.read(totalMessageContent, 0, (int) messageFormatStream1.getSize());
      messageFormatStream2
          .read(totalMessageContent, (int) messageFormatStream1.getSize(), (int) messageFormatStream2.getSize());
      messageFormatStream3
          .read(totalMessageContent, (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
              (int) messageFormatStream3.getSize());

      InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageContent));

      List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
      msgInfoList.add(msgInfo1);
      msgInfoList.add(msgInfo2);
      msgInfoList.add(msgInfo3);

      MessageSievingInputStream validMessageDetectionInputStream =
          new MessageSievingInputStream(inputStream, msgInfoList, new MockIdFactory(), new MetricRegistry());
      Assert.fail("IllegalStateException should have been thrown due to delete record ");
    } catch (IllegalStateException e) {
      Assert.assertTrue("IllegalStateException thrown as expected ", true);
    }
  }

  private boolean verifyBlob(MessageSievingInputStream validMessageDetectionInputStream, int headerSize,
      int blobPropertiesRecordSize, int userMetadataSize, int blobSize, StoreKey key, int blobPropertiesSize,
      String serviceId, byte[] usermetadata, byte[] data, short blobVersion, BlobType blobType)
      throws IOException {
    // verify header
    byte[] headerOutput = new byte[headerSize];
    validMessageDetectionInputStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(1, headerBuf.getShort());
    Assert.assertEquals(blobPropertiesRecordSize + userMetadataSize + blobSize, headerBuf.getLong());
    Assert.assertEquals(headerSize + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Assert.assertEquals(headerSize + key.sizeInBytes() + blobPropertiesRecordSize, headerBuf.getInt());
    Assert
        .assertEquals(headerSize + key.sizeInBytes() + blobPropertiesRecordSize + userMetadataSize, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    validMessageDetectionInputStream.read(handleOutput);
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());

    // verify blob properties
    byte[] blobPropertiesOutput = new byte[blobPropertiesRecordSize];
    ByteBuffer blobPropertiesBuf = ByteBuffer.wrap(blobPropertiesOutput);
    validMessageDetectionInputStream.read(blobPropertiesOutput);
    Assert.assertEquals(blobPropertiesBuf.getShort(), 1);
    BlobProperties propOutput = BlobPropertiesSerDe
        .getBlobPropertiesFromStream(new DataInputStream(new ByteBufferInputStream(blobPropertiesBuf)));
    Assert.assertEquals(blobPropertiesSize, propOutput.getBlobSize());
    Assert.assertEquals(serviceId, propOutput.getServiceId());
    crc = new Crc32();
    crc.update(blobPropertiesOutput, 0, blobPropertiesRecordSize - MessageFormatRecord.Crc_Size);
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
    crc.update(userMetadataOutput, 0, userMetadataSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), userMetadataBuf.getLong());

    // verify blob
    CrcInputStream crcstream = new CrcInputStream(validMessageDetectionInputStream);
    DataInputStream streamData = new DataInputStream(crcstream);
    Assert.assertEquals(streamData.readShort(), blobVersion);
    if (blobVersion == MessageFormatRecord.Blob_Version_V2) {
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
