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

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class MessageFormatInputStreamTest {

  @Test
  public void messageFormatBlobPropertiesTest()
      throws IOException, MessageFormatException {
    messageFormatBlobPropertiesTest(MessageFormatRecord.Blob_Version_V1, BlobType.DataBlob);
    messageFormatBlobPropertiesTest(MessageFormatRecord.Blob_Version_V2, BlobType.DataBlob);
    messageFormatBlobPropertiesTest(MessageFormatRecord.Blob_Version_V2, BlobType.MetadataBlob);
  }

  private void messageFormatBlobPropertiesTest(short blobVersion, BlobType blobType)
      throws IOException, MessageFormatException {
    StoreKey key = new MockId("id1");
    BlobProperties prop = new BlobProperties(10, "servid");
    byte[] usermetadata = new byte[1000];
    new Random().nextBytes(usermetadata);
    int blobContentSize = 2000;
    byte[] data = new byte[blobContentSize];
    new Random().nextBytes(data);
    long blobSize = -1;

    if (blobVersion == MessageFormatRecord.Blob_Version_V1) {
      blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.DataBlob) {
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    } else if (blobVersion == MessageFormatRecord.Blob_Version_V2 && blobType == BlobType.MetadataBlob) {
      ByteBuffer byteBufferBlob = MessageFormatTestUtils.getBlobContentForMetadataBlob(blobContentSize);
      data = byteBufferBlob.array();
      blobSize = (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobContentSize);
    }

    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(data));

    MessageFormatInputStream messageFormatStream =
        (blobVersion == MessageFormatRecord.Blob_Version_V2) ? new PutMessageFormatInputStream(key, prop,
            ByteBuffer.wrap(usermetadata), stream, blobContentSize, blobType)
            : new PutMessageFormatBlobV1InputStream(key, prop, ByteBuffer.wrap(usermetadata), stream, blobContentSize,
                blobType);

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop);
    int userMetadataSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata));

    Assert.assertEquals(messageFormatStream.getSize(), headerSize + blobPropertiesRecordSize +
        userMetadataSize + blobSize + key.sizeInBytes());

    // verify header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
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
    messageFormatStream.read(handleOutput);

    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());

    // verify blob properties
    byte[] blobPropertiesOutput = new byte[blobPropertiesRecordSize];
    ByteBuffer blobPropertiesBuf = ByteBuffer.wrap(blobPropertiesOutput);
    messageFormatStream.read(blobPropertiesOutput);
    Assert.assertEquals(blobPropertiesBuf.getShort(), 1);
    BlobProperties propOutput = BlobPropertiesSerDe
        .getBlobPropertiesFromStream(new DataInputStream(new ByteBufferInputStream(blobPropertiesBuf)));
    Assert.assertEquals(10, propOutput.getBlobSize());
    Assert.assertEquals("servid", propOutput.getServiceId());
    crc = new Crc32();
    crc.update(blobPropertiesOutput, 0, blobPropertiesRecordSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), blobPropertiesBuf.getLong());

    // verify user metadata
    byte[] userMetadataOutput = new byte[userMetadataSize];
    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadataOutput);
    messageFormatStream.read(userMetadataOutput);
    Assert.assertEquals(userMetadataBuf.getShort(), 1);
    Assert.assertEquals(userMetadataBuf.getInt(), 1000);
    dest = new byte[1000];
    userMetadataBuf.get(dest);
    Assert.assertArrayEquals(dest, usermetadata);
    crc = new Crc32();
    crc.update(userMetadataOutput, 0, userMetadataSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), userMetadataBuf.getLong());

    // verify blob
    CrcInputStream crcstream = new CrcInputStream(messageFormatStream);
    DataInputStream streamData = new DataInputStream(crcstream);
    Assert.assertEquals(streamData.readShort(), blobVersion);
    if (blobVersion == MessageFormatRecord.Blob_Version_V2) {
      Assert.assertEquals(streamData.readShort(), blobType.ordinal());
    }
    Assert.assertEquals(streamData.readLong(), blobContentSize);
    for (int i = 0; i < blobContentSize; i++) {
      Assert.assertEquals((byte) streamData.read(), data[i]);
    }
    long crcVal = crcstream.getValue();
    Assert.assertEquals(crcVal, streamData.readLong());
  }

  @Test
  public void messageFormatDeleteRecordTest()
      throws IOException, MessageFormatException {
    StoreKey key = new MockId("id1");
    MessageFormatInputStream messageFormatStream = new DeleteMessageFormatInputStream(key);
    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int deleteRecordSize = MessageFormatRecord.Delete_Format_V1.getDeleteRecordSize();
    Assert.assertEquals(headerSize + deleteRecordSize + key.sizeInBytes(), messageFormatStream.getSize());

    // check header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(1, headerBuf.getShort());
    Assert.assertEquals(deleteRecordSize, headerBuf.getLong());
    Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Assert.assertEquals(headerSize + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Assert.assertEquals(MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());

    // check delete record
    byte[] deleteRecordOutput = new byte[deleteRecordSize];
    ByteBuffer deleteRecordBuf = ByteBuffer.wrap(deleteRecordOutput);
    messageFormatStream.read(deleteRecordOutput);
    Assert.assertEquals(deleteRecordBuf.getShort(), 1);
    Assert.assertEquals(true, deleteRecordBuf.get() == 1 ? true : false);
    crc = new Crc32();
    crc.update(deleteRecordOutput, 0, deleteRecordSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), deleteRecordBuf.getLong());
  }
}
