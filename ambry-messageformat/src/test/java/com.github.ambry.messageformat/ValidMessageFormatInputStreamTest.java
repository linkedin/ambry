package com.github.ambry.messageformat;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.slf4j.LoggerFactory;


public class ValidMessageFormatInputStreamTest {

  MockClusterMap clusterMap;

  public ValidMessageFormatInputStreamTest() throws IOException{
    clusterMap = new MockClusterMap();
  }

  @Test
  public void validMessageFormatInputStreamTestForValidBlobs()
      throws IOException, MessageFormatException {

    MockClusterMap clusterMap = new MockClusterMap();
    // ValidMessageFormatInputStream contains put records for 3 valid blobs
    // id1(put record for valid blob), id2(put record for valid blob) and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    BlobProperties prop1 = new BlobProperties(10, "servid1");
    byte[] usermetadata1 = new byte[1000];
    new Random().nextBytes(usermetadata1);
    byte[] data1 = new byte[2000];
    new Random().nextBytes(data1);
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        new PutMessageFormatInputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1, 2000);

    MessageInfo msgInfo1 = new MessageInfo(key1, messageFormatStream1.getSize(), false, -1);

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    BlobProperties prop2 = new BlobProperties(10, "servid2");
    byte[] usermetadata2 = new byte[1000];
    new Random().nextBytes(usermetadata2);
    byte[] data2 = new byte[2000];
    new Random().nextBytes(data2);
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream2 =
        new PutMessageFormatInputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2, 2000);

    MessageInfo msgInfo2 = new MessageInfo(key2, messageFormatStream2.getSize(), false, -1);

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    BlobProperties prop3 = new BlobProperties(10, "servid3");
    byte[] usermetadata3 = new byte[1000];
    new Random().nextBytes(usermetadata3);
    byte[] data3 = new byte[2000];
    new Random().nextBytes(data3);
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        new PutMessageFormatInputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3, 2000);

    MessageInfo msgInfo3 = new MessageInfo(key3, messageFormatStream3.getSize(), false, -1);

    //create input stream for all blob messages together
    byte[] totalMessageStreamContent = new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
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

    ValidMessageFormatInputStream validMessageFormatInputStream =
        new ValidMessageFormatInputStream(inputStream, msgInfoList, new MockIdFactory(),
            LoggerFactory.getLogger(getClass()), true, clusterMap.getMetricRegistry());

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));
    long blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(2000);

    int totalHeadSize = 3 * headerSize;
    int totalBlobPropertiesSize = 3 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 3 * userMetadataSize;
    int totalBlobSize = 3 * (int) blobSize;
    int totalKeySize = key1.sizeInBytes() + key2.sizeInBytes() + key3.sizeInBytes();

    Assert.assertEquals(validMessageFormatInputStream.getSize(),
        totalHeadSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize + totalKeySize);

    Assert.assertEquals(validMessageFormatInputStream.getSize(),
        messageFormatStream1.getSize() + messageFormatStream2.getSize() + messageFormatStream3.getSize());

    Assert.assertEquals(true,
        verifyBlob(validMessageFormatInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key1, 10, "servid1", usermetadata1, data1));

    Assert.assertEquals(true,
        verifyBlob(validMessageFormatInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key2, 10, "servid2", usermetadata2, data2));

    Assert.assertEquals(true,
        verifyBlob(validMessageFormatInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key3, 10, "servid3", usermetadata3, data3));
  }

  @Test
  public void validMessageFormatInputStreamTestForInValidBlobs()
      throws IOException, MessageFormatException {

    // ValidMessageFormatInputStream contains put records for 2 valid blobs and 1 corrupt blob
    // id1(put record for valid blob), id2(corrupt) and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    BlobProperties prop1 = new BlobProperties(10, "servid1");
    byte[] usermetadata1 = new byte[1000];
    new Random().nextBytes(usermetadata1);
    byte[] data1 = new byte[2000];
    new Random().nextBytes(data1);
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        new PutMessageFormatInputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1, 2000);

    MessageInfo msgInfo1 = new MessageInfo(key1, messageFormatStream1.getSize(), false, -1);

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    BlobProperties prop2 = new BlobProperties(10, "servid2");
    byte[] usermetadata2 = new byte[1000];
    new Random().nextBytes(usermetadata2);
    byte[] data2 = new byte[2000];
    new Random().nextBytes(data2);
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(data2));

    MessageFormatInputStream messageFormatStream2 =
        new PutMessageFormatInputStream(key2, prop2, ByteBuffer.wrap(usermetadata2), stream2, 2000);

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
    byte[] data3 = new byte[2000];
    new Random().nextBytes(data3);
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        new PutMessageFormatInputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3, 2000);

    MessageInfo msgInfo3 = new MessageInfo(key3, messageFormatStream3.getSize(), false, -1);

    //create input stream for all blob messages together
    byte[] totalMessageStreamContent = new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
        + (int) messageFormatStream3.getSize()];
    messageFormatStream1.read(totalMessageStreamContent, 0, (int) messageFormatStream1.getSize());
    corruptStream.read(totalMessageStreamContent, (int) messageFormatStream1.getSize(), (int) messageFormatStream2.getSize());
    messageFormatStream3
        .read(totalMessageStreamContent, (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
            (int) messageFormatStream3.getSize());

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageStreamContent));

    List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);

    ValidMessageFormatInputStream validMessageFormatInputStream =
        new ValidMessageFormatInputStream(inputStream, msgInfoList, new MockIdFactory(),
            LoggerFactory.getLogger(getClass()), true, clusterMap.getMetricRegistry());

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));
    long blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(2000);

    int totalHeadSize = 2 * headerSize;
    int totalBlobPropertiesSize = 2 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 2 * userMetadataSize;
    int totalBlobSize = 2 * (int) blobSize;
    int totalKeySize = key1.sizeInBytes() + key3.sizeInBytes();

    Assert.assertEquals(validMessageFormatInputStream.getSize(),
        totalHeadSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize + totalKeySize);

    Assert.assertEquals(validMessageFormatInputStream.getSize(),
        messageFormatStream1.getSize() + messageFormatStream3.getSize());

    Assert.assertEquals(true,
        verifyBlob(validMessageFormatInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key1, 10, "servid1", usermetadata1, data1));

    Assert.assertEquals(true,
        verifyBlob(validMessageFormatInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key3, 10, "servid3", usermetadata3, data3));
  }

  @Test
  public void validMessageFormatInputStreamTestForDeletedBlobs()
      throws IOException, MessageFormatException {

    // ValidMessageFormatInputStream contains put records for 2 valid blobs and 1 deleted blob
    // id1(put record for valid blob), id2(delete record) and id3(put record for valid blob)

    // create message stream for blob 1
    StoreKey key1 = new MockId("id1");
    BlobProperties prop1 = new BlobProperties(10, "servid1");
    byte[] usermetadata1 = new byte[1000];
    new Random().nextBytes(usermetadata1);
    byte[] data1 = new byte[2000];
    new Random().nextBytes(data1);
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(data1));

    MessageFormatInputStream messageFormatStream1 =
        new PutMessageFormatInputStream(key1, prop1, ByteBuffer.wrap(usermetadata1), stream1, 2000);

    MessageInfo msgInfo1 = new MessageInfo(key1, messageFormatStream1.getSize(), false, -1);

    // create message stream for blob 2
    StoreKey key2 = new MockId("id2");
    MessageFormatInputStream messageFormatStream2 = new DeleteMessageFormatInputStream(key2);

    MessageInfo msgInfo2 = new MessageInfo(key2, messageFormatStream2.getSize(), true, -1);

    // create message stream for blob 3
    StoreKey key3 = new MockId("id3");
    BlobProperties prop3 = new BlobProperties(10, "servid3");
    byte[] usermetadata3 = new byte[1000];
    new Random().nextBytes(usermetadata3);
    byte[] data3 = new byte[2000];
    new Random().nextBytes(data3);
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(data3));

    MessageFormatInputStream messageFormatStream3 =
        new PutMessageFormatInputStream(key3, prop3, ByteBuffer.wrap(usermetadata3), stream3, 2000);

    MessageInfo msgInfo3 = new MessageInfo(key3, messageFormatStream3.getSize(), false, -1);

    //create input stream for all blob messages together
    byte[] totalMessageContent = new byte[(int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize()
        + (int) messageFormatStream3.getSize()];
    messageFormatStream1.read(totalMessageContent, 0, (int) messageFormatStream1.getSize());
    messageFormatStream2.read(totalMessageContent, (int) messageFormatStream1.getSize(), (int) messageFormatStream2.getSize());
    messageFormatStream3
        .read(totalMessageContent, (int) messageFormatStream1.getSize() + (int) messageFormatStream2.getSize(),
            (int) messageFormatStream3.getSize());

    InputStream inputStream = new ByteBufferInputStream(ByteBuffer.wrap(totalMessageContent));

    List<MessageInfo> msgInfoList = new ArrayList<MessageInfo>();
    msgInfoList.add(msgInfo1);
    msgInfoList.add(msgInfo2);
    msgInfoList.add(msgInfo3);

    ValidMessageFormatInputStream validMessageFormatInputStream =
        new ValidMessageFormatInputStream(inputStream, msgInfoList, new MockIdFactory(),
            LoggerFactory.getLogger(getClass()), true, clusterMap.getMetricRegistry());

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(prop1);
    int userMetadataSize =
        MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(ByteBuffer.wrap(usermetadata1));
    long blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(2000);

    int totalHeadSize = 2 * headerSize;
    int totalBlobPropertiesSize = 2 * blobPropertiesRecordSize;
    int totalUserMetadataSize = 2 * userMetadataSize;
    int totalBlobSize = 2 * (int) blobSize;
    int totalKeySize = key1.sizeInBytes() + key3.sizeInBytes();

    Assert.assertEquals(validMessageFormatInputStream.getSize(),
        totalHeadSize + totalBlobPropertiesSize + totalUserMetadataSize + totalBlobSize + totalKeySize);

    Assert.assertEquals(true,
        verifyBlob(validMessageFormatInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key1, 10, "servid1", usermetadata1, data1));

    Assert.assertEquals(true,
        verifyBlob(validMessageFormatInputStream, headerSize, blobPropertiesRecordSize, userMetadataSize,
            (int) blobSize, key3, 10, "servid3", usermetadata3, data3));
  }

  private boolean verifyBlob(ValidMessageFormatInputStream messageFormatByteBufferInputStream, int headerSize,
      int blobPropertiesRecordSize, int userMetadataSize, int blobSize, StoreKey key, int blobPropertiesSize,
      String serviceId, byte[] usermetadata, byte[] data)
      throws IOException {
    // verify header
    byte[] headerOutput = new byte[headerSize];
    messageFormatByteBufferInputStream.read(headerOutput);
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
    messageFormatByteBufferInputStream.read(handleOutput);
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());

    // verify blob properties
    byte[] blobPropertiesOutput = new byte[blobPropertiesRecordSize];
    ByteBuffer blobPropertiesBuf = ByteBuffer.wrap(blobPropertiesOutput);
    messageFormatByteBufferInputStream.read(blobPropertiesOutput);
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
    messageFormatByteBufferInputStream.read(userMetadataOutput);
    Assert.assertEquals(userMetadataBuf.getShort(), 1);
    Assert.assertEquals(userMetadataBuf.getInt(), usermetadata.length);
    dest = new byte[usermetadata.length];
    userMetadataBuf.get(dest);
    Assert.assertArrayEquals(dest, usermetadata);
    crc = new Crc32();
    crc.update(userMetadataOutput, 0, userMetadataSize - MessageFormatRecord.Crc_Size);
    Assert.assertEquals(crc.getValue(), userMetadataBuf.getLong());

    // verify blob
    CrcInputStream crcstream = new CrcInputStream(messageFormatByteBufferInputStream);
    DataInputStream streamData = new DataInputStream(crcstream);
    Assert.assertEquals(streamData.readShort(), 1);
    Assert.assertEquals(streamData.readLong(), data.length);
    for (int i = 0; i < data.length; i++) {
      Assert.assertEquals((byte) streamData.read(), data[i]);
    }
    long crcVal = crcstream.getValue();
    Assert.assertEquals(crcVal, streamData.readLong());
    return true;
  }
}
