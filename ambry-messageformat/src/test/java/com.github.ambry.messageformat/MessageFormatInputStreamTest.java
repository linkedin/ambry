package com.github.ambry.messageformat;


import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class MessageFormatInputStreamTest {

  public static class MockId implements StoreKey {

    String id;

    public MockId(String id) {
      this.id = id;
    }

    @Override
    public ByteBuffer toBytes() {
      return ByteBuffer.wrap(id.getBytes());
    }

    @Override
    public short sizeInBytes() {
      return (short)id.length();
    }
  }

  @Test
  public void messageFormatBlobPropertyTest() throws IOException {
    StoreKey key = new MockId("id1");
    BlobProperties prop = new BlobProperties(10, "servid");
    byte[] usermetadata = new byte[1000];
    new Random().nextBytes(usermetadata);
    byte[] data = new byte[2000];
    new Random().nextBytes(data);
    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(data));

    MessageFormatInputStream messageFormatStream = new MessageFormatInputStream(key, prop,
            ByteBuffer.wrap(usermetadata), stream, 2000);

    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionBlobPropertyRecordSize(prop);
    int userMetadataSize = MessageFormat.getCurrentVersionUserMetadataSize(ByteBuffer.wrap(usermetadata));
    long dataSize = MessageFormat.getCurrentVersionDataSize(2000);

    Assert.assertEquals(messageFormatStream.getSize(), headerSize + systemMetadataSize + userMetadataSize + dataSize + 4 + key.sizeInBytes());

    // verify header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(1, headerBuf.getShort());
    Assert.assertEquals(systemMetadataSize + userMetadataSize + dataSize, headerBuf.getLong());
    Assert.assertEquals(headerSize + 4 + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(headerSize + 4 + key.sizeInBytes() + systemMetadataSize, headerBuf.getInt());
    Assert.assertEquals(headerSize + 4 + key.sizeInBytes() + systemMetadataSize + userMetadataSize, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - 8);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());


    // verify handle
    byte[] handleOutput = new byte[4 + key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);
    Assert.assertEquals(handleOutputBuf.getInt(), key.sizeInBytes());
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes().array());

    // verify system metadata
    byte[] systemMetadataOutput = new byte[systemMetadataSize];
    ByteBuffer systemMetadataBuf = ByteBuffer.wrap(systemMetadataOutput);
    messageFormatStream.read(systemMetadataOutput);
    Assert.assertEquals(systemMetadataBuf.getShort(), 1);
    Assert.assertEquals(MessageFormat.SystemMetadataType.BlobPropertyRecord.ordinal(), systemMetadataBuf.getShort());
    BlobProperties propOutput =
            BlobPropertySerDe.getBlobPropertyFromStream(new DataInputStream(new ByteBufferInputStream(systemMetadataBuf)));
    Assert.assertEquals(10, propOutput.getBlobSize());
    Assert.assertEquals("servid", propOutput.getServiceId());
    crc = new Crc32();
    crc.update(systemMetadataOutput, 0, systemMetadataSize - 8);
    Assert.assertEquals(crc.getValue(), systemMetadataBuf.getLong());

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
    crc.update(userMetadataOutput, 0, userMetadataSize - 8);
    Assert.assertEquals(crc.getValue(), userMetadataBuf.getLong());

    // verify data
    CrcInputStream crcstream = new CrcInputStream(messageFormatStream);
    DataInputStream streamData = new DataInputStream(crcstream);
    Assert.assertEquals(streamData.readShort(), 1);
    Assert.assertEquals(streamData.readLong(), 2000);
    for (int i = 0; i < 2000; i++) {
      Assert.assertEquals(streamData.read(), data[i]);
    }
    long crcVal = crcstream.getValue();
    Assert.assertEquals(crcVal, streamData.readLong());
  }

  @Test
  public void messageFormatDeleteRecordTest() throws IOException {
    StoreKey key = new MockId("id1");
    MessageFormatInputStream messageFormatStream = new MessageFormatInputStream(key, true);
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionDeleteRecordSize();
    Assert.assertEquals(headerSize + systemMetadataSize + 4 + key.sizeInBytes(), messageFormatStream.getSize());

    // check header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(1, headerBuf.getShort());
    Assert.assertEquals(systemMetadataSize, headerBuf.getLong());
    Assert.assertEquals(headerSize + 4 + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(-1, headerBuf.getInt());
    Assert.assertEquals(-1, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - 8);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[4 + key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);
    Assert.assertEquals(handleOutputBuf.getInt(), key.sizeInBytes());
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes().array());


    // check system metadata record
    byte[] systemMetadataOutput = new byte[systemMetadataSize];
    ByteBuffer systemMetadataBuf = ByteBuffer.wrap(systemMetadataOutput);
    messageFormatStream.read(systemMetadataOutput);
    Assert.assertEquals(systemMetadataBuf.getShort(), 1);
    Assert.assertEquals(MessageFormat.SystemMetadataType.DeleteRecord.ordinal(), systemMetadataBuf.getShort());
    Assert.assertEquals(true, systemMetadataBuf.get() == 1 ? true : false);
    crc = new Crc32();
    crc.update(systemMetadataOutput, 0, systemMetadataSize - 8);
    Assert.assertEquals(crc.getValue(), systemMetadataBuf.getLong());
  }

  @Test
  public void messageFormatTTLTest() throws IOException {
    StoreKey key = new MockId("id1");
    MessageFormatInputStream messageFormatStream = new MessageFormatInputStream(key, 1124);
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionTTLRecordSize();
    Assert.assertEquals(headerSize + systemMetadataSize + 4 + key.sizeInBytes(), messageFormatStream.getSize());

    // check header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(1, headerBuf.getShort());
    Assert.assertEquals(systemMetadataSize, headerBuf.getLong());
    Assert.assertEquals(headerSize + 4 + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(-1, headerBuf.getInt());
    Assert.assertEquals(-1, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - 8);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[4 + key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);
    Assert.assertEquals(handleOutputBuf.getInt(), key.sizeInBytes());
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes().array());


    // check system metadata record
    byte[] systemMetadataOutput = new byte[systemMetadataSize];
    ByteBuffer systemMetadataBuf = ByteBuffer.wrap(systemMetadataOutput);
    messageFormatStream.read(systemMetadataOutput);
    Assert.assertEquals(systemMetadataBuf.getShort(), 1);
    Assert.assertEquals(MessageFormat.SystemMetadataType.TTLRecord.ordinal(), systemMetadataBuf.getShort());
    Assert.assertEquals(1124, systemMetadataBuf.getLong());
    crc = new Crc32();
    crc.update(systemMetadataOutput, 0, systemMetadataSize - 8);
    Assert.assertEquals(crc.getValue(), systemMetadataBuf.getLong());
  }

}
