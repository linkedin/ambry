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

  private static final int Key_Size = 4;

  public static class MockId extends StoreKey {

    String id;

    public MockId(String id) {
      this.id = id;
    }

    @Override
    public byte[] toBytes() {
      return id.getBytes();
    }

    @Override
    public short sizeInBytes() {
      return (short)id.length();
    }

    @Override
    public int compareTo(StoreKey o) {
      if (o == null)
        throw new NullPointerException("input argument null");
      MockId other = (MockId) o;
      return id.compareTo(other.id);
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

    MessageFormatInputStream messageFormatStream = new PutMessageFormatInputStream(key, prop,
            ByteBuffer.wrap(usermetadata), stream, 2000);

    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionBlobPropertyRecordSize(prop);
    int userMetadataSize = MessageFormat.getCurrentVersionUserMetadataSize(ByteBuffer.wrap(usermetadata));
    long dataSize = MessageFormat.getCurrentVersionDataSize(2000);

    Assert.assertEquals(messageFormatStream.getSize(), headerSize + systemMetadataSize +
                        userMetadataSize + dataSize + Key_Size + key.sizeInBytes());

    // verify header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(MessageFormat.Header_Current_Version, headerBuf.getShort());
    Assert.assertEquals(systemMetadataSize + userMetadataSize + dataSize, headerBuf.getLong());
    Assert.assertEquals(headerSize + Key_Size + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(headerSize + Key_Size + key.sizeInBytes() + systemMetadataSize, headerBuf.getInt());
    Assert.assertEquals(headerSize + Key_Size + key.sizeInBytes() + systemMetadataSize + userMetadataSize, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - MessageFormat.Crc_Size);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());


    // verify handle
    byte[] handleOutput = new byte[Key_Size + key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);
    Assert.assertEquals(handleOutputBuf.getInt(), key.sizeInBytes());
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());

    // verify system metadata
    byte[] systemMetadataOutput = new byte[systemMetadataSize];
    ByteBuffer systemMetadataBuf = ByteBuffer.wrap(systemMetadataOutput);
    messageFormatStream.read(systemMetadataOutput);
    Assert.assertEquals(systemMetadataBuf.getShort(), MessageFormat.SystemMetadata_Current_Version);
    Assert.assertEquals(MessageFormat.SystemMetadataRecordType.BlobPropertyRecord.ordinal(), systemMetadataBuf.getShort());
    BlobProperties propOutput =
            BlobPropertySerDe.getBlobPropertyFromStream(new DataInputStream(new ByteBufferInputStream(systemMetadataBuf)));
    Assert.assertEquals(10, propOutput.getBlobSize());
    Assert.assertEquals("servid", propOutput.getServiceId());
    crc = new Crc32();
    crc.update(systemMetadataOutput, 0, systemMetadataSize - MessageFormat.Crc_Size);
    Assert.assertEquals(crc.getValue(), systemMetadataBuf.getLong());

    // verify user metadata
    byte[] userMetadataOutput = new byte[userMetadataSize];
    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadataOutput);
    messageFormatStream.read(userMetadataOutput);
    Assert.assertEquals(userMetadataBuf.getShort(), MessageFormat.UserMetadata_Current_Version);
    Assert.assertEquals(userMetadataBuf.getInt(), 1000);
    dest = new byte[1000];
    userMetadataBuf.get(dest);
    Assert.assertArrayEquals(dest, usermetadata);
    crc = new Crc32();
    crc.update(userMetadataOutput, 0, userMetadataSize - MessageFormat.Crc_Size);
    Assert.assertEquals(crc.getValue(), userMetadataBuf.getLong());

    // verify data
    CrcInputStream crcstream = new CrcInputStream(messageFormatStream);
    DataInputStream streamData = new DataInputStream(crcstream);
    Assert.assertEquals(streamData.readShort(), MessageFormat.Data_Current_Version);
    Assert.assertEquals(streamData.readLong(), 2000);
    for (int i = 0; i < 2000; i++) {
      Assert.assertEquals((byte)streamData.read(), data[i]);
    }
    long crcVal = crcstream.getValue();
    Assert.assertEquals(crcVal, streamData.readLong());
  }

  @Test
  public void messageFormatDeleteRecordTest() throws IOException {
    StoreKey key = new MockId("id1");
    MessageFormatInputStream messageFormatStream = new DeleteMessageFormatInputStream(key);
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionDeleteRecordSize();
    Assert.assertEquals(headerSize + systemMetadataSize + Key_Size + key.sizeInBytes(), messageFormatStream.getSize());

    // check header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(MessageFormat.Header_Current_Version, headerBuf.getShort());
    Assert.assertEquals(systemMetadataSize, headerBuf.getLong());
    Assert.assertEquals(headerSize + Key_Size + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(MessageFormat.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Assert.assertEquals(MessageFormat.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - MessageFormat.Crc_Size);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[Key_Size + key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);
    Assert.assertEquals(handleOutputBuf.getInt(), key.sizeInBytes());
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());


    // check system metadata record
    byte[] systemMetadataOutput = new byte[systemMetadataSize];
    ByteBuffer systemMetadataBuf = ByteBuffer.wrap(systemMetadataOutput);
    messageFormatStream.read(systemMetadataOutput);
    Assert.assertEquals(systemMetadataBuf.getShort(), MessageFormat.SystemMetadata_Current_Version);
    Assert.assertEquals(MessageFormat.SystemMetadataRecordType.DeleteRecord.ordinal(), systemMetadataBuf.getShort());
    Assert.assertEquals(true, systemMetadataBuf.get() == 1 ? true : false);
    crc = new Crc32();
    crc.update(systemMetadataOutput, 0, systemMetadataSize - MessageFormat.Crc_Size);
    Assert.assertEquals(crc.getValue(), systemMetadataBuf.getLong());
  }

  @Test
  public void messageFormatTTLTest() throws IOException {
    StoreKey key = new MockId("id1");
    MessageFormatInputStream messageFormatStream = new TTLMessageFormatInputStream(key, 1124);
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionTTLRecordSize();
    Assert.assertEquals(headerSize + systemMetadataSize + Key_Size + key.sizeInBytes(), messageFormatStream.getSize());

    // check header
    byte[] headerOutput = new byte[headerSize];
    messageFormatStream.read(headerOutput);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerOutput);
    Assert.assertEquals(MessageFormat.Header_Current_Version, headerBuf.getShort());
    Assert.assertEquals(systemMetadataSize, headerBuf.getLong());
    Assert.assertEquals(headerSize + Key_Size + key.sizeInBytes(), headerBuf.getInt());
    Assert.assertEquals(MessageFormat.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Assert.assertEquals(MessageFormat.Message_Header_Invalid_Relative_Offset, headerBuf.getInt());
    Crc32 crc = new Crc32();
    crc.update(headerOutput, 0, headerSize - MessageFormat.Crc_Size);
    Assert.assertEquals(crc.getValue(), headerBuf.getLong());

    // verify handle
    byte[] handleOutput = new byte[Key_Size + key.sizeInBytes()];
    ByteBuffer handleOutputBuf = ByteBuffer.wrap(handleOutput);
    messageFormatStream.read(handleOutput);
    Assert.assertEquals(handleOutputBuf.getInt(), key.sizeInBytes());
    byte[] dest = new byte[key.sizeInBytes()];
    handleOutputBuf.get(dest);
    Assert.assertArrayEquals(dest, key.toBytes());


    // check system metadata record
    byte[] systemMetadataOutput = new byte[systemMetadataSize];
    ByteBuffer systemMetadataBuf = ByteBuffer.wrap(systemMetadataOutput);
    messageFormatStream.read(systemMetadataOutput);
    Assert.assertEquals(systemMetadataBuf.getShort(), MessageFormat.SystemMetadata_Current_Version);
    Assert.assertEquals(MessageFormat.SystemMetadataRecordType.TTLRecord.ordinal(), systemMetadataBuf.getShort());
    Assert.assertEquals(1124, systemMetadataBuf.getLong());
    crc = new Crc32();
    crc.update(systemMetadataOutput, 0, systemMetadataSize - MessageFormat.Crc_Size);
    Assert.assertEquals(crc.getValue(), systemMetadataBuf.getLong());
  }

}
