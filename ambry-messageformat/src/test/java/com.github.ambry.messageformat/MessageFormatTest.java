package com.github.ambry.messageformat;

import java.io.BufferedInputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import org.junit.Assert;
import org.junit.Test;

public class MessageFormatTest {
  @Test
  public void deserializeTest() {
    try {
      BlobProperties properties = new BlobProperties(1234, true, "test", "member", "parent", 1234, "id");
      ByteBuffer stream = ByteBuffer.allocate(MessageFormat.getCurrentVersionBlobPropertyRecordSize(properties));
      MessageFormat.serializeCurrentVersionBlobPropertyRecord(stream, properties);
      stream.flip();
      BlobProperties result = MessageFormat.deserializeBlobProperties(new ByteBufferInputStream(stream));
      Assert.assertEquals(properties.getBlobSize(), result.getBlobSize());
      Assert.assertEquals(properties.getContentType(), result.getContentType());
      Assert.assertEquals(properties.getCreationTimeInMs(), result.getCreationTimeInMs());
      Assert.assertEquals(properties.getMemberId(), result.getMemberId());
      Assert.assertEquals(properties.getServiceId(), result.getServiceId());

      // corrupt blob property
      stream.flip();
      stream.put(10, (byte)10);
      try {
        BlobProperties resultCorrupt = MessageFormat.deserializeBlobProperties(new ByteBufferInputStream(stream));
        Assert.assertEquals(true, false);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }

      ByteBuffer deleteRecord = ByteBuffer.allocate(MessageFormat.getCurrentVersionDeleteRecordSize());
      MessageFormat.serializeCurrentVersionDeleteRecord(deleteRecord, true);
      deleteRecord.flip();
      boolean deleted = MessageFormat.deserializeDeleteRecord(new ByteBufferInputStream(deleteRecord));
      Assert.assertEquals(deleted, true);

      // corrupt delete record
      deleteRecord.flip();
      deleteRecord.put(10, (byte)4);
      try {
        boolean corruptDeleted = MessageFormat.deserializeDeleteRecord(new ByteBufferInputStream(deleteRecord));
        Assert.assertEquals(true, false);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }

      ByteBuffer header = ByteBuffer.allocate(MessageFormat.getCurrentVersionHeaderSize());
      MessageFormat.serializeCurrentVersionHeader(header, 1000, 10, 20, 30);
      header.flip();
      MessageFormat.MessageHeader_Format_V1 format = new MessageFormat.MessageHeader_Format_V1(header);
      Assert.assertEquals(format.getMessageSize(), 1000);
      Assert.assertEquals(format.getSystemMetadataRelativeOffset(), 10);
      Assert.assertEquals(format.getUserMetadataRelativeOffset(), 20);
      Assert.assertEquals(format.getDataRelativeOffset(), 30);

      // corrupt header
      header.put(10, (byte) 1);
      format = new MessageFormat.MessageHeader_Format_V1(header);
      try {
        format.verifyCrc();
        Assert.assertEquals(true, false);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }

      ByteBuffer ttl = ByteBuffer.allocate(MessageFormat.getCurrentVersionTTLRecordSize());
      MessageFormat.serializeCurrentVersionTTLRecord(ttl, -1);
      ttl.flip();
      long ttlValue = MessageFormat.deserializeTTLRecord(new ByteBufferInputStream(ttl));
      Assert.assertEquals(ttlValue, -1);

      // corrupt ttl
      ttl.flip();
      ttl.put(10, (byte)4);
      try {
        ttlValue = MessageFormat.deserializeTTLRecord(new ByteBufferInputStream(ttl));
        Assert.assertEquals(true, false);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }

      ByteBuffer usermetadata = ByteBuffer.allocate(1000);
      new Random().nextBytes(usermetadata.array());
      ByteBuffer output = ByteBuffer.allocate(MessageFormat.getCurrentVersionUserMetadataSize(usermetadata));
      MessageFormat.serializeCurrentVersionUserMetadata(output, usermetadata);
      output.flip();
      ByteBuffer bufOutput = MessageFormat.deserializeMetadata(new ByteBufferInputStream(output));
      Assert.assertArrayEquals(usermetadata.array(), bufOutput.array());

      // corrupt usermetadata
      output.flip();
      output.put(10, (byte)1);
      try {
        MessageFormat.deserializeMetadata(new ByteBufferInputStream(output));
        Assert.assertEquals(true, false);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }

      ByteBuffer data = ByteBuffer.allocate(2000);
      new Random().nextBytes(data.array());
      long size = MessageFormat.getCurrentVersionDataSize(2000);
      ByteBuffer sData = ByteBuffer.allocate((int)size);
      MessageFormat.serializeCurrentVersionPartialData(sData, 2000);
      sData.put(data);
      Crc32 crc = new Crc32();
      crc.update(sData.array(), 0, sData.position());
      sData.putLong(crc.getValue());
      sData.flip();
      BlobOutput outputData = MessageFormat.deserializeData(new ByteBufferInputStream(sData));
      Assert.assertEquals(outputData.getSize(), 2000);
      byte[] verify = new byte[2000];
      outputData.getStream().read(verify);
      Assert.assertArrayEquals(verify, data.array());

      // corrupt data
      sData.flip();
      sData.put(10, (byte)10);
      try {
        MessageFormat.deserializeData(new ByteBufferInputStream(sData));
        Assert.assertEquals(true, false);
      }
      catch (MessageFormatException e) {
        Assert.assertEquals(e.getErrorCode(), MessageFormatErrorCodes.Data_Corrupt);
      }
    }
    catch (Exception e) {
      Assert.assertTrue(false);
    }
   }
}
