package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.CrcInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Converts a set of message inputs into the right message format
 */
public class MessageFormatInputStream extends InputStream {

  private ByteBuffer buffer = null;
  private CrcInputStream stream = null;
  private long streamLength = 0;
  private long streamRead = 0;
  private static int StoreKey_Size_Field_Size_In_Bytes = 4;
  ByteBuffer crc = ByteBuffer.allocate(MessageFormat.Crc_Size);
  private long messageLength;

  public MessageFormatInputStream(StoreKey key, BlobProperties blobProperty,
                                  ByteBuffer userMetadata, InputStream data,
                                  long streamSize) {

    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionBlobPropertyRecordSize(blobProperty);
    int userMetadataSize = MessageFormat.getCurrentVersionUserMetadataSize(userMetadata);
    long dataSize = MessageFormat.getCurrentVersionDataSize(streamSize);
    buffer = ByteBuffer.allocate(headerSize +
                                 StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes() +
                                 systemMetadataSize +
                                 userMetadataSize +
                                 (int)(dataSize - streamSize - MessageFormat.Crc_Size));

    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize + userMetadataSize + dataSize,
                                                headerSize,
                                                headerSize + systemMetadataSize,
                                                headerSize + systemMetadataSize + userMetadataSize);
    buffer.putInt(key.sizeInBytes());
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionBlobPropertyRecord(buffer, blobProperty);
    MessageFormat.serializeCurrentVersionUserMetadata(buffer, userMetadata);
    MessageFormat.serializeCurrentVersionPartialData(buffer, streamSize);

    stream = new CrcInputStream(data);
    streamLength = streamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormat.Crc_Size;
  }

  public MessageFormatInputStream(StoreKey key, boolean deleteFlag) {
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionDeleteRecordSize();
    buffer = ByteBuffer.allocate(headerSize +
                                 StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes() +
                                 systemMetadataSize);
    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize,
                                                headerSize,
                                                -1,
                                                -1);
    buffer.putInt(key.sizeInBytes());
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionDeleteRecord(buffer, deleteFlag);
    messageLength = buffer.capacity();
  }

  public MessageFormatInputStream(StoreKey key, long ttl) {
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionTTLRecordSize();
    buffer = ByteBuffer.allocate(headerSize +
                                 StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes() +
                                 systemMetadataSize);
    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize,
                                                headerSize,
                                                -1,
                                                -1);
    buffer.putInt(key.sizeInBytes());
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionTTLRecord(buffer, ttl);
    messageLength = buffer.capacity();
  }

  @Override
  public int read() throws IOException {
    if (buffer != null && buffer.remaining() > 0) {
      return buffer.get();
    }
    if (stream != null && streamRead < streamLength) {
      streamRead++;
      return stream.read();
    }
    if (stream != null) {
      if (crc.limit() == 0) {
        crc.putLong(stream.getValue());
      }
      if (crc.remaining() > 0)
        return crc.get();
    }
    return -1;
  }

  public long getSize() {
    return messageLength;
  }
}
