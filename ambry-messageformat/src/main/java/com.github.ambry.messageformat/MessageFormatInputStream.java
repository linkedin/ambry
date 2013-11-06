package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Crc32;

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
    int idSize = StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes();
    buffer = ByteBuffer.allocate(headerSize +
                                 idSize +
                                 systemMetadataSize +
                                 userMetadataSize +
                                 (int)(dataSize - streamSize - MessageFormat.Crc_Size));

    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize + userMetadataSize + dataSize,
                                                headerSize + idSize,
                                                headerSize + idSize + systemMetadataSize,
                                                headerSize + idSize + systemMetadataSize + userMetadataSize);
    buffer.putInt(key.sizeInBytes());
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionBlobPropertyRecord(buffer, blobProperty);
    MessageFormat.serializeCurrentVersionUserMetadata(buffer, userMetadata);
    int bufferDataStart = buffer.position();
    MessageFormat.serializeCurrentVersionPartialData(buffer, streamSize);
    Crc32 crc = new Crc32();
    crc.update(buffer.array(), bufferDataStart, buffer.position() - bufferDataStart);
    stream = new CrcInputStream(crc, data);
    streamLength = streamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormat.Crc_Size;
    buffer.flip();
  }

  public MessageFormatInputStream(StoreKey key, boolean deleteFlag) {
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionDeleteRecordSize();
    int idSize = StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes();
    buffer = ByteBuffer.allocate(headerSize +
                                 idSize +
                                 systemMetadataSize);
    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize,
                                                headerSize + idSize,
                                                -1,
                                                -1);
    buffer.putInt(key.sizeInBytes());
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionDeleteRecord(buffer, deleteFlag);
    messageLength = buffer.capacity();
    buffer.flip();
  }

  public MessageFormatInputStream(StoreKey key, long ttl) {
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionTTLRecordSize();
    int idSize = StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes();
    buffer = ByteBuffer.allocate(headerSize +
                                 idSize +
                                 systemMetadataSize);
    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize,
                                                headerSize + idSize,
                                                -1,
                                                -1);
    buffer.putInt(key.sizeInBytes());
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionTTLRecord(buffer, ttl);
    messageLength = buffer.capacity();
    buffer.flip();
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
      if (crc.position() == 0) {
        crc.putLong(stream.getValue());
        crc.flip();
      }
      if (crc.remaining() > 0)
        return crc.get();
    }
    return -1;
  }

  // keep reading. the caller will decide when to end
  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int c = read();

    b[off] = (byte)c;

    int i = 1;
    try {
      for (; i < len ; i++) {
        c = read();
        b[off + i] = (byte)c;
      }
    }
    catch (IOException ee) {
    }
    return i;
  }

  public long getSize() {
    return messageLength;
  }
}
