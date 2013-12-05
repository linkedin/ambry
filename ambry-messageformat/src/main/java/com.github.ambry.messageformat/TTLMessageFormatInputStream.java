package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;

import java.nio.ByteBuffer;

/**
 * The message format input stream that helps to serialize and write a
 * ttl record to the underlying store
 */
public class TTLMessageFormatInputStream extends MessageFormatInputStream {

  public TTLMessageFormatInputStream(StoreKey key, long timeToLiveInMs) {
    if (timeToLiveInMs < 0) {
      logger.error("Invalid TTL {}", timeToLiveInMs);
      throw new IllegalArgumentException("Invalid TTL " + timeToLiveInMs);
    }

    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionTTLRecordSize();
    int idSize = StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes();
    buffer = ByteBuffer.allocate(headerSize + idSize + systemMetadataSize);
    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize,
                                                headerSize + idSize,
                                                MessageFormat.Message_Header_Invalid_Relative_Offset,
                                                MessageFormat.Message_Header_Invalid_Relative_Offset);
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionTTLRecord(buffer, timeToLiveInMs);
    messageLength = buffer.capacity();
    buffer.flip();
  }
}
