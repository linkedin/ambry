package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Utils;

import java.nio.ByteBuffer;

/**
 * Represents a message that consist of the ttl record.
 * This format is used to modify the ttl of a blob.
 *
 *  - - - - - - - - - - - - -
 * |     Message Header      |
 *  - - - - - - - - - - - - -
 * |       blob key          |
 *  - - - - - - - - - - - - -
 * |      TTL Record         |
 *  - - - - - - - - - - - - -
 *
 */
public class TTLMessageFormatInputStream extends MessageFormatInputStream {

  public TTLMessageFormatInputStream(StoreKey key, long timeToLiveInMs) throws MessageFormatException {
    if (timeToLiveInMs < Utils.Infinite_Time) {
      logger.error("Invalid TTL {}", timeToLiveInMs);
      throw new IllegalArgumentException("Invalid TTL " + timeToLiveInMs);
    }

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int ttlRecordSize = MessageFormatRecord.TTL_Format_V1.getTTLRecordSize();
    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + ttlRecordSize);
    MessageFormatRecord.MessageHeader_Format_V1.serializeHeader(buffer,
                                                                ttlRecordSize,
                                                                MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
                                                                headerSize + key.sizeInBytes(),
                                                                MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
                                                                MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
                                                                MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    buffer.put(key.toBytes());
    MessageFormatRecord.TTL_Format_V1.serializeTTLRecord(buffer, timeToLiveInMs);
    messageLength = buffer.capacity();
    buffer.flip();
  }
}
