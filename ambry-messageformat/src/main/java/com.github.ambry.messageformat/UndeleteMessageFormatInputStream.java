package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import java.nio.ByteBuffer;


/**
 * Represents a Undelete message.
 *
 *  - - - - - - - - - - - - -
 * |     Message Header      |
 *  - - - - - - - - - - - - -
 * |       blob key          |
 *  - - - - - - - - - - - - -
 * |     update Record       |
 *  - - - - - - - - - - - - -
 *
 */
public class UndeleteMessageFormatInputStream extends MessageFormatInputStream {
  public UndeleteMessageFormatInputStream(StoreKey key, short accountId, short containerId,
      long updateTimeInMs, short lifeVersion) throws MessageFormatException {
    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse);
    int recordSize = MessageFormatRecord.Update_Format_V3.getRecordSize(SubRecord.Type.UNDELETE);
    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + recordSize);
    MessageFormatRecord.MessageHeader_Format_V3.serializeHeader(buffer, lifeVersion, recordSize,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    buffer.put(key.toBytes());
    MessageFormatRecord.Update_Format_V3.serialize(buffer,
        new UpdateRecord(accountId, containerId, updateTimeInMs, new UndeleteSubRecord()));
    messageLength = buffer.capacity();
    buffer.flip();
  }
}