package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import java.nio.ByteBuffer;


/**
 * Represents a message that consists of the delete record in version {@link MessageFormatRecord.Update_Format_V3}
 * This format is used to delete a blob
 *
 *  - - - - - - - - - - - - -
 * |     Message Header  V2  |
 *  - - - - - - - - - - - - -
 * |       blob key          |
 *  - - - - - - - - - - - - -
 * |      Update Record      |
 *  - - - - - - - - - - - - -
 *
 */
public class DeleteMessageFormatV3InputStream extends MessageFormatInputStream {
  DeleteMessageFormatV3InputStream(StoreKey key, short accountId, short containerId, long deletionTimeMs)
      throws MessageFormatException {
    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse);
    int deleteRecordSize = MessageFormatRecord.Update_Format_V3.getRecordSize(UpdateRecord.Type.DELETE);
    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + deleteRecordSize);
    if (MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V1) {
      MessageFormatRecord.MessageHeader_Format_V1.serializeHeader(buffer, deleteRecordSize,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    } else {
      MessageFormatRecord.MessageHeader_Format_V2.serializeHeader(buffer, deleteRecordSize,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    }
    buffer.put(key.toBytes());
    // set the message as deleted
    MessageFormatRecord.Update_Format_V3.serialize(buffer,
        new UpdateRecord(accountId, containerId, deletionTimeMs, new DeleteSubRecord()));
    messageLength = buffer.capacity();
    buffer.flip();
  }
}
