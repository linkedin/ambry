package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;

import java.nio.ByteBuffer;

/**
 * The message format input stream that helps to serialize and write a delete
 * record to the underlying store
 */
public class DeleteMessageFormatInputStream extends MessageFormatInputStream {
  public DeleteMessageFormatInputStream(StoreKey key) {
    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionDeleteRecordSize();
    int idSize = StoreKey_Size_Field_Size_In_Bytes + key.sizeInBytes();
    buffer = ByteBuffer.allocate(headerSize + idSize + systemMetadataSize);
    MessageFormat.serializeCurrentVersionHeader(buffer,
                                                systemMetadataSize,
                                                headerSize + idSize,
                                                MessageFormat.Message_Header_Invalid_Relative_Offset,
                                                MessageFormat.Message_Header_Invalid_Relative_Offset);
    buffer.put(key.toBytes());
    // set the message as deleted
    MessageFormat.serializeCurrentVersionDeleteRecord(buffer, true);
    messageLength = buffer.capacity();
    buffer.flip();
  }
}
