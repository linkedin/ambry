/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import java.nio.ByteBuffer;


/**
 * Represents a message that consists of the delete record.
 * This format is used to delete a blob
 *
 *  - - - - - - - - - - - - -
 * |     Message Header      |
 *  - - - - - - - - - - - - -
 * |       blob key          |
 *  - - - - - - - - - - - - -
 * |      Update Record      |
 *  - - - - - - - - - - - - -
 *
 */
public class DeleteMessageFormatInputStream extends MessageFormatInputStream {
  private static final int HEADER_SIZE;
  private static final int DELETE_RECORD_SIZE;

  static {
    try {
      HEADER_SIZE = MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get Header size for version " + MessageFormatRecord.headerVersionToUse, e);
    }
    DELETE_RECORD_SIZE = MessageFormatRecord.Update_Format_V3.getRecordSize(SubRecord.Type.DELETE);
  }

  public DeleteMessageFormatInputStream(StoreKey key, short accountId, short containerId, long deletionTimeMs)
      throws MessageFormatException {
    this(key, accountId, containerId, deletionTimeMs, (short) 0);
  }

  public DeleteMessageFormatInputStream(StoreKey key, short accountId, short containerId, long deletionTimeMs,
      short lifeVersion) throws MessageFormatException {
    buffer = ByteBuffer.allocate(HEADER_SIZE + key.sizeInBytes() + DELETE_RECORD_SIZE);

    if (MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V1) {
      MessageFormatRecord.MessageHeader_Format_V1.serializeHeader(buffer, DELETE_RECORD_SIZE,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset, HEADER_SIZE + key.sizeInBytes(),
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    } else if (MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V2) {
      MessageFormatRecord.MessageHeader_Format_V2.serializeHeader(buffer, DELETE_RECORD_SIZE,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset, HEADER_SIZE + key.sizeInBytes(),
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    } else {
      MessageFormatRecord.MessageHeader_Format_V3.serializeHeader(buffer, lifeVersion, DELETE_RECORD_SIZE,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset, HEADER_SIZE + key.sizeInBytes(),
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

  /**
   * Return the {@link DeleteMessageFormatInputStream} size for the given {@code key}.
   * @param key The {@link StoreKey}
   * @return The size of {@link DeleteMessageFormatInputStream}
   */
  public static long getDeleteMessageFormatInputStreamSize(StoreKey key) {
    return HEADER_SIZE + key.sizeInBytes() + DELETE_RECORD_SIZE;
  }
}
