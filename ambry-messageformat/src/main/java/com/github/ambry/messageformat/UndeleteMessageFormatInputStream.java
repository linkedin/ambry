/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 * Represents a message that consists of the undelete record.
 * This format is used to undelete a blob. Message Header will start at least at version 3.
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
public class UndeleteMessageFormatInputStream extends MessageFormatInputStream {
  public static final short UNDELETE_MESSAGE_HEADER_VERSION = MessageFormatRecord.Message_Header_Version_V3;

  public UndeleteMessageFormatInputStream(StoreKey key, short accountId, short containerId, long updateTimeInMs,
      short lifeVersion) throws MessageFormatException {
    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(UNDELETE_MESSAGE_HEADER_VERSION);
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
