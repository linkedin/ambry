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
 * Represents a TTL update message.
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
public class TtlUpdateMessageFormatInputStream extends MessageFormatInputStream {
  public TtlUpdateMessageFormatInputStream(StoreKey key, short accountId, short containerId, long expiresAtMs,
      long updateTimeInMs) throws MessageFormatException {
    this(key, accountId, containerId, expiresAtMs, updateTimeInMs, (short) 0);
  }

  public TtlUpdateMessageFormatInputStream(StoreKey key, short accountId, short containerId, long expiresAtMs,
      long updateTimeInMs, short lifeVersion) throws MessageFormatException {
    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse);
    int recordSize = MessageFormatRecord.Update_Format_V3.getRecordSize(SubRecord.Type.TTL_UPDATE);
    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + recordSize);
    if (MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V3) {
      MessageFormatRecord.MessageHeader_Format_V3.serializeHeader(buffer, lifeVersion, recordSize,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    } else {
      MessageFormatRecord.MessageHeader_Format_V2.serializeHeader(buffer, recordSize,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
          MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    }
    buffer.put(key.toBytes());
    MessageFormatRecord.Update_Format_V3.serialize(buffer,
        new UpdateRecord(accountId, containerId, updateTimeInMs, new TtlUpdateSubRecord(expiresAtMs)));
    messageLength = buffer.capacity();
    buffer.flip();
  }
}
