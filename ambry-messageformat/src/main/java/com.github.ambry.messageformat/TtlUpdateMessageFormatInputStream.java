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
import com.github.ambry.utils.Utils;
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
  public TtlUpdateMessageFormatInputStream(StoreKey key, short accountId, short containerId, long updateTimeInMs)
      throws MessageFormatException {
    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse);
    int recordSize = MessageFormatRecord.Update_Format_V3.getRecordSize(UpdateRecord.Type.TTL_UPDATE);
    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + recordSize);
    MessageFormatRecord.MessageHeader_Format_V2.serializeHeader(buffer, recordSize,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    buffer.put(key.toBytes());
    MessageFormatRecord.Update_Format_V3.serialize(buffer,
        new UpdateRecord(accountId, containerId, updateTimeInMs, new TtlUpdateSubRecord(Utils.Infinite_Time)));
    messageLength = buffer.capacity();
    buffer.flip();
  }
}
