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
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * PutMessageFormatInputStream which uses Blob Format V1 instead of the default V2
 */
class PutMessageFormatBlobV1InputStream extends MessageFormatInputStream {
  PutMessageFormatBlobV1InputStream(StoreKey key, BlobProperties blobProperties, ByteBuffer userMetadata,
      InputStream blobStream, long streamSize, BlobType blobType) throws MessageFormatException {
    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize =
        MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties);
    int userMetadataSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
    long blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(streamSize);

    buffer = ByteBuffer.allocate(
        headerSize + key.sizeInBytes() + blobPropertiesRecordSize + userMetadataSize + (int) (blobSize - streamSize
            - MessageFormatRecord.Crc_Size));

    MessageFormatRecord.MessageHeader_Format_V1.serializeHeader(buffer,
        blobPropertiesRecordSize + userMetadataSize + blobSize, headerSize + key.sizeInBytes(),
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
        headerSize + key.sizeInBytes() + blobPropertiesRecordSize,
        headerSize + key.sizeInBytes() + blobPropertiesRecordSize + userMetadataSize);
    buffer.put(key.toBytes());
    MessageFormatRecord.BlobProperties_Format_V1.serializeBlobPropertiesRecord(buffer, blobProperties);
    MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(buffer, userMetadata);
    int bufferBlobStart = buffer.position();
    MessageFormatRecord.Blob_Format_V1.serializePartialBlobRecord(buffer, streamSize);
    Crc32 crc = new Crc32();
    crc.update(buffer.array(), bufferBlobStart, buffer.position() - bufferBlobStart);
    stream = new CrcInputStream(crc, blobStream);
    streamLength = streamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormatRecord.Crc_Size;
    buffer.flip();
  }
}

/**
 * Represents a message that consist of the delete record in version {@link MessageFormatRecord.Delete_Format_V1}
 * This format is used to delete a blob
 *
 *  - - - - - - - - - - - - -
 * |     Message Header  V1  |
 *  - - - - - - - - - - - - -
 * |       blob key          |
 *  - - - - - - - - - - - - -
 * |      Delete Record      |
 *  - - - - - - - - - - - - -
 *
 */
class DeleteMessageFormatV1InputStream extends MessageFormatInputStream {
  DeleteMessageFormatV1InputStream(StoreKey key, short accountId, short containerId, long deletionTimeMs)
      throws MessageFormatException {
    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int deleteRecordSize = MessageFormatRecord.Delete_Format_V1.getDeleteRecordSize();
    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + deleteRecordSize);
    MessageFormatRecord.MessageHeader_Format_V1.serializeHeader(buffer, deleteRecordSize,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    buffer.put(key.toBytes());
    // set the message as deleted
    MessageFormatRecord.Delete_Format_V1.serializeDeleteRecord(buffer,
        new DeleteRecord(accountId, containerId, deletionTimeMs));
    messageLength = buffer.capacity();
    buffer.flip();
  }
}

