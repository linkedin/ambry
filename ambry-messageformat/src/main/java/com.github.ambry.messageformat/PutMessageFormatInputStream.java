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
 * Represents a message that consist of the blob, blob properties and user metadata.
 * This format is used to put a new blob into the store
 *
 *  - - - - - - - - - - - - - -
 * |     Message Header        |
 *  - - - - - - - - - - - - - -
 * |       blob key            |
 *  - - - - - - - - - - - - - -
 * |  Blob Properties Record   |
 *  - - - - - - - - - - - - - -
 * |  User metadata Record     |
 *  - - - - - - - - - - - - - -
 * |       Blob Record         |
 *  - - - - - - - - - - - - - -
 */
public class PutMessageFormatInputStream extends MessageFormatInputStream {

  public PutMessageFormatInputStream(StoreKey key, BlobProperties blobProperties, ByteBuffer userMetadata,
      InputStream blobStream, long streamSize, BlobType blobType)
      throws MessageFormatException {
    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize =
        MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties);
    int userMetadataSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
    long blobSize = MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(streamSize);

    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + blobPropertiesRecordSize + userMetadataSize +
        (int) (blobSize - streamSize - MessageFormatRecord.Crc_Size));

    MessageFormatRecord.MessageHeader_Format_V1
        .serializeHeader(buffer, blobPropertiesRecordSize + userMetadataSize + blobSize, headerSize + key.sizeInBytes(),
            MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
            headerSize + key.sizeInBytes() + blobPropertiesRecordSize,
            headerSize + key.sizeInBytes() + blobPropertiesRecordSize + userMetadataSize);
    buffer.put(key.toBytes());
    MessageFormatRecord.BlobProperties_Format_V1.serializeBlobPropertiesRecord(buffer, blobProperties);
    MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(buffer, userMetadata);
    int bufferBlobStart = buffer.position();
    MessageFormatRecord.Blob_Format_V2.serializePartialBlobRecord(buffer, streamSize, blobType);
    Crc32 crc = new Crc32();
    crc.update(buffer.array(), bufferBlobStart, buffer.position() - bufferBlobStart);
    stream = new CrcInputStream(crc, blobStream);
    streamLength = streamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormatRecord.Crc_Size;
    buffer.flip();
  }

  public PutMessageFormatInputStream(StoreKey key, BlobProperties blobProperties, ByteBuffer userMetadata,
      InputStream blobStream, long streamSize)
      throws MessageFormatException {
    this(key, blobProperties, userMetadata, blobStream, streamSize, BlobType.DataBlob);
  }
}
