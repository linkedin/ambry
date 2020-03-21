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

import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.ZeroBytesInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * Represents a message that consist of just the user metadata and blob content. Additionally, these fields are
 * zeroed out.
 * This format is used to replace a put record's user metadata and blob content part as part of hard deleting it.
 * The usermetadata and blob record versions of the replacement stream will have to be the same as the versions in
 * the original put record.
 *
 *  - - - - - - - - - - - - - - - - - - -
 * |           Message Header            |
 *  - - - - - - - - - - - - - - - - - - -
 * |              blob key               |
 *  - - - - - - - - - - - - - - - - - - -
 * |       Blob Properties Record        |
 *  - - - - - - - - - - - - - - - - - - -
 * |  User metadata Record (Zeroed out)  |
 *  - - - - - - - - - - - - - - - - - - -
 * |       Blob Record (Zeroed out)      |
 *  - - - - - - - - - - - - - - - - - - -
 */
public class HardDeleteMessageFormatInputStream extends MessageFormatInputStream {

  private int hardDeleteStreamRelativeOffset;

  /**
   * Creates a hard delete stream using the given parameters to replace the usermetadata and blob record fields.
   * @param userMetadataRelativeOffset the relative offset of userMetadata.
   * @param userMetadataVersion the version of the userMetadata.
   * @param userMetadataSize the size of the userMetadata field.
   * @param blobRecordVersion the version of the blob record.
   * @param blobType {@link BlobType} of the blob
   * @param blobStreamSize the size of the blob stream.
   * @throws MessageFormatException
   * @throws IOException
   */
  public HardDeleteMessageFormatInputStream(int userMetadataRelativeOffset, short userMetadataVersion,
      int userMetadataSize, short blobRecordVersion, BlobType blobType, long blobStreamSize)
      throws MessageFormatException, IOException {

    ByteBuffer userMetadata = ByteBuffer.allocate(userMetadataSize);
    InputStream blobStream = new ZeroBytesInputStream(blobStreamSize);

    hardDeleteStreamRelativeOffset = userMetadataRelativeOffset;

    int userMetadataRecordSize;
    ByteBuffer serializedUserMetadata;
    switch (userMetadataVersion) {
      case MessageFormatRecord.UserMetadata_Version_V1:
        userMetadataRecordSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
        serializedUserMetadata = ByteBuffer.allocate(userMetadataRecordSize);
        MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(serializedUserMetadata, userMetadata);
        serializedUserMetadata.flip();
        break;
      default:
        throw new MessageFormatException("Unknown version encountered when creating hard delete stream",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }

    long blobRecordSize;
    ByteBuffer serializedBlobPartialRecord;
    switch (blobRecordVersion) {
      case MessageFormatRecord.Blob_Version_V1:
        blobRecordSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blobStreamSize);
        serializedBlobPartialRecord =
            ByteBuffer.allocate((int) (blobRecordSize - blobStreamSize - MessageFormatRecord.Crc_Size));
        MessageFormatRecord.Blob_Format_V1.serializePartialBlobRecord(serializedBlobPartialRecord, blobStreamSize);
        serializedBlobPartialRecord.flip();
        break;
      case MessageFormatRecord.Blob_Version_V2:
        blobRecordSize = MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(blobStreamSize);
        serializedBlobPartialRecord =
            ByteBuffer.allocate((int) (blobRecordSize - blobStreamSize - MessageFormatRecord.Crc_Size));
        MessageFormatRecord.Blob_Format_V2.serializePartialBlobRecord(serializedBlobPartialRecord, blobStreamSize,
            blobType);
        serializedBlobPartialRecord.flip();
        break;
      default:
        throw new MessageFormatException("Unknown version encountered when creating hard delete stream",
            MessageFormatErrorCodes.Unknown_Format_Version);
    }

    buffer = ByteBuffer.allocate(
        userMetadataRecordSize + (int) (blobRecordSize - blobStreamSize - MessageFormatRecord.Crc_Size));

    buffer.put(serializedUserMetadata);
    int bufferBlobStart = buffer.position();
    buffer.put(serializedBlobPartialRecord);
    Crc32 crc = new Crc32();
    crc.update(buffer.array(), bufferBlobStart, buffer.position() - bufferBlobStart);
    stream = new CrcInputStream(crc, blobStream);
    streamLength = blobStreamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormatRecord.Crc_Size;
    buffer.flip();
  }

  /**
   * @return The relative offset of the original message that corresponds to this hard delete stream.
   */
  public int getHardDeleteStreamRelativeOffset() {
    return hardDeleteStreamRelativeOffset;
  }
}
