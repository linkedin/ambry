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
 * |     Encryption key        |
 *  - - - - - - - - - - - - - -
 * |  Blob Properties Record   |
 *  - - - - - - - - - - - - - -
 * |  User metadata Record     |
 *  - - - - - - - - - - - - - -
 * |       Blob Record         |
 *  - - - - - - - - - - - - - -
 */
public class PutMessageFormatInputStream extends MessageFormatInputStream {

  public PutMessageFormatInputStream(StoreKey key, ByteBuffer blobEncryptionKey, BlobProperties blobProperties,
      ByteBuffer userMetadata, InputStream blobStream, long streamSize, BlobType blobType)
      throws MessageFormatException {
    this(key, blobEncryptionKey, blobProperties, userMetadata, blobStream, streamSize, blobType, (short) 0);
  }

  public PutMessageFormatInputStream(StoreKey key, ByteBuffer blobEncryptionKey, BlobProperties blobProperties,
      ByteBuffer userMetadata, InputStream blobStream, long streamSize) throws MessageFormatException {
    this(key, blobEncryptionKey, blobProperties, userMetadata, blobStream, streamSize, BlobType.DataBlob, (short) 0);
  }

  public PutMessageFormatInputStream(StoreKey key, ByteBuffer blobEncryptionKey, BlobProperties blobProperties,
      ByteBuffer userMetadata, InputStream blobStream, long streamSize, BlobType blobType, short lifeVersion)
      throws MessageFormatException {
    if (MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V1) {
      createStreamWithMessageHeaderV1(key, blobProperties, userMetadata, blobStream, streamSize, blobType);
    } else {
      createStreamWithMessageHeader(key, blobEncryptionKey, blobProperties, userMetadata, blobStream, streamSize,
          blobType, lifeVersion);
    }
  }

  /**
   * Helper method to create a stream with encryption key record. This will be the standard once all nodes in a cluster
   * understand reading messages with encryption key record.
   */
  private void createStreamWithMessageHeader(StoreKey key, ByteBuffer blobEncryptionKey, BlobProperties blobProperties,
      ByteBuffer userMetadata, InputStream blobStream, long streamSize, BlobType blobType, short lifeVersion)
      throws MessageFormatException {
    int headerSize = MessageFormatRecord.getHeaderSizeForVersion(MessageFormatRecord.headerVersionToUse);
    int blobEncryptionKeySize = blobEncryptionKey == null ? 0
        : MessageFormatRecord.BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(blobEncryptionKey);
    int blobPropertiesRecordSize =
        MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties);
    int userMetadataSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
    long blobSize = MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(streamSize);

    buffer = ByteBuffer.allocate(
        headerSize + key.sizeInBytes() + blobEncryptionKeySize + blobPropertiesRecordSize + userMetadataSize + (int) (
            blobSize - streamSize - MessageFormatRecord.Crc_Size));

    long totalSize = blobEncryptionKeySize + blobPropertiesRecordSize + userMetadataSize + blobSize;
    int blobEncryptionKeyRecordRelativeOffset =
        blobEncryptionKey == null ? MessageFormatRecord.Message_Header_Invalid_Relative_Offset
            : headerSize + key.sizeInBytes();
    int blobPropertiesRecordRelativeOffset = blobEncryptionKey == null ? headerSize + key.sizeInBytes()
        : blobEncryptionKeyRecordRelativeOffset + blobEncryptionKeySize;
    int updateRecordRelativeOffset = MessageFormatRecord.Message_Header_Invalid_Relative_Offset;
    int userMetadataRecordRelativeOffset = blobPropertiesRecordRelativeOffset + blobPropertiesRecordSize;
    int blobRecordRelativeOffset = userMetadataRecordRelativeOffset + userMetadataSize;
    if (MessageFormatRecord.headerVersionToUse == MessageFormatRecord.Message_Header_Version_V2) {
      MessageFormatRecord.MessageHeader_Format_V2.serializeHeader(buffer, totalSize,
          blobEncryptionKeyRecordRelativeOffset, blobPropertiesRecordRelativeOffset, updateRecordRelativeOffset,
          userMetadataRecordRelativeOffset, blobRecordRelativeOffset);
    } else {
      MessageFormatRecord.MessageHeader_Format_V3.serializeHeader(buffer, lifeVersion, totalSize,
          blobEncryptionKeyRecordRelativeOffset, blobPropertiesRecordRelativeOffset, updateRecordRelativeOffset,
          userMetadataRecordRelativeOffset, blobRecordRelativeOffset);
    }
    buffer.put(key.toBytes());
    if (blobEncryptionKey != null) {
      MessageFormatRecord.BlobEncryptionKey_Format_V1.serializeBlobEncryptionKeyRecord(buffer, blobEncryptionKey);
    }
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

  /**
   * Helper method to create a stream without encryption key record. This is the default currently, but once all nodes
   * once all nodes in a cluster understand reading messages with encryption key record, and writing in the new format
   * is enabled, this method can be removed.
   */
  private void createStreamWithMessageHeaderV1(StoreKey key, BlobProperties blobProperties, ByteBuffer userMetadata,
      InputStream blobStream, long streamSize, BlobType blobType) throws MessageFormatException {
    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize =
        MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties);
    int userMetadataSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
    long blobSize = MessageFormatRecord.Blob_Format_V2.getBlobRecordSize(streamSize);

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
    MessageFormatRecord.Blob_Format_V2.serializePartialBlobRecord(buffer, streamSize, blobType);
    Crc32 crc = new Crc32();
    crc.update(buffer.array(), bufferBlobStart, buffer.position() - bufferBlobStart);
    stream = new CrcInputStream(crc, blobStream);
    streamLength = streamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormatRecord.Crc_Size;
    buffer.flip();
  }
}
