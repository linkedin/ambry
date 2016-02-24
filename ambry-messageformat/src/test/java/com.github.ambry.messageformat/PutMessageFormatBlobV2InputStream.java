package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * PutMessageFormatInputStream which uses Blob Format Version 2 instead of V1
 * // TODO: Once we switch from Blob format V1 to V2 in actual Put Flow, this class has to switch from V2 to V1
 */
public class PutMessageFormatBlobV2InputStream extends MessageFormatInputStream {

  public PutMessageFormatBlobV2InputStream(StoreKey key, BlobProperties blobProperties, ByteBuffer userMetadata,
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

  public PutMessageFormatBlobV2InputStream(StoreKey key, BlobProperties blobProperties, ByteBuffer userMetadata,
      InputStream blobStream, long streamSize)
      throws MessageFormatException {
    this(key, blobProperties, userMetadata, blobStream, streamSize, BlobType.DataBlob);
  }
}
