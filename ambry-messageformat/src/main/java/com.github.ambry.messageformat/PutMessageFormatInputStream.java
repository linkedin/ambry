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
 *
 * TODO rename blob property to blob properties across code base
 */
public class PutMessageFormatInputStream extends MessageFormatInputStream {

  public PutMessageFormatInputStream(StoreKey key, BlobProperties blobProperty, ByteBuffer userMetadata,
      InputStream data, long streamSize)
      throws MessageFormatException {

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertyRecordSize = MessageFormatRecord.BlobProperty_Format_V1.getBlobPropertyRecordSize(blobProperty);
    int userMetadataSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
    long blobSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(streamSize);

    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + blobPropertyRecordSize + userMetadataSize +
        (int) (blobSize - streamSize - MessageFormatRecord.Crc_Size));

    MessageFormatRecord.MessageHeader_Format_V1
        .serializeHeader(buffer, blobPropertyRecordSize + userMetadataSize + blobSize, headerSize + key.sizeInBytes(),
            MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
            MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
            headerSize + key.sizeInBytes() + blobPropertyRecordSize,
            headerSize + key.sizeInBytes() + blobPropertyRecordSize + userMetadataSize);
    buffer.put(key.toBytes());
    MessageFormatRecord.BlobProperty_Format_V1.serializeBlobPropertyRecord(buffer, blobProperty);
    MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(buffer, userMetadata);
    int bufferBlobStart = buffer.position();
    MessageFormatRecord.Blob_Format_V1.serializePartialBlobRecord(buffer, streamSize);
    Crc32 crc = new Crc32();
    crc.update(buffer.array(), bufferBlobStart, buffer.position() - bufferBlobStart);
    stream = new CrcInputStream(crc, data);
    streamLength = streamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormatRecord.Crc_Size;
    buffer.flip();
  }
}
