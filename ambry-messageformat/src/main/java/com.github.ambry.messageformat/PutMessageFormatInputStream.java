package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.CrcInputStream;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * The message format input stream that helps to serialize and write a new
 * message to the underlying store
 */
public class PutMessageFormatInputStream extends MessageFormatInputStream {

  public PutMessageFormatInputStream(StoreKey key, BlobProperties blobProperty,
                                     ByteBuffer userMetadata, InputStream data,
                                     long streamSize) {

    int headerSize = MessageFormat.getCurrentVersionHeaderSize();
    int systemMetadataSize = MessageFormat.getCurrentVersionBlobPropertyRecordSize(blobProperty);
    int userMetadataSize = MessageFormat.getCurrentVersionUserMetadataSize(userMetadata);
    long dataSize = MessageFormat.getCurrentVersionDataSize(streamSize);
    buffer = ByteBuffer.allocate(headerSize +
            key.sizeInBytes() +
            systemMetadataSize +
            userMetadataSize +
            (int)(dataSize - streamSize - MessageFormat.Crc_Size));

    MessageFormat.serializeCurrentVersionHeader(buffer,
            systemMetadataSize + userMetadataSize + dataSize,
            headerSize + key.sizeInBytes(),
            headerSize + key.sizeInBytes() + systemMetadataSize,
            headerSize + key.sizeInBytes() + systemMetadataSize + userMetadataSize);
    buffer.put(key.toBytes());
    MessageFormat.serializeCurrentVersionBlobPropertyRecord(buffer, blobProperty);
    MessageFormat.serializeCurrentVersionUserMetadata(buffer, userMetadata);
    int bufferDataStart = buffer.position();
    MessageFormat.serializeCurrentVersionPartialData(buffer, streamSize);
    Crc32 crc = new Crc32();
    crc.update(buffer.array(), bufferDataStart, buffer.position() - bufferDataStart);
    stream = new CrcInputStream(crc, data);
    streamLength = streamSize;
    messageLength = buffer.capacity() + streamLength + MessageFormat.Crc_Size;
    buffer.flip();
  }
}
