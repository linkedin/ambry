package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
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
   * Creates a hard delete stream using the given parameters to replace the usermetadata and blob record fields. The
   * method takes the blobProperties field as a parameter, but that is just used to generate the stream.
   * @param key the store key of the given blob.
   * @param headerVersion the version of the header.
   * @param blobProperties the blobProperties fo the given blob. This is used to find the relative offsets of other fields.
   * @param userMetadataVersion the version of the userMetadata.
   * @param userMetadataSize the size of the userMetadata field.
   * @param blobRecordVersion the version of the blob record.
   * @param blobStreamSize the size of the blob stream.
   * @throws MessageFormatException
   * @throws IOException
   */
  public HardDeleteMessageFormatInputStream(StoreKey key, short headerVersion, BlobProperties blobProperties,
      short userMetadataVersion, int userMetadataSize, short blobRecordVersion, long blobStreamSize)
      throws MessageFormatException, IOException {

    InputStream blobStream = new ZeroBytesInputStream(blobStreamSize);
    ByteBuffer userMetadata = ByteBuffer.allocate(userMetadataSize);

    if (headerVersion != MessageFormatRecord.Message_Header_Version_V1 ||
        userMetadataVersion != MessageFormatRecord.UserMetadata_Version_V1 ||
        blobRecordVersion != MessageFormatRecord.Blob_Version_V1) {
      throw new MessageFormatException("Unknown version encountered when creating hard delete stream",
          MessageFormatErrorCodes.Unknown_Format_Version);
    }

    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int blobPropertiesRecordSize =
        MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties);
    int userMetadataRecordSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
    long blobRecordSize = MessageFormatRecord.Blob_Format_V1.getBlobRecordSize(blobStreamSize);

    hardDeleteStreamRelativeOffset = headerSize + key.sizeInBytes() + blobPropertiesRecordSize;

    buffer = ByteBuffer
        .allocate(userMetadataRecordSize + (int) (blobRecordSize - blobStreamSize - MessageFormatRecord.Crc_Size));

    MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(buffer, userMetadata);
    int bufferBlobStart = buffer.position();
    MessageFormatRecord.Blob_Format_V1.serializePartialBlobRecord(buffer, blobStreamSize);
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
