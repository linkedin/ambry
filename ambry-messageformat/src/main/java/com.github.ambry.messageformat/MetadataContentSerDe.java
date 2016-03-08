package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * A class to serialize and deserialize MetadataContent which forms the content of a Metadata Blob.
 */
public class MetadataContentSerDe {
  /**
   * Serialize the input list of keys that form the metadata content.
   * @param keys the input list of keys that form the metadata content.
   * @return a ByteBuffer containing the serialized output.
   */
  public static ByteBuffer serializeMetadataContent(List<StoreKey> keys) {
    int bufSize =
        MessageFormatRecord.Metadata_Content_Format_V1.getMetadataContentSize(keys.get(0).sizeInBytes(), keys.size());
    ByteBuffer outputBuf = ByteBuffer.allocate(bufSize);
    MessageFormatRecord.Metadata_Content_Format_V1.serializeMetadataContentRecord(outputBuf, keys);
    return outputBuf;
  }

  /**
   * Deserialize the serialized metadata content in the input ByteBuffer using the given {@link StoreKeyFactory} as a
   * reference.
   * @param buf ByteBuffer containing the serialized metadata content.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use to deserialize the content.
   * @return a list of {@link StoreKey} containing the deserialized output.
   * @throws IOException if an IOException is encountered during deserialization.
   * @throws MessageFormatException if an unknown version is encountered in the header of the serialized input.
   */
  public static List<StoreKey> deserializeMetadataContentRecord(ByteBuffer buf, StoreKeyFactory storeKeyFactory)
      throws IOException, MessageFormatException {
    int version = buf.getShort();
    switch (version) {
      case MessageFormatRecord.Message_Header_Version_V1:
        return MessageFormatRecord.Metadata_Content_Format_V1
            .deserializeMetadataContentRecord(new DataInputStream(new ByteBufferInputStream(buf)), storeKeyFactory);
      default:
        throw new MessageFormatException("Unknown version encountered for MetadataContent: " + version,
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }
}
