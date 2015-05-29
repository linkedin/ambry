package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ZeroBytesInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Represents a message that consist of the blob properties with the user metadata and blob
 * zeroed out.
 * This format is used to replace a put record with a hard deleted blob into the store
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
public class HardDeleteMessageFormatInputStream extends PutMessageFormatInputStream {

  public HardDeleteMessageFormatInputStream(StoreKey key, BlobProperties blobProperties, int userMetadataSize,
      int blobStreamSize)
      throws MessageFormatException, IOException {
    super(key, blobProperties, ByteBuffer.allocate(userMetadataSize),
        new ZeroBytesInputStream(blobStreamSize), blobStreamSize);
  }
}
