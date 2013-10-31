package com.github.ambry.messageformat;

import com.github.ambry.shared.BlobId;
import java.nio.ByteBuffer;

/**
 * Represents the message format of the data that gets written to the store
 */
public class MessageFormat {
  public static class MessageHeader_V1 {
    ByteBuffer buffer;
    public static final int Message_Header_Size_V1 = 24 + BlobId.size;

    public MessageHeader_V1(ByteBuffer input) {
      this.buffer = input;
    }

    public long getSize() {
      return buffer.getLong(0);
    }

    public BlobId getId() {
      byte [] buf = new byte[BlobId.size];
      buffer.position(8);
      buffer.get(buf);
      return new BlobId(new String(buf));
    }

    public int getSystemMetadataOffset() {
      return buffer.getInt(8 + BlobId.size);
    }

    public int getUserMetadataOffset() {
      return buffer.getInt(8 + BlobId.size + 4);
    }

    public int getDataOffset() {
      return buffer.getInt(8 + BlobId.size + 4 + 4);
    }

    public int getCrc() {
      return buffer.getInt(8 + BlobId.size + 4 + 4 + 4);
    }
  }
}
