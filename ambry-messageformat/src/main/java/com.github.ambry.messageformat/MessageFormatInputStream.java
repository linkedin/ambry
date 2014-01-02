package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Crc32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Converts a set of message inputs into the right message format.
 * This provides the base implementation for all types of messages
 * that need to be persisted.
 */
public abstract class MessageFormatInputStream extends InputStream {

  protected ByteBuffer buffer = null;
  protected CrcInputStream stream = null;
  protected long streamLength = 0;
  protected long streamRead = 0;
  protected static int StoreKey_Size_Field_Size_In_Bytes = 2;
  ByteBuffer crc = ByteBuffer.allocate(MessageFormatRecord.Crc_Size);
  protected long messageLength;
  protected Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public int read() throws IOException {
    if (buffer != null && buffer.remaining() > 0) {
      return buffer.get();
    }
    if (stream != null && streamRead < streamLength) {
      streamRead++;
      return stream.read();
    }
    if (stream != null) {
      if (crc.position() == 0) {
        crc.putLong(stream.getValue());
        crc.flip();
      }
      if (crc.remaining() > 0)
        return crc.get();
    }
    return -1;
  }

  // keep reading. the caller will decide when to end
  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int c = read();

    b[off] = (byte)c;

    int i = 1;
    try {
      for (; i < len ; i++) {
        c = read();
        b[off + i] = (byte)c;
      }
    }
    catch (IOException ee) {
    }
    return i;
  }

  public long getSize() {
    return messageLength;
  }
}
