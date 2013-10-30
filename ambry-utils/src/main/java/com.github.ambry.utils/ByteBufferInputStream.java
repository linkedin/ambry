package com.github.ambry.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
  private ByteBuffer byteBuffer;

  public ByteBufferInputStream (ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  public int read () throws IOException {
    if (!byteBuffer.hasRemaining()) return -1;
      return byteBuffer.get();
  }

  public int read (byte[] bytes, int offset, int length) throws IOException {
    int count = Math.min(byteBuffer.remaining(), length);
    if (count == 0) return -1;
    byteBuffer.get(bytes, offset, length);
    return count;
  }

  public int available () throws IOException {
    return byteBuffer.remaining();
  }
}



