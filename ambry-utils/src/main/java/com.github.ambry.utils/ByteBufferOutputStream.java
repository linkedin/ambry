package com.github.ambry.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
  private ByteBuffer buffer;

  public ByteBufferOutputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public void write(int b) throws IOException {
    buffer.put((byte) b);
  }

  public void write(byte[] bytes, int off, int len) throws IOException {
    buffer.put(bytes, off, len);
  }
}
