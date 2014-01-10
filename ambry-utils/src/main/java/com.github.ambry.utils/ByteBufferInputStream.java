package com.github.ambry.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

public class ByteBufferInputStream extends InputStream {
  private ByteBuffer byteBuffer;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public ByteBufferInputStream(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  public ByteBufferInputStream(InputStream stream, int size) throws IOException {
    this.byteBuffer = ByteBuffer.allocate(size);
    int read = 0;
    while (read < size) {
      read += Channels.newChannel(stream).read(byteBuffer);
    }
    byteBuffer.flip();
  }

  @Override
  public int read() throws IOException {
    if (!byteBuffer.hasRemaining()) return -1;
    return byteBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    int count = Math.min(byteBuffer.remaining(), length);
    if (count == 0) return -1;
    byteBuffer.get(bytes, offset, length);
    return count;
  }

  @Override
  public int available() throws IOException {
    return byteBuffer.remaining();
  }

  public ByteBufferInputStream duplicate() {
    return new ByteBufferInputStream(byteBuffer.duplicate());
  }
}



