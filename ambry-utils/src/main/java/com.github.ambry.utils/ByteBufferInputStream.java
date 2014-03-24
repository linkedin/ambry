package com.github.ambry.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

public class ByteBufferInputStream extends InputStream {
  private ByteBuffer byteBuffer;
  private int mark;
  private int readLimit;

  public ByteBufferInputStream(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
    this.mark = -1;
    this.readLimit = -1;
  }

  public ByteBufferInputStream(InputStream stream, int size) throws IOException {
    this.byteBuffer = ByteBuffer.allocate(size);
    int read = 0;
    while (read < size) {
      read += Channels.newChannel(stream).read(byteBuffer);
    }
    byteBuffer.flip();
    this.mark = -1;
    this.readLimit = -1;
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
    byteBuffer.get(bytes, offset, count);
    return count;
  }

  @Override
  public int available() throws IOException {
    return byteBuffer.remaining();
  }

  @Override
  public synchronized void reset() throws IOException {
    if (readLimit == -1 || mark == -1 ) {
      throw new IOException("Mark not set before reset invoked.");
    }
    if (byteBuffer.position() - mark > readLimit) {
      throw new IOException("Read limit exceeded before reset invoked.");
    }
    byteBuffer.reset();
  }

  @Override
  public synchronized void mark(int readLimit) {
    this.mark = byteBuffer.position();
    this.readLimit = readLimit;
    byteBuffer.mark();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  public ByteBufferInputStream duplicate() {
    return new ByteBufferInputStream(byteBuffer.duplicate());
  }
}

