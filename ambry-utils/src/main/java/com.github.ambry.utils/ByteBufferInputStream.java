package com.github.ambry.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;


public class ByteBufferInputStream extends InputStream {
  private ByteBuffer byteBuffer;
  private int mark;
  private int readLimit;

  public ByteBufferInputStream(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
    this.mark = -1;
    this.readLimit = -1;
  }

  /**
   * Reads 'size' amount of bytes from the stream into the buffer
   * @param stream The stream from which bytes needs to be read
   * @param size The size that needs to be read from the stream
   * @throws IOException
   */
  public ByteBufferInputStream(InputStream stream, int size)
      throws IOException {
    this(stream, size, -1);
  }

  /**
   * Reads 'size' amount of bytes from the stream into the buffer. It spends 'readTimeoutMs' amount of time
   * reading from the stream. If the timeout exceeds, IOException is thrown.
   * @param stream The stream from which bytes need to be read
   * @param size The size that needs to be read from the stream
   * @param readTimeoutMs The max amount of time spent on reading from the input stream
   * @throws IOException
   */
  public ByteBufferInputStream(InputStream stream, int size, long readTimeoutMs)
      throws IOException {
    this.byteBuffer = ByteBuffer.allocate(size);
    int read = 0;
    long elapsedTimeMs = 0;
    ReadableByteChannel readableByteChannel = Channels.newChannel(stream);
    while (read < size) {
      long readStartTimeMs = SystemTime.getInstance().milliseconds();
      read += readableByteChannel.read(byteBuffer);
      long readTime = SystemTime.getInstance().milliseconds() - readStartTimeMs;
      elapsedTimeMs += readTime;
      if (read < size && readTimeoutMs != -1 && elapsedTimeMs > readTimeoutMs) {
        throw new IOException(
            "Time taken to read from stream to buffer is greater than readTimeoutMs " + readTimeoutMs);
      }
    }
    byteBuffer.flip();
    this.mark = -1;
    this.readLimit = -1;
  }

  @Override
  public int read()
      throws IOException {
    if (!byteBuffer.hasRemaining()) {
      return -1;
    }
    return byteBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length)
      throws IOException {
    int count = Math.min(byteBuffer.remaining(), length);
    if (count == 0) {
      return -1;
    }
    byteBuffer.get(bytes, offset, count);
    return count;
  }

  @Override
  public int available()
      throws IOException {
    return byteBuffer.remaining();
  }

  @Override
  public synchronized void reset()
      throws IOException {
    if (readLimit == -1 || mark == -1) {
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

