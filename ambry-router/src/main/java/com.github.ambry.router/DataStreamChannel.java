package com.github.ambry.router;

import com.github.ambry.network.ReadableStreamChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Represents blob data as a {@link ReadableStreamChannel}. Consumes an {@link InputStream} containing the bytes in the
 * blob and stores the data in a {@link ByteBuffer}.
 */
class DataStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ReentrantLock bufferReadLock = new ReentrantLock();
  private final ByteBuffer buffer;

  /**
   * Constructs a DataStreamChannel by consuming {@code size} bytes of data from the given {@link InputStream}.
   * @param inputStream the {@link InputStream} to consume bytes from.
   * @param size the number of bytes to consume.
   * @throws IllegalStateException if the stream reached EOF before {@code size} bytes of data was read from it.
   * @throws IOException if data from the backing {@link InputStream} could not be read.
   */
  public DataStreamChannel(InputStream inputStream, long size)
      throws IOException {
    byte[] buf = new byte[(int) size];
    int read = 0;
    while (read < size) {
      int sizeToRead = (int) size - read;
      int sizeRead = inputStream.read(buf, read, sizeToRead);
      if (sizeRead == -1) {
        break;
      }
      read += sizeRead;
    }
    if (read != size) {
      throw new IllegalStateException(
          "Stream reached EOF with " + read + " bytes read out of an expected size of " + size);
    }
    buffer = ByteBuffer.wrap(buf);
  }

  @Override
  public long getSize() {
    return buffer.capacity();
  }

  @Override
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten = 0;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else if (!buffer.hasRemaining()) {
      bytesWritten = -1;
    } else {
      try {
        bufferReadLock.lock();
        bytesWritten = channel.write(buffer);
      } finally {
        bufferReadLock.unlock();
      }
    }
    return bytesWritten;
  }

  @Override
  public void close()
      throws IOException {
    channelOpen.set(false);
  }
}
