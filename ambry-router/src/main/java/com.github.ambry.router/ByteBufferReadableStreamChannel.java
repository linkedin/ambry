package com.github.ambry.router;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Represents a {@link ByteBuffer} as a {@link ReadableStreamChannel}.
 */
class ByteBufferReadableStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ReentrantLock bufferReadLock = new ReentrantLock();
  private final ByteBuffer buffer;

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code buffer}.
   * @param buffer the {@link ByteBuffer} that is used to retrieve data from on invocation of read operations.
   */
  public ByteBufferReadableStreamChannel(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public long getSize() {
    return buffer.capacity();
  }

  @Override
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else {
      try {
        bufferReadLock.lock();
        bytesWritten = -1;
        if (buffer.hasRemaining()) {
          bytesWritten = channel.write(buffer);
        }
      } finally {
        bufferReadLock.unlock();
      }
    }
    return bytesWritten;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    channelOpen.set(false);
  }
}
