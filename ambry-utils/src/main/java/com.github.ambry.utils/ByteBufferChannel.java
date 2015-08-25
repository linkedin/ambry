package com.github.ambry.utils;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A {@link WritableByteChannel} that stores the bytes written into it in a {@link ByteBuffer}.
 */
public class ByteBufferChannel implements WritableByteChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ByteBuffer buffer;

  /**
   * Gets the {@link ByteBuffer} that is being used to receive writes.
   * @return the {@link ByteBuffer} that is receives writes to this channel.
   */
  public ByteBuffer getBuffer() {
    return buffer;
  }

  /**
   * This object needs to be instantiated with a {@link ByteBuffer} that is provided by the caller. The maximum
   * number of bytes that can be written into the {@code buffer} is determined by {@code buffer.remaining()}.
   * @param buffer the buffer that can be used to recieve writes.
   */
  public ByteBufferChannel(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Copies bytes from {@code src} into the {@link ByteBuffer} ({@code buffer}) backing this channel. The number of
   * bytes copied is the minimum of {@code src.remaining()} and {@code buffer.remaining()}.
   * @param src the source {@link ByteBuffer} to copy bytes from.
   * @return the number of bytes copied.
   * @throws ClosedChannelException if the channel is closed when this function was called.
   */
  @Override
  public int write(ByteBuffer src)
      throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    int bytesToWrite = Math.min(src.remaining(), buffer.remaining());
    for (int i = 0; i < bytesToWrite; i++) {
      buffer.put(src.get());
    }
    return bytesToWrite;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() {
    channelOpen.set(false);
  }
}
