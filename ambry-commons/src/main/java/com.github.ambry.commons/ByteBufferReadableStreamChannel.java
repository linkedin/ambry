package com.github.ambry.commons;

import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.ScheduledWriteChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Represents a {@link ByteBuffer} as a {@link ReadableStreamChannel}.
 */
public class ByteBufferReadableStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean channelEmptied = new AtomicBoolean(false);
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
  @Deprecated
  public int read(WritableByteChannel channel)
      throws IOException {
    // NOTE: This function is deprecated and will be removed soon. Therefore no changes have been made here.
    int bytesWritten = -1;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else {
      bufferReadLock.lock();
      try {
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
  public Future<Long> readInto(ScheduledWriteChannel scheduledWriteChannel, Callback<Long> callback) {
    Future<Long> future;
    if (!channelOpen.get()) {
      ClosedChannelException cce = new ClosedChannelException();
      FutureResult<Long> futureResult = new FutureResult<Long>();
      futureResult.done(0L, new IllegalStateException(cce));
      future = futureResult;
      if (callback != null) {
        callback.onCompletion(0L, cce);
      }
    } else if (!channelEmptied.compareAndSet(false, true)) {
      throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
    } else {
      future = scheduledWriteChannel.write(buffer, callback);
    }
    return future;
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
