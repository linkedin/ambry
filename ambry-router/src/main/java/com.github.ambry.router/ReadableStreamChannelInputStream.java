package com.github.ambry.router;

import com.github.ambry.commons.ByteBufferScheduledWriteChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 *  Class that converts a (possibly non-blocking) {@link ReadableStreamChannel} into a blocking {@link InputStream}.
 *  <p/>
 *  This class is not thread-safe and will result in undefined behaviour if accesses to the stream are not synchronized.
 */
class ReadableStreamChannelInputStream extends InputStream {
  private final ByteBufferScheduledWriteChannel scheduledWriteChannel = new ByteBufferScheduledWriteChannel();

  private ByteBuffer currentChunk = null;
  private volatile long bytesAvailable;

  /**
   * Create a ReadableStreamChannelInputStream with the given {@link ReadableStreamChannel}.
   * @param readableStreamChannel the {@link ReadableStreamChannel} that needs to be converted into an
   * {@link InputStream}.
   */
  public ReadableStreamChannelInputStream(ReadableStreamChannel readableStreamChannel) {
    bytesAvailable = readableStreamChannel.getSize();
    readableStreamChannel.readInto(scheduledWriteChannel, new CloseWriteChannelCallback(scheduledWriteChannel));
  }

  @Override
  public int available() {
    return bytesAvailable < Integer.MAX_VALUE ? (int) bytesAvailable : Integer.MAX_VALUE;
  }

  @Override
  public int read() {
    int data = -1;
    if (loadData()) {
      data = currentChunk.get() & 0xFF;
      bytesAvailable--;
    }
    return data;
  }

  @Override
  public int read(byte b[], int off, int len)
      throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int startOff = off;
    while (len > 0 && loadData()) {
      int toRead = Math.min(len, currentChunk.remaining());
      currentChunk.get(b, off, toRead);
      len -= toRead;
      off += toRead;
    }

    int bytesRead = off - startOff;
    if (bytesRead > 0) {
      bytesAvailable -= bytesRead;
    } else {
      bytesRead = -1;
    }
    return bytesRead;
  }

  @Override
  public void close() {
    scheduledWriteChannel.close();
  }

  /**
   * Loads more data for reading. Blocks until data is either available or no more data is expected.
   * @return {@code true} if data is available for reading. {@link false} otherwise.
   * @throws IllegalStateException if the wait for the next chunk is interrupted.
   */
  private boolean loadData() {
    if (currentChunk == null || !currentChunk.hasRemaining()) {
      if (currentChunk != null) {
        scheduledWriteChannel.resolveChunk(currentChunk, null);
      }
      try {
        currentChunk = scheduledWriteChannel.getNextChunk();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    return currentChunk != null;
  }
}

/**
 * Callback for {@link ByteBufferScheduledWriteChannel} that closes the channel on
 * {@link #onCompletion(Long, Exception)}.
 */
class CloseWriteChannelCallback implements Callback<Long> {
  private final ByteBufferScheduledWriteChannel channel;

  /**
   * Creates a callback to close {@code channel} on {@link #onCompletion(Long, Exception)}.
   * @param channel the {@link ByteBufferScheduledWriteChannel} that needs to be closed.
   */
  public CloseWriteChannelCallback(ByteBufferScheduledWriteChannel channel) {
    this.channel = channel;
  }

  @Override
  public void onCompletion(Long result, Exception exception) {
    channel.close();
  }
}
