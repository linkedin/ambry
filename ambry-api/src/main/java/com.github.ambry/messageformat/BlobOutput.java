package com.github.ambry.messageformat;

import com.github.ambry.network.ReadableChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Contains the blob output
 */
public class BlobOutput implements ReadableChannel {
  private final AtomicBoolean channelOpened = new AtomicBoolean(false);
  private final AtomicBoolean channelClosed = new AtomicBoolean(false);
  private final ReentrantLock bufferReadLock = new ReentrantLock();
  private final long size;
  private final InputStream stream;
  private ByteBuffer buffer;

  /**
   * The blob output that helps to read a blob
   * @param size The size of the blob
   * @param stream The stream that contains the blob
   */
  public BlobOutput(long size, InputStream stream) {
    this.size = size;
    this.stream = stream;
  }

  public long getSize() {
    return size;
  }

  public InputStream getStream() {
    return stream;
  }

  public BlobOutput openForReading()
      throws IOException {
    if (channelOpened.getAndSet(true)) {
      throw new IllegalStateException("Cannot open channel more than once for reading");
    } else if (channelClosed.get()) {
      throw new ClosedChannelException();
    }
    byte[] buf = new byte[(int) getSize()];
    int read = 0;
    while (read < getSize()) {
      int sizeToRead = (int) getSize() - read;
      int sizeRead = getStream().read(buf, read, sizeToRead);
      if (sizeRead == -1) {
        break;
      }
      read += sizeRead;
    }
    if (read != getSize()) {
      throw new IOException("Blob size " + getSize() + " did not match total bytes read " + read);
    } else if (getStream().read() != -1) {
      throw new IOException("Stream did not end after " + getSize() + " bytes were read");
    }
    buffer = ByteBuffer.wrap(buf);
    return this;
  }

  @Override
  public int writeTo(WritableByteChannel channel)
      throws IOException {
    if (!channelOpened.get()) {
      throw new NonReadableChannelException();
    } else if (channelClosed.get()) {
      throw new ClosedChannelException();
    } else if (!buffer.hasRemaining()) {
      return -1;
    }
    int bytesWritten = 0;
    try {
      bufferReadLock.lock();
      bytesWritten = channel.write(buffer);
    } finally {
      bufferReadLock.unlock();
    }
    return bytesWritten;
  }

  @Override
  public int read(ByteBuffer dst)
      throws IOException {
    if (!channelOpened.get()) {
      throw new NonReadableChannelException();
    } else if (channelClosed.get()) {
      throw new ClosedChannelException();
    } else if (!buffer.hasRemaining()) {
      return -1;
    }
    int bytesRead = 0;
    try {
      bufferReadLock.lock();
      int prevPosition = buffer.position();
      dst.put(buffer);
      bytesRead = buffer.position() - prevPosition;
    } finally {
      bufferReadLock.unlock();
    }
    return bytesRead;
  }

  @Override
  public boolean isOpen() {
    return channelOpened.get() && !channelClosed.get();
  }

  @Override
  public void close()
      throws IOException {
    channelClosed.set(true);
  }
}
