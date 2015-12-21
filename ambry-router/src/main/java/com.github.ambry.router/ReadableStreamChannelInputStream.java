package com.github.ambry.router;

import com.github.ambry.utils.ByteBufferChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Class that converts a (possibly non-blocking) {@link ReadableStreamChannel} into a blocking {@link InputStream}.
 *  <p/>
 *  This class is not thread-safe and will result in undefined behaviour if accesses to the stream are not synchronized.
 */
class ReadableStreamChannelInputStream extends InputStream {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ByteBufferChannel singleByteBufferChannel = new ByteBufferChannel(ByteBuffer.allocate(1));
  private final ReadableStreamChannel readableStreamChannel;
  private volatile long totalBytesRead = 0;

  public ReadableStreamChannelInputStream(ReadableStreamChannel readableStreamChannel) {
    this.readableStreamChannel = readableStreamChannel;
  }

  @Override
  public int available() {
    return readableStreamChannel.getSize() - totalBytesRead < Integer.MAX_VALUE ? (int) (readableStreamChannel.getSize()
        - totalBytesRead) : Integer.MAX_VALUE;
  }

  @Override
  public int read()
      throws IOException {
    ByteBuffer buffer = singleByteBufferChannel.getBuffer();
    buffer.clear();
    int data = -1;
    if (read(singleByteBufferChannel) != -1) {
      buffer.flip();
      data = buffer.get() & 0xFF;
      totalBytesRead++;
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

    ByteBufferChannel byteBufferChannel = new ByteBufferChannel(ByteBuffer.wrap(b, off, len));
    int bytesRead = read(byteBufferChannel);
    if (bytesRead > 0) {
      totalBytesRead += bytesRead;
    }
    return bytesRead;
  }

  /**
   * Uses the provided {@link WritableByteChannel} to read from the {@code readableStreamChannel} and returns the number
   * of bytes actually read.
   * <p/>
   * This method blocks until at least one byte is available, end of stream is reached or if  there is either an
   * {@link IOException} while reading from the {@code readableStreamChannel} or there is an
   * {@link InterruptedException} during the sleep awaiting data.
   * @param channel the {@link WritableByteChannel} to use.
   * @return the number of bytes read from the {@code readableStreamChannel}. -1 if end of stream is reached.
   * @throws IOException if there is an exception while reading from the {@code readableStreamChannel} or if there is an
   *          {@link InterruptedException} during the sleep awaiting data.
   */
  private int read(WritableByteChannel channel)
      throws IOException {
    int SINGLE_WAIT_TIME = 10;
    int WARN_INTERVAL = 50;
    int totalWaitTime = 0;
    int bytesRead;
    while (true) {
      bytesRead = readableStreamChannel.read(channel);
      if (bytesRead == 0) {
        try {
          Thread.sleep(SINGLE_WAIT_TIME);
          totalWaitTime += SINGLE_WAIT_TIME;
          if (totalWaitTime % WARN_INTERVAL == 0) {
            logger.warn("ReadableStreamChannelInputStream has been waiting for " + totalWaitTime + "ms for data");
          }
        } catch (InterruptedException e) {
          throw new IOException("Wait for data interrupted", e);
        }
      } else {
        break;
      }
    }
    return bytesRead;
  }
}

