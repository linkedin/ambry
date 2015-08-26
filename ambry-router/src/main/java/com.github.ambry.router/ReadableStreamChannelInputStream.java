package com.github.ambry.router;

import com.github.ambry.network.ReadableStreamChannel;
import com.github.ambry.utils.ByteBufferChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Class that converts a (possibly non-blocking) {@link ReadableStreamChannel} into a blocking {@link InputStream}.
 */
class ReadableStreamChannelInputStream extends InputStream {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ByteBufferChannel singleByteBufferChannel = new ByteBufferChannel(ByteBuffer.allocate(1));
  private final ReadableStreamChannel readableStreamChannel;

  public ReadableStreamChannelInputStream(ReadableStreamChannel readableStreamChannel) {
    this.readableStreamChannel = readableStreamChannel;
  }

  @Override
  public int read()
      throws IOException {
    ByteBuffer buffer = singleByteBufferChannel.getBuffer();
    buffer.clear();
    int bytesRead = read(singleByteBufferChannel);
    int data = -1;
    if (bytesRead != -1) {
      buffer.flip();
      data = buffer.get() & 0xFF;
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
    return read(byteBufferChannel);
  }

  /**
   * Uses the provided {@link WritableByteChannel} to read from the {@code readableStreamChannel} and returns the number
   * of bytes actually read.
   * <p/>
   * This method blocks until at least one byte is available, end of stream is reached or if  there is either an
   * {@link IOException} while reading from the {@code readableStreamChannel} or there is an
   * {@link InterruptedException} during the sleep awaiting data.
   * @param channel the {@link WritableByteChannel} to use.
   * @return the number of bytes read from the {@code readableStreamChannel}.
   * @throws IOException if there is an exception while reading from the {@code readableStreamChannel} or if there is an
   *          {@link InterruptedException} during the sleep awaiting data.
   */
  private int read(WritableByteChannel channel)
      throws IOException {
    int waitTime = 0;
    int bytesRead;
    while (true) {
      bytesRead = readableStreamChannel.read(channel);
      if (bytesRead == 0) {
        try {
          Thread.sleep(10);
          waitTime += 10;
          logger.warn("ReadableStreamChannelInputStream has been waiting for " + waitTime + "ms for data");
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

