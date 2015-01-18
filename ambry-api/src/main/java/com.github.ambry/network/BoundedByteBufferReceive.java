package com.github.ambry.network;

import java.io.EOFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.ByteBuffer;


/**
 * A byte buffer version of Receive to buffer the incoming request or response.
 */
public class BoundedByteBufferReceive implements Receive {

  private ByteBuffer buffer = null;
  private ByteBuffer sizeBuffer;
  private int sizeToRead;        // need to change to long
  private int sizeRead;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public BoundedByteBufferReceive() {
    sizeToRead = 0;
    sizeRead = 0;
    sizeBuffer = ByteBuffer.allocate(8);
  }

  @Override
  public boolean isReadComplete() {
    return !(buffer == null || sizeRead < sizeToRead);
  }

  @Override
  public long readFrom(ReadableByteChannel channel)
      throws IOException {
    long bytesRead = 0;
    if (buffer == null) {
      bytesRead = channel.read(sizeBuffer);
      if (bytesRead < 0) {
        throw new EOFException();
      }
      if (sizeBuffer.position() == sizeBuffer.capacity()) {
        sizeBuffer.flip();
        // for now we support only intmax size. We need to extend it to streaming
        sizeToRead = (int) sizeBuffer.getLong();
        sizeRead += 8;
        bytesRead += 8;
        buffer = ByteBuffer.allocate(sizeToRead - 8);
      }
    }
    if (buffer != null && sizeRead < sizeToRead) {
      long bytesReadFromChannel = channel.read(buffer);
      if (bytesReadFromChannel < 0) {
        throw new EOFException();
      }
      sizeRead += bytesReadFromChannel;
      bytesRead += bytesReadFromChannel;
      if (sizeRead == sizeToRead) {
        buffer.flip();
      }
    }
    logger.trace("size read from channel {}", sizeRead);
    return bytesRead;
  }

  public ByteBuffer getPayload() {
    return buffer;
  }
}
