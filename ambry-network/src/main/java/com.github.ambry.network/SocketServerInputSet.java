package com.github.ambry.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.*;
import java.nio.ByteBuffer;

/**
 * This represents data received from the channel and provides an input stream
 * interface to read from it. This class is responsible for deciding when to buffer
 * the input data or stream content directly from the channel
 */
public class SocketServerInputSet extends InputStream implements Receive {

  private ByteBuffer buffer = null;
  private int sizeToRead;        // need to change to long
  private int sizeRead;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public SocketServerInputSet() {
    sizeToRead = 0;
    sizeRead = 0;
  }

  @Override
  public int read() throws IOException {
    return buffer.get();
  }

  @Override
  public boolean readComplete() {
    return !(sizeRead < sizeToRead);
  }

  @Override
  public void readFrom(ReadableByteChannel channel) throws IOException {
    if (buffer == null) {
      ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
      int read = 0;
      while (read < 8) {
        read += channel.read(sizeBuffer);
      }
      sizeBuffer.flip();
      // for now we support only intmax size. We need to extend it to streaming
      sizeToRead = (int)sizeBuffer.getLong();
      buffer = ByteBuffer.allocate(sizeToRead - 8);
      sizeRead += 8;
    }
    if (sizeRead < sizeToRead) {
      sizeRead += channel.read(buffer);
      if (sizeRead == sizeToRead) {
        buffer.flip();
      }
    }
    logger.trace("size read from channel {}", sizeRead);
  }
}
