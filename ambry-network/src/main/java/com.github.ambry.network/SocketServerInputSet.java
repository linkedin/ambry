package com.github.ambry.network;

import com.github.ambry.utils.ByteBufferInputStream;
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
  private ByteBufferInputStream stream;
  private ByteBuffer sizeBuffer;
  private int sizeToRead;        // need to change to long
  private int sizeRead;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public SocketServerInputSet() {
    sizeToRead = 0;
    sizeRead = 0;
    sizeBuffer = ByteBuffer.allocate(8);
  }

  @Override
  public int read() throws IOException {
    return (buffer.get() & 0xFF);
  }

  @Override
  public boolean isReadComplete() {
    return !(buffer == null || sizeRead < sizeToRead);
  }

  @Override
  public void readFrom(ReadableByteChannel channel) throws IOException {
    if (buffer == null) {
      channel.read(sizeBuffer);
      if (sizeBuffer.position() == sizeBuffer.capacity()) {
        sizeBuffer.flip();
        // for now we support only intmax size. We need to extend it to streaming
        sizeToRead = (int)sizeBuffer.getLong();
        sizeRead += 8;
        buffer = ByteBuffer.allocate(sizeToRead - 8);
      }
    }
    if (buffer != null && sizeRead < sizeToRead) {
      sizeRead += channel.read(buffer);
      if (sizeRead == sizeToRead) {
        buffer.flip();
      }
    }
    logger.trace("size read from channel {}", sizeRead);
  }
}
