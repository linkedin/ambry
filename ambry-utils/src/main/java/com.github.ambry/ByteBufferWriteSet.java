package com.github.ambry;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/24/13
 * Time: 4:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class ByteBufferWriteSet implements MessageWriteSet {
  private final ByteBuffer buffer;

  public ByteBufferWriteSet(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public long writeTo(GatheringByteChannel channel, long offset) throws IOException {
    if (buffer.remaining() > 0) {
      return channel.write(buffer);
    }
    return 0;
  }
}
