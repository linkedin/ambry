package com.github.ambry.utils;

import com.github.ambry.store.MessageWriteSet;
import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.nio.ByteBuffer;


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
