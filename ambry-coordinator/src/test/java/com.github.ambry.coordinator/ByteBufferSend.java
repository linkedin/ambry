package com.github.ambry.coordinator;

import com.github.ambry.network.Send;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * A byte buffer version of Send that sends a materialized byte buffer. This breaks the contract of Send (only
 * materialize onto the network) and so is only suitable for use in tests.
 */
class ByteBufferSend implements Send {
  private final ByteBuffer buffer;

  public ByteBufferSend(ByteBuffer byteBuffer)
      throws IOException {
    this.buffer = byteBuffer.duplicate();
  }

  @Override
  public long writeTo(WritableByteChannel channel)
      throws IOException {
    long written = 0;
    if (!isSendComplete()) {
      written = channel.write(buffer);
    }
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return buffer.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    return buffer.limit();
  }
}
