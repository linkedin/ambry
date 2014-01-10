package com.github.ambry.coordinator;

import com.github.ambry.network.Send;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * A byte buffer version of Send that sends a materialized byte buffer. This breaks the contract of Send (only
 * materialize onto the network) and so is only suitable for use in tests.
 */
public class ByteBufferSend implements Send {
  private final ByteBuffer buffer;

  public ByteBufferSend(ByteBuffer byteBuffer) throws IOException {
    this.buffer = byteBuffer.duplicate();
  }

  public void writeTo(WritableByteChannel channel) throws IOException {
    if (!isSendComplete()) {
      channel.write(buffer);
    }
  }

  public boolean isSendComplete() {
    return buffer.remaining() == 0;
  }

  public long sizeInBytes() {
    return buffer.limit();
  }
}
