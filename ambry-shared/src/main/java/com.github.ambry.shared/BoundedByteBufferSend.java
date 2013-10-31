package com.github.ambry.shared;

import com.github.ambry.network.Send;
import com.github.ambry.utils.ByteBufferOutputStream;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.*;


/**
 * A byte buffer version of Send to buffer the outgoing responses before sending.
 * This is mainly used to optimize serialization of response objects on the request handler
 * threads rather than the network threads.
 */
public class BoundedByteBufferSend implements Send {

  private final ByteBuffer buffer;

  public BoundedByteBufferSend(Send request) throws IOException {
    // Avoid possibility of overflow for 2GB-4 byte buffer
    if(request.sizeInBytes() > Integer.MAX_VALUE - 4)
      throw new IllegalStateException("Maximum allowable size for a bounded buffer is "
              + (Integer.MAX_VALUE - 4) + ".");

    int size = (int)request.sizeInBytes();
    buffer = ByteBuffer.allocate(size + 4);
    buffer.putInt(size);
    WritableByteChannel channel = Channels.newChannel(new ByteBufferOutputStream(buffer));
    request.writeTo(channel);
    buffer.rewind();
  }

  public void writeTo(WritableByteChannel channel) throws IOException {
    if (!isComplete()) {
      channel.write(buffer);
    }
  }

  public boolean isComplete() {
   return buffer.remaining() == 0;
  }

  public long sizeInBytes() {
    return buffer.limit();
  }
}