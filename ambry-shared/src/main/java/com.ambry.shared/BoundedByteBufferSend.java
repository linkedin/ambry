package com.ambry.shared;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/22/13
 * Time: 11:25 AM
 * To change this template use File | Settings | File Templates.
 */


import com.github.ambry.ByteBufferOutputStream;
import com.github.ambry.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.*;
import java.nio.channels.*;


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