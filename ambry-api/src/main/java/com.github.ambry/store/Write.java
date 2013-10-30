package com.github.ambry.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/24/13
 * Time: 5:25 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Write {
  /**
   * Appends the buffer into the underlying write interface. Returns the number of bytes
   * successfully written
   */
  int appendFrom(ByteBuffer buffer) throws IOException;

  /**
   * Appends the channel to the underlying write interface. Writes "size" number of bytes
   * to the interface and returns the number of bytes written successfully
   */
  long appendFrom(ReadableByteChannel channel, long size) throws IOException;
}
