package com.github.ambry.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A write interface provided by the underlying store to write to it
 */
public interface Write {

  /**
   * Appends the buffer into the underlying write interface (eg: file). Returns the number of bytes
   * successfully written
   * @param buffer The buffer from which data needs to be written from
   * @return The number of bytes written to the write interface
   * @throws IOException
   */
  int appendFrom(ByteBuffer buffer) throws IOException;

  /**
   * Appends the channel to the underlying write interface. Writes "size" number of bytes
   * to the interface and returns the number of bytes written successfully
   * @param channel The channel from which data needs to be written from
   * @param size The amount of data in bytes to be written from the channel
   * @return The number of bytes written to the write interface
   * @throws IOException
   */
  long appendFrom(ReadableByteChannel channel, long size) throws IOException;
}
