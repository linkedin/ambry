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
  int appendFrom(ByteBuffer buffer)
      throws IOException;

  /**
   * Appends the channel to the underlying write interface. Writes "size" number of bytes
   * to the interface and returns the number of bytes written successfully
   * @param channel The channel from which data needs to be written from
   * @param size The amount of data in bytes to be written from the channel
   * @throws IOException
   */
  void appendFrom(ReadableByteChannel channel, long size)
      throws IOException;

  /**
   * Writes the channel to the underlying write interface at the given offset.
   * Writes "size" number of bytes to the interface at the offset and returns the number
   * of bytes written successfully
   * @param channel The channel from which data needs to be written from.
   * @param offset The offset at which to write in the underlying write interface.
   * @param size The amount of data in bytes to be written from the channel.
   * @throws IOException
   */
  void writeFrom(ReadableByteChannel channel, long offset, long size)
      throws IOException;
}
