package com.github.ambry.network;

import java.nio.channels.WritableByteChannel;
import java.io.IOException;

/**
 * Any data that needs to be sent over the network can implement
 * this interface
 */
public interface Send {
  /**
   * Writes content into the provided channel
   * @param channel The channel into which data needs to be written to
   * @throws IOException
   */
  void writeTo(WritableByteChannel channel) throws IOException;

  /**
   * Returns true if the all data has been written
   * @return True if all the data has been written else false
   */
  boolean isComplete();

  /**
   * The total size in bytes that needs to be written to the channel
   * @return The size of the data in bytes to be written
   */
  long sizeInBytes();
}
