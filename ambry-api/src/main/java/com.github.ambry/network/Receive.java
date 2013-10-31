package com.github.ambry.network;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Used to receive data from the network channel. Any implementation of this interface
 * can be used to read data from the network
 */
public interface Receive {
  /**
   * Indicates if the read has been completed
   * @return  true if read is complete, else false
   */
  boolean readComplete();

  /**
   * Reads some bytes from the provided channel
   * @param channel The channel to read from
   * @throws IOException
   */
  void readFrom(ReadableByteChannel channel) throws IOException;
}
