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
   * @return true if read is complete, else false
   */
  boolean isReadComplete();

  /**
   * Reads some bytes from the provided channel
   * @param channel The channel to read from
   * @return Number of bytes read. Returns -1 if EOS is reached
   * @throws IOException
   */

  long readFrom(ReadableByteChannel channel) throws IOException;
}
