package com.github.ambry.store;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * The message set that needs to be written to a channel
 */
public interface MessageWriteSet {

  /**
   * Write the messages in this set to the given channel from the given offset
   * @param channel The channel to write the message to
   * @param offset The start offset from where write needs to start
   * @return  The size in bytes that was written to the channel
   * @throws IOException
   */
  long writeTo(GatheringByteChannel channel, long offset) throws IOException;
}