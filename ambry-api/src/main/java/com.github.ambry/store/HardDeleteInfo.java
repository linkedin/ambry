package com.github.ambry.store;

import java.nio.channels.ReadableByteChannel;


/**
 * A class that contains the info about the hard delete messages.
 */
public class HardDeleteInfo {
  ReadableByteChannel channel;
  long streamSize;

  public HardDeleteInfo(ReadableByteChannel channel , long streamSize) {
    this.channel = channel;
    this.streamSize = streamSize;
  }

  public ReadableByteChannel getChannel() {
    return channel;
  }

  public long getSize() {
    return streamSize;
  }
}
