package com.github.ambry.store;

import java.nio.channels.ReadableByteChannel;


/**
 * A class that contains the info about the hard delete messages.
 */
public class HardDeleteInfo {
  ReadableByteChannel channel;
  long streamSize;
  long relativeOffset;
  byte[] recoveryInfo;

  public HardDeleteInfo(ReadableByteChannel channel, long streamSize, long relativeOffset, byte[] recoveryInfo) {
    this.channel = channel;
    this.streamSize = streamSize;
    this.relativeOffset = relativeOffset;
    this.recoveryInfo = recoveryInfo;
  }

  public ReadableByteChannel getChannel() {
    return channel;
  }

  public long getSize() {
    return streamSize;
  }

  public long getRelativeOffset() {
    return relativeOffset;
  }

  byte[] getRecoveryMetadata() {
    return recoveryInfo;
  }
}
