package com.github.ambry.store;

import java.nio.channels.ReadableByteChannel;


/**
 * A class that contains information about hard delete messages.
 */
public class HardDeleteInfo {
  private ReadableByteChannel channel;
  private long streamSize;
  private long hardDeleteRelativeOffsetInMessage;
  private byte[] recoveryInfo;

  public HardDeleteInfo(ReadableByteChannel channel, long streamSize, long hardDeleteRelativeOffsetInMessage,
      byte[] recoveryInfo) {
    this.channel = channel;
    this.streamSize = streamSize;
    this.hardDeleteRelativeOffsetInMessage = hardDeleteRelativeOffsetInMessage;
    this.recoveryInfo = recoveryInfo;
  }

  public ReadableByteChannel getChannel() {
    return channel;
  }

  public long getSize() {
    return streamSize;
  }

  public long getHardDeleteRelativeOffsetInMessage() {
    return hardDeleteRelativeOffsetInMessage;
  }

  public byte[] getRecoveryInfo() {
    return recoveryInfo;
  }
}
