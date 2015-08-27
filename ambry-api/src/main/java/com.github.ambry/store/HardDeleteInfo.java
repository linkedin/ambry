package com.github.ambry.store;

import java.nio.channels.ReadableByteChannel;


/**
 * A class that contains information about hard delete messages.
 */
public class HardDeleteInfo {
  // @todo add comments
  private ReadableByteChannel hardDeleteChannel;
  private long hardDeletedMessageSize;
  private long startOffsetInMessage;
  private byte[] recoveryInfo;

  public HardDeleteInfo(ReadableByteChannel hardDeletedMessage, long hardDeletedMessageSize, long startOffsetInMessage,
      byte[] recoveryInfo) {
    this.hardDeleteChannel = hardDeletedMessage;
    this.hardDeletedMessageSize = hardDeletedMessageSize;
    this.startOffsetInMessage = startOffsetInMessage;
    this.recoveryInfo = recoveryInfo;
  }

  public ReadableByteChannel getHardDeleteChannel() {
    return hardDeleteChannel;
  }

  public long getHardDeletedMessageSize() {
    return hardDeletedMessageSize;
  }

  public long getStartOffsetInMessage() {
    return startOffsetInMessage;
  }

  public byte[] getRecoveryInfo() {
    return recoveryInfo;
  }
}
