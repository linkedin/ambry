package com.github.ambry.store;

import java.nio.channels.ReadableByteChannel;


/**
 * A class that contains information about hard delete messages.
 */
public class HardDeleteInfo {
  /* The channel containing the bytes for replacing original record */
  private ReadableByteChannel hardDeleteChannel;
  /* The length of bytes to be read from the channel */
  private long hardDeletedMessageSize;
  /* The start offset in the message that the first byte in the channel corresponds to*/
  private long startOffsetInMessage;
  /* The information associated with this message that can be used by the MessageStoreHardDelete component to
     recover the information required to reconstruct the HardDeleteInfo for this message */
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
