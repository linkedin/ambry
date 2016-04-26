/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import java.nio.channels.ReadableByteChannel;


/**
 * A class that contains information about a hard delete message.
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
