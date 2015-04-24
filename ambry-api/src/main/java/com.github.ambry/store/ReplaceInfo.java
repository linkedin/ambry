package com.github.ambry.store;

import java.nio.channels.ReadableByteChannel;


public class ReplaceInfo {
  ReadableByteChannel channel;
  long streamSize;

  public ReplaceInfo(ReadableByteChannel channel , long streamSize) {
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
