package com.github.ambry.shared;

import java.io.InputStream;


/**
 * The receive on the connected channel provides a ChannelOutput that
 * can be used to read the output from.
 */
public class ChannelOutput {

  private InputStream inputStream;

  private long streamSize;

  public ChannelOutput(InputStream inputStream, long streamSize) {
    this.inputStream = inputStream;
    this.streamSize = streamSize;
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  public long getStreamSize() {
    return streamSize;
  }
}
