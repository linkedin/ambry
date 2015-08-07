package com.github.ambry.network;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;


/**
 * TODO: write description
 */
public interface ReadableChannel extends ReadableByteChannel {

  public int writeTo(WritableByteChannel channel)
      throws IOException;
}
