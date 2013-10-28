package com.github.ambry;

import java.nio.channels.WritableByteChannel;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/15/13
 * Time: 9:28 AM
 * To change this template use File | Settings | File Templates.
 */
public interface Send {
  void writeTo(WritableByteChannel channel) throws IOException;
  boolean isComplete();
  long sizeInBytes();
}
