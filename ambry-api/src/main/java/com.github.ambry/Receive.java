package com.github.ambry;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/16/13
 * Time: 5:52 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Receive {
  boolean readComplete();
  void readFrom(ReadableByteChannel channel) throws IOException;
}
