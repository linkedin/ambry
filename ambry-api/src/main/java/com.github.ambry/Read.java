package com.github.ambry;

import java.nio.ByteBuffer;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/24/13
 * Time: 5:16 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Read {
  /**
   * Read from the underlying file into the buffer starting at the given position
   */
  void readInto(ByteBuffer buffer , int position) throws IOException;
}
