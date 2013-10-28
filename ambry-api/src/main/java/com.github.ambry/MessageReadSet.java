package com.github.ambry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/11/13
 * Time: 5:21 PM
 * To change this template use File | Settings | File Templates.
 */
public interface MessageReadSet {
  /**
   * Write the message referred by the index in this set to the given channel from the given relative offset
   * up to maxSize. Less than the complete amount may be written, but no more than maxSize can be. The number
   * of bytes written is returned
   */
  long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException;

  /**
   * returns the total number of messages in this set
   */
  int count();

  /**
   * returns the size of the ith message
   */
  long sizeInBytes(int index);
}
