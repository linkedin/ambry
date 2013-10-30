package com.github.ambry.store;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/11/13
 * Time: 5:21 PM
 * To change this template use File | Settings | File Templates.
 */
public interface MessageWriteSet {
  /** Write the messages in this set to the given channel from the given offset */
  long writeTo(GatheringByteChannel channel, long offset) throws IOException;
}