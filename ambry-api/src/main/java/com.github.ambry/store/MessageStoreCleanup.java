package com.github.ambry.store;

import java.io.IOException;
import java.util.List;


/**
 * The cleanup component used by the store to cleanup deleted entries in the log.
 */
public interface MessageStoreCleanup {
  /**
   * Gets a replacement MessageWriteSet for cleaning up a message in a MessageReadSet.
   * @param readSet The set of messages to be replaced.
   * @param factory the store key factory.
   * @return The readable byte channel containing the replacement message
   * @throws IOException
   */
  List<ReplaceInfo> getReplacementInfo(MessageReadSet readSet, StoreKeyFactory factory)
      throws IOException;
}