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
   * @param readSetIndex The index of the message in the readSet for which the replacement write set is to be obtained.
   * @param factory the store key factory.
   * @return ReplaceInfo with the readable byte channel containing the replacement message
   * @throws IOException
   */
  ReplaceInfo getReplacementInfo(MessageReadSet readSet, int readSetIndex, StoreKeyFactory factory)
      throws IOException;
}
