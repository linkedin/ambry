package com.github.ambry.store;

import java.io.IOException;
import java.util.List;


/**
 * The cleanup component used by the store to cleanup deleted entries in the log.
 */
public interface MessageStoreCleanup {
  /**
   * Recovers the messages from the underlying store using the read interface. It recovers from the startOffset
   * till the endOffset. The expectation of this interface is that read never blocks
   * @param write The ReadWrite interface that represents the underlying store
   * @param storeInfo The StoreInfo representing the set of messages that are to be cleaned up.
   * //reuturn @TODO
   * @throws IOException
   */

  /**
   * Gets a replacement MessageWriteSet for cleaning up a message in a MessageReadSet.
   * @param readSet The set of messages.
   * @param factory the store key factory
   * @return The readable byte channel containing the replacement message
   * @throws IOException
   */
  List<ReplaceInfo> getReplacementInfo(MessageReadSet readSet, StoreKeyFactory factory)
      throws IOException;
}