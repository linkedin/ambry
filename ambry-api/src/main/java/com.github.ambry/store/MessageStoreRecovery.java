package com.github.ambry.store;

import java.util.List;
import java.io.IOException;
import java.util.Set;


/**
 * The recovery component used by the store to recover
 * the index from the log.
 */
public interface MessageStoreRecovery {
  /**
   * Recovers the messages from the underlying store using the read interface. It recovers from the startOffset
   * till the endOffset. The expectation of this interface is that read never blocks
   * @param read The read interface that represents the underlying store
   * @param startOffset The start offset from where the recovery needs to begin
   * @param endOffset The end offset where the recovery needs to complete. The recovery can end before this offset if
   *                  there were malformed messages
   * @param factory The store key factory used to create the store key
   * @param offsetsToIgnoreCrcCheck offsets to ignore crc checks.
   * @return A list of messages that were successfully recovered
   * @throws IOException
   */
  List<MessageInfo> recover(Read read, long startOffset, long endOffset, StoreKeyFactory factory,
      Set<Long> offsetsToIgnoreCrcCheck)
      throws IOException;
}
