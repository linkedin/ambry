package com.github.ambry.store;

import java.util.Iterator;


/**
 * The component used by the store to hard delete entries in the log.
 */
public interface MessageStoreHardDelete {
  /**
   * Returns an iterator over the HardDeleteInfo of the messages in the readSet.
   * @param readSet The set of messages to be replaced.
   * @param factory the store key factory.
   * @return iterator over the HardDeleteInfo for the messages in the readSet.
   */
  public Iterator<HardDeleteInfo> getHardDeletedMessages(MessageReadSet readSet, StoreKeyFactory factory);
}
