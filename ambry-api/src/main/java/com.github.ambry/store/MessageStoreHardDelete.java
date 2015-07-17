package com.github.ambry.store;

import java.io.DataInputStream;
import java.io.IOException;
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
  public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory factory);

  /**
   * Returns the message info of message at the given offset from the given Read interface.
   * @param read The read interface from which the message info is to be read.
   * @param offset The start offset of the message.
   * @param factory the store key factory.
   * @return a MessageInfo object for the message at the offset.
   */
  public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory factory)
      throws IOException;

  /**
   * Processes the metadata for a message in the stream that will be used during recovery. Processing involves
   * reading from the stream and maintaining the state of the blobs that need recovery.
   * @param stream the input stream representing the persisted information
   * @param key the StoreKey of the given message.
   */

  public byte[] processAndReturnRecoveryMetadata(DataInputStream stream, StoreKey key)
      throws IOException;
}
