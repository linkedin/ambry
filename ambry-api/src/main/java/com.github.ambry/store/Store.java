
package com.github.ambry.store;

import java.io.InputStream;
import java.util.List;

/**
 * The object store's underlying store
 */
public interface Store {

  /**
   * Starts the store
   * @throws StoreException
   */
  void start() throws StoreException;

  /**
   * Returns the message set for the given ids
   * @param ids The list of ids whose messages need to be retrieved
   * @return The message set that pertains to the given set of ids
   * @throws StoreException
   */
  MessageReadSet get(List<? extends StoreKey> ids) throws StoreException;

  /**
   * Puts a set of messages into the store
   * @param messageSetToWrite The message set to write to the store
   * @throws StoreException
   */
  void put (MessageWriteSet messageSetToWrite) throws StoreException;

  /**
   * Deletes all the messages that are specified by the list of ids
   * @param ids The list of message ids that need to be deleted
   * @throws StoreException
   */
  void delete(List<? extends StoreKey> ids) throws StoreException;

  /**
   * Shutsdown the store
   */
  void shutdown();
}
