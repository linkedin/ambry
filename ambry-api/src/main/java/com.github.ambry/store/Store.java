
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
   * Returns the store info for the given ids
   * @param ids The list of ids whose messages need to be retrieved
   * @return The store info for the given ids
   * @throws StoreException
   */
  StoreInfo get(List<? extends StoreKey> ids) throws StoreException;

  /**
   * Puts a set of messages into the store
   * @param messageSetToWrite The message set to write to the store
   * @throws StoreException
   */
  void put (MessageWriteSet messageSetToWrite) throws StoreException;

  /**
   * Deletes all the messages that are part of the message set
   * @param messageSetToDelete The list of messages that need to be deleted
   * @throws StoreException
   */
  void delete(MessageWriteSet messageSetToDelete) throws StoreException;

  /**
   * Updates TTL for all the messages that are part of the message set
   * @param messageSetToUpdateTTL The list of messages whose ttl needs to be updated
   * @throws StoreException
   */
  void updateTTL(MessageWriteSet messageSetToUpdateTTL) throws StoreException;

  /**
   * Shutsdown the store
   */
  void shutdown() throws StoreException;
}
