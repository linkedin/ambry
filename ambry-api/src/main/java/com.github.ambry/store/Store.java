
package com.github.ambry.store;

import java.io.InputStream;
import java.util.ArrayList;

/**
 * The object store's underlying store
 */
public interface Store {

  /**
   *
   * @throws StoreException
   */
  void start() throws StoreException;

  /**
   *
   * @param handles
   * @return
   * @throws StoreException
   */
  MessageReadSet get(ArrayList<String> handles) throws StoreException;

  /**
   *
   * @param handle
   * @param value
   * @throws StoreException
   */
  void put (String handle, InputStream value) throws StoreException;

  /**
   *
   * @param handles
   * @throws StoreException
   */
  void delete(ArrayList<String> handles) throws StoreException;

  /**
   *
   */
  void shutdown();
}
