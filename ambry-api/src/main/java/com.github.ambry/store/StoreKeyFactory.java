package com.github.ambry.store;

import java.io.IOException;
import java.io.DataInputStream;

/**
 * Factory to create an index key
 */
public interface StoreKeyFactory {

  /**
   * The store key created using the stream provided
   * @param stream The stream used to create the store key
   * @return The store key created from the stream
   */
  StoreKey getStoreKey(DataInputStream stream) throws IOException;
}

