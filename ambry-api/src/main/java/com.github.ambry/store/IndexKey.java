package com.github.ambry.store;

import java.nio.ByteBuffer;

/**
 * Represents the index key. To make an object part of an index key,
 * this interface can be implemented
 */
public interface IndexKey {

  /**
   * The byte version of this key
   * @return A byte buffer that represents the key
   */
  ByteBuffer toBytes();
}
