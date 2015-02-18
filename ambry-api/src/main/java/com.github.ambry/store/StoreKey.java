package com.github.ambry.store;

/**
 * Represents the index key. To make an object part of an index key,
 * this interface can be implemented
 */
public abstract class StoreKey implements Comparable<StoreKey> {

  /**
   * The byte version of this key
   * @return A byte buffer that represents the key
   */
  public abstract byte[] toBytes();

  /**
   * The size of the serialized version of the key
   * @return The size of the key
   */
  public abstract short sizeInBytes();

  /**
   * Get the key in String form
   * @return the key in String form
   */
  public abstract String getIDString();
}
