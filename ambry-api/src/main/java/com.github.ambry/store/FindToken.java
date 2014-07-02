package com.github.ambry.store;

/**
 * The find token used to search entries in the store
 */
public interface FindToken {
  /**
   * Returns the contents of the token in bytes
   * @return The byte array representing the token
   */
  byte[] toBytes();

  // returns the total bytes read so far until this token
  public long getBytesRead();
}
