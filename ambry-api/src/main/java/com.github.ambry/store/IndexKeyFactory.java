package com.github.ambry.store;


/**
 * Factory to create an index key
 */
public interface IndexKeyFactory {

  /**
   * The index key created using the value provided
   * @param value The value the key needs to store  in string format
   * @return The index key containing the value provided
   */
  IndexKey getKey(String value);
}

