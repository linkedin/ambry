package com.github.ambry.store;


import java.util.Properties;


/**
 * The storage engine factory that is used to create a given type of a store
 */
public interface StoreFactory {

  /**
   *  Returns the store after creating the store instance with the given properties
   * @param config The properties config that is used by the store
   * @return The store represented by this factory
   */
  public Store getStore(Properties config);
}