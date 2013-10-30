package com.github.ambry.store;


import java.util.Properties;


/**
 * An object provided by the storage engine implementation to create instances
 * of the given storage engine type.
 */
public interface StoreFactory {
    public Store getStore(Properties config);
}