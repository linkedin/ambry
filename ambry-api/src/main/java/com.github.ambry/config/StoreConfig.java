package com.github.ambry.config;

/**
 * The configs for the store
 */

public class StoreConfig {


  /**
   * The number of network threads that the server uses for handling network requests
   */
  @Config("store.data.dir")
  @Default("/tmp/ambrydir")
  public final String storeDataDir;


  public StoreConfig(VerifiableProperties verifiableProperties) {

    storeDataDir = verifiableProperties.getString("store.data.dir", "/tmp/ambrydir");
  }
}

