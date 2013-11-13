package com.github.ambry.config;

/**
 * The configs for the store
 */

public class StoreConfig {


  /**
   * The directory for the store from where it can read the data
   */
  @Config("store.data.dir")
  @Default("/tmp/ambrydir")
  public final String storeDataDir;


  public StoreConfig(VerifiableProperties verifiableProperties) {

    storeDataDir = verifiableProperties.getString("store.data.dir", "/tmp/ambrydir");
  }
}

