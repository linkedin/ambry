package com.github.ambry.config;

/**
 * The configs for the store
 */

public class StoreConfig {


  /**
   * The directory for the store from where it can read the data
   */
  @Config("store.data.dir")
  @Default("/tmp")
  public final String storeDataDir;

  /**
   * The factory class the store uses to creates its keys
   */
  @Config("store.key.factory")
  @Default("com.github.ambry.shared.BlobIdFactory")
  public final String storeKeyFactory;

  /**
   * The frequency at which the data gets flushed to disk
   */
  @Config("store.data.flush.interval.seconds")
  @Default("60")
  public final long storeDataFlushIntervalSeconds;

  /**
   * The max size of the index that can reside in memory in bytes for a single store
   */
  @Config("store.index.memory.size.bytes")
  @Default("20971520")
  public final int storeIndexMemorySizeBytes;

  /**
   *  The delay after which the data flush thread starts on startup
   */
  @Config("store.data.flush.delay.seconds")
  @Default("5")
  public final int storeDataFlushDelaySeconds;


  public StoreConfig(VerifiableProperties verifiableProperties) {

    storeDataDir = verifiableProperties.getString("store.data.dir", "/tmp/ambrydir");
    storeKeyFactory = verifiableProperties.getString("store.key.factory", "com.github.ambry.shared.BlobIdFactory");
    storeDataFlushIntervalSeconds = verifiableProperties.getLong("store.data.flush.interval.seconds", 60);
    storeIndexMemorySizeBytes = verifiableProperties.getInt("store.index.memory.size.bytes", 20971520);
    storeDataFlushDelaySeconds = verifiableProperties.getInt("store.data.flush.delay.seconds", 5);
  }
}

