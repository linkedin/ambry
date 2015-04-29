package com.github.ambry.config;

/**
 * The configs for the store
 */

public class StoreConfig {

  /**
   * The factory class the store uses to creates its keys
   */
  @Config("store.key.factory")
  @Default("com.github.ambry.commons.BlobIdFactory")
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
  @Config("store.index.max.memory.size.bytes")
  @Default("20971520")
  public final int storeIndexMaxMemorySizeBytes;

  /**
   * The delay after which the data flush thread starts on startup
   */
  @Config("store.data.flush.delay.seconds")
  @Default("5")
  public final int storeDataFlushDelaySeconds;

  /**
   * The max number of the elements in the index that can be in memory for a single store
   */
  @Config("store.index.max.number.of.inmem.elements")
  @Default("10000")
  public final int storeIndexMaxNumberOfInmemElements;

  /**
   * The max number of entries that the journal will return each time it is queried for entries
   */
  @Config("store.max.number.of.entries.to.return.from.journal")
  @Default("5000")
  public final int storeMaxNumberOfEntriesToReturnFromJournal;

  /**
   * The factory class the store uses to create the journal.
   */
  @Config("store.journal.factory")
  @Default("com.github.ambry.commons.InMemoryJournalFactory")
  public final String storeJournalFactory;

  /**
   * The max probability of a false positive for the index bloom filter
   */
  @Config("store.index.bloom.max.false.positive.probability")
  @Default("0.01")
  public final double storeIndexBloomMaxFalsePositiveProbability;

  /**
   * The delay after which the data cleanup thread starts on startup
   */
  @Config("store.data.cleanup.delay.seconds")
  @Default("10")
  public final int storeDataCleanupDelaySeconds;

  /**
   * How long a key must be in deleted state before it is considered for cleanup.
   */
  @Config("store.data.cleanup.age.days")
  @Default("7")
  public final int storeDataCleanupAgeDays;

  /**
   * Cleanup batch size. The size of entries processed in every iteration
   */
  @Config("store.data.cleanup.batch.size.in.bytes")
  @Default("10 * 1024 * 1024")
  public final int storeDataCleanupBatchSizeInBytes;


  public StoreConfig(VerifiableProperties verifiableProperties) {

    storeKeyFactory = verifiableProperties.getString("store.key.factory", "com.github.ambry.commons.BlobIdFactory");
    storeDataFlushIntervalSeconds = verifiableProperties.getLong("store.data.flush.interval.seconds", 60);
    storeIndexMaxMemorySizeBytes = verifiableProperties.getInt("store.index.max.memory.size.bytes", 20 * 1024 * 1024);
    storeDataFlushDelaySeconds = verifiableProperties.getInt("store.data.flush.delay.seconds", 5);
    storeIndexMaxNumberOfInmemElements = verifiableProperties.getInt("store.index.max.number.of.inmem.elements", 10000);
    storeIndexBloomMaxFalsePositiveProbability =
        verifiableProperties.getDoubleInRange("store.index.bloom.max.false.positive.probability", 0.01, 0.0, 1.0);
    storeJournalFactory = verifiableProperties.getString("store.journal.factory", "com.github.ambry.store.InMemoryJournalFactory");
    storeMaxNumberOfEntriesToReturnFromJournal =
        verifiableProperties.getIntInRange("store.max.number.of.entries.to.return.from.journal", 5000, 1, 10000);
    storeDataCleanupDelaySeconds = verifiableProperties.getInt("store.data.cleanup.delay.seconds", 10);
    storeDataCleanupAgeDays = verifiableProperties.getInt("store.data.cleanup.age.days", 7);
    storeDataCleanupBatchSizeInBytes = verifiableProperties.getInt("store.data.cleanup.batch.size.in.bytes", 10 * 1024 * 1024);
  }
}

