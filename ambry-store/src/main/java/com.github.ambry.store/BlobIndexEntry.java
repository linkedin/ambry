package com.github.ambry.store;

/**
 * A key and value that represents an index entry
 */
public class BlobIndexEntry {
  private StoreKey key;
  private BlobIndexValue value;

  public BlobIndexEntry(StoreKey key, BlobIndexValue value) {
    this.key = key;
    this.value = value;
  }

  public StoreKey getKey() {
    return this.key;
  }

  public BlobIndexValue getValue() {
    return this.value;
  }
}
