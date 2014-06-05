package com.github.ambry.store;

/**
 * A key and value that represents an index entry
 */
public class IndexEntry {
  private StoreKey key;
  private IndexValue value;

  public IndexEntry(StoreKey key, IndexValue value) {
    this.key = key;
    this.value = value;
  }

  public StoreKey getKey() {
    return this.key;
  }

  public IndexValue getValue() {
    return this.value;
  }
}
